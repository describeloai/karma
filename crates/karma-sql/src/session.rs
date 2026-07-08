//! `KarmaSession` — the SQL front (spec Build 4). It binds the user's SQL against **our
//! catalog + our rules**, lowers it to the **Substrait contract**, executes it over
//! Karma-pruned Iceberg tables (DataFusion the adopted executor), and returns **explicit
//! columns + types**. This is the seam Carbon's SQL Editor calls — `run(sql) →
//! {columns, types, rows}`; Build 5 wires Node to it (gated by the freshness oracle, PG
//! fallback), deleting the Postgres dialect shim.
//!
//! The three phases mirror Carbon's durable design (`docs/warehouse-sql/` Block 2 §0):
//! **① resolve** (FQN via the catalog) and **② validate** are the durable language;
//! **③ execute** is what Karma inverts — and it stays behind the Substrait boundary so
//! the plan is portable (Doberman-swappable) and diffable (the shadow harness).

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::dataframe::DataFrame;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::SessionContext;
use datafusion_substrait::logical_plan::producer::to_substrait_plan;
use prost::Message;
use std::sync::Arc;

use karma_parquet::SnapshotResolver;

use crate::catalog::{IndexStrategy, KarmaCatalogList, SnapshotTableResolver, TableResolver};
use crate::substrait_bytes_to_plan;
use crate::udf::register_karma_functions;

/// A result column's name and (Arrow) type — the explicit contract that fixes Carbon's
/// weak client-side column inference.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnMeta {
    pub name: String,
    pub data_type: String,
}

/// The engine's answer: explicit `columns` (name + type), the result `batches` (Arrow),
/// and the `substrait` contract bytes (persist / diff them). Serializing `batches` to a
/// wire `{rows}` shape (ndjson / arrow) is the Node boundary's job (Build 5).
pub struct QueryResult {
    pub columns: Vec<ColumnMeta>,
    pub batches: Vec<RecordBatch>,
    pub substrait: Vec<u8>,
}

impl QueryResult {
    pub fn row_count(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }
}

/// Front rules that shape the plan. v1 is thin but real; the rest grow against Block 2.
#[derive(Clone, Debug)]
pub struct KarmaConfig {
    /// Rows returned when a query has no top-level `LIMIT` (Carbon's default 100).
    pub default_limit: usize,
    /// Ceiling for an explicit `LIMIT` (Carbon's max 5000). Clamping is a follow-on;
    /// held here so the policy has one home.
    pub max_limit: usize,
}

impl Default for KarmaConfig {
    fn default() -> Self {
        Self { default_limit: 100, max_limit: 5000 }
    }
}

/// The SQL front. Owns a DataFusion [`SessionContext`] configured with our resolver-
/// backed catalog and our function registry.
pub struct KarmaSession {
    ctx: SessionContext,
    config: KarmaConfig,
}

impl KarmaSession {
    /// Build a front over any [`TableResolver`] (the core constructor; tests inject a
    /// resolver returning observed tables).
    pub fn new(resolver: Arc<dyn TableResolver>) -> Self {
        let ctx = SessionContext::new();
        // Our vocabulary via DataFusion's extension points: the catalog (FQN resolution)
        // and the function registry (Databricks-parity funcs DataFusion lacks).
        ctx.register_catalog_list(Arc::new(KarmaCatalogList::new(resolver)));
        register_karma_functions(&ctx);
        Self { ctx, config: KarmaConfig::default() }
    }

    /// Prod path: a Lakekeeper [`SnapshotResolver`] + an index strategy.
    pub fn from_snapshot_resolver(
        resolver: Arc<dyn SnapshotResolver>,
        index: Arc<dyn IndexStrategy>,
    ) -> Self {
        Self::new(Arc::new(SnapshotTableResolver::new(resolver, index)))
    }

    /// Override the front rules (limits).
    pub fn with_config(mut self, config: KarmaConfig) -> Self {
        self.config = config;
        self
    }

    /// The underlying context (for advanced callers / tests).
    pub fn context(&self) -> &SessionContext {
        &self.ctx
    }

    /// Run `sql` end-to-end: **① resolve + ② validate** (bind against our catalog/rules),
    /// lower to the **Substrait contract**, then **③ execute** the contract over Karma-
    /// pruned tables. Returns explicit columns + types + the contract bytes.
    pub async fn run(&self, sql: &str) -> DFResult<QueryResult> {
        // ①② bind → analyzed logical plan (keeps explicit Filter nodes for pushdown).
        let plan = self.ctx.state().create_logical_plan(sql).await?;
        let plan = self.apply_auto_limit(plan)?;

        // Explicit columns + types from the bound plan's schema (robust to zero rows and
        // to any Substrait name normalization — we read the user-facing names here).
        let columns = plan
            .schema()
            .fields()
            .iter()
            .map(|f| ColumnMeta { name: f.name().clone(), data_type: f.data_type().to_string() })
            .collect();

        // Lower to the Substrait contract (portable, diffable, engine-neutral).
        let substrait = to_substrait_plan(&plan, &self.ctx.state())?.encode_to_vec();

        // ③ execute *through the contract* so filter pushdown / Karma pruning survive it.
        let logical = substrait_bytes_to_plan(&self.ctx, &substrait).await?;
        let batches = DataFrame::new(self.ctx.state(), logical).collect().await?;

        Ok(QueryResult { columns, batches, substrait })
    }

    /// Rule: append the default `LIMIT` to a relation-yielding query that doesn't bound
    /// its own rows. Metadata/DDL/DML/EXPLAIN/ANALYZE and an existing `LIMIT` are left
    /// untouched (the metadata plane and read-only gate live in Carbon / other blocks).
    fn apply_auto_limit(&self, plan: LogicalPlan) -> DFResult<LogicalPlan> {
        let untouched = matches!(
            plan,
            LogicalPlan::Limit(_)
                | LogicalPlan::Explain(_)
                | LogicalPlan::Analyze(_)
                | LogicalPlan::Statement(_)
                | LogicalPlan::Dml(_)
                | LogicalPlan::Ddl(_)
                | LogicalPlan::Copy(_)
                | LogicalPlan::DescribeTable(_)
        );
        if untouched {
            return Ok(plan);
        }
        LogicalPlanBuilder::from(plan).limit(0, Some(self.config.default_limit))?.build()
    }
}
