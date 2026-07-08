//! # karma-sql — the SQL engine spine
//!
//! `SQL → DataFusion LogicalPlan → **Substrait** → DataFusion LogicalPlan → execute`,
//! over karma-parquet `TableProvider`s (Iceberg-backed, Karma-pruned). This is the
//! seam the Carbon SQL Editor will call: `(userSql, catalog) → {rows, columns}`.
//!
//! **Substrait is the contract boundary.** The plan we hand the engine is a portable,
//! byte-serializable, engine-neutral artifact — so the *same* plan can be executed by
//! another engine (Doberman) tomorrow without touching the front, and can be persisted
//! and diffed (the shadow/differential harness). We adopt DataFusion as the executor;
//! we do not rebuild it. The ownable work is the front (binding + our rules) and this
//! Substrait boundary — never a new optimizer.
//!
//! This crate is currently the **spine spike**: it proves the round-trip preserves
//! results *and* Karma row-group pruning (filters survive the Substrait contract and
//! still reach the provider's `scan`). The SQL front (our rules) and the object-store /
//! Iceberg catalog binding land next — see `docs/proposals/0002-karma-sql-engine.md`.

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::dataframe::DataFrame;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;

use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use datafusion_substrait::logical_plan::producer::to_substrait_plan;
use prost::Message;
use substrait::proto::Plan;

/// Bind `sql` (against `ctx`'s catalog + rules) to a logical plan and lower it to the
/// **Substrait contract**, returned as bytes — the portable, diffable, engine-neutral
/// plan artifact.
pub async fn sql_to_substrait_bytes(ctx: &SessionContext, sql: &str) -> DFResult<Vec<u8>> {
    // The analyzed (not yet physically-optimized) plan keeps explicit Filter nodes,
    // which round-trip cleanly and let the executor re-push them into the scan.
    let plan = ctx.state().create_logical_plan(sql).await?;
    let substrait = to_substrait_plan(&plan, &ctx.state())?;
    Ok(substrait.encode_to_vec())
}

/// Parse a Substrait plan from bytes back into a DataFusion logical plan (resolving
/// tables against `ctx`'s catalog).
pub async fn substrait_bytes_to_plan(ctx: &SessionContext, bytes: &[u8]) -> DFResult<LogicalPlan> {
    let plan = Plan::decode(bytes).map_err(|e| DataFusionError::External(Box::new(e)))?;
    from_substrait_plan(&ctx.state(), &plan).await
}

/// Execute a Substrait plan (bytes) on `ctx`, collecting the result batches.
pub async fn execute_substrait_bytes(ctx: &SessionContext, bytes: &[u8]) -> DFResult<Vec<RecordBatch>> {
    let logical = substrait_bytes_to_plan(ctx, bytes).await?;
    DataFrame::new(ctx.state(), logical).collect().await
}

/// The whole spine in one call: `SQL → Substrait bytes → execute`. Returns the result
/// batches *and* the Substrait contract bytes (persist/diff them).
pub async fn run_via_substrait(ctx: &SessionContext, sql: &str) -> DFResult<(Vec<RecordBatch>, Vec<u8>)> {
    let bytes = sql_to_substrait_bytes(ctx, sql).await?;
    let batches = execute_substrait_bytes(ctx, &bytes).await?;
    Ok((batches, bytes))
}
