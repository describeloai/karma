//! # karma-datafusion — a pruning `TableProvider` over karma-index zone maps
//!
//! This is where the index earns its keep: a DataFusion [`TableProvider`] that,
//! given a query's filters, uses a [`ZoneMap`] to **skip whole zones** (row
//! ranges) that provably cannot match — so the scan reads less.
//!
//! The data source here is deliberately **in-memory** (one [`RecordBatch`] per
//! zone): this crate proves the *DataFusion integration* — filter pushdown,
//! `Expr` → predicate translation, AND-of-predicates pruning, projection, and
//! (crucially) *correctness* — not the object-store / Parquet read path, which is
//! a separate concern. Swapping the in-memory zones for Parquet row groups later
//! changes only where the batches come from, not this wiring.
//!
//! ## Correctness
//! We report pushed-down filters as [`Inexact`](TableProviderFilterPushDown::Inexact):
//! we *prune zones*, but DataFusion still applies the real filter on the surviving
//! rows. So the result is correct **regardless** of pruning — pruning only changes
//! how much is read, never what comes out. (And the pruning itself is conservative
//! in `karma-index`.) The tests assert this by diffing against an unindexed
//! `MemTable` over the same data.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{Column, ScalarValue};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;

use karma_index::{surviving_zones, Predicate, Value, ZoneMap};

/// A DataFusion table whose scan is pruned by a karma-index zone map.
///
/// `batches[i]` holds the rows of `zone_map.zones[i]` (same order). `field_ids`
/// maps each column *name* (how DataFusion refers to it) to its Iceberg *field id*
/// (how the zone map refers to it).
#[derive(Debug)]
pub struct KarmaZoneTable {
    schema: SchemaRef,
    zone_map: ZoneMap,
    batches: Vec<RecordBatch>,
    field_ids: HashMap<String, i32>,
}

impl KarmaZoneTable {
    pub fn new(
        schema: SchemaRef,
        zone_map: ZoneMap,
        batches: Vec<RecordBatch>,
        field_ids: HashMap<String, i32>,
    ) -> Self {
        assert_eq!(zone_map.zones.len(), batches.len(), "one batch per zone");
        Self { schema, zone_map, batches, field_ids }
    }

    /// The indices of the zones that survive the filters (must be scanned). This
    /// is the pruning payoff — pure and independently testable. Zones surviving
    /// *every* translatable predicate (AND semantics); untranslatable filters are
    /// ignored here (DataFusion re-applies them on the rows).
    pub fn surviving_zone_indices(&self, filters: &[Expr]) -> Vec<usize> {
        let mut preds = Vec::new();
        for f in filters {
            self.collect_predicates(f, &mut preds);
        }
        let mut alive: Vec<usize> = (0..self.zone_map.zones.len()).collect();
        for p in &preds {
            let surviving: HashSet<u32> = surviving_zones(&self.zone_map, p).into_iter().collect();
            alive.retain(|&i| surviving.contains(&self.zone_map.zones[i].zone_id));
        }
        alive
    }

    /// Flatten a filter into zone-map predicates, splitting top-level `AND`s and
    /// translating each `col <op> literal` (or `literal <op> col`) comparison.
    fn collect_predicates(&self, expr: &Expr, out: &mut Vec<Predicate>) {
        let Expr::BinaryExpr(BinaryExpr { left, op, right }) = expr else {
            return;
        };
        if *op == Operator::And {
            self.collect_predicates(left, out);
            self.collect_predicates(right, out);
            return;
        }
        // Normalize to `column <op> value`, flipping the operator if the literal
        // is on the left (`5 < x`  ≡  `x > 5`).
        let (col, op, scalar): (&Column, Operator, &ScalarValue) = match (left.as_ref(), right.as_ref()) {
            (Expr::Column(c), Expr::Literal(s, _)) => (c, *op, s),
            (Expr::Literal(s, _), Expr::Column(c)) => (c, flip_op(*op), s),
            _ => return,
        };
        let (Some(&field_id), Some(value)) = (self.field_ids.get(col.name.as_str()), scalar_to_value(scalar))
        else {
            return;
        };
        if let Some(p) = make_predicate(field_id, op, value) {
            out.push(p);
        }
    }
}

fn flip_op(op: Operator) -> Operator {
    match op {
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        other => other,
    }
}

fn make_predicate(field_id: i32, op: Operator, v: Value) -> Option<Predicate> {
    Some(match op {
        Operator::Eq => Predicate::Eq(field_id, v),
        Operator::Lt => Predicate::Lt(field_id, v),
        Operator::LtEq => Predicate::LtEq(field_id, v),
        Operator::Gt => Predicate::Gt(field_id, v),
        Operator::GtEq => Predicate::GtEq(field_id, v),
        _ => return None, // NotEq / others: a zone map can't prune these
    })
}

fn scalar_to_value(s: &ScalarValue) -> Option<Value> {
    Some(match s {
        ScalarValue::Int64(Some(v)) => Value::I64(*v),
        ScalarValue::Int32(Some(v)) => Value::I64(*v as i64),
        ScalarValue::Float64(Some(v)) => Value::F64(*v),
        ScalarValue::Float32(Some(v)) => Value::F64(*v as f64),
        ScalarValue::Utf8(Some(v)) | ScalarValue::LargeUtf8(Some(v)) => Value::Bytes(v.clone().into_bytes()),
        ScalarValue::Boolean(Some(v)) => Value::Bool(*v),
        _ => return None,
    })
}

#[async_trait]
impl TableProvider for KarmaZoneTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> DFResult<Vec<TableProviderFilterPushDown>> {
        // Inexact where we can extract at least one prunable predicate (we prune,
        // DataFusion re-checks rows); Unsupported otherwise.
        Ok(filters
            .iter()
            .map(|f| {
                let mut preds = Vec::new();
                self.collect_predicates(f, &mut preds);
                if preds.is_empty() {
                    TableProviderFilterPushDown::Unsupported
                } else {
                    TableProviderFilterPushDown::Inexact
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let alive = self.surviving_zone_indices(filters);
        let batches: Vec<RecordBatch> = alive.iter().map(|&i| self.batches[i].clone()).collect();
        // One partition holding the surviving zones' batches, in zone order.
        let exec = MemorySourceConfig::try_new_exec(&[batches], self.schema.clone(), projection.cloned())?;
        Ok(exec)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;
    use karma_index::{ColumnStats, ZoneStats};

    // Two zones. id: zone0 ∈ [0,4], zone1 ∈ [20,24]. name mirrors id as strings.
    // field ids: id=1, name=2.
    fn fixture() -> (SchemaRef, ZoneMap, Vec<RecordBatch>, HashMap<String, i32>) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = |ids: Vec<i64>, names: Vec<&str>| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(ids)),
                    Arc::new(StringArray::from(names)),
                ],
            )
            .unwrap()
        };
        let b0 = batch(vec![0, 1, 2, 3, 4], vec!["r0", "r1", "r2", "r3", "r4"]);
        let b1 = batch(vec![20, 21, 22, 23, 24], vec!["r20", "r21", "r22", "r23", "r24"]);

        let col = |fid: i32, mn: Value, mx: Value, vc: u64| ColumnStats {
            field_id: fid,
            min: mn,
            max: mx,
            null_count: 0,
            value_count: vc,
        };
        let zm = ZoneMap::new(vec![
            ZoneStats {
                zone_id: 0,
                row_offset: 0,
                row_count: 5,
                columns: vec![
                    col(1, Value::I64(0), Value::I64(4), 5),
                    col(2, Value::str("r0"), Value::str("r4"), 5),
                ],
            },
            ZoneStats {
                zone_id: 1,
                row_offset: 5,
                row_count: 5,
                columns: vec![
                    col(1, Value::I64(20), Value::I64(24), 5),
                    col(2, Value::str("r20"), Value::str("r24"), 5),
                ],
            },
        ]);
        let field_ids = HashMap::from([("id".to_string(), 1), ("name".to_string(), 2)]);
        (schema, zm, vec![b0, b1], field_ids)
    }

    fn karma_ctx() -> SessionContext {
        let (schema, zm, batches, field_ids) = fixture();
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(KarmaZoneTable::new(schema, zm, batches, field_ids)))
            .unwrap();
        ctx
    }

    fn mem_ctx() -> SessionContext {
        let (schema, _zm, batches, _f) = fixture();
        let ctx = SessionContext::new();
        ctx.register_table("t", Arc::new(MemTable::try_new(schema, vec![batches]).unwrap()))
            .unwrap();
        ctx
    }

    async fn ids(ctx: &SessionContext, sql: &str) -> Vec<i64> {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let mut out = Vec::new();
        for b in &batches {
            let col = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..col.len() {
                out.push(col.value(i));
            }
        }
        out.sort();
        out
    }

    // ── the payoff: pruning actually drops zones ──
    #[test]
    fn prunes_zones_by_predicate() {
        let (schema, zm, batches, field_ids) = fixture();
        let t = KarmaZoneTable::new(schema, zm, batches, field_ids);
        let gt10 = datafusion::prelude::col("id").gt(datafusion::prelude::lit(10i64));
        assert_eq!(t.surviving_zone_indices(&[gt10]), vec![1], "id>10 prunes zone 0");

        let eq100 = datafusion::prelude::col("id").eq(datafusion::prelude::lit(100i64));
        assert_eq!(t.surviving_zone_indices(&[eq100]), Vec::<usize>::new(), "id=100 prunes both");

        let lt3 = datafusion::prelude::col("id").lt(datafusion::prelude::lit(3i64));
        assert_eq!(t.surviving_zone_indices(&[lt3]), vec![0], "id<3 prunes zone 1");

        // AND of two predicates.
        let both = datafusion::prelude::col("id")
            .gt(datafusion::prelude::lit(1i64))
            .and(datafusion::prelude::col("id").lt(datafusion::prelude::lit(24i64)));
        assert_eq!(t.surviving_zone_indices(&[both]), vec![0, 1]);

        // Non-indexed / unsupported → never prunes (conservative).
        let neq = datafusion::prelude::col("name").not_eq(datafusion::prelude::lit("r0"));
        assert_eq!(t.surviving_zone_indices(&[neq]), vec![0, 1], "NotEq can't prune");
    }

    // ── correctness: pruned scan == full scan, for every query ──
    #[tokio::test]
    async fn results_match_unindexed_memtable() {
        let (kc, mc) = (karma_ctx(), mem_ctx());
        for sql in [
            "SELECT id FROM t WHERE id > 10",
            "SELECT id FROM t WHERE id = 22",
            "SELECT id FROM t WHERE id = 100",
            "SELECT id FROM t WHERE id < 3",
            "SELECT id FROM t WHERE id >= 4 AND id <= 21",
            "SELECT id FROM t WHERE name <> 'r0'",
            "SELECT id FROM t",
        ] {
            assert_eq!(ids(&kc, sql).await, ids(&mc, sql).await, "mismatch for: {sql}");
        }
    }

    // ── projection + a real end-to-end pruned query result ──
    #[tokio::test]
    async fn projected_pruned_query() {
        let ctx = karma_ctx();
        assert_eq!(ids(&ctx, "SELECT id FROM t WHERE id > 10").await, vec![20, 21, 22, 23, 24]);
        assert_eq!(ids(&ctx, "SELECT id FROM t WHERE id < 3").await, vec![0, 1, 2]);
        assert!(ids(&ctx, "SELECT id FROM t WHERE id = 100").await.is_empty());
    }
}
