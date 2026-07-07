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

use karma_index::{surviving_zones_indexed, Predicate, Value, ZoneBlooms, ZoneMap};

/// A DataFusion table whose scan is pruned by a karma-index zone map.
///
/// `batches[i]` holds the rows of `zone_map.zones[i]` (same order). `field_ids`
/// maps each column *name* (how DataFusion refers to it) to its Iceberg *field id*
/// (how the zone map refers to it).
#[derive(Debug)]
pub struct KarmaZoneTable {
    schema: SchemaRef,
    zone_map: ZoneMap,
    /// Optional Bloom index — tightens `col = v` pruning on high-cardinality
    /// columns, where the zone map's min/max cannot help.
    blooms: Option<ZoneBlooms>,
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
        Self { schema, zone_map, blooms: None, batches, field_ids }
    }

    /// Attach a Bloom index (one Bloom per zone/column) to prune equality on
    /// high-cardinality columns.
    pub fn with_blooms(mut self, blooms: ZoneBlooms) -> Self {
        self.blooms = Some(blooms);
        self
    }

    /// The indices of the zones that survive the filters (must be scanned). This
    /// is the pruning payoff — pure and independently testable. Zones surviving
    /// *every* translatable predicate (AND semantics); untranslatable filters are
    /// ignored here (DataFusion re-applies them on the rows).
    pub fn surviving_zone_indices(&self, filters: &[Expr]) -> Vec<usize> {
        let mut preds = Vec::new();
        let mut inlists: Vec<(i32, Vec<Value>)> = Vec::new();
        for f in filters {
            self.collect(f, &mut preds, &mut inlists);
        }
        let zone_id = |i: usize| self.zone_map.zones[i].zone_id;
        let mut alive: Vec<usize> = (0..self.zone_map.zones.len()).collect();

        // Comparison predicates are ANDed: a zone must survive every one.
        for p in &preds {
            let surviving: HashSet<u32> =
                surviving_zones_indexed(&self.zone_map, self.blooms.as_ref(), p).into_iter().collect();
            alive.retain(|&i| surviving.contains(&zone_id(i)));
        }
        // `col IN (v₁ … vₙ)`: a zone survives if it survives `= vⱼ` for SOME j (OR).
        for (field, vals) in &inlists {
            let mut surviving: HashSet<u32> = HashSet::new();
            for v in vals {
                let pred = Predicate::Eq(*field, v.clone());
                surviving.extend(surviving_zones_indexed(&self.zone_map, self.blooms.as_ref(), &pred));
            }
            alive.retain(|&i| surviving.contains(&zone_id(i)));
        }
        alive
    }

    /// Flatten a filter into prunable constraints: comparison predicates (splitting
    /// top-level `AND`s, translating `col <op> literal` / `literal <op> col`), and
    /// `col IN (literal…)` lists. Anything else is left for DataFusion's re-check.
    fn collect(&self, expr: &Expr, preds: &mut Vec<Predicate>, inlists: &mut Vec<(i32, Vec<Value>)>) {
        match expr {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                if *op == Operator::And {
                    self.collect(left, preds, inlists);
                    self.collect(right, preds, inlists);
                    return;
                }
                // Normalize to `column <op> value`, flipping if the literal is on
                // the left (`5 < x` ≡ `x > 5`).
                let (col, op, scalar): (&Column, Operator, &ScalarValue) = match (left.as_ref(), right.as_ref()) {
                    (Expr::Column(c), Expr::Literal(s, _)) => (c, *op, s),
                    (Expr::Literal(s, _), Expr::Column(c)) => (c, flip_op(*op), s),
                    _ => return,
                };
                if let (Some(&fid), Some(value)) = (self.field_ids.get(col.name.as_str()), scalar_to_value(scalar)) {
                    if let Some(p) = make_predicate(fid, op, value) {
                        preds.push(p);
                    }
                }
            }
            Expr::InList(il) if !il.negated => {
                if let Expr::Column(c) = il.expr.as_ref() {
                    if let Some(&fid) = self.field_ids.get(c.name.as_str()) {
                        let vals: Vec<Value> = il
                            .list
                            .iter()
                            .filter_map(|e| match e {
                                Expr::Literal(s, _) => scalar_to_value(s),
                                _ => None,
                            })
                            .collect();
                        if !vals.is_empty() && vals.len() == il.list.len() {
                            inlists.push((fid, vals));
                        }
                    }
                }
            }
            _ => {}
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
                let (mut preds, mut inlists) = (Vec::new(), Vec::new());
                self.collect(f, &mut preds, &mut inlists);
                if preds.is_empty() && inlists.is_empty() {
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
    use karma_index::{Bloom, BloomEntry, ColumnStats, ZoneBlooms, ZoneStats, DEFAULT_BITS_PER_VALUE};

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

    // ── Bloom fixture: a high-cardinality `code` column (field 3) with OVERLAPPING
    //    zone min/max but DISJOINT values (zone 0 = even, zone 1 = odd). The zone
    //    map cannot prune `code = 'k-03'`; the Bloom can. ──
    fn bloom_fixture() -> (SchemaRef, ZoneMap, ZoneBlooms, Vec<RecordBatch>, HashMap<String, i32>) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("code", DataType::Utf8, false),
        ]));
        let batch = |ids: Vec<i64>, codes: Vec<&str>| {
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(ids)), Arc::new(StringArray::from(codes))]).unwrap()
        };
        let b0 = batch(vec![0, 1, 2], vec!["k-00", "k-02", "k-04"]);
        let b1 = batch(vec![3, 4, 5], vec!["k-01", "k-03", "k-05"]);

        let col = |fid: i32, mn: Value, mx: Value| ColumnStats { field_id: fid, min: mn, max: mx, null_count: 0, value_count: 3 };
        let zm = ZoneMap::new(vec![
            ZoneStats { zone_id: 0, row_offset: 0, row_count: 3, columns: vec![
                col(1, Value::I64(0), Value::I64(2)),
                col(3, Value::str("k-00"), Value::str("k-04")), // range straddles k-01..k-03
            ]},
            ZoneStats { zone_id: 1, row_offset: 3, row_count: 3, columns: vec![
                col(1, Value::I64(3), Value::I64(5)),
                col(3, Value::str("k-01"), Value::str("k-05")),
            ]},
        ]);
        let bloom = |vs: &[&str]| Bloom::build(&vs.iter().map(|s| Value::str(s)).collect::<Vec<_>>(), DEFAULT_BITS_PER_VALUE);
        let zb = ZoneBlooms::new(vec![
            BloomEntry { zone_id: 0, field_id: 3, bloom: bloom(&["k-00", "k-02", "k-04"]) },
            BloomEntry { zone_id: 1, field_id: 3, bloom: bloom(&["k-01", "k-03", "k-05"]) },
        ]);
        let field_ids = HashMap::from([("id".to_string(), 1), ("code".to_string(), 3)]);
        (schema, zm, zb, vec![b0, b1], field_ids)
    }

    #[test]
    fn bloom_prunes_where_zonemap_cannot() {
        use datafusion::prelude::{col, lit};
        let (schema, zm, zb, batches, fids) = bloom_fixture();
        let eq = col("code").eq(lit("k-03")); // odd → only in zone 1

        // Without a Bloom, the overlapping zone-map ranges keep BOTH zones.
        let t_no = KarmaZoneTable::new(schema.clone(), zm.clone(), batches.clone(), fids.clone());
        assert_eq!(t_no.surviving_zone_indices(&[eq.clone()]), vec![0, 1]);

        // With the Bloom, zone 0 (which lacks 'k-03') is pruned.
        let t = KarmaZoneTable::new(schema, zm, batches, fids).with_blooms(zb);
        assert_eq!(t.surviving_zone_indices(&[eq]), vec![1]);
    }

    #[tokio::test]
    async fn bloom_results_match_memtable() {
        let (schema, zm, zb, batches, fids) = bloom_fixture();
        let kctx = SessionContext::new();
        kctx.register_table("t", Arc::new(KarmaZoneTable::new(schema.clone(), zm, batches.clone(), fids).with_blooms(zb))).unwrap();
        let mctx = SessionContext::new();
        mctx.register_table("t", Arc::new(MemTable::try_new(schema, vec![batches]).unwrap())).unwrap();

        for sql in [
            "SELECT id FROM t WHERE code = 'k-03'",
            "SELECT id FROM t WHERE code = 'k-02'",
            "SELECT id FROM t WHERE code = 'k-99'",
            "SELECT id FROM t WHERE code IN ('k-01', 'k-04')",
        ] {
            assert_eq!(ids(&kctx, sql).await, ids(&mctx, sql).await, "mismatch for: {sql}");
        }
    }
}
