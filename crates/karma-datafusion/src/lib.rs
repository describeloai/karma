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

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;

use karma_index::{ZoneBlooms, ZoneMap};

pub mod translate;

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

    /// The indices of the zones that survive the filters (must be scanned) — the
    /// pruning payoff, pure and independently testable. Delegates to the shared
    /// [`translate`] module so this and the Parquet provider never drift.
    pub fn surviving_zone_indices(&self, filters: &[Expr]) -> Vec<usize> {
        translate::surviving_zone_indices(&self.zone_map, self.blooms.as_ref(), &self.field_ids, filters)
    }
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
        Ok(translate::filters_pushdown(&self.field_ids, filters))
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
    use datafusion::common::ScalarValue;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;
    use karma_index::{Bloom, BloomEntry, ColumnStats, Value, ZoneBlooms, ZoneStats, DEFAULT_BITS_PER_VALUE};

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

    // ── decimal + temporal exact bounds (Deliverable B) ──
    // amount Decimal(10,2) field 1, ts Timestamp(µs) field 2. Overlap-free zones:
    // zone 0 = 1.00..50.00 in 2023; zone 1 = 100.50..999.99 in 2024.
    fn decimal_temporal_fixture() -> (SchemaRef, ZoneMap, Vec<RecordBatch>, HashMap<String, i32>) {
        use datafusion::arrow::array::{Decimal128Array, TimestampMicrosecondArray};
        use datafusion::arrow::datatypes::TimeUnit;
        let schema = Arc::new(Schema::new(vec![
            Field::new("amount", DataType::Decimal128(10, 2), false),
            Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        ]));
        let ts_2023: i64 = 1_672_531_200_000_000; // 2023-01-01T00:00:00Z
        let ts_2024: i64 = 1_704_067_200_000_000; // 2024-01-01T00:00:00Z
        let batch = |amts: Vec<i128>, tss: Vec<i64>| {
            let a = Decimal128Array::from(amts).with_precision_and_scale(10, 2).unwrap();
            let t = TimestampMicrosecondArray::from(tss);
            RecordBatch::try_new(schema.clone(), vec![Arc::new(a), Arc::new(t)]).unwrap()
        };
        let b0 = batch(vec![100, 2500, 5000], vec![ts_2023, ts_2023 + 1_000_000, ts_2024 - 1]);
        let b1 = batch(vec![10050, 50000, 99999], vec![ts_2024, ts_2024 + 1_000_000, ts_2024 + 2_000_000]);

        let dec = |u: i128| Value::Decimal { unscaled: u, scale: 2 };
        let col = |fid, mn, mx| ColumnStats { field_id: fid, min: mn, max: mx, null_count: 0, value_count: 3 };
        let zm = ZoneMap::new(vec![
            ZoneStats { zone_id: 0, row_offset: 0, row_count: 3, columns: vec![
                col(1, dec(100), dec(5000)),
                col(2, Value::Timestamp(ts_2023), Value::Timestamp(ts_2024 - 1)),
            ]},
            ZoneStats { zone_id: 1, row_offset: 3, row_count: 3, columns: vec![
                col(1, dec(10050), dec(99999)),
                col(2, Value::Timestamp(ts_2024), Value::Timestamp(ts_2024 + 2_000_000)),
            ]},
        ]);
        let field_ids = HashMap::from([("amount".to_string(), 1), ("ts".to_string(), 2)]);
        (schema, zm, vec![b0, b1], field_ids)
    }

    // The pruning payoff for exact decimal/temporal bounds (constructed Exprs so the
    // literal reaches the provider at the column's native scale/unit, not SQL-coerced).
    #[test]
    fn decimal_and_temporal_prune() {
        let (schema, zm, batches, fids) = decimal_temporal_fixture();
        let t = KarmaZoneTable::new(schema, zm, batches, fids);

        // amount > 100.50 (unscaled 10050, scale 2) → zone 0 (max 50.00) pruned.
        let gt = datafusion::prelude::col("amount")
            .gt(Expr::Literal(ScalarValue::Decimal128(Some(10050), 10, 2), None));
        assert_eq!(t.surviving_zone_indices(&[gt]), vec![1], "amount>100.50 prunes zone 0");

        // amount = 25.00 (unscaled 2500) → only zone 0.
        let eq = datafusion::prelude::col("amount")
            .eq(Expr::Literal(ScalarValue::Decimal128(Some(2500), 10, 2), None));
        assert_eq!(t.surviving_zone_indices(&[eq]), vec![0], "amount=25.00 prunes zone 1");

        // ts >= 2024-01-01 → zone 0 pruned.
        let ts_2024: i64 = 1_704_067_200_000_000;
        let ge = datafusion::prelude::col("ts")
            .gt_eq(Expr::Literal(ScalarValue::TimestampMicrosecond(Some(ts_2024), None), None));
        assert_eq!(t.surviving_zone_indices(&[ge]), vec![1], "ts>='2024-01-01' prunes zone 0");

        // A millisecond literal normalizes to micros before comparison.
        let ge_ms = datafusion::prelude::col("ts")
            .gt_eq(Expr::Literal(ScalarValue::TimestampMillisecond(Some(ts_2024 / 1_000), None), None));
        assert_eq!(t.surviving_zone_indices(&[ge_ms]), vec![1], "ms literal normalizes to µs");
    }

    // Correctness: the pruned scan returns exactly the full-scan rows, for decimal
    // and timestamp predicates driven through real SQL (including string→timestamp
    // and decimal-literal coercion, where pruning may fall back to conservative).
    #[tokio::test]
    async fn decimal_temporal_results_match_memtable() {
        let (schema, zm, batches, fids) = decimal_temporal_fixture();
        let kctx = SessionContext::new();
        kctx.register_table("t", Arc::new(KarmaZoneTable::new(schema.clone(), zm, batches.clone(), fids))).unwrap();
        let mctx = SessionContext::new();
        mctx.register_table("t", Arc::new(MemTable::try_new(schema, vec![batches]).unwrap())).unwrap();

        async fn dump(ctx: &SessionContext, sql: &str) -> String {
            let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
            datafusion::arrow::util::pretty::pretty_format_batches(&batches).unwrap().to_string()
        }
        for sql in [
            "SELECT amount, ts FROM t WHERE amount > 100.50 ORDER BY amount",
            "SELECT amount, ts FROM t WHERE amount = 25.00 ORDER BY amount",
            "SELECT amount, ts FROM t WHERE ts >= '2024-01-01T00:00:00' ORDER BY ts",
            "SELECT amount, ts FROM t WHERE ts < '2024-01-01T00:00:00' ORDER BY ts",
            "SELECT amount, ts FROM t ORDER BY amount",
        ] {
            assert_eq!(dump(&kctx, sql).await, dump(&mctx, sql).await, "mismatch for: {sql}");
        }
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
