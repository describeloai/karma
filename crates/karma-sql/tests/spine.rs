//! The Substrait-spine de-risk: prove that routing a query through the Substrait
//! contract (SQL → Substrait bytes → LogicalPlan → execute) over a karma-parquet
//! `TableProvider`
//!   1. returns **identical results** to direct execution, and
//!   2. still drives **Karma row-group pruning** — the filter survives the round-trip
//!      and reaches the provider's `scan`, so the index still earns its keep.
//! If either failed, the "DataFusion + Karma, contract in Substrait" architecture would
//! be unsound; this test is the go/no-go before building the object-store reader.

use std::sync::{Arc, Mutex};

use datafusion::arrow::array::Int64Array;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;

use karma_parquet::{bench, ParquetZoneTable};

const ROWS: u64 = 50_000;
const RG: u64 = 5_000; // → 10 row groups

fn extract(batches: &[RecordBatch]) -> Vec<(i64, i64)> {
    let mut out = Vec::new();
    for b in batches {
        let id = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let uid = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..b.num_rows() {
            out.push((id.value(i), uid.value(i)));
        }
    }
    out.sort();
    out
}

#[tokio::test]
async fn substrait_roundtrip_preserves_results_and_pruning() {
    let path = std::env::temp_dir().join("karma_sql_spine.parquet");
    bench::generate_parquet(&path, ROWS, RG).unwrap();
    let sidecar = bench::build(&path).unwrap();
    assert_eq!(sidecar.zone_map.zones.len(), 10);

    // Record what each scan reads, so we can prove pruning fired through Substrait.
    let scans: Arc<Mutex<Vec<Vec<usize>>>> = Arc::new(Mutex::new(Vec::new()));
    let table = ParquetZoneTable::from_sidecar(path.clone(), sidecar).observe_scans(scans.clone());
    let ctx = SessionContext::new();
    ctx.register_table("events", Arc::new(table)).unwrap();

    // A scattered, unique high-cardinality point lookup — only the bloom can prune it.
    let q = bench::queries(ROWS).into_iter().find(|q| q.name == "event_id = existing").unwrap();
    let sql = format!("SELECT id, user_id FROM events WHERE {}", q.sql_where);

    // Baseline: direct execution.
    let baseline = extract(&ctx.sql(&sql).await.unwrap().collect().await.unwrap());
    assert!(!baseline.is_empty(), "the point lookup matches at least one row");

    // Through the Substrait contract.
    let bytes = karma_sql::sql_to_substrait_bytes(&ctx, &sql).await.unwrap();
    assert!(!bytes.is_empty(), "the Substrait contract is a non-empty byte artifact");

    scans.lock().unwrap().clear(); // drop the baseline scan; measure only the Substrait one
    let via_substrait = extract(&karma_sql::execute_substrait_bytes(&ctx, &bytes).await.unwrap());

    // (1) correctness through the contract.
    assert_eq!(baseline, via_substrait, "results identical through the Substrait contract");

    // (2) Karma pruning survived the round-trip.
    let recorded = scans.lock().unwrap().clone();
    assert!(!recorded.is_empty(), "the Substrait plan drove a scan on the karma provider");
    let read = recorded.last().unwrap();
    assert!(
        read.len() < 10,
        "Karma pruning fired through Substrait — read {} of 10 row groups (filter survived the contract)",
        read.len()
    );
}

#[tokio::test]
async fn zone_map_range_prunes_through_substrait() {
    let path = std::env::temp_dir().join("karma_sql_spine_range.parquet");
    bench::generate_parquet(&path, ROWS, RG).unwrap();
    let sidecar = bench::build(&path).unwrap();

    let scans: Arc<Mutex<Vec<Vec<usize>>>> = Arc::new(Mutex::new(Vec::new()));
    let table = ParquetZoneTable::from_sidecar(path.clone(), sidecar).observe_scans(scans.clone());
    let ctx = SessionContext::new();
    ctx.register_table("events", Arc::new(table)).unwrap();

    // Clustered id → the zone map alone prunes; verify it survives Substrait too.
    let sql = "SELECT id, user_id FROM events WHERE id >= 45000";
    let baseline = extract(&ctx.sql(sql).await.unwrap().collect().await.unwrap());

    let (batches, _bytes) = karma_sql::run_via_substrait(&ctx, sql).await.unwrap();
    assert_eq!(baseline, extract(&batches), "range results identical through Substrait");

    let read = scans.lock().unwrap().last().unwrap().clone();
    assert!(read.len() <= 2, "id>=45000 prunes to the last row group(s) through Substrait: {read:?}");
}
