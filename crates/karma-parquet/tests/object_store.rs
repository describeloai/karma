//! The object-store read-path (R2/S3), exercised over an in-memory object store:
//!   1. a sidecar built **over the object store** + a `ParquetZoneTable` reading from
//!      it returns exactly what DataFusion's native reader over the same file does
//!      (differential correctness), while
//!   2. **pruning still fires over the network path** (the scan reads <10 of 10 row
//!      groups), and
//!   3. the reader **honors the row-group selection** — reading `[1]` fetches only row
//!      group 1's rows, i.e. the pruned row groups' bytes are never requested.
//! In-memory `ObjectStore` stands in for R2 — the code path (`ParquetObjectReader` +
//! `with_row_groups`) is identical; only the `AmazonS3Builder` config differs.

use std::sync::{Arc, Mutex};

use datafusion::arrow::array::Int64Array;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use object_store::memory::InMemory;
use object_store::{path::Path as ObjPath, ObjectStore, PutOptions, PutPayload};

use karma_parquet::{bench, build, read, ParquetZoneTable};

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

/// Generate the bench fixture locally, then upload it into an in-memory object store.
async fn put_fixture(tag: &str) -> (Arc<dyn ObjectStore>, ObjPath, std::path::PathBuf) {
    let tmp = std::env::temp_dir().join(format!("karma_obj_{tag}.parquet"));
    bench::generate_parquet(&tmp, ROWS, RG).unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let data = std::fs::read(&tmp).unwrap();
    let path = ObjPath::from("warehouse/events.parquet");
    store.put_opts(&path, PutPayload::from(bytes::Bytes::from(data)), PutOptions::default()).await.unwrap();
    (store, path, tmp)
}

async fn rows(ctx: &SessionContext, sql: &str) -> Vec<(i64, i64)> {
    extract(&ctx.sql(sql).await.unwrap().collect().await.unwrap())
}

#[tokio::test]
async fn object_store_read_matches_local_and_prunes() {
    let (store, path, tmp) = put_fixture("main").await;

    // The sidecar is built ENTIRELY over the object store (no local file access).
    let sidecar = build::build_sidecar_object(store.clone(), &path, &bench::index_fields()).await.unwrap();
    assert_eq!(sidecar.zone_map.zones.len(), 10, "one zone per row group, indexed over the object store");

    let scans: Arc<Mutex<Vec<Vec<usize>>>> = Arc::new(Mutex::new(Vec::new()));
    let table = ParquetZoneTable::from_sidecar_object(store.clone(), path.clone(), sidecar).observe_scans(scans.clone());
    let kctx = SessionContext::new();
    kctx.register_table("events", Arc::new(table)).unwrap();

    // Truth: DataFusion's own Parquet reader over the same file (local).
    let fctx = SessionContext::new();
    fctx.register_parquet("events", tmp.to_str().unwrap(), ParquetReadOptions::default()).await.unwrap();

    let ev = bench::queries(ROWS).into_iter().find(|q| q.name == "event_id = existing").unwrap();
    for sql in [
        format!("SELECT id, user_id FROM events WHERE {}", ev.sql_where), // bloom
        "SELECT id, user_id FROM events WHERE id >= 45000".to_string(),   // zone map (range)
        "SELECT id, user_id FROM events WHERE id < 5000".to_string(),     // zone map (range)
    ] {
        scans.lock().unwrap().clear();
        assert_eq!(rows(&kctx, &sql).await, rows(&fctx, &sql).await, "object-store read == local for: {sql}");
        let read = scans.lock().unwrap().last().unwrap().clone();
        assert!(read.len() < 10, "pruned over the object store for `{sql}`: read {} of 10 row groups", read.len());
    }
}

#[tokio::test]
async fn object_reader_honors_row_group_selection() {
    let (store, path, _tmp) = put_fixture("selection").await;

    let (_s, all) = read::read_row_groups_object(store.clone(), &path, &(0..10).collect::<Vec<_>>()).await.unwrap();
    let (_s, one) = read::read_row_groups_object(store.clone(), &path, &[1]).await.unwrap();

    let rows_all: usize = all.iter().map(|b| b.num_rows()).sum();
    let rows_one: usize = one.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows_all, 50_000, "reading all 10 row groups yields every row");
    assert_eq!(rows_one, 5_000, "reading [1] over the object store fetches only row group 1's rows");
}
