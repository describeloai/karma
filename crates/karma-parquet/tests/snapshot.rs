//! The multi-file `SnapshotTable` — an Iceberg snapshot's several data files read as
//! one logical table, each pruned by its own Karma sidecar:
//!   1. results are identical to a plain DataFusion scan over the same files (a
//!      directory `ListingTable`), and
//!   2. pruning works **across files** — a range predicate skips whole data files whose
//!      id bounds cannot match (each file's scan reads 0 row groups), and reads only the
//!      surviving row groups of the file that does match.
//! In-memory `ObjectStore` stands in for R2; the resolver (Lakekeeper) only supplies the
//! file list, which we provide directly here.

use std::sync::{Arc, Mutex};

use datafusion::arrow::array::Int64Array;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use object_store::memory::InMemory;
use object_store::{path::Path as ObjPath, ObjectStore, PutOptions, PutPayload};

use karma_parquet::{bench, ParquetZoneTable, SnapshotTable};

const RG: u64 = 5_000;
const PER_FILE: u64 = 20_000; // → 4 row groups per file

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

async fn rows(ctx: &SessionContext, sql: &str) -> Vec<(i64, i64)> {
    extract(&ctx.sql(sql).await.unwrap().collect().await.unwrap())
}

#[tokio::test]
async fn snapshot_table_unions_files_and_prunes_across_them() {
    // Three data files with DISJOINT id ranges — a stand-in for one Iceberg snapshot's
    // data files: file0 = ids [0,20k), file1 = [20k,40k), file2 = [40k,60k).
    let dir = std::env::temp_dir().join("karma_snapshot_parts");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let mem = InMemory::new();
    let mut paths = Vec::new();
    for i in 0..3u64 {
        let fp = dir.join(format!("part-{i}.parquet"));
        bench::generate_parquet_range(&fp, i * PER_FILE, PER_FILE, RG).unwrap();
        let data = std::fs::read(&fp).unwrap();
        let op = ObjPath::from(format!("warehouse/part-{i}.parquet"));
        mem.put_opts(&op, PutPayload::from(bytes::Bytes::from(data)), PutOptions::default()).await.unwrap();
        paths.push(op);
    }
    let store: Arc<dyn ObjectStore> = Arc::new(mem);

    // Build the snapshot table, one observed ParquetZoneTable per data file.
    let mut files = Vec::new();
    let mut obs = Vec::new();
    for op in &paths {
        let o: Arc<Mutex<Vec<Vec<usize>>>> = Arc::new(Mutex::new(Vec::new()));
        let t = ParquetZoneTable::from_parquet_object(store.clone(), op.clone(), &bench::index_fields())
            .await
            .unwrap()
            .observe_scans(o.clone());
        obs.push(o);
        files.push(t);
    }
    let schema = datafusion::catalog::TableProvider::schema(&files[0]);
    let snap = SnapshotTable::new(schema, files);
    assert_eq!(snap.file_count(), 3);

    let kctx = SessionContext::new();
    kctx.register_table("events", Arc::new(snap)).unwrap();

    // Truth: DataFusion's own reader over the same three files (a directory listing).
    let fctx = SessionContext::new();
    fctx.register_parquet("events", dir.to_str().unwrap(), ParquetReadOptions::default()).await.unwrap();

    // Cross-file pruning: a range predicate skips whole files whose id bounds miss.
    let cases: [(&str, [bool; 3]); 2] = [
        ("SELECT id, user_id FROM events WHERE id >= 45000", [false, false, true]), // only file2
        ("SELECT id, user_id FROM events WHERE id < 20000", [true, false, false]),  // only file0
    ];
    for (sql, touched) in cases {
        for o in &obs {
            o.lock().unwrap().clear();
        }
        assert_eq!(rows(&kctx, sql).await, rows(&fctx, sql).await, "union == directory scan for: {sql}");
        for (i, &t) in touched.iter().enumerate() {
            let read = obs[i].lock().unwrap().last().cloned().unwrap_or_default();
            if t {
                assert!(!read.is_empty(), "file {i} must be read for `{sql}`");
            } else {
                assert!(read.is_empty(), "file {i} must be fully pruned for `{sql}`, read {read:?}");
            }
        }
    }

    // Within-file pruning survives too: `id >= 45000` over file2 (ids [40k,60k), 4 row
    // groups of 5k) skips its first row group (40k–45k) → reads 3 of 4.
    for o in &obs {
        o.lock().unwrap().clear();
    }
    let _ = rows(&kctx, "SELECT id, user_id FROM events WHERE id >= 45000").await;
    assert_eq!(obs[2].lock().unwrap().last().unwrap().len(), 3, "file2 skips its first row group");

    // A predicate spanning all three files still matches the directory scan exactly.
    let spanning = "SELECT id, user_id FROM events WHERE id >= 18000 AND id < 42000";
    assert_eq!(rows(&kctx, spanning).await, rows(&fctx, spanning).await, "spanning predicate matches");
    // And a full scan.
    assert_eq!(rows(&kctx, "SELECT id, user_id FROM events").await.len(), 60_000);
}
