//! The Deliverable-A done-criteria, exercised end to end over a real multi-row-group
//! Parquet file:
//!   1. **I/O skip is observable** — reading only the surviving row groups fetches
//!      strictly fewer bytes than a full read (instrumented `ChunkReader`).
//!   2. **Correctness** — a pruned `ParquetZoneTable` scan returns exactly the rows a
//!      full-file scan does, for every query.
//!   3. **Bloom beats stats** — the sidecar bloom prunes a row group whose min/max
//!      (Parquet's own, mirrored by our zone map) cannot be pruned.

use std::fs::File;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::parquet::file::reader::{ChunkReader, Length};
use datafusion::prelude::{col, lit, SessionContext};

use karma_parquet::build::IndexField;
use karma_parquet::read::read_row_groups;
use karma_parquet::ParquetZoneTable;

// Two row groups of 4 rows. id ascends 0..7 (so `id` min/max prune by row group);
// `code` is high-cardinality with OVERLAPPING ranges but DISJOINT membership —
// RG0 = even suffixes ["k-00","k-06"], RG1 = odd ["k-01","k-07"] — so `code = 'k-03'`
// (odd, only in RG1) cannot be pruned by min/max, only by the bloom.
fn write_fixture(tag: &str) -> (PathBuf, Arc<Schema>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("code", DataType::Utf8, false),
    ]));
    let batch = |ids: Vec<i64>, codes: Vec<&str>| {
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(ids)), Arc::new(StringArray::from(codes))]).unwrap()
    };
    let rg0 = batch(vec![0, 1, 2, 3], vec!["k-00", "k-02", "k-04", "k-06"]);
    let rg1 = batch(vec![4, 5, 6, 7], vec!["k-01", "k-03", "k-05", "k-07"]);

    let path = std::env::temp_dir().join(format!("karma_parquet_{tag}.parquet"));
    // `set_max_row_group_size` is deprecated in favour of `_row_count`, but the latter
    // isn't in this parquet build; the deprecated call is exact for our fixed 4-row
    // groups. (Local allow — it's a test-only fixture writer.)
    #[allow(deprecated)]
    let props = WriterProperties::builder().set_max_row_group_size(4).build();
    let mut w = ArrowWriter::try_new(File::create(&path).unwrap(), schema.clone(), Some(props)).unwrap();
    w.write(&rg0).unwrap();
    w.write(&rg1).unwrap();
    w.close().unwrap();
    (path, schema)
}

fn index_fields() -> Vec<IndexField> {
    vec![IndexField::zonemap("id", 1), IndexField::zonemap_and_bloom("code", 2)]
}

// ── 1. I/O skip is observable ──────────────────────────────────────────────────
// A `ChunkReader` that tallies every byte the parquet reader fetches.
struct Counting {
    inner: Bytes,
    read: Arc<AtomicU64>,
}
impl Length for Counting {
    fn len(&self) -> u64 {
        self.inner.len() as u64
    }
}
impl ChunkReader for Counting {
    type T = <Bytes as ChunkReader>::T;
    fn get_read(&self, start: u64) -> datafusion::parquet::errors::Result<Self::T> {
        // A whole-remainder read (footer path); count the remainder so either read
        // strategy makes the pruned total strictly smaller.
        self.read.fetch_add(self.len() - start, Ordering::Relaxed);
        self.inner.get_read(start)
    }
    fn get_bytes(&self, start: u64, length: usize) -> datafusion::parquet::errors::Result<Bytes> {
        self.read.fetch_add(length as u64, Ordering::Relaxed);
        self.inner.get_bytes(start, length)
    }
}

#[test]
fn reading_surviving_row_groups_fetches_fewer_bytes() {
    let (path, _schema) = write_fixture("io_skip");
    let data = std::fs::read(&path).unwrap();

    let count = |rgs: &[usize]| {
        let read = Arc::new(AtomicU64::new(0));
        let r = Counting { inner: Bytes::from(data.clone()), read: read.clone() };
        let (_s, batches) = read_row_groups(r, rgs).unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        (read.load(Ordering::Relaxed), rows)
    };

    let (bytes_full, rows_full) = count(&[0, 1]);
    let (bytes_pruned, rows_pruned) = count(&[1]);

    assert_eq!(rows_full, 8, "full read yields all rows");
    assert_eq!(rows_pruned, 4, "pruned read yields only row group 1's rows");
    assert!(bytes_pruned > 0, "the surviving row group is still read");
    assert!(
        bytes_pruned < bytes_full,
        "pruning row group 0 must fetch fewer bytes: pruned={bytes_pruned} full={bytes_full}"
    );
}

// ── 2. Correctness: pruned scan == full-file scan ──────────────────────────────
// Typed extractor (robust for empty results, unlike pretty-printing an empty batch).
async fn rows(ctx: &SessionContext, sql: &str) -> Vec<(i64, String)> {
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    let mut out = Vec::new();
    for b in &batches {
        let ids = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let codes = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..b.num_rows() {
            out.push((ids.value(i), codes.value(i).to_string()));
        }
    }
    out.sort();
    out
}

#[tokio::test]
async fn pruned_scan_matches_full_file_scan() {
    let (path, _schema) = write_fixture("differential");

    let table = ParquetZoneTable::from_parquet(&path, &index_fields()).unwrap();
    let kctx = SessionContext::new();
    kctx.register_table("t", Arc::new(table)).unwrap();

    // Full baseline: every row group loaded into a MemTable.
    let (schema, all) = read_row_groups(File::open(&path).unwrap(), &[0, 1]).unwrap();
    let mctx = SessionContext::new();
    mctx.register_table("t", Arc::new(MemTable::try_new(schema, vec![all]).unwrap())).unwrap();

    for sql in [
        "SELECT id, code FROM t WHERE id >= 4",
        "SELECT id, code FROM t WHERE id < 4",
        "SELECT id, code FROM t WHERE code = 'k-03'",
        "SELECT id, code FROM t WHERE code = 'k-99'",
        "SELECT id, code FROM t WHERE code IN ('k-01', 'k-06')",
        "SELECT id, code FROM t WHERE id >= 2 AND id <= 5",
        "SELECT id, code FROM t",
    ] {
        assert_eq!(rows(&kctx, sql).await, rows(&mctx, sql).await, "mismatch for: {sql}");
    }
}

// ── 3. Bloom prunes a row group Parquet's own stats cannot ─────────────────────
#[test]
fn sidecar_bloom_prunes_where_stats_cannot() {
    let (path, _schema) = write_fixture("bloom_beats_stats");
    let eq = col("code").eq(lit("k-03")); // odd → only in row group 1

    // Zone map only (stats): RG0 code ∈ ["k-00","k-06"], RG1 ∈ ["k-01","k-07"] — both
    // ranges straddle "k-03", so neither is pruned.
    let stats_only =
        ParquetZoneTable::from_parquet(&path, &[IndexField::zonemap("id", 1), IndexField::zonemap("code", 2)]).unwrap();
    assert_eq!(stats_only.surviving_row_groups(&[eq.clone()]), vec![0, 1], "min/max cannot prune");

    // With the sidecar bloom, row group 0 (which lacks 'k-03') is pruned.
    let with_bloom = ParquetZoneTable::from_parquet(&path, &index_fields()).unwrap();
    assert_eq!(with_bloom.surviving_row_groups(&[eq]), vec![1], "bloom prunes row group 0");

    // And a plain zone-map prune still works on the ascending id column.
    assert_eq!(with_bloom.surviving_row_groups(&[col("id").gt_eq(lit(4i64))]), vec![1]);
    assert_eq!(with_bloom.surviving_row_groups(&[col("id").lt(lit(4i64))]), vec![0]);
}

// ── builder sanity: one zone per row group, correct offsets/counts ─────────────
#[test]
fn sidecar_has_one_zone_per_row_group() {
    let (path, _schema) = write_fixture("builder_sanity");
    let sc = karma_parquet::build_sidecar(&path, &index_fields()).unwrap();
    assert_eq!(sc.zone_map.zones.len(), 2);
    assert_eq!(sc.zone_map.zones[0].zone_id, 0);
    assert_eq!(sc.zone_map.zones[0].row_offset, 0);
    assert_eq!(sc.zone_map.zones[0].row_count, 4);
    assert_eq!(sc.zone_map.zones[1].row_offset, 4);
    // id bounds per row group: [0,3] then [4,7].
    let id0 = sc.zone_map.zones[0].column(1).unwrap();
    assert_eq!((id0.min.clone(), id0.max.clone()), (karma_index::Value::I64(0), karma_index::Value::I64(3)));
    // A bloom entry exists for the code column of each zone.
    assert!(sc.blooms.get(0, 2).is_some() && sc.blooms.get(1, 2).is_some());
    // field_ids maps names → Iceberg field ids.
    assert_eq!(sc.field_ids.get("code"), Some(&2));
}
