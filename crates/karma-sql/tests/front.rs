//! The SQL front (Build 4) end-to-end over an in-memory catalog — no live Lakekeeper,
//! no live R2. An in-memory `ObjectStore` holds an Iceberg snapshot's data files; a
//! test [`TableResolver`] hands the front an **observed** `SnapshotTable` so we can
//! assert, through the real DataFusion + Substrait path, that:
//!   1. **FQN addressing** — bare `events` and `main.default.events` resolve to the same
//!      table (Block 2 §1),
//!   2. **explicit columns + types** come back from the plan schema,
//!   3. the **Substrait contract** is emitted,
//!   4. **Karma pruning fires** end-to-end (a range predicate reads only the file that
//!      can match; the others are skipped whole),
//!   5. **auto-LIMIT** caps an unbounded query,
//!   6. an **unknown / unowned table** is a clean error (tenancy),
//!   7. our **function registry** (`get_json_object`, `approx_count_distinct`) binds.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::arrow::array::{Array, Int64Array, StringArray, UInt64Array};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::TableProvider;
use datafusion::error::DataFusionError;
use object_store::memory::InMemory;
use object_store::{path::Path as ObjPath, ObjectStore, PutOptions, PutPayload};

use karma_parquet::{bench, ParquetZoneTable, SnapshotTable};
use karma_sql::{KarmaSession, TableResolver};

type Observer = Arc<Mutex<Vec<Vec<usize>>>>;

const RG: u64 = 5_000;
const PER_FILE: u64 = 20_000; // 3 files → ids [0,20k), [20k,40k), [40k,60k)

/// Build an observed 3-file `events` snapshot in an in-memory object store. `tag` keeps
/// concurrent tests on disjoint temp paths.
async fn events_snapshot(tag: &str) -> (Arc<dyn TableProvider>, Vec<Observer>) {
    let dir = std::env::temp_dir().join(format!("karma_front_{tag}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let mem = InMemory::new();
    let mut paths = Vec::new();
    for i in 0..3u64 {
        let fp = dir.join(format!("part-{i}.parquet"));
        bench::generate_parquet_range(&fp, i * PER_FILE, PER_FILE, RG).unwrap();
        let data = std::fs::read(&fp).unwrap();
        let op = ObjPath::from(format!("warehouse/{tag}-part-{i}.parquet"));
        mem.put_opts(&op, PutPayload::from(bytes::Bytes::from(data)), PutOptions::default())
            .await
            .unwrap();
        paths.push(op);
    }
    let store: Arc<dyn ObjectStore> = Arc::new(mem);

    let mut files = Vec::new();
    let mut observers = Vec::new();
    for op in &paths {
        let o: Observer = Arc::new(Mutex::new(Vec::new()));
        let t = ParquetZoneTable::from_parquet_object(store.clone(), op.clone(), &bench::index_fields())
            .await
            .unwrap()
            .observe_scans(o.clone());
        observers.push(o);
        files.push(t);
    }
    let schema = TableProvider::schema(&files[0]);
    (Arc::new(SnapshotTable::new(schema, files)), observers)
}

/// A resolver that answers `*.events` (any catalog/schema) with a fixed table, and
/// everything else with "not found".
struct EventsResolver(Arc<dyn TableProvider>);

#[async_trait]
impl TableResolver for EventsResolver {
    async fn resolve_table(&self, fqn: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        let leaf = fqn.rsplit('.').next().unwrap_or(fqn);
        Ok((leaf == "events").then(|| self.0.clone()))
    }
}

/// A resolver with no tables (for the table-less function tests).
struct NoTables;

#[async_trait]
impl TableResolver for NoTables {
    async fn resolve_table(&self, _fqn: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        Ok(None)
    }
}

fn ids(batches: &[RecordBatch]) -> Vec<i64> {
    let mut out = Vec::new();
    for b in batches {
        let c = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..c.len() {
            out.push(c.value(i));
        }
    }
    out.sort();
    out
}

fn string0(batches: &[RecordBatch]) -> Option<String> {
    let b = batches.first()?;
    let c = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
    (!c.is_null(0)).then(|| c.value(0).to_string())
}

/// `approx_distinct` returns UInt64 (the HLL cardinality estimate).
fn count0(batches: &[RecordBatch]) -> u64 {
    batches[0].column(0).as_any().downcast_ref::<UInt64Array>().unwrap().value(0)
}

fn last_read(o: &Observer) -> Vec<usize> {
    o.lock().unwrap().last().cloned().unwrap_or_default()
}

#[tokio::test]
async fn front_resolves_fqn_prunes_and_returns_typed_columns() {
    let (snap, observers) = events_snapshot("core").await;
    let session = KarmaSession::new(Arc::new(EventsResolver(snap)));

    // (1) FQN addressing: bare and catalog.schema.table resolve to the SAME table, and
    //     the filtered rows are exact through the front.
    let bare = session.run("SELECT id FROM events WHERE id >= 59900").await.unwrap();
    let fqn = session.run("SELECT id FROM main.default.events WHERE id >= 59900").await.unwrap();
    assert_eq!(ids(&bare.batches), ids(&fqn.batches), "bare == FQN");
    assert_eq!(ids(&bare.batches), (59_900..60_000).collect::<Vec<_>>(), "exact filtered rows");

    // (2) explicit columns + types from the plan schema.
    let typed = session.run("SELECT id, user_id FROM events WHERE id >= 59990").await.unwrap();
    let cols: Vec<(&str, &str)> =
        typed.columns.iter().map(|c| (c.name.as_str(), c.data_type.as_str())).collect();
    assert_eq!(cols, vec![("id", "Int64"), ("user_id", "Int64")]);

    // (3) the Substrait contract is emitted.
    assert!(!typed.substrait.is_empty(), "Substrait contract is a non-empty artifact");

    // (4) Karma pruning fires end-to-end: id>=45000 reads only file2 (ids [40k,60k));
    //     files 0 and 1 are skipped whole.
    for o in &observers {
        o.lock().unwrap().clear();
    }
    let _ = session.run("SELECT id, user_id FROM events WHERE id >= 45000").await.unwrap();
    assert!(last_read(&observers[0]).is_empty(), "file0 fully pruned");
    assert!(last_read(&observers[1]).is_empty(), "file1 fully pruned");
    assert!(!last_read(&observers[2]).is_empty(), "file2 read");

    // (5) auto-LIMIT caps an unbounded query at the default (100).
    let capped = session.run("SELECT id FROM events").await.unwrap();
    assert_eq!(capped.row_count(), 100, "auto-LIMIT caps unbounded query");

    // (6) unknown table → clean error (tenancy: unowned is invisible).
    assert!(session.run("SELECT * FROM nope").await.is_err(), "unknown table errors");
}

#[tokio::test]
async fn front_get_json_object_udf() {
    let session = KarmaSession::new(Arc::new(NoTables));
    let doc = r#"{"sensor":{"id":"A12"},"tags":["x","y"]}"#;
    let r = session
        .run(&format!("SELECT get_json_object('{doc}', '$.sensor.id') AS v"))
        .await
        .unwrap();
    assert_eq!(string0(&r.batches).as_deref(), Some("A12"), "nested key");
    let r2 = session
        .run(&format!("SELECT get_json_object('{doc}', '$.tags[1]') AS v"))
        .await
        .unwrap();
    assert_eq!(string0(&r2.batches).as_deref(), Some("y"), "array index");
}

#[tokio::test]
async fn front_approx_count_distinct_alias() {
    let (snap, _obs) = events_snapshot("approx").await;
    let session = KarmaSession::new(Arc::new(EventsResolver(snap)));
    // id is unique 0..60000 → approx distinct ≈ 60000 (HLL error a few %).
    let r = session.run("SELECT approx_count_distinct(id) AS c FROM events").await.unwrap();
    let c = count0(&r.batches);
    assert!((54_000..=66_000).contains(&c), "approx_count_distinct alias resolves: got {c}");
}
