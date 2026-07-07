//! Keeps the benchmark harness honest and green at tiny scale: it must generate,
//! index, prune across the spectrum as designed, and its counts must match a full
//! scan. (The headline numbers come from `cargo run --release --example bench`.)

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use datafusion::arrow::array::Int64Array;
use datafusion::prelude::{ParquetReadOptions, SessionContext};

use karma_parquet::bench;
use karma_parquet::ParquetZoneTable;

const ROWS: u64 = 50_000;
const RG: u64 = 5_000; // → 10 row groups

fn gen(tag: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("karma_bench_smoke_{tag}.parquet"));
    bench::generate_parquet(&path, ROWS, RG).unwrap();
    path
}

#[test]
fn spectrum_prunes_as_designed() {
    let path = gen("spectrum");
    let sidecar = bench::build(&path).unwrap();
    assert_eq!(sidecar.zone_map.zones.len(), 10, "one zone per row group");

    let rows = bench::run_analytical(&path, &sidecar).unwrap();
    let by: HashMap<&str, &bench::AnalyticRow> = rows.iter().map(|r| (r.name, r)).collect();
    let total = 10;

    // Clustered point → the zone map alone nails it (1 row group).
    let id = by["id = mid"];
    assert_eq!(id.rg_karma, 1, "clustered id point keeps exactly 1 row group");

    // Scattered high-card point: min/max keep ~everything, the BLOOM prunes.
    for name in ["user_id = X", "event_id = existing"] {
        let r = by[name];
        assert!(r.rg_karma >= 1, "{name}: value present → keep its row group");
        assert!(r.rg_karma < r.rg_zonemap.max(1), "{name}: bloom must prune below stats ({} !< {})", r.rg_karma, r.rg_zonemap);
        assert_eq!(r.rg_zonemap, total, "{name}: scattered → min/max cannot prune");
    }

    // Absent value → the bloom prunes EVERY row group (no false negatives).
    let absent = by["event_id = ABSENT (in-range)"];
    assert_eq!(absent.rg_karma, 0, "absent value → all row groups pruned");
    assert!(absent.io_saved_pct() > 99.9, "absent → ~100% I/O saved");

    // Low-cardinality-everywhere → nothing prunes it (the honest negative).
    let region = by["region = 'eu'"];
    assert_eq!(region.rg_karma, total, "'eu' is in every row group → 0 pruned");
}

#[tokio::test]
async fn counts_match_full_scan() {
    let path = gen("counts");
    let sidecar = bench::build(&path).unwrap();

    let ctx = SessionContext::new();
    ctx.register_table("karma", Arc::new(ParquetZoneTable::from_sidecar(path.clone(), sidecar))).unwrap();
    ctx.register_parquet("full", path.to_str().unwrap(), ParquetReadOptions::default()).await.unwrap();

    async fn count(ctx: &SessionContext, table: &str, where_: &str) -> i64 {
        let sql = format!("SELECT count(*) AS c FROM {table} WHERE {where_}");
        let b = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
        b[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0)
    }

    // The pruned karma scan must return exactly what a full scan does.
    for q in bench::queries(ROWS).into_iter().filter(|q| q.latency) {
        let k = count(&ctx, "karma", &q.sql_where).await;
        let f = count(&ctx, "full", &q.sql_where).await;
        assert_eq!(k, f, "count mismatch for `{}`: karma {k} vs full {f}", q.name);
    }
}
