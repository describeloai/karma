//! Billion-row read-path benchmark.
//!
//! ```sh
//! # Quick run (default 5M rows):
//! cargo run --release --example bench
//! # The headline run (adjust to taste / disk):
//! cargo run --release --example bench -- --rows 100000000 --row-group-size 1000000 --out bench.md
//! # A literal billion (needs the disk + RAM for the sidecar blooms):
//! cargo run --release --example bench -- --rows 1000000000 --row-group-size 1000000
//! ```
//!
//! Two measurements: an **analytical** table (row groups pruned + bytes fetched, from
//! Parquet metadata — no data materialized, so it scales to a billion) and a
//! **measured latency** table for the heavily-pruned queries (karma vs DataFusion's
//! own Parquet reader).

use std::path::PathBuf;
use std::time::{Duration, Instant};

use datafusion::arrow::array::Int64Array;
use datafusion::prelude::{ParquetReadOptions, SessionContext};

use karma_parquet::bench;
use karma_parquet::ParquetZoneTable;
use std::sync::Arc;

struct Args {
    rows: u64,
    rg: u64,
    out: Option<PathBuf>,
    keep: bool,
}

fn parse_args() -> Args {
    let mut rows = 5_000_000u64;
    let mut rg = 1_000_000u64;
    let mut out = None;
    let mut keep = false;
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        match a.as_str() {
            "--rows" => rows = it.next().and_then(|v| v.parse().ok()).expect("--rows N"),
            "--row-group-size" => rg = it.next().and_then(|v| v.parse().ok()).expect("--row-group-size N"),
            "--out" => out = it.next().map(PathBuf::from),
            "--keep" => keep = true,
            other => panic!("unknown arg: {other}"),
        }
    }
    Args { rows, rg, out, keep }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args();
    let path = std::env::temp_dir().join(format!("karma_bench_{}_{}.parquet", args.rows, args.rg));

    println!("· generating {} rows ({} per row group) → {}", args.rows, args.rg, path.display());
    let t = Instant::now();
    bench::generate_parquet(&path, args.rows, args.rg)?;
    let gen_secs = t.elapsed().as_secs_f64();
    let file_bytes = std::fs::metadata(&path)?.len();
    println!("  wrote {} in {:.1}s", bench::human_bytes(file_bytes), gen_secs);

    println!("· building sidecar (zone map + blooms)…");
    let t = Instant::now();
    let sidecar = bench::build(&path)?;
    let build_secs = t.elapsed().as_secs_f64();
    let n_rg = sidecar.zone_map.zones.len();
    println!("  {n_rg} zones (row groups) indexed in {:.1}s", build_secs);

    // ── analytical: pruning + bytes for the whole spectrum ──
    let rows_ana = bench::run_analytical(&path, &sidecar)?;
    let table = bench::markdown_table(&rows_ana);
    println!("\n{table}");

    // ── measured latency: heavily-pruned karma scan vs DataFusion's native reader ──
    let ctx = SessionContext::new();
    ctx.register_table("karma", Arc::new(ParquetZoneTable::from_sidecar(path.clone(), sidecar)))?;
    ctx.register_parquet("full", path.to_str().unwrap(), ParquetReadOptions::default()).await?;

    let mut lat_md = String::new();
    lat_md.push_str("| Query | karma | DataFusion-native | speedup | rows |\n|---|--:|--:|--:|--:|\n");
    println!("latency (SELECT count(*) … WHERE …):");
    for q in bench::queries(args.rows).into_iter().filter(|q| q.latency) {
        let (c_k, d_k) = count(&ctx, "karma", &q.sql_where).await?;
        let (c_f, d_f) = count(&ctx, "full", &q.sql_where).await?;
        assert_eq!(c_k, c_f, "count mismatch for {} (karma {c_k} vs full {c_f})", q.name);
        let speedup = d_f.as_secs_f64() / d_k.as_secs_f64().max(1e-9);
        println!("  {:<28} karma {:>8.2}ms  native {:>8.2}ms  ({speedup:.1}×, {c_k} rows)", q.name, ms(d_k), ms(d_f));
        lat_md.push_str(&format!(
            "| `{}` | {:.2} ms | {:.2} ms | **{:.1}×** | {} |\n",
            q.name,
            ms(d_k),
            ms(d_f),
            speedup,
            c_k
        ));
    }

    if let Some(out) = &args.out {
        let doc = report_doc(args.rows, args.rg, n_rg, file_bytes, gen_secs, build_secs, &table, &lat_md);
        std::fs::write(out, doc)?;
        println!("\n· wrote report → {}", out.display());
    }
    if !args.keep {
        let _ = std::fs::remove_file(&path);
    } else {
        println!("· kept {}", path.display());
    }
    Ok(())
}

async fn count(ctx: &SessionContext, table: &str, where_: &str) -> Result<(i64, Duration), Box<dyn std::error::Error>> {
    let sql = format!("SELECT count(*) AS c FROM {table} WHERE {where_}");
    let t = Instant::now();
    let batches = ctx.sql(&sql).await?.collect().await?;
    let elapsed = t.elapsed();
    let c = batches
        .first()
        .and_then(|b| b.column(0).as_any().downcast_ref::<Int64Array>())
        .map(|a| a.value(0))
        .unwrap_or(0);
    Ok((c, elapsed))
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

#[allow(clippy::too_many_arguments)]
fn report_doc(
    rows: u64,
    rg: u64,
    n_rg: usize,
    file_bytes: u64,
    gen_secs: f64,
    build_secs: f64,
    table: &str,
    lat_md: &str,
) -> String {
    format!(
        "# karma-parquet — read-path benchmark\n\n\
         Synthetic dataset: **{rows} rows**, {rg} rows/row-group → **{n_rg} row groups**, \
         {file} on disk (snappy). Generated in {gen:.1}s; sidecar (zone map + blooms) built \
         in {build:.1}s. Reproduce: `cargo run --release --example bench -- --rows {rows} \
         --row-group-size {rg}`.\n\n\
         ## Pruning + bytes (analytical, scale-invariant)\n\n\
         `RG kept (stats)` = survivors with the zone map alone (≈ Parquet's own min/max). \
         `RG kept (karma)` = with the zone map **and** the bloom. The gap on the scattered \
         high-cardinality rows is the bloom's contribution — pruning min/max cannot do. \
         `Bytes read` sums each surviving row group's Parquet `compressed_size`; `I/O saved` \
         is relative to a full scan.\n\n{table}\n\
         ## Measured latency (heavily-pruned queries)\n\n\
         `SELECT count(*) … WHERE …`, karma (`ParquetZoneTable`) vs DataFusion's own Parquet \
         reader (which prunes by native min/max). Where the value is scattered/unique, native \
         stats can't prune and read the whole file; the karma bloom reads one row group (or \
         none).\n\n{lat_md}\n\
         ## Projection to a billion rows\n\n\
         The pruning ratios above are **scale-invariant** (they depend on selectivity and the \
         row-group count, not the absolute row count), and bytes-read scales linearly with the \
         data. At 1e9 rows / {rg} per group ({thousand_x}× this run) the same queries fetch the \
         same *fraction* of the file — e.g. an absent-value or unique-point lookup still reads \
         ~one row group out of {n_rg_b}, i.e. sub-percent of the bytes. Run it: \
         `--rows 1000000000`.\n",
        rows = rows,
        rg = rg,
        n_rg = n_rg,
        file = bench::human_bytes(file_bytes),
        gen = gen_secs,
        build = build_secs,
        table = table,
        lat_md = lat_md,
        thousand_x = 1_000_000_000 / rows.max(1),
        n_rg_b = 1_000_000_000 / rg.max(1),
    )
}
