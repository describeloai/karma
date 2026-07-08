//! Benchmark harness for the Parquet read-path — the "does the index earn its keep
//! at scale?" answer, off the critical path.
//!
//! It generates a deterministic synthetic Parquet file (scalable to a billion rows,
//! written one row group at a time so RAM stays bounded), builds the sidecar, and
//! measures two things across a **spectrum** of predicates:
//!
//! 1. **Pruning + bytes (analytical, scale-invariant).** For each query: how many row
//!    groups survive with the zone map alone (≈ what Parquet's own min/max stats do)
//!    vs. with the zone map **and** the bloom, and how many *bytes* a scan would fetch
//!    (summed from each surviving row group's Parquet `compressed_size`). No data is
//!    materialized, so this runs at any scale. Bytes-fetched is the transferable claim
//!    — object-storage cost is ∝ bytes read.
//! 2. **Latency (measured, in the CLI example).** Wall-clock of a heavily-pruned karma
//!    scan vs. DataFusion's own Parquet reader.
//!
//! The schema is chosen to exercise the whole spectrum:
//! - `id` (i64) and `ts` (timestamp) are **monotonic/clustered** → the zone map prunes
//!   them well (so does Parquet's native min/max — karma ties here, honestly).
//! - `user_id` (i64) and `event_id` (utf8, unique per row) are **scattered
//!   high-cardinality** → min/max cannot prune a point lookup, the **bloom** can. This
//!   is the differentiator.
//! - `region` (utf8, 8 values) is **low-cardinality everywhere** → nothing prunes it
//!   (the honest negative case).

use std::path::Path;

use datafusion::arrow::array::{Decimal128Array, Int64Array, StringArray, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::Expr;
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::basic::Compression;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::prelude::{col, lit};
use std::fs::File;
use std::sync::Arc;

use crate::build::{build_sidecar, IndexField, Sidecar};
use crate::KarmaParquetError;

// Field IDs (stable across the sidecar).
pub const F_ID: i32 = 1;
pub const F_TS: i32 = 2;
pub const F_USER_ID: i32 = 3;
pub const F_EVENT_ID: i32 = 4;
pub const F_AMOUNT: i32 = 5;
pub const F_REGION: i32 = 6;

const USER_CARD: u64 = 10_000_000; // distinct user_ids — scattered high cardinality
const TS_BASE: i64 = 1_600_000_000_000_000; // 2020-09-13T12:26:40Z in microseconds
const TS_STEP: i64 = 1_000_000; // +1 second per row → monotonic/clustered
const REGIONS: [&str; 8] = ["af", "as", "eu", "na", "sa", "oc", "me", "an"];

/// The benchmark's Arrow schema.
pub fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        Field::new("user_id", DataType::Int64, false),
        Field::new("event_id", DataType::Utf8, false),
        Field::new("amount", DataType::Decimal128(12, 2), false),
        Field::new("region", DataType::Utf8, false),
    ]))
}

/// The columns to index: `user_id`/`event_id` get blooms (scattered high-card); the
/// rest get zone maps only.
pub fn index_fields() -> Vec<IndexField> {
    vec![
        IndexField::zonemap("id", F_ID),
        IndexField::zonemap("ts", F_TS),
        IndexField::zonemap_and_bloom("user_id", F_USER_ID),
        IndexField::zonemap_and_bloom("event_id", F_EVENT_ID),
        IndexField::zonemap("amount", F_AMOUNT),
        IndexField::zonemap("region", F_REGION),
    ]
}

/// SplitMix64 — a deterministic, dependency-free scatter for a row index.
fn mix(i: u64) -> u64 {
    let mut z = i.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Generate the synthetic Parquet file at `path`: `rows` rows in row groups of
/// `row_group_size`, written one group at a time (bounded RAM). Deterministic — the
/// same (rows, row_group_size) always produce byte-identical data.
pub fn generate_parquet(path: &Path, rows: u64, row_group_size: u64) -> Result<(), KarmaParquetError> {
    generate_parquet_range(path, 0, rows, row_group_size)
}

/// Like [`generate_parquet`] but the row indices span `[first_row, first_row + rows)`.
/// Distinct `first_row`s make disjoint id/ts ranges — a stand-in for the several data
/// files of one Iceberg snapshot (so a range predicate can prune whole *files*).
pub fn generate_parquet_range(path: &Path, first_row: u64, rows: u64, row_group_size: u64) -> Result<(), KarmaParquetError> {
    let schema = schema();
    let props = {
        // `set_max_row_group_size` is deprecated for `_row_count`, which isn't in this
        // parquet build; the deprecated call is exact for our fixed group size.
        #[allow(deprecated)]
        WriterProperties::builder()
            .set_max_row_group_size(row_group_size as usize)
            .set_compression(Compression::SNAPPY)
            .build()
    };
    let mut writer = ArrowWriter::try_new(File::create(path)?, schema.clone(), Some(props))?;

    let mut done: u64 = 0;
    while done < rows {
        let n = row_group_size.min(rows - done);
        let base = first_row + done;
        let mut ids = Vec::with_capacity(n as usize);
        let mut ts = Vec::with_capacity(n as usize);
        let mut user_ids = Vec::with_capacity(n as usize);
        let mut event_ids: Vec<String> = Vec::with_capacity(n as usize);
        let mut amounts = Vec::with_capacity(n as usize);
        let mut regions: Vec<&str> = Vec::with_capacity(n as usize);
        for r in base..base + n {
            let h = mix(r);
            ids.push(r as i64); // monotonic → clustered
            ts.push(TS_BASE + (r as i64) * TS_STEP); // monotonic → clustered
            user_ids.push((h % USER_CARD) as i64); // scattered high-card
            // Unique (SplitMix64 is a bijection) AND scattered (hash-ordered, not row-
            // ordered) → its per-row-group min/max span the whole space, so only the
            // bloom can prune a point lookup, never min/max.
            event_ids.push(format!("evt-{h:016x}"));
            amounts.push((h % 10_000_000) as i128); // decimal(12,2): 0.00..99999.99
            regions.push(REGIONS[(h % 8) as usize]); // low-card everywhere
        }
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(TimestampMicrosecondArray::from(ts)),
                Arc::new(Int64Array::from(user_ids)),
                Arc::new(StringArray::from_iter_values(event_ids)),
                Arc::new(Decimal128Array::from(amounts).with_precision_and_scale(12, 2)?),
                Arc::new(StringArray::from_iter_values(regions)),
            ],
        )?;
        writer.write(&batch)?;
        done += n;
    }
    writer.close()?;
    Ok(())
}

/// A benchmark query and the predicate class it exercises.
pub struct Query {
    pub name: &'static str,
    pub class: &'static str,
    pub filters: Vec<Expr>,
    /// SQL predicate (for the measured-latency phase in the CLI).
    pub sql_where: String,
    /// Include in the (memory-safe, heavily-pruned) latency subset.
    pub latency: bool,
}

fn decimal_lit(unscaled: i128) -> Expr {
    Expr::Literal(ScalarValue::Decimal128(Some(unscaled), 12, 2), None)
}

/// The query spectrum, parameterized by the row count so the picked values exist.
pub fn queries(rows: u64) -> Vec<Query> {
    let mid = (rows / 2) as i64;
    let p90_ts = TS_BASE + ((rows as f64 * 0.90) as i64) * TS_STEP;
    let existing_row = rows / 3;
    let existing_user = (mix(existing_row) % USER_CARD) as i64;
    let existing_event = format!("evt-{:016x}", mix(existing_row));
    // An existing scattered value with a non-hex suffix appended: never generated (so
    // the bloom prunes every row group), and lexically adjacent to a real value (so it
    // sits inside the file's min/max — min/max cannot prune it).
    let absent_event = format!("{existing_event}z");

    vec![
        Query {
            name: "id = mid",
            class: "clustered point (zone map)",
            filters: vec![col("id").eq(lit(mid))],
            sql_where: format!("id = {mid}"),
            latency: true,
        },
        Query {
            name: "ts >= p90",
            class: "clustered range (zone map)",
            filters: vec![col("ts").gt_eq(Expr::Literal(ScalarValue::TimestampMicrosecond(Some(p90_ts), None), None))],
            sql_where: format!("ts >= arrow_cast({p90_ts}, 'Timestamp(Microsecond, None)')"),
            latency: false,
        },
        Query {
            name: "user_id = X",
            class: "scattered point — BLOOM vs stats",
            filters: vec![col("user_id").eq(lit(existing_user))],
            sql_where: format!("user_id = {existing_user}"),
            latency: false,
        },
        Query {
            name: "event_id = existing",
            class: "unique point — BLOOM vs stats",
            filters: vec![col("event_id").eq(lit(existing_event.clone()))],
            sql_where: format!("event_id = '{existing_event}'"),
            latency: true,
        },
        Query {
            name: "event_id = ABSENT (in-range)",
            class: "absent value — BLOOM prunes ALL",
            filters: vec![col("event_id").eq(lit(absent_event.clone()))],
            sql_where: format!("event_id = '{absent_event}'"),
            latency: true,
        },
        Query {
            name: "amount > 99000.00",
            class: "scattered numeric (zone map — honest miss)",
            filters: vec![col("amount").gt(decimal_lit(9_900_000))],
            sql_where: "amount > 99000.00".to_string(),
            latency: false,
        },
        Query {
            name: "region = 'eu'",
            class: "low-card everywhere (honest 0%)",
            filters: vec![col("region").eq(lit("eu"))],
            sql_where: "region = 'eu'".to_string(),
            latency: false,
        },
    ]
}

/// One row of the analytical report.
pub struct AnalyticRow {
    pub name: &'static str,
    pub class: &'static str,
    pub rg_total: usize,
    pub rg_zonemap: usize, // survivors with zone map only (≈ Parquet native min/max)
    pub rg_karma: usize,   // survivors with zone map + bloom (karma)
    pub bytes_total: u64,
    pub bytes_karma: u64,
}

impl AnalyticRow {
    pub fn io_saved_pct(&self) -> f64 {
        if self.bytes_total == 0 {
            0.0
        } else {
            100.0 * (1.0 - self.bytes_karma as f64 / self.bytes_total as f64)
        }
    }
    pub fn rg_pruned_pct(&self) -> f64 {
        if self.rg_total == 0 {
            0.0
        } else {
            100.0 * (1.0 - self.rg_karma as f64 / self.rg_total as f64)
        }
    }
}

/// Sum the Parquet `compressed_size` of the given zone indices' row groups (+ the
/// footer), i.e. the bytes a scan of exactly those zones would fetch.
fn bytes_for(meta: &datafusion::parquet::file::metadata::ParquetMetaData, sidecar: &Sidecar, zone_indices: &[usize]) -> u64 {
    let footer = 8; // magic + length; negligible, included for honesty
    zone_indices
        .iter()
        .map(|&i| {
            let rg = sidecar.zone_map.zones[i].zone_id as usize;
            meta.row_group(rg).compressed_size().max(0) as u64
        })
        .sum::<u64>()
        + footer
}

/// Run the analytical (no-materialization) pruning + bytes measurement for every
/// query. Builds the sidecar from `path` if `sidecar` is `None`.
pub fn run_analytical(path: &Path, sidecar: &Sidecar) -> Result<Vec<AnalyticRow>, KarmaParquetError> {
    let meta = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?.metadata().clone();
    let all: Vec<usize> = (0..sidecar.zone_map.zones.len()).collect();
    let bytes_total = bytes_for(&meta, sidecar, &all);

    let mut out = Vec::new();
    for q in queries_for(sidecar, path)? {
        let zm_only =
            karma_datafusion::translate::surviving_zone_indices(&sidecar.zone_map, None, &sidecar.field_ids, &q.filters);
        let karma = karma_datafusion::translate::surviving_zone_indices(
            &sidecar.zone_map,
            Some(&sidecar.blooms),
            &sidecar.field_ids,
            &q.filters,
        );
        out.push(AnalyticRow {
            name: q.name,
            class: q.class,
            rg_total: sidecar.zone_map.zones.len(),
            rg_zonemap: zm_only.len(),
            rg_karma: karma.len(),
            bytes_total,
            bytes_karma: bytes_for(&meta, sidecar, &karma),
        });
    }
    Ok(out)
}

/// The queries, sized to the file's row count (read from the zone map's total rows).
fn queries_for(sidecar: &Sidecar, _path: &Path) -> Result<Vec<Query>, KarmaParquetError> {
    let rows: u64 = sidecar.zone_map.zones.iter().map(|z| z.row_count).sum();
    Ok(queries(rows))
}

/// Build the sidecar for a generated file (convenience for callers).
pub fn build(path: &Path) -> Result<Sidecar, KarmaParquetError> {
    build_sidecar(path, &index_fields())
}

/// Render the analytical rows as a GitHub-flavored markdown table.
pub fn markdown_table(rows: &[AnalyticRow]) -> String {
    let mut s = String::new();
    s.push_str("| Query | Class | RG total | RG kept (stats) | RG kept (karma) | RG pruned | Bytes read | I/O saved |\n");
    s.push_str("|---|---|--:|--:|--:|--:|--:|--:|\n");
    for r in rows {
        s.push_str(&format!(
            "| `{}` | {} | {} | {} | {} | {:.1}% | {} | **{:.1}%** |\n",
            r.name,
            r.class,
            r.rg_total,
            r.rg_zonemap,
            r.rg_karma,
            r.rg_pruned_pct(),
            human_bytes(r.bytes_karma),
            r.io_saved_pct(),
        ));
    }
    s
}

pub fn human_bytes(b: u64) -> String {
    const U: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut v = b as f64;
    let mut i = 0;
    while v >= 1024.0 && i < U.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    format!("{v:.1} {}", U[i])
}
