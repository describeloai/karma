//! Build a karma-index **Puffin sidecar** (zone map + optional blooms) from a
//! Parquet file — one zone per row group.
//!
//! A "zone" is a Parquet **row group** (`zone_id` = row-group ordinal). For each
//! indexed field we read that row group's column values once and derive, in a single
//! pass: `{min, max, null_count, value_count}` for the zone map (min/max ordered with
//! [`karma_index::compare`], the *same* ordering the pruner uses) and — where asked —
//! a bloom over the non-null values (for high-cardinality equality the min/max can't
//! prune). The result is the exact input a [`crate::ParquetZoneTable`] needs.
//!
//! We derive bounds from the *values*, not from Parquet's own column statistics: it
//! keeps this builder self-contained and lets the bloom and the min/max come from one
//! read. (Consuming Parquet's page/column stats directly is a later optimization.)

use std::collections::HashMap;
use std::path::Path;

use std::fs::File;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array, Int32Array,
    Int64Array, LargeStringArray, StringArray, TimestampMicrosecondArray,
};
use datafusion::arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use datafusion::parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};
use futures::TryStreamExt;
use object_store::{path::Path as ObjPath, ObjectStore};

use karma_index::{compare, Bloom, BloomEntry, ColumnStats, Value, ZoneBlooms, ZoneMap, ZoneStats, DEFAULT_BITS_PER_VALUE};

use crate::KarmaParquetError;

/// One column to index in the sidecar.
#[derive(Clone, Debug)]
pub struct IndexField {
    /// Column name as it appears in the Parquet/Arrow schema.
    pub name: String,
    /// The Iceberg field ID the zone map / blooms will key on.
    pub field_id: i32,
    /// Also build a bloom over this column (for high-cardinality equality pruning).
    pub bloom: bool,
}

impl IndexField {
    pub fn zonemap(name: impl Into<String>, field_id: i32) -> Self {
        Self { name: name.into(), field_id, bloom: false }
    }
    pub fn zonemap_and_bloom(name: impl Into<String>, field_id: i32) -> Self {
        Self { name: name.into(), field_id, bloom: true }
    }
}

/// A built sidecar: everything a [`crate::ParquetZoneTable`] needs about a file.
#[derive(Clone, Debug)]
pub struct Sidecar {
    pub schema: SchemaRef,
    pub zone_map: ZoneMap,
    pub blooms: ZoneBlooms,
    pub field_ids: HashMap<String, i32>,
}

/// Build a [`Sidecar`] from the Parquet file at `path`, indexing `fields`. Blooms are
/// sized at [`DEFAULT_BITS_PER_VALUE`]; use [`build_sidecar_sized`] to override.
pub fn build_sidecar(path: &Path, fields: &[IndexField]) -> Result<Sidecar, KarmaParquetError> {
    build_sidecar_sized(path, fields, DEFAULT_BITS_PER_VALUE)
}

/// Like [`build_sidecar`] but with an explicit bloom bits-per-value.
pub fn build_sidecar_sized(
    path: &Path,
    fields: &[IndexField],
    bits_per_value: usize,
) -> Result<Sidecar, KarmaParquetError> {
    // Read the footer for the schema + per-row-group metadata. We reopen the file
    // once per row group below rather than slurping the whole file into memory, so a
    // billion-row file's sidecar builds in bounded RAM (one row group at a time).
    let head = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
    let schema = head.schema().clone();
    let meta = head.metadata().clone();
    let n_row_groups = meta.num_row_groups();
    drop(head);

    let resolved = resolve_fields(&schema, fields)?;
    let mut zones = Vec::with_capacity(n_row_groups);
    let mut bloom_entries = Vec::new();
    let mut row_offset: u64 = 0;

    for rg in 0..n_row_groups {
        let row_count = meta.row_group(rg).num_rows() as u64;
        // Read just this row group; a row group may arrive as several batches.
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?.with_row_groups(vec![rg]).build()?;
        let mut accs: Vec<ColAccum> = resolved.iter().map(|_| ColAccum::default()).collect();
        for batch in reader {
            fold_batch(&batch?, &resolved, &mut accs)?;
        }
        zones.push(zone_from_accs(&resolved, accs, rg, row_offset, row_count, bits_per_value, &mut bloom_entries));
        row_offset += row_count;
    }
    Ok(assemble_sidecar(schema, fields, zones, bloom_entries))
}

/// Build a [`Sidecar`] from a Parquet file **in an object store** (R2/S3). The
/// object-store counterpart of [`build_sidecar`]: reads one row group at a time over
/// the network (bounded RAM), fetching only each row group's bytes.
pub async fn build_sidecar_object(
    store: Arc<dyn ObjectStore>,
    path: &ObjPath,
    fields: &[IndexField],
) -> Result<Sidecar, KarmaParquetError> {
    build_sidecar_object_sized(store, path, fields, DEFAULT_BITS_PER_VALUE).await
}

/// Like [`build_sidecar_object`] but with an explicit bloom bits-per-value.
pub async fn build_sidecar_object_sized(
    store: Arc<dyn ObjectStore>,
    path: &ObjPath,
    fields: &[IndexField],
    bits_per_value: usize,
) -> Result<Sidecar, KarmaParquetError> {
    let head = ParquetRecordBatchStreamBuilder::new(ParquetObjectReader::new(store.clone(), path.clone())).await?;
    let schema = head.schema().clone();
    let meta = head.metadata().clone();
    let n_row_groups = meta.num_row_groups();
    drop(head);

    let resolved = resolve_fields(&schema, fields)?;
    let mut zones = Vec::with_capacity(n_row_groups);
    let mut bloom_entries = Vec::new();
    let mut row_offset: u64 = 0;

    for rg in 0..n_row_groups {
        let row_count = meta.row_group(rg).num_rows() as u64;
        let stream = ParquetRecordBatchStreamBuilder::new(ParquetObjectReader::new(store.clone(), path.clone()))
            .await?
            .with_row_groups(vec![rg])
            .build()?;
        let batches = stream.try_collect::<Vec<_>>().await?;
        let mut accs: Vec<ColAccum> = resolved.iter().map(|_| ColAccum::default()).collect();
        for batch in &batches {
            fold_batch(batch, &resolved, &mut accs)?;
        }
        zones.push(zone_from_accs(&resolved, accs, rg, row_offset, row_count, bits_per_value, &mut bloom_entries));
        row_offset += row_count;
    }
    Ok(assemble_sidecar(schema, fields, zones, bloom_entries))
}

type Resolved<'a> = Vec<(usize, DataType, &'a IndexField)>;

/// Resolve each indexed field to its `(column index, data type)` in the schema.
fn resolve_fields<'a>(schema: &SchemaRef, fields: &'a [IndexField]) -> Result<Resolved<'a>, KarmaParquetError> {
    let mut resolved = Vec::with_capacity(fields.len());
    for f in fields {
        let idx = schema.index_of(&f.name).map_err(|_| KarmaParquetError::MissingColumn(f.name.clone()))?;
        resolved.push((idx, schema.field(idx).data_type().clone(), f));
    }
    Ok(resolved)
}

/// Fold one record batch into the per-field accumulators.
fn fold_batch(batch: &RecordBatch, resolved: &Resolved, accs: &mut [ColAccum]) -> Result<(), KarmaParquetError> {
    for (slot, (col_idx, dtype, field)) in accs.iter_mut().zip(resolved.iter()) {
        fold_column(dtype, batch.column(*col_idx), field.bloom, slot)?;
    }
    Ok(())
}

/// Turn a row group's finished accumulators into a [`ZoneStats`] (+ push blooms).
fn zone_from_accs(
    resolved: &Resolved,
    accs: Vec<ColAccum>,
    rg: usize,
    row_offset: u64,
    row_count: u64,
    bits_per_value: usize,
    bloom_entries: &mut Vec<BloomEntry>,
) -> ZoneStats {
    let mut columns = Vec::with_capacity(resolved.len());
    for (acc, (_idx, _dtype, field)) in accs.into_iter().zip(resolved.iter()) {
        columns.push(ColumnStats {
            field_id: field.field_id,
            min: acc.min.unwrap_or(Value::Null),
            max: acc.max.unwrap_or(Value::Null),
            null_count: acc.null_count,
            value_count: acc.value_count,
        });
        if field.bloom {
            bloom_entries.push(BloomEntry { zone_id: rg as u32, field_id: field.field_id, bloom: Bloom::build(&acc.bloom_values, bits_per_value) });
        }
    }
    ZoneStats { zone_id: rg as u32, row_offset, row_count, columns }
}

fn assemble_sidecar(schema: SchemaRef, fields: &[IndexField], zones: Vec<ZoneStats>, blooms: Vec<BloomEntry>) -> Sidecar {
    let field_ids = fields.iter().map(|f| (f.name.clone(), f.field_id)).collect();
    Sidecar { schema, zone_map: ZoneMap::new(zones), blooms: ZoneBlooms::new(blooms), field_ids }
}

/// Running per-column accumulator over a row group's batches.
#[derive(Default)]
struct ColAccum {
    min: Option<Value>,
    max: Option<Value>,
    null_count: u64,
    value_count: u64,
    bloom_values: Vec<Value>,
}

/// Fold one Arrow column array into `acc`: bump null/value counts, widen the min/max
/// bounds (via [`compare`], so incomparable values — e.g. `NaN` — never move a bound),
/// and collect non-null values for the bloom when requested.
fn fold_column(dtype: &DataType, arr: &ArrayRef, want_bloom: bool, acc: &mut ColAccum) -> Result<(), KarmaParquetError> {
    let values = array_values(dtype, arr)?;
    for ov in values {
        match ov {
            None => acc.null_count += 1,
            Some(v) => {
                acc.value_count += 1;
                match &acc.min {
                    Some(cur) if compare(&v, cur) != Some(std::cmp::Ordering::Less) => {}
                    _ => acc.min = Some(v.clone()),
                }
                match &acc.max {
                    Some(cur) if compare(&v, cur) != Some(std::cmp::Ordering::Greater) => {}
                    _ => acc.max = Some(v.clone()),
                }
                if want_bloom {
                    acc.bloom_values.push(v);
                }
            }
        }
    }
    Ok(())
}

/// Convert an Arrow column to karma-index [`Value`]s (one per row; `None` = null).
/// The supported set mirrors the zone-map wire types (incl. exact decimal/temporal).
fn array_values(dtype: &DataType, arr: &ArrayRef) -> Result<Vec<Option<Value>>, KarmaParquetError> {
    macro_rules! map_arr {
        ($ty:ty, $f:expr) => {{
            let a = arr.as_any().downcast_ref::<$ty>().ok_or_else(|| downcast_err(dtype))?;
            (0..a.len()).map(|i| if a.is_null(i) { None } else { Some($f(a.value(i))) }).collect()
        }};
    }
    Ok(match dtype {
        DataType::Int64 => map_arr!(Int64Array, |v: i64| Value::I64(v)),
        DataType::Int32 => map_arr!(Int32Array, |v: i32| Value::I64(v as i64)),
        DataType::Float64 => map_arr!(Float64Array, |v: f64| Value::F64(v)),
        DataType::Float32 => map_arr!(Float32Array, |v: f32| Value::F64(v as f64)),
        DataType::Boolean => map_arr!(BooleanArray, |v: bool| Value::Bool(v)),
        DataType::Utf8 => map_arr!(StringArray, |v: &str| Value::str(v)),
        DataType::LargeUtf8 => map_arr!(LargeStringArray, |v: &str| Value::str(v)),
        DataType::Decimal128(_p, scale) => {
            let s = *scale as i32;
            map_arr!(Decimal128Array, move |v: i128| Value::Decimal { unscaled: v, scale: s })
        }
        DataType::Date32 => map_arr!(Date32Array, |v: i32| Value::Date(v)),
        DataType::Timestamp(TimeUnit::Microsecond, _tz) => {
            map_arr!(TimestampMicrosecondArray, |v: i64| Value::Timestamp(v))
        }
        other => return Err(KarmaParquetError::UnsupportedType(format!("{other:?}"))),
    })
}

fn downcast_err(dtype: &DataType) -> KarmaParquetError {
    KarmaParquetError::UnsupportedType(format!("array did not downcast for {dtype:?}"))
}
