//! The `karma-zonemap-v1` blob — the highest-leverage index for scan pruning.
//!
//! A data file is divided into **zones** (contiguous row ranges — typically the
//! Parquet row groups, but the format is agnostic). For each zone and each indexed
//! column the blob stores `{min, max, null_count, value_count}`. An engine planning
//! a scan can then skip any zone whose `[min, max]` cannot satisfy the predicate
//! (see [`crate::prune`]) — reading far less than the whole file.
//!
//! ## Binary layout (v1, structural integers little-endian)
//! ```text
//!   u8   version            (= 1)
//!   u32  zone_count
//!   repeat zone_count times:
//!     u32  zone_id
//!     u64  row_offset       (first row index of the zone within the data file)
//!     u64  row_count
//!     u32  column_count
//!     repeat column_count times:
//!       i32  field_id       (Iceberg field ID)
//!       u64  null_count
//!       u64  value_count    (non-null values)
//!       u32  min_len ; min bytes   (Iceberg single-value serialization; len 0 = absent)
//!       u32  max_len ; max bytes
//! ```
//! **Bounds use Iceberg's Appendix-D single-value serialization** — byte-for-byte the
//! same encoding as manifest `lower_bounds`/`upper_bounds`. A bound carries no type
//! tag; its type is resolved from the table schema via its field ID (a
//! [`ColumnTypes`] map passed to [`ZoneMap::encode`]/[`ZoneMap::decode`]). This makes
//! the zone map a fine-grained refinement of what Iceberg already does at file
//! granularity, and lets engines reuse their existing bound comparators. See
//! `docs/proposals/0001-puffin-secondary-indexes.md`.

use std::collections::HashMap;

pub const ZONEMAP_BLOB_TYPE: &str = "karma-zonemap-v1";
const VERSION: u8 = 1;

#[derive(Debug, thiserror::Error)]
pub enum ZoneMapError {
    #[error("truncated zone-map payload")]
    Truncated,
    #[error("unsupported zone-map version {0} (this build understands {VERSION})")]
    BadVersion(u8),
    #[error("no type in the schema for field id {0} (needed to decode its bound)")]
    MissingType(i32),
}

/// An Iceberg primitive type — the schema information needed to (de)serialize a bound
/// with Appendix-D single-value serialization. Supplied per field id via a
/// [`ColumnTypes`] map; the zone-map blob itself stores no type tags.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IcebergType {
    Boolean,
    /// 32-bit integer (4-byte little-endian bound).
    Int,
    /// 64-bit integer (8-byte little-endian bound).
    Long,
    /// 32-bit float (4-byte little-endian bound).
    Float,
    /// 64-bit float (8-byte little-endian bound).
    Double,
    /// Days since 1970-01-01 (4-byte little-endian).
    Date,
    /// Microseconds since midnight (8-byte little-endian).
    Time,
    /// Microseconds since epoch (8-byte little-endian).
    Timestamp,
    /// UTF-8 string (raw bytes).
    String,
    /// Opaque binary (raw bytes).
    Binary,
    /// `decimal(_, scale)`; the unscaled value is stored as minimum-width,
    /// two's-complement, **big-endian** bytes (scale comes from the type).
    Decimal { scale: i32 },
}

/// Field id → Iceberg type. The schema an engine already has; used to (de)serialize
/// bounds. Fields with only absent (all-null) bounds need no entry.
pub type ColumnTypes = HashMap<i32, IcebergType>;

/// An ordered column bound. `Null` means "no non-null values in this zone", so the
/// bound is absent (serialized as a zero-length byte string).
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    I64(i64),
    F64(f64),
    Bytes(Vec<u8>),
    /// An exact decimal bound. Within a column the scale is fixed, so ordering
    /// reduces to comparing `unscaled` (see [`crate::prune`]).
    Decimal { unscaled: i128, scale: i32 },
    /// Iceberg `date` — days since 1970-01-01.
    Date(i32),
    /// Iceberg `time` — microseconds since midnight.
    Time(i64),
    /// Iceberg `timestamp`/`timestamptz` — microseconds since the Unix epoch.
    Timestamp(i64),
}

impl Value {
    pub fn str(s: &str) -> Value {
        Value::Bytes(s.as_bytes().to_vec())
    }

    /// The Iceberg type this value serializes as, when the schema does not say
    /// otherwise (integers → `Long`, floats → `Double`, bytes → `String`).
    fn inferred_type(&self) -> IcebergType {
        match self {
            Value::Bool(_) => IcebergType::Boolean,
            Value::I64(_) => IcebergType::Long,
            Value::F64(_) => IcebergType::Double,
            Value::Bytes(_) => IcebergType::String,
            Value::Date(_) => IcebergType::Date,
            Value::Time(_) => IcebergType::Time,
            Value::Timestamp(_) => IcebergType::Timestamp,
            Value::Decimal { scale, .. } => IcebergType::Decimal { scale: *scale },
            Value::Null => IcebergType::Long,
        }
    }
}

/// Per-column stats within one zone.
#[derive(Clone, Debug, PartialEq)]
pub struct ColumnStats {
    pub field_id: i32,
    pub min: Value,
    pub max: Value,
    pub null_count: u64,
    pub value_count: u64,
}

impl ColumnStats {
    /// The Iceberg type to use for this column's bounds, preferring the schema and
    /// falling back to inference from a non-null bound.
    fn resolve_type(&self, types: &ColumnTypes) -> IcebergType {
        if let Some(t) = types.get(&self.field_id) {
            return *t;
        }
        match (&self.min, &self.max) {
            (Value::Null, Value::Null) => IcebergType::Long, // all-null: type unused
            (Value::Null, v) | (v, _) => v.inferred_type(),
        }
    }
}

/// Stats for one zone (a contiguous row range of the data file).
#[derive(Clone, Debug, PartialEq)]
pub struct ZoneStats {
    pub zone_id: u32,
    pub row_offset: u64,
    pub row_count: u64,
    pub columns: Vec<ColumnStats>,
}

impl ZoneStats {
    pub fn column(&self, field_id: i32) -> Option<&ColumnStats> {
        self.columns.iter().find(|c| c.field_id == field_id)
    }
}

/// The whole zone map for one data file.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ZoneMap {
    pub zones: Vec<ZoneStats>,
}

impl ZoneMap {
    pub fn new(zones: Vec<ZoneStats>) -> Self {
        Self { zones }
    }

    /// Serialize to the `karma-zonemap-v1` payload (goes into a Puffin blob body).
    /// `types` gives each field's Iceberg type so bounds are serialized with
    /// single-value serialization; a field absent from `types` is inferred from its
    /// bound (integers → `Long`, floats → `Double`, bytes → `String`).
    pub fn encode(&self, types: &ColumnTypes) -> Vec<u8> {
        let mut out = Vec::new();
        out.push(VERSION);
        put_u32(&mut out, self.zones.len() as u32);
        for z in &self.zones {
            put_u32(&mut out, z.zone_id);
            put_u64(&mut out, z.row_offset);
            put_u64(&mut out, z.row_count);
            put_u32(&mut out, z.columns.len() as u32);
            for c in &z.columns {
                put_i32(&mut out, c.field_id);
                put_u64(&mut out, c.null_count);
                put_u64(&mut out, c.value_count);
                let ty = c.resolve_type(types);
                put_bound(&mut out, &c.min, ty);
                put_bound(&mut out, &c.max, ty);
            }
        }
        out
    }

    /// Parse a `karma-zonemap-v1` payload. `types` must contain every field id that
    /// has a non-null bound (a bound's type is not stored in the blob — it comes from
    /// the schema, exactly as manifest bounds do).
    pub fn decode(bytes: &[u8], types: &ColumnTypes) -> Result<ZoneMap, ZoneMapError> {
        let mut c = Cur { b: bytes, p: 0 };
        let version = c.u8()?;
        if version != VERSION {
            return Err(ZoneMapError::BadVersion(version));
        }
        let zone_count = c.u32()?;
        let mut zones = Vec::with_capacity(zone_count as usize);
        for _ in 0..zone_count {
            let zone_id = c.u32()?;
            let row_offset = c.u64()?;
            let row_count = c.u64()?;
            let col_count = c.u32()?;
            let mut columns = Vec::with_capacity(col_count as usize);
            for _ in 0..col_count {
                let field_id = c.i32()?;
                let null_count = c.u64()?;
                let value_count = c.u64()?;
                let ty = types.get(&field_id).copied();
                let min = c.bound(ty, field_id)?;
                let max = c.bound(ty, field_id)?;
                columns.push(ColumnStats { field_id, min, max, null_count, value_count });
            }
            zones.push(ZoneStats { zone_id, row_offset, row_count, columns });
        }
        Ok(ZoneMap { zones })
    }
}

// ── little-endian writers ────────────────────────────────────────────────────
fn put_u32(o: &mut Vec<u8>, v: u32) {
    o.extend_from_slice(&v.to_le_bytes());
}
fn put_i32(o: &mut Vec<u8>, v: i32) {
    o.extend_from_slice(&v.to_le_bytes());
}
fn put_u64(o: &mut Vec<u8>, v: u64) {
    o.extend_from_slice(&v.to_le_bytes());
}

/// Write one bound as `u32 length + single-value bytes` (length 0 = absent/`Null`).
fn put_bound(o: &mut Vec<u8>, v: &Value, ty: IcebergType) {
    let bytes = single_value_bytes(v, ty);
    put_u32(o, bytes.len() as u32);
    o.extend_from_slice(&bytes);
}

/// Iceberg Appendix-D single-value serialization of `v` interpreted as `ty`. `Null`
/// serializes to an empty byte string.
pub(crate) fn single_value_bytes(v: &Value, ty: IcebergType) -> Vec<u8> {
    match (ty, v) {
        (_, Value::Null) => Vec::new(),
        (IcebergType::Boolean, Value::Bool(b)) => vec![*b as u8],
        (IcebergType::Int, Value::I64(x)) => (*x as i32).to_le_bytes().to_vec(),
        (IcebergType::Long, Value::I64(x)) => x.to_le_bytes().to_vec(),
        (IcebergType::Float, Value::F64(x)) => (*x as f32).to_le_bytes().to_vec(),
        (IcebergType::Double, Value::F64(x)) => x.to_le_bytes().to_vec(),
        (IcebergType::Date, Value::Date(d)) => d.to_le_bytes().to_vec(),
        (IcebergType::Time, Value::Time(t)) => t.to_le_bytes().to_vec(),
        (IcebergType::Timestamp, Value::Timestamp(t)) => t.to_le_bytes().to_vec(),
        (IcebergType::String, Value::Bytes(b)) | (IcebergType::Binary, Value::Bytes(b)) => b.clone(),
        (IcebergType::Decimal { .. }, Value::Decimal { unscaled, .. }) => decimal_min_be(*unscaled),
        // Type/value disagreement (shouldn't happen with a consistent schema): fall
        // back to the value's own inferred type so the bytes are still well-formed.
        (_, other) => single_value_bytes(other, other.inferred_type()),
    }
}

/// A decimal's unscaled value as minimum-width, two's-complement, big-endian bytes
/// (Java `BigInteger.toByteArray` / Iceberg decimal single-value serialization).
pub(crate) fn decimal_min_be(unscaled: i128) -> Vec<u8> {
    let full = unscaled.to_be_bytes(); // 16 bytes, two's-complement, big-endian
    let mut start = 0;
    // Strip redundant sign bytes: a leading 0x00 whose successor's high bit is clear,
    // or a leading 0xFF whose successor's high bit is set, is redundant.
    while start < full.len() - 1 {
        let redundant = (full[start] == 0x00 && full[start + 1] & 0x80 == 0)
            || (full[start] == 0xFF && full[start + 1] & 0x80 != 0);
        if redundant {
            start += 1;
        } else {
            break;
        }
    }
    full[start..].to_vec()
}

/// Inverse of [`decimal_min_be`]: sign-extend minimal big-endian bytes to `i128`.
fn decimal_from_min_be(bytes: &[u8]) -> i128 {
    let negative = bytes.first().is_some_and(|b| b & 0x80 != 0);
    let mut buf = if negative { [0xFFu8; 16] } else { [0u8; 16] };
    let start = 16 - bytes.len();
    buf[start..].copy_from_slice(bytes);
    i128::from_be_bytes(buf)
}

// ── cursor / little-endian readers ───────────────────────────────────────────
struct Cur<'a> {
    b: &'a [u8],
    p: usize,
}
impl<'a> Cur<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], ZoneMapError> {
        let end = self.p.checked_add(n).ok_or(ZoneMapError::Truncated)?;
        let s = self.b.get(self.p..end).ok_or(ZoneMapError::Truncated)?;
        self.p = end;
        Ok(s)
    }
    fn arr<const N: usize>(&mut self) -> Result<[u8; N], ZoneMapError> {
        self.take(N)?.try_into().map_err(|_| ZoneMapError::Truncated)
    }
    fn u8(&mut self) -> Result<u8, ZoneMapError> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<u32, ZoneMapError> {
        Ok(u32::from_le_bytes(self.arr()?))
    }
    fn i32(&mut self) -> Result<i32, ZoneMapError> {
        Ok(i32::from_le_bytes(self.arr()?))
    }
    fn u64(&mut self) -> Result<u64, ZoneMapError> {
        Ok(u64::from_le_bytes(self.arr()?))
    }

    /// Read one length-prefixed bound. A zero length is `Null`; otherwise the bytes
    /// are single-value serialization interpreted with `ty` (required when present).
    fn bound(&mut self, ty: Option<IcebergType>, field_id: i32) -> Result<Value, ZoneMapError> {
        let len = self.u32()? as usize;
        if len == 0 {
            return Ok(Value::Null);
        }
        let ty = ty.ok_or(ZoneMapError::MissingType(field_id))?;
        let bytes = self.take(len)?;
        value_from_single_value(bytes, ty)
    }
}

fn value_from_single_value(bytes: &[u8], ty: IcebergType) -> Result<Value, ZoneMapError> {
    let fixed = |n: usize| -> Result<&[u8], ZoneMapError> {
        if bytes.len() == n {
            Ok(bytes)
        } else {
            Err(ZoneMapError::Truncated)
        }
    };
    Ok(match ty {
        IcebergType::Boolean => Value::Bool(fixed(1)?[0] != 0),
        IcebergType::Int => Value::I64(i32::from_le_bytes(fixed(4)?.try_into().unwrap()) as i64),
        IcebergType::Long => Value::I64(i64::from_le_bytes(fixed(8)?.try_into().unwrap())),
        IcebergType::Float => Value::F64(f32::from_le_bytes(fixed(4)?.try_into().unwrap()) as f64),
        IcebergType::Double => Value::F64(f64::from_le_bytes(fixed(8)?.try_into().unwrap())),
        IcebergType::Date => Value::Date(i32::from_le_bytes(fixed(4)?.try_into().unwrap())),
        IcebergType::Time => Value::Time(i64::from_le_bytes(fixed(8)?.try_into().unwrap())),
        IcebergType::Timestamp => Value::Timestamp(i64::from_le_bytes(fixed(8)?.try_into().unwrap())),
        IcebergType::String | IcebergType::Binary => Value::Bytes(bytes.to_vec()),
        IcebergType::Decimal { scale } => Value::Decimal { unscaled: decimal_from_min_be(bytes), scale },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn types() -> ColumnTypes {
        HashMap::from([
            (1, IcebergType::Int),
            (2, IcebergType::String),
            (5, IcebergType::Double),
            (6, IcebergType::Long),
            (7, IcebergType::Boolean),
            (10, IcebergType::Decimal { scale: 2 }),
            (11, IcebergType::Decimal { scale: 9 }),
            (12, IcebergType::Date),
            (13, IcebergType::Time),
            (14, IcebergType::Timestamp),
        ])
    }

    fn sample() -> ZoneMap {
        ZoneMap::new(vec![
            ZoneStats {
                zone_id: 0,
                row_offset: 0,
                row_count: 100,
                columns: vec![
                    ColumnStats { field_id: 1, min: Value::I64(0), max: Value::I64(10), null_count: 3, value_count: 97 },
                    ColumnStats { field_id: 2, min: Value::str("alpha"), max: Value::str("mid"), null_count: 0, value_count: 100 },
                ],
            },
            ZoneStats {
                zone_id: 1,
                row_offset: 100,
                row_count: 50,
                columns: vec![ColumnStats {
                    field_id: 1,
                    min: Value::I64(20),
                    max: Value::I64(30),
                    null_count: 0,
                    value_count: 50,
                }],
            },
        ])
    }

    #[test]
    fn encode_decode_roundtrip() {
        let zm = sample();
        let decoded = ZoneMap::decode(&zm.encode(&types()), &types()).unwrap();
        assert_eq!(zm, decoded);
    }

    #[test]
    fn floats_and_nulls_roundtrip() {
        let zm = ZoneMap::new(vec![ZoneStats {
            zone_id: 9,
            row_offset: 0,
            row_count: 1,
            columns: vec![
                ColumnStats { field_id: 5, min: Value::F64(-1.5), max: Value::F64(3.25), null_count: 0, value_count: 1 },
                ColumnStats { field_id: 6, min: Value::Null, max: Value::Null, null_count: 1, value_count: 0 },
            ],
        }]);
        assert_eq!(zm, ZoneMap::decode(&zm.encode(&types()), &types()).unwrap());
    }

    #[test]
    fn decimal_and_temporal_roundtrip() {
        let zm = ZoneMap::new(vec![ZoneStats {
            zone_id: 3,
            row_offset: 0,
            row_count: 4,
            columns: vec![
                // Decimal(10,2): 100.50 .. 9999.99 → unscaled 10050 .. 999999, scale 2.
                ColumnStats { field_id: 10, min: Value::Decimal { unscaled: 10050, scale: 2 }, max: Value::Decimal { unscaled: 999_999, scale: 2 }, null_count: 0, value_count: 4 },
                // A negative / large-magnitude decimal exercises the i128 sign path.
                ColumnStats { field_id: 11, min: Value::Decimal { unscaled: -170_141_183_460_469_231_731i128, scale: 9 }, max: Value::Decimal { unscaled: i128::MAX, scale: 9 }, null_count: 0, value_count: 4 },
                ColumnStats { field_id: 12, min: Value::Date(-1), max: Value::Date(19_723), null_count: 0, value_count: 4 },
                ColumnStats { field_id: 13, min: Value::Time(0), max: Value::Time(86_399_999_999), null_count: 0, value_count: 4 },
                ColumnStats { field_id: 14, min: Value::Timestamp(i64::MIN), max: Value::Timestamp(1_704_067_200_000_000), null_count: 0, value_count: 4 },
            ],
        }]);
        assert_eq!(zm, ZoneMap::decode(&zm.encode(&types()), &types()).unwrap());
    }

    #[test]
    fn decimal_min_be_matches_biginteger() {
        // Canonical minimal two's-complement big-endian (Java BigInteger.toByteArray).
        assert_eq!(decimal_min_be(0), vec![0x00]);
        assert_eq!(decimal_min_be(1), vec![0x01]);
        assert_eq!(decimal_min_be(127), vec![0x7f]);
        assert_eq!(decimal_min_be(128), vec![0x00, 0x80]);
        assert_eq!(decimal_min_be(-1), vec![0xff]);
        assert_eq!(decimal_min_be(-128), vec![0x80]);
        assert_eq!(decimal_min_be(-129), vec![0xff, 0x7f]);
        assert_eq!(decimal_min_be(10050), vec![0x27, 0x42]);
        assert_eq!(decimal_min_be(-10050), vec![0xd8, 0xbe]);
        // Round-trip across the range, including the extremes.
        for v in [0i128, 1, -1, 255, -255, 256, i128::MIN, i128::MAX, -170_141_183_460_469_231_731] {
            assert_eq!(decimal_from_min_be(&decimal_min_be(v)), v, "roundtrip {v}");
        }
    }

    #[test]
    fn rejects_bad_version() {
        let mut b = sample().encode(&types());
        b[0] = 99;
        assert!(matches!(ZoneMap::decode(&b, &types()), Err(ZoneMapError::BadVersion(99))));
    }

    #[test]
    fn decode_without_type_errors_on_present_bound() {
        let zm = sample();
        let empty = ColumnTypes::new();
        assert!(matches!(ZoneMap::decode(&zm.encode(&types()), &empty), Err(ZoneMapError::MissingType(_))));
    }
}
