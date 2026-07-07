//! The `karma-zonemap-v1` blob — the highest-leverage index for scan pruning.
//!
//! A data file is divided into **zones** (contiguous row ranges — typically the
//! Parquet row groups, but the format is agnostic). For each zone and each indexed
//! column the blob stores `{min, max, null_count, value_count}`. An engine planning
//! a scan can then skip any zone whose `[min, max]` cannot satisfy the predicate
//! (see [`crate::prune`]) — reading far less than the whole file.
//!
//! ## Binary layout (v1, all integers little-endian)
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
//!       Value min
//!       Value max
//!   Value := u8 tag  +  payload
//!            0 Null      → (no payload)
//!            1 Bool      → u8 (0|1)
//!            2 I64       → i64
//!            3 F64       → f64 (IEEE-754 bits)
//!            4 Bytes     → u32 len + len bytes   (utf8 strings & opaque both ride here)
//!            5 Decimal   → i128 unscaled (16B LE) + i32 scale (LE)
//!            6 Date      → i32 days since 1970-01-01 (LE)
//!            7 Time      → i64 microseconds since midnight (LE)
//!            8 Timestamp → i64 microseconds since epoch (LE)
//! ```
//! Tags 0-4 collapse the common types to ordered wire forms: integer-like → `I64`,
//! float → `F64`, string/binary → `Bytes`, boolean → `Bool`, all-null bound → `Null`.
//! Tags 5-8 carry the types that *cannot* be collapsed without losing pruning
//! correctness: a decimal compared as `F64` loses precision (wrong pruning → dropped
//! rows), and a temporal needs a defined unit. They are engine-neutral (Iceberg
//! `decimal`/`date`/`time`/`timestamp` single-value forms). The Iceberg column's real
//! type lives in table metadata; the zone map only needs ordered bounds.

pub const ZONEMAP_BLOB_TYPE: &str = "karma-zonemap-v1";
const VERSION: u8 = 1;

#[derive(Debug, thiserror::Error)]
pub enum ZoneMapError {
    #[error("truncated zone-map payload")]
    Truncated,
    #[error("unsupported zone-map version {0} (this build understands {VERSION})")]
    BadVersion(u8),
    #[error("unknown Value tag {0}")]
    BadValueTag(u8),
}

/// An ordered column bound. `Null` means "no non-null values in this zone", so the
/// bound is absent.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    I64(i64),
    F64(f64),
    Bytes(Vec<u8>),
    /// An exact decimal bound. Within a column the scale is fixed, so ordering
    /// reduces to comparing `unscaled` (see [`crate::prune`]); a decimal must NOT
    /// ride as `F64`, which would lose precision and mis-prune.
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
    pub fn encode(&self) -> Vec<u8> {
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
                put_value(&mut out, &c.min);
                put_value(&mut out, &c.max);
            }
        }
        out
    }

    /// Parse a `karma-zonemap-v1` payload.
    pub fn decode(bytes: &[u8]) -> Result<ZoneMap, ZoneMapError> {
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
                let min = c.value()?;
                let max = c.value()?;
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
fn put_value(o: &mut Vec<u8>, v: &Value) {
    match v {
        Value::Null => o.push(0),
        Value::Bool(b) => {
            o.push(1);
            o.push(*b as u8);
        }
        Value::I64(x) => {
            o.push(2);
            o.extend_from_slice(&x.to_le_bytes());
        }
        Value::F64(x) => {
            o.push(3);
            o.extend_from_slice(&x.to_bits().to_le_bytes());
        }
        Value::Bytes(bs) => {
            o.push(4);
            put_u32(o, bs.len() as u32);
            o.extend_from_slice(bs);
        }
        Value::Decimal { unscaled, scale } => {
            o.push(5);
            o.extend_from_slice(&unscaled.to_le_bytes());
            o.extend_from_slice(&scale.to_le_bytes());
        }
        Value::Date(d) => {
            o.push(6);
            o.extend_from_slice(&d.to_le_bytes());
        }
        Value::Time(t) => {
            o.push(7);
            o.extend_from_slice(&t.to_le_bytes());
        }
        Value::Timestamp(t) => {
            o.push(8);
            o.extend_from_slice(&t.to_le_bytes());
        }
    }
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
    fn u8(&mut self) -> Result<u8, ZoneMapError> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<u32, ZoneMapError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn i32(&mut self) -> Result<i32, ZoneMapError> {
        Ok(i32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64, ZoneMapError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn i64(&mut self) -> Result<i64, ZoneMapError> {
        Ok(i64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn i128(&mut self) -> Result<i128, ZoneMapError> {
        Ok(i128::from_le_bytes(self.take(16)?.try_into().unwrap()))
    }
    fn value(&mut self) -> Result<Value, ZoneMapError> {
        let tag = self.u8()?;
        Ok(match tag {
            0 => Value::Null,
            1 => Value::Bool(self.u8()? != 0),
            2 => Value::I64(self.i64()?),
            3 => Value::F64(f64::from_bits(self.u64()?)),
            4 => {
                let len = self.u32()? as usize;
                Value::Bytes(self.take(len)?.to_vec())
            }
            5 => Value::Decimal { unscaled: self.i128()?, scale: self.i32()? },
            6 => Value::Date(self.i32()?),
            7 => Value::Time(self.i64()?),
            8 => Value::Timestamp(self.i64()?),
            other => return Err(ZoneMapError::BadValueTag(other)),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let decoded = ZoneMap::decode(&zm.encode()).unwrap();
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
        assert_eq!(zm, ZoneMap::decode(&zm.encode()).unwrap());
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
        assert_eq!(zm, ZoneMap::decode(&zm.encode()).unwrap());
    }

    #[test]
    fn rejects_bad_version() {
        let mut b = sample().encode();
        b[0] = 99;
        assert!(matches!(ZoneMap::decode(&b), Err(ZoneMapError::BadVersion(99))));
    }
}
