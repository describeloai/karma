//! The `karma-bloom-v1` blob — split-block Bloom filters (SBBF) for pruning
//! equality on high-cardinality columns, where a [`crate::zonemap`] can't help.
//!
//! The construction is **Apache Parquet's** split-block Bloom filter, verbatim
//! (see RFC-0002 / the Parquet BloomFilter spec): 256-bit blocks of eight 32-bit
//! words, XXH64 (seed 0) of the value's canonical bytes, block chosen by the hash's
//! upper 32 bits, and a per-word mask from eight odd salts. Reusing Parquet's math
//! means our blob is byte-compatible with an index the Iceberg ecosystem already
//! knows — we just store it as a Puffin sidecar.
//!
//! **No false negatives:** [`Bloom::might_contain`] returning `false` proves the
//! value is absent from the zone (safe to skip); `true` is only "maybe" (read it).

use crate::zonemap::Value;

pub const BLOOM_BLOB_TYPE: &str = "karma-bloom-v1";
const VERSION: u8 = 1;

/// The eight odd salts of the Parquet SBBF mask.
const SALT: [u32; 8] = [
    0x47b6_137b, 0x4497_4d91, 0x8824_ad5b, 0xa2b7_289d, 0x7054_95c7, 0x2df1_424b, 0x9efc_4947, 0x5c6b_fb31,
];

/// Default Bloom sizing: bits per distinct value (≈ 1% false-positive rate for SBBF).
pub const DEFAULT_BITS_PER_VALUE: usize = 16;

#[derive(Debug, thiserror::Error)]
pub enum BloomError {
    #[error("truncated bloom payload")]
    Truncated,
    #[error("unsupported bloom version {0}")]
    BadVersion(u8),
}

/// XXH64 (seed 0) of a value's canonical bytes; `None` for `Null` (never inserted
/// or probed — a value predicate never matches NULL).
///
/// The canonical bytes match the zone-map wire payload for each variant (minus the
/// tag). NOTE: this is Bloom-*internal* — an equality probe only matches when it
/// hashes the value identically, which it does. It is **not yet** Parquet-exact for
/// `Decimal` (Parquet hashes the minimal two's-complement big-endian form); a
/// future RFC pins a Parquet-compatible decimal hashing so our blooms interoperate
/// with Parquet's own. Until then a decimal bloom is self-consistent, not shared.
pub fn value_hash(v: &Value) -> Option<u64> {
    let bytes: Vec<u8> = match v {
        Value::Bytes(b) => b.clone(),                      // Parquet-compatible for string/binary
        Value::I64(x) => x.to_le_bytes().to_vec(),
        Value::F64(x) => x.to_bits().to_le_bytes().to_vec(),
        Value::Bool(b) => vec![*b as u8],
        Value::Decimal { unscaled, scale } => {
            let mut b = unscaled.to_le_bytes().to_vec(); // 16 bytes
            b.extend_from_slice(&scale.to_le_bytes());
            b
        }
        Value::Date(d) => d.to_le_bytes().to_vec(),
        Value::Time(t) | Value::Timestamp(t) => t.to_le_bytes().to_vec(),
        Value::Null => return None,
    };
    Some(twox_hash::XxHash64::oneshot(0, &bytes))
}

/// A split-block Bloom filter: `num_blocks` blocks of eight 32-bit words.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Bloom {
    blocks: Vec<[u32; 8]>,
}

impl Bloom {
    /// An empty filter with a fixed number of blocks (≥ 1).
    pub fn with_blocks(num_blocks: usize) -> Bloom {
        Bloom { blocks: vec![[0u32; 8]; num_blocks.max(1)] }
    }

    /// Build a filter over `values`, sized `bits_per_value` bits per value.
    pub fn build(values: &[Value], bits_per_value: usize) -> Bloom {
        let n = values.iter().filter(|v| !matches!(v, Value::Null)).count();
        let bits = n * bits_per_value;
        let num_blocks = bits.div_ceil(256).max(1);
        let mut b = Bloom::with_blocks(num_blocks);
        for v in values {
            b.insert(v);
        }
        b
    }

    pub fn num_blocks(&self) -> usize {
        self.blocks.len()
    }

    fn block_index(&self, h: u64) -> usize {
        (((h >> 32) * self.blocks.len() as u64) >> 32) as usize
    }

    pub fn insert(&mut self, v: &Value) {
        if let Some(h) = value_hash(v) {
            self.insert_hash(h);
        }
    }

    fn insert_hash(&mut self, h: u64) {
        let idx = self.block_index(h);
        let x = h as u32;
        for i in 0..8 {
            let bit = (x.wrapping_mul(SALT[i])) >> 27; // 0..=31
            self.blocks[idx][i] |= 1 << bit;
        }
    }

    /// `false` ⇒ `v` is DEFINITELY absent; `true` ⇒ maybe present. `Null` probes and
    /// empty filters answer `true` (conservative — never a false "absent").
    pub fn might_contain(&self, v: &Value) -> bool {
        match value_hash(v) {
            Some(h) => self.check_hash(h),
            None => true,
        }
    }

    fn check_hash(&self, h: u64) -> bool {
        let idx = self.block_index(h);
        let x = h as u32;
        for i in 0..8 {
            let bit = (x.wrapping_mul(SALT[i])) >> 27;
            if self.blocks[idx][i] & (1 << bit) == 0 {
                return false;
            }
        }
        true
    }

    fn write_bytes(&self, out: &mut Vec<u8>) {
        for blk in &self.blocks {
            for w in blk {
                out.extend_from_slice(&w.to_le_bytes());
            }
        }
    }

    fn read_bytes(cur: &mut Cur, num_blocks: usize) -> Result<Bloom, BloomError> {
        let mut blocks = Vec::with_capacity(num_blocks);
        for _ in 0..num_blocks {
            let mut blk = [0u32; 8];
            for w in blk.iter_mut() {
                *w = cur.u32()?;
            }
            blocks.push(blk);
        }
        Ok(Bloom { blocks })
    }
}

/// One Bloom per (zone, column) — the `karma-bloom-v1` blob payload.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ZoneBlooms {
    pub entries: Vec<BloomEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BloomEntry {
    pub zone_id: u32,
    pub field_id: i32,
    pub bloom: Bloom,
}

impl ZoneBlooms {
    pub fn new(entries: Vec<BloomEntry>) -> Self {
        Self { entries }
    }

    pub fn get(&self, zone_id: u32, field_id: i32) -> Option<&Bloom> {
        self.entries.iter().find(|e| e.zone_id == zone_id && e.field_id == field_id).map(|e| &e.bloom)
    }

    /// Conservative membership for a (zone, column): a missing Bloom is "maybe"
    /// (can't prove absence) so the zone is NOT skipped on that axis.
    pub fn might_contain(&self, zone_id: u32, field_id: i32, v: &Value) -> bool {
        self.get(zone_id, field_id).map_or(true, |b| b.might_contain(v))
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.push(VERSION);
        out.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());
        for e in &self.entries {
            out.extend_from_slice(&e.zone_id.to_le_bytes());
            out.extend_from_slice(&e.field_id.to_le_bytes());
            out.extend_from_slice(&(e.bloom.num_blocks() as u32).to_le_bytes());
            e.bloom.write_bytes(&mut out);
        }
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<ZoneBlooms, BloomError> {
        let mut c = Cur { b: bytes, p: 0 };
        let version = c.u8()?;
        if version != VERSION {
            return Err(BloomError::BadVersion(version));
        }
        let count = c.u32()?;
        let mut entries = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let zone_id = c.u32()?;
            let field_id = c.i32()?;
            let num_blocks = c.u32()? as usize;
            let bloom = Bloom::read_bytes(&mut c, num_blocks)?;
            entries.push(BloomEntry { zone_id, field_id, bloom });
        }
        Ok(ZoneBlooms { entries })
    }
}

struct Cur<'a> {
    b: &'a [u8],
    p: usize,
}
impl Cur<'_> {
    fn take(&mut self, n: usize) -> Result<&[u8], BloomError> {
        let end = self.p.checked_add(n).ok_or(BloomError::Truncated)?;
        let s = self.b.get(self.p..end).ok_or(BloomError::Truncated)?;
        self.p = end;
        Ok(s)
    }
    fn u8(&mut self) -> Result<u8, BloomError> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<u32, BloomError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn i32(&mut self) -> Result<i32, BloomError> {
        Ok(i32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xxh64_test_vector() {
        // XXH64("", seed 0) — the canonical xxHash vector — pins our hashing.
        assert_eq!(value_hash(&Value::Bytes(vec![])), Some(0xEF46_DB37_51D8_E999));
    }

    #[test]
    fn no_false_negatives() {
        let values: Vec<Value> = (0..500).map(|i| Value::str(&format!("uuid-{i:08}"))).collect();
        let b = Bloom::build(&values, DEFAULT_BITS_PER_VALUE);
        for v in &values {
            assert!(b.might_contain(v), "false negative for {v:?}");
        }
    }

    #[test]
    fn false_positive_rate_is_bounded() {
        let inserted: Vec<Value> = (0..1000).map(|i| Value::str(&format!("k{i}"))).collect();
        let b = Bloom::build(&inserted, DEFAULT_BITS_PER_VALUE);
        let mut fp = 0;
        let probes = 5000;
        for i in 0..probes {
            if b.might_contain(&Value::str(&format!("absent-{i}"))) {
                fp += 1;
            }
        }
        // 16 bits/value SBBF ≈ 1% FPR; allow generous slack for the test.
        assert!(fp * 100 < probes * 5, "FPR too high: {fp}/{probes}");
    }

    #[test]
    fn encode_decode_roundtrip() {
        let mk = |ids: &[&str]| Bloom::build(&ids.iter().map(|s| Value::str(s)).collect::<Vec<_>>(), 16);
        let zb = ZoneBlooms::new(vec![
            BloomEntry { zone_id: 0, field_id: 1, bloom: mk(&["a", "b", "c"]) },
            BloomEntry { zone_id: 1, field_id: 1, bloom: mk(&["x", "y"]) },
        ]);
        assert_eq!(ZoneBlooms::decode(&zb.encode()).unwrap(), zb);
    }

    #[test]
    fn membership_prunes_absent_zone() {
        let zb = ZoneBlooms::new(vec![BloomEntry {
            zone_id: 0,
            field_id: 1,
            bloom: Bloom::build(&[Value::str("present")], 16),
        }]);
        assert!(zb.might_contain(0, 1, &Value::str("present")));
        assert!(!zb.might_contain(0, 1, &Value::str("definitely-absent-value")));
        // No bloom for zone 9 → conservative "maybe".
        assert!(zb.might_contain(9, 1, &Value::str("anything")));
    }
}
