# RFC-0002 · `karma-bloom-v1` — split-block Bloom filters for Apache Iceberg over Puffin

> **Status:** Draft · reference implementation in `crates/karma-index` (`bloom` module)
> **Depends on:** [RFC-0001](RFC-0001-karma-index-format.md) (the Puffin binding and the
> zone/`Value` model are shared).
> **Companion to** `karma-zonemap-v1`: the zone map prunes *ranges*; this prunes
> *equality on high-cardinality columns*, where min/max is useless.

## 1. Motivation

A zone map prunes `col > x` / `col BETWEEN …` via per-zone min/max. It cannot prune
`col = 'a-uuid'` on a high-cardinality column: nearly every zone's `[min, max]`
straddles the probe value, so nothing is skipped. A **Bloom filter** answers *"is `v`
possibly in this zone?"* with **no false negatives** — if it says *no*, `v` is
definitely absent and the zone is skipped — and a bounded false-positive rate.
`karma-bloom-v1` stores one Bloom per (zone, column) as a Puffin blob, to prune
`col = v` and `col IN (…)`.

**We reuse Apache Parquet's Bloom filter construction verbatim** — the *split-block
Bloom filter* (SBBF) with XXH64 — rather than inventing one. Same math, just stored
as a Puffin sidecar instead of a Parquet footer: SIMD-friendly, cache-resident, and
byte-compatible with an ecosystem the Iceberg community already ships.

## 2. Container: the Puffin binding

Standard Puffin (see [RFC-0001 §2](RFC-0001-karma-index-format.md)). One
`karma-bloom-v1` blob holds the SBBFs for every (zone, column) it covers:

- `type` = `karma-bloom-v1`
- `fields` = the Iceberg field IDs with a Bloom in this blob
- `properties.bits-per-value` = the sizing used (decimal string, advisory)

## 3. The split-block Bloom filter (SBBF)

Identical to the [Parquet Bloom filter spec](https://github.com/apache/parquet-format/blob/master/BloomFilter.md).

### 3.1 Block

A **block** is 256 bits = eight 32-bit words, all initially 0.

### 3.2 Hash

Each value is hashed to a 64-bit `h` with **XXH64, seed 0**, over the value's
*canonical bytes* (§3.5).

### 3.3 Block selection

```
block_index = ((h >> 32) * num_blocks) >> 32        // upper 32 bits of h
```
(a multiply-shift in lieu of modulo). `num_blocks ≥ 1`.

### 3.4 Mask (within a block)

Let `x = h & 0xFFFFFFFF` (lower 32 bits). Eight odd salts:

```
salt[8] = { 0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d,
            0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31 }
```
For `i` in `0..8`: `y = (x * salt[i]) mod 2^32`; set bit `(y >> 27)` (a value in
`0..31`) of word `i`.

- **Insert(h):** set those 8 bits in `block[block_index]`.
- **Check(h):** true iff all 8 of those bits are already set (all multiplications
  are unsigned 32-bit wrapping).

### 3.5 Canonical value bytes (what gets hashed)

| `Value` | Bytes hashed |
|---|---|
| `Bytes` (string/binary) | the bytes as-is — **XXH64-compatible with Parquet string Blooms** |
| `I64` | 8 bytes, little-endian |
| `F64` | 8 bytes, IEEE-754 little-endian |
| `Bool` | 1 byte (`0`/`1`) |
| `Null` | not inserted, never probed (a value predicate never matches NULL) |

String/binary is the primary high-cardinality case and is byte-for-byte
Parquet-compatible. Numeric encodings are canonical to karma (documented here);
aligning them with Parquet's numeric hashing is a follow-up (§6).

## 4. Blob payload layout (v1, little-endian)

```
u8   version = 1
u32  entry_count
repeat entry_count:
  u32  zone_id
  i32  field_id
  u32  num_blocks
  bytes[num_blocks * 32]   the SBBF: num_blocks blocks, each 8 words as u32 LE
```

One entry per (zone, column). A reader locates the entry for the zone/field it is
probing and runs Check.

### 4.1 Sizing

`num_blocks = max(1, ceil(ndv * bits_per_value / 256))`, where `ndv` is the number
of distinct (non-null) values in the zone. `bits_per_value` is configurable; the
reference default is `16` (≈ 1% false-positive rate for SBBF). The exact sizing is
**not** part of the on-wire format — a reader only needs `num_blocks` (stored) to
Check.

## 5. Pruning semantics (normative)

For `col = v`, a zone MAY be skipped iff **either** index proves absence:
- the zone map proves `v ∉ [min, max]` (RFC-0001 §4), **or**
- a Bloom exists for (zone, col) and `Check(hash(v))` is **false**.

For `col IN (v₁ … vₙ)`, the zone survives iff it survives `= vⱼ` for **some** `j`
(OR). **Conservatism (mandatory):** if no Bloom exists for (zone, col), the Bloom
contributes nothing (the zone survives on that axis). A `Check` of `true` is only
"maybe" → the zone is read. Only a definite `false` skips. No false negatives, ever.

## 6. Open questions

1. **Parquet-exact numeric hashing** — match Parquet's per-type hash input for
   ints/floats/decimals (v1 is exact for strings/binary, canonical for numerics).
2. **Direct Parquet Bloom reuse** — read an existing Parquet column Bloom in place of
   building our own, when the data file already ships one.
3. **Adaptive sizing** — pick `bits_per_value`/`num_blocks` from observed cardinality
   and a target FPR budget per column.
