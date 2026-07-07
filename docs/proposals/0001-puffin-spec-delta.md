# Spec delta — normative text for `format/puffin-spec.md`

> PR-ready text adding two blob types to the Iceberg Puffin spec. Style follows the
> existing `deletion-vector-v1` / `apache-datasketches-theta-v1` entries. Rationale and
> evidence are in [`0001-puffin-secondary-indexes.md`](0001-puffin-secondary-indexes.md).
>
> **All multi-byte integers below are little-endian**, matching the Puffin footer's
> `FooterPayloadSize`. **Column bounds use the single-value serialization of Appendix D**
> of the Iceberg table spec — the same encoding as manifest `lower_bounds`/`upper_bounds`
> — with the value's type resolved from the schema via its field id.

---

## Blob type `zone-map-v1`

A `zone-map-v1` blob stores, for one data file, per-**zone** and per-column min/max bounds
and null/value counts, so an engine can skip zones that provably cannot satisfy a
predicate. A **zone** is a contiguous range of rows in the data file (typically a Parquet
row group).

**`BlobMetadata`**

- `type` MUST be `zone-map-v1`.
- `fields` MUST list every field id for which the blob stores bounds.
- `snapshot-id` and `sequence-number` are the snapshot the blob was computed for.
- `properties`:
  - `referenced-data-file` (required) — the location of the data file this blob indexes.
  - `zone-count` (optional) — the number of zones, as a decimal string.
- `compression-codec` MAY be `zstd`.

**Payload**

```
  byte    version                 = 0x01
  int32   zone_count
  repeat zone_count times:
    int32   zone_id               ordinal of the zone within the data file
    int64   row_offset            index of the zone's first row in the data file
    int64   row_count             number of rows in the zone
    int32   column_count
    repeat column_count times:
      int32   field_id            Iceberg field id
      int64   null_count          number of null values in the zone for this column
      int64   value_count         number of non-null values
      int32   min_length          byte length of the serialized lower bound (0 = absent)
      byte[min_length] min        single-value serialization (Appendix D) of the min
      int32   max_length          byte length of the serialized upper bound (0 = absent)
      byte[max_length] max        single-value serialization (Appendix D) of the max
```

A bound length of `0` means the bound is absent (e.g. a zone whose values are all null).
Bounds follow the same contract as manifest bounds: the lower bound is ≤ and the upper
bound is ≥ every non-null, non-NaN value in the zone for that column.

**Pruning (informative).** A zone MAY be skipped for a single-column predicate only when
its bounds prove no row matches: `col = v` when `v < min` or `v > max`; `col < v` when
`min ≥ v`; `col ≤ v` when `min > v`; `col > v` when `max ≤ v`; `col ≥ v` when `max < v`. A
zone with `value_count = 0` is skipped for any value comparison. If a bound is absent or a
float bound is `NaN`, the zone is not skipped. Skipping is always conservative: results
are identical whether or not the blob is present.

---

## Blob type `bloom-filter-v1`

A `bloom-filter-v1` blob stores, per zone and per column, a split-block Bloom filter over
the column's non-null values, so an engine can skip a zone that provably does not contain
a probed equality value. It uses the Apache Parquet split-block Bloom filter (SBBF)
construction.

**`BlobMetadata`**

- `type` MUST be `bloom-filter-v1`.
- `fields` MUST list every field id for which the blob stores a filter.
- `snapshot-id` / `sequence-number` — the snapshot the blob was computed for.
- `properties`:
  - `referenced-data-file` (required) — the data file this blob indexes.
  - `bits-per-value` (optional) — the sizing used, as a decimal string.
- `compression-codec` MAY be `zstd`.

**Payload**

```
  byte    version                 = 0x01
  int32   entry_count
  repeat entry_count times:
    int32   zone_id
    int32   field_id
    int32   num_blocks
    repeat num_blocks times:
      int32[8]  block             eight 32-bit words (256-bit SBBF block)
```

**Hashing.** A value is hashed with `XxHash64` (seed 0) of its Appendix-D single-value
serialization (identical to the `zone-map-v1` bound bytes for the same value). Let `h` be
the 64-bit hash. The block is `block_index = ((h >> 32) * num_blocks) >> 32`. Within a
block, for each of the eight words `i` in `0..8`, set bit `((h_low * SALT[i]) >> 27)` where
`h_low = h & 0xFFFFFFFF` and

```
  SALT = [0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d,
          0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31]
```

**Membership (informative).** A value is *definitely absent* from a zone when any of its
eight mask bits is unset; otherwise it *may* be present. A bloom therefore only tightens
equality pruning and never causes a zone the zone map keeps to be wrongly skipped — no
false negatives. `bits-per-value ≈ 16` yields ~1 % false positives.

---

# Appendix — anticipated reviewer objections & answers

**"Parquet already has row-group min/max and inline bloom filters — why duplicate them in
Puffin?"**
Because the Iceberg value of a secondary index is *decoupling it from the data file*, the
same reason deletion vectors are a Puffin blob rather than a rewrite. A Puffin index is
(1) **format-neutral** — ORC and Avro have no SBBF bloom; (2) **planner-visible** — it is
discoverable from table metadata before any data file is opened, whereas Parquet footers
must be read file by file; (3) **rebuildable** — you can add, refresh, or drop it without
rewriting data (Parquet blooms are fixed at write time and often omitted); and (4)
**cross-file capable** — one blob can index a whole partition. It reuses Parquet's exact
SBBF math, so it is not a new index *format*, only a managed *location* for one.

**"Manifests already carry lower/upper bounds."**
At *file* granularity only. A zone map is the same bound contract and the same single-value
serialization one level finer — skipping row groups inside the files that survive manifest
pruning. And manifests carry no bloom, so they cannot prune high-cardinality equality at
all; that is the gap `bloom-filter-v1` fills.

**"Bloom filters can be megabytes."**
Puffin was designed for large stat/index blobs; blobs are `zstd`-compressible and the
index is opt-in per column and snapshot. Being decoupled, it never bloats the data files.

**"Will a stale or missing index return wrong results?"**
No — this is guaranteed by the conservatism rule. A zone is skipped only when *proven*
non-matching (bloom has no false negatives; bounds are ≤/≥ all values). A missing, partial,
or stale index can only cause *under*-pruning (a full or larger scan), never a wrong skip.
Correctness is independent of index state, which is what makes lazy, incremental
maintenance safe.

**"Who else will read it?"**
The format's robustness test is that a *second, independent* engine reads it. Two
implementations (one in Rust, one in pure-stdlib Python written from this spec) already
produce and consume byte-identical blobs. The proposal ships that as the executable proof.

**"Why little-endian framing but Appendix-D bounds?"**
The fixed-width structural integers match the Puffin footer's own little-endian
`FooterPayloadSize`; the *bounds* reuse Appendix-D single-value serialization verbatim so
they are byte-identical to manifest bounds and share the engine's existing comparators
(note: Appendix D stores `decimal` as minimum-width two's-complement **big-endian**, which
the bound bytes carry unchanged).
