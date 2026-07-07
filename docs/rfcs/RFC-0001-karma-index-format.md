# RFC-0001 · The `karma-index` format — open indexes for Apache Iceberg over Puffin

> **Status:** Draft · founding · reference implementation in `crates/karma-index`
> **Scope of this RFC:** the container binding (Puffin) and the first blob type,
> `karma-zonemap-v1`. Bloom / bitmap / inverted are *reserved* here and specified in
> later RFCs.
> **Audience:** the Iceberg/Puffin community as much as Karma — this format is meant
> to be readable by *any* engine, and (per the project thesis) eventually proposed
> upstream. Nothing here depends on Karma the engine.

## 1. Motivation

Apache Iceberg standardizes table metadata and per-column **statistics**, and Puffin
gives it an extensible sidecar for **sketches** (`apache-datasketches-theta-v1`) and
**deletion vectors** (`deletion-vector-v1`). What Iceberg still lacks is a portable
**index format**: the min/max zone maps, bloom filters, bitmaps and inverted indexes
an engine uses to *skip work* and serve interactive queries sub-second — today every
engine reinvents these privately, or bolts on a separate serving system (Pinot,
Druid, StarRocks) with a second copy of the data.

`karma-index` fills that gap **in place**, as Puffin blobs beside the Iceberg data,
so the index travels with the table and any Puffin-capable reader can find it.

**Design tenets**

1. **Ride Puffin, don't reinvent a container.** We define *blob types*, not a new
   file format. A generic Puffin reader (pyiceberg, iceberg-rust) can already list
   `karma-*` blobs from a file we write.
2. **Self-describing, engine-neutral bytes.** Every blob payload has an explicit,
   little-endian, versioned layout — no language- or engine-specific serialization.
   The robustness test is *"a second engine reads it"*.
3. **Conservative by construction.** An index may only ever cause an engine to *skip
   provably-empty work*. Any uncertainty resolves to "read it". A false skip drops
   real rows — unacceptable, especially under governance.

## 2. Container: the Puffin binding

A `karma-index` file is a standard Puffin file
([spec](https://iceberg.apache.org/puffin-spec/)):

```
Magic  Blob₁ … Blobₙ  Footer
Magic  := 0x50 0x46 0x41 0x31            ("PFA1")
Footer := Magic  FooterPayload(JSON)  FooterPayloadSize(i32 LE)  Flags(4B)  Magic
```

Each `karma-*` blob is described by a standard Puffin `BlobMetadata` entry in the
footer's `FileMetadata.blobs`:

| Puffin field | `karma-index` use |
|---|---|
| `type` | the blob type, e.g. `karma-zonemap-v1` |
| `fields` | the Iceberg **field IDs** the index covers |
| `snapshot-id`, `sequence-number` | the snapshot the index is valid for; `-1` for an index not yet bound to a snapshot (mirrors `deletion-vector-v1`) |
| `offset`, `length` | locate the blob payload in the file body |
| `compression-codec` | `lz4` \| `zstd` \| omitted (v1 reference writes uncompressed) |
| `properties` | blob-type-specific key/values (see below) |

**v1 reference scope:** uncompressed footer (`Flags = 0`) and uncompressed blob
payloads. Compression is spec-legal and additive; readers must honor
`compression-codec` when present.

## 3. Blob type: `karma-zonemap-v1`

A zone map is the highest-leverage index: cheap to build, and it prunes whole row
ranges before any data is read.

### 3.1 Model

A data file is divided into **zones** — contiguous row ranges, typically the Parquet
**row groups** (but the format does not require that). For each zone and each indexed
column the blob records `{min, max, null_count, value_count}`. `min`/`max` are
*bounds*, not exact values; truncated bounds are permitted as long as `min` is a true
lower bound and `max` a true upper bound of the column within the zone.

### 3.2 Recommended `BlobMetadata`

- `type` = `karma-zonemap-v1`
- `fields` = every Iceberg field ID present in the payload
- `properties.zone-count` = decimal string, the number of zones (optional, advisory)

### 3.3 Payload layout (structural integers little-endian)

```
u8   version            = 1
u32  zone_count
repeat zone_count:
  u32  zone_id
  u64  row_offset       first row index of the zone within the data file
  u64  row_count
  u32  column_count
  repeat column_count:
    i32  field_id       Iceberg field ID
    u64  null_count
    u64  value_count    number of non-null values
    u32  min_len ; min bytes   single-value serialization of the lower bound (len 0 = absent)
    u32  max_len ; max bytes   single-value serialization of the upper bound
```

**Bounds use Iceberg Appendix-D single-value serialization** — byte-for-byte the same
encoding as manifest `lower_bounds`/`upper_bounds`. A bound carries **no type tag**; its
type is resolved from the table schema via its field ID (the engine already has it,
exactly as for manifest bounds). The reference codec threads a `field-id → IcebergType`
map (`ColumnTypes`) into `encode`/`decode`. Per-type serialization:

| Iceberg type | bytes |
|---|---|
| `boolean` | 1 byte (`0x00`/`0x01`) |
| `int` / `long` | 4- / 8-byte little-endian two's complement |
| `float` / `double` | 4- / 8-byte little-endian IEEE-754 |
| `date` | `int` days since 1970-01-01 |
| `time` | `long` microseconds since midnight |
| `timestamp`/`timestamptz` | `long` microseconds since epoch |
| `string` / `binary` / `fixed` | raw bytes (UTF-8 for string) |
| `decimal(P,S)` | unscaled value as **minimum-width two's-complement big-endian** (scale from the type) |

This makes the zone map a fine-grained refinement of Iceberg's own file-level bounds:
engines reuse their existing comparators, and the bytes are identical to what a manifest
already stores. A zero-length bound is an absent bound (an all-null zone). Ordering within
a column is exact — in particular a `decimal` is compared by its unscaled value at a
common scale (a scale mismatch is treated as incomparable, §4). This encoding is the
alignment behind the upstream proposal (§6 item 4;
`docs/proposals/0001-puffin-secondary-indexes.md`).

### 3.4 Reserved

`karma-bloom-v1`, `karma-bitmap-v1`, `karma-inverted-v1` are reserved blob-type names
for subsequent RFCs. Readers MUST ignore blob types they do not recognize (Puffin
already guarantees this — unknown blobs are just bytes).

## 4. Pruning semantics (normative)

Given a zone's `{min, max, null_count, value_count}` for a column and a single-column
predicate, a zone MAY be skipped only when the bounds **prove** no row matches:

| Predicate | Skip the zone iff |
|---|---|
| `col = v` | `v < min` **or** `v > max` |
| `col < v` | `min >= v` |
| `col <= v` | `min > v` |
| `col > v` | `max <= v` |
| `col >= v` | `max < v` |

Additionally, a zone with `value_count = 0` (all nulls) is skipped for any value
comparison (SQL three-valued logic: `col <op> v` is never TRUE when `col IS NULL`).

**Conservatism is mandatory.** If the column has no stats in the zone, if the two
values are of different `Value` variants, if a float bound is `NaN`, or if two
`Decimal` bounds carry different `scale`s, the comparison is *undefined* and the zone
**is not skipped**. Every "unsure" is "read it". The reference implementation encodes
this by making the comparison return `None` and only skipping on a definite
`Some(ordering)`.

## 5. Reference implementation

`crates/karma-index`:
- `puffin` — spec-compliant Puffin reader/writer (§2).
- `zonemap` — the `karma-zonemap-v1` codec (§3).
- `prune` — the conservative pruning of §4 (`surviving_zones`, `can_skip`).

`crates/karma-parquet` is the **read-path**: a Parquet-backed `TableProvider` where a
zone is a row group and the sidecar (zone map + blooms) is built from the file by
`build_sidecar`; a scan reads only the surviving row groups (proven by an instrumented
reader — the pruned row group's bytes are never fetched). The `Expr → Predicate`
translation is shared with `crates/karma-datafusion` (`translate`) so the two providers
can't drift.

Tested: Puffin round-trip, zone-map round-trip (ints/floats/strings/nulls/decimals/
dates/times/timestamps), the full write→read→decode→prune loop, exact decimal/temporal
pruning (incl. scale-mismatch conservatism), and the Parquet read-path (I/O-skip,
full-scan differential, bloom-beats-stats); boundary and conservatism cases are asserted.

**Cross-read validation.** A *second, independent* implementation (pure-stdlib
Python, written from this spec — not ported from Rust) reads what Rust writes and
vice-versa, and both encode the zone-map payload to **byte-identical** bytes. This is
the format's robustness test — *"a second engine reads it"* — and an executable check
that this RFC is unambiguous: two implementations from the text produced the same
bytes. See `interop/` (harness + golden Puffin fixtures).

## 6. Open questions (for later RFCs / upstream discussion)

1. ~~**Decimal/temporal bound encoding** — exact, engine-neutral forms instead of
   collapsing to `I64`/`F64`.~~ **Resolved** (this RFC, §3.3): bounds are Iceberg
   Appendix-D single-value serialization — `decimal` = minimum-width two's-complement
   big-endian unscaled value (scale from schema), temporals as `int`/`long`, type from
   the field's schema type. Exact, engine-neutral, byte-identical to manifest bounds,
   and cross-read byte-identical between the two implementations. The bloom hashes the
   *same* single-value bytes (`bloom.rs::value_hash`), so its decimal hashing is now
   Parquet/Iceberg-consistent too. This encoding *is* the upstream alignment (item 4).
2. **Truncated bounds** — a canonical truncation rule so bounds stay small yet valid.
3. **Zone identity** — *Partially resolved.* The `karma-parquet` reference read-path
   binds **`zone_id` = Parquet row-group ordinal** (and `row_offset` = the row group's
   first row), so a query reads only the surviving row groups — the index skips I/O,
   not just RAM. `build_sidecar` derives the sidecar from a file's row groups. *Still
   open:* multi-file (manifest-level) zone maps, and independent chunking finer or
   coarser than a row group.
4. **Upstreaming** — *In progress.* A proposal to add the zone-map and bloom blobs to
   Apache Iceberg as standard Puffin blob types (`zone-map-v1`, `bloom-filter-v1`,
   vendor-neutral names) is drafted in `docs/proposals/0001-puffin-secondary-indexes.md`
   with PR-ready spec text in `0001-puffin-spec-delta.md`. It aligns the bound encoding
   to Iceberg Appendix-D single-value serialization so the zone map is byte-compatible
   with manifest bounds. Being the co-designed standard is the strategic goal (project
   thesis / HANDOFF §7, audit §9): own the format, whatever engine runs.
