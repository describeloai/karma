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

### 3.3 Payload layout (little-endian)

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
    Value min
    Value max

Value := u8 tag  +  payload
  0  Null   → (no payload)          — an absent bound (e.g. an all-null zone)
  1  Bool   → u8 (0|1)
  2  I64    → i64
  3  F64    → f64 (IEEE-754 bits)
  4  Bytes  → u32 len + len bytes    — UTF-8 strings and opaque binary both ride here
```

**Type mapping.** The five wire forms cover Iceberg's primitive types by *ordering
class*: integer-like → `I64`; floating/decimal → `F64`; string/binary → `Bytes`
(compared bytewise, which equals UTF-8 code-point order); boolean → `Bool`. The
column's real Iceberg type lives in table metadata; the zone map only needs
comparable bounds. (Decimal/temporal bound encodings are refined in a follow-up;
v1 readers treat unknown-but-typed bounds conservatively — see §4.)

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
values are of different `Value` variants, or if a float bound is `NaN`, the
comparison is *undefined* and the zone **is not skipped**. Every "unsure" is "read
it". The reference implementation encodes this by making the comparison return
`None` and only skipping on a definite `Some(ordering)`.

## 5. Reference implementation

`crates/karma-index`:
- `puffin` — spec-compliant Puffin reader/writer (§2).
- `zonemap` — the `karma-zonemap-v1` codec (§3).
- `prune` — the conservative pruning of §4 (`surviving_zones`, `can_skip`).

Tested: Puffin round-trip, zone-map round-trip (ints/floats/strings/nulls), and the
full write→read→decode→prune loop; boundary and conservatism cases are asserted.

## 6. Open questions (for later RFCs / upstream discussion)

1. **Decimal/temporal bound encoding** — exact, engine-neutral forms (e.g. Iceberg's
   single-value serialization) instead of collapsing to `I64`/`F64`.
2. **Truncated bounds** — a canonical truncation rule so bounds stay small yet valid.
3. **Zone identity** — binding `zone_id`/`row_offset` to Parquet row-group indices vs.
   an independent chunking, and multi-file (manifest-level) zone maps.
4. **Upstreaming** — whether `karma-zonemap-v1` should be proposed as a standard
   Iceberg/Puffin blob type. Being the co-designed standard is the strategic goal
   (project thesis / HANDOFF §7, audit §9): own the format, whatever engine runs.
