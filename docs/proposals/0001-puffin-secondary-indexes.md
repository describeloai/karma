# Proposal: secondary-index blobs for Apache Iceberg — zone maps & bloom filters (Puffin)

> **Target:** Apache Iceberg `dev@` discussion → a spec change to
> [`format/puffin-spec.md`](https://iceberg.apache.org/puffin-spec/).
> **Status:** draft for upstream. **License:** Apache-2.0, contributable under the ASF CLA.
> **Companion:** [`0001-puffin-spec-delta.md`](0001-puffin-spec-delta.md) — the exact,
> PR-ready normative text for `puffin-spec.md`.
>
> This document is written to be handed to the Iceberg community. It is deliberately
> aligned to Iceberg's *existing* conventions (Puffin container, `BlobMetadata`,
> Appendix-D single-value serialization, the deletion-vector precedent) rather than
> introducing a parallel design. Where our reference implementation currently differs
> from what we propose as the standard, §10 says so plainly.

---

## 1. Summary

Add two standard Puffin blob types that give Iceberg the fine-grained, format-neutral
**secondary indexes** it does not yet have:

- **`zone-map-v1`** — per *zone* (a contiguous row range, typically a Parquet row group),
  per indexed field: `{min, max, null_count, value_count}`. Skips zones *within* a file
  that provably cannot match — the tier below manifest file-bounds.
- **`bloom-filter-v1`** — per zone, per indexed field: a split-block Bloom filter (the
  Parquet SBBF construction) over the column's non-null values. Prunes **high-cardinality
  equality**, where min/max are structurally useless.

Both live in a **Puffin sidecar decoupled from the data files** (the deletion-vector
model): built, rebuilt, or dropped without rewriting multi-gigabyte data. Bounds use
Iceberg's **Appendix-D single-value serialization** — byte-for-byte the encoding already
used in manifest `lower_bounds`/`upper_bounds` — so engines reuse their existing
comparators, and the zone map is a natural refinement of what Iceberg already does at
file granularity.

Evidence that this is real and unambiguous: a reference implementation (Apache-2.0), a
**second, independent implementation** that reads the first's blobs **byte-identically**
(the multi-engine robustness test the ASF asks for), and a benchmark showing a
**99–100 % cut in bytes read** exactly where manifest and Parquet min/max are blind.

## 2. Motivation

Iceberg today gives a planner three pruning inputs:

1. **Manifest file-bounds** — `lower_bounds`/`upper_bounds` per data file (`map<field-id,
   binary>`, v1–v3; typed `content_stats` in v4). These skip *whole files*.
2. **Puffin stats** — `apache-datasketches-theta-v1` (distinct-value estimate) for
   planning, not row skipping.
3. **Deletion vectors** — `deletion-vector-v1`, a per-file Puffin bitmap of deleted rows.

What is missing is a **standard secondary index**:

- Nothing prunes **within** a file at the table layer. Once a file survives its manifest
  bounds, Iceberg reads it whole (or falls back to *format-internal* structures — Parquet
  row-group stats — that are locked inside the file and invisible to the planner).
- Nothing prunes **high-cardinality equality**. Min/max cannot skip `id = <uuid>` or
  `user_id = 42` on a scattered column: every file's `[min, max]` spans the value. This
  is the single most common serving query, and it is the one Iceberg cannot accelerate.

The consequence is the *serving gap*: sub-second point/needle queries over Iceberg today
require either copying data into a separate serving system (Pinot/Druid/StarRocks) or
relying on Parquet-internal indexes that are format-locked, per-file, optional, and not
declaratively managed by the table.

Puffin was explicitly designed as the home for "large stats and index blobs", and the
ecosystem is already building on that assumption — recent work attaches ANN/vector
indexes to Puffin snapshots and **presumes a zone-map/bloom coarse-pruning stage that has
no standard yet** [3]. Standardizing that coarse stage is the missing primitive: it is
foundational to the index tier Iceberg is visibly growing toward.

## 3. Design principles

1. **Decoupled from data files** — the index is a Puffin sidecar referenced by table
   metadata, built/rebuilt/dropped without rewriting data. This is precisely why deletion
   vectors are a Puffin blob and not a rewrite; secondary indexes have the same need.
2. **Format-neutral** — bounds are serialized with Iceberg's own single-value form, and
   the bloom hashes the same serialized bytes, so the index works for Parquet, ORC, Avro,
   and future formats. It does not depend on Parquet footers.
3. **Field-id keyed** — every entry references an Iceberg **field id**, so the index is
   schema-evolution safe (rename/reorder-proof), exactly like manifest bounds.
4. **Conservative by construction** — a zone is skipped only when the index *proves* no
   row can match; every "unsure" resolves to "read it". Therefore **query results are
   correct regardless of whether the index exists, is complete, or is stale** — the index
   can only change *how much* is read, never *what* comes out (§7). This is the property
   that makes incremental adoption and lazy maintenance safe.
5. **Provably multi-engine** — the robustness test for a format is *"a second engine reads
   it"*, not *"a vendor adopted it"*. Two independent implementations produce and consume
   byte-identical blobs (§8).

## 4. Blob type `zone-map-v1`

**Model.** A data file is divided into **zones** — contiguous row ranges, typically the
Parquet row groups, but the format is agnostic to how a writer chooses zones. For each
zone and each indexed column the blob stores `{min, max, null_count, value_count}`.

**Puffin `BlobMetadata`.**

| field | value |
|---|---|
| `type` | `zone-map-v1` |
| `fields` | the list of indexed Iceberg field ids |
| `snapshot-id` / `sequence-number` | the snapshot the index was built for |
| `properties` | `referenced-data-file` (the data file this blob indexes), `zone-count` |
| `compression-codec` | optional (`zstd`) |

**Payload.** Little-endian framing; **each bound is Iceberg Appendix-D single-value
serialization**, length-prefixed, with a zero length meaning "bound absent" (an all-null
zone). The bound's *type* is taken from the table schema via its field id, so no per-value
type tag is stored — identical in spirit to manifest `lower_bounds`. See the spec-delta
for the exact byte layout.

**Pruning semantics (normative, conservative).** Given a zone's `{min, max, null_count,
value_count}` for a column and a single-column predicate, a zone MAY be skipped only when
the bounds prove no row matches:

| predicate | skip iff |
|---|---|
| `col = v` | `v < min` or `v > max` |
| `col < v` | `min ≥ v` |
| `col ≤ v` | `min > v` |
| `col > v` | `max ≤ v` |
| `col ≥ v` | `max < v` |

A zone with `value_count = 0` (all nulls) is skipped for any value comparison
(three-valued logic). If a bound is absent, the values are incomparable, or a float bound
is `NaN`, the zone is **not** skipped. This is the same bound contract as Iceberg manifest
bounds ("each value must be ≤ / ≥ all non-null, non-NaN values in the column"), applied at
zone granularity.

## 5. Blob type `bloom-filter-v1`

**Model.** Per zone, per indexed field, a **split-block Bloom filter** built with the
*Apache Parquet* SBBF construction verbatim — 256-bit blocks of eight 32-bit words, XXH64
(seed 0) of the value's canonical bytes, block chosen by the hash's upper 32 bits, a
per-word mask from eight odd salts. Reusing Parquet's math means the filter is
byte-compatible with an index the Iceberg ecosystem already understands; the only
difference is that it lives in a managed Puffin sidecar instead of a Parquet footer.

**Canonical hashed bytes.** The value is hashed as its **Iceberg single-value
serialization** (the same bytes as the zone-map bound), so a bloom is engine-neutral and
consistent with the bounds. (For string/binary this is identical to Parquet's own bloom
input; see §10 for the decimal alignment note.)

**Semantics.** No false negatives: `might_contain(v) = false` **proves** `v` absent (skip
the zone); `true` is "maybe" (read it). A bloom only *tightens* equality pruning — a
missing bloom leaves the zone-map verdict untouched, and a bloom never keeps a zone the
zone map already skipped. Sizing is tunable in bits-per-value (~1 % false-positive rate at
16 bits/value); blobs are `zstd`-compressible; the index is opt-in per column.

## 6. Relationship to existing Iceberg mechanisms

- **vs. manifest file-bounds** — *same* semantics and *same* single-value serialization,
  one granularity finer (zone vs. file). The zone map is the tier directly below manifest
  bounds: manifests skip files, zone maps skip row groups within the surviving files.
- **vs. Parquet row-group stats + inline bloom** — the format-internal structures are
  (a) **format-locked** (no equivalent in ORC/Avro), (b) **buried in file footers** — a
  planner must open every file to consult them, whereas a Puffin blob is discoverable from
  table metadata *before* touching data, (c) **not rebuildable** — you cannot add or
  refresh a Parquet bloom without rewriting the data file, and writers frequently omit
  them, and (d) **per-file only** — a Puffin blob can index a whole partition/manifest.
  The Puffin index is the *declaratively managed, table-owned* counterpart, exactly as
  deletion vectors are the table-owned counterpart of format-internal delete encodings.
- **vs. `apache-datasketches-theta-v1` and `deletion-vector-v1`** — complementary. Theta
  estimates cardinality (planning); deletion vectors carry deletes; these two blobs are
  the **skip/prune** tier. They follow the deletion-vector conventions (a
  `referenced-data-file` property, Puffin framing).

## 7. Correctness & maintenance

**The conservatism invariant is the adoption-safety property.** Because a zone is skipped
only when *proven* non-matching, a query is correct no matter the index state:

- **No index** → nothing is skipped → full scan → correct.
- **Partial index** (some files/zones unindexed, e.g. data appended after the index was
  built) → unindexed files are simply not pruned → correct, just slower.
- **Stale index** → same: staleness can only cause *under*-pruning, never a wrong skip,
  because a writer never emits an index that claims a value is absent when it is present
  (bloom: no false negatives; zone-map: bounds are ≤/≥ all values).

This means engines may adopt reading incrementally and writers may rebuild lazily (on
compaction / rewrite), snapshot-scoped like deletion vectors, with **zero risk of
returning wrong rows** at any point — the reason a governance-grade format can accept it.

## 8. Evidence (reference implementation + independent cross-read + benchmark)

- **Reference implementation** (Apache-2.0): a complete codec for both blobs, the
  conservative pruning of §4, and a query-engine `TableProvider` that reads **only the
  surviving Parquet row groups** — proven by an instrumented reader that shows the pruned
  row group's bytes are never fetched.
- **Independent second implementation** (pure-stdlib Python, written from the spec, *not*
  ported): it reads the reference impl's Puffin blobs and vice-versa, and both encode the
  payloads to **byte-identical** bytes — the executable proof that the spec is
  unambiguous and that *a second engine reads it*.
- **Benchmark** (10 M rows, 100 row groups): on **scattered high-cardinality** columns,
  manifest/Parquet min/max keep **all 100** row groups while the sidecar bloom keeps **1**
  (a present value) or **0** (an absent one) — a **99–100 % cut in bytes read**, measured
  **3.9×/38.6×** faster than the engine's native (min/max-only) reader. The pruning ratios
  are scale-invariant and project to a billion rows. Honest in both directions: on
  *clustered* columns the zone map ties native min/max, and where nothing can prune
  (low-cardinality, unindexed) it prunes nothing.

## 9. Naming & scope

We propose **vendor-neutral, feature-named** types — `zone-map-v1` and `bloom-filter-v1`
— matching Iceberg's own `deletion-vector-v1`. (The reference implementation currently
emits `karma-zonemap-v1` / `karma-bloom-v1` with *identical payloads*; it would adopt the
standard names on acceptance.) Owning the *format* under a neutral name is the point: the
index becomes part of Iceberg, read by whatever engine runs.

Scope of this proposal is the two coarse-pruning blobs. Bitmap and inverted-index blobs,
and multi-file (manifest-level) zone maps, are natural follow-ons (§13).

## 10. Reference-impl deltas to align before contribution

In the spirit of not hiding divergence, the reference codec's `v1` differs from the
standard proposed here in exactly two mechanical ways, both about **bound encoding**:

1. **Self-describing vs. schema-typed bounds.** The reference `v1` tags each bound with a
   type byte and always widens integers to 8 bytes. The standard instead uses Iceberg
   **Appendix-D single-value serialization** with the type taken from the field id — no
   tag, `int` = 4 bytes, etc. — so bounds match manifest bounds exactly.
2. **Decimal.** The reference `v1` stores a decimal as `i128` little-endian + an `i32`
   scale. Iceberg (and this proposal) store the **unscaled value as minimum-width,
   two's-complement big-endian**, with scale taken from the column type. The temporal and
   string/boolean encodings already match Iceberg single-value serialization; only decimal
   and the integer width/tagging change.

These are small, mechanical edits to the codec and the cross-read fixtures, done as the
pre-contribution step so the upstreamed `zone-map-v1` is byte-compatible with Iceberg
manifest bounds from day one.

## 11. Adoption path

1. **`dev@` discussion** around this design doc.
2. **Spec PR** adding the two blob types to `format/puffin-spec.md` (text in the
   companion spec-delta).
3. **Reference reader** in `iceberg-rust` / `pyiceberg`, leveraging the two existing
   implementations.
4. **Writer integration** — compaction / maintenance actions produce the sidecar.
5. **Engine pruning** — planners (DataFusion, Spark, Trino) consult the blobs at scan
   planning; the reference `TableProvider` is a worked example.

Each step lands value independently and none blocks correctness (a reader can ignore
unknown blobs; a planner that ignores the index is still correct).

## 12. IP & licensing

Everything is Apache-2.0 and contributable under the ASF CLA. The SBBF construction and
XXH64 are Apache Parquet's own; the container is Iceberg's own Puffin; the single-value
serialization is Iceberg's own. No new dependencies, no patents.

## 13. Open questions for the community

- **Granularity of zones** — bind `zone_id` to Parquet row-group ordinals, or an
  independent chunking, or both (a `zone-kind` property)?
- **Multi-file / manifest-level** zone maps — one blob indexing many files or a whole
  partition, pruning at plan time before opening any file.
- **v4 `content_stats`** interaction — should zone bounds reuse the v4 typed stats struct?
- **Follow-on index blobs** — `bitmap-v1` (roaring, for low-cardinality set membership)
  and `inverted-v1` (text), building on the same Puffin conventions.

## References

1. Apache Iceberg — Puffin spec (`format/puffin-spec.md`): container, `BlobMetadata`,
   `apache-datasketches-theta-v1`, `deletion-vector-v1`.
2. Apache Iceberg — table spec, Appendix D "Single-value serialization"; manifest
   `lower_bounds`/`upper_bounds`.
3. *Puffin-Backed Vector Indexes* (arXiv 2606.04196) — attaches ANN indexes to Iceberg
   Puffin snapshots and assumes a bloom/zone-map coarse-pruning stage.
4. Apache Parquet — split-block Bloom filter specification (SBBF + XXH64).
