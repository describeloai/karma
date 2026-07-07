# karma-parquet — read-path benchmark

**The one number:** on a **scattered high-cardinality** column, Parquet's own min/max
keep **all 100 row groups** while the karma sidecar bloom keeps **1** (a present value)
or **0** (an absent one) — a **99–100% cut in bytes read**. That gap is precisely the
index Iceberg does not have today. On clustered columns karma ties Parquet's native
pruning (both use ordered bounds); on columns nothing can prune, karma honestly prunes
nothing. The index adds pruning exactly where stats are blind, and invents none where
they aren't.

Environment: `--release`, single machine, warm OS page cache (so the latency numbers
*understate* the object-storage win — there, cost is ∝ bytes read, and the analytical
table below is the transferable claim).

Synthetic dataset: **10,000,000 rows**, 100,000 rows/row-group → **100 row groups**, 447.6 MB on disk (snappy). Generated in 12.5s; sidecar (zone map + blooms) built in 9.2s. Reproduce: `cargo run --release --example bench -- --rows 10000000 --row-group-size 100000`.

## Pruning + bytes (analytical, scale-invariant)

`RG kept (stats)` = survivors with the zone map alone (≈ Parquet's own min/max). `RG kept (karma)` = with the zone map **and** the bloom. The gap on the scattered high-cardinality rows is the bloom's contribution — pruning min/max cannot do. `Bytes read` sums each surviving row group's Parquet `compressed_size`; `I/O saved` is relative to a full scan.

| Query | Class | RG total | RG kept (stats) | RG kept (karma) | RG pruned | Bytes read | I/O saved |
|---|---|--:|--:|--:|--:|--:|--:|
| `id = mid` | clustered point (zone map) | 100 | 1 | 1 | 99.0% | 4.5 MB | **99.0%** |
| `ts >= p90` | clustered range (zone map) | 100 | 10 | 10 | 90.0% | 44.7 MB | **90.0%** |
| `user_id = X` | scattered point — BLOOM vs stats | 100 | 100 | 1 | 99.0% | 4.5 MB | **99.0%** |
| `event_id = existing` | unique point — BLOOM vs stats | 100 | 100 | 1 | 99.0% | 4.5 MB | **99.0%** |
| `event_id = ABSENT (in-range)` | absent value — BLOOM prunes ALL | 100 | 100 | 0 | 100.0% | 8.0 B | **100.0%** |
| `amount > 99000.00` | scattered numeric (zone map — honest miss) | 100 | 100 | 100 | 0.0% | 447.4 MB | **0.0%** |
| `region = 'eu'` | low-card everywhere (honest 0%) | 100 | 100 | 100 | 0.0% | 447.4 MB | **0.0%** |

## Measured latency (heavily-pruned queries)

`SELECT count(*) … WHERE …`, karma (`ParquetZoneTable`) vs DataFusion's own Parquet
reader. The baseline prunes by **native min/max only** — the file carries no inline
Parquet bloom filters, which is the Iceberg-today baseline (stats, no open index). Where
the value is scattered/unique, native stats can't prune and read the whole file; the
karma sidecar bloom reads one row group (or none).

| Query | karma | DataFusion-native | speedup | rows |
|---|--:|--:|--:|--:|
| `id = mid` | 41.10 ms | 16.35 ms | **0.4×** | 1 |
| `event_id = existing` | 37.88 ms | 146.79 ms | **3.9×** | 1 |
| `event_id = ABSENT (in-range)` | 3.70 ms | 142.70 ms | **38.6×** | 0 |

**Reading the numbers honestly:**
- **Where the bloom prunes (scattered / absent): 3.9× and 38.6×.** Native scans the whole
  file; karma reads one row group or none. This is the differentiator, and it *widens*
  with scale (native full-scan grows with the data; karma stays at one row group).
- **`id = mid` is 0.4× — karma is *slower* here, and the benchmark says so.** On a
  clustered column native min/max already prunes to one row group, so there's no pruning
  edge left; meanwhile karma's current read-path materializes the whole surviving row
  group into an in-memory batch before filtering, while DataFusion streams. Closing this
  is the **streaming-integration follow-up** (hand DataFusion a row-group selection
  instead of collecting batches ourselves). Until then, karma's win is I/O-bound
  workloads (object storage) and bloom-territory predicates, not warm-cache clustered
  scans.

## Projection to a billion rows

The pruning ratios above are **scale-invariant** (they depend on selectivity and the row-group count, not the absolute row count), and bytes-read scales linearly with the data. At 1e9 rows / 100000 per group (100× this run) the same queries fetch the same *fraction* of the file — e.g. an absent-value or unique-point lookup still reads ~one row group out of 10000, i.e. sub-percent of the bytes. Run it: `--rows 1000000000`.
