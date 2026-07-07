# karma-datafusion

A [DataFusion](https://datafusion.apache.org/) `TableProvider` that prunes scans
using [`karma-index`](../karma-index) zone maps — the point where the index earns
its keep: a query's filters skip whole zones (row ranges) that provably cannot
match, so the scan reads less.

## What it does (full breadth of the provider contract)

- **Filter pushdown** — `supports_filters_pushdown` reports prunable filters as
  `Inexact` (we prune zones; DataFusion still re-applies the filter to the rows).
- **`Expr` → predicate** — translates `col <op> literal` / `literal <op> col`
  (`=,<,<=,>,>=`, with operator flip and top-level `AND` splitting) into
  `karma-index` predicates, mapping column names → Iceberg field ids.
- **Pruning** — `surviving_zone_indices` intersects the surviving zones across all
  predicates (AND) and the `scan` reads only those zones.
- **Projection** — honored via `MemorySourceConfig`.

## Correctness

Because filters are pushed down as **`Inexact`**, DataFusion re-checks the surviving
rows, so results are correct *regardless* of pruning — pruning only changes how much
is read. The tests assert this by diffing **every** query against an unindexed
`MemTable` over the same data (`results_match_unindexed_memtable`).

The data source here is **in-memory** (one `RecordBatch` per zone): this crate proves
the DataFusion *integration*, not the object-store / Parquet read path. Swapping the
in-memory zones for Parquet row groups later changes only where the batches come
from, not this wiring.

## Building on Windows (GNU toolchain)

The DataFusion dependency tree needs a C compiler (`zstd-sys`, `liblzma-sys`) and an
assembler for `raw-dylib` import libraries (`windows-sys`), which the bare
`x86_64-pc-windows-gnu` rustup toolchain lacks. On such a host: install a complete
MinGW-w64 (e.g. w64devkit), copy its `as`/`ar` into the toolchain's
`…/bin/self-contained/` dir, and point the `cc` crate at its gcc:

```sh
export CC_x86_64_pc_windows_gnu="C:/w64devkit/bin/gcc.exe"
export AR_x86_64_pc_windows_gnu="C:/w64devkit/bin/ar.exe"
```

Linux, macOS, and Windows-MSVC toolchains build this with no such setup.
