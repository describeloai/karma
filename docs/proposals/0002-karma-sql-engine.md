# Plan: Karma SQL — a governed dialect over a DataFusion+Karma engine (Carbon Warehouse)

> Companion to the Iceberg upstream proposal (`0001-*`). This is the **platform-side**
> plan: adopt DataFusion (kernelled) + Karma (index/pruning) as the **execution engine of
> the Warehouse SQL surface**, replacing the Postgres path — starting **read-only** in the
> SQL Editor. Grounded in an exploration of the current Carbon code (Aug 2026).
>
> **Status:** design locked (IR = Substrait). First build = the **Substrait spine** spike.

## 1. Thesis

Keep SQL as the user's surface, but **own the semantics**: the user's SQL is bound and
analyzed by *our* rules and lowered to a **canonical, engine-neutral contract
(Substrait)**, which DataFusion executes over the **Iceberg warehouse** with **Karma
pruning** (zone-map ∧ bloom). Postgres-over-JSONB caps at ≤10M rows; DataFusion over
Iceberg + Karma scales to billions, sub-second (proven read-path: 99–100% bytes cut on
scattered high-cardinality — `docs/benchmarks/read-path-benchmark.md`).

We adopt the engine; we do **not** rebuild it. The "from scratch" work is the **front**
(parser binding + our rules + catalog) and the **Substrait contract boundary** — never a
new optimizer/executor.

## 2. What the exploration found (current state)

- **The Warehouse SQL Editor doesn't run SQL on real PG tables.** It rewrites each table
  into a CTE over `dataset_rows` (JSONB), shredding `data->>'col'` with casts, then runs
  that against a read-only PG pool. `app/api/sql-editor/execute/route.ts` (`translateQuery`
  → `executeReadOnlyQuery`).
- **The codebase already plans for this.** The dialect shim (`lib/warehouse/query/dialect.ts`)
  and the JSONB-CTE translator are explicitly *"deleted, not migrated, when Karma lands"*
  — "Karma replaces only the executor." The **durable language** (Block 2 of
  `docs/warehouse-sql/`: `catalog.schema.table` addressing, views-as-catalog-objects,
  EXPLAIN, CTEs, medallion naming, tenancy, auto-LIMIT, UPPERCASE types) stays.
- **The seam** is the span `translateQuery → executeReadOnlyQuery` (`route.ts:1206-1217`):
  input `(userSql, catalog map)`, output `{rows, columns}`. The facade `resolveSqlSource`
  (`lib/lakehouse/sql-source-hydration.ts`) + the read-router (`lib/lakehouse/read-router.ts`)
  route per-dataset (PG vs Iceberg) — our routing hook.
- **Iceberg is a dormant, opt-in mirror.** Authoritative bytes today = PG `dataset_rows`;
  Iceberg (overwrite-per-commit, at **Cloudflare R2** `s3://lakehouse/warehouse`, table
  `datasets.ds_<uuid-no-dashes>`, catalog = PyIceberg SqlCatalog **or Lakekeeper REST**) is
  populated only when `ENABLE_ICEBERG_SYNC` is on, and **no reader serves from it by
  default** — a freshness oracle + reader-flags gate every read, falling back to PG.

## 3. Architecture

```
User SQL ─▶ OUR FRONT (parser + binder + rules)          ← from scratch, ownable
             · FQN catalog.schema.table  · views-as-object
             · function registry (Databricks-parity)  · medallion · tenancy · types
        ─▶ CANONICAL CONTRACT: Substrait plan           ← portable, versioned, diffable,
             (SQL → DataFusion LogicalPlan → Substrait)    engine-neutral (Doberman-swappable)
        ─▶ DataFusion (adopted engine)
        ─▶ karma-parquet TableProviders over Iceberg on R2   ← zone-map ∧ bloom pruning
   (freshness oracle routes Iceberg vs PG per-dataset — only while PG is write-of-record)
```

**Producing Substrait without rebuilding the engine:** SQL → `sqlparser` (DataFusion's) →
`SqlToRel` with **our `SessionState`** (catalog = karma TableProviders, function registry =
our funcs, dialect config = our rules) → `LogicalPlan` → **Substrait** (via
`datafusion-substrait`). The **contract we own** is the Substrait plan; the producer starts
by reusing DataFusion's SQL frontend + our rules and hardens toward a bespoke binder as
needed.

## 4. Obsolete vs. load-bearing (in the base already built)

| Obsolete — the clean engine deletes these | Load-bearing — keep |
|---|---|
| PG-shim `translateQuery` / `dialect.ts` (code says so) | R2 + **Lakekeeper REST** catalog (the interface Karma consumes) |
| `hydrated` bridge (Iceberg rows → `jsonb_to_recordset` → PG) | dual-write + identity cols `__row_index/__row_id/__created_at` |
| | freshness oracle / read-router (until writes go native) |
| | FQN resolver `lib/warehouse/qualified-name.ts` + `buildCatalog` |

## 5. Build sequence

| # | Build | Repo | De-risks / delivers |
|---|---|---|---|
| **3→1** ✅ | **Substrait spine** — prove `SQL → LogicalPlan → Substrait (bytes) → LogicalPlan → execute` over a **karma-parquet** TableProvider, results identical + Karma pruning survives | Karma | **The IR decision — GO** (`crates/karma-sql`) |
| **1→2** ✅ | **Object-store reader** (R2/S3) in `karma-parquet` — async `ParquetObjectReader` fetches only surviving row groups over the wire; `build_sidecar_object`; `ParquetZoneTable` over a `Source::{Local,Object}`; `S3Config`/`s3_store` for R2 | Karma | Local `File` → **Iceberg Parquet on R2**. Tested over an in-memory store: differential vs native, pruning over the network, selection honored |
| **2→3** ✅ | **Iceberg resolution** — `SnapshotTable` reads an Iceberg snapshot's many data files as one table (per-file zone-map/bloom pruning, whole files skipped when bounds miss); `RestResolver` (feature `rest-catalog`) consumes Lakekeeper via iceberg-rust — `load_table → scan → plan_files` gives the data-file paths, karma reads them | Karma | The catalog binding. iceberg-rust does *all* the Iceberg mechanics; only file paths cross the boundary. `SnapshotTable` tested over an in-memory store (union + cross-file pruning); `RestResolver` validates against a live Lakekeeper |
| 4 | **Front: SQL → Substrait with our rules** | Karma | "Our vocabulary" — **next** |
| 5 | **Seam swap in Carbon** — route calls the Karma engine at `translateQuery→execute`, gated by the freshness oracle, PG fallback | Carbon | Deletes the shim |
| 6 | **Shadow rollout + own the dialect** — diff PG↔Karma read-only, flip per-dataset via reader-flags | both | Safe cutover |

## 6. Safety & phasing

- The SQL Editor is **read-only** (`isReadOnly` gate, `BEGIN READ ONLY`, 30s timeout);
  curated DDL is answered from the metadata plane. The beachhead **never touches a write**.
- "Pure warehouse" (PG out of the read path) requires Iceberg to be authoritative-or-fresh
  for a dataset. **Interim:** the freshness oracle bridges (read the mirror when fresh, else
  PG). **Endgame:** native writes retire PG. The Substrait contract + engine are the
  through-line across both.
- Sub-second at billions is credible **with** Karma pruning (read few row groups); cold
  object-store first-byte latency is the remaining factor — the eventual Doberman NVMe cache
  tier. The index makes it possible; the cache makes it consistent.
