# Apache Karma — Landscape & Founding Thesis

> **Status:** DRAFT · founding session (June 2026) · *headline-direction decision pending*
> **Purpose:** the strategic memo that defines what Karma *is* before we write a line of engine code.

---

## 0. The mandate

Build our own data-processing engine — working name **Apache Karma** — that satisfies two constraints simultaneously:

1. **It is Node's primary compute engine.** The same relationship Spark has to Databricks, or Velox/Photon to their platforms. Node (our Foundry-class governance + ontology platform) runs *on* Karma.
2. **It is universal.** Karma must be valuable to thousands of organizations that have never heard of Node — a standalone, donatable, eventually-Apache project. A single-vendor engine does not graduate the Apache Incubator and does not become "the next Spark."

These two constraints are in tension, and resolving that tension *is* the strategy. The wrong move is to build "Node's internal engine" and hope it generalizes. The right move is to find the **universal pain** whose best solution happens to also be exactly what Node needs.

---

## 1. The 2026 landscape (where the players actually are)

| Engine | Substrate | Core bet | 2026 status |
|---|---|---|---|
| **Spark** | JVM | General distributed batch+micro-batch | Incumbent, but being *accelerated away* — Comet (DataFusion) and Gluten (Velox) replace its execution core. JVM is now legacy tax. |
| **Flink** | JVM | True event-at-a-time streaming, stateful | Strong in streaming niche; batch story weak; JVM tax. |
| **DataFusion** | Rust + Arrow | Embeddable, modular query engine | **Rising to "foundational infrastructure."** The default execution core for *new* platforms. |
| **Polars** | Rust + Arrow | Fast single-node DataFrame | Exploding; now extending to distributed. |
| **DuckDB** | C++ | In-process OLAP ("SQLite for analytics") | Exploding; the "big data is dead" thesis made real. |
| **Daft / Ray Data** | Rust / Python | **Multimodal + AI workloads** (images, video, embeddings, GPU) | Hot frontier. Daft benchmarks 2–7× Ray Data, 4–18× Spark on multimodal. |
| **Velox** | C++ | Composable execution *library* (not a full engine) | The shared C++ kernel; Meta + Voltron Data. |
| **Feldera / Materialize** | Rust | **Incremental computation** (DBSP / differential dataflow) | Verifiable, bit-identical incremental SQL. Deep technical moat, narrow adoption so far. |

**The white space, stated plainly:** every engine in that table processes **rows and columns**. They are *semantically blind*. Meaning (what a column represents), provenance (where a value came from), policy (who may see it), and ontology (how entities relate) are all reconstructed **above** the engine — by parsing query plans after the fact (OpenLineage, Spline) or by external catalogs (Unity, Atlan, Polaris). **No engine treats meaning, lineage, and governance as native primitives of the execution model itself.**

That is the gap. And it is the gap Node already lives in.

---

## 2. The three converging vectors of 2026

The research is unambiguous about where the tide is going:

**Vector A — Rust + Arrow + Substrait is the new substrate.** JVM is the legacy tax; new engines are Rust/C++, Arrow-native, and increasingly speak Substrait as a shared IR. Building a new engine on the JVM in 2026 is dead on arrival. This is not a differentiator — it is *table stakes*. Karma is Rust + Arrow, full stop.

**Vector B — Agents are becoming the primary data consumers.** Over 80% of new Databricks databases are now created by AI agents, not engineers. OpenAI's internal data agent serves ~4,000 of 5,000 employees daily. Gartner projects 40% of enterprise apps embed agents by end of 2026. Machines, not humans, are the workload. And machines need something humans could fake without: **explicit, machine-readable semantics and provenance** — an agent must *cite* where an answer came from, and must not hallucinate pipeline intent.

**Vector C — Governance / semantics / lineage move from bolt-on to by-design.** The emerging enterprise stack is described as five layers: a **semantic layer** for governed metrics, an **ontology** for entity relationships, operational playbooks for routing, **lineage for provenance**, and active metadata for decision memory. Governance in 2026 is "embedded directly into data engineering workflows… by design." Write-audit-publish becomes the default, not the exception.

These three vectors point at one thing: **the engine of the agentic era must understand what data *means*, prove where it *came from*, and enforce who may *touch it* — natively.**

---

## 3. The Karma thesis (recommended headline identity)

> **Karma is a Rust/Arrow-native execution engine whose fundamental runtime citizen is not the row or the DataFrame, but the *semantic object with provenance*. Ontology bindings, column- and cell-level lineage, and governance policy are first-class primitives carried through the engine's IR and enforced at execution time — not reconstructed above it.**

In one line: **take Foundry's ontology idea and push it *down into the kernel*.** Foundry kept Spark underneath and built the ontology/governance/lineage *on top*. Karma's bet is that the next platform-defining engine is the one where the ontology *is* the engine's native data model.

Why this is the right bet:

- **It is the genuine white space.** Everyone else is racing on raw speed (Daft) or purity of incremental (Feldera). Nobody owns "the engine that knows what your data means and proves where it came from."
- **It is universal.** Lineage-for-AI-citations, governance-by-design (EU AI Act, data residency, model-training provenance), and semantic context for agents are pains felt by *every* serious data org in 2026 — not a Node niche.
- **It is defensible.** Semantics-as-a-primitive is an architectural choice baked in at the bottom. Retrofitting it into Spark/DataFusion from the outside is exactly the expensive, lossy thing the whole industry is struggling to do today.
- **It is perfectly Node-aligned.** Node *is* a Foundry-class ontology/governance platform. Node-on-Karma becomes the clean Databricks-on-Spark analogy — and the same primitives that power Node are the ones the open-source world wants.

The other three 2026 vectors are not abandoned — they are *composed underneath*: Rust/Arrow/Substrait foundation (Vector A, table stakes), incremental computation as a phase-2 deepening, multimodal as a data-type extension rather than the core identity.

---

## 4. The decision: four candidate identities

The headline identity is a real fork. Karma can be exactly one of these *as its public face* (they compose under the hood, but only one is the brand and the wedge):

| | Identity | Differentiator | Aligns with Node | Crowded? | Moat |
|---|---|---|---|---|---|
| **A** ⭐ | **Semantics / governance-native** | Ontology + lineage + policy as execution primitives | Highest (it *is* Node's DNA) | Empty white space | Architectural; hard to retrofit |
| **B** | **AI / multimodal-native** | Embeddings, tensors, unstructured, GPU, inference first-class | Medium (Node has ML) | Crowded (Daft, Ray, Lance) | Speed; erodible |
| **C** | **Incremental-computation-native** | Everything incrementally maintained; batch+stream collapse | High (Node's incremental paradigm) | Sparse (Feldera) but brutal CS | Deepest tech moat; narrowest wedge |
| **D** | **Composable Arrow/Substrait kernel** | Best embeddable runtime others build on | Low | This is literally DataFusion's game | Hard to out-execute DataFusion |

**Recommendation: A**, built on a Rust/Arrow/Substrait foundation (the discipline of D), with C as a phase-2 deepening and B as a data-type extension.

---

## 5. Can it clear the Apache bar?

The Incubator graduates projects on **community**, not novelty: 3+ independent committers, public meritocratic governance, clean IP/licensing/trademark, ~1.5-year incubation. Implication for us:

- Novelty (the semantics-native thesis) earns *attention* and contributors. **Community earns graduation.**
- "Universal, not single-vendor" is therefore non-negotiable — it is the literal graduation criterion, not just good positioning.
- Practical path: build in the open from day one, design for embeddability (others must be able to adopt Karma *without* Node), court mentors early, keep the IP clean.

## 6. What "AI lowers the cost of building an engine" changes — and doesn't

True: a small team + models can now build what an AMPLab full of PhDs built in 2010. But an execution engine is correctness- and performance-critical systems code. **AI compresses the *implementation*, not the *judgment*** — the invariants, the IR design, the benchmark discipline, the correctness guarantees (especially anything incremental). So the cheap-codegen era makes *this document* — design clarity, chosen wedge — *more* decisive, not less. The bottleneck moved from typing to deciding.

---

## 7. Open decision & next steps

- [ ] **DECIDE: headline identity (A / B / C / D).** Everything downstream forks here.
- [ ] Lock substrate (Rust + Arrow + Substrait — near-certain).
- [ ] Define the smallest end-to-end vertical slice (the "hello world" that proves the thesis).
- [ ] Scaffold repo: governance docs, license (ASF-style), README positioning, architecture RFC-0001.
- [ ] Name check / trademark sanity ("Karma").
