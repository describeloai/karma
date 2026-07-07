# Cross-read validation

The founding claim of `karma-index` is that it is a **format**, not just one
engine's private encoding — *"a second engine can read it."* This directory proves
that claim end to end, in **both directions**, with an implementation of the format
written **independently from the spec** (not ported from the Rust crate).

## What's here

| File | Role |
|---|---|
| `karma_index.py` | An **independent, pure-stdlib** Python impl of the Puffin container + the `karma-zonemap-v1` codec, written from RFC-0001 (no `pyiceberg`/`pyarrow`, no port of the Rust). |
| `interop_check.py` | The Rust→Python check + the byte-identity assertion; also emits the Python-written fixture. |
| `fixtures/zonemap.expected.json` | The canonical zone map — the language-neutral source of truth both sides build from. |
| `fixtures/zonemap.rust.puffin` | Golden Puffin file written by the Rust reference impl. |
| `fixtures/zonemap.python.puffin` | Golden Puffin file written by the independent Python impl. |

The Rust side is `crates/karma-index/examples/gen_fixture.rs` (writer) and
`crates/karma-index/tests/interop.rs` (the Python→Rust reader).

## What it proves

1. **Rust → Python** — Python reads the Rust-written Puffin *generically* (as any
   Puffin reader would), finds the `karma-zonemap-v1` blob by its footer metadata,
   and decodes the payload to exactly the canonical zone map.
2. **Python → Rust** — Rust reads the Python-written Puffin and decodes it to the
   same canonical zone map.
3. **Byte-identity** — the two independent impls encode the zone-map payload to
   **byte-identical** bytes. (In practice even the whole 393-byte file matches — but
   only payload identity is *asserted*, since Puffin does not mandate footer-JSON key
   order.) Byte-identity is the strong evidence that RFC-0001 is unambiguous: two
   people reading the spec produced the same bytes.

## Run it

From the repo root (Rust toolchain + Python 3.8+; no third-party Python deps):

```sh
cargo run --example gen_fixture      # 1) Rust writes fixtures/zonemap.rust.puffin
python interop/interop_check.py      # 2) Python reads it, checks bytes, writes its own
cargo test --test interop            # 3) Rust reads the Python-written file
```

All three must succeed. `interop_check.py` exits non-zero on any mismatch.

## Note on third-party readers

We deliberately validate with an **independent reimplementation** rather than a
library, because that is the stronger portability proof (a library port could share
our bugs). A generic third-party reader (e.g. `pyiceberg`'s Puffin support, or
`iceberg-rust`) listing our blobs is a nice additional smoke test and is planned;
it is not run here because the target Python couldn't build those wheels, and it
would prove strictly less than the independent impl already does.
