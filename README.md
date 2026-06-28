# Karma

Karma is a Rust/Arrow-native execution engine whose fundamental runtime citizen is not the row or the DataFrame, but the **semantic object with provenance**. Ontology bindings, column- and cell-level lineage, and governance policy are first-class primitives carried through the engine's IR and enforced at execution time — not reconstructed above it.

Karma is being built as an open, Apache-bound project from day one. See [`docs/00-landscape-and-thesis.md`](docs/00-landscape-and-thesis.md) for the founding thesis.

## License

Karma is licensed under the [Apache License, Version 2.0](LICENSE).
