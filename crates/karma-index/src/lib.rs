//! # karma-index — the open index format for Apache Iceberg
//!
//! Iceberg gives you table metadata and column *statistics*, but never a real
//! **index format** — the min/max zone maps, bloom filters, bitmaps and inverted
//! indexes an engine needs to *skip work* and serve queries sub-second. `karma-index`
//! defines that format, stored in Iceberg's own extensible sidecar container,
//! **Puffin** ([`puffin`]).
//!
//! It is deliberately **format-first**: this crate is a spec + reference codec, not
//! an engine. The robustness test for the format is *"can a second engine read it"*,
//! not *"did a giant adopt our engine"* — so everything here is a plain,
//! self-describing byte layout (documented in `docs/rfcs/RFC-0001`), readable by
//! anyone who can read a Puffin file.
//!
//! ## What's here (v1)
//! - [`puffin`] — a minimal, spec-compliant Puffin container reader/writer.
//! - [`zonemap`] — the `karma-zonemap-v1` blob: per-zone, per-column min/max +
//!   null/value counts, the highest-leverage index for scan pruning.
//! - [`prune`] — turn a predicate + a zone map into a *conservative* set of zones
//!   an engine may skip (never a false skip — that would drop real rows).
//!
//! Reserved for later blob types: `karma-bloom-v1`, `karma-bitmap-v1`,
//! `karma-inverted-v1`.

pub mod bloom;
pub mod prune;
pub mod puffin;
pub mod zonemap;

pub use bloom::{value_hash, Bloom, BloomEntry, BloomError, ZoneBlooms, BLOOM_BLOB_TYPE, DEFAULT_BITS_PER_VALUE};
pub use prune::{surviving_zones, surviving_zones_indexed, Predicate};
pub use puffin::{read_puffin, write_puffin, BlobMetadata, BlobToWrite, PuffinError, PuffinFile};
pub use zonemap::{ColumnStats, Value, ZoneMap, ZoneMapError, ZoneStats, ZONEMAP_BLOB_TYPE};
