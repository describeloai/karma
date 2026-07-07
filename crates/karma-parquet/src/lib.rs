//! # karma-parquet — a Parquet-backed pruning `TableProvider`
//!
//! Where [`karma_datafusion::KarmaZoneTable`] proves the *DataFusion integration* over
//! in-memory zones, this crate proves the thing that makes the index worth having: it
//! **skips I/O**. A zone here is a **Parquet row group**, and the karma-index (zone map
//! + blooms) lives in a **Puffin sidecar** beside the data file. On a query we prune
//! row groups with the sidecar and hand the `parquet` reader *only the survivors* — so
//! the pruned row groups' column chunks are never fetched from storage.
//!
//! ```no_run
//! use karma_parquet::{build::IndexField, ParquetZoneTable};
//! use std::path::Path;
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Build the sidecar from the file (id = zone map; code = zone map + bloom), then
//! // register a table that reads only surviving row groups.
//! let table = ParquetZoneTable::from_parquet(
//!     Path::new("events.parquet"),
//!     &[IndexField::zonemap("id", 1), IndexField::zonemap_and_bloom("code", 2)],
//! )?;
//! # let _ = table; Ok(()) }
//! ```
//!
//! ## Correctness
//! Like the in-memory provider, pushed-down filters are reported `Inexact`: we prune
//! row groups, DataFusion still applies the real predicate on surviving rows — so the
//! result is correct regardless of pruning, which only changes how much is read. The
//! `Expr → Predicate` translation is the *shared* [`karma_datafusion::translate`], so
//! the two providers can never diverge in what they skip.

use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;

use karma_index::{ZoneBlooms, ZoneMap};

pub mod bench;
pub mod build;
pub mod read;

pub use build::{build_sidecar, build_sidecar_sized, IndexField, Sidecar};

#[derive(Debug, thiserror::Error)]
pub enum KarmaParquetError {
    #[error("parquet: {0}")]
    Parquet(#[from] datafusion::parquet::errors::ParquetError),
    #[error("arrow: {0}")]
    Arrow(#[from] datafusion::arrow::error::ArrowError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("column '{0}' not found in the parquet schema")]
    MissingColumn(String),
    #[error("unsupported column type for indexing: {0}")]
    UnsupportedType(String),
}

fn to_df_err(e: KarmaParquetError) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}

/// A DataFusion table backed by a Parquet file, pruned by a karma-index Puffin sidecar.
///
/// `zone_map.zones[i].zone_id` is the Parquet **row-group ordinal** of zone `i`. On a
/// scan we compute the surviving zones, map them to row-group ordinals, and read only
/// those from the file.
#[derive(Debug)]
pub struct ParquetZoneTable {
    path: PathBuf,
    schema: SchemaRef,
    zone_map: ZoneMap,
    blooms: Option<ZoneBlooms>,
    field_ids: HashMap<String, i32>,
}

impl ParquetZoneTable {
    /// Construct from a fully-specified sidecar (schema read from the file).
    pub fn try_new(
        path: impl Into<PathBuf>,
        zone_map: ZoneMap,
        blooms: Option<ZoneBlooms>,
        field_ids: HashMap<String, i32>,
    ) -> Result<Self, KarmaParquetError> {
        let path = path.into();
        let schema = read::read_schema(File::open(&path)?)?;
        Ok(Self { path, schema, zone_map, blooms, field_ids })
    }

    /// Construct from a [`Sidecar`] built by [`build_sidecar`]. An empty bloom set is
    /// stored as `None` (nothing to consult).
    pub fn from_sidecar(path: impl Into<PathBuf>, sidecar: Sidecar) -> Self {
        let blooms = (!sidecar.blooms.entries.is_empty()).then_some(sidecar.blooms);
        Self {
            path: path.into(),
            schema: sidecar.schema,
            zone_map: sidecar.zone_map,
            blooms,
            field_ids: sidecar.field_ids,
        }
    }

    /// Build the sidecar from the Parquet file and construct the table in one step.
    pub fn from_parquet(path: &Path, fields: &[IndexField]) -> Result<Self, KarmaParquetError> {
        let sidecar = build_sidecar(path, fields)?;
        Ok(Self::from_sidecar(path, sidecar))
    }

    /// The **indices** (positions in `zone_map.zones`) of the zones that survive
    /// `filters` — delegated to the shared translation so this and the in-memory
    /// provider prune identically.
    pub fn surviving_zone_indices(&self, filters: &[Expr]) -> Vec<usize> {
        karma_datafusion::translate::surviving_zone_indices(&self.zone_map, self.blooms.as_ref(), &self.field_ids, filters)
    }

    /// The Parquet **row-group ordinals** that survive `filters` (what the reader is
    /// told to decode). This is the observable payoff — the set read from disk.
    pub fn surviving_row_groups(&self, filters: &[Expr]) -> Vec<usize> {
        self.surviving_zone_indices(filters)
            .into_iter()
            .map(|i| self.zone_map.zones[i].zone_id as usize)
            .collect()
    }
}

#[async_trait]
impl TableProvider for ParquetZoneTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> DFResult<Vec<TableProviderFilterPushDown>> {
        Ok(karma_datafusion::translate::filters_pushdown(&self.field_ids, filters))
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let row_groups = self.surviving_row_groups(filters);
        // Read ONLY the surviving row groups — the pruned ones' bytes are never fetched.
        let file = File::open(&self.path).map_err(|e| to_df_err(e.into()))?;
        let (_schema, batches) = read::read_row_groups(file, &row_groups).map_err(to_df_err)?;
        // One partition holding the surviving row groups' batches, in row-group order.
        let exec = MemorySourceConfig::try_new_exec(&[batches], self.schema.clone(), projection.cloned())?;
        Ok(exec)
    }
}
