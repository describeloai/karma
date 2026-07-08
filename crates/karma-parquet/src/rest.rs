//! A [`SnapshotResolver`] backed by an **Iceberg REST catalog** (Lakekeeper). Feature
//! `rest-catalog`.
//!
//! This is the elegant consumption of Lakekeeper: iceberg-rust does *all* the Iceberg
//! mechanics — REST `loadTable`, snapshot selection, manifest-list + manifest traversal
//! — and hands us the **data-file paths** of the table's current snapshot. We then read
//! those Parquet files ourselves, over the object store, with Karma's zone-map/bloom
//! pruning ([`crate::SnapshotTable`]). We reimplement none of Iceberg's metadata; the
//! only thing that crosses the boundary is a list of file paths.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use futures::TryStreamExt;
use iceberg::scan::FileScanTask;
use iceberg::{Catalog, CatalogBuilder, TableIdent};
use iceberg_catalog_rest::{RestCatalog, RestCatalogBuilder, REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE};
use object_store::{path::Path as ObjPath, ObjectStore};

use crate::{KarmaParquetError, ResolvedSnapshot, SnapshotResolver};

fn ice(e: impl std::fmt::Display) -> KarmaParquetError {
    KarmaParquetError::Iceberg(e.to_string())
}

/// Resolves `namespace.table` to its current snapshot's data files via a Lakekeeper
/// Iceberg REST catalog. The `store` reads the data files (build it with
/// [`crate::s3_store`] pointed at the same warehouse bucket).
pub struct RestResolver {
    catalog: RestCatalog,
    store: Arc<dyn ObjectStore>,
}

impl RestResolver {
    /// Connect to the REST catalog at `uri` for `warehouse` (e.g. `s3://lakehouse`),
    /// reading data files through `store`.
    pub async fn connect(
        uri: impl Into<String>,
        warehouse: impl Into<String>,
        store: Arc<dyn ObjectStore>,
    ) -> Result<Self, KarmaParquetError> {
        let catalog = RestCatalogBuilder::default()
            .load(
                "rest",
                HashMap::from([
                    (REST_CATALOG_PROP_URI.to_string(), uri.into()),
                    (REST_CATALOG_PROP_WAREHOUSE.to_string(), warehouse.into()),
                ]),
            )
            .await
            .map_err(ice)?;
        Ok(Self { catalog, store })
    }

    /// Resolve directly to a ready [`crate::SnapshotTable`], indexing `index_fields`.
    pub async fn table(
        &self,
        table: &str,
        index_fields: &[crate::IndexField],
    ) -> Result<crate::SnapshotTable, KarmaParquetError> {
        let resolved = self.resolve(table).await?;
        crate::SnapshotTable::from_resolved(resolved, index_fields).await
    }
}

#[async_trait]
impl SnapshotResolver for RestResolver {
    async fn resolve(&self, table: &str) -> Result<ResolvedSnapshot, KarmaParquetError> {
        // `table` is a dotted identifier: `namespace[.sub…].table`.
        let ident = TableIdent::from_strs(table.split('.')).map_err(ice)?;
        let tbl = self.catalog.load_table(&ident).await.map_err(ice)?;

        // iceberg-rust plans the current snapshot's data files (manifest traversal).
        let scan = tbl.scan().select_all().build().map_err(ice)?;
        let tasks: Vec<FileScanTask> = scan.plan_files().await.map_err(ice)?.try_collect().await.map_err(ice)?;

        let files = tasks
            .iter()
            .map(|t| uri_to_object_path(&t.data_file_path))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ResolvedSnapshot { store: self.store.clone(), files })
    }
}

/// Turn an Iceberg data-file location (`s3://bucket/key…`) into the object-store key
/// (`key…`) — the store is already scoped to the bucket.
fn uri_to_object_path(uri: &str) -> Result<ObjPath, KarmaParquetError> {
    let after_scheme = uri
        .strip_prefix("s3://")
        .or_else(|| uri.strip_prefix("s3a://"))
        .unwrap_or(uri);
    let key = after_scheme
        .split_once('/')
        .map(|(_bucket, key)| key)
        .ok_or_else(|| KarmaParquetError::Config(format!("cannot parse object key from {uri}")))?;
    Ok(ObjPath::from(key))
}
