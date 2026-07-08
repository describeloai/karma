//! The lazy, resolver-backed catalog — how the front binds `catalog.schema.table`
//! (FQN addressing, Block 2 §1) to a live Iceberg snapshot.
//!
//! DataFusion resolves a table reference by navigating a [`CatalogProviderList`] →
//! [`CatalogProvider`] → [`SchemaProvider`]. Our providers construct themselves lazily
//! for **any** name and, at the leaf, resolve the full `catalog.schema.table` through a
//! [`TableResolver`] — reading it as a Karma-pruned table. The whole `a.b.c` path
//! reaches the resolver, so bare refs (default `datafusion.public.*`) and qualified refs
//! (`main.default.*`) are handled by one rule.
//!
//! **Tenancy** lives here: the resolver is user-scoped, so a table the user can't see
//! simply doesn't resolve (`Ok(None)` → DataFusion's "table not found"). The engine has
//! no path to a table except through the resolver.
//!
//! **Caching** is shared at the list root (an `Arc<Mutex<..>>` every lazily-created
//! provider clones), so a table resolved once — its sidecar built once, a full Parquet
//! read — is reused across the Substrait producer/consumer passes and across queries.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, CatalogProviderList, SchemaProvider, TableProvider};
use datafusion::error::DataFusionError;

use karma_parquet::{IndexField, SnapshotResolver, SnapshotTable};

/// Resolve a fully-qualified `catalog.schema.table` to a live [`TableProvider`], or
/// `None` if it doesn't exist / the caller can't see it (tenancy). `Err` is reserved for
/// real failures (I/O, corrupt table), never mere absence.
///
/// This is the front's catalog boundary. The prod implementation is
/// [`SnapshotTableResolver`] (Lakekeeper → Karma `SnapshotTable`); tests inject a
/// resolver returning observed tables to assert pruning.
#[async_trait]
pub trait TableResolver: Send + Sync {
    async fn resolve_table(&self, fqn: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError>;
}

/// Which columns to index (zone map / bloom) when a table is first read — per-table,
/// since schemas differ. Prod derives this from the dataset schema (or, per the open
/// question, from Parquet footer stats to avoid a full read); tests supply a fixed set.
pub trait IndexStrategy: Send + Sync {
    fn fields_for(&self, table: &str) -> Vec<IndexField>;
}

/// Index nothing — always correct (DataFusion applies the real filter), just no pruning
/// speed-up. The safe default.
#[derive(Debug, Default)]
pub struct NoIndex;

impl IndexStrategy for NoIndex {
    fn fields_for(&self, _table: &str) -> Vec<IndexField> {
        Vec::new()
    }
}

/// One index specification applied to every table (tests / uniform schemas).
#[derive(Debug)]
pub struct FixedIndex(pub Vec<IndexField>);

impl IndexStrategy for FixedIndex {
    fn fields_for(&self, _table: &str) -> Vec<IndexField> {
        self.0.clone()
    }
}

/// The prod [`TableResolver`]: a [`SnapshotResolver`] (Lakekeeper REST) + an index
/// strategy → a Karma-pruned [`SnapshotTable`] per table. A resolve failure is treated
/// as "table not found" (`Ok(None)`), which also models tenancy: an unowned table is
/// invisible rather than an error.
pub struct SnapshotTableResolver {
    resolver: Arc<dyn SnapshotResolver>,
    index: Arc<dyn IndexStrategy>,
}

impl SnapshotTableResolver {
    pub fn new(resolver: Arc<dyn SnapshotResolver>, index: Arc<dyn IndexStrategy>) -> Self {
        Self { resolver, index }
    }
}

impl fmt::Debug for SnapshotTableResolver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SnapshotTableResolver").finish_non_exhaustive()
    }
}

#[async_trait]
impl TableResolver for SnapshotTableResolver {
    async fn resolve_table(&self, fqn: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        let resolved = match self.resolver.resolve(fqn).await {
            Ok(r) => r,
            Err(_) => return Ok(None),
        };
        let fields = self.index.fields_for(fqn);
        let table = SnapshotTable::from_resolved(resolved, &fields)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Some(Arc::new(table)))
    }
}

/// State shared by every lazily-created provider (Arc-cloned down the tree): the
/// resolver and the resolved-table cache.
struct Shared {
    resolver: Arc<dyn TableResolver>,
    cache: Mutex<HashMap<String, Arc<dyn TableProvider>>>,
}

impl fmt::Debug for Shared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Shared")
            .field("cached_tables", &self.cache.lock().unwrap().len())
            .finish()
    }
}

/// The catalog list DataFusion consults. Serves a lazy [`KarmaCatalog`] for any catalog
/// name; what actually exists is decided by the resolver at the leaf.
#[derive(Debug)]
pub struct KarmaCatalogList {
    shared: Arc<Shared>,
}

impl KarmaCatalogList {
    pub fn new(resolver: Arc<dyn TableResolver>) -> Self {
        Self {
            shared: Arc::new(Shared { resolver, cache: Mutex::new(HashMap::new()) }),
        }
    }
}

impl CatalogProviderList for KarmaCatalogList {
    fn register_catalog(
        &self,
        _name: String,
        _catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        None // the catalog is resolver-driven, not registration-driven
    }
    fn catalog_names(&self) -> Vec<String> {
        Vec::new() // lazy: we do not enumerate the catalog space
    }
    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        Some(Arc::new(KarmaCatalog { catalog: name.to_string(), shared: self.shared.clone() }))
    }
}

#[derive(Debug)]
struct KarmaCatalog {
    catalog: String,
    shared: Arc<Shared>,
}

impl CatalogProvider for KarmaCatalog {
    fn schema_names(&self) -> Vec<String> {
        Vec::new()
    }
    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        Some(Arc::new(KarmaSchema {
            catalog: self.catalog.clone(),
            schema: name.to_string(),
            shared: self.shared.clone(),
        }))
    }
}

#[derive(Debug)]
struct KarmaSchema {
    catalog: String,
    schema: String,
    shared: Arc<Shared>,
}

#[async_trait]
impl SchemaProvider for KarmaSchema {
    fn table_names(&self) -> Vec<String> {
        // Best-effort: the tables resolved so far under this catalog.schema.
        let prefix = format!("{}.{}.", self.catalog, self.schema);
        self.shared
            .cache
            .lock()
            .unwrap()
            .keys()
            .filter_map(|k| k.strip_prefix(&prefix).map(str::to_string))
            .collect()
    }
    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        let full = format!("{}.{}.{}", self.catalog, self.schema, name);
        if let Some(t) = self.shared.cache.lock().unwrap().get(&full) {
            return Ok(Some(t.clone()));
        }
        let Some(provider) = self.shared.resolver.resolve_table(&full).await? else {
            return Ok(None);
        };
        self.shared.cache.lock().unwrap().insert(full, provider.clone());
        Ok(Some(provider))
    }
    fn table_exist(&self, _name: &str) -> bool {
        // Optimistic: the authoritative check is the async `table()`. Read-only planning
        // never gates on this for a plain SELECT.
        true
    }
}
