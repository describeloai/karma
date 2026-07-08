//! The ml-runner-backed table resolver + Karma's client for it.
//!
//! Carbon's Iceberg catalog is a PyIceberg **SqlCatalog** (Postgres) with no REST
//! endpoint, so Karma resolves a table's data files through the **ml-runner** — the
//! Python service that already owns the catalog. `POST /lakehouse/plan-files` returns the
//! current snapshot's data-file object paths; Karma reads those Parquet files itself from
//! R2 with its zone-map/bloom pruning. Only paths cross the boundary
//! (the Karma <-> lakehouse I/O contract).

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use datafusion::error::DataFusionError;
use object_store::{path::Path as ObjPath, ObjectStore};
use serde::Deserialize;

use karma_parquet::{IndexField, ResolvedSnapshot, SnapshotTable};
use karma_sql::TableResolver;

/// One referenced table's coordinate, as Carbon's SQL Editor sends it in the request.
#[derive(Clone, Debug, Deserialize)]
pub struct TableRef {
    pub dataset_id: String,
    #[serde(default = "default_namespace")]
    pub namespace: String,
}

fn default_namespace() -> String {
    "datasets".to_string()
}

/// HTTP client for the ml-runner's `/lakehouse/plan-files` (Bearer-token auth).
#[derive(Clone)]
pub struct MlRunnerClient {
    base: String,
    token: String,
    http: reqwest::Client,
}

#[derive(Deserialize)]
struct PlanFilesResponse {
    files: Vec<String>,
}

impl MlRunnerClient {
    pub fn new(base: impl Into<String>, token: impl Into<String>) -> Self {
        // trim(): env values pasted into a deploy UI often carry a trailing newline /
        // spaces / quotes, which make the URL or the Authorization header invalid
        // (reqwest "builder error"). Be defensive.
        Self {
            base: base.into().trim().trim_matches('"').trim_end_matches('/').to_string(),
            token: token.into().trim().trim_matches('"').to_string(),
            http: reqwest::Client::new(),
        }
    }

    /// Resolve a dataset to its current snapshot's data-file object paths.
    pub async fn plan_files(&self, namespace: &str, dataset_id: &str) -> Result<Vec<String>, String> {
        let url = format!("{}/lakehouse/plan-files", self.base);
        let resp = self
            .http
            .post(&url)
            .bearer_auth(&self.token)
            .json(&serde_json::json!({ "namespace": namespace, "dataset_id": dataset_id }))
            .send()
            .await
            .map_err(|e| {
                // Surface the underlying cause (invalid URL / header) — reqwest's Display
                // for a builder error is otherwise just "builder error".
                use std::error::Error;
                let mut msg = format!("plan-files POST {url} failed: {e}");
                let mut src = e.source();
                while let Some(s) = src {
                    msg.push_str(&format!(" | {s}"));
                    src = s.source();
                }
                msg
            })?;
        if !resp.status().is_success() {
            let code = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(format!("plan-files {code}: {}", body.chars().take(400).collect::<String>()));
        }
        let parsed: PlanFilesResponse = resp.json().await.map_err(|e| format!("plan-files bad json: {e}"))?;
        Ok(parsed.files)
    }
}

/// Turn an Iceberg data-file location (`s3://bucket/key…`) into the object-store key
/// (`key…`) — the store is already scoped to the bucket.
pub fn uri_to_object_path(uri: &str) -> Result<ObjPath, String> {
    let after = uri.strip_prefix("s3://").or_else(|| uri.strip_prefix("s3a://")).unwrap_or(uri);
    let key = after
        .split_once('/')
        .map(|(_bucket, key)| key)
        .ok_or_else(|| format!("cannot parse object key from {uri}"))?;
    Ok(ObjPath::from(key))
}

/// A request-scoped [`TableResolver`]: resolves each referenced table name to its Iceberg
/// snapshot — files listed by the ml-runner, read from `store` (R2). The `tables` map
/// (from the request) carries name → dataset coordinate; a name not in the map, or a
/// dataset with no snapshot, resolves to `None` (→ "table not found").
pub struct MlRunnerResolver {
    client: MlRunnerClient,
    store: Arc<dyn ObjectStore>,
    tables: HashMap<String, TableRef>,
    index_fields: Vec<IndexField>,
}

impl MlRunnerResolver {
    /// `tables` keys must be lowercased table names. v1 indexes nothing (correct, but
    /// unpruned — `SnapshotTable` keeps every row group; footer-stats zone maps are the
    /// pruning follow-on, per the Karma NEXT open question).
    pub fn new(client: MlRunnerClient, store: Arc<dyn ObjectStore>, tables: HashMap<String, TableRef>) -> Self {
        Self { client, store, tables, index_fields: Vec::new() }
    }
}

#[async_trait]
impl TableResolver for MlRunnerResolver {
    async fn resolve_table(&self, fqn: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        let leaf = fqn.rsplit('.').next().unwrap_or(fqn).to_lowercase();
        let Some(tref) = self.tables.get(&leaf) else {
            return Ok(None);
        };
        let paths = self
            .client
            .plan_files(&tref.namespace, &tref.dataset_id)
            .await
            .map_err(|e| DataFusionError::External(e.into()))?;
        if paths.is_empty() {
            return Ok(None);
        }
        let files = paths
            .iter()
            .map(|p| uri_to_object_path(p))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| DataFusionError::External(e.into()))?;
        let resolved = ResolvedSnapshot { store: self.store.clone(), files };
        let table = SnapshotTable::from_resolved(resolved, &self.index_fields)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(Some(Arc::new(table)))
    }
}

#[cfg(test)]
mod tests {
    use super::uri_to_object_path;

    #[test]
    fn strips_scheme_and_bucket() {
        assert_eq!(
            uri_to_object_path("s3://lakehouse/datasets/ds_x/data/a.parquet").unwrap().as_ref(),
            "datasets/ds_x/data/a.parquet"
        );
        assert_eq!(
            uri_to_object_path("s3a://lakehouse/datasets/ds_x/b.parquet").unwrap().as_ref(),
            "datasets/ds_x/b.parquet"
        );
    }
}
