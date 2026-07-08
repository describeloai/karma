//! Object-store configuration — build an `Arc<dyn ObjectStore>` for the S3-compatible
//! store the Iceberg warehouse lives in (Cloudflare R2, Supabase S3, AWS S3). The
//! read-path ([`crate::read::read_row_groups_object`]) and sidecar build
//! ([`crate::build::build_sidecar_object`]) then fetch only the surviving row groups
//! from it — the I/O skip, on the wire.

use std::sync::Arc;

use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;

use crate::KarmaParquetError;

/// Connection config for an S3-compatible object store (R2 / Supabase / AWS).
#[derive(Clone, Debug)]
pub struct S3Config {
    /// e.g. `https://<account>.r2.cloudflarestorage.com` (R2).
    pub endpoint: String,
    pub bucket: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    /// `"auto"` for Cloudflare R2.
    pub region: String,
}

impl S3Config {
    /// Read the config from the Carbon lakehouse env vars (`LAKEHOUSE_S3_*`), so a
    /// karma engine points at the same warehouse the dual-write fills.
    pub fn from_env() -> Result<Self, KarmaParquetError> {
        let need = |k: &str| std::env::var(k).map_err(|_| KarmaParquetError::Config(format!("missing env {k}")));
        Ok(Self {
            endpoint: need("LAKEHOUSE_S3_ENDPOINT")?,
            bucket: std::env::var("LAKEHOUSE_S3_BUCKET").unwrap_or_else(|_| "lakehouse".into()),
            access_key_id: need("LAKEHOUSE_S3_ACCESS_KEY_ID")?,
            secret_access_key: need("LAKEHOUSE_S3_SECRET_ACCESS_KEY")?,
            region: std::env::var("LAKEHOUSE_S3_REGION").unwrap_or_else(|_| "auto".into()),
        })
    }
}

/// Build an `Arc<dyn ObjectStore>` for `cfg`, using **path-style** addressing (which
/// S3-compatibles like R2/Supabase require).
pub fn s3_store(cfg: &S3Config) -> Result<Arc<dyn ObjectStore>, KarmaParquetError> {
    let store = AmazonS3Builder::new()
        .with_endpoint(&cfg.endpoint)
        .with_bucket_name(&cfg.bucket)
        .with_access_key_id(&cfg.access_key_id)
        .with_secret_access_key(&cfg.secret_access_key)
        .with_region(&cfg.region)
        .with_virtual_hosted_style_request(false)
        .build()?;
    Ok(Arc::new(store))
}
