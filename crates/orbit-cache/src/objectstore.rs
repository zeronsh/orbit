//! Pluggable object storage for replica snapshots.
//!
//! The replicator periodically writes a [`ReplicaSnapshot`] (every table's rows +
//! the WAL LSN it reflects). A view-syncer restores the latest snapshot on boot
//! instead of re-syncing the whole dataset from Postgres, then catches up via the
//! change-stream. [`LocalObjectStore`] (filesystem) backs tests and shared-volume
//! deployments; an S3/Tigris impl lives behind the `s3` feature.

use anyhow::{Context, Result};
use oql::value::Row;
use std::path::PathBuf;

/// A key/value blob store. Implemented by a local filesystem and (with the `s3`
/// feature) any S3-compatible service such as Tigris.
#[allow(async_fn_in_trait)]
pub trait ObjectStore {
    async fn put(&self, key: &str, bytes: Vec<u8>) -> Result<()>;
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>>;
}

/// A full snapshot of the replica plus the change-stream position it reflects.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct ReplicaSnapshot {
    /// The replicator's change-stream sequence the snapshot reflects (resume point).
    pub pos: u64,
    pub tables: Vec<(String, Vec<Row>)>,
}

impl ReplicaSnapshot {
    /// The object key the replicator overwrites and view-syncers read.
    pub const KEY: &'static str = "snapshot/latest.json";

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("serialize snapshot")
    }
    pub fn from_bytes(b: &[u8]) -> Result<Self> {
        serde_json::from_slice(b).context("parse replica snapshot")
    }
}

/// Filesystem-backed object store (also usable across nodes via a shared volume).
pub struct LocalObjectStore {
    root: PathBuf,
}

impl LocalObjectStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        std::fs::create_dir_all(&root).ok();
        LocalObjectStore { root }
    }
}

impl ObjectStore for LocalObjectStore {
    async fn put(&self, key: &str, bytes: Vec<u8>) -> Result<()> {
        let path = self.root.join(key);
        if let Some(p) = path.parent() {
            tokio::fs::create_dir_all(p).await.ok();
        }
        // Write to a temp file then rename so readers never see a torn snapshot.
        let tmp = path.with_extension("writing");
        tokio::fs::write(&tmp, &bytes).await.with_context(|| format!("write {tmp:?}"))?;
        tokio::fs::rename(&tmp, &path).await.with_context(|| format!("rename into {path:?}"))?;
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        match tokio::fs::read(self.root.join(key)).await {
            Ok(b) => Ok(Some(b)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e).context("read object"),
        }
    }
}

/// An S3-compatible object store (AWS S3, **Tigris**, MinIO, …). Enable with the
/// `s3` feature. For Tigris, point `endpoint` at `https://t3.storage.dev`.
#[cfg(feature = "s3")]
pub struct S3ObjectStore {
    inner: object_store::aws::AmazonS3,
}

#[cfg(feature = "s3")]
impl S3ObjectStore {
    /// Build from explicit settings. `endpoint` is the S3 endpoint URL (for
    /// Tigris: `https://t3.storage.dev`); leave `None` for AWS S3.
    pub fn new(
        bucket: &str,
        endpoint: Option<&str>,
        region: &str,
        access_key_id: &str,
        secret_access_key: &str,
    ) -> Result<Self> {
        use object_store::aws::AmazonS3Builder;
        let mut b = AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_region(region)
            .with_access_key_id(access_key_id)
            .with_secret_access_key(secret_access_key);
        if let Some(ep) = endpoint {
            b = b.with_endpoint(ep).with_allow_http(ep.starts_with("http://"));
        }
        Ok(S3ObjectStore { inner: b.build().context("build S3 client")? })
    }

    /// Build from the standard env vars: `AWS_ACCESS_KEY_ID`,
    /// `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT_URL` (Tigris: `https://t3.storage.dev`),
    /// `AWS_REGION` (default `auto`), and `ORBIT_BUCKET`.
    pub fn from_env() -> Result<Self> {
        let bucket = std::env::var("ORBIT_BUCKET").context("ORBIT_BUCKET not set")?;
        let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "auto".into());
        let key = std::env::var("AWS_ACCESS_KEY_ID").context("AWS_ACCESS_KEY_ID not set")?;
        let secret = std::env::var("AWS_SECRET_ACCESS_KEY").context("AWS_SECRET_ACCESS_KEY not set")?;
        let endpoint = std::env::var("AWS_ENDPOINT_URL").ok();
        Self::new(&bucket, endpoint.as_deref(), &region, &key, &secret)
    }
}

#[cfg(feature = "s3")]
impl ObjectStore for S3ObjectStore {
    async fn put(&self, key: &str, bytes: Vec<u8>) -> Result<()> {
        use object_store::ObjectStore as _;
        self.inner
            .put(&object_store::path::Path::from(key), bytes.into())
            .await
            .context("s3 put")?;
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        use object_store::ObjectStore as _;
        match self.inner.get(&object_store::path::Path::from(key)).await {
            Ok(r) => Ok(Some(r.bytes().await.context("s3 read body")?.to_vec())),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e).context("s3 get"),
        }
    }
}
