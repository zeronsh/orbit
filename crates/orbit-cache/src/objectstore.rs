//! Pluggable object storage for replica snapshots.
//!
//! The replicator periodically writes a [`ReplicaSnapshot`] (every table's rows +
//! the WAL LSN it reflects). A view-syncer restores the latest snapshot on boot
//! instead of re-syncing the whole dataset from Postgres, then catches up via the
//! change-stream. [`LocalObjectStore`] (filesystem) backs tests and shared-volume
//! deployments; an S3/Tigris impl lives behind the `s3` feature.

use anyhow::{Context, Result};
use bytes::Bytes;
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use oql::value::Row;
use std::path::{Path, PathBuf};

/// A fallible stream of byte chunks (streaming put/get payloads).
pub type ByteStream = futures_util::stream::BoxStream<'static, Result<Bytes>>;

/// A key/value blob store. Implemented by a local filesystem and (with the `s3`
/// feature) any S3-compatible service such as Tigris.
///
/// `put`/`get` buffer whole objects — fine for small metadata. Large objects
/// (SQLite-file snapshots) go through `put_stream`/`get_stream`, which bound
/// memory to O(part_size) regardless of object size.
#[allow(async_fn_in_trait)]
pub trait ObjectStore {
    async fn put(&self, key: &str, bytes: Vec<u8>) -> Result<()>;
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>>;
    /// Stream `data` into `key` with bounded memory. `part_size` bounds
    /// per-part buffering (S3 multipart parts; local write chunks) — peak
    /// memory is ~2× `part_size` (the part being filled + one in flight).
    async fn put_stream(&self, key: &str, data: ByteStream, part_size: usize) -> Result<()>;
    /// Stream the object at `key` (`None` if absent).
    async fn get_stream(&self, key: &str) -> Result<Option<ByteStream>>;
    /// Delete the object at `key` (no error if absent). Used to garbage-collect
    /// retired backup generations.
    async fn delete(&self, key: &str) -> Result<()>;
}

/// Upload the file at `path` to `key`, streamed with `part_size` read buffers.
pub async fn put_file<O: ObjectStore>(
    store: &O,
    key: &str,
    path: &Path,
    part_size: usize,
) -> Result<()> {
    let file = tokio::fs::File::open(path).await.with_context(|| format!("open {path:?}"))?;
    let stream = tokio_util::io::ReaderStream::with_capacity(file, part_size.clamp(64 * 1024, 8 << 20))
        .map_err(anyhow::Error::from)
        .boxed();
    store.put_stream(key, stream, part_size).await
}

/// Download `key` into the file at `dest`, chunk by chunk, then fsync.
/// `Ok(false)` when the key doesn't exist. On any error the partial file is
/// removed (callers may also write to a tmp path and rename).
pub async fn get_to_file<O: ObjectStore>(store: &O, key: &str, dest: &Path) -> Result<bool> {
    let Some(mut stream) = store.get_stream(key).await? else {
        return Ok(false);
    };
    if let Some(p) = dest.parent() {
        tokio::fs::create_dir_all(p).await.ok();
    }
    let result: Result<()> = async {
        let mut f = tokio::fs::File::create(dest).await.with_context(|| format!("create {dest:?}"))?;
        while let Some(chunk) = stream.try_next().await? {
            tokio::io::AsyncWriteExt::write_all(&mut f, &chunk).await?;
        }
        f.sync_all().await?;
        Ok(())
    }
    .await;
    if result.is_err() {
        let _ = tokio::fs::remove_file(dest).await;
    }
    result.map(|_| true)
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

impl LocalObjectStore {
    /// Stream `data` into `key` via the temp-file + fsync + rename dance:
    /// readers never see a torn snapshot, and a power loss can't rename a
    /// not-yet-flushed file into place (rename is only atomic for data the
    /// filesystem has persisted). Unique tmp name: two writers (deploy
    /// overlap — the departing and the new replicator both snapshot) must not
    /// interleave into one tmp file.
    async fn put_stream_impl(&self, key: &str, mut data: ByteStream) -> Result<()> {
        let path = self.root.join(key);
        if let Some(p) = path.parent() {
            tokio::fs::create_dir_all(p).await.ok();
        }
        let unique = format!(
            "writing.{}.{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        );
        let tmp = path.with_extension(unique);
        let write = async {
            let mut f = tokio::fs::File::create(&tmp).await.with_context(|| format!("create {tmp:?}"))?;
            while let Some(chunk) = data.try_next().await? {
                tokio::io::AsyncWriteExt::write_all(&mut f, &chunk)
                    .await
                    .with_context(|| format!("write {tmp:?}"))?;
            }
            f.sync_all().await.with_context(|| format!("fsync {tmp:?}"))?;
            Ok::<_, anyhow::Error>(())
        }
        .await;
        if write.is_err() {
            let _ = tokio::fs::remove_file(&tmp).await;
            return write;
        }
        tokio::fs::rename(&tmp, &path).await.with_context(|| format!("rename into {path:?}"))?;
        Ok(())
    }
}

impl ObjectStore for LocalObjectStore {
    async fn put(&self, key: &str, bytes: Vec<u8>) -> Result<()> {
        let one = futures_util::stream::once(async move { Ok(Bytes::from(bytes)) }).boxed();
        self.put_stream_impl(key, one).await
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        match tokio::fs::read(self.root.join(key)).await {
            Ok(b) => Ok(Some(b)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e).context("read object"),
        }
    }

    async fn put_stream(&self, key: &str, data: ByteStream, _part_size: usize) -> Result<()> {
        self.put_stream_impl(key, data).await
    }

    async fn get_stream(&self, key: &str) -> Result<Option<ByteStream>> {
        match tokio::fs::File::open(self.root.join(key)).await {
            Ok(f) => Ok(Some(
                tokio_util::io::ReaderStream::with_capacity(f, 1 << 20)
                    .map_err(anyhow::Error::from)
                    .boxed(),
            )),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e).context("open object"),
        }
    }

    async fn delete(&self, key: &str) -> Result<()> {
        match tokio::fs::remove_file(self.root.join(key)).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e).context("delete object"),
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

    async fn put_stream(&self, key: &str, mut data: ByteStream, part_size: usize) -> Result<()> {
        use object_store::ObjectStore as _;
        // S3 multipart parts must be ≥ 5 MiB (except the last).
        let part_size = part_size.max(5 << 20);
        let upload = self
            .inner
            .put_multipart(&object_store::path::Path::from(key))
            .await
            .context("s3 start multipart")?;
        let mut w = object_store::WriteMultipart::new_with_chunk_size(upload, part_size);
        let result: Result<()> = async {
            while let Some(chunk) = data.try_next().await? {
                // Bound in-flight parts to 1: peak memory ≈ the part being
                // filled + one part uploading (~2 × part_size).
                w.wait_for_capacity(1).await.context("s3 multipart backpressure")?;
                w.write(&chunk);
            }
            Ok(())
        }
        .await;
        if let Err(e) = result {
            // Best effort: dropping the writer abandons the multipart upload;
            // incomplete uploads are reaped by bucket lifecycle rules.
            return Err(e);
        }
        w.finish().await.context("s3 finish multipart")?;
        Ok(())
    }

    async fn get_stream(&self, key: &str) -> Result<Option<ByteStream>> {
        use object_store::ObjectStore as _;
        match self.inner.get(&object_store::path::Path::from(key)).await {
            Ok(r) => Ok(Some(r.into_stream().map_err(anyhow::Error::from).boxed())),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e).context("s3 get"),
        }
    }

    async fn delete(&self, key: &str) -> Result<()> {
        use object_store::ObjectStore as _;
        match self.inner.delete(&object_store::path::Path::from(key)).await {
            Ok(()) => Ok(()),
            Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(e).context("s3 delete"),
        }
    }
}
