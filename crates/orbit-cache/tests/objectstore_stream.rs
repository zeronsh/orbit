//! Streaming object-store paths: multi-chunk put/get round-trips with bounded
//! buffers, missing-key behavior, and the file helpers used by SQLite-file
//! snapshots.

use bytes::Bytes;
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use orbit_cache::objectstore::{get_to_file, put_file, ByteStream};
use orbit_cache::{LocalObjectStore, ObjectStore};

fn tmp_dir(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("orbit_objstream_{tag}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn chunks(total: usize, chunk: usize) -> (Vec<u8>, ByteStream) {
    let data: Vec<u8> = (0..total).map(|i| (i % 251) as u8).collect();
    let stream = futures_util::stream::iter(
        data.chunks(chunk).map(|c| Ok(Bytes::copy_from_slice(c))).collect::<Vec<_>>(),
    )
    .boxed();
    (data, stream)
}

#[tokio::test]
async fn put_stream_get_stream_roundtrip_multi_chunk() {
    let root = tmp_dir("roundtrip");
    let store = LocalObjectStore::new(&root);

    // Many chunks, deliberately larger in total than the part size.
    let (data, stream) = chunks(3 * 1024 * 1024 + 17, 64 * 1024);
    store.put_stream("snap/latest.db", stream, 256 * 1024).await.unwrap();

    let mut got = Vec::new();
    let mut s = store.get_stream("snap/latest.db").await.unwrap().expect("object exists");
    while let Some(chunk) = s.try_next().await.unwrap() {
        got.extend_from_slice(&chunk);
    }
    assert_eq!(got, data, "byte-identical after streamed roundtrip");

    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn get_stream_missing_key_is_none() {
    let root = tmp_dir("missing");
    let store = LocalObjectStore::new(&root);
    assert!(store.get_stream("nope").await.unwrap().is_none());
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn put_file_get_to_file_roundtrip() {
    let root = tmp_dir("files");
    let store = LocalObjectStore::new(&root);

    let src = root.join("src.bin");
    let data: Vec<u8> = (0..2_000_000u32).map(|i| (i % 253) as u8).collect();
    std::fs::write(&src, &data).unwrap();

    put_file(&store, "obj/file.bin", &src, 128 * 1024).await.unwrap();

    let dest = root.join("dest.bin");
    assert!(get_to_file(&store, "obj/file.bin", &dest).await.unwrap());
    assert_eq!(std::fs::read(&dest).unwrap(), data, "byte-identical file transfer");

    assert!(!get_to_file(&store, "obj/absent.bin", &root.join("x")).await.unwrap());

    let _ = std::fs::remove_dir_all(&root);
}

/// A failing upload stream must not leave a torn object behind (tmp + rename).
#[tokio::test]
async fn failed_put_stream_leaves_no_object() {
    let root = tmp_dir("torn");
    let store = LocalObjectStore::new(&root);

    let stream: ByteStream = futures_util::stream::iter(vec![
        Ok(Bytes::from_static(b"partial")),
        Err(anyhow::anyhow!("source died")),
    ])
    .boxed();
    assert!(store.put_stream("snap/latest.db", stream, 1024).await.is_err());
    assert!(
        store.get("snap/latest.db").await.unwrap().is_none(),
        "no torn object visible after failed upload"
    );

    let _ = std::fs::remove_dir_all(&root);
}
