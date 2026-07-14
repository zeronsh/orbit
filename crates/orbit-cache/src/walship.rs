//! Incremental replica backup: **WAL-segment shipping** (litestream-style).
//!
//! The full-file snapshot path re-uploads the entire replica every interval —
//! a 50 GB replica re-ships 50 GB per cycle. This module ships only the bytes
//! SQLite itself appended to the WAL since the last cycle:
//!
//! * A **generation** starts with a `wal_checkpoint(TRUNCATE)` and one full
//!   upload of the (now WAL-free) database file. With auto-checkpointing
//!   disabled, the main db file then stays byte-stable until the *next*
//!   generation — every subsequent change lives only in the WAL.
//! * Each backup cycle reads the WAL's new bytes **up to the last committed
//!   frame** (frame salts must match the WAL header; a commit frame carries a
//!   nonzero db-size) and uploads them as one contiguous segment object.
//! * A tiny JSON **manifest** is atomically overwritten after every upload:
//!   the current generation's base object, its ordered segment list, and a
//!   resume-position hint.
//! * **Restore** downloads the base file, concatenates the segments into a
//!   `-wal` sidecar, and lets SQLite's own recovery fold them in (the bytes
//!   are verbatim prefixes of a real WAL, so salts/checksums verify).
//!
//! The previous generation is kept until the next roll (a view-syncer may be
//! mid-download of the old manifest); the one before that is deleted.

use crate::objectstore::{get_to_file, put_file, ObjectStore};
use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// The atomically-overwritten backup pointer.
pub const MANIFEST_KEY: &str = "backup/manifest.json";

/// One uploaded WAL segment: `offset..offset+len` of the generation's WAL.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentMeta {
    pub key: String,
    pub offset: u64,
    pub len: u64,
}

/// The backup pointer: everything needed to reassemble the replica.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupManifest {
    pub version: u32,
    pub generation: String,
    pub base_key: String,
    /// Ordered, contiguous WAL segments (offset 0 includes the WAL header).
    pub segments: Vec<SegmentMeta>,
    /// Advisory change-stream position at the last upload (the authoritative
    /// position rides inside the database itself).
    pub pos_hint: u64,
    /// Keys of the *previous* generation — kept until the next roll so an
    /// in-flight restore of the old manifest still finds its objects.
    #[serde(default)]
    pub previous: Vec<String>,
}

/// Shipping state for the current generation (replicator-side, in memory).
pub struct ShipState {
    pub generation: String,
    /// Bytes of the WAL file already uploaded (0 = nothing; the first segment
    /// starts at 0 and includes the 32-byte WAL header).
    pub shipped_offset: u64,
    /// WAL header salts of this generation (learned from the first scan). A
    /// mismatch later means the WAL was reset behind our back → roll.
    pub salts: Option<(u32, u32)>,
    manifest: BackupManifest,
}

/// Outcome of a ship cycle.
pub enum ShipOutcome {
    /// Uploaded `bytes` of new WAL (0 = nothing new).
    Shipped { bytes: u64 },
    /// The WAL was reset/restarted outside our control — caller must roll a
    /// new generation.
    NeedsNewGeneration,
}

// --- SQLite file-format helpers -------------------------------------------------

/// Read the database page size from the main db file header.
pub fn read_page_size(db: &Path) -> Result<u32> {
    let mut buf = [0u8; 18];
    let data = std::fs::read(db).with_context(|| format!("read {db:?}"))?;
    if data.len() < 18 {
        bail!("db file too short");
    }
    buf.copy_from_slice(&data[..18]);
    let raw = u16::from_be_bytes([buf[16], buf[17]]);
    Ok(if raw == 1 { 65536 } else { raw as u32 })
}

/// Result of scanning a WAL file for its committed prefix.
pub struct WalScan {
    /// Byte offset just past the last committed frame (≥ 32 when any frame is
    /// committed; 0 for an empty/absent WAL).
    pub committed_len: u64,
    pub salts: (u32, u32),
}

/// Scan `wal` and return the length of its committed prefix. Frames whose
/// salts differ from the header (pre-restart leftovers) or that follow the
/// last commit frame (uncommitted tail) are excluded.
pub fn scan_wal(wal: &Path, page_size: u32) -> Result<Option<WalScan>> {
    let data = match std::fs::read(wal) {
        Ok(d) => d,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e).with_context(|| format!("read {wal:?}")),
    };
    if data.len() < 32 {
        return Ok(None);
    }
    let u32at = |i: usize| u32::from_be_bytes(data[i..i + 4].try_into().unwrap());
    let magic = u32at(0);
    if magic != 0x377f_0682 && magic != 0x377f_0683 {
        bail!("bad WAL magic {magic:#x}");
    }
    let wal_page = u32at(8);
    if wal_page != page_size {
        bail!("WAL page size {wal_page} != db page size {page_size}");
    }
    let salts = (u32at(16), u32at(20));
    let frame = 24 + page_size as usize;
    let mut off = 32usize;
    let mut committed = 0u64;
    while off + frame <= data.len() {
        let db_size = u32at(off + 4);
        let fs1 = u32at(off + 8);
        let fs2 = u32at(off + 12);
        if (fs1, fs2) != salts {
            break; // stale frame from before a WAL restart
        }
        off += frame;
        if db_size != 0 {
            committed = off as u64; // commit frame: everything up to here is durable
        }
    }
    Ok(Some(WalScan { committed_len: committed.max(32), salts }))
}

// --- Shipping -------------------------------------------------------------------

fn gen_id() -> String {
    format!(
        "{}-{:x}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    )
}

fn base_key(generation: &str) -> String {
    format!("backup/gen-{generation}/base.db")
}
fn segment_key(generation: &str, offset: u64) -> String {
    format!("backup/gen-{generation}/wal/{offset:016x}.seg")
}

async fn put_manifest<O: ObjectStore>(store: &O, m: &BackupManifest) -> Result<()> {
    store
        .put(MANIFEST_KEY, serde_json::to_vec(m).expect("serialize manifest"))
        .await
        .context("upload backup manifest")
}

pub async fn load_manifest<O: ObjectStore>(store: &O) -> Result<Option<BackupManifest>> {
    match store.get(MANIFEST_KEY).await? {
        Some(bytes) => Ok(Some(serde_json::from_slice(&bytes).context("parse backup manifest")?)),
        None => Ok(None),
    }
}

/// Start a new generation: checkpoint the WAL into the main file (caller runs
/// the checkpoint — it owns the connection), upload the full db file, and
/// point the manifest at it. `prev_manifest` (if any) becomes the retained
/// previous generation; the generation *before that* is deleted.
pub async fn new_generation<O: ObjectStore>(
    store: &O,
    db: &Path,
    pos: u64,
    part_size: usize,
    prev: Option<&BackupManifest>,
) -> Result<ShipState> {
    let generation = gen_id();
    let base = base_key(&generation);
    put_file(store, &base, db, part_size).await.context("upload backup base")?;
    // Delete the grandparent generation's objects (the previous one is kept
    // for restores already in flight against the old manifest).
    if let Some(p) = prev {
        for key in &p.previous {
            let _ = store.delete(key).await;
        }
    }
    let previous = prev
        .map(|p| {
            let mut keys: Vec<String> = p.segments.iter().map(|s| s.key.clone()).collect();
            keys.push(p.base_key.clone());
            keys
        })
        .unwrap_or_default();
    let manifest = BackupManifest {
        version: 1,
        generation: generation.clone(),
        base_key: base,
        segments: Vec::new(),
        pos_hint: pos,
        previous,
    };
    put_manifest(store, &manifest).await?;
    Ok(ShipState { generation, shipped_offset: 0, salts: None, manifest })
}

/// Ship any newly-committed WAL bytes for the current generation.
pub async fn ship<O: ObjectStore>(
    store: &O,
    db: &Path,
    state: &mut ShipState,
    pos: u64,
) -> Result<ShipOutcome> {
    let wal = wal_path(db);
    let page_size = read_page_size(db)?;
    let scan = match scan_wal(&wal, page_size)? {
        Some(s) => s,
        None => return Ok(ShipOutcome::Shipped { bytes: 0 }), // no WAL yet
    };
    match state.salts {
        None => state.salts = Some(scan.salts),
        Some(salts) if salts != scan.salts => {
            // WAL restarted (checkpoint outside our control): our shipped
            // offsets no longer describe this file.
            return Ok(ShipOutcome::NeedsNewGeneration);
        }
        _ => {}
    }
    if scan.committed_len < state.shipped_offset {
        // File shrank without a salt change — shouldn't happen; be safe.
        return Ok(ShipOutcome::NeedsNewGeneration);
    }
    if scan.committed_len == state.shipped_offset || scan.committed_len <= 32 && state.shipped_offset > 0
    {
        return Ok(ShipOutcome::Shipped { bytes: 0 });
    }
    let start = state.shipped_offset;
    let end = scan.committed_len;
    if end == start {
        return Ok(ShipOutcome::Shipped { bytes: 0 });
    }
    // Read the committed byte range. std read is fine: segments are bounded by
    // the generation-roll WAL cap.
    let bytes = {
        use std::io::{Read, Seek, SeekFrom};
        let mut f = std::fs::File::open(&wal).with_context(|| format!("open {wal:?}"))?;
        f.seek(SeekFrom::Start(start))?;
        let mut buf = vec![0u8; (end - start) as usize];
        f.read_exact(&mut buf).context("read WAL segment")?;
        buf
    };
    // Defense: the WAL could have been reset between scan and read. Re-check
    // the salts on the header portion we're about to trust.
    if start == 0 && bytes.len() >= 24 {
        let s1 = u32::from_be_bytes(bytes[16..20].try_into().unwrap());
        let s2 = u32::from_be_bytes(bytes[20..24].try_into().unwrap());
        if (s1, s2) != scan.salts {
            return Ok(ShipOutcome::NeedsNewGeneration);
        }
    }
    let key = segment_key(&state.generation, start);
    let len = bytes.len() as u64;
    store.put(&key, bytes).await.context("upload WAL segment")?;
    state.shipped_offset = end;
    state.manifest.segments.push(SegmentMeta { key, offset: start, len });
    state.manifest.pos_hint = pos;
    put_manifest(store, &state.manifest).await?;
    Ok(ShipOutcome::Shipped { bytes: len })
}

pub fn wal_path(db: &Path) -> PathBuf {
    let mut p = db.to_path_buf().into_os_string();
    p.push("-wal");
    PathBuf::from(p)
}

// --- Restore --------------------------------------------------------------------

/// Restore the manifest's replica into `dest_db`: download the base file,
/// concatenate the WAL segments into `dest_db-wal`, and fold them in with a
/// real SQLite recovery + checkpoint. Returns `Ok(false)` when no manifest
/// exists (caller falls back to the legacy full-snapshot object).
pub async fn restore<O: ObjectStore>(store: &O, dest_db: &Path) -> Result<bool> {
    let Some(manifest) = load_manifest(store).await? else {
        return Ok(false);
    };
    if !get_to_file(store, &manifest.base_key, dest_db).await? {
        bail!("backup base {} missing (manifest raced a generation roll?)", manifest.base_key);
    }
    let wal = wal_path(dest_db);
    let _ = std::fs::remove_file(&wal);
    if !manifest.segments.is_empty() {
        // Verify contiguity before writing anything.
        let mut expect = manifest.segments[0].offset;
        if expect != 0 {
            bail!("first WAL segment starts at {expect}, not 0");
        }
        for s in &manifest.segments {
            if s.offset != expect {
                bail!("WAL segments not contiguous at {expect} (got {})", s.offset);
            }
            expect += s.len;
        }
        let mut assembled: Vec<u8> = Vec::with_capacity(expect as usize);
        for s in &manifest.segments {
            let Some(bytes) = store.get(&s.key).await? else {
                bail!("WAL segment {} missing (manifest raced a generation roll?)", s.key);
            };
            if bytes.len() as u64 != s.len {
                bail!("WAL segment {} length {} != manifest {}", s.key, bytes.len(), s.len);
            }
            assembled.extend_from_slice(&bytes);
        }
        std::fs::write(&wal, &assembled).with_context(|| format!("write {wal:?}"))?;
    }
    // Fold the WAL in with SQLite's own recovery, then checkpoint+truncate so
    // the result is a single self-contained file (and passes read-only
    // validation).
    let dest = dest_db.to_path_buf();
    tokio::task::spawn_blocking(move || -> Result<()> {
        let conn = rusqlite::Connection::open(&dest).context("open restored db")?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        let _ = conn.pragma_update(None, "journal_mode", "WAL");
        conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))
            .context("fold restored WAL")?;
        Ok(())
    })
    .await
    .map_err(|e| anyhow::anyhow!("restore fold task panicked: {e}"))??;
    let _ = std::fs::remove_file(&wal);
    let mut shm = dest_db.to_path_buf().into_os_string();
    shm.push("-shm");
    let _ = std::fs::remove_file(PathBuf::from(shm));
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::objectstore::LocalObjectStore;
    use crate::replica::ReplicaBackend;
    use crate::sqlite_source::SqliteReplica;
    use oql::ivm::ColumnType;
    use oql::value::Value;

    fn tmpdir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("orbit_walship_{tag}_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn replica_at(dir: &Path) -> SqliteReplica {
        let mut r = SqliteReplica::durable(dir);
        r.add_table(
            "t",
            vec![("id".into(), ColumnType::String), ("n".into(), ColumnType::Number)],
            vec!["id".into()],
        );
        r
    }

    fn insert(r: &SqliteReplica, id: &str, n: f64, lsn: u64) {
        r.begin_txn().unwrap();
        let mut row = oql::value::Row::new();
        row.insert("id", Value::String(id.into()));
        row.insert("n", Value::Number(n));
        r.apply(crate::LogicalEvent::Insert { table: "t".into(), row }).unwrap();
        r.commit_txn(lsn, lsn).unwrap();
    }

    fn rows_of(dir: &Path) -> Vec<(String, f64)> {
        let r = replica_at(dir);
        r.source("t")
            .unwrap()
            .borrow()
            .all_rows()
            .iter()
            .map(|row| {
                let id = match row.get("id") {
                    Some(Value::String(s)) => s.clone(),
                    other => panic!("bad id {other:?}"),
                };
                let n = match row.get("n") {
                    Some(Value::Number(n)) => *n,
                    other => panic!("bad n {other:?}"),
                };
                (id, n)
            })
            .collect()
    }

    #[tokio::test]
    async fn ship_and_restore_round_trip() {
        let src_dir = tmpdir("src");
        let dst_dir = tmpdir("dst");
        let store_dir = tmpdir("store");
        let store = LocalObjectStore::new(&store_dir);
        let db = src_dir.join("replica.db");

        let replica = replica_at(&src_dir);
        replica.set_wal_autocheckpoint(0).unwrap();
        insert(&replica, "a", 1.0, 10);

        // Generation 0: checkpoint + full base upload.
        replica.checkpoint_truncate().unwrap();
        let mut state = new_generation(&store, &db, 10, 1 << 20, None).await.unwrap();

        // Incremental cycle 1.
        insert(&replica, "b", 2.0, 11);
        let ShipOutcome::Shipped { bytes } = ship(&store, &db, &mut state, 11).await.unwrap()
        else {
            panic!("unexpected generation roll");
        };
        assert!(bytes > 0, "must ship the new WAL bytes");

        // Incremental cycle 2 (nothing new → zero bytes).
        let ShipOutcome::Shipped { bytes: b2 } = ship(&store, &db, &mut state, 11).await.unwrap()
        else {
            panic!()
        };
        assert_eq!(b2, 0);

        // Incremental cycle 3.
        insert(&replica, "c", 3.0, 12);
        let ShipOutcome::Shipped { bytes: b3 } = ship(&store, &db, &mut state, 12).await.unwrap()
        else {
            panic!()
        };
        assert!(b3 > 0);

        // Restore elsewhere: all three rows present, watermark carried.
        let dest = dst_dir.join("replica.db");
        assert!(restore(&store, &dest).await.unwrap());
        assert_eq!(rows_of(&dst_dir), vec![("a".into(), 1.0), ("b".into(), 2.0), ("c".into(), 3.0)]);
        let restored = replica_at(&dst_dir);
        assert_eq!(restored.resume_watermark(), Some(12));
        assert_eq!(restored.resume_pos(), Some(12));
    }

    #[tokio::test]
    async fn incremental_ships_far_less_than_full() {
        let src_dir = tmpdir("delta");
        let store_dir = tmpdir("delta_store");
        let store = LocalObjectStore::new(&store_dir);
        let db = src_dir.join("replica.db");

        let replica = replica_at(&src_dir);
        replica.set_wal_autocheckpoint(0).unwrap();
        // A "big" replica: 2000 rows.
        replica.begin_txn().unwrap();
        for i in 0..2000 {
            let mut row = oql::value::Row::new();
            row.insert("id", Value::String(format!("row{i:05}")));
            row.insert("n", Value::Number(i as f64));
            replica.apply(crate::LogicalEvent::Insert { table: "t".into(), row }).unwrap();
        }
        replica.commit_txn(1, 1).unwrap();
        replica.checkpoint_truncate().unwrap();
        let base_size = std::fs::metadata(&db).unwrap().len();
        let mut state = new_generation(&store, &db, 1, 1 << 20, None).await.unwrap();

        // One small change → the shipped delta must be a fraction of the base.
        insert(&replica, "zzz", 9.0, 2);
        let ShipOutcome::Shipped { bytes } = ship(&store, &db, &mut state, 2).await.unwrap()
        else {
            panic!()
        };
        assert!(bytes > 0);
        assert!(
            bytes * 4 < base_size,
            "delta ({bytes}B) must be much smaller than the full file ({base_size}B)"
        );
    }

    #[tokio::test]
    async fn wal_restart_triggers_generation_roll() {
        let src_dir = tmpdir("roll");
        let store_dir = tmpdir("roll_store");
        let store = LocalObjectStore::new(&store_dir);
        let db = src_dir.join("replica.db");

        let replica = replica_at(&src_dir);
        replica.set_wal_autocheckpoint(0).unwrap();
        insert(&replica, "a", 1.0, 1);
        replica.checkpoint_truncate().unwrap();
        let mut state = new_generation(&store, &db, 1, 1 << 20, None).await.unwrap();
        insert(&replica, "b", 2.0, 2);
        assert!(matches!(
            ship(&store, &db, &mut state, 2).await.unwrap(),
            ShipOutcome::Shipped { .. }
        ));

        // A checkpoint outside our control restarts the WAL (new salts).
        replica.checkpoint_truncate().unwrap();
        insert(&replica, "c", 3.0, 3);
        assert!(matches!(
            ship(&store, &db, &mut state, 3).await.unwrap(),
            ShipOutcome::NeedsNewGeneration
        ));

        // Roll and continue; restore sees everything.
        let prev = state.manifest.clone();
        replica.checkpoint_truncate().unwrap();
        let mut state = new_generation(&store, &db, 3, 1 << 20, Some(&prev)).await.unwrap();
        insert(&replica, "d", 4.0, 4);
        assert!(matches!(
            ship(&store, &db, &mut state, 4).await.unwrap(),
            ShipOutcome::Shipped { .. }
        ));

        let dst_dir = tmpdir("roll_dst");
        let dest = dst_dir.join("replica.db");
        assert!(restore(&store, &dest).await.unwrap());
        let ids: Vec<String> = rows_of(&dst_dir).into_iter().map(|(id, _)| id).collect();
        assert_eq!(ids, vec!["a", "b", "c", "d"]);
    }

    #[tokio::test]
    async fn restore_returns_false_without_manifest() {
        let store = LocalObjectStore::new(tmpdir("empty_store"));
        let dest = tmpdir("empty_dst").join("replica.db");
        assert!(!restore(&store, &dest).await.unwrap());
    }
}

impl ShipState {
    /// The manifest as last uploaded (for generation rolls).
    pub fn manifest(&self) -> &BackupManifest {
        &self.manifest
    }
}
