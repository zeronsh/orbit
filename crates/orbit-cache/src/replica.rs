//! [`Replica`]: the local materialized copy of the upstream tables, as a set of
//! OQL [`MemorySource`]s, updated by applying [`LogicalEvent`]s.
//!
//! In Zero the replica is SQLite plus a change-log; here we feed changes
//! straight into the in-memory IVM sources. The mapping is the interesting part:
//! Postgres insert/update/delete become OQL [`SourceChange`]s, which
//! [`source_push`] propagates through every materialized query built over the
//! source.

use crate::pg::pgoutput::LogicalEvent;
use oql::ivm::{source_push, ColumnType, MemorySource, SourceChange};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::rc::Rc;

/// The behavior `run_server` needs from a replica, regardless of backend
/// (in-memory [`Replica`] or [`SqliteReplica`](crate::sqlite_source::SqliteReplica)).
pub trait ReplicaBackend: oql::SourceProvider {
    /// Apply a decoded replication event (idempotently).
    fn apply(&self, event: LogicalEvent);
    /// Seed a row during initial sync (no change propagation).
    fn seed(&self, table: &str, row: oql::value::Row);
    /// The declared columns of `table`, for the initial-sync SELECT.
    fn table_columns(&self, table: &str) -> Vec<(String, ColumnType)>;
    /// All rows of every table — for snapshotting the replica to object storage
    /// (view-syncer nodes restore from this instead of re-syncing from Postgres).
    fn snapshot(&self) -> Vec<(String, Vec<oql::value::Row>)>;

    // --- Durability hooks (no-ops for the in-memory replica) ---------------

    /// Start of a replication transaction: a durable backend opens a storage
    /// transaction so the whole upstream transaction commits atomically (a
    /// crash mid-transaction rolls back instead of persisting a torn half).
    fn begin_txn(&self) {}
    /// End of a replication transaction. `lsn` is the upstream position this
    /// replica follows (the WAL commit LSN; 0 for a view-syncer applying the
    /// change-stream), `pos` the replicator change-stream sequence of the
    /// commit (0 outside cluster mode). A durable backend records both as its
    /// resume watermark **inside** the same storage transaction, then commits —
    /// so the watermark can never disagree with the data it describes.
    fn commit_txn(&self, _lsn: u64, _pos: u64) {}
    /// The durably-recorded resume point from a previous run, if any. `Some`
    /// lets the server skip the full initial sync and resume from the slot.
    fn resume_watermark(&self) -> Option<u64> {
        None
    }
    /// The durably-recorded change-stream position from a previous run, if any
    /// (cluster resume: replicator seq continuity / view-syncer delta resume).
    fn resume_pos(&self) -> Option<u64> {
        None
    }
    /// Reset all replicated data before a fresh initial sync. A durable backend
    /// must drop stale rows here — initial sync only upserts, so rows deleted
    /// upstream while the server was offline would otherwise survive as
    /// phantoms.
    fn start_fresh(&self) {}

    /// A point-in-time memory/size sample for the metrics exporter.
    fn metrics_sample(&self) -> ReplicaSample {
        ReplicaSample::default()
    }
}

/// What a replica reports to the metrics sampler. Fields a backend can't
/// cheaply measure stay 0.
#[derive(Default, Clone, Copy, Debug)]
pub struct ReplicaSample {
    pub rows: u64,
    /// Estimated logical bytes of stored rows (in-memory backend).
    pub logical_bytes: u64,
    /// On-disk size of the SQLite database (SQLite backend).
    pub file_bytes: u64,
}

/// A registry of replicated tables.
#[derive(Default)]
pub struct Replica {
    sources: HashMap<String, Rc<RefCell<MemorySource>>>,
    columns: HashMap<String, Vec<(String, ColumnType)>>,
}

impl Replica {
    pub fn new() -> Self {
        Replica::default()
    }

    /// Register a table and create its backing source.
    pub fn add_table(
        &mut self,
        name: &str,
        columns: BTreeMap<String, ColumnType>,
        primary_key: Vec<String>,
    ) -> Rc<RefCell<MemorySource>> {
        self.columns
            .insert(name.to_string(), columns.iter().map(|(k, v)| (k.clone(), *v)).collect());
        let src = MemorySource::new(name, columns, primary_key);
        self.sources.insert(name.to_string(), Rc::clone(&src));
        src
    }

    pub fn source(&self, name: &str) -> Option<Rc<RefCell<MemorySource>>> {
        self.sources.get(name).map(Rc::clone)
    }

    /// Apply a decoded replication event to the corresponding source, pushing it
    /// through the IVM pipelines. Events for unregistered tables are ignored.
    ///
    /// Application is **idempotent** so the initial-snapshot/stream overlap (a
    /// change between slot creation and the snapshot SELECT is re-delivered) is
    /// handled with at-least-once semantics: inserts of existing rows become
    /// edits, deletes of missing rows are skipped, updates of missing rows
    /// become inserts. Old rows for edit/remove are taken from current state so
    /// the source preconditions always hold.
    pub fn apply(&self, event: LogicalEvent) {
        match event {
            LogicalEvent::Insert { table, row } => {
                if let Some(src) = self.sources.get(&table) {
                    let existing = src.borrow().lookup(&row);
                    match existing {
                        None => source_push(src, SourceChange::Add(row)),
                        Some(old) => source_push(src, SourceChange::Edit { row, old_row: old }),
                    }
                }
            }
            LogicalEvent::Delete { table, old_row } => {
                if let Some(src) = self.sources.get(&table) {
                    // Bind in its own statement so the `borrow()` is released
                    // before `source_push` re-borrows.
                    let stored = src.borrow().lookup(&old_row);
                    if let Some(stored) = stored {
                        source_push(src, SourceChange::Remove(stored));
                    }
                }
            }
            LogicalEvent::Update { table, row, old_row } => {
                if let Some(src) = self.sources.get(&table) {
                    // Use the actually-stored row as the edit's old row (handles
                    // missing REPLICA IDENTITY and snapshot overlap).
                    let key = old_row.as_ref().unwrap_or(&row);
                    let existing = src.borrow().lookup(key);
                    match existing {
                        Some(old) => source_push(src, SourceChange::Edit { row, old_row: old }),
                        None => source_push(src, SourceChange::Add(row)),
                    }
                }
            }
            LogicalEvent::Relation { table, columns } => {
                if let Some(src) = self.sources.get(&table) {
                    src.borrow_mut().reconcile_columns(&columns);
                }
            }
            LogicalEvent::Begin | LogicalEvent::Commit | LogicalEvent::Other => {}
        }
    }
}

impl ReplicaBackend for Replica {
    fn apply(&self, event: LogicalEvent) {
        Replica::apply(self, event)
    }
    fn seed(&self, table: &str, row: oql::value::Row) {
        if let Some(src) = self.sources.get(table) {
            src.borrow_mut().insert_initial(row);
        }
    }
    fn table_columns(&self, table: &str) -> Vec<(String, ColumnType)> {
        self.columns.get(table).cloned().unwrap_or_default()
    }
    fn snapshot(&self) -> Vec<(String, Vec<oql::value::Row>)> {
        self.sources
            .iter()
            .map(|(name, src)| (name.clone(), src.borrow().all_rows()))
            .collect()
    }
    fn metrics_sample(&self) -> ReplicaSample {
        let mut s = ReplicaSample::default();
        for src in self.sources.values() {
            let (rows, bytes) = src.borrow().estimated_bytes();
            s.rows += rows as u64;
            s.logical_bytes += bytes as u64;
        }
        s
    }
}
