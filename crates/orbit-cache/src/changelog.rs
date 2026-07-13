//! Durable change-log: a Postgres-backed, ordered log of every change the
//! replicator publishes. It lets view-syncers resume by **delta** across a
//! replicator restart (or after the in-memory ring evicts their resume point)
//! instead of re-restoring the whole replica.
//!
//! Performance: writes are batched on a background task fed by a **bounded**
//! channel plus a byte-budget semaphore, so the replication hot path normally
//! only does a channel send (no PG round-trip, no JSON on the hot path — the
//! writer serialises). Reads (resume catch-up, checkpoint, retention) happen
//! rarely and use short-lived connections.
//!
//! # Backpressure model
//!
//! `append` is async and **blocks when the byte budget or the queue is full**.
//! The chain when Postgres is slow or down: the budget fills → `append().await`
//! parks → `ChangeStreamServer::publish().await` parks → the replication pump
//! stops reading (and acking) WAL → the **source Postgres retains WAL on its
//! disk** in the replication slot instead of Orbit accumulating events in RAM.
//! This is safe: WAL acks were already gated on `durable_lsn`, so the slot
//! retains WAL whenever the log lags; bounding the queue merely also stops
//! *reading*. Tradeoff: a prolonged stall grows `pg_wal` on the source and can
//! hit `max_slot_wal_keep_size` (slot invalidation → full re-sync) — that risk
//! exists with ack gating alone; prefer it over an Orbit OOM.
//!
//! The byte budget covers queued entries **plus** the writer's in-flight batch
//! and a failed batch parked for retry: permits are only returned after a
//! successful flush, so retry memory can never exceed the budget + one event.
//!
//! Positions are the change-stream's `pos` (a monotonic seq). The log also stores
//! each change's WAL `lsn` so the replicator can resume the seq across restarts
//! and skip already-logged re-deliveries (see `run_replicator`). `INSERT … ON
//! CONFLICT (pos) DO NOTHING` makes re-delivery idempotent.

use anyhow::Result;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, Semaphore};
use tokio_postgres::types::ToSql;

use crate::pg::tls::{self, PgTlsMode};
use crate::LogicalEvent;

/// One logged change: `(pos, lsn, event, estimated bytes held as permits)`.
type LoggedChange = (u64, u64, Arc<LogicalEvent>, u32);

/// Tuning for the durable change-log queue and write batches. Defaults come
/// from `ORBIT_CHANGELOG_*` env vars via [`ChangeLogConfig::from_env`].
#[derive(Clone, Copy, Debug)]
pub struct ChangeLogConfig {
    /// Bounded channel capacity in *entries* (each entry is ~40 bytes; the
    /// real bound is `queue_bytes`).
    pub queue_events: usize,
    /// Byte budget (estimated event bytes) covering the queue, the in-flight
    /// write batch, and a failed batch awaiting retry.
    pub queue_bytes: usize,
    /// Flush a write batch at this many events…
    pub max_batch_events: usize,
    /// …or at this many estimated bytes, whichever comes first.
    pub max_batch_bytes: usize,
}

impl Default for ChangeLogConfig {
    fn default() -> Self {
        ChangeLogConfig {
            queue_events: 65_536,
            queue_bytes: 32 << 20,    // 32 MiB
            max_batch_events: 1_024,
            max_batch_bytes: 4 << 20, // 4 MiB
        }
    }
}

impl ChangeLogConfig {
    /// Read `ORBIT_CHANGELOG_QUEUE_EVENTS` / `ORBIT_CHANGELOG_QUEUE_BYTES` /
    /// `ORBIT_CHANGELOG_BATCH_EVENTS` / `ORBIT_CHANGELOG_BATCH_BYTES`, falling
    /// back to the defaults for unset or unparsable values.
    pub fn from_env() -> Self {
        fn env_usize(name: &str, default: usize) -> usize {
            match std::env::var(name) {
                Ok(v) => v.trim().parse().unwrap_or_else(|_| {
                    eprintln!("change-log: ignoring unparsable {name}={v:?}");
                    default
                }),
                Err(_) => default,
            }
        }
        let d = ChangeLogConfig::default();
        ChangeLogConfig {
            queue_events: env_usize("ORBIT_CHANGELOG_QUEUE_EVENTS", d.queue_events),
            queue_bytes: env_usize("ORBIT_CHANGELOG_QUEUE_BYTES", d.queue_bytes),
            max_batch_events: env_usize("ORBIT_CHANGELOG_BATCH_EVENTS", d.max_batch_events),
            max_batch_bytes: env_usize("ORBIT_CHANGELOG_BATCH_BYTES", d.max_batch_bytes),
        }
    }
}

/// Live queue-depth counters for observability. Incremented on `append`,
/// decremented when the writer drains an entry into a batch (so they track the
/// *channel*, not the in-flight batch — the byte-budget semaphore covers both).
#[derive(Default)]
pub struct ChangeLogStats {
    pub queued_events: AtomicU64,
    pub queued_bytes: AtomicU64,
}

/// Handle to the durable change-log: an awaitable, byte-bounded `append` plus
/// rare read paths.
pub struct PgChangeLog {
    conn_str: String,
    /// Table name, namespaced per replication slot (one log per replicator).
    table: String,
    tx: mpsc::Sender<LoggedChange>,
    /// 1 permit = 1 estimated byte. Acquired in `append`, returned by the
    /// writer only after a successful flush.
    byte_budget: Arc<Semaphore>,
    /// Clamp for a single event's permit request: an event larger than the
    /// whole budget takes the whole budget and proceeds alone (never deadlocks).
    max_event_permits: u32,
    stats: Arc<ChangeLogStats>,
    tls: PgTlsMode,
    /// Highest WAL LSN durably flushed to the log. The replicator acknowledges
    /// replication WAL only up to this point, so a crash between the hot-path
    /// `append` (a channel send) and the batched flush replays from the slot
    /// instead of leaving a silent hole in the log (which a delta-resuming
    /// view-syncer would skip right over — pos stays contiguous).
    durable_lsn: Arc<AtomicU64>,
}

impl PgChangeLog {
    /// [`PgChangeLog::open_with`] with env-derived config.
    pub async fn open(conn_str: String, table: String, tls: PgTlsMode) -> Result<PgChangeLog> {
        PgChangeLog::open_with(ChangeLogConfig::from_env(), conn_str, table, tls).await
    }

    /// Ensure the schema and spawn the batched writer (it owns its own connection).
    /// `table` must be a trusted identifier (it's derived from the slot name).
    pub async fn open_with(
        cfg: ChangeLogConfig,
        conn_str: String,
        table: String,
        tls: PgTlsMode,
    ) -> Result<PgChangeLog> {
        let client = connect(&conn_str, tls).await?;
        client
            .batch_execute(&format!(
                "CREATE TABLE IF NOT EXISTS {table} (
                     pos   bigint PRIMARY KEY,
                     lsn   bigint NOT NULL,
                     event text   NOT NULL
                 );"
            ))
            .await?;
        drop(client);

        // acquire_many takes u32; Semaphore::MAX_PERMITS is usize::MAX >> 3.
        let queue_bytes = cfg.queue_bytes.max(1).min(u32::MAX as usize);
        let (tx, rx) = mpsc::channel(cfg.queue_events.max(1));
        let byte_budget = Arc::new(Semaphore::new(queue_bytes));
        let stats = Arc::new(ChangeLogStats::default());
        let writer_conn = conn_str.clone();
        let writer_table = table.clone();
        let durable_lsn = Arc::new(AtomicU64::new(0));
        let writer_durable = Arc::clone(&durable_lsn);
        let writer_budget = Arc::clone(&byte_budget);
        let writer_stats = Arc::clone(&stats);
        tokio::spawn(async move {
            run_writer(
                writer_conn,
                writer_table,
                rx,
                tls,
                writer_durable,
                writer_budget,
                writer_stats,
                cfg.max_batch_events.max(1),
                cfg.max_batch_bytes.max(1),
            )
            .await;
        });
        Ok(PgChangeLog {
            conn_str,
            table,
            tx,
            byte_budget,
            max_event_permits: queue_bytes as u32,
            stats,
            tls,
            durable_lsn,
        })
    }

    /// Highest WAL LSN durably flushed by the background writer (0 until the
    /// first flush this run).
    pub fn durable_lsn(&self) -> u64 {
        self.durable_lsn.load(Ordering::Acquire)
    }

    /// Live queue-depth counters (shared with the metrics exporter).
    pub fn stats(&self) -> Arc<ChangeLogStats> {
        Arc::clone(&self.stats)
    }

    /// Append a change from the hot path. Awaits (backpressure) when the byte
    /// budget or the queue is full — see the module doc for why parking the
    /// replication pump here is the intended behaviour.
    pub async fn append(&self, pos: u64, lsn: u64, event: Arc<LogicalEvent>) {
        let est = (event.estimated_bytes() as u64)
            .min(self.max_event_permits as u64)
            .max(1) as u32;
        match self.byte_budget.acquire_many(est).await {
            // Ownership of the permits transfers to the queued entry; the
            // writer returns them after a successful flush.
            Ok(permits) => permits.forget(),
            Err(_) => return, // semaphore closed → shutting down
        }
        self.stats.queued_events.fetch_add(1, Ordering::Relaxed);
        self.stats.queued_bytes.fetch_add(est as u64, Ordering::Relaxed);
        if self.tx.send((pos, lsn, event, est)).await.is_err() {
            // Writer gone (shutdown): undo the accounting.
            self.stats.queued_events.fetch_sub(1, Ordering::Relaxed);
            self.stats.queued_bytes.fetch_sub(est as u64, Ordering::Relaxed);
            self.byte_budget.add_permits(est as usize);
        }
    }

    /// The highest durably-recorded `(pos, lsn)`, if any — the replicator's resume
    /// point after a restart.
    pub async fn checkpoint(&self) -> Result<Option<(u64, u64)>> {
        let client = connect(&self.conn_str, self.tls).await?;
        let row = client
            .query_opt(
                &format!("SELECT pos, lsn FROM {} ORDER BY pos DESC LIMIT 1", self.table),
                &[],
            )
            .await?;
        Ok(row.map(|r| (r.get::<_, i64>(0) as u64, r.get::<_, i64>(1) as u64)))
    }

    /// Changes with `pos > after`, in order — up to `limit` rows or roughly
    /// `max_bytes` of stored JSON, whichever cuts first (always at least one
    /// row, so callers make progress past oversized events). Also returns the
    /// smallest `pos` still present (so the caller can detect that `after` was
    /// pruned away). A byte-capped short page is indistinguishable from a
    /// count-capped one; callers already re-query from the last pos returned.
    pub async fn read_after(
        &self,
        after: u64,
        limit: i64,
        max_bytes: usize,
    ) -> Result<(Option<u64>, Vec<(u64, LogicalEvent)>)> {
        let client = connect(&self.conn_str, self.tls).await?;
        let min: Option<i64> = client
            .query_one(&format!("SELECT min(pos) FROM {}", self.table), &[])
            .await?
            .get(0);
        let rows = client
            .query(
                &format!("SELECT pos, event FROM {} WHERE pos > $1 ORDER BY pos LIMIT $2", self.table),
                &[&(after as i64), &limit],
            )
            .await?;
        let mut out = Vec::with_capacity(rows.len());
        let mut total = 0usize;
        for r in &rows {
            let pos = r.get::<_, i64>(0) as u64;
            let json: String = r.get(1);
            if !out.is_empty() && total.saturating_add(json.len()) > max_bytes {
                break; // byte cap — stop *before* exceeding it
            }
            total += json.len();
            match serde_json::from_str(&json) {
                Ok(event) => out.push((pos, event)),
                Err(e) => {
                    // Do NOT substitute a no-op: a corrupt/unknown event (version
                    // skew, corruption) delivered with its real pos would advance
                    // the reader's watermark past a change it never applied —
                    // silent divergence. Stop here; the caller's contiguity check
                    // turns the cut into a Reset (full re-restore). Correct.
                    eprintln!("change-log: unreadable event at pos {pos} ({e}); truncating read");
                    break;
                }
            }
        }
        Ok((min.map(|m| m as u64), out))
    }

    /// Delete entries with `pos < before` (retention; called periodically).
    pub async fn prune_before(&self, before: u64) -> Result<()> {
        let client = connect(&self.conn_str, self.tls).await?;
        client
            .execute(
                &format!("DELETE FROM {} WHERE pos < $1", self.table),
                &[&(before as i64)],
            )
            .await?;
        Ok(())
    }
}

impl Drop for PgChangeLog {
    fn drop(&mut self) {
        // Wake any `append` parked in `acquire_many` so shutdown doesn't hang;
        // it observes the closed semaphore and returns without queueing.
        self.byte_budget.close();
    }
}

async fn connect(conn_str: &str, mode: PgTlsMode) -> Result<tokio_postgres::Client> {
    let (client, driver) = tls::connect(conn_str, mode).await?;
    tokio::spawn(driver);
    Ok(client)
}

/// Background writer: drains the channel into batched multi-row inserts,
/// flushing at `max_batch_events` events or `max_batch_bytes` estimated bytes.
/// A failed flush is RETRIED — never dropped — because the log's positions are
/// contiguous: a silently missing batch would be skipped unnoticed by
/// delta-resuming view-syncers. `ON CONFLICT DO NOTHING` makes the retry
/// idempotent. After each successful flush the batch's highest LSN becomes the
/// durable watermark that gates the replicator's WAL acknowledgements, and the
/// batch's byte permits return to the budget (NOT before — a parked retry
/// batch keeps its permits so total buffered memory stays inside the budget).
#[allow(clippy::too_many_arguments)]
async fn run_writer(
    conn_str: String,
    table: String,
    mut rx: mpsc::Receiver<LoggedChange>,
    tls: PgTlsMode,
    durable_lsn: Arc<AtomicU64>,
    byte_budget: Arc<Semaphore>,
    stats: Arc<ChangeLogStats>,
    max_batch_events: usize,
    max_batch_bytes: usize,
) {
    let mut pending: Option<Vec<LoggedChange>> = None;
    loop {
        let client = match connect(&conn_str, tls).await {
            Ok(c) => c,
            Err(e) => {
                eprintln!("change-log writer: connect failed ({e}); retrying in 2s");
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                continue;
            }
        };
        loop {
            let batch = match pending.take() {
                Some(b) => b, // retry the batch that failed before reconnecting
                None => {
                    let first = match rx.recv().await {
                        Some(x) => x,
                        None => return, // sender dropped → replicator shutting down
                    };
                    let mut batch_bytes = first.3 as usize;
                    let mut batch = vec![first];
                    while batch.len() < max_batch_events && batch_bytes < max_batch_bytes {
                        match rx.try_recv() {
                            Ok(x) => {
                                batch_bytes += x.3 as usize;
                                batch.push(x);
                            }
                            Err(_) => break,
                        }
                    }
                    // The entries left the channel; stats track queue depth.
                    stats
                        .queued_events
                        .fetch_sub(batch.len() as u64, Ordering::Relaxed);
                    stats.queued_bytes.fetch_sub(
                        batch.iter().map(|(_, _, _, est)| *est as u64).sum(),
                        Ordering::Relaxed,
                    );
                    batch
                }
            };
            if let Err(e) = flush_batch(&client, &table, &batch).await {
                eprintln!("change-log writer: flush failed ({e:#}); reconnecting to retry");
                pending = Some(batch);
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                break; // reconnect, then retry the same batch (permits still held)
            }
            if let Some(max) = batch.iter().map(|(_, lsn, _, _)| *lsn).max() {
                durable_lsn.fetch_max(max, Ordering::Release);
            }
            // Durably flushed: return the batch's bytes to the budget.
            byte_budget
                .add_permits(batch.iter().map(|(_, _, _, est)| *est as usize).sum());
        }
    }
}

async fn flush_batch(client: &tokio_postgres::Client, table: &str, batch: &[LoggedChange]) -> Result<()> {
    // Owned param storage outlives the borrowed `params` slice.
    let mut poss: Vec<i64> = Vec::with_capacity(batch.len());
    let mut lsns: Vec<i64> = Vec::with_capacity(batch.len());
    let mut evs: Vec<String> = Vec::with_capacity(batch.len());
    for (pos, lsn, ev, _) in batch {
        poss.push(*pos as i64);
        lsns.push(*lsn as i64);
        evs.push(serde_json::to_string(ev.as_ref())?);
    }
    let mut sql = format!("INSERT INTO {table} (pos, lsn, event) VALUES ");
    let mut params: Vec<&(dyn ToSql + Sync)> = Vec::with_capacity(batch.len() * 3);
    for i in 0..batch.len() {
        if i > 0 {
            sql.push(',');
        }
        let b = i * 3;
        sql.push_str(&format!("(${},${},${})", b + 1, b + 2, b + 3));
        params.push(&poss[i]);
        params.push(&lsns[i]);
        params.push(&evs[i]);
    }
    sql.push_str(" ON CONFLICT (pos) DO NOTHING");
    client.execute(sql.as_str(), &params).await?;
    Ok(())
}
