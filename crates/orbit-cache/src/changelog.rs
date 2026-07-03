//! Durable change-log: a Postgres-backed, ordered log of every change the
//! replicator publishes. It lets view-syncers resume by **delta** across a
//! replicator restart (or after the in-memory ring evicts their resume point)
//! instead of re-restoring the whole replica.
//!
//! Performance: writes are batched on a background task fed by an unbounded
//! channel, so the replication hot path only does a non-blocking `send` (no PG
//! round-trip, no JSON on the hot path — the writer serialises). Reads
//! (resume catch-up, checkpoint, retention) happen rarely and use short-lived
//! connections.
//!
//! Positions are the change-stream's `pos` (a monotonic seq). The log also stores
//! each change's WAL `lsn` so the replicator can resume the seq across restarts
//! and skip already-logged re-deliveries (see `run_replicator`). `INSERT … ON
//! CONFLICT (pos) DO NOTHING` makes re-delivery idempotent.

use anyhow::Result;
use tokio::sync::mpsc;
use tokio_postgres::types::ToSql;

use crate::pg::tls::{self, PgTlsMode};
use crate::LogicalEvent;

/// One logged change: `(pos, lsn, event)`.
type LoggedChange = (u64, u64, LogicalEvent);

/// Handle to the durable change-log: a non-blocking `append` plus rare read paths.
pub struct PgChangeLog {
    conn_str: String,
    /// Table name, namespaced per replication slot (one log per replicator).
    table: String,
    tx: mpsc::UnboundedSender<LoggedChange>,
    tls: PgTlsMode,
    /// Highest WAL LSN durably flushed to the log. The replicator acknowledges
    /// replication WAL only up to this point, so a crash between the hot-path
    /// `append` (a channel send) and the batched flush replays from the slot
    /// instead of leaving a silent hole in the log (which a delta-resuming
    /// view-syncer would skip right over — pos stays contiguous).
    durable_lsn: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

impl PgChangeLog {
    /// Ensure the schema and spawn the batched writer (it owns its own connection).
    /// `table` must be a trusted identifier (it's derived from the slot name).
    pub async fn open(conn_str: String, table: String, tls: PgTlsMode) -> Result<PgChangeLog> {
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

        let (tx, rx) = mpsc::unbounded_channel();
        let writer_conn = conn_str.clone();
        let writer_table = table.clone();
        let durable_lsn = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let writer_durable = std::sync::Arc::clone(&durable_lsn);
        tokio::spawn(async move {
            run_writer(writer_conn, writer_table, rx, tls, writer_durable).await;
        });
        Ok(PgChangeLog { conn_str, table, tx, tls, durable_lsn })
    }

    /// Highest WAL LSN durably flushed by the background writer (0 until the
    /// first flush this run).
    pub fn durable_lsn(&self) -> u64 {
        self.durable_lsn.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Append a change from the hot path. Non-blocking; never awaits PG.
    pub fn append(&self, pos: u64, lsn: u64, event: LogicalEvent) {
        let _ = self.tx.send((pos, lsn, event));
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

    /// Up to `limit` changes with `pos > after`, in order, plus the smallest `pos`
    /// still present (so the caller can detect that `after` was pruned away).
    pub async fn read_after(&self, after: u64, limit: i64) -> Result<(Option<u64>, Vec<(u64, LogicalEvent)>)> {
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
        for r in &rows {
            let pos = r.get::<_, i64>(0) as u64;
            let json: String = r.get(1);
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

async fn connect(conn_str: &str, mode: PgTlsMode) -> Result<tokio_postgres::Client> {
    let (client, driver) = tls::connect(conn_str, mode).await?;
    tokio::spawn(driver);
    Ok(client)
}

/// Background writer: drains the channel into batched multi-row inserts. The
/// hot path never blocks (unbounded channel), but a failed flush is RETRIED —
/// never dropped — because the log's positions are contiguous: a silently
/// missing batch would be skipped unnoticed by delta-resuming view-syncers.
/// `ON CONFLICT DO NOTHING` makes the retry idempotent. After each successful
/// flush the batch's highest LSN becomes the durable watermark that gates the
/// replicator's WAL acknowledgements.
async fn run_writer(
    conn_str: String,
    table: String,
    mut rx: mpsc::UnboundedReceiver<LoggedChange>,
    tls: PgTlsMode,
    durable_lsn: std::sync::Arc<std::sync::atomic::AtomicU64>,
) {
    const MAX_BATCH: usize = 1024;
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
                    let mut batch = vec![first];
                    while batch.len() < MAX_BATCH {
                        match rx.try_recv() {
                            Ok(x) => batch.push(x),
                            Err(_) => break,
                        }
                    }
                    batch
                }
            };
            if let Err(e) = flush_batch(&client, &table, &batch).await {
                eprintln!("change-log writer: flush failed ({e:#}); reconnecting to retry");
                pending = Some(batch);
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                break; // reconnect, then retry the same batch
            }
            if let Some(max) = batch.iter().map(|(_, lsn, _)| *lsn).max() {
                durable_lsn.fetch_max(max, std::sync::atomic::Ordering::Release);
            }
        }
    }
}

async fn flush_batch(client: &tokio_postgres::Client, table: &str, batch: &[LoggedChange]) -> Result<()> {
    // Owned param storage outlives the borrowed `params` slice.
    let mut poss: Vec<i64> = Vec::with_capacity(batch.len());
    let mut lsns: Vec<i64> = Vec::with_capacity(batch.len());
    let mut evs: Vec<String> = Vec::with_capacity(batch.len());
    for (pos, lsn, ev) in batch {
        poss.push(*pos as i64);
        lsns.push(*lsn as i64);
        evs.push(serde_json::to_string(ev)?);
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
