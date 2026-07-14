//! The runnable, integrated Orbit server.
//!
//! Assembles every layer: connect to Postgres, set up publication + slot,
//! initial-sync each table into the shared [`Replica`], run the replication pump
//! (apply changes once, broadcast a tick), and accept WebSocket clients that
//! each materialize their own queries over the shared replica and flush on tick.
//! Mutations write through to Postgres and converge back via replication.
//!
//! The IVM pipelines are `!Send`, so everything runs on a single-thread
//! [`LocalSet`].

use crate::changestream::{ChangeMsg, ChangeStreamClient, ChangeStreamServer};
use crate::mutators::MutatorRegistry;
use crate::objectstore::{ObjectStore, ReplicaSnapshot};
use crate::pg::{create_publication, initial_sync_backend};
use crate::queries::QueryRegistry;
use crate::replica::{Replica, ReplicaBackend};
use crate::server::serve_client;
use crate::{LogicalEvent, ReplicationStream};
use anyhow::{Context, Result};
use oql::ivm::ColumnType;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio::task::{spawn_local, LocalSet};
use crate::pg::tls::{self, PgTlsMode};

// How many recent changes the replicator keeps IN MEMORY (the ring) for fast
// view-syncer resume is configured via `ChangeStreamConfig` (65,536 events /
// 64 MiB by default; `ORBIT_CHANGE_RING_CAPACITY` / `ORBIT_CHANGE_RING_BYTES`).
// A view-syncer lagging further than the ring resumes from the durable
// change-log instead.

/// How many recent changes the durable change-log (Postgres) retains for resume
/// after a longer gap or a restart. On disk, so kept far larger than the ring.
const LOG_RETENTION: u64 = 2_000_000;

/// A table to replicate and serve.
pub struct TableConfig {
    pub name: String,
    pub columns: Vec<(String, ColumnType)>,
    pub primary_key: Vec<String>,
}

/// Server configuration.
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub database: String,
    /// Postgres password (`None` for trust auth). Sent on every PG connection.
    pub password: Option<String>,
    /// TLS mode for every Postgres connection (SQL + replication). Default off.
    pub tls: PgTlsMode,
    pub tables: Vec<TableConfig>,
    pub publication: String,
    pub slot: String,
    pub listen_addr: String,
    /// App push endpoint for custom mutators (`None` = none). Mutations are
    /// forwarded here with the client's auth (see [`Forwarder`](crate::forward)).
    pub mutate_url: Option<String>,
    /// App query endpoint that transforms named queries (`None` = none).
    pub query_url: Option<String>,
    /// Shared secret sent as `X-Api-Key` to the endpoints.
    pub api_key: Option<String>,
    /// Forward the client's `Cookie` header to the endpoints.
    pub forward_cookies: bool,
}

impl ServerConfig {
    fn forward_config(&self) -> crate::forward::ForwardConfig {
        crate::forward::ForwardConfig {
            mutate_url: self.mutate_url.clone(),
            query_url: self.query_url.clone(),
            api_key: self.api_key.clone(),
            forward_cookies: self.forward_cookies,
        }
    }
}

impl ServerConfig {
    /// The `tokio-postgres` connection string, including the `password` when set
    /// (properly quoted). Trust-auth local PG needs none.
    pub fn conn_str(&self) -> String {
        tls::conn_str(&self.host, self.port, &self.user, &self.database, self.password.as_deref())
    }
}

/// Run the server with the default in-memory replica.
pub async fn run_server(cfg: ServerConfig, mutators: MutatorRegistry) -> Result<()> {
    let mut replica = Replica::new();
    for t in &cfg.tables {
        replica.add_table(&t.name, t.columns.iter().cloned().collect(), t.primary_key.clone());
    }
    run_server_with(cfg, mutators, replica).await
}

/// Run the server with a SQLite-backed (durable if `dir` is `Some`) replica.
pub async fn run_server_sqlite(
    cfg: ServerConfig,
    mutators: MutatorRegistry,
    dir: Option<std::path::PathBuf>,
    opts: crate::sqlite_source::SqliteReplicaOpts,
) -> Result<()> {
    let mut replica = match dir {
        Some(d) => crate::sqlite_source::SqliteReplica::durable_with(d, &opts),
        None => crate::sqlite_source::SqliteReplica::in_memory_with(&opts),
    };
    for t in &cfg.tables {
        replica.add_table(&t.name, t.columns.clone(), t.primary_key.clone());
    }
    run_server_with(cfg, mutators, replica).await
}

/// Run a **multi-core** server: `num_shards` worker threads, each owning its own
/// replica + clients' pipelines (see [`ShardedServer`](crate::sharded)). The
/// main thread runs the single replication pump (one slot) and fans every event
/// out to all shards; new connections are dispatched round-robin. Each shard
/// opens its own Postgres client for client mutation write-through, which
/// converges back to all shards via the fan-out.
///
/// Trade-off: each shard holds its own copy of the replica (lock-free reads at
/// the cost of per-shard dataset memory). Pass `num_shards = 1` for the
/// single-thread behavior of [`run_server`].
pub async fn run_server_sharded(cfg: ServerConfig, num_shards: usize) -> Result<()> {
    let local = LocalSet::new();
    local.run_until(run_sharded_inner(cfg, num_shards)).await
}

async fn run_sharded_inner(cfg: ServerConfig, num_shards: usize) -> Result<()> {
    let (pg, driver) = tls::connect(&cfg.conn_str(), cfg.tls).await?;
    spawn_local(driver);
    // Shared-CVR tables (per-client view + version), shared across shards.
    crate::cvr::PgCvrStore::ensure_schema(&pg).await?;

    // CVR GC (same sweep as the other serving paths): ephemeral clients leave
    // their materialized views + lastMutationIDs in Postgres forever otherwise.
    {
        let gc_conn = cfg.conn_str();
        let gc_tls = cfg.tls;
        spawn_local(async move {
            let Ok((gc_pg, driver)) = tls::connect(&gc_conn, gc_tls).await else { return };
            spawn_local(driver);
            loop {
                match crate::cvr::PgCvrStore::gc_stale_clients(&gc_pg, 7).await {
                    Ok(n) if n > 0 => eprintln!("cvr gc: swept {n} stale client rows"),
                    Ok(_) => {}
                    Err(e) => eprintln!("cvr gc failed (retrying next cycle): {e:#}"),
                }
                tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
            }
        });
    }

    let table_names: Vec<&str> = cfg.tables.iter().map(|t| t.name.as_str()).collect();
    create_publication(&pg, &cfg.publication, &table_names)
        .await
        .with_context(|| format!("creating publication for tables {table_names:?} — do they all exist in the database?"))?;
    let (start_lsn, exported) = crate::pg::ensure_slot_with_snapshot(
        &pg, &cfg.host, cfg.port, &cfg.user, &cfg.database,
        cfg.password.as_deref(), cfg.tls, &cfg.slot,
    )
    .await
    .context("creating logical replication slot — is the server started with wal_level=logical?")?;

    // Take the initial snapshot once and seed every shard with the same rows.
    // A freshly-created slot pins the reads to its exact consistent point.
    if let Some(e) = &exported {
        pg.batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ").await?;
        pg.execute(format!("SET TRANSACTION SNAPSHOT '{}'", e.snapshot).as_str(), &[]).await?;
        eprintln!("initial sync pinned to slot snapshot {}", e.snapshot);
    }
    let mut shard_tables = Vec::new();
    for t in &cfg.tables {
        let rows = crate::pg::select_all_rows(&pg, &t.name, &t.columns).await?;
        eprintln!("initial sync: {} rows from {}", rows.len(), t.name);
        shard_tables.push(crate::sharded::ShardTable {
            name: t.name.clone(),
            columns: t.columns.clone(),
            primary_key: t.primary_key.clone(),
            seed: rows,
        });
    }
    if exported.is_some() {
        let _ = pg.batch_execute("COMMIT").await;
    }
    drop(exported); // the snapshot's replication connection may close now

    let server = std::sync::Arc::new(crate::sharded::ShardedServer::start_with_pg(
        num_shards,
        shard_tables,
        Some(cfg.conn_str()),
        cfg.tls,
    ));

    // Replication pump: decode each event once, fan it out to every shard.
    {
        let server = server.clone();
        let (host, port, user, db) = (cfg.host.clone(), cfg.port, cfg.user.clone(), cfg.database.clone());
        let (slot, publication) = (cfg.slot.clone(), cfg.publication.clone());
        let (password, tls_mode) = (cfg.password.clone(), cfg.tls);
        spawn_local(async move {
            let mut stream = match ReplicationStream::start_with_tls(&host, port, &user, &db, &slot, &publication, start_lsn, password.as_deref(), tls_mode).await {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("replication start failed: {e}");
                    return;
                }
            };
            loop {
                match stream.next_event().await {
                    Ok((lsn, ev)) => {
                        server.broadcast_event(ev);
                        // In-memory shards re-initial-sync on boot, so WAL needs no
                        // durable-replay guarantee: confirm on receipt.
                        stream.confirm(lsn);
                    }
                    Err(e) => {
                        // Crash-only (same policy as the view-syncer's Reset): an
                        // in-process reconnect could double-apply re-delivered WAL
                        // into the in-memory replica, and merely breaking the loop
                        // would leave the server serving silently-stale data forever.
                        // Exiting lets the orchestrator restart us; the fresh
                        // initial sync guarantees convergence.
                        eprintln!("replication error: {e:#}; exiting to re-sync (restart me)");
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                        std::process::exit(1);
                    }
                }
            }
        });
    }

    // Accept loop: dispatch each connection (as a std stream) to a shard.
    let listener = TcpListener::bind(&cfg.listen_addr).await?;
    eprintln!("orbit-server (sharded x{}) listening on {}", num_shards, cfg.listen_addr);
    loop {
        let (sock, _) = listener.accept().await?;
        let std_sock = sock.into_std()?;
        server.dispatch(std_sock);
    }
}

/// Run the server over any [`ReplicaBackend`]. Drives a [`LocalSet`] so the
/// `!Send` IVM state stays on one thread.
pub async fn run_server_with<B: ReplicaBackend + 'static>(
    cfg: ServerConfig,
    mutators: MutatorRegistry,
    backend: B,
) -> Result<()> {
    let local = LocalSet::new();
    local.run_until(run_inner(cfg, mutators, QueryRegistry::new(), backend)).await
}

/// Run the in-memory server with custom mutators **and** custom (named) queries
/// registered — the idiomatic Orbit/Zero setup (`orbit.mutateCustom(...)` and
/// subscribing to a named query).
pub async fn run_server_full(
    cfg: ServerConfig,
    mutators: MutatorRegistry,
    queries: QueryRegistry,
) -> Result<()> {
    let mut replica = Replica::new();
    for t in &cfg.tables {
        replica.add_table(&t.name, t.columns.iter().cloned().collect(), t.primary_key.clone());
    }
    let local = LocalSet::new();
    local.run_until(run_inner(cfg, mutators, queries, replica)).await
}

/// Byte cap for buffering one replication transaction before switching to
/// streaming apply (`ORBIT_TXN_BUFFER_BYTES`, default 32 MiB). Below the cap a
/// transaction applies atomically at Commit (no locking); above it, events
/// apply as they arrive under the node's replica-consistency write lock —
/// bounded memory instead of a fleet-wide OOM on a giant transaction (audit
/// Tier 1.2; Zero streams in bounded batches).
fn txn_buffer_cap() -> usize {
    static CAP: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *CAP.get_or_init(|| {
        std::env::var("ORBIT_TXN_BUFFER_BYTES")
            .ok()
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(32 << 20)
            .max(64 * 1024)
    })
}

/// Per-transaction apply state for a serving node's replication pump.
///
/// Fast path: buffer events and apply the whole transaction synchronously at
/// Commit — atomic by construction (no awaits inside the apply). Oversized
/// path: once the buffer exceeds the cap, take the node's replica-consistency
/// WRITE lock, open the storage transaction, drain the buffer, and apply every
/// further event on arrival. The write lock keeps hydrations and tick flushes
/// from observing (or sending) a torn mid-transaction state across the socket
/// awaits between events.
struct TxnApply {
    cap: usize,
    buf: Vec<LogicalEvent>,
    buf_bytes: usize,
    streaming: bool,
    dirty: bool,
    /// Tables the transaction touched — carried on the tick so client tasks
    /// whose queries read none of them skip their drain (no-op suppression).
    tables: std::collections::HashSet<String>,
    guard: Option<tokio::sync::OwnedRwLockWriteGuard<()>>,
}

/// Record the table(s) `ev` touches into `out`.
fn add_event_tables(ev: &LogicalEvent, out: &mut std::collections::HashSet<String>) {
    match ev {
        LogicalEvent::Insert { table, .. }
        | LogicalEvent::Update { table, .. }
        | LogicalEvent::Delete { table, .. }
        | LogicalEvent::Relation { table, .. } => {
            out.insert(table.clone());
        }
        LogicalEvent::Truncate { tables } => {
            out.extend(tables.iter().cloned());
        }
        LogicalEvent::Begin | LogicalEvent::Commit | LogicalEvent::Other => {}
    }
}

fn is_data_event(ev: &LogicalEvent) -> bool {
    matches!(
        ev,
        LogicalEvent::Insert { .. }
            | LogicalEvent::Update { .. }
            | LogicalEvent::Delete { .. }
            | LogicalEvent::Truncate { .. }
    )
}

impl TxnApply {
    fn new() -> Self {
        TxnApply {
            cap: txn_buffer_cap(),
            buf: Vec::new(),
            buf_bytes: 0,
            streaming: false,
            dirty: false,
            tables: std::collections::HashSet::new(),
            guard: None,
        }
    }

    /// Feed one in-transaction event.
    async fn push<B: ReplicaBackend>(
        &mut self,
        replica: &B,
        lmids: &crate::server::LmidMap,
        ev: LogicalEvent,
    ) -> Result<()> {
        self.dirty |= is_data_event(&ev);
        add_event_tables(&ev, &mut self.tables);
        if self.streaming {
            crate::server::capture_lmid(&ev, lmids);
            return replica.apply(ev);
        }
        self.buf_bytes += ev.estimated_bytes();
        self.buf.push(ev);
        if self.buf_bytes > self.cap {
            // Oversized transaction: switch to streaming. Block consistency
            // readers for the REST of the transaction, open the storage txn,
            // and drain what we buffered so far.
            eprintln!(
                "transaction exceeds {} buffered bytes; streaming apply (hydrations wait until its commit)",
                self.cap
            );
            self.guard = Some(crate::server::replica_apply_lock().write_owned().await);
            replica.begin_txn()?;
            for ev in self.buf.drain(..) {
                crate::server::capture_lmid(&ev, lmids);
                replica.apply(ev)?;
            }
            self.buf_bytes = 0;
            self.streaming = true;
        }
        Ok(())
    }

    /// Commit the transaction. Returns the touched tables when it contained
    /// data changes (the tick payload), `None` when clean.
    fn commit<B: ReplicaBackend>(
        mut self,
        replica: &B,
        lmids: &crate::server::LmidMap,
        lsn: u64,
        pos: u64,
    ) -> Result<Option<crate::server::Tick>> {
        if !self.streaming {
            replica.begin_txn()?;
            for ev in self.buf.drain(..) {
                crate::server::capture_lmid(&ev, lmids);
                replica.apply(ev)?;
            }
        }
        replica.apply(LogicalEvent::Commit)?;
        replica.commit_txn(lsn, pos)?;
        self.guard.take(); // release consistency writers after the commit
        Ok(if self.dirty {
            Some(std::sync::Arc::new(self.tables.into_iter().collect()))
        } else {
            None
        })
    }

    /// Abandon the transaction. Returns `true` when it had already STREAMED
    /// changes into live pipelines — storage rolls back, but pipeline pushes
    /// cannot be un-pushed, so the caller must crash-exit (restart restores a
    /// consistent replica; clients resync). Buffered-only transactions abort
    /// for free.
    fn abort<B: ReplicaBackend>(mut self, replica: &B) -> bool {
        let streamed = self.streaming;
        if streamed {
            replica.rollback_txn();
        }
        self.guard.take();
        streamed
    }
}

/// Fresh (or crash-resumed) initial sync: ONE storage transaction per table,
/// each clearing the table's stale rows, seeding it, and registering it in the
/// synced-tables registry — a crash resumes at the next unsynced table instead
/// of redoing everything (audit Tier 1.7; previously one giant all-or-nothing
/// transaction). When the slot was just created, `exported` pins every SELECT
/// to the slot's exact consistent snapshot, so the sync and the stream tile
/// with NO overlap window (Zero: parallel COPY pinned to the slot snapshot).
/// A crash-resume continues unpinned at "now" — safe because the unconfirmed
/// slot re-delivers everything since its creation and apply is idempotent.
pub async fn initial_sync_all<B: ReplicaBackend>(
    pg: &tokio_postgres::Client,
    replica: &B,
    cfg: &ServerConfig,
    exported: Option<&crate::pg::ExportedSlot>,
    pos: u64,
) -> Result<()> {
    let synced = replica.synced_tables().unwrap_or_default();
    let todo: Vec<&TableConfig> =
        cfg.tables.iter().filter(|t| !synced.contains(&t.name)).collect();
    for t in cfg.tables.iter().filter(|t| synced.contains(&t.name)) {
        eprintln!("initial sync: {} already synced (crash resume)", t.name);
    }
    if todo.is_empty() {
        return Ok(());
    }
    // Pin to the exported snapshot when available. Safe on the shared client:
    // this runs during single-threaded startup, before any other pg user.
    let pinned = match exported {
        Some(e) => {
            pg.batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ").await?;
            pg.execute(format!("SET TRANSACTION SNAPSHOT '{}'", e.snapshot).as_str(), &[])
                .await?;
            eprintln!(
                "initial sync pinned to slot snapshot {} (consistent point {})",
                e.snapshot, e.start_lsn
            );
            true
        }
        None => false,
    };
    let result: Result<()> = async {
        for t in &todo {
            replica.begin_txn()?;
            let seeded: Result<usize> = async {
                replica.clear_table(&t.name)?;
                initial_sync_backend(pg, replica, &t.name).await
            }
            .await;
            match seeded {
                Ok(n) => {
                    replica.mark_synced(&t.name)?;
                    replica.commit_txn(0, pos)?;
                    eprintln!("initial sync: {n} rows from {}", t.name);
                }
                Err(e) => {
                    replica.rollback_txn();
                    return Err(e.context(format!("initial sync of table {}", t.name)));
                }
            }
        }
        Ok(())
    }
    .await;
    if pinned {
        let _ = pg.batch_execute("COMMIT").await;
    }
    result
}

/// Backfill tables added to the config after a durable replica was first
/// synced (audit Tier 0.5): a watermark resume skips initial sync entirely, so
/// a table newly added to `ORBIT_TABLES` would stream future changes while
/// silently missing every pre-existing row. Runs in one storage transaction
/// that PRESERVES the existing watermark; idempotent apply absorbs the overlap
/// between the backfill SELECT and the stream resume (same discipline as the
/// slot-creation/snapshot window). Zero's analog is the `BackfillManager`.
pub async fn backfill_missing_tables<B: ReplicaBackend>(
    pg: &tokio_postgres::Client,
    replica: &B,
    cfg: &ServerConfig,
) -> Result<()> {
    let Some(synced) = replica.synced_tables() else {
        return Ok(()); // backend re-syncs from scratch every boot
    };
    let missing: Vec<&TableConfig> =
        cfg.tables.iter().filter(|t| !synced.contains(&t.name)).collect();
    if missing.is_empty() {
        return Ok(());
    }
    let lsn = replica.resume_watermark().unwrap_or(0);
    let pos = replica.resume_pos().unwrap_or(0);
    replica.begin_txn().context("opening backfill storage transaction")?;
    for t in &missing {
        let n = match initial_sync_backend(pg, replica, &t.name)
            .await
            .with_context(|| format!("backfilling newly-added table {}", t.name))
        {
            Ok(n) => n,
            Err(e) => {
                replica.rollback_txn();
                return Err(e);
            }
        };
        if let Err(e) = replica.mark_synced(&t.name) {
            replica.rollback_txn();
            return Err(e);
        }
        eprintln!("backfill: {} rows from newly-added table {}", n, t.name);
    }
    // Re-commit the SAME watermark: the backfill doesn't advance replication.
    replica.commit_txn(lsn, pos).context("committing backfill")?;
    Ok(())
}

async fn run_inner<B: ReplicaBackend + 'static>(
    cfg: ServerConfig,
    mutators: MutatorRegistry,
    queries: QueryRegistry,
    backend: B,
) -> Result<()> {
    // Metrics + readiness first: /ready answers 503 through initial sync.
    let metrics = crate::metrics::Metrics::new(crate::metrics::Role::SingleNode);
    crate::metrics::set_node_metrics(metrics.clone());
    if let Some(addr) = crate::metrics::metrics_listen_from_env() {
        let m = metrics.clone();
        spawn_local(async move {
            if let Err(e) = crate::metrics::serve_metrics(addr, m).await {
                eprintln!("metrics server error: {e:#}");
            }
        });
    }

    let (pg, driver) = tls::connect(&cfg.conn_str(), cfg.tls).await?;
    tokio::spawn(driver);
    // Shared-CVR tables (per-client view + version) so identifying clients can
    // resume as a delta; without this the serving path errors on first checkpoint.
    crate::cvr::PgCvrStore::ensure_schema(&pg).await?;
    let pg = Rc::new(pg);

    let table_names: Vec<&str> = cfg.tables.iter().map(|t| t.name.as_str()).collect();
    create_publication(&pg, &cfg.publication, &table_names)
        .await
        .with_context(|| format!("creating publication for tables {table_names:?} — do they all exist in the database?"))?;
    let (start_lsn, exported) = crate::pg::ensure_slot_with_snapshot(
        &pg, &cfg.host, cfg.port, &cfg.user, &cfg.database,
        cfg.password.as_deref(), cfg.tls, &cfg.slot,
    )
    .await
    .context("creating logical replication slot — is the server started with wal_level=logical?")?;

    let replica = Rc::new(backend);
    // A durable backend that recorded a watermark resumes from the slot
    // instead of re-syncing; otherwise the per-table resumable sync runs,
    // pinned to the freshly-created slot's exported snapshot when available.
    let resume_watermark = replica.resume_watermark();
    match resume_watermark {
        Some(w) => {
            eprintln!("durable replica: resuming from watermark {w} (skipping initial sync)");
            // Tables added to the config AFTER the first sync still need their
            // pre-existing rows (the stream only carries new changes).
            backfill_missing_tables(&pg, replica.as_ref(), &cfg).await?;
        }
        None => {
            initial_sync_all(&pg, replica.as_ref(), &cfg, exported.as_ref(), 0).await?;
        }
    }
    drop(exported); // the snapshot's replication connection may close now
    let mutators = Rc::new(mutators);
    let queries = Rc::new(queries);
    let forwarder = Rc::new(crate::forward::Forwarder::new(cfg.forward_config()));

    let (ticks_tx, _) = broadcast::channel::<crate::server::Tick>(1024);

    // Replication pump: apply each change once, then notify all clients.
    // Per-client lastMutationIDs, advanced from replicated `orbit_client_mutations`.
    // Seeded from Postgres at boot so mutations processed before a restart can
    // still be acked to reconnecting clients (the stream only carries NEW ones).
    let lmids: crate::server::LmidMap = Rc::new(std::cell::RefCell::new(std::collections::HashMap::new()));
    {
        let sql = format!("SELECT client_id, last_mutation_id FROM {}", crate::pg::LMID_TABLE);
        if let Ok(rows) = pg.query(&sql, &[]).await {
            let mut m = lmids.borrow_mut();
            for r in rows {
                m.insert(r.get::<_, String>(0), r.get::<_, i64>(1) as u64);
            }
        }
    }
    // CVR GC: ephemeral clients (each non-persisted tab gets a random clientID)
    // leave their materialized views in Postgres forever without a sweep.
    {
        let gc_pg = Rc::clone(&pg);
        spawn_local(async move {
            loop {
                match crate::cvr::PgCvrStore::gc_stale_clients(&gc_pg, 7).await {
                    Ok(n) if n > 0 => eprintln!("cvr gc: swept {n} stale client rows"),
                    Ok(_) => {}
                    Err(e) => eprintln!("cvr gc failed (retrying next cycle): {e:#}"),
                }
                tokio::time::sleep(Duration::from_secs(3600)).await;
            }
        });
    }

    {
        let replica = replica.clone();
        let ticks_tx = ticks_tx.clone();
        let lmids = lmids.clone();
        let (host, port, user, db) = (cfg.host.clone(), cfg.port, cfg.user.clone(), cfg.database.clone());
        let (slot, publication) = (cfg.slot.clone(), cfg.publication.clone());
        let (password, tls_mode) = (cfg.password.clone(), cfg.tls);
        spawn_local(async move {
            let mut stream = match ReplicationStream::start_with_tls(&host, port, &user, &db, &slot, &publication, start_lsn, password.as_deref(), tls_mode).await {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("replication start failed: {e}");
                    return;
                }
            };
            // One transaction at a time, applied atomically at its Commit
            // (durable backends record the commit LSN as their watermark
            // inside the same storage transaction; the tick fires once per
            // transaction). A Begin's position IS the transaction's commit
            // LSN (`final_lsn`), so re-delivered transactions at or below the
            // watermark are skipped UPFRONT, and transactions bigger than the
            // buffer cap stream through bounded memory instead of
            // accumulating in RAM (audit Tier 1.2). WAL is acknowledged only
            // up to the last durably-committed transaction.
            let mut dedup_lsn: u64 = resume_watermark.unwrap_or(0);
            let mut txn: Option<(u64, TxnApply)> = None;
            let halt = |msg: String| async move {
                eprintln!("{msg}; exiting to re-sync (restart me)");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                std::process::exit(1);
            };
            loop {
                match stream.next_event().await {
                    Ok((final_lsn, LogicalEvent::Begin)) => {
                        if let Some((l, t)) = txn.take() {
                            // A new Begin with a transaction open means the
                            // previous one was cut short upstream.
                            if t.abort(replica.as_ref()) {
                                halt(format!(
                                    "transaction at lsn {l} aborted after streaming into live pipelines"
                                ))
                                .await;
                            }
                        }
                        if final_lsn > dedup_lsn {
                            txn = Some((final_lsn, TxnApply::new()));
                        } // else: whole txn already durable — skip its events
                    }
                    Ok((_lsn, LogicalEvent::Commit)) => {
                        if let Some((final_lsn, t)) = txn.take() {
                            // Apply errors (SQL failure in a durable backend)
                            // roll back and halt cleanly: never commit a
                            // watermark over a torn apply, never panic.
                            match t.commit(replica.as_ref(), &lmids, final_lsn, 0) {
                                Ok(tick) => {
                                    dedup_lsn = final_lsn;
                                    if let Some(tick) = tick {
                                        let _ = ticks_tx.send(tick);
                                    }
                                }
                                Err(e) => {
                                    replica.rollback_txn();
                                    halt(format!(
                                        "replica apply error at lsn {final_lsn}: {e:#}; rolled back"
                                    ))
                                    .await;
                                }
                            }
                        }
                        stream.confirm(dedup_lsn);
                    }
                    Ok((_lsn, ev)) => {
                        if let Some((final_lsn, t)) = txn.as_mut() {
                            if let Err(e) = t.push(replica.as_ref(), &lmids, ev).await {
                                let l = *final_lsn;
                                replica.rollback_txn();
                                halt(format!("replica apply error at lsn {l}: {e:#}; rolled back")).await;
                            }
                        }
                    }
                    Err(e) => {
                        // Crash-only (same policy as the view-syncer's Reset): see
                        // run_sharded_inner — reconnecting in-process risks
                        // double-applying re-delivered WAL, and breaking the loop
                        // would serve silently-stale data forever.
                        eprintln!("replication error: {e:#}; exiting to re-sync (restart me)");
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                        std::process::exit(1);
                    }
                }
            }
        });
    }

    // Periodic replica sampler (rows / bytes / file size).
    {
        let replica = replica.clone();
        let m = metrics.clone();
        spawn_local(async move {
            loop {
                let s = replica.metrics_sample();
                m.replica_rows.store(s.rows, std::sync::atomic::Ordering::Relaxed);
                m.replica_logical_bytes.store(s.logical_bytes, std::sync::atomic::Ordering::Relaxed);
                m.replica_sqlite_file_bytes.store(s.file_bytes, std::sync::atomic::Ordering::Relaxed);
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
        });
    }

    // Accept loop.
    metrics.mark_ready(crate::metrics::ReadyComponent::Restored);
    let listener = TcpListener::bind(&cfg.listen_addr).await?;
    eprintln!("orbit-server listening on {}", cfg.listen_addr);
    metrics.mark_ready(crate::metrics::ReadyComponent::ListenerBound);
    accept_ws_clients(listener, replica, pg, mutators, queries, forwarder, ticks_tx, lmids, None).await
}

/// Accept WebSocket clients and serve each over the shared `replica`, flushing on
/// `ticks`. Shared by the single-process server and the view-syncer.
#[allow(clippy::too_many_arguments)]
async fn accept_ws_clients<B: ReplicaBackend + 'static>(
    listener: TcpListener,
    replica: Rc<B>,
    pg: Rc<tokio_postgres::Client>,
    mutators: Rc<MutatorRegistry>,
    queries: Rc<QueryRegistry>,
    forwarder: Rc<crate::forward::Forwarder>,
    ticks_tx: broadcast::Sender<crate::server::Tick>,
    lmids: crate::server::LmidMap,
    replica_pos: Option<Rc<std::cell::Cell<u64>>>,
) -> Result<()> {
    // Graceful drain (audit Tier 2 — previously crash-only, so every deploy
    // was an instant fleet-wide rehydration storm): on SIGTERM, flip /ready to
    // 503 so the load balancer stops routing new clients here, keep serving
    // existing connections for ORBIT_DRAIN_SECONDS (default 15; clients keep
    // getting pokes and their CVR checkpoints stay fresh), then exit cleanly.
    // New connections are still ACCEPTED during the window (a client that
    // races the LB update gets served rather than refused).
    {
        spawn_local(async move {
            let Ok(mut term) =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            else {
                return;
            };
            term.recv().await;
            if let Some(m) = crate::metrics::node_metrics() {
                m.mark_draining();
            }
            let secs = std::env::var("ORBIT_DRAIN_SECONDS")
                .ok()
                .and_then(|v| v.trim().parse().ok())
                .unwrap_or(15u64);
            eprintln!("SIGTERM: draining for {secs}s, then exiting");
            tokio::time::sleep(Duration::from_secs(secs)).await;
            std::process::exit(0);
        });
    }
    loop {
        let (sock, _) = listener.accept().await?;
        let replica = replica.clone();
        let pg = pg.clone();
        let mutators = mutators.clone();
        let queries = queries.clone();
        let forwarder = forwarder.clone();
        let ticks = ticks_tx.subscribe();
        let lmids = lmids.clone();
        let replica_pos = replica_pos.clone();
        spawn_local(async move {
            match crate::handshake::accept_zero_ws(sock).await {
                Ok((ws, info)) => {
                    let auth = crate::forward::AuthContext { token: info.auth_token, cookie: info.cookie };
                    if let Err(e) = serve_client(
                        ws,
                        replica.as_ref(),
                        Some(pg.as_ref()),
                        &mutators,
                        &queries,
                        &forwarder,
                        &auth,
                        info.desired_queries,
                        info.client_id,
                        info.base_cookie,
                        ticks,
                        &lmids,
                        replica_pos,
                    )
                    .await
                    {
                        eprintln!("connection ended: {e:#}");
                    }
                }
                Err(e) => eprintln!("websocket handshake failed: {e}"),
            }
        });
    }
}

/// How the cluster roles persist and restore replica snapshots. Two
/// strategies: whole-dataset JSON blobs for the in-memory replica (legacy) and
/// streamed SQLite files for the durable replica (bounded memory). Static
/// dispatch — everything runs on the `LocalSet`, no `Send` bounds.
#[allow(async_fn_in_trait)]
pub trait SnapshotStrategy {
    type Backend: ReplicaBackend + 'static;
    /// Persist a snapshot of `replica` reflecting change-stream position `pos`.
    /// Returns the snapshot's size in bytes (metrics).
    async fn write<O: ObjectStore>(&self, store: &O, replica: &Self::Backend, pos: u64) -> Result<u64>;
    /// The position of the latest stored snapshot, if any. The replicator reads
    /// this on startup (last-resort fallback) so its change-stream sequence
    /// continues from where the last instance left off rather than resetting to 0.
    async fn stored_pos<O: ObjectStore>(&self, store: &O) -> Result<Option<u64>>;
    /// Build the backend restored from the latest snapshot (waiting if none
    /// exists yet), returning `(backend, position)`.
    async fn restore<O: ObjectStore>(&self, store: &O, cfg: &ServerConfig) -> Result<(Self::Backend, u64)>;
    /// Drop any local restore state so the next boot re-downloads (called on a
    /// change-stream `Reset` before the crash-only exit — otherwise a durable
    /// node whose position fell out of retention would resume back to the same
    /// stale point forever).
    fn invalidate_local(&self) {}
}

/// Whole-dataset JSON snapshots for the in-memory [`Replica`] (legacy cluster
/// mode). O(dataset) memory on both ends — prefer [`SqliteSnapshots`].
pub struct JsonSnapshots;

impl SnapshotStrategy for JsonSnapshots {
    type Backend = Replica;

    async fn write<O: ObjectStore>(&self, store: &O, replica: &Replica, pos: u64) -> Result<u64> {
        let snap = ReplicaSnapshot { pos, tables: replica.snapshot() }; // sync gather (no await)
        let bytes = snap.to_bytes();
        let n = bytes.len() as u64;
        store.put(ReplicaSnapshot::KEY, bytes).await?;
        Ok(n)
    }

    async fn stored_pos<O: ObjectStore>(&self, store: &O) -> Result<Option<u64>> {
        match store.get(ReplicaSnapshot::KEY).await? {
            Some(bytes) => Ok(Some(ReplicaSnapshot::from_bytes(&bytes)?.pos)),
            None => Ok(None),
        }
    }

    async fn restore<O: ObjectStore>(&self, store: &O, cfg: &ServerConfig) -> Result<(Replica, u64)> {
        let mut replica = Replica::new();
        for t in &cfg.tables {
            replica.add_table(&t.name, t.columns.iter().cloned().collect(), t.primary_key.clone());
        }
        loop {
            if let Some(bytes) = store.get(ReplicaSnapshot::KEY).await? {
                let snap = ReplicaSnapshot::from_bytes(&bytes)?;
                for (table, rows) in snap.tables {
                    for row in rows {
                        replica.seed(&table, row)?;
                    }
                }
                eprintln!("view-syncer restored snapshot @ pos {}", snap.pos);
                return Ok((replica, snap.pos));
            }
            eprintln!("view-syncer: waiting for replicator snapshot…");
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
}

/// SQLite cluster-node options: replica placement + connection tuning +
/// snapshot upload buffering.
#[derive(Clone, Debug)]
pub struct SqliteClusterConfig {
    /// Home of `replica.db` and snapshot staging files (a per-node volume).
    pub dir: std::path::PathBuf,
    /// Page-cache / mmap tuning (`ORBIT_REPLICA_CACHE_MB` / `ORBIT_REPLICA_MMAP_MB`).
    pub opts: crate::sqlite_source::SqliteReplicaOpts,
    /// Multipart part & buffer size for snapshot upload/download — the memory
    /// ceiling of a snapshot transfer is ~2× this. Default 8 MiB.
    pub snapshot_part_size: usize,
    /// Incremental (WAL-segment) backups: each cycle ships only the WAL bytes
    /// appended since the last one, instead of re-uploading the whole replica
    /// file (`ORBIT_BACKUP=full` opts out). Default on.
    pub backup_incremental: bool,
    /// WAL size that triggers a generation roll (checkpoint + fresh full
    /// upload) under incremental backups. Bounds both local WAL growth and
    /// restore replay length. Default 64 MiB (`ORBIT_BACKUP_MAX_WAL_MB`).
    pub max_wal_bytes: u64,
}

impl SqliteClusterConfig {
    pub fn new(dir: impl Into<std::path::PathBuf>) -> Self {
        SqliteClusterConfig {
            dir: dir.into(),
            opts: crate::sqlite_source::SqliteReplicaOpts::default(),
            snapshot_part_size: 8 << 20,
            backup_incremental: true,
            max_wal_bytes: 64 << 20,
        }
    }
}

/// Streamed SQLite-file snapshots for the durable [`SqliteReplica`]. The
/// snapshot is a `VACUUM INTO` copy of the replica file — self-describing: its
/// `orbit_replication_state.pos` is transaction-atomic with its rows — uploaded
/// multipart with O(part_size) memory and restored straight to disk.
pub struct SqliteSnapshots {
    pub cfg: SqliteClusterConfig,
    /// Incremental-backup shipping state for the current generation
    /// (replicator-side; view-syncers only restore). `RefCell`: strategies
    /// live on a single-thread `LocalSet` behind an `Rc`.
    ship: std::cell::RefCell<Option<crate::walship::ShipState>>,
}

impl SqliteSnapshots {
    pub fn new(cfg: SqliteClusterConfig) -> Self {
        SqliteSnapshots { cfg, ship: std::cell::RefCell::new(None) }
    }

    /// One incremental-backup cycle (audit Tier 1.4). First call per process
    /// rolls a generation: checkpoint + ONE full upload. Every later call
    /// ships only the WAL bytes committed since the previous cycle — a 50 GB
    /// replica no longer re-ships 50 GB per interval. Rolls a new generation
    /// when the shipped WAL exceeds `max_wal_bytes` (bounding local WAL growth
    /// and restore replay) or when the WAL restarted outside our control.
    async fn write_incremental<O: ObjectStore>(
        &self,
        store: &O,
        replica: &crate::sqlite_source::SqliteReplica,
        db: &std::path::Path,
        pos: u64,
    ) -> Result<u64> {
        use crate::walship::{self, ShipOutcome};
        // Manual checkpoint control: the base file must stay byte-stable
        // between generation rolls. Idempotent and cheap.
        replica.set_wal_autocheckpoint(0)?;
        let roll = |prev: Option<crate::walship::BackupManifest>| async move {
            replica.checkpoint_truncate().context("checkpoint before generation roll")?;
            walship::new_generation(store, db, pos, self.cfg.snapshot_part_size, prev.as_ref())
                .await
        };
        let current = self.ship.borrow_mut().take();
        let (state, shipped) = match current {
            None => {
                let state = roll(None).await?;
                let n = std::fs::metadata(db).map(|m| m.len()).unwrap_or(0);
                (state, n)
            }
            Some(mut state) => {
                if state.shipped_offset > self.cfg.max_wal_bytes {
                    let prev = state.manifest().clone();
                    let state = roll(Some(prev)).await?;
                    let n = std::fs::metadata(db).map(|m| m.len()).unwrap_or(0);
                    (state, n)
                } else {
                    match walship::ship(store, db, &mut state, pos).await? {
                        ShipOutcome::Shipped { bytes } => (state, bytes),
                        ShipOutcome::NeedsNewGeneration => {
                            let prev = state.manifest().clone();
                            let state = roll(Some(prev)).await?;
                            let n = std::fs::metadata(db).map(|m| m.len()).unwrap_or(0);
                            (state, n)
                        }
                    }
                }
            }
        };
        *self.ship.borrow_mut() = Some(state);
        Ok(shipped)
    }
}

/// The object key holding the latest SQLite snapshot file…
const SQLITE_SNAPSHOT_KEY: &str = "snapshot/latest.db";
/// …and a tiny advisory copy of its position (only consulted when a replicator
/// restarts with neither a changelog checkpoint nor a local replica file).
const SQLITE_SNAPSHOT_POS_KEY: &str = "snapshot/latest.pos";

impl SqliteSnapshots {
    fn build_replica(&self, cfg: &ServerConfig) -> crate::sqlite_source::SqliteReplica {
        let mut replica =
            crate::sqlite_source::SqliteReplica::durable_with(&self.cfg.dir, &self.cfg.opts);
        for t in &cfg.tables {
            replica.add_table(&t.name, t.columns.clone(), t.primary_key.clone());
        }
        replica
    }

    fn replica_db(&self) -> std::path::PathBuf {
        self.cfg.dir.join("replica.db")
    }

    /// Remove stale snapshot staging files from a previous run (crash /
    /// deploy-overlap leftovers).
    fn sweep_staging(&self) {
        if let Ok(entries) = std::fs::read_dir(&self.cfg.dir) {
            for e in entries.flatten() {
                let name = e.file_name();
                let name = name.to_string_lossy();
                if name.starts_with("snapshot.") && name.ends_with(".db") {
                    let _ = std::fs::remove_file(e.path());
                }
            }
        }
    }
}

/// Open the SQLite file at `path` read-only and verify it is a usable
/// snapshot: passes `PRAGMA quick_check` and carries a replication-state row.
/// Returns the snapshot's change-stream position. Blocking work runs off the
/// `LocalSet`.
async fn validate_sqlite_snapshot(path: std::path::PathBuf) -> Result<u64> {
    tokio::task::spawn_blocking(move || -> Result<u64> {
        let conn = rusqlite::Connection::open_with_flags(
            &path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .context("open snapshot for validation")?;
        let ok: String = conn
            .query_row("PRAGMA quick_check", [], |r| r.get(0))
            .context("quick_check")?;
        anyhow::ensure!(ok == "ok", "snapshot failed quick_check: {ok}");
        let pos: i64 = conn
            .query_row("SELECT pos FROM orbit_replication_state WHERE id = 1", [], |r| r.get(0))
            .context("snapshot has no replication-state row")?;
        Ok(pos as u64)
    })
    .await
    .map_err(|e| anyhow::anyhow!("validation task panicked: {e}"))?
}

impl SnapshotStrategy for SqliteSnapshots {
    type Backend = crate::sqlite_source::SqliteReplica;

    async fn write<O: ObjectStore>(
        &self,
        store: &O,
        replica: &crate::sqlite_source::SqliteReplica,
        pos: u64,
    ) -> Result<u64> {
        let src = replica
            .db_path()
            .context("file snapshots need a durable (file-backed) replica")?
            .to_owned();
        if self.cfg.backup_incremental {
            return self.write_incremental(store, replica, &src, pos).await;
        }
        // Unique staging name: deploy overlap means two replicators may
        // snapshot concurrently into the same volume.
        let tmp = self.cfg.dir.join(format!(
            "snapshot.{}.{}.db",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        let result: Result<u64> = async {
            crate::sqlite_source::SqliteReplica::backup_to(src, tmp.clone()).await?;
            let n = tokio::fs::metadata(&tmp).await.map(|m| m.len()).unwrap_or(0);
            crate::objectstore::put_file(store, SQLITE_SNAPSHOT_KEY, &tmp, self.cfg.snapshot_part_size)
                .await?;
            // Advisory pos, written AFTER the db object so it never over-claims.
            // (The authoritative pos rides inside the file itself.)
            store.put(SQLITE_SNAPSHOT_POS_KEY, pos.to_string().into_bytes()).await?;
            Ok(n)
        }
        .await;
        let _ = tokio::fs::remove_file(&tmp).await;
        result
    }

    async fn stored_pos<O: ObjectStore>(&self, store: &O) -> Result<Option<u64>> {
        // Incremental manifest first; legacy full-snapshot pos as fallback.
        if let Some(m) = crate::walship::load_manifest(store).await? {
            return Ok(Some(m.pos_hint));
        }
        Ok(store
            .get(SQLITE_SNAPSHOT_POS_KEY)
            .await?
            .and_then(|b| String::from_utf8(b).ok())
            .and_then(|s| s.trim().parse().ok()))
    }

    async fn restore<O: ObjectStore>(
        &self,
        store: &O,
        cfg: &ServerConfig,
    ) -> Result<(crate::sqlite_source::SqliteReplica, u64)> {
        std::fs::create_dir_all(&self.cfg.dir).ok();
        self.sweep_staging();
        // Which config tables a restored replica hasn't initial-synced. A
        // view-syncer can't backfill from Postgres itself — it must restore a
        // snapshot taken AFTER the replicator backfilled the new table.
        let missing = |replica: &crate::sqlite_source::SqliteReplica| -> Vec<String> {
            match replica.synced_tables() {
                Some(synced) => cfg
                    .tables
                    .iter()
                    .filter(|t| !synced.contains(&t.name))
                    .map(|t| t.name.clone())
                    .collect(),
                None => Vec::new(),
            }
        };
        // Local short-circuit: a durable view-syncer that recorded its applied
        // position resumes by DELTA from its own replica file — no download.
        if self.replica_db().exists() {
            let replica = self.build_replica(cfg);
            if let Some(p) = replica.resume_pos() {
                let miss = missing(&replica);
                if miss.is_empty() {
                    eprintln!(
                        "view-syncer: resuming from local replica.db @ pos {p} (skipping snapshot download)"
                    );
                    return Ok((replica, p));
                }
                // Tables were added to the config: the local file predates
                // them (audit Tier 0.5) — fall through to snapshot restore.
                eprintln!(
                    "view-syncer: local replica.db missing newly-added tables {miss:?}; re-restoring from snapshot"
                );
            }
            // Foreign, half-built, or table-incomplete file: close the
            // connection, then clear it.
            drop(replica);
            self.invalidate_local();
        }
        let tmp = self.cfg.dir.join(format!("replica.db.tmp.{}", std::process::id()));
        loop {
            // Incremental backups: assemble base + WAL segments from the
            // manifest. Falls back to the legacy full-snapshot object when no
            // manifest exists (older replicator still running).
            let fetched = match crate::walship::restore(store, &tmp).await {
                Ok(true) => Ok(true),
                Ok(false) => crate::objectstore::get_to_file(store, SQLITE_SNAPSHOT_KEY, &tmp).await,
                Err(e) => Err(e),
            };
            match fetched {
                Ok(true) => match validate_sqlite_snapshot(tmp.clone()).await {
                    Ok(pos) => {
                        self.invalidate_local();
                        std::fs::rename(&tmp, self.replica_db())
                            .context("rename snapshot into place")?;
                        let replica = self.build_replica(cfg);
                        let miss = missing(&replica);
                        if !miss.is_empty() {
                            // The replicator hasn't published a snapshot that
                            // includes the newly-added tables yet — keep
                            // waiting rather than serving silent empty history.
                            eprintln!(
                                "view-syncer: snapshot @ pos {pos} predates newly-added tables {miss:?}; waiting for the replicator to backfill + re-snapshot"
                            );
                            drop(replica);
                            self.invalidate_local();
                        } else {
                            eprintln!("view-syncer restored snapshot @ pos {pos}");
                            return Ok((replica, pos));
                        }
                    }
                    Err(e) => {
                        // Corrupt / torn / garbage object: never rename it into
                        // place; retry with a fresh GET.
                        eprintln!("downloaded snapshot invalid ({e:#}); retrying");
                        let _ = tokio::fs::remove_file(&tmp).await;
                    }
                },
                Ok(false) => eprintln!(
                    "view-syncer: waiting for replicator snapshot at {SQLITE_SNAPSHOT_KEY} — is the replicator running ORBIT_REPLICA=sqlite too?"
                ),
                Err(e) => eprintln!("snapshot download failed: {e:#}; retrying"),
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    fn invalidate_local(&self) {
        let db = self.replica_db();
        for suffix in ["", "-wal", "-shm"] {
            let mut p = db.clone().into_os_string();
            p.push(suffix);
            let _ = std::fs::remove_file(std::path::PathBuf::from(p));
        }
    }
}

/// Run a **replicator** node: owns the single Postgres replication slot, applies
/// WAL to its replica, broadcasts every change to view-syncers over
/// `change_stream_addr`, and snapshots the replica to `store` every
/// `snapshot_interval`. Does not serve WebSocket clients. Mirrors Zero's
/// `replication-manager`.
pub async fn run_replicator<O: ObjectStore + 'static>(
    cfg: ServerConfig,
    store: O,
    change_stream_addr: String,
    snapshot_interval: Duration,
) -> Result<()> {
    let mut replica = Replica::new();
    for t in &cfg.tables {
        replica.add_table(&t.name, t.columns.iter().cloned().collect(), t.primary_key.clone());
    }
    let local = LocalSet::new();
    local
        .run_until(run_replicator_inner(
            cfg,
            store,
            change_stream_addr,
            snapshot_interval,
            Rc::new(JsonSnapshots),
            replica,
        ))
        .await
}

/// [`run_replicator`] over a durable SQLite replica with streamed SQLite-file
/// snapshots: steady memory is O(page cache), snapshot transfer memory is
/// O(part size), and a restart resumes from the local file (no re-sync).
pub async fn run_replicator_sqlite<O: ObjectStore + 'static>(
    cfg: ServerConfig,
    store: O,
    change_stream_addr: String,
    snapshot_interval: Duration,
    sqlite: SqliteClusterConfig,
) -> Result<()> {
    let mut replica = crate::sqlite_source::SqliteReplica::durable_with(&sqlite.dir, &sqlite.opts);
    for t in &cfg.tables {
        replica.add_table(&t.name, t.columns.clone(), t.primary_key.clone());
    }
    let local = LocalSet::new();
    local
        .run_until(run_replicator_inner(
            cfg,
            store,
            change_stream_addr,
            snapshot_interval,
            Rc::new(SqliteSnapshots::new(sqlite)),
            replica,
        ))
        .await
}

async fn run_replicator_inner<O: ObjectStore + 'static, S: SnapshotStrategy + 'static>(
    cfg: ServerConfig,
    store: O,
    change_stream_addr: String,
    snapshot_interval: Duration,
    snap: Rc<S>,
    replica: S::Backend,
) -> Result<()> {
    // Metrics + readiness first: /ready answers 503 through initial sync and
    // the first snapshot write.
    let metrics = crate::metrics::Metrics::new(crate::metrics::Role::Replicator);
    crate::metrics::set_node_metrics(metrics.clone());
    if let Some(addr) = crate::metrics::metrics_listen_from_env() {
        let m = metrics.clone();
        spawn_local(async move {
            if let Err(e) = crate::metrics::serve_metrics(addr, m).await {
                eprintln!("metrics server error: {e:#}");
            }
        });
    }

    let (pg, driver) = tls::connect(&cfg.conn_str(), cfg.tls).await?;
    tokio::spawn(driver);

    let table_names: Vec<&str> = cfg.tables.iter().map(|t| t.name.as_str()).collect();
    create_publication(&pg, &cfg.publication, &table_names).await?;
    let (start_lsn, exported) = crate::pg::ensure_slot_with_snapshot(
        &pg, &cfg.host, cfg.port, &cfg.user, &cfg.database,
        cfg.password.as_deref(), cfg.tls, &cfg.slot,
    )
    .await
    .context("creating logical replication slot — is wal_level=logical?")?;

    let replica = Rc::new(replica);
    let store = Rc::new(store);

    // Durable change-log: lets view-syncers resume by *delta* across a restart
    // instead of re-restoring. Its checkpoint is the authoritative resume point —
    // the change-stream seq continues from there (falling back to the snapshot
    // watermark, then 0), so positions stay monotonic across restarts. `dedup_lsn`
    // is the WAL position of the last logged change: on restart the slot re-delivers
    // from its confirmed LSN, so we skip events at/below it (already logged) to keep
    // each change's pos stable.
    // Opt out with ORBIT_DISABLE_CHANGELOG (e.g. a deployment that prefers the
    // re-restore-on-restart behavior over the extra PG write load, or to A/B the
    // log's cost).
    let log: Option<Arc<crate::changelog::PgChangeLog>> =
        if std::env::var("ORBIT_DISABLE_CHANGELOG").is_ok() {
            eprintln!("durable change-log DISABLED (ORBIT_DISABLE_CHANGELOG)");
            None
        } else {
            // The CDC log defaults to the source Postgres, which re-writes
            // every replicated change into the database it was read from
            // (audit Tier 1.8 — measured 392 MB of change log vs 375 MB of
            // data in 12 days). Point ORBIT_CDC_PG at a separate (cheap,
            // unreplicated) Postgres to take that write load off the source.
            let cdc_conn = std::env::var("ORBIT_CDC_PG").unwrap_or_else(|_| cfg.conn_str());
            if std::env::var("ORBIT_CDC_PG").is_ok() {
                eprintln!("durable change-log using dedicated CDC database (ORBIT_CDC_PG)");
            }
            Some(Arc::new(
                crate::changelog::PgChangeLog::open(
                    cdc_conn,
                    format!("orbit_change_log_{}", cfg.slot),
                    cfg.tls,
                )
                .await?,
            ))
        };
    let checkpoint = match &log {
        Some(l) => l.checkpoint().await?,
        None => None,
    };
    // Resume seq: changelog checkpoint → the durable replica's own recorded
    // position → the stored snapshot's advisory position → 0.
    let start_seq = match checkpoint {
        Some((pos, _)) => pos,
        None => match replica.resume_pos() {
            Some(pos) => pos,
            None => snap.stored_pos(store.as_ref()).await?.unwrap_or(0),
        },
    };
    let mut dedup_lsn = checkpoint.map(|(_, lsn)| lsn).unwrap_or(0);

    // A durable backend that recorded a watermark resumes from the slot instead
    // of re-syncing; a fresh sync first CLEARS the backend (initial sync only
    // upserts, so rows deleted upstream while offline would otherwise survive
    // as phantoms). Recording `start_seq` with the fresh sync keeps the
    // snapshot file's position aligned with the continued stream.
    let mut apply_watermark = replica.resume_watermark().unwrap_or(0);
    if apply_watermark > 0 {
        eprintln!(
            "durable replica: resuming from watermark {apply_watermark} (skipping initial sync)"
        );
        // Tables added to the config AFTER the first sync still need their
        // pre-existing rows (the stream only carries new changes).
        backfill_missing_tables(&pg, replica.as_ref(), &cfg).await?;
    } else {
        initial_sync_all(&pg, replica.as_ref(), &cfg, exported.as_ref(), start_seq).await?;
    }
    drop(exported); // the snapshot's replication connection may close now

    // Ring/broadcast tuning from ORBIT_CHANGE_RING_BYTES / ORBIT_CHANGE_RING_CAPACITY /
    // ORBIT_BROADCAST_CAP; the struct defaults match CHANGE_RING_CAPACITY + 64 MiB.
    let server = ChangeStreamServer::with_config(
        crate::changestream::ChangeStreamConfig::from_env(),
        start_seq,
        log.clone(),
    );
    {
        // Bind before spawning the accept loop so the readiness component
        // reflects an actually-listening change stream.
        let listener = TcpListener::bind(&change_stream_addr).await?;
        eprintln!("change-stream listening on {change_stream_addr}");
        metrics.mark_ready(crate::metrics::ReadyComponent::ListenerBound);
        let server = server.clone();
        tokio::spawn(async move {
            if let Err(e) = server.serve_on(listener).await {
                eprintln!("change-stream server error: {e:#}");
            }
        });
    }

    // The change-stream position of the most recent snapshot upload — a
    // pruning floor (restore reservation): a restoring view-syncer resumes
    // from at least this position.
    let last_snapshot_pos = Arc::new(std::sync::atomic::AtomicU64::new(start_seq));

    // Refresh the snapshot with the freshly-synced replica at the continued
    // watermark, so view-syncers restore current data aligned with `start_seq`.
    let snap_bytes = snap.write(store.as_ref(), replica.as_ref(), start_seq).await?;
    metrics.snapshot_bytes.store(snap_bytes, std::sync::atomic::Ordering::Relaxed);
    metrics.mark_ready(crate::metrics::ReadyComponent::Restored);
    eprintln!("replicator change-stream resuming at seq {start_seq} (dedup lsn {dedup_lsn})");

    // Periodic sampler: ring/broadcast/changelog/replica gauges.
    {
        let server = server.clone();
        let replica = replica.clone();
        let m = metrics.clone();
        let log_stats = log.as_ref().map(|l| l.stats());
        spawn_local(async move {
            use std::sync::atomic::Ordering::Relaxed;
            loop {
                let (entries, bytes) = server.ring_stats();
                m.change_ring_entries.store(entries as u64, Relaxed);
                m.change_ring_bytes.store(bytes as u64, Relaxed);
                m.change_stream_seq.store(server.current_seq(), Relaxed);
                m.replica_pos.store(server.current_seq(), Relaxed);
                m.change_stream_subscribers.store(server.subscriber_count() as u64, Relaxed);
                if let Some(s) = &log_stats {
                    m.changelog_queue_depth.store(s.queued_events.load(Relaxed), Relaxed);
                    m.changelog_queue_bytes.store(s.queued_bytes.load(Relaxed), Relaxed);
                }
                let s = replica.metrics_sample();
                m.replica_rows.store(s.rows, Relaxed);
                m.replica_logical_bytes.store(s.logical_bytes, Relaxed);
                m.replica_sqlite_file_bytes.store(s.file_bytes, Relaxed);
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        });
    }

    // Retention: prune the durable log by SUBSCRIBER-ACK CONSENSUS (audit
    // Tier 1.6 — a live-but-slow view-syncer is never force-Reset just
    // because it lagged a fixed window) with two additional floors:
    //  * the latest object-store snapshot position (restore reservation: a
    //    node restoring that snapshot will resume from it), and
    //  * LOG_RETENTION as the HARD upper bound on retention, so a wedged or
    //    zombie subscriber can't pin the log forever (it re-restores instead).
    if let Some(log) = log.clone() {
        let server = server.clone();
        let snap_floor = last_snapshot_pos.clone();
        spawn_local(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(60)).await;
                let seq = server.current_seq();
                let hard_floor = seq.saturating_sub(LOG_RETENTION);
                let consensus = server.min_subscriber_ack().unwrap_or(seq);
                let reservation = snap_floor.load(std::sync::atomic::Ordering::Relaxed);
                let keep_from = consensus.min(reservation).max(hard_floor);
                if keep_from > 0 {
                    if let Err(e) = log.prune_before(keep_from).await {
                        eprintln!("change-log prune failed: {e:#}");
                    }
                }
            }
        });
    }

    // Periodic snapshot loop — captures the current change-stream position.
    // Fixed cadence (a slow cycle doesn't silently degrade the interval to
    // duration + interval), with a lag warning and WEDGE DETECTION: if no
    // backup has succeeded for max(5×interval, 5 min), the node crashes out —
    // silently running without restorable backups is the worst failure mode
    // (mirrors Zero's litestream backup monitor).
    {
        let store = store.clone();
        let replica = replica.clone();
        let server = server.clone();
        let snap = snap.clone();
        let m = metrics.clone();
        let snap_pos = last_snapshot_pos.clone();
        spawn_local(async move {
            let wedge_limit = snapshot_interval
                .saturating_mul(5)
                .max(Duration::from_secs(300));
            let mut ticker = tokio::time::interval(snapshot_interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticker.tick().await; // first tick fires immediately; boot already snapshotted
            let mut last_success = tokio::time::Instant::now();
            loop {
                ticker.tick().await;
                let pos = server.current_seq();
                let started = tokio::time::Instant::now();
                match snap.write(store.as_ref(), replica.as_ref(), pos).await {
                    Ok(n) => {
                        m.snapshot_bytes.store(n, std::sync::atomic::Ordering::Relaxed);
                        snap_pos.store(pos, std::sync::atomic::Ordering::Relaxed);
                        last_success = tokio::time::Instant::now();
                        let took = started.elapsed();
                        if took > snapshot_interval {
                            eprintln!(
                                "backup cycle took {took:?} (> interval {snapshot_interval:?}); cadence is lagging"
                            );
                        }
                    }
                    Err(e) => {
                        eprintln!("backup cycle failed: {e:#}");
                        if last_success.elapsed() > wedge_limit {
                            eprintln!(
                                "backup wedged: no successful backup for {:?} (> {:?}); exiting so the orchestrator restarts with a fresh generation",
                                last_success.elapsed(),
                                wedge_limit
                            );
                            tokio::time::sleep(Duration::from_secs(1)).await;
                            std::process::exit(1);
                        }
                    }
                }
                m.snapshot_age_seconds.store(
                    last_success.elapsed().as_secs(),
                    std::sync::atomic::Ordering::Relaxed,
                );
            }
        });
    }

    // Replication pump: apply each event, then publish it to view-syncers.
    // Reconnect the stream in-process on any error instead of exiting — exiting
    // restarts the process, which would reset the change-stream seq and force every
    // view-syncer to re-restore. Reconnecting keeps `server` (and its seq) alive, so
    // a transient PG/stream blip is invisible to view-syncers (PG resumes the slot
    // from its confirmed LSN; re-delivered changes apply idempotently).
    let (host, port, user, db) = (cfg.host.clone(), cfg.port, cfg.user.clone(), cfg.database.clone());
    let (slot, publication) = (cfg.slot.clone(), cfg.publication.clone());
    let (password, tls_mode) = (cfg.password.clone(), cfg.tls);
    // The stream position recorded with the replica's last committed txn —
    // carried forward across publish-skipped (already-logged) replays.
    let mut last_committed_pos: u64 = start_seq;
    loop {
        // Retry START_REPLICATION while the slot is held by a departing instance
        // (redeploy overlap). The loser waits instead of fighting — see create_slot.
        let mut stream = match ReplicationStream::start_with_tls(
            &host, port, &user, &db, &slot, &publication, start_lsn, password.as_deref(), tls_mode,
        )
        .await
        {
            Ok(s) => s,
            Err(e) => {
                eprintln!("replication start failed ({e:#}); slot busy or PG blip, retrying in 3s");
                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                continue;
            }
        };
        eprintln!("replicator streaming on slot {slot}; change-stream at {change_stream_addr}");
        // STREAMING per-transaction pipeline (audit Tier 1.2): a Begin's
        // position is the transaction's COMMIT LSN (`final_lsn`), so both
        // decisions — publish (> `dedup_lsn`: not yet durably logged) and
        // apply (> `apply_watermark`: not yet in the replica) — are made
        // upfront, and every event flows straight through: applied into the
        // open storage transaction and published to the stream AS IT ARRIVES.
        // Nothing is buffered, so a 100 MB upstream transaction costs O(one
        // event) replicator memory instead of being fully resident. The
        // replicator serves no client pipelines, so a mid-transaction cut is
        // a plain storage rollback; consumers discard the partial publish at
        // the re-delivered Begin.
        //
        // Crash-recovery invariant: the replica commits synchronously BEFORE
        // the changelog's async flush, so `apply_watermark >= dedup_lsn` — a
        // re-delivered txn may need publishing (to fill the log) without
        // re-applying over newer state.
        let mut txn: Option<(u64, bool, bool)> = None; // (final_lsn, publish, apply)
        loop {
            // Apply errors are fatal (rollback + clean error return): never
            // commit a watermark over a torn apply, never panic.
            match stream.next_event().await {
                Ok((final_lsn, LogicalEvent::Begin)) => {
                    if let Some((l, _, apply)) = txn.take() {
                        if apply {
                            replica.rollback_txn();
                        }
                        eprintln!("discarding partial transaction at lsn {l} (upstream re-sent Begin)");
                    }
                    let publish = final_lsn > dedup_lsn;
                    let apply = final_lsn > apply_watermark;
                    if publish || apply {
                        if apply {
                            if let Err(err) = replica.begin_txn() {
                                replica.rollback_txn();
                                return Err(err.context(format!(
                                    "replica begin_txn failed at lsn {final_lsn} (restart to re-sync)"
                                )));
                            }
                        }
                        if publish {
                            server.publish(final_lsn, LogicalEvent::Begin).await;
                        }
                        txn = Some((final_lsn, publish, apply));
                    } // else: whole txn already applied AND logged — skip its events
                }
                Ok((_lsn, LogicalEvent::Commit)) => {
                    if let Some((final_lsn, publish, apply)) = txn.take() {
                        if publish {
                            server.publish(final_lsn, LogicalEvent::Commit).await;
                            dedup_lsn = final_lsn;
                        }
                        if apply {
                            if let Err(err) = replica
                                .apply(LogicalEvent::Commit)
                                .and_then(|()| {
                                    // The pump is single-threaded, so right after
                                    // publishing, `current_seq()` is exactly this
                                    // commit's stream position. On a publish-skip
                                    // replay, carry the previous position (a safe,
                                    // idempotent resume point).
                                    let pos = if publish {
                                        server.current_seq()
                                    } else {
                                        last_committed_pos
                                    };
                                    replica.commit_txn(final_lsn, pos)?;
                                    apply_watermark = final_lsn;
                                    last_committed_pos = pos;
                                    Ok(())
                                })
                            {
                                replica.rollback_txn();
                                return Err(err.context(format!(
                                    "replica commit failed at lsn {final_lsn}; rolled back (restart to re-sync)"
                                )));
                            }
                        }
                    }
                    // Ack only durably-logged WAL: with a durable log, the
                    // writer's flushed watermark; without one (ring-only mode,
                    // where any gap already forces a snapshot Reset), the
                    // applied position. A crash between receipt and flush then
                    // replays from the slot instead of leaving a silent hole
                    // that delta-resuming view-syncers would skip over.
                    match &log {
                        Some(l) => stream.confirm(l.durable_lsn()),
                        None => stream.confirm(dedup_lsn),
                    }
                }
                Ok((l, ev)) => {
                    if let Some((final_lsn, publish, apply)) = txn.as_ref() {
                        if *apply {
                            if let Err(err) = replica.apply(ev.clone()) {
                                let fl = *final_lsn;
                                replica.rollback_txn();
                                return Err(err.context(format!(
                                    "replica apply failed at lsn {fl}; rolled back (restart to re-sync)"
                                )));
                            }
                        }
                        if *publish {
                            // Awaits only when the durable log's byte budget is
                            // full — the intended backpressure point that parks
                            // WAL consumption (see changelog module doc).
                            server.publish(l, ev).await;
                        }
                    }
                }
                Err(e) => {
                    if let Some((_, _, apply)) = txn.take() {
                        if apply {
                            replica.rollback_txn();
                        }
                    }
                    eprintln!("replication stream error ({e:#}); reconnecting in 2s");
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                    break;
                }
            }
        }
    }
}

/// Run a **view-syncer** node: restore the replica from the latest object-store
/// snapshot, follow the replicator's change-stream (no Postgres slot of its own),
/// and serve WebSocket clients. Horizontally scalable. Mirrors Zero's `view-syncer`.
pub async fn run_view_syncer<O: ObjectStore + 'static>(
    cfg: ServerConfig,
    store: O,
    change_stream_addr: String,
    mutators: MutatorRegistry,
    queries: QueryRegistry,
) -> Result<()> {
    let local = LocalSet::new();
    local
        .run_until(run_view_syncer_inner(
            cfg,
            store,
            change_stream_addr,
            mutators,
            queries,
            Rc::new(JsonSnapshots),
        ))
        .await
}

/// [`run_view_syncer`] over a durable SQLite replica restored from streamed
/// SQLite-file snapshots. A restart with an intact local `replica.db` resumes
/// by delta — no snapshot download at all.
pub async fn run_view_syncer_sqlite<O: ObjectStore + 'static>(
    cfg: ServerConfig,
    store: O,
    change_stream_addr: String,
    mutators: MutatorRegistry,
    queries: QueryRegistry,
    sqlite: SqliteClusterConfig,
) -> Result<()> {
    let local = LocalSet::new();
    local
        .run_until(run_view_syncer_inner(
            cfg,
            store,
            change_stream_addr,
            mutators,
            queries,
            Rc::new(SqliteSnapshots::new(sqlite)),
        ))
        .await
}

async fn run_view_syncer_inner<O: ObjectStore + 'static, S: SnapshotStrategy + 'static>(
    cfg: ServerConfig,
    store: O,
    change_stream_addr: String,
    mutators: MutatorRegistry,
    queries: QueryRegistry,
    snap: Rc<S>,
) -> Result<()> {
    // Metrics + readiness first, so /ready answers 503 through the whole boot
    // (snapshot restore + change-stream catch-up can take a while).
    let metrics = crate::metrics::Metrics::new(crate::metrics::Role::ViewSyncer);
    crate::metrics::set_node_metrics(metrics.clone());
    if let Some(addr) = crate::metrics::metrics_listen_from_env() {
        let m = metrics.clone();
        spawn_local(async move {
            if let Err(e) = crate::metrics::serve_metrics(addr, m).await {
                eprintln!("metrics server error: {e:#}");
            }
        });
    }

    // Track peak RSS across the restore (the acceptance test asserts restore
    // stays inside the container budget).
    let restore_sampler = {
        let m = metrics.clone();
        spawn_local(async move {
            loop {
                if let Some(rss) = crate::metrics::rss_bytes() {
                    m.snapshot_restore_peak_rss_bytes.fetch_max(rss, std::sync::atomic::Ordering::Relaxed);
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
    };
    let (replica, watermark) = snap.restore(&store, &cfg).await?;
    restore_sampler.abort();
    metrics.mark_ready(crate::metrics::ReadyComponent::Restored);
    let replica = Rc::new(replica);

    // Postgres connection only for client mutation write-through (no slot here);
    // those writes flow back through the replicator's change-stream.
    let (pg, driver) = tls::connect(&cfg.conn_str(), cfg.tls).await?;
    tokio::spawn(driver);
    // Shared CVR tables (per-client view) so a client reconnecting to this node —
    // having last been on another — resumes as a delta.
    crate::cvr::PgCvrStore::ensure_schema(&pg).await?;
    let pg = Rc::new(pg);
    let mutators = Rc::new(mutators);
    let queries = Rc::new(queries);
    let forwarder = Rc::new(crate::forward::Forwarder::new(cfg.forward_config()));
    let (ticks_tx, _) = broadcast::channel::<crate::server::Tick>(1024);
    // Per-client lastMutationIDs, advanced from replicated `orbit_client_mutations`
    // events forwarded through the change-stream.
    let lmids: crate::server::LmidMap = Rc::new(std::cell::RefCell::new(std::collections::HashMap::new()));
    // This node's applied change-stream position — the staleness gate compares it
    // to each connecting client's persisted view position.
    let replica_pos: Rc<std::cell::Cell<u64>> = Rc::new(std::cell::Cell::new(watermark));

    // CVR GC: ephemeral clients (each non-persisted tab gets a random clientID)
    // leave their materialized views in Postgres forever without a sweep.
    {
        let gc_pg = Rc::clone(&pg);
        spawn_local(async move {
            loop {
                match crate::cvr::PgCvrStore::gc_stale_clients(&gc_pg, 7).await {
                    Ok(n) if n > 0 => eprintln!("cvr gc: swept {n} stale client rows"),
                    Ok(_) => {}
                    Err(e) => eprintln!("cvr gc failed (retrying next cycle): {e:#}"),
                }
                tokio::time::sleep(Duration::from_secs(3600)).await;
            }
        });
    }

    // Change-stream pump: apply remote changes to the local replica + tick.
    {
        let replica = replica.clone();
        let ticks_tx = ticks_tx.clone();
        let lmids = lmids.clone();
        let addr = change_stream_addr.clone();
        let replica_pos = replica_pos.clone();
        let snap = snap.clone();
        let metrics = metrics.clone();
        spawn_local(async move {
            let mut watermark = watermark;
            replica_pos.set(watermark);
            // Consecutive read failures at the SAME watermark: a deterministic
            // decode error (e.g. serde skew during a rolling upgrade) would
            // otherwise reconnect-loop forever at a frozen watermark while the
            // WS server serves ever-staler data. Crash-only after a few tries.
            let mut stuck_failures = 0u32;
            loop {
                // Bounded connect: DNS/TCP to the replicator can hang
                // indefinitely right after a container restart (observed with
                // Docker's embedded DNS) — an unbounded connect wedges the
                // pump forever while the WS server serves ever-staler data.
                let connect = tokio::time::timeout(
                    Duration::from_secs(5),
                    ChangeStreamClient::connect(&addr, watermark),
                );
                let mut client = match connect.await {
                    Ok(Ok(c)) => c,
                    Ok(Err(e)) => {
                        eprintln!("change-stream connect failed: {e:#}; retrying");
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        continue;
                    }
                    Err(_) => {
                        eprintln!("change-stream connect timed out; retrying");
                        continue;
                    }
                };
                let reconnect_watermark = watermark;
                let mut last_ack = std::time::Instant::now();
                // One transaction at a time. Small transactions buffer and
                // apply in one synchronous slice at Commit (no awaits between
                // events → a concurrent `subscribe` hydration on this LocalSet
                // can never read a torn mid-transaction state). Transactions
                // beyond the buffer cap STREAM through bounded memory under
                // the node's replica-consistency write lock, which parks
                // hydrations and tick flushes for the transaction's duration
                // instead (audit Tier 1.2 — previously every view-syncer held
                // the entire transaction in RAM simultaneously).
                let mut txn: Option<TxnApply> = None;
                loop {
                    match client.next().await {
                        Ok(Some(ChangeMsg::Change { pos, event })) => {
                            if matches!(event, LogicalEvent::Begin) {
                                if let Some(t) = txn.take() {
                                    if t.abort(replica.as_ref()) {
                                        // Streamed pushes can't be un-pushed
                                        // from live pipelines: crash-only.
                                        eprintln!("partial streamed transaction cut by upstream; exiting to re-restore snapshot");
                                        tokio::time::sleep(Duration::from_secs(1)).await;
                                        std::process::exit(1);
                                    }
                                }
                                txn = Some(TxnApply::new());
                                continue;
                            }
                            if !matches!(event, LogicalEvent::Commit) {
                                // Tolerate a resume that lands mid-stream
                                // without a Begin (legacy replicators):
                                // open a transaction implicitly.
                                let t = txn.get_or_insert_with(TxnApply::new);
                                if let Err(e) = t.push(replica.as_ref(), &lmids, event).await {
                                    replica.rollback_txn();
                                    eprintln!("replica apply error: {e:#}; rolled back; exiting to re-restore snapshot");
                                    tokio::time::sleep(Duration::from_secs(1)).await;
                                    std::process::exit(1);
                                }
                                continue;
                            }
                            // Commit: one storage transaction per upstream
                            // transaction — a durable backend records its
                            // applied position (`commit_txn(0, watermark)`)
                            // atomically with the rows, enabling delta resume
                            // from the local file.
                            let Some(t) = txn.take() else {
                                watermark = pos;
                                continue;
                            };
                            match t.commit(replica.as_ref(), &lmids, 0, pos) {
                                Ok(tick) => {
                                    watermark = pos;
                                    if let Some(tick) = tick {
                                        let _ = ticks_tx.send(tick);
                                    }
                                }
                                Err(e) => {
                                    // Same crash-only policy as Reset: rollback,
                                    // exit, restore fresh — never serve a torn or
                                    // silently-stale replica.
                                    replica.rollback_txn();
                                    eprintln!("replica apply error: {e:#}; rolled back; exiting to re-restore snapshot");
                                    tokio::time::sleep(Duration::from_secs(1)).await;
                                    std::process::exit(1);
                                }
                            }
                            replica_pos.set(watermark);
                            metrics.replica_pos.store(watermark, std::sync::atomic::Ordering::Relaxed);
                            // Throttled ack: tells the replicator how far this
                            // node has durably applied, so log pruning keeps
                            // what live subscribers still need (Tier 1.6).
                            if last_ack.elapsed() >= Duration::from_secs(2) {
                                let _ = client.ack(watermark).await;
                                last_ack = std::time::Instant::now();
                            }
                            // Mixed-version fallback: an old replicator never
                            // sends CaughtUp — the first applied live commit
                            // still proves we're current.
                            metrics.mark_ready(crate::metrics::ReadyComponent::CaughtUp);
                            stuck_failures = 0;
                        }
                        Ok(Some(ChangeMsg::CaughtUp { .. })) => {
                            // Backlog replayed — this node serves current data.
                            metrics.mark_ready(crate::metrics::ReadyComponent::CaughtUp);
                        }
                        Ok(Some(ChangeMsg::Reset)) => {
                            // Resume point can't be served (replicator restarted, or
                            // we fell too far behind). Exit so the orchestrator
                            // restarts us and we re-restore the latest snapshot — a
                            // bare `return` would only kill this task and leave the
                            // WS server happily serving stale data forever.
                            // Invalidate local restore state first: a durable
                            // replica would otherwise short-circuit right back
                            // to the same stale position on every restart.
                            snap.invalidate_local();
                            eprintln!("change-stream Reset (stale resume point, e.g. replicator restarted); exiting to re-restore snapshot");
                            tokio::time::sleep(Duration::from_secs(1)).await;
                            std::process::exit(1);
                        }
                        Ok(None) => break, // disconnected — reconnect from watermark
                        Err(e) => {
                            if watermark == reconnect_watermark {
                                stuck_failures += 1;
                                if stuck_failures >= 5 {
                                    // Same rationale as Reset: without this, a
                                    // durable replica resumes at the same stuck
                                    // watermark (and the same decode failure)
                                    // on every restart.
                                    snap.invalidate_local();
                                    eprintln!("change-stream read failed {stuck_failures}x at watermark {watermark} ({e:#}); exiting to re-restore snapshot");
                                    tokio::time::sleep(Duration::from_secs(1)).await;
                                    std::process::exit(1);
                                }
                            } else {
                                stuck_failures = 0;
                            }
                            eprintln!("change-stream read error: {e:#}; reconnecting");
                            break;
                        }
                    }
                }
                // Disconnected mid-transaction: a buffered transaction aborts
                // for free (replay resumes from the committed watermark); a
                // STREAMED one has already pushed into live pipelines and
                // cannot be un-pushed — crash-only.
                if let Some(t) = txn.take() {
                    if t.abort(replica.as_ref()) {
                        eprintln!("disconnected mid streamed transaction; exiting to re-restore snapshot");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        std::process::exit(1);
                    }
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        });
    }

    // Periodic replica sampler (rows / bytes / file size).
    {
        let replica = replica.clone();
        let m = metrics.clone();
        spawn_local(async move {
            loop {
                let s = replica.metrics_sample();
                m.replica_rows.store(s.rows, std::sync::atomic::Ordering::Relaxed);
                m.replica_logical_bytes.store(s.logical_bytes, std::sync::atomic::Ordering::Relaxed);
                m.replica_sqlite_file_bytes.store(s.file_bytes, std::sync::atomic::Ordering::Relaxed);
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
        });
    }

    let listener = TcpListener::bind(&cfg.listen_addr).await?;
    eprintln!("view-syncer listening on {}", cfg.listen_addr);
    metrics.mark_ready(crate::metrics::ReadyComponent::ListenerBound);
    accept_ws_clients(listener, replica, pg, mutators, queries, forwarder, ticks_tx, lmids, Some(replica_pos)).await
}
