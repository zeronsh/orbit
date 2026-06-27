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
use crate::pg::{create_publication, create_slot, initial_sync_backend};
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
use tokio_postgres::NoTls;

/// How many recent changes the replicator keeps IN MEMORY (the ring) for fast
/// view-syncer resume. Kept small so the footprint stays bounded — the `VecDeque`
/// never shrinks, so a 1M cap let it climb toward ~100MB+ under a high change rate
/// (e.g. cursor presence). A view-syncer lagging further than this resumes from the
/// durable change-log instead.
const CHANGE_RING_CAPACITY: usize = 65_536;

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
    fn conn_str(&self) -> String {
        let mut s = format!(
            "host={} port={} user={} dbname={}",
            self.host, self.port, self.user, self.database
        );
        // Managed Postgres (e.g. Railway) requires a password; pick it up from the
        // environment so the config struct stays unchanged. Local trust-auth PG
        // needs nothing here.
        if let Ok(pw) = std::env::var("ORBIT_PG_PASSWORD").or_else(|_| std::env::var("PGPASSWORD")) {
            if !pw.is_empty() {
                s.push_str(&format!(" password={pw}"));
            }
        }
        s
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
) -> Result<()> {
    let mut replica = match dir {
        Some(d) => crate::sqlite_source::SqliteReplica::durable(d),
        None => crate::sqlite_source::SqliteReplica::in_memory(),
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
    let (pg, connection) = tokio_postgres::connect(&cfg.conn_str(), NoTls).await?;
    spawn_local(async move {
        if let Err(e) = connection.await {
            eprintln!("postgres connection error: {e}");
        }
    });
    // Shared-CVR tables (per-client view + version), shared across shards.
    crate::cvr::PgCvrStore::ensure_schema(&pg).await?;

    let table_names: Vec<&str> = cfg.tables.iter().map(|t| t.name.as_str()).collect();
    create_publication(&pg, &cfg.publication, &table_names)
        .await
        .with_context(|| format!("creating publication for tables {table_names:?} — do they all exist in the database?"))?;
    let start_lsn = create_slot(&pg, &cfg.slot)
        .await
        .context("creating logical replication slot — is the server started with wal_level=logical?")?;

    // Take the initial snapshot once and seed every shard with the same rows.
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

    let server = std::sync::Arc::new(crate::sharded::ShardedServer::start_with_pg(
        num_shards,
        shard_tables,
        Some(cfg.conn_str()),
    ));

    // Replication pump: decode each event once, fan it out to every shard.
    {
        let server = server.clone();
        let (host, port, user, db) = (cfg.host.clone(), cfg.port, cfg.user.clone(), cfg.database.clone());
        let (slot, publication) = (cfg.slot.clone(), cfg.publication.clone());
        spawn_local(async move {
            let mut stream = match ReplicationStream::start(&host, port, &user, &db, &slot, &publication, start_lsn).await {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("replication start failed: {e}");
                    return;
                }
            };
            loop {
                match stream.next_event().await {
                    Ok((_lsn, ev)) => server.broadcast_event(ev),
                    Err(e) => {
                        eprintln!("replication error: {e}");
                        break;
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

async fn run_inner<B: ReplicaBackend + 'static>(
    cfg: ServerConfig,
    mutators: MutatorRegistry,
    queries: QueryRegistry,
    backend: B,
) -> Result<()> {
    let (pg, connection) = tokio_postgres::connect(&cfg.conn_str(), NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("postgres connection error: {e}");
        }
    });
    // Shared-CVR tables (per-client view + version) so identifying clients can
    // resume as a delta; without this the serving path errors on first checkpoint.
    crate::cvr::PgCvrStore::ensure_schema(&pg).await?;
    let pg = Rc::new(pg);

    let table_names: Vec<&str> = cfg.tables.iter().map(|t| t.name.as_str()).collect();
    create_publication(&pg, &cfg.publication, &table_names)
        .await
        .with_context(|| format!("creating publication for tables {table_names:?} — do they all exist in the database?"))?;
    let start_lsn = create_slot(&pg, &cfg.slot)
        .await
        .context("creating logical replication slot — is the server started with wal_level=logical?")?;

    let replica = Rc::new(backend);
    for t in &cfg.tables {
        let n = initial_sync_backend(&pg, replica.as_ref(), &t.name)
            .await
            .with_context(|| format!("initial sync of table {}", t.name))?;
        eprintln!("initial sync: {} rows from {}", n, t.name);
    }
    let mutators = Rc::new(mutators);
    let queries = Rc::new(queries);
    let forwarder = Rc::new(crate::forward::Forwarder::new(cfg.forward_config()));

    let (ticks_tx, _) = broadcast::channel::<()>(1024);

    // Replication pump: apply each change once, then notify all clients.
    {
        let replica = replica.clone();
        let ticks_tx = ticks_tx.clone();
        let (host, port, user, db) = (cfg.host.clone(), cfg.port, cfg.user.clone(), cfg.database.clone());
        let (slot, publication) = (cfg.slot.clone(), cfg.publication.clone());
        spawn_local(async move {
            let mut stream = match ReplicationStream::start(&host, port, &user, &db, &slot, &publication, start_lsn).await {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("replication start failed: {e}");
                    return;
                }
            };
            // Apply every change in a transaction, but poke subscribers once at
            // Commit so a multi-statement transaction is delivered atomically.
            let mut dirty = false;
            loop {
                match stream.next_event().await {
                    Ok((_lsn, ev)) => {
                        let is_data = matches!(
                            ev,
                            LogicalEvent::Insert { .. } | LogicalEvent::Update { .. } | LogicalEvent::Delete { .. }
                        );
                        let is_commit = matches!(ev, LogicalEvent::Commit);
                        replica.apply(ev);
                        if is_data {
                            dirty = true;
                        }
                        if is_commit && dirty {
                            let _ = ticks_tx.send(());
                            dirty = false;
                        }
                    }
                    Err(e) => {
                        eprintln!("replication error: {e}");
                        break;
                    }
                }
            }
        });
    }

    // Accept loop.
    let listener = TcpListener::bind(&cfg.listen_addr).await?;
    eprintln!("orbit-server listening on {}", cfg.listen_addr);
    accept_ws_clients(listener, replica, pg, mutators, queries, forwarder, ticks_tx).await
}

/// Accept WebSocket clients and serve each over the shared `replica`, flushing on
/// `ticks`. Shared by the single-process server and the view-syncer.
async fn accept_ws_clients<B: ReplicaBackend + 'static>(
    listener: TcpListener,
    replica: Rc<B>,
    pg: Rc<tokio_postgres::Client>,
    mutators: Rc<MutatorRegistry>,
    queries: Rc<QueryRegistry>,
    forwarder: Rc<crate::forward::Forwarder>,
    ticks_tx: broadcast::Sender<()>,
) -> Result<()> {
    loop {
        let (sock, _) = listener.accept().await?;
        let replica = replica.clone();
        let pg = pg.clone();
        let mutators = mutators.clone();
        let queries = queries.clone();
        let forwarder = forwarder.clone();
        let ticks = ticks_tx.subscribe();
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

/// Take a snapshot of `replica` at change-stream position `pos` and write it to
/// the object store.
async fn write_snapshot<O: ObjectStore>(store: &O, replica: &Replica, pos: u64) -> Result<()> {
    let snap = ReplicaSnapshot { pos, tables: replica.snapshot() }; // sync gather (no await)
    store.put(ReplicaSnapshot::KEY, snap.to_bytes()).await
}

/// The watermark of the latest persisted snapshot, if any. The replicator reads
/// this on startup so its change-stream sequence continues from where the last
/// instance left off (continuity across restarts), rather than resetting to 0.
async fn snapshot_watermark<O: ObjectStore>(store: &O) -> Result<Option<u64>> {
    match store.get(ReplicaSnapshot::KEY).await? {
        Some(bytes) => Ok(Some(ReplicaSnapshot::from_bytes(&bytes)?.pos)),
        None => Ok(None),
    }
}

/// Restore the latest snapshot from the object store into `replica`, returning
/// the change-stream position it reflects. Waits for a snapshot if none exists yet.
async fn restore_snapshot<O: ObjectStore>(store: &O, replica: &Replica) -> Result<u64> {
    loop {
        if let Some(bytes) = store.get(ReplicaSnapshot::KEY).await? {
            let snap = ReplicaSnapshot::from_bytes(&bytes)?;
            for (table, rows) in snap.tables {
                for row in rows {
                    replica.seed(&table, row);
                }
            }
            eprintln!("view-syncer restored snapshot @ pos {}", snap.pos);
            return Ok(snap.pos);
        }
        eprintln!("view-syncer: waiting for replicator snapshot…");
        tokio::time::sleep(Duration::from_millis(500)).await;
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
    let local = LocalSet::new();
    local
        .run_until(run_replicator_inner(cfg, store, change_stream_addr, snapshot_interval))
        .await
}

async fn run_replicator_inner<O: ObjectStore + 'static>(
    cfg: ServerConfig,
    store: O,
    change_stream_addr: String,
    snapshot_interval: Duration,
) -> Result<()> {
    let (pg, connection) = tokio_postgres::connect(&cfg.conn_str(), NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("postgres connection error: {e}");
        }
    });

    let table_names: Vec<&str> = cfg.tables.iter().map(|t| t.name.as_str()).collect();
    create_publication(&pg, &cfg.publication, &table_names).await?;
    let start_lsn = create_slot(&pg, &cfg.slot)
        .await
        .context("creating logical replication slot — is wal_level=logical?")?;

    let mut replica = Replica::new();
    for t in &cfg.tables {
        replica.add_table(&t.name, t.columns.iter().cloned().collect(), t.primary_key.clone());
    }
    let replica = Rc::new(replica);
    for t in &cfg.tables {
        let n = initial_sync_backend(&pg, replica.as_ref(), &t.name).await?;
        eprintln!("initial sync: {} rows from {}", n, t.name);
    }
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
            Some(Arc::new(
                crate::changelog::PgChangeLog::open(
                    cfg.conn_str(),
                    format!("orbit_change_log_{}", cfg.slot),
                )
                .await?,
            ))
        };
    let checkpoint = match &log {
        Some(l) => l.checkpoint().await?,
        None => None,
    };
    let start_seq = match checkpoint {
        Some((pos, _)) => pos,
        None => snapshot_watermark(store.as_ref()).await?.unwrap_or(0),
    };
    let mut dedup_lsn = checkpoint.map(|(_, lsn)| lsn).unwrap_or(0);

    let server = ChangeStreamServer::new_with_log(CHANGE_RING_CAPACITY, start_seq, log.clone());
    {
        let server = server.clone();
        let addr = change_stream_addr.clone();
        tokio::spawn(async move {
            if let Err(e) = server.serve(&addr).await {
                eprintln!("change-stream server error: {e:#}");
            }
        });
    }

    // Refresh the snapshot with the freshly-synced replica at the continued
    // watermark, so view-syncers restore current data aligned with `start_seq`.
    write_snapshot(store.as_ref(), replica.as_ref(), start_seq).await?;
    eprintln!("replicator change-stream resuming at seq {start_seq} (dedup lsn {dedup_lsn})");

    // Retention: prune log entries well past the in-memory ring window. A
    // view-syncer behind by more than this re-restores (rare); the rest resume by
    // delta from the log.
    if let Some(log) = log.clone() {
        let server = server.clone();
        spawn_local(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(60)).await;
                let keep_from = server
                    .current_seq()
                    .saturating_sub(LOG_RETENTION);
                if keep_from > 0 {
                    if let Err(e) = log.prune_before(keep_from).await {
                        eprintln!("change-log prune failed: {e:#}");
                    }
                }
            }
        });
    }

    // Periodic snapshot loop — captures the current change-stream position.
    {
        let store = store.clone();
        let replica = replica.clone();
        let server = server.clone();
        spawn_local(async move {
            loop {
                tokio::time::sleep(snapshot_interval).await;
                let pos = server.current_seq();
                if let Err(e) = write_snapshot(store.as_ref(), replica.as_ref(), pos).await {
                    eprintln!("snapshot failed: {e:#}");
                }
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
    let mut txn_buf: Vec<(u64, LogicalEvent)> = Vec::new();
    loop {
        // Retry START_REPLICATION while the slot is held by a departing instance
        // (redeploy overlap). The loser waits instead of fighting — see create_slot.
        let mut stream = match ReplicationStream::start(
            &host, port, &user, &db, &slot, &publication, start_lsn,
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
        // Buffer each transaction and emit it atomically at Commit. Dedup is at
        // transaction granularity using the Commit's LSN — which is distinct and
        // monotonic, unlike per-event `wal_start` (Begin and the first change share
        // an LSN, and Relation messages carry 0). On restart the slot re-delivers
        // whole committed transactions; we skip those already durably logged
        // (commit LSN <= `dedup_lsn`) so each change keeps a stable pos.
        txn_buf.clear(); // discard any partial txn from a dropped connection
        loop {
            match stream.next_event().await {
                Ok((lsn, LogicalEvent::Commit)) => {
                    if lsn > dedup_lsn {
                        for (l, e) in txn_buf.drain(..) {
                            replica.apply(e.clone());
                            server.publish(l, e);
                        }
                        replica.apply(LogicalEvent::Commit);
                        server.publish(lsn, LogicalEvent::Commit);
                        dedup_lsn = lsn;
                    } else {
                        txn_buf.clear(); // already logged (re-delivered) → skip the txn
                    }
                }
                Ok((lsn, ev)) => txn_buf.push((lsn, ev)),
                Err(e) => {
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
        .run_until(run_view_syncer_inner(cfg, store, change_stream_addr, mutators, queries))
        .await
}

async fn run_view_syncer_inner<O: ObjectStore + 'static>(
    cfg: ServerConfig,
    store: O,
    change_stream_addr: String,
    mutators: MutatorRegistry,
    queries: QueryRegistry,
) -> Result<()> {
    let mut replica = Replica::new();
    for t in &cfg.tables {
        replica.add_table(&t.name, t.columns.iter().cloned().collect(), t.primary_key.clone());
    }
    let replica = Rc::new(replica);
    let watermark = restore_snapshot(&store, replica.as_ref()).await?;

    // Postgres connection only for client mutation write-through (no slot here);
    // those writes flow back through the replicator's change-stream.
    let (pg, connection) = tokio_postgres::connect(&cfg.conn_str(), NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("postgres connection error: {e}");
        }
    });
    // Shared CVR tables (per-client view) so a client reconnecting to this node —
    // having last been on another — resumes as a delta.
    crate::cvr::PgCvrStore::ensure_schema(&pg).await?;
    let pg = Rc::new(pg);
    let mutators = Rc::new(mutators);
    let queries = Rc::new(queries);
    let forwarder = Rc::new(crate::forward::Forwarder::new(cfg.forward_config()));
    let (ticks_tx, _) = broadcast::channel::<()>(1024);

    // Change-stream pump: apply remote changes to the local replica + tick.
    {
        let replica = replica.clone();
        let ticks_tx = ticks_tx.clone();
        let addr = change_stream_addr.clone();
        spawn_local(async move {
            let mut watermark = watermark;
            loop {
                let mut client = match ChangeStreamClient::connect(&addr, watermark).await {
                    Ok(c) => c,
                    Err(e) => {
                        eprintln!("change-stream connect failed: {e:#}; retrying");
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        continue;
                    }
                };
                let mut dirty = false;
                loop {
                    match client.next().await {
                        Ok(Some(ChangeMsg::Change { pos, event })) => {
                            let is_data = matches!(
                                event,
                                LogicalEvent::Insert { .. } | LogicalEvent::Update { .. } | LogicalEvent::Delete { .. }
                            );
                            let is_commit = matches!(event, LogicalEvent::Commit);
                            replica.apply(event);
                            watermark = pos;
                            if is_data {
                                dirty = true;
                            }
                            if is_commit && dirty {
                                let _ = ticks_tx.send(());
                                dirty = false;
                            }
                        }
                        Ok(Some(ChangeMsg::Reset)) => {
                            // Resume point can't be served (replicator restarted, or
                            // we fell too far behind). Exit so the orchestrator
                            // restarts us and we re-restore the latest snapshot — a
                            // bare `return` would only kill this task and leave the
                            // WS server happily serving stale data forever.
                            eprintln!("change-stream Reset (stale resume point, e.g. replicator restarted); exiting to re-restore snapshot");
                            tokio::time::sleep(Duration::from_secs(1)).await;
                            std::process::exit(1);
                        }
                        Ok(None) => break, // disconnected — reconnect from watermark
                        Err(e) => {
                            eprintln!("change-stream read error: {e:#}; reconnecting");
                            break;
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        });
    }

    let listener = TcpListener::bind(&cfg.listen_addr).await?;
    eprintln!("view-syncer listening on {}", cfg.listen_addr);
    accept_ws_clients(listener, replica, pg, mutators, queries, forwarder, ticks_tx).await
}
