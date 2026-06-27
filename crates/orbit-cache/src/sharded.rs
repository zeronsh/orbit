//! Multi-core sharded serving.
//!
//! The IVM pipelines are `!Send` (`Rc`/`RefCell`), so a single query pipeline
//! can't move across threads — and a *single* query's incremental push is a
//! sequential dependency chain anyway. But a sync server runs many independent
//! query pipelines (queries × clients), which is embarrassingly parallel.
//!
//! [`ShardedServer`] runs one worker **OS thread per shard**, each with its own
//! current-thread Tokio runtime + [`LocalSet`] owning its own [`Replica`] and
//! its clients' pipelines. There is **no shared mutable state and no locking on
//! the hot path** — the structural parallelism a single-threaded JS engine
//! (Zero) cannot have in one process.
//!
//! * New connections are distributed round-robin across shards (handed over as
//!   `std::net::TcpStream` and re-registered on the shard's reactor, since a
//!   Tokio stream is bound to the runtime that accepted it).
//! * Replication events fan out to every shard, which applies them to its own
//!   replica and pokes its own clients.
//!
//! Trade-off: each shard holds its own replica, so the base dataset is resident
//! once per shard. That buys lock-free, contention-free reads; partition the
//! shards by table/key if the dataset is too large to replicate per shard.

use crate::handshake::accept_zero_ws;
use crate::mutators::MutatorRegistry;
use crate::pg::pgoutput::LogicalEvent;
use crate::queries::QueryRegistry;
use crate::replica::{Replica, ReplicaBackend};
use crate::server::serve_client;
use oql::ivm::ColumnType;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::JoinHandle;
use tokio::sync::{broadcast, mpsc};
use tokio::task::{spawn_local, LocalSet};

/// A table replicated into every shard, with its initial rows.
#[derive(Clone)]
pub struct ShardTable {
    pub name: String,
    pub columns: Vec<(String, ColumnType)>,
    pub primary_key: Vec<String>,
    /// Initial rows seeded into every shard's replica (no change propagation).
    pub seed: Vec<oql::value::Row>,
}

/// A pool of shard worker threads serving clients across all cores.
pub struct ShardedServer {
    job_txs: Vec<mpsc::UnboundedSender<std::net::TcpStream>>,
    event_txs: Vec<mpsc::UnboundedSender<LogicalEvent>>,
    next: AtomicUsize,
    handles: Vec<JoinHandle<()>>,
}

impl ShardedServer {
    /// Spawn `num_shards` worker threads, each building its own replica from
    /// `tables` (seeded identically). No client mutation write-through.
    pub fn start(num_shards: usize, tables: Vec<ShardTable>) -> ShardedServer {
        ShardedServer::start_with_pg(num_shards, tables, None)
    }

    /// Like [`start`](Self::start) but each shard opens its own Postgres client
    /// (`pg_conn`) so client mutations write through; the change converges back
    /// to every shard via the replication fan-out.
    pub fn start_with_pg(
        num_shards: usize,
        tables: Vec<ShardTable>,
        pg_conn: Option<String>,
    ) -> ShardedServer {
        assert!(num_shards >= 1, "need at least one shard");
        let mut job_txs = Vec::with_capacity(num_shards);
        let mut event_txs = Vec::with_capacity(num_shards);
        let mut handles = Vec::with_capacity(num_shards);
        for id in 0..num_shards {
            let (jtx, jrx) = mpsc::unbounded_channel::<std::net::TcpStream>();
            let (etx, erx) = mpsc::unbounded_channel::<LogicalEvent>();
            job_txs.push(jtx);
            event_txs.push(etx);
            let tables = tables.clone();
            let pg_conn = pg_conn.clone();
            let h = std::thread::Builder::new()
                .name(format!("orbit-shard-{id}"))
                .spawn(move || shard_main(tables, pg_conn, jrx, erx))
                .expect("spawn shard thread");
            handles.push(h);
        }
        ShardedServer {
            job_txs,
            event_txs,
            next: AtomicUsize::new(0),
            handles,
        }
    }

    pub fn num_shards(&self) -> usize {
        self.job_txs.len()
    }

    /// Hand a freshly-accepted connection to the next shard (round-robin).
    ///
    /// Takes a `std::net::TcpStream` because a Tokio stream is bound to the
    /// runtime that accepted it; the shard re-registers it on its own reactor.
    pub fn dispatch(&self, sock: std::net::TcpStream) {
        let i = self.next.fetch_add(1, Ordering::Relaxed) % self.job_txs.len();
        let _ = self.job_txs[i].send(sock);
    }

    /// Fan a replication event out to every shard.
    pub fn broadcast_event(&self, ev: LogicalEvent) {
        for tx in &self.event_txs {
            let _ = tx.send(ev.clone());
        }
    }

    /// Stop the shards (closes the channels) and join their threads.
    pub fn shutdown(self) {
        drop(self.job_txs);
        drop(self.event_txs);
        for h in self.handles {
            let _ = h.join();
        }
    }
}

fn shard_main(
    tables: Vec<ShardTable>,
    pg_conn: Option<String>,
    mut jobs: mpsc::UnboundedReceiver<std::net::TcpStream>,
    mut events: mpsc::UnboundedReceiver<LogicalEvent>,
) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("shard runtime");
    let local = LocalSet::new();
    rt.block_on(local.run_until(async move {
        // This shard's own replica + pipelines (thread-local, lock-free).
        let mut replica = Replica::new();
        for t in &tables {
            replica.add_table(&t.name, t.columns.iter().cloned().collect(), t.primary_key.clone());
            for r in &t.seed {
                replica.seed(&t.name, r.clone());
            }
        }
        let replica = Rc::new(replica);
        let mutators = Rc::new(MutatorRegistry::new());
        let queries = Rc::new(QueryRegistry::new());
        let forwarder = Rc::new(crate::forward::Forwarder::new(crate::forward::ForwardConfig::default()));

        // Optional per-shard Postgres client for client mutation write-through.
        let pg: Option<Rc<tokio_postgres::Client>> = match pg_conn {
            Some(conn) => match tokio_postgres::connect(&conn, tokio_postgres::NoTls).await {
                Ok((client, connection)) => {
                    spawn_local(async move {
                        if let Err(e) = connection.await {
                            eprintln!("shard postgres connection error: {e}");
                        }
                    });
                    Some(Rc::new(client))
                }
                Err(e) => {
                    eprintln!("shard postgres connect failed: {e}");
                    None
                }
            },
            None => None,
        };

        let (tick_tx, _) = broadcast::channel::<()>(1024);

        // Apply fanned-out replication events; poke this shard's clients at the
        // commit boundary so a transaction is delivered atomically.
        {
            let replica = replica.clone();
            let tick_tx = tick_tx.clone();
            spawn_local(async move {
                let mut dirty = false;
                while let Some(ev) = events.recv().await {
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
                        let _ = tick_tx.send(());
                        dirty = false;
                    }
                }
            });
        }

        // Serve the connections assigned to this shard.
        while let Some(std_sock) = jobs.recv().await {
            let replica = replica.clone();
            let mutators = mutators.clone();
            let queries = queries.clone();
            let forwarder = forwarder.clone();
            let pg = pg.clone();
            let ticks = tick_tx.subscribe();
            spawn_local(async move {
                // Re-register the socket on this shard's reactor.
                let _ = std_sock.set_nonblocking(true);
                let sock = match tokio::net::TcpStream::from_std(std_sock) {
                    Ok(s) => s,
                    Err(_) => return,
                };
                match accept_zero_ws(sock).await {
                    Ok((ws, info)) => {
                        let auth = crate::forward::AuthContext { token: info.auth_token, cookie: info.cookie };
                        let _ = serve_client(
                            ws,
                            &*replica,
                            pg.as_deref(),
                            &mutators,
                            &queries,
                            &forwarder,
                            &auth,
                            info.desired_queries,
                            info.client_id,
                            info.base_cookie,
                            ticks,
                        )
                        .await;
                    }
                    Err(_) => {}
                }
            });
        }
    }));
}
