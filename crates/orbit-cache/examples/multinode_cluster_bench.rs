//! **Actual multinode cluster** throughput benchmark — real Postgres, end-to-end.
//!
//! Stands up the real topology: 1 replicator (owns the PG slot, serves the change-
//! stream) + N view-syncer nodes (each its own runtime/thread, restoring from a
//! shared object store and following the change-stream over TCP) + K WebSocket
//! clients spread round-robin across the nodes. It then drives M mutations through
//! Postgres and measures end-to-end propagation to *every* client:
//!
//!   PG insert → WAL → replicator → change-stream(TCP) → each view-syncer applies
//!   → WebSocket pokes → clients.
//!
//! Unlike `multinode_bench` (an in-process fan-out micro-bench of the engine), this
//! exercises the whole pipeline, so it's bottlenecked by real logical replication —
//! i.e. it reports the system's actual multinode throughput, not just the operator
//! speed. All nodes run on one machine over loopback (separate runtimes), so it's a
//! multi-node-process test, not multi-machine.
//!
//! Requires the `orbit-pg` container on 127.0.0.1:5433 (see STATUS.md).
//!   cargo run --release --example multinode_cluster_bench -p orbit-cache -- [view_syncers] [clients] [mutations]

use std::collections::HashSet;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use oql::ast::Direction;
use oql::ivm::ColumnType;
use oql::value::Value;
use oql::Query;
use orbit_cache::{
    run_replicator, run_view_syncer, LocalObjectStore, MutatorRegistry, QueryRegistry, ServerConfig,
    TableConfig,
};
use orbit_protocol::{ChangeDesiredQueriesBody, Downstream, QueriesPatchOp, RowPatchOp, Upstream};
use tokio_postgres::NoTls;
use tokio_tungstenite::tungstenite::Message;

type Ws = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

const CS_ADDR: &str = "127.0.0.1:40000";
const SLOT: &str = "orbit_mncb_slot";
const PUB: &str = "orbit_mncb_pub";

fn ws_addr(shard: usize) -> String {
    format!("127.0.0.1:{}", 40010 + shard)
}

fn cfg(listen: &str) -> ServerConfig {
    ServerConfig {
        host: "127.0.0.1".into(),
        port: 5433,
        user: "orbit".into(),
        database: "orbit".into(),
        tables: vec![TableConfig {
            name: "mncb_item".into(),
            columns: vec![("id".into(), ColumnType::String), ("n".into(), ColumnType::Number)],
            primary_key: vec!["id".into()],
        }],
        publication: PUB.into(),
        slot: SLOT.into(),
        listen_addr: listen.into(),
        mutate_url: None,
        query_url: None,
        api_key: None,
        forward_cookies: false,
    }
}

/// Run a node's server future to completion on a dedicated current-thread runtime.
fn spawn_node<F, Fut>(make: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = anyhow::Result<()>>,
{
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        if let Err(e) = rt.block_on(make()) {
            eprintln!("NODE EXITED WITH ERROR: {e:#}");
        }
    });
}

async fn next_down(ws: &mut Ws) -> Downstream {
    loop {
        match tokio::time::timeout(Duration::from_secs(30), ws.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => return serde_json::from_str(&t).unwrap(),
            Ok(Some(Ok(_))) => continue,
            other => panic!("ws closed/timeout: {other:?}"),
        }
    }
}

/// Connect, subscribe to "all rows of mncb_item", and drain the initial poke — so
/// by the time this returns the server is provably serving this client (no missed
/// rows once we start inserting).
async fn connect_subscribe(addr: &str) -> Ws {
    let mut ws = None;
    for _ in 0..200 {
        if let Ok((s, _)) = tokio_tungstenite::connect_async(format!("ws://{addr}/")).await {
            ws = Some(s);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let mut ws = ws.expect("connect to view-syncer");
    assert!(matches!(next_down(&mut ws).await, Downstream::Connected(_)));
    let ast = Query::table("mncb_item").order_by("id", Direction::Asc).build();
    ws.send(Message::Text(
        serde_json::to_string(&Upstream::ChangeDesiredQueries(ChangeDesiredQueriesBody {
            desired_queries_patch: vec![QueriesPatchOp::Put { hash: "h1".into(), ast: Some(ast), name: None, args: None, ttl: None }],
            traceparent: None,
        }))
        .unwrap(),
    ))
    .await
    .unwrap();
    // Drain to the initial pokeEnd (subscription materialized; table is empty).
    loop {
        if let Downstream::PokeEnd(_) = next_down(&mut ws).await {
            break;
        }
    }
    ws
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let vs: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(3);
    let clients: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(60);
    let muts: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(1000);

    // --- Postgres setup -----------------------------------------------------
    let conn_str = "host=127.0.0.1 port=5433 user=orbit dbname=orbit";
    let (pg, connection) = tokio_postgres::connect(conn_str, NoTls).await.expect("connect orbit-pg");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    orbit_cache::PgCvrStore::ensure_schema(&pg).await.unwrap();
    pg.batch_execute(&format!(
        "SELECT pg_drop_replication_slot('{SLOT}') FROM pg_replication_slots WHERE slot_name='{SLOT}';
         DROP TABLE IF EXISTS mncb_item;
         DROP TABLE IF EXISTS orbit_change_log_{SLOT};
         CREATE TABLE mncb_item (id text PRIMARY KEY, n int);
         ALTER TABLE mncb_item REPLICA IDENTITY FULL;",
    ))
    .await
    .unwrap();

    let dir = std::env::temp_dir().join(format!("orbit-mncb-{}", std::process::id()));
    std::fs::remove_dir_all(&dir).ok();

    // --- Cluster: 1 replicator + N view-syncers -----------------------------
    {
        let dir = dir.clone();
        spawn_node(move || async move {
            run_replicator(cfg("127.0.0.1:40001"), LocalObjectStore::new(&dir), CS_ADDR.into(), Duration::from_secs(3600)).await
        });
    }
    for s in 0..vs {
        let dir = dir.clone();
        spawn_node(move || async move {
            run_view_syncer(cfg(&ws_addr(s)), LocalObjectStore::new(&dir), CS_ADDR.into(), MutatorRegistry::new(), QueryRegistry::new()).await
        });
    }

    // --- Connect K clients across the N nodes -------------------------------
    let (ready_tx, mut ready_rx) = tokio::sync::mpsc::channel::<()>(clients);
    let (done_tx, mut done_rx) = tokio::sync::mpsc::channel::<Instant>(clients);
    for c in 0..clients {
        let addr = ws_addr(c % vs);
        let ready_tx = ready_tx.clone();
        let done_tx = done_tx.clone();
        tokio::spawn(async move {
            let mut ws = connect_subscribe(&addr).await;
            ready_tx.send(()).await.ok();
            // Count distinct row ids until we've seen all M mutations.
            let mut seen: HashSet<String> = HashSet::new();
            while seen.len() < muts {
                if let Downstream::PokePart(p) = next_down(&mut ws).await {
                    for op in p.rows_patch.unwrap_or_default() {
                        if let RowPatchOp::Put { value, .. } = op {
                            if let Some(Value::String(id)) = value.get("id") {
                                seen.insert(id.clone());
                            }
                        }
                    }
                }
            }
            done_tx.send(Instant::now()).await.ok();
        });
    }
    for _ in 0..clients {
        ready_rx.recv().await.expect("client ready");
    }
    // Small settle so every subscription is fully wired before the burst.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // --- Drive M mutations, time end-to-end propagation to ALL clients -------
    let start = Instant::now();
    pg.batch_execute(&format!(
        "INSERT INTO mncb_item SELECT 'i' || g, g FROM generate_series(0, {}) g",
        muts - 1
    ))
    .await
    .unwrap();

    let mut durations: Vec<Duration> = Vec::with_capacity(clients);
    for _ in 0..clients {
        let t = done_rx.recv().await.expect("client done");
        durations.push(t - start);
    }
    durations.sort();

    // --- Report -------------------------------------------------------------
    let wall = *durations.last().unwrap();
    let p50 = durations[durations.len() / 2];
    let min = durations[0];
    let fanouts = clients * muts;
    println!("ORBIT multinode CLUSTER (real PG, end-to-end): view_syncers={vs} clients={clients} mutations={muts}");
    println!("  topology     1 replicator + {vs} view-syncers, {clients} clients (~{} per node)", clients / vs);
    println!("  propagate-all {:>8.1} ms  (all {clients} clients have all {muts} rows)", wall.as_secs_f64() * 1e3);
    println!("  mutation rate {:>10.0} mutations/s fully propagated to every client", muts as f64 / wall.as_secs_f64());
    println!("  fan-out rate  {:>10.0} client-rows/s aggregate ({fanouts} total)", fanouts as f64 / wall.as_secs_f64());
    println!("  client latency  min {:.1} ms / p50 {:.1} ms / max {:.1} ms", min.as_secs_f64() * 1e3, p50.as_secs_f64() * 1e3, wall.as_secs_f64() * 1e3);

    pg.batch_execute(&format!(
        "SELECT pg_drop_replication_slot('{SLOT}') FROM pg_replication_slots WHERE slot_name='{SLOT}'"
    ))
    .await
    .ok();
}
