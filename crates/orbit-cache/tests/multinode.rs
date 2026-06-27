//! Multinode end-to-end against **real Postgres**: one replicator (owns the slot,
//! snapshots to a shared object store, serves the change-stream) + two
//! view-syncers (restore from the store, follow the change-stream, serve WS).
//! A Postgres INSERT must reach clients on BOTH view-syncers — proving the
//! replicator/view-syncer split (Zero's multinode model).
//!
//! Requires the `orbit-pg` container on 127.0.0.1:5433 (see STATUS.md).

use std::time::Duration;

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

const CS_ADDR: &str = "127.0.0.1:39711";
const WS_A: &str = "127.0.0.1:39712";
const WS_B: &str = "127.0.0.1:39713";
const SLOT: &str = "orbit_mn_slot";
const PUB: &str = "orbit_mn_pub";

fn cfg(listen: &str) -> ServerConfig {
    ServerConfig {
        host: "127.0.0.1".into(),
        port: 5433,
        user: "orbit".into(),
        database: "orbit".into(),
        tables: vec![TableConfig {
            name: "mn_item".into(),
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
        } else {
            eprintln!("NODE EXITED (ok)");
        }
    });
}

async fn next_down(ws: &mut Ws) -> Downstream {
    loop {
        match tokio::time::timeout(Duration::from_secs(15), ws.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => return serde_json::from_str(&t).unwrap(),
            Ok(Some(Ok(_))) => continue,
            other => panic!("ws closed/timeout: {other:?}"),
        }
    }
}

async fn connect_subscribe(addr: &str) -> Ws {
    // Retry until the view-syncer's listener is up.
    let mut ws = None;
    for _ in 0..100 {
        if let Ok((s, _)) = tokio_tungstenite::connect_async(format!("ws://{addr}/")).await {
            ws = Some(s);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let mut ws = ws.expect("connect to view-syncer");
    assert!(matches!(next_down(&mut ws).await, Downstream::Connected(_)));
    let ast = Query::table("mn_item").order_by("id", Direction::Asc).build();
    ws.send(Message::Text(
        serde_json::to_string(&Upstream::ChangeDesiredQueries(ChangeDesiredQueriesBody {
            desired_queries_patch: vec![QueriesPatchOp::Put { hash: "h1".into(), ast: Some(ast), name: None, args: None, ttl: None }],
            traceparent: None,
        }))
        .unwrap(),
    ))
    .await
    .unwrap();
    // Initial poke confirms the subscription materialized.
    assert!(matches!(next_down(&mut ws).await, Downstream::PokeStart(_)));
    ws
}

/// Drain pokes until the result contains `id`, returning once seen (or panic on timeout).
async fn wait_for_id(ws: &mut Ws, id: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        assert!(std::time::Instant::now() < deadline, "timed out waiting for {id}");
        let d = next_down(ws).await;
        if let Downstream::PokePart(p) = d {
            let has = p
                .rows_patch
                .unwrap_or_default()
                .iter()
                .any(|op| matches!(op, RowPatchOp::Put { value, .. } if value.get("id") == Some(&Value::String(id.into()))));
            if has {
                return;
            }
        }
    }
}

#[tokio::test]
async fn mutation_reaches_clients_on_two_view_syncers() {
    let host = "127.0.0.1";
    let conn_str = format!("host={host} port=5433 user=orbit dbname=orbit");
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await.expect("connect orbit-pg");
    tokio::spawn(async move {
        let _ = connection.await;
    });

    // Fresh table + drop any leftover slot from a previous (killed) run.
    client
        .batch_execute(&format!(
            "SELECT pg_drop_replication_slot('{SLOT}') FROM pg_replication_slots WHERE slot_name='{SLOT}';
             DROP TABLE IF EXISTS mn_item;
             DROP TABLE IF EXISTS orbit_change_log_{SLOT};
             CREATE TABLE mn_item (id text PRIMARY KEY, n int);
             ALTER TABLE mn_item REPLICA IDENTITY FULL;",
        ))
        .await
        .unwrap();

    let dir = std::env::temp_dir().join(format!("orbit-mn-{}", std::process::id()));
    std::fs::remove_dir_all(&dir).ok();

    // Replicator: owns the slot, snapshots to `dir`, serves the change-stream.
    {
        let dir = dir.clone();
        spawn_node(move || async move {
            run_replicator(cfg("127.0.0.1:39710"), LocalObjectStore::new(&dir), CS_ADDR.into(), Duration::from_secs(60)).await
        });
    }
    // Two view-syncers restoring from the same store + following the change-stream.
    for ws_addr in [WS_A, WS_B] {
        let dir = dir.clone();
        spawn_node(move || async move {
            run_view_syncer(cfg(ws_addr), LocalObjectStore::new(&dir), CS_ADDR.into(), MutatorRegistry::new(), QueryRegistry::new()).await
        });
    }

    let mut a = connect_subscribe(WS_A).await;
    let mut b = connect_subscribe(WS_B).await;

    // Mutate Postgres once.
    client.batch_execute("INSERT INTO mn_item VALUES ('i1', 1)").await.unwrap();

    // The single replicator's change-stream fans the insert to BOTH view-syncers,
    // and each pokes its own client.
    wait_for_id(&mut a, "i1").await;
    wait_for_id(&mut b, "i1").await;

    client
        .batch_execute(&format!(
            "SELECT pg_drop_replication_slot('{SLOT}') FROM pg_replication_slots WHERE slot_name='{SLOT}'"
        ))
        .await
        .ok();
}
