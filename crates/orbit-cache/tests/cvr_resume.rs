//! Shared-CVR cross-node **incremental resume** against real Postgres.
//!
//! A client subscribes on view-syncer A and receives the full result. It then
//! disconnects, a row is inserted, and it reconnects to a DIFFERENT view-syncer B
//! (same `clientID`). Because the per-client view is persisted in shared Postgres,
//! B must send only the *delta* (the one new row) — never re-sending the rows the
//! client already held. This is the efficiency win of Zero's shared CVR.
//!
//! Requires the `orbit-pg` container on 127.0.0.1:5433 (see STATUS.md).

use std::collections::HashSet;
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

const CS_ADDR: &str = "127.0.0.1:39721";
const WS_A: &str = "127.0.0.1:39722";
const WS_B: &str = "127.0.0.1:39723";
const SLOT: &str = "orbit_cvr_slot";
const PUB: &str = "orbit_cvr_pub";

fn cfg(listen: &str) -> ServerConfig {
    ServerConfig {
        host: "127.0.0.1".into(),
        port: 5433,
        user: "orbit".into(),
        database: "orbit".into(),
        password: None,
        tls: orbit_cache::PgTlsMode::Disable,
        tables: vec![TableConfig {
            name: "cvr_item".into(),
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
        match tokio::time::timeout(Duration::from_secs(15), ws.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => return serde_json::from_str(&t).unwrap(),
            Ok(Some(Ok(_))) => continue,
            other => panic!("ws closed/timeout: {other:?}"),
        }
    }
}

/// Connect identifying as `client_id`, optionally reporting the last cookie it
/// applied (Zero passes both as connect query params).
async fn connect(addr: &str, client_id: &str, base_cookie: Option<&str>) -> Ws {
    let mut url = format!("ws://{addr}/?clientID={client_id}");
    if let Some(c) = base_cookie {
        url.push_str(&format!("&baseCookie={c}"));
    }
    let mut ws = None;
    for _ in 0..100 {
        if let Ok((s, _)) = tokio_tungstenite::connect_async(&url).await {
            ws = Some(s);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let mut ws = ws.expect("connect to view-syncer");
    assert!(matches!(next_down(&mut ws).await, Downstream::Connected(_)));
    ws
}

async fn subscribe(ws: &mut Ws) {
    let ast = Query::table("cvr_item").order_by("id", Direction::Asc).build();
    ws.send(Message::Text(
        serde_json::to_string(&Upstream::ChangeDesiredQueries(ChangeDesiredQueriesBody {
            desired_queries_patch: vec![QueriesPatchOp::Put { hash: "h1".into(), ast: Some(ast), name: None, args: None, ttl: None }],
            traceparent: None,
        }))
        .unwrap(),
    ))
    .await
    .unwrap();
}

/// Drain pokes, collecting every `put` id, until `target` is seen. Also returns
/// the cookie of the poke that delivered `target` (to feed the next reconnect).
#[allow(unused_assignments)] // `cookie` is seeded then overwritten in the loop
async fn collect_puts_until(ws: &mut Ws, target: &str) -> (HashSet<String>, String) {
    let mut puts = HashSet::new();
    let mut cookie = String::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        assert!(std::time::Instant::now() < deadline, "timed out waiting for {target}");
        match next_down(ws).await {
            Downstream::PokePart(p) => {
                for op in p.rows_patch.unwrap_or_default() {
                    if let RowPatchOp::Put { value, .. } = op {
                        if let Some(Value::String(id)) = value.get("id") {
                            puts.insert(id.clone());
                        }
                    }
                }
            }
            Downstream::PokeEnd(e) => {
                cookie = e.cookie;
                if puts.contains(target) {
                    return (puts, cookie);
                }
            }
            _ => {}
        }
    }
}

#[tokio::test]
async fn reconnect_to_other_node_resumes_as_delta() {
    let conn_str = "host=127.0.0.1 port=5433 user=orbit dbname=orbit";
    let (client, connection) = tokio_postgres::connect(conn_str, NoTls).await.expect("connect orbit-pg");
    tokio::spawn(async move {
        let _ = connection.await;
    });

    // Fresh table seeded with i1, i2 BEFORE the nodes start, so both view-syncers
    // restore them in their initial snapshot. Also clear this client's CVR.
    orbit_cache::PgCvrStore::ensure_schema(&client).await.unwrap();
    client
        .batch_execute(&format!(
            "SELECT pg_drop_replication_slot('{SLOT}') FROM pg_replication_slots WHERE slot_name='{SLOT}';
             DROP TABLE IF EXISTS cvr_item;
             DROP TABLE IF EXISTS orbit_change_log_{SLOT};
             CREATE TABLE cvr_item (id text PRIMARY KEY, n int);
             ALTER TABLE cvr_item REPLICA IDENTITY FULL;
             INSERT INTO cvr_item VALUES ('i1', 1), ('i2', 2);
             DELETE FROM orbit_cvr_client_rows WHERE client_id = 'c1';",
        ))
        .await
        .unwrap();

    let dir = std::env::temp_dir().join(format!("orbit-cvr-{}", std::process::id()));
    std::fs::remove_dir_all(&dir).ok();

    {
        let dir = dir.clone();
        spawn_node(move || async move {
            run_replicator(cfg("127.0.0.1:39720"), LocalObjectStore::new(&dir), CS_ADDR.into(), Duration::from_secs(60)).await
        });
    }
    for ws_addr in [WS_A, WS_B] {
        let dir = dir.clone();
        spawn_node(move || async move {
            run_view_syncer(cfg(ws_addr), LocalObjectStore::new(&dir), CS_ADDR.into(), MutatorRegistry::new(), QueryRegistry::new()).await
        });
    }

    // Client c1 connects to node A and gets the full result (i1, i2). Capture the
    // cookie A hands it — that's its proof of what it has applied.
    let mut a = connect(WS_A, "c1", None).await;
    subscribe(&mut a).await;
    let (seen_on_a, cookie_a) = collect_puts_until(&mut a, "i2").await;
    assert!(seen_on_a.contains("i1") && seen_on_a.contains("i2"), "A sends the full set: {seen_on_a:?}");

    // Let A's CVR checkpoint (written right after the subscribe) land in Postgres,
    // then disconnect.
    tokio::time::sleep(Duration::from_millis(800)).await;
    a.close(None).await.ok();
    drop(a);

    // While the client is away, a new row appears.
    client.batch_execute("INSERT INTO cvr_item VALUES ('i3', 3)").await.unwrap();

    // FAST PATH: reconnect to a DIFFERENT node (B), same clientID, reporting the
    // cookie from A. B proves the client holds {i1,i2} (cookie matches the stored
    // version) and sends ONLY the delta: i3, never the already-held i1/i2.
    let mut b = connect(WS_B, "c1", Some(&cookie_a)).await;
    subscribe(&mut b).await;
    let (puts_on_b, _) = collect_puts_until(&mut b, "i3").await;
    assert_eq!(
        puts_on_b,
        HashSet::from(["i3".to_string()]),
        "B must resume as a delta — only i3, not the already-held i1/i2: got {puts_on_b:?}"
    );
    tokio::time::sleep(Duration::from_millis(800)).await;
    b.close(None).await.ok();
    drop(b);

    // Another change while away.
    client.batch_execute("INSERT INTO cvr_item VALUES ('i4', 4)").await.unwrap();

    // SAFE FALLBACK: reconnect with NO cookie (as if the client lost it / missed
    // pokes). The server can't prove a delta is safe, so it FULL-resyncs the
    // complete current set — convergence over minimality, never a lost row.
    let mut c = connect(WS_A, "c1", None).await;
    subscribe(&mut c).await;
    let (puts_on_c, _) = collect_puts_until(&mut c, "i4").await;
    for id in ["i1", "i2", "i3", "i4"] {
        assert!(puts_on_c.contains(id), "stale-cookie reconnect must full-resync the whole set; missing {id}: {puts_on_c:?}");
    }

    client
        .batch_execute(&format!(
            "SELECT pg_drop_replication_slot('{SLOT}') FROM pg_replication_slots WHERE slot_name='{SLOT}'"
        ))
        .await
        .ok();
}
