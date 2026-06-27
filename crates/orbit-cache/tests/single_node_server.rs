//! Single-node `run_server` path with an identifying client (real Postgres).
//!
//! Regression test for: the single-node server connected to Postgres but never
//! created the shared-CVR tables (only the multinode view-syncer did). Once the TS
//! client started sending a `clientID`, the serving path turned CVR on, sent the
//! initial poke, then errored on the post-subscribe checkpoint
//! (`relation "orbit_cvr_client_rows" does not exist`) and dropped the connection
//! into a reconnect loop. This drives `run_server` with a clientID client and
//! asserts the connection SURVIVES a checkpoint by delivering a post-subscribe row.
//!
//! Requires the `orbit-pg` container on 127.0.0.1:5433 (see STATUS.md).

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use oql::ast::Direction;
use oql::ivm::ColumnType;
use oql::value::Value;
use oql::Query;
use orbit_cache::{run_server, MutatorRegistry, ServerConfig, TableConfig};
use orbit_protocol::{ChangeDesiredQueriesBody, Downstream, QueriesPatchOp, RowPatchOp, Upstream};
use tokio_postgres::NoTls;
use tokio_tungstenite::tungstenite::Message;

type Ws = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

const WS: &str = "127.0.0.1:39730";
const SLOT: &str = "orbit_sn_slot";
const PUB: &str = "orbit_sn_pub";

fn cfg() -> ServerConfig {
    ServerConfig {
        host: "127.0.0.1".into(),
        port: 5433,
        user: "orbit".into(),
        database: "orbit".into(),
        tables: vec![TableConfig {
            name: "sn_item".into(),
            columns: vec![("id".into(), ColumnType::String), ("n".into(), ColumnType::Number)],
            primary_key: vec!["id".into()],
        }],
        publication: PUB.into(),
        slot: SLOT.into(),
        listen_addr: WS.into(),
        mutate_url: None,
        query_url: None,
        api_key: None,
        forward_cookies: false,
    }
}

async fn next_down(ws: &mut Ws) -> Downstream {
    loop {
        match tokio::time::timeout(Duration::from_secs(15), ws.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => return serde_json::from_str(&t).unwrap(),
            Ok(Some(Ok(_))) => continue,
            other => panic!("ws closed/timeout (connection dropped — checkpoint error?): {other:?}"),
        }
    }
}

async fn wait_for_id(ws: &mut Ws, id: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        assert!(std::time::Instant::now() < deadline, "timed out waiting for {id}");
        if let Downstream::PokePart(p) = next_down(ws).await {
            let has = p.rows_patch.unwrap_or_default().iter().any(|op| {
                matches!(op, RowPatchOp::Put { value, .. } if value.get("id") == Some(&Value::String(id.into())))
            });
            if has {
                return;
            }
        }
    }
}

#[tokio::test]
async fn single_node_server_serves_identifying_client() {
    let conn_str = "host=127.0.0.1 port=5433 user=orbit dbname=orbit";
    let (client, connection) = tokio_postgres::connect(conn_str, NoTls).await.expect("connect orbit-pg");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!(
            "SELECT pg_drop_replication_slot('{SLOT}') FROM pg_replication_slots WHERE slot_name='{SLOT}';
             DROP TABLE IF EXISTS sn_item;
             CREATE TABLE sn_item (id text PRIMARY KEY, n int);
             ALTER TABLE sn_item REPLICA IDENTITY FULL;
             INSERT INTO sn_item VALUES ('a', 1);",
        ))
        .await
        .unwrap();

    // Single-node server (the `run_server` path the demo app uses).
    std::thread::spawn(|| {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        if let Err(e) = rt.block_on(run_server(cfg(), MutatorRegistry::new())) {
            eprintln!("SERVER EXITED WITH ERROR: {e:#}");
        }
    });

    // Connect AS AN IDENTIFYING CLIENT (clientID in the URL → CVR turns on).
    let mut ws = None;
    for _ in 0..100 {
        if let Ok((s, _)) = tokio_tungstenite::connect_async(format!("ws://{WS}/?clientID=sn1")).await {
            ws = Some(s);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let mut ws = ws.expect("connect to single-node server");
    assert!(matches!(next_down(&mut ws).await, Downstream::Connected(_)));
    let ast = Query::table("sn_item").order_by("id", Direction::Asc).build();
    ws.send(Message::Text(
        serde_json::to_string(&Upstream::ChangeDesiredQueries(ChangeDesiredQueriesBody {
            desired_queries_patch: vec![QueriesPatchOp::Put { hash: "h1".into(), ast: Some(ast), name: None, args: None, ttl: None }],
            traceparent: None,
        }))
        .unwrap(),
    ))
    .await
    .unwrap();

    // Initial result (the seeded row).
    wait_for_id(&mut ws, "a").await;

    // A post-subscribe mutation must still arrive — i.e. the connection SURVIVED
    // the CVR checkpoint that follows the subscribe (the bug dropped it here).
    client.batch_execute("INSERT INTO sn_item VALUES ('b', 2)").await.unwrap();
    wait_for_id(&mut ws, "b").await;

    client
        .batch_execute(&format!(
            "SELECT pg_drop_replication_slot('{SLOT}') FROM pg_replication_slots WHERE slot_name='{SLOT}'"
        ))
        .await
        .ok();
}
