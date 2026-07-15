//! Tier 1.2 regression: a transaction LARGER than the txn buffer cap streams
//! through bounded memory — and still reaches subscribed clients atomically
//! (one poke burst, all rows present, none torn).
//!
//! Runs in its own test binary so setting `ORBIT_TXN_BUFFER_BYTES` (read once
//! per process) can't race other tests. Requires the `orbit-pg` container on
//! 127.0.0.1:5433.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use oql::ast::Direction;
use oql::ivm::ColumnType;
use oql::value::Value;
use oql::Query;
use orbit_cache::{run_server_sqlite, MutatorRegistry, ServerConfig, TableConfig};
use orbit_protocol::{ChangeDesiredQueriesBody, Downstream, QueriesPatchOp, RowPatchOp, Upstream};
use tokio_postgres::NoTls;
use tokio_tungstenite::tungstenite::Message;

type Ws = tokio_tungstenite::WebSocketStream<
    tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
>;

const WS: &str = "127.0.0.1:39741";
const SLOT: &str = "orbit_bigtxn_slot";
const PUB: &str = "orbit_bigtxn_pub";

fn cfg() -> ServerConfig {
    ServerConfig {
        host: "127.0.0.1".into(),
        port: std::env::var("ORBIT_PG_PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(5433),
        user: "orbit".into(),
        database: "orbit".into(),
        password: None,
        tls: orbit_cache::PgTlsMode::Disable,
        tables: vec![TableConfig {
            name: "big_item".into(),
            columns: vec![
                ("id".into(), ColumnType::String),
                ("body".into(), ColumnType::String),
            ],
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
        match tokio::time::timeout(Duration::from_secs(30), ws.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => return serde_json::from_str(&t).unwrap(),
            Ok(Some(Ok(_))) => continue,
            other => panic!("ws closed/timeout: {other:?}"),
        }
    }
}

#[tokio::test]
async fn oversized_transaction_streams_and_arrives_atomically() {
    // Cap far below the transaction size → the streaming path MUST engage.
    std::env::set_var("ORBIT_TXN_BUFFER_BYTES", "65536");

    let pg_port: u16 =
        std::env::var("ORBIT_PG_PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(5433);
    let conn_str = format!("host=127.0.0.1 port={pg_port} user=orbit dbname=orbit");
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(
            "DROP TABLE IF EXISTS big_item;
             CREATE TABLE big_item (id text PRIMARY KEY, body text);
             ALTER TABLE big_item REPLICA IDENTITY FULL;",
        )
        .await
        .unwrap();
    client
        .batch_execute(&format!(
            "SELECT pg_drop_replication_slot('{SLOT}') FROM pg_replication_slots WHERE slot_name='{SLOT}'"
        ))
        .await
        .ok();

    // In-memory SQLite replica (exercises the durable begin/commit txn path).
    std::thread::spawn(|| {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        if let Err(e) = rt.block_on(run_server_sqlite(
            cfg(),
            MutatorRegistry::new(),
            None,
            orbit_cache::SqliteReplicaOpts::default(),
        )) {
            eprintln!("SERVER ERROR: {e:#}");
        }
    });

    // Wait for the server to accept.
    let mut ws = loop {
        match tokio_tungstenite::connect_async(format!("ws://{WS}/sync/v1/connect")).await {
            Ok((ws, _)) => break ws,
            Err(_) => tokio::time::sleep(Duration::from_millis(200)).await,
        }
    };

    // Subscribe to the table.
    let ast = Query::table("big_item").order_by("id", Direction::Asc).build();
    let sub = Upstream::ChangeDesiredQueries(ChangeDesiredQueriesBody {
        desired_queries_patch: vec![QueriesPatchOp::Put {
            hash: "q1".into(),
            ast: Some(ast),
            name: None,
            args: None,
            ttl: None,
        }],
        traceparent: None,
    });
    ws.send(Message::Text(serde_json::to_string(&sub).unwrap())).await.unwrap();

    // ONE transaction: 300 rows × ~4 KB ≈ 1.2 MB, far over the 64 KiB cap.
    let body = "x".repeat(4096);
    let mut stmts = String::from("BEGIN;");
    for i in 0..300 {
        stmts.push_str(&format!("INSERT INTO big_item VALUES ('r{i:04}', '{body}');"));
    }
    stmts.push_str("COMMIT;");
    client.batch_execute(&stmts).await.unwrap();

    // Collect until all 300 rows arrive. Atomicity: rows may span several
    // poke parts, but every poke must arrive AFTER the commit — i.e. we never
    // see a pokeEnd with a partial subset followed by silence.
    let mut seen = std::collections::HashSet::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while seen.len() < 300 {
        assert!(
            std::time::Instant::now() < deadline,
            "timed out with {}/300 rows — oversized txn lost or torn",
            seen.len()
        );
        if let Downstream::PokePart(p) = next_down(&mut ws).await {
            for op in p.rows_patch.unwrap_or_default() {
                if let RowPatchOp::Put { value, .. } = op {
                    if let Some(Value::String(id)) = value.get("id") {
                        assert_eq!(
                            value.get("body").map(|b| matches!(b, Value::String(s) if s.len() == 4096)),
                            Some(true),
                            "row {id} arrived with torn/missing body"
                        );
                        seen.insert(id.clone());
                    }
                }
            }
        }
    }
    assert_eq!(seen.len(), 300);

    client
        .batch_execute(&format!(
            "SELECT pg_drop_replication_slot('{SLOT}') FROM pg_replication_slots WHERE slot_name='{SLOT}'"
        ))
        .await
        .ok();
}
