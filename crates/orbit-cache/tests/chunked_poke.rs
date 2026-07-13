//! Chunked hydration pokes (real Postgres): with a tiny `ORBIT_POKE_PART_BYTES`
//! a large initial subscription must arrive as MANY `pokePart` frames inside
//! ONE `pokeStart`/`pokeEnd` transaction, in order, with the first part
//! carrying the got-query metadata — exactly what the TS client accumulates
//! (client.ts appends `rowsPatch` across parts and applies at `pokeEnd`).
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

const WS: &str = "127.0.0.1:39740";
const SLOT: &str = "orbit_chunk_slot";
const PUB: &str = "orbit_chunk_pub";
const ROWS: usize = 100;

fn cfg() -> ServerConfig {
    ServerConfig {
        host: "127.0.0.1".into(),
        port: std::env::var("ORBIT_PG_PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(5433),
        user: "orbit".into(),
        database: "orbit".into(),
        password: None,
        tls: orbit_cache::PgTlsMode::Disable,
        tables: vec![TableConfig {
            name: "chunk_item".into(),
            columns: vec![("id".into(), ColumnType::String), ("body".into(), ColumnType::String)],
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
            other => panic!("ws closed/timeout: {other:?}"),
        }
    }
}

#[tokio::test]
async fn large_hydration_arrives_as_many_parts_in_one_poke() {
    // Must be set before the server thread first calls poke() (OnceLock).
    std::env::set_var("ORBIT_POKE_PART_BYTES", "4096");

    let pg_port: u16 =
        std::env::var("ORBIT_PG_PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(5433);
    let conn_str = format!("host=127.0.0.1 port={pg_port} user=orbit dbname=orbit");
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await.expect("connect orbit-pg");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!(
            "SELECT pg_drop_replication_slot('{SLOT}') FROM pg_replication_slots WHERE slot_name='{SLOT}';
             DROP TABLE IF EXISTS chunk_item;
             CREATE TABLE chunk_item (id text PRIMARY KEY, body text);
             ALTER TABLE chunk_item REPLICA IDENTITY FULL;
             INSERT INTO chunk_item
               SELECT lpad(g::text, 4, '0'), repeat('x', 512) FROM generate_series(1, {ROWS}) g;",
        ))
        .await
        .unwrap();

    std::thread::spawn(|| {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        if let Err(e) = rt.block_on(run_server(cfg(), MutatorRegistry::new())) {
            eprintln!("SERVER EXITED WITH ERROR: {e:#}");
        }
    });

    let mut ws = None;
    for _ in 0..100 {
        if let Ok((s, _)) = tokio_tungstenite::connect_async(format!("ws://{WS}/")).await {
            ws = Some(s);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let mut ws = ws.expect("connect to server");
    assert!(matches!(next_down(&mut ws).await, Downstream::Connected(_)));

    let ast = Query::table("chunk_item").order_by("id", Direction::Asc).build();
    ws.send(Message::Text(
        serde_json::to_string(&Upstream::ChangeDesiredQueries(ChangeDesiredQueriesBody {
            desired_queries_patch: vec![QueriesPatchOp::Put { hash: "h1".into(), ast: Some(ast), name: None, args: None, ttl: None }],
            traceparent: None,
        }))
        .unwrap(),
    ))
    .await
    .unwrap();

    // Accumulate exactly the way the TS client does: pokeStart opens a buffer,
    // every pokePart appends, pokeEnd applies.
    let mut started: Option<String> = None;
    let mut parts = 0usize;
    let mut got_query_on_first_part = false;
    let mut ids: Vec<String> = Vec::new();
    loop {
        match next_down(&mut ws).await {
            Downstream::PokeStart(s) => {
                assert!(started.is_none(), "nested pokeStart");
                started = Some(s.poke_id);
            }
            Downstream::PokePart(p) => {
                let poke_id = started.as_deref().expect("pokePart before pokeStart");
                assert_eq!(p.poke_id, poke_id, "part belongs to the open poke");
                if parts == 0 {
                    got_query_on_first_part = p.got_queries_patch.is_some();
                } else {
                    assert!(p.got_queries_patch.is_none(), "metadata only on the first part");
                }
                parts += 1;
                for op in p.rows_patch.unwrap_or_default() {
                    if let RowPatchOp::Put { value, .. } = op {
                        if let Some(Value::String(id)) = value.get("id") {
                            ids.push(id.clone());
                        }
                    }
                }
            }
            Downstream::PokeEnd(e) => {
                assert_eq!(Some(e.poke_id.as_str()), started.as_deref());
                break;
            }
            _ => {}
        }
    }

    // ~100 rows × ~600 serialized bytes at a 4 KiB cap → many parts.
    assert!(parts >= 10, "expected many byte-capped parts, got {parts}");
    assert!(got_query_on_first_part, "gotQueriesPatch must ride the first part");
    assert_eq!(ids.len(), ROWS, "all rows arrive exactly once across parts");
    let mut sorted = ids.clone();
    sorted.sort();
    assert_eq!(ids, sorted, "parts preserve patch order");

    client
        .batch_execute(&format!(
            "SELECT pg_drop_replication_slot('{SLOT}') FROM pg_replication_slots WHERE slot_name='{SLOT}'"
        ))
        .await
        .ok();
}
