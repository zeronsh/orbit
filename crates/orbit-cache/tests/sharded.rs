//! Multi-core sharded serving test (no Postgres).
//!
//! Starts a [`ShardedServer`] with several worker threads, distributes many
//! WebSocket clients round-robin across the shards, then fans a replication
//! event out to every shard and asserts every client — on every thread —
//! receives the incremental poke. This exercises the real `serve_client` path
//! concurrently across OS threads.

use futures_util::{SinkExt, StreamExt};
use oql::ast::Direction;
use oql::ivm::ColumnType;
use oql::value::Value;
use oql::Query;
use orbit_cache::{LogicalEvent, ShardTable, ShardedServer};
use orbit_protocol::{ChangeDesiredQueriesBody, Downstream, QueriesPatchOp, RowPatchOp, Upstream};
use std::sync::Arc;
use tokio::net::TcpListener;

type Ws = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

fn row(pairs: &[(&str, Value)]) -> oql::value::Row {
    pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
}

async fn next_down(ws: &mut Ws) -> Downstream {
    loop {
        if let tokio_tungstenite::tungstenite::Message::Text(t) = ws.next().await.unwrap().unwrap() {
            return serde_json::from_str(&t).unwrap();
        }
    }
}

async fn read_poke_ids(ws: &mut Ws) -> Vec<String> {
    assert!(matches!(next_down(ws).await, Downstream::PokeStart(_)));
    let part = match next_down(ws).await {
        Downstream::PokePart(p) => p,
        o => panic!("expected pokePart, got {o:?}"),
    };
    assert!(matches!(next_down(ws).await, Downstream::PokeEnd(_)));
    part.rows_patch
        .unwrap_or_default()
        .iter()
        .filter_map(|p| match p {
            RowPatchOp::Put { value, .. } => match value.get("id") {
                Some(Value::String(s)) => Some(s.clone()),
                _ => None,
            },
            _ => None,
        })
        .collect()
}

async fn connect_and_subscribe(addr: std::net::SocketAddr) -> Ws {
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/")).await.unwrap();
    assert!(matches!(next_down(&mut ws).await, Downstream::Connected(_)));
    let ast = Query::table("item").order_by("id", Direction::Asc).build();
    let sub = Upstream::ChangeDesiredQueries(ChangeDesiredQueriesBody {
        desired_queries_patch: vec![QueriesPatchOp::Put {
            hash: "h1".into(),
            ttl: None,
            ast: Some(ast),
            name: None,
            args: None,
        }],
        traceparent: None,
    });
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        serde_json::to_string(&sub).unwrap(),
    ))
    .await
    .unwrap();
    ws
}

#[tokio::test]
async fn clients_across_shards_all_receive_pokes() {
    // 4 shard threads, each with its own replica seeded with item i1.
    let server = Arc::new(ShardedServer::start(
        4,
        vec![ShardTable {
            name: "item".into(),
            columns: vec![("id".into(), ColumnType::String)],
            primary_key: vec!["id".into()],
            seed: vec![row(&[("id", "i1".into())])],
        }],
    ));

    // Accept connections and hand them (as std streams) to shards round-robin.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    {
        let server = server.clone();
        tokio::spawn(async move {
            loop {
                let (sock, _) = listener.accept().await.unwrap();
                let std_sock = sock.into_std().unwrap();
                server.dispatch(std_sock);
            }
        });
    }

    // 8 clients spread across the 4 shards; each gets the initial poke (i1).
    let mut clients = Vec::new();
    for _ in 0..8 {
        let mut ws = connect_and_subscribe(addr).await;
        assert_eq!(read_poke_ids(&mut ws).await, vec!["i1"]);
        clients.push(ws);
    }

    // Fan an insert out to every shard.
    server.broadcast_event(LogicalEvent::Insert {
        table: "item".into(),
        row: row(&[("id", "i2".into())]),
    });
    server.broadcast_event(LogicalEvent::Commit);

    // Every client — on every shard thread — receives the incremental poke (i2).
    for mut ws in clients {
        assert_eq!(read_poke_ids(&mut ws).await, vec!["i2"]);
    }
}
