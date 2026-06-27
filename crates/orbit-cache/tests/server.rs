//! WebSocket sync-server test: a client connects, subscribes, receives the
//! initial result as a poke, then receives an *incremental* poke when the
//! replica changes — exercising the reactive sync loop over the real wire
//! protocol.

use std::collections::BTreeMap;

use futures_util::{SinkExt, StreamExt};
use oql::ast::Direction;
use oql::ivm::SourceChange;
use oql::value::Value;
use oql::Query;
use orbit_cache::{serve_connection, Replica};
use orbit_protocol::{
    ChangeDesiredQueriesBody, CrudArg, CrudOp, Downstream, Mutation, PushBody, QueriesPatchOp,
    RowPatchOp, Upstream, CRUD_MUTATION_NAME,
};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;

fn row(pairs: &[(&str, Value)]) -> oql::value::Row {
    pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
}

async fn next_downstream<S>(ws: &mut tokio_tungstenite::WebSocketStream<S>) -> Downstream
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    loop {
        let msg = ws.next().await.expect("stream ended").expect("ws error");
        if let Message::Text(t) = msg {
            return serde_json::from_str(&t).expect("parse downstream");
        }
    }
}

/// Read a full poke (start/part/end) and return the row-patch ids of the part.
async fn read_poke_put_ids<S>(ws: &mut tokio_tungstenite::WebSocketStream<S>) -> Vec<String>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    assert!(matches!(next_downstream(ws).await, Downstream::PokeStart(_)));
    let part = match next_downstream(ws).await {
        Downstream::PokePart(p) => p,
        other => panic!("expected pokePart, got {other:?}"),
    };
    assert!(matches!(next_downstream(ws).await, Downstream::PokeEnd(_)));
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

#[tokio::test]
async fn websocket_subscribe_then_incremental_poke() {
    let mut replica = Replica::new();
    let mut cols = BTreeMap::new();
    cols.insert("id".to_string(), oql::ivm::ColumnType::String);
    cols.insert("name".to_string(), oql::ivm::ColumnType::String);
    let src = replica.add_table("widget", cols, vec!["id".into()]);
    src.borrow_mut().insert_initial(row(&[("id", "w1".into()), ("name", "Gear".into())]));
    src.borrow_mut().insert_initial(row(&[("id", "w2".into()), ("name", "Cog".into())]));

    let (change_tx, change_rx) = tokio::sync::mpsc::unbounded_channel();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = async {
        let (sock, _) = listener.accept().await.unwrap();
        let ws = tokio_tungstenite::accept_async(sock).await.unwrap();
        serve_connection(ws, &replica, change_rx).await.unwrap();
    };

    let client = async {
        let url = format!("ws://{addr}/sync/v51/connect");
        let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
        assert!(matches!(next_downstream(&mut ws).await, Downstream::Connected(_)));

        // Subscribe: widget ORDER BY id.
        let ast = Query::table("widget").order_by("id", Direction::Asc).build();
        let up = Upstream::ChangeDesiredQueries(ChangeDesiredQueriesBody {
            desired_queries_patch: vec![QueriesPatchOp::Put {
                hash: "h1".into(),
                ttl: None,
                ast: Some(ast),
                name: None,
                args: None,
            }],
            traceparent: None,
        });
        ws.send(Message::Text(serde_json::to_string(&up).unwrap())).await.unwrap();

        // Initial poke: w1, w2.
        assert_eq!(read_poke_put_ids(&mut ws).await, vec!["w1", "w2"]);

        // A live replica change: add w3.
        change_tx
            .send((
                "widget".to_string(),
                SourceChange::Add(row(&[("id", "w3".into()), ("name", "Bolt".into())])),
            ))
            .unwrap();

        // Incremental poke: just w3.
        assert_eq!(read_poke_put_ids(&mut ws).await, vec!["w3"]);

        // Now push a CRUD insert over the socket (write-through) -> incremental poke.
        let push = Upstream::Push(PushBody {
            client_group_id: "g1".into(),
            mutations: vec![Mutation::Crud {
                id: 1,
                client_id: "c1".into(),
                name: CRUD_MUTATION_NAME.into(),
                args: vec![CrudArg {
                    ops: vec![CrudOp::Insert {
                        table_name: "widget".into(),
                        primary_key: vec!["id".into()],
                        value: row(&[("id", "w4".into()), ("name", "Spring".into())]),
                    }],
                }],
                timestamp: 0.0,
            }],
            push_version: 1,
            schema_version: None,
            timestamp: 0.0,
            request_id: "r1".into(),
            traceparent: None,
        });
        ws.send(Message::Text(serde_json::to_string(&push).unwrap())).await.unwrap();
        // First poke acks the mutation (lastMutationIDChanges), then the row poke.
        let ack = match next_downstream(&mut ws).await {
            Downstream::PokeStart(_) => {
                let part = match next_downstream(&mut ws).await {
                    Downstream::PokePart(p) => p,
                    o => panic!("expected pokePart, got {o:?}"),
                };
                assert!(matches!(next_downstream(&mut ws).await, Downstream::PokeEnd(_)));
                part.last_mutation_id_changes
            }
            o => panic!("expected pokeStart, got {o:?}"),
        };
        assert_eq!(ack.unwrap().get("c1"), Some(&1u64), "mutation 1 from c1 acked");
        // Then the row patch for w4.
        assert_eq!(read_poke_put_ids(&mut ws).await, vec!["w4"]);

        ws.close(None).await.unwrap();
    };

    tokio::join!(server, client);
}
