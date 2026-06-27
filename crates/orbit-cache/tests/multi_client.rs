//! Multi-client test: two WebSocket clients subscribe to the same query over a
//! shared replica; when the replica advances (one tick broadcast), both clients
//! receive an incremental poke. This is the fan-out the integrated server uses.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use futures_util::{SinkExt, StreamExt};
use oql::ast::Direction;
use oql::ivm::{source_push, MemorySource, SourceChange};
use oql::value::Value;
use oql::Query;
use orbit_cache::{serve_client, MutatorRegistry, Replica};
use orbit_protocol::{ChangeDesiredQueriesBody, Downstream, QueriesPatchOp, RowPatchOp, Upstream};
use tokio::net::TcpListener;
use tokio::task::{spawn_local, LocalSet};
use tokio_tungstenite::tungstenite::Message;

type Ws = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

fn row(pairs: &[(&str, Value)]) -> oql::value::Row {
    pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
}

async fn next_down(ws: &mut Ws) -> Downstream {
    loop {
        if let Message::Text(t) = ws.next().await.unwrap().unwrap() {
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
    ws.send(Message::Text(serde_json::to_string(&sub).unwrap())).await.unwrap();
    ws
}

#[tokio::test]
async fn two_clients_both_receive_pokes() {
    let local = LocalSet::new();
    local
        .run_until(async {
            // Shared replica with one item.
            let mut replica = Replica::new();
            let mut cols = BTreeMap::new();
            cols.insert("id".to_string(), oql::ivm::ColumnType::String);
            replica.add_table("item", cols, vec!["id".into()]);
            let src: Rc<RefCell<MemorySource>> = replica.source("item").unwrap();
            src.borrow_mut().insert_initial(row(&[("id", "i1".into())]));

            let replica = Rc::new(replica);
            let mutators = Rc::new(MutatorRegistry::new());
            let (tick_tx, _) = tokio::sync::broadcast::channel::<()>(16);

            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();

            // Accept loop.
            {
                let replica = replica.clone();
                let mutators = mutators.clone();
                let tick_tx = tick_tx.clone();
                spawn_local(async move {
                    loop {
                        let (sock, _) = listener.accept().await.unwrap();
                        let replica = replica.clone();
                        let mutators = mutators.clone();
                        let ticks = tick_tx.subscribe();
                        spawn_local(async move {
                            let ws = tokio_tungstenite::accept_async(sock).await.unwrap();
                            let queries = orbit_cache::QueryRegistry::new();
                            let _ = serve_client(ws, &*replica, None, &mutators, &queries, &orbit_cache::Forwarder::new(Default::default()), &orbit_cache::AuthContext::default(), vec![], None, None, ticks).await;
                        });
                    }
                });
            }

            // Both clients connect + get the initial poke (i1).
            let mut c1 = connect_and_subscribe(addr).await;
            assert_eq!(read_poke_ids(&mut c1).await, vec!["i1"]);
            let mut c2 = connect_and_subscribe(addr).await;
            assert_eq!(read_poke_ids(&mut c2).await, vec!["i1"]);

            // Advance the shared replica + notify all clients.
            source_push(&src, SourceChange::Add(row(&[("id", "i2".into())])));
            tick_tx.send(()).unwrap();

            // Both clients receive the incremental poke (i2).
            assert_eq!(read_poke_ids(&mut c1).await, vec!["i2"]);
            assert_eq!(read_poke_ids(&mut c2).await, vec!["i2"]);
        })
        .await;
}
