//! Custom (named) query backend: a client subscribes by `name` + `args` (no
//! AST). The server resolves the named query via the `QueryRegistry` to an AST,
//! builds the pipeline, and streams the result — Zero's "custom / synced
//! queries" feature, server-authored.

use std::collections::BTreeMap;
use std::rc::Rc;

use futures_util::{SinkExt, StreamExt};
use oql::ast::{Direction, SimpleOperator};
use oql::value::Value;
use oql::Query;
use orbit_cache::{serve_client, MutatorRegistry, QueryRegistry, Replica};
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

#[tokio::test]
async fn named_query_resolved_server_side() {
    let local = LocalSet::new();
    local
        .run_until(async {
            let mut replica = Replica::new();
            let mut cols = BTreeMap::new();
            cols.insert("id".to_string(), oql::ivm::ColumnType::String);
            cols.insert("owner".to_string(), oql::ivm::ColumnType::String);
            let src = replica.add_table("issue", cols, vec!["id".into()]);
            src.borrow_mut().insert_initial(row(&[("id", "i1".into()), ("owner", "u1".into())]));
            src.borrow_mut().insert_initial(row(&[("id", "i2".into()), ("owner", "u2".into())]));
            src.borrow_mut().insert_initial(row(&[("id", "i3".into()), ("owner", "u1".into())]));
            let replica = Rc::new(replica);
            let mutators = Rc::new(MutatorRegistry::new());

            // Server-authored named query: issuesByOwner(owner).
            let mut q = QueryRegistry::new();
            q.register("issuesByOwner", |args| {
                let owner = args.first().and_then(|v| v.as_str()).unwrap_or("").to_string();
                Query::table("issue")
                    .where_("owner", SimpleOperator::Eq, owner)
                    .order_by("id", Direction::Asc)
                    .build()
            });
            let queries = Rc::new(q);
            let (tick_tx, _) = tokio::sync::broadcast::channel::<()>(16);

            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();

            {
                let replica = replica.clone();
                let mutators = mutators.clone();
                let queries = queries.clone();
                let tick_tx = tick_tx.clone();
                spawn_local(async move {
                    let (sock, _) = listener.accept().await.unwrap();
                    let ws = tokio_tungstenite::accept_async(sock).await.unwrap();
                    let _ = serve_client(ws, &*replica, None, &mutators, &queries, &orbit_cache::Forwarder::new(Default::default()), &orbit_cache::AuthContext::default(), vec![], None, None, tick_tx.subscribe()).await;
                });
            }

            let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/")).await.unwrap();
            assert!(matches!(next_down(&mut ws).await, Downstream::Connected(_)));

            // Subscribe by NAME + ARGS (no AST).
            let sub = Upstream::ChangeDesiredQueries(ChangeDesiredQueriesBody {
                desired_queries_patch: vec![QueriesPatchOp::Put {
                    hash: "h1".into(),
                    ttl: None,
                    ast: None,
                    name: Some("issuesByOwner".into()),
                    args: Some(vec![serde_json::json!("u1")]),
                }],
                traceparent: None,
            });
            ws.send(Message::Text(serde_json::to_string(&sub).unwrap())).await.unwrap();

            // Server resolved the named query -> only u1's issues.
            assert_eq!(read_poke_ids(&mut ws).await, vec!["i1", "i3"]);
        })
        .await;
}
