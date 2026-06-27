//! Zero-client handshake compatibility: the client sends `initConnection` in the
//! `Sec-WebSocket-Protocol` header (not as a message). The server must decode it
//! and serve the initial poke without any follow-up message — exactly what the
//! real Zero TypeScript client does.

use std::collections::BTreeMap;
use std::rc::Rc;

use futures_util::StreamExt;
use oql::ast::Direction;
use oql::value::Value;
use oql::Query;
use orbit_cache::handshake::{accept_zero_ws, encode_sec_protocol};
use orbit_cache::{serve_client, MutatorRegistry, Replica};
use orbit_protocol::{Downstream, RowPatchOp};
use tokio::net::TcpListener;
use tokio::task::{spawn_local, LocalSet};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

fn row(pairs: &[(&str, Value)]) -> oql::value::Row {
    pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
}

#[tokio::test]
async fn zero_client_handshake_via_sec_protocol_header() {
    let local = LocalSet::new();
    local
        .run_until(async {
            let mut replica = Replica::new();
            let mut cols = BTreeMap::new();
            cols.insert("id".to_string(), oql::ivm::ColumnType::String);
            let src = replica.add_table("item", cols, vec!["id".into()]);
            src.borrow_mut().insert_initial(row(&[("id", "i1".into())]));
            let replica = Rc::new(replica);
            let mutators = Rc::new(MutatorRegistry::new());
            let (tick_tx, _) = tokio::sync::broadcast::channel::<()>(16);

            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();

            {
                let replica = replica.clone();
                let mutators = mutators.clone();
                let tick_tx = tick_tx.clone();
                spawn_local(async move {
                    let (sock, _) = listener.accept().await.unwrap();
                    let (ws, info) = accept_zero_ws(sock).await.unwrap();
                    let queries = orbit_cache::QueryRegistry::new();
                    let _ = serve_client(ws, &*replica, None, &mutators, &queries, &orbit_cache::Forwarder::new(Default::default()), &orbit_cache::AuthContext::default(), info.desired_queries, info.client_id, info.base_cookie, tick_tx.subscribe()).await;
                });
            }

            // Build the initConnection body with a query AST, encode it the way
            // the Zero client does, and put it in the Sec-WebSocket-Protocol header.
            let ast = Query::table("item").order_by("id", Direction::Asc).build();
            let body = serde_json::json!({
                "desiredQueriesPatch": [{
                    "op": "put",
                    "hash": "h1",
                    "ast": serde_json::to_value(&ast).unwrap(),
                }]
            });
            let encoded = encode_sec_protocol(&body, None);

            let mut req = format!("ws://{addr}/sync/v51/connect").into_client_request().unwrap();
            req.headers_mut().insert("sec-websocket-protocol", encoded.parse().unwrap());
            let (mut ws, _resp) = tokio_tungstenite::connect_async(req).await.unwrap();

            // Read `connected` then the initial poke — no message sent by us.
            let mut got_connected = false;
            let mut poke_ids: Vec<String> = Vec::new();
            while poke_ids.is_empty() {
                if let Message::Text(t) = ws.next().await.unwrap().unwrap() {
                    match serde_json::from_str::<Downstream>(&t).unwrap() {
                        Downstream::Connected(_) => got_connected = true,
                        Downstream::PokePart(p) => {
                            poke_ids = p
                                .rows_patch
                                .unwrap_or_default()
                                .iter()
                                .filter_map(|op| match op {
                                    RowPatchOp::Put { value, .. } => match value.get("id") {
                                        Some(Value::String(s)) => Some(s.clone()),
                                        _ => None,
                                    },
                                    _ => None,
                                })
                                .collect();
                        }
                        _ => {}
                    }
                }
            }
            assert!(got_connected, "server sent connected");
            assert_eq!(poke_ids, vec!["i1"], "initial poke from header-supplied query");
        })
        .await;
}
