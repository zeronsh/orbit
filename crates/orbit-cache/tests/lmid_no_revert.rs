//! Regression test for the "pixels revert while another user draws" bug.
//!
//! A client's optimistic overlay is dropped when the server confirms its
//! `lastMutationID`. That ack MUST ride in the same poke as the mutation's rows
//! (which the server derives from the replicated `orbit_client_mutations` table,
//! same commit → same tick). The bug was that the ack was flushed on ANY tick —
//! so another client's write (its own tick) would ack your mutation before your
//! row arrived, reverting it. This test drives two clients over one shared replica
//! + shared lastMutationID map and asserts:
//!   1. a tick caused by client B's write never carries client A's ack, and
//!   2. client A's ack arrives together with A's own row.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;

use futures_util::{SinkExt, StreamExt};
use oql::ast::Direction;
use oql::ivm::{source_push, MemorySource, SourceChange};
use oql::value::Value;
use oql::Query;
use orbit_cache::{serve_client, LmidMap, MutatorRegistry, Replica};
use orbit_protocol::{
    ChangeDesiredQueriesBody, Downstream, Mutation, PushBody, QueriesPatchOp, RowPatchOp, Upstream,
};
use tokio::net::TcpListener;
use tokio::task::{spawn_local, LocalSet};
use tokio_tungstenite::tungstenite::Message;

type Ws = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

fn row(id: &str) -> oql::value::Row {
    [("id".to_string(), Value::String(id.into()))].into_iter().collect()
}

async fn next_down(ws: &mut Ws) -> Downstream {
    loop {
        if let Message::Text(t) = ws.next().await.unwrap().unwrap() {
            return serde_json::from_str(&t).unwrap();
        }
    }
}

/// Read one full poke: the PUT ids in its rows, and its lastMutationID changes.
async fn read_poke(ws: &mut Ws) -> (Vec<String>, Option<HashMap<String, u64>>) {
    assert!(matches!(next_down(ws).await, Downstream::PokeStart(_)));
    let part = match next_down(ws).await {
        Downstream::PokePart(p) => p,
        o => panic!("expected pokePart, got {o:?}"),
    };
    assert!(matches!(next_down(ws).await, Downstream::PokeEnd(_)));
    let ids = part
        .rows_patch
        .unwrap_or_default()
        .iter()
        .filter_map(|p| match p {
            RowPatchOp::Put { value, .. } => match value.get("id") {
                Some(Value::String(s)) => Some(s.clone()),
                _ => None,
            },
            _ => None,
        })
        .collect();
    (ids, part.last_mutation_id_changes)
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

fn push(client_id: &str, id: u64) -> Upstream {
    Upstream::Push(PushBody {
        client_group_id: "g1".into(),
        mutations: vec![Mutation::Custom {
            id,
            client_id: client_id.into(),
            name: "noop".into(),
            args: vec![],
            timestamp: 0.0,
        }],
        push_version: 1,
        schema_version: None,
        timestamp: 0.0,
        request_id: "r1".into(),
        traceparent: None,
    })
}

#[tokio::test]
async fn lmid_ack_rides_with_own_rows_not_another_clients_tick() {
    let local = LocalSet::new();
    local
        .run_until(async {
            // Shared replica + shared lastMutationID map, as the view-syncer holds them.
            let mut replica = Replica::new();
            let mut cols = BTreeMap::new();
            cols.insert("id".to_string(), oql::ivm::ColumnType::String);
            replica.add_table("item", cols, vec!["id".into()]);
            let src: Rc<RefCell<MemorySource>> = replica.source("item").unwrap();

            let replica = Rc::new(replica);
            let mutators = Rc::new(MutatorRegistry::new());
            let lmids: LmidMap = Rc::new(RefCell::new(HashMap::new()));
            let (tick_tx, _) = tokio::sync::broadcast::channel::<orbit_cache::server::Tick>(16);

            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();

            // Accept loop: first connection is client "cA", second is "cB".
            {
                let replica = replica.clone();
                let mutators = mutators.clone();
                let lmids = lmids.clone();
                let tick_tx = tick_tx.clone();
                spawn_local(async move {
                    let ids = ["cA", "cB"];
                    let mut n = 0;
                    loop {
                        let (sock, _) = listener.accept().await.unwrap();
                        let (replica, mutators, lmids) = (replica.clone(), mutators.clone(), lmids.clone());
                        let ticks = tick_tx.subscribe();
                        let cid = ids[n.min(1)].to_string();
                        n += 1;
                        spawn_local(async move {
                            let ws = tokio_tungstenite::accept_async(sock).await.unwrap();
                            let queries = orbit_cache::QueryRegistry::new();
                            let _ = serve_client(
                                ws,
                                &*replica,
                                None,
                                &mutators,
                                &queries,
                                &orbit_cache::Forwarder::new(Default::default()),
                                &orbit_cache::AuthContext::default(),
                                vec![],
                                Some(cid),
                                None,
                                ticks,
                                &lmids,
                                None,
                            )
                            .await;
                        });
                    }
                });
            }

            let mut a = connect_and_subscribe(addr).await;
            assert_eq!(read_poke(&mut a).await.0, Vec::<String>::new());
            let mut b = connect_and_subscribe(addr).await;
            assert_eq!(read_poke(&mut b).await.0, Vec::<String>::new());

            // Client A optimistically mutates. The server does not ack on receipt —
            // the ack must come from replication with A's row.
            a.send(Message::Text(serde_json::to_string(&push("cA", 1)).unwrap())).await.unwrap();

            // B's write commits: B's row + B's lastMutationID land together, then tick.
            source_push(&src, SourceChange::Add(row("b1")));
            lmids.borrow_mut().insert("cB".into(), 1);
            tick_tx.send(std::sync::Arc::new(Vec::new())).unwrap();

            // A sees B's row but NOT an ack for A's own mutation (the bug would ack
            // cA here — before A's row exists — and revert A's optimistic pixel).
            let (a_ids, a_lmids) = read_poke(&mut a).await;
            assert_eq!(a_ids, vec!["b1"]);
            assert!(
                a_lmids.as_ref().is_none_or(|m| !m.contains_key("cA")),
                "another client's tick must not ack A's mutation: {a_lmids:?}"
            );
            // B gets B's row together with its own ack.
            let (b_ids, b_lmids) = read_poke(&mut b).await;
            assert_eq!(b_ids, vec!["b1"]);
            assert_eq!(b_lmids.unwrap().get("cB"), Some(&1));

            // A's mutation now returns via replication: A's row + A's lastMutationID
            // in the same commit → the ack rides with the row.
            source_push(&src, SourceChange::Add(row("a1")));
            lmids.borrow_mut().insert("cA".into(), 1);
            tick_tx.send(std::sync::Arc::new(Vec::new())).unwrap();

            let (a_ids, a_lmids) = read_poke(&mut a).await;
            assert_eq!(a_ids, vec!["a1"]);
            assert_eq!(
                a_lmids.expect("A's ack must arrive with its row").get("cA"),
                Some(&1),
                "A's lastMutationID ack must ride with A's own row"
            );
        })
        .await;
}
