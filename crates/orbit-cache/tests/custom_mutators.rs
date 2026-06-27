//! Custom-mutator test: a registered server-side mutator turns a `push` of a
//! custom mutation into CRUD ops, which apply to the replica and poke the
//! subscriber.

use std::collections::BTreeMap;

use futures_util::{SinkExt, StreamExt};
use oql::ast::Direction;
use oql::value::Value;
use oql::Query;
use orbit_cache::{serve_connection_with_mutators, MutatorRegistry, Replica};
use orbit_protocol::{
    ChangeDesiredQueriesBody, CrudOp, Downstream, Mutation, PushBody, QueriesPatchOp, RowPatchOp,
    Upstream,
};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;

async fn next_downstream<S>(ws: &mut tokio_tungstenite::WebSocketStream<S>) -> Downstream
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    loop {
        if let Message::Text(t) = ws.next().await.unwrap().unwrap() {
            return serde_json::from_str(&t).unwrap();
        }
    }
}

async fn read_poke_put_ids<S>(ws: &mut tokio_tungstenite::WebSocketStream<S>) -> Vec<String>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    assert!(matches!(next_downstream(ws).await, Downstream::PokeStart(_)));
    let part = match next_downstream(ws).await {
        Downstream::PokePart(p) => p,
        o => panic!("expected pokePart, got {o:?}"),
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
async fn custom_mutator_writes_through() {
    let mut replica = Replica::new();
    let mut cols = BTreeMap::new();
    cols.insert("id".to_string(), oql::ivm::ColumnType::String);
    cols.insert("name".to_string(), oql::ivm::ColumnType::String);
    replica.add_table("widget", cols, vec!["id".into()]);

    // Register a custom mutator: createWidget({id, name}) -> insert.
    let mut mutators = MutatorRegistry::new();
    mutators.register("createWidget", |_replica, args| {
        let obj = args[0].as_object().expect("object arg");
        let id = obj["id"].as_str().unwrap().to_string();
        let name = obj["name"].as_str().unwrap().to_string();
        let mut value = oql::value::Row::new();
        value.insert("id".to_string(), Value::String(id));
        value.insert("name".to_string(), Value::String(name));
        vec![CrudOp::Insert {
            table_name: "widget".into(),
            primary_key: vec!["id".into()],
            value,
        }]
    });

    let (_change_tx, mut change_rx) = tokio::sync::mpsc::unbounded_channel();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = async {
        let (sock, _) = listener.accept().await.unwrap();
        let ws = tokio_tungstenite::accept_async(sock).await.unwrap();
        serve_connection_with_mutators(ws, &replica, &mutators, &mut change_rx)
            .await
            .unwrap();
    };

    let client = async {
        let url = format!("ws://{addr}/sync/v51/connect");
        let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
        assert!(matches!(next_downstream(&mut ws).await, Downstream::Connected(_)));

        // Subscribe (empty initial result).
        let ast = Query::table("widget").order_by("id", Direction::Asc).build();
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
        assert_eq!(read_poke_put_ids(&mut ws).await, Vec::<String>::new());

        // Push a custom mutation -> mutator runs -> insert -> poke.
        let push = Upstream::Push(PushBody {
            client_group_id: "g1".into(),
            mutations: vec![Mutation::Custom {
                id: 1,
                client_id: "c1".into(),
                name: "createWidget".into(),
                args: vec![serde_json::json!({"id": "w5", "name": "Widget5"})],
                timestamp: 0.0,
            }],
            push_version: 1,
            schema_version: None,
            timestamp: 0.0,
            request_id: "r1".into(),
            traceparent: None,
        });
        ws.send(Message::Text(serde_json::to_string(&push).unwrap())).await.unwrap();
        // First poke acks the mutation (empty rows); second carries w5.
        assert_eq!(read_poke_put_ids(&mut ws).await, Vec::<String>::new());
        assert_eq!(read_poke_put_ids(&mut ws).await, vec!["w5"]);

        ws.close(None).await.unwrap();
    };

    tokio::join!(server, client);
}
