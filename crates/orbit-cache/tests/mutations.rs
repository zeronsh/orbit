//! Write-path test against **real Postgres**: a client CRUD mutation is applied
//! to Postgres by the mutagen, flows back through logical replication into the
//! replica, and updates the materialized OQL view — the full write-through loop.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::time::Duration;

use oql::ast::Direction;
use oql::ivm::operator::Link;
use oql::ivm::Catch;
use oql::value::Value;
use oql::{build_pipeline, Query, SourceProvider};

use orbit_cache::pg::pgoutput::LogicalEvent;
use orbit_cache::pg::{create_publication, create_slot};
use orbit_cache::{apply_mutation, Replica};
use orbit_protocol::{CrudArg, CrudOp, Mutation, CRUD_MUTATION_NAME};

use tokio_postgres::{Client, NoTls};

struct ReplicaProvider<'a>(&'a Replica);
impl<'a> SourceProvider for ReplicaProvider<'a> {
    fn get_source(&self, table: &str) -> Option<Rc<RefCell<oql::ivm::MemorySource>>> {
        self.0.source(table)
    }
}

fn row(pairs: &[(&str, Value)]) -> oql::value::Row {
    pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
}

fn crud(id: u64, op: CrudOp) -> Mutation {
    Mutation::Crud {
        id,
        client_id: "c1".into(),
        name: CRUD_MUTATION_NAME.into(),
        args: vec![CrudArg { ops: vec![op] }],
        timestamp: 0.0,
    }
}

/// Pull the next data event, applying it to the replica.
async fn pump(stream: &mut orbit_cache::ReplicationStream, replica: &Replica) {
    loop {
        let (_lsn, ev) = tokio::time::timeout(Duration::from_secs(10), stream.next_event())
            .await
            .expect("timeout")
            .expect("repl error");
        let is_data = matches!(
            ev,
            LogicalEvent::Insert { .. } | LogicalEvent::Update { .. } | LogicalEvent::Delete { .. }
        );
        replica.apply(ev);
        if is_data {
            return;
        }
    }
}

async fn ids(catch: &Rc<RefCell<Catch>>) -> Vec<String> {
    let mut v: Vec<String> = catch
        .borrow()
        .fetch()
        .iter()
        .map(|n| match n.row.get("id") {
            Some(Value::String(s)) => s.clone(),
            _ => unreachable!(),
        })
        .collect();
    v.sort();
    v
}

#[tokio::test]
async fn crud_mutation_writes_through_to_view() {
    let host = std::env::var("ORBIT_PG_HOST").unwrap_or_else(|_| "127.0.0.1".into());
    let port: u16 = std::env::var("ORBIT_PG_PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(5433);
    let conn_str = format!("host={host} port={port} user=orbit dbname=orbit");
    let (client, connection): (Client, _) = tokio_postgres::connect(&conn_str, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });

    client
        .batch_execute(
            "DROP TABLE IF EXISTS note;
             CREATE TABLE note (id text PRIMARY KEY, body text);
             ALTER TABLE note REPLICA IDENTITY FULL;",
        )
        .await
        .unwrap();
    create_publication(&client, "orbit_pub_mut", &["note"]).await.unwrap();
    let start_lsn = create_slot(&client, "orbit_slot_mut").await.unwrap();
    let mut stream = orbit_cache::ReplicationStream::start(
        &host, port, "orbit", "orbit", "orbit_slot_mut", "orbit_pub_mut", start_lsn,
    )
    .await
    .unwrap();

    let mut replica = Replica::new();
    let mut cols = BTreeMap::new();
    cols.insert("id".to_string(), oql::ivm::ColumnType::String);
    cols.insert("body".to_string(), oql::ivm::ColumnType::String);
    let _ = replica.add_table("note", cols, vec!["id".into()]);

    let provider = ReplicaProvider(&replica);
    let ast = Query::table("note").order_by("id", Direction::Asc).build();
    let top = build_pipeline(&ast, &provider);
    let catch = Catch::new(top.input.clone());
    let link: Link = catch.clone();
    top.set_output(link);

    // INSERT via the mutagen → write-through → view.
    apply_mutation(
        &client,
        &crud(
            1,
            CrudOp::Insert {
                table_name: "note".into(),
                primary_key: vec!["id".into()],
                value: row(&[("id", "n1".into()), ("body", "hello".into())]),
            },
        ),
    )
    .await
    .unwrap();
    pump(&mut stream, &replica).await;
    assert_eq!(ids(&catch).await, vec!["n1"]);

    // UPDATE via the mutagen.
    apply_mutation(
        &client,
        &crud(
            2,
            CrudOp::Update {
                table_name: "note".into(),
                primary_key: vec!["id".into()],
                value: row(&[("id", "n1".into()), ("body", "updated".into())]),
            },
        ),
    )
    .await
    .unwrap();
    pump(&mut stream, &replica).await;
    let body = catch.borrow().fetch()[0].row.get("body").cloned();
    assert_eq!(body, Some(Value::String("updated".into())));

    // DELETE via the mutagen.
    apply_mutation(
        &client,
        &crud(
            3,
            CrudOp::Delete {
                table_name: "note".into(),
                primary_key: vec!["id".into()],
                value: row(&[("id", "n1".into())]),
            },
        ),
    )
    .await
    .unwrap();
    pump(&mut stream, &replica).await;
    assert!(ids(&catch).await.is_empty());

    client
        .batch_execute("SELECT pg_drop_replication_slot('orbit_slot_mut') FROM pg_replication_slots WHERE slot_name='orbit_slot_mut'")
        .await
        .ok();
}
