//! Initial-snapshot test against **real Postgres**: pre-existing rows are copied
//! into the replica, then live replication streams further changes on top.

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use oql::ast::Direction;
use oql::ivm::operator::Link;
use oql::ivm::{Catch, ColumnType};
use oql::value::Value;
use oql::{build_pipeline, Query, SourceProvider};

use orbit_cache::pg::{create_publication, create_slot, initial_sync};
use orbit_cache::Replica;
use tokio_postgres::NoTls;

struct P<'a>(&'a Replica);
impl<'a> SourceProvider for P<'a> {
    fn get_source(&self, table: &str) -> Option<Rc<RefCell<oql::ivm::MemorySource>>> {
        self.0.source(table)
    }
}

#[tokio::test]
async fn initial_snapshot_then_stream() {
    let host = std::env::var("ORBIT_PG_HOST").unwrap_or_else(|_| "127.0.0.1".into());
    let port: u16 = std::env::var("ORBIT_PG_PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(5433);
    let conn_str = format!("host={host} port={port} user=orbit dbname=orbit");
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });

    // Pre-existing data BEFORE any replication setup.
    client
        .batch_execute(
            "DROP TABLE IF EXISTS account;
             CREATE TABLE account (id text PRIMARY KEY, balance int);
             ALTER TABLE account REPLICA IDENTITY FULL;
             INSERT INTO account VALUES ('a1', 100), ('a2', 200);",
        )
        .await
        .unwrap();

    create_publication(&client, "orbit_pub_isync", &["account"]).await.unwrap();
    let start_lsn = create_slot(&client, "orbit_slot_isync").await.unwrap();

    // Build the replica and seed it from the snapshot.
    let mut replica = Replica::new();
    let cols: Vec<(String, ColumnType)> = vec![
        ("id".into(), ColumnType::String),
        ("balance".into(), ColumnType::Number),
    ];
    let src = replica.add_table("account", cols.iter().cloned().collect(), vec!["id".into()]);
    let n = initial_sync(&client, &src, &cols).await.unwrap();
    assert_eq!(n, 2, "two pre-existing rows seeded");

    // Materialize a query over the seeded replica.
    let provider = P(&replica);
    let ast = Query::table("account").order_by("id", Direction::Asc).build();
    let top = build_pipeline(&ast, &provider);
    let catch = Catch::new(top.input.clone());
    let link: Link = catch.clone();
    top.set_output(link);

    // Initial view already has the snapshot rows.
    let initial: Vec<String> = catch.borrow().fetch().iter().map(|n| match n.row.get("id") {
        Some(Value::String(s)) => s.clone(),
        _ => unreachable!(),
    }).collect();
    assert_eq!(initial, vec!["a1", "a2"]);

    // Now stream a live insert on top of the snapshot.
    let mut stream = orbit_cache::ReplicationStream::start(
        &host, port, "orbit", "orbit", "orbit_slot_isync", "orbit_pub_isync", start_lsn,
    )
    .await
    .unwrap();
    client.batch_execute("INSERT INTO account VALUES ('a3', 300)").await.unwrap();

    // Pump events (tolerating the snapshot overlap) until a3 appears.
    let deadline = Duration::from_secs(10);
    let mut have_a3 = false;
    while !have_a3 {
        let (_lsn, ev) = tokio::time::timeout(deadline, stream.next_event()).await.unwrap().unwrap();
        replica.apply(ev);
        have_a3 = catch.borrow().fetch().iter().any(|n| n.row.get("id") == Some(&Value::String("a3".into())));
    }

    let mut ids: Vec<String> = catch.borrow().fetch().iter().map(|n| match n.row.get("id") {
        Some(Value::String(s)) => s.clone(),
        _ => unreachable!(),
    }).collect();
    ids.sort();
    assert_eq!(ids, vec!["a1", "a2", "a3"]);

    client
        .batch_execute("SELECT pg_drop_replication_slot('orbit_slot_isync') FROM pg_replication_slots WHERE slot_name='orbit_slot_isync'")
        .await
        .ok();
}
