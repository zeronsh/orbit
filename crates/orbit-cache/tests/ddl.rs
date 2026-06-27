//! Schema migration (DDL) against **real Postgres**: an `ALTER TABLE ADD COLUMN`
//! mid-stream is picked up — the pgoutput Relation message refreshes the decoder
//! and subsequent rows carry the new column into the materialized view.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::time::Duration;

use oql::ast::Direction;
use oql::ivm::operator::Link;
use oql::ivm::{Catch, ColumnType};
use oql::value::Value;
use oql::{build_pipeline, Query, SourceProvider};

use orbit_cache::pg::pgoutput::LogicalEvent;
use orbit_cache::pg::{create_publication, create_slot};
use orbit_cache::Replica;
use tokio_postgres::NoTls;

struct P<'a>(&'a Replica);
impl<'a> SourceProvider for P<'a> {
    fn get_source(&self, table: &str) -> Option<Rc<RefCell<oql::ivm::MemorySource>>> {
        self.0.source(table)
    }
}

#[tokio::test]
async fn add_column_mid_stream() {
    let host = std::env::var("ORBIT_PG_HOST").unwrap_or_else(|_| "127.0.0.1".into());
    let port: u16 = std::env::var("ORBIT_PG_PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(5433);
    let conn_str = format!("host={host} port={port} user=orbit dbname=orbit");
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });

    client
        .batch_execute(
            "DROP TABLE IF EXISTS thing;
             CREATE TABLE thing (id text PRIMARY KEY, a text);
             ALTER TABLE thing REPLICA IDENTITY FULL;",
        )
        .await
        .unwrap();
    create_publication(&client, "orbit_pub_ddladd", &["thing"]).await.unwrap();
    let start_lsn = create_slot(&client, "orbit_slot_ddladd").await.unwrap();
    let mut stream = orbit_cache::ReplicationStream::start(
        &host, port, "orbit", "orbit", "orbit_slot_ddladd", "orbit_pub_ddladd", start_lsn,
    )
    .await
    .unwrap();

    let mut replica = Replica::new();
    let mut cols = BTreeMap::new();
    cols.insert("id".to_string(), ColumnType::String);
    cols.insert("a".to_string(), ColumnType::String);
    let _ = replica.add_table("thing", cols, vec!["id".into()]);

    let provider = P(&replica);
    let ast = Query::table("thing").order_by("id", Direction::Asc).build();
    let top = build_pipeline(&ast, &provider);
    let catch = Catch::new(top.input.clone());
    let link: Link = catch.clone();
    top.set_output(link);

    // Migrate: add a column, then insert a row using it.
    client
        .batch_execute(
            "ALTER TABLE thing ADD COLUMN b text;
             INSERT INTO thing (id, a, b) VALUES ('x1', 'hello', 'world');",
        )
        .await
        .unwrap();

    // Pump until the insert arrives.
    loop {
        let (_lsn, ev) = tokio::time::timeout(Duration::from_secs(10), stream.next_event())
            .await
            .unwrap()
            .unwrap();
        let is_insert = matches!(ev, LogicalEvent::Insert { .. });
        replica.apply(ev);
        if is_insert {
            break;
        }
    }

    let nodes = catch.borrow().fetch();
    assert_eq!(nodes.len(), 1);
    // The new column `b` flows through even though the replica was created
    // before the ALTER.
    assert_eq!(nodes[0].row.get("b"), Some(&Value::String("world".into())));
    assert_eq!(nodes[0].row.get("a"), Some(&Value::String("hello".into())));

    client
        .batch_execute("SELECT pg_drop_replication_slot('orbit_slot_ddladd') FROM pg_replication_slots WHERE slot_name='orbit_slot_ddladd'")
        .await
        .ok();
}

#[tokio::test]
async fn drop_column_mid_stream() {
    let host = std::env::var("ORBIT_PG_HOST").unwrap_or_else(|_| "127.0.0.1".into());
    let port: u16 = std::env::var("ORBIT_PG_PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(5433);
    let conn_str = format!("host={host} port={port} user=orbit dbname=orbit");
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });

    client
        .batch_execute(
            "DROP TABLE IF EXISTS gadget;
             CREATE TABLE gadget (id text PRIMARY KEY, a text, b text);
             ALTER TABLE gadget REPLICA IDENTITY FULL;",
        )
        .await
        .unwrap();
    create_publication(&client, "orbit_pub_drop", &["gadget"]).await.unwrap();
    let start_lsn = create_slot(&client, "orbit_slot_drop").await.unwrap();
    let mut stream = orbit_cache::ReplicationStream::start(
        &host, port, "orbit", "orbit", "orbit_slot_drop", "orbit_pub_drop", start_lsn,
    )
    .await
    .unwrap();

    let mut replica = Replica::new();
    let mut cols = BTreeMap::new();
    cols.insert("id".to_string(), ColumnType::String);
    cols.insert("a".to_string(), ColumnType::String);
    cols.insert("b".to_string(), ColumnType::String);
    let _ = replica.add_table("gadget", cols, vec!["id".into()]);

    let provider = P(&replica);
    let ast = Query::table("gadget").order_by("id", Direction::Asc).build();
    let top = build_pipeline(&ast, &provider);
    let catch = Catch::new(top.input.clone());
    let link: Link = catch.clone();
    top.set_output(link);

    // Insert a row with b, then drop column b and insert another.
    client
        .batch_execute(
            "INSERT INTO gadget (id, a, b) VALUES ('g1', 'x', 'y');
             ALTER TABLE gadget DROP COLUMN b;
             INSERT INTO gadget (id, a) VALUES ('g2', 'z');",
        )
        .await
        .unwrap();

    // Pump until both inserts have been applied (and the drop reconciled).
    let mut inserts = 0;
    while inserts < 2 {
        let (_lsn, ev) = tokio::time::timeout(Duration::from_secs(10), stream.next_event())
            .await
            .unwrap()
            .unwrap();
        if matches!(ev, LogicalEvent::Insert { .. }) {
            inserts += 1;
        }
        replica.apply(ev);
    }

    let nodes = catch.borrow().fetch();
    assert_eq!(nodes.len(), 2);
    // After DROP COLUMN b, neither row carries `b` (g1 reconciled, g2 never had it).
    for n in &nodes {
        assert!(n.row.get("b").is_none(), "column b dropped from {:?}", n.row.get("id"));
        assert!(n.row.get("a").is_some());
    }

    client
        .batch_execute("SELECT pg_drop_replication_slot('orbit_slot_drop') FROM pg_replication_slots WHERE slot_name='orbit_slot_drop'")
        .await
        .ok();
}
