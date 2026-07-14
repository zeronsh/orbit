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

/// `ALTER COLUMN … TYPE` mid-stream: Postgres rewrites its table but logical
/// replication never re-sends rows — the replica must convert its own stored
/// values when the Relation message reveals the new type (audit Tier 0.4:
/// previously invisible, reconcile compared names only).
#[tokio::test]
async fn alter_column_type_mid_stream_converts_stored_values() {
    let host = std::env::var("ORBIT_PG_HOST").unwrap_or_else(|_| "127.0.0.1".into());
    let port: u16 = std::env::var("ORBIT_PG_PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(5433);
    let conn_str = format!("host={host} port={port} user=orbit dbname=orbit");
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });

    client
        .batch_execute(
            "DROP TABLE IF EXISTS retype;
             CREATE TABLE retype (id text PRIMARY KEY, n text);
             ALTER TABLE retype REPLICA IDENTITY FULL;",
        )
        .await
        .unwrap();
    client
        .batch_execute("SELECT pg_drop_replication_slot('orbit_slot_retype') FROM pg_replication_slots WHERE slot_name='orbit_slot_retype'")
        .await
        .ok();
    create_publication(&client, "orbit_pub_retype", &["retype"]).await.unwrap();
    let start_lsn = create_slot(&client, "orbit_slot_retype").await.unwrap();
    let mut stream = orbit_cache::ReplicationStream::start(
        &host, port, "orbit", "orbit", "orbit_slot_retype", "orbit_pub_retype", start_lsn,
    )
    .await
    .unwrap();

    let mut replica = Replica::new();
    let mut cols = BTreeMap::new();
    cols.insert("id".to_string(), ColumnType::String);
    cols.insert("n".to_string(), ColumnType::String);
    let _ = replica.add_table("retype", cols, vec!["id".into()]);

    // Row exists as text, THEN the column becomes an integer, then another row
    // arrives typed int8. The stored 'r1' value must convert to a number.
    client
        .batch_execute(
            "INSERT INTO retype (id, n) VALUES ('r1', '41');
             ALTER TABLE retype ALTER COLUMN n TYPE int8 USING n::int8;
             INSERT INTO retype (id, n) VALUES ('r2', 42);",
        )
        .await
        .unwrap();

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

    let src = replica.source("retype").unwrap();
    let rows = src.borrow().all_rows();
    assert_eq!(rows.len(), 2);
    for r in &rows {
        let id = match r.get("id") {
            Some(Value::String(s)) => s.clone(),
            other => panic!("bad id {other:?}"),
        };
        let want = if id == "r1" { 41.0 } else { 42.0 };
        assert_eq!(
            r.get("n"),
            Some(&Value::Number(want)),
            "row {id}: stored value must be converted to the new column type"
        );
    }

    client
        .batch_execute("SELECT pg_drop_replication_slot('orbit_slot_retype') FROM pg_replication_slots WHERE slot_name='orbit_slot_retype'")
        .await
        .ok();
}

/// `ALTER TABLE … RENAME TO` mid-stream: the relation OID keeps flowing under
/// a new name — the replica aliases it so clients subscribed under the
/// ORIGINAL name keep receiving changes (previously: silent data loss).
#[tokio::test]
async fn rename_table_mid_stream_keeps_replicating() {
    let host = std::env::var("ORBIT_PG_HOST").unwrap_or_else(|_| "127.0.0.1".into());
    let port: u16 = std::env::var("ORBIT_PG_PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(5433);
    let conn_str = format!("host={host} port={port} user=orbit dbname=orbit");
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });

    client
        .batch_execute(
            "DROP TABLE IF EXISTS widget; DROP TABLE IF EXISTS widget_renamed;
             CREATE TABLE widget (id text PRIMARY KEY, a text);
             ALTER TABLE widget REPLICA IDENTITY FULL;",
        )
        .await
        .unwrap();
    client
        .batch_execute("SELECT pg_drop_replication_slot('orbit_slot_ren') FROM pg_replication_slots WHERE slot_name='orbit_slot_ren'")
        .await
        .ok();
    create_publication(&client, "orbit_pub_ren", &["widget"]).await.unwrap();
    let start_lsn = create_slot(&client, "orbit_slot_ren").await.unwrap();
    let mut stream = orbit_cache::ReplicationStream::start(
        &host, port, "orbit", "orbit", "orbit_slot_ren", "orbit_pub_ren", start_lsn,
    )
    .await
    .unwrap();

    let mut replica = Replica::new();
    let mut cols = BTreeMap::new();
    cols.insert("id".to_string(), ColumnType::String);
    cols.insert("a".to_string(), ColumnType::String);
    let _ = replica.add_table("widget", cols, vec!["id".into()]);

    client
        .batch_execute(
            "INSERT INTO widget VALUES ('w1', 'before');
             ALTER TABLE widget RENAME TO widget_renamed;
             INSERT INTO widget_renamed VALUES ('w2', 'after');",
        )
        .await
        .unwrap();

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

    // Both rows landed in the source clients know as "widget".
    let rows = replica.source("widget").unwrap().borrow().all_rows();
    let ids: Vec<String> = rows
        .iter()
        .map(|r| match r.get("id") {
            Some(Value::String(s)) => s.clone(),
            other => panic!("bad id {other:?}"),
        })
        .collect();
    assert_eq!(ids, vec!["w1", "w2"], "post-rename inserts must keep flowing to the aliased source");

    client
        .batch_execute("SELECT pg_drop_replication_slot('orbit_slot_ren') FROM pg_replication_slots WHERE slot_name='orbit_slot_ren'")
        .await
        .ok();
    client.batch_execute("DROP TABLE IF EXISTS widget_renamed").await.ok();
}

/// `ALTER TABLE … RENAME COLUMN` mid-stream: values survive under the new
/// column name (previously reconciled as drop+add → values lost).
#[tokio::test]
async fn rename_column_mid_stream_preserves_values() {
    let host = std::env::var("ORBIT_PG_HOST").unwrap_or_else(|_| "127.0.0.1".into());
    let port: u16 = std::env::var("ORBIT_PG_PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(5433);
    let conn_str = format!("host={host} port={port} user=orbit dbname=orbit");
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });

    client
        .batch_execute(
            "DROP TABLE IF EXISTS relabel;
             CREATE TABLE relabel (id text PRIMARY KEY, old_name text);
             ALTER TABLE relabel REPLICA IDENTITY FULL;",
        )
        .await
        .unwrap();
    client
        .batch_execute("SELECT pg_drop_replication_slot('orbit_slot_relabel') FROM pg_replication_slots WHERE slot_name='orbit_slot_relabel'")
        .await
        .ok();
    create_publication(&client, "orbit_pub_relabel", &["relabel"]).await.unwrap();
    let start_lsn = create_slot(&client, "orbit_slot_relabel").await.unwrap();
    let mut stream = orbit_cache::ReplicationStream::start(
        &host, port, "orbit", "orbit", "orbit_slot_relabel", "orbit_pub_relabel", start_lsn,
    )
    .await
    .unwrap();

    let mut replica = Replica::new();
    let mut cols = BTreeMap::new();
    cols.insert("id".to_string(), ColumnType::String);
    cols.insert("old_name".to_string(), ColumnType::String);
    let _ = replica.add_table("relabel", cols, vec!["id".into()]);

    client
        .batch_execute(
            "INSERT INTO relabel VALUES ('r1', 'precious');
             ALTER TABLE relabel RENAME COLUMN old_name TO new_name;
             INSERT INTO relabel (id, new_name) VALUES ('r2', 'fresh');",
        )
        .await
        .unwrap();

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

    let rows = replica.source("relabel").unwrap().borrow().all_rows();
    assert_eq!(rows.len(), 2);
    let r1 = rows.iter().find(|r| r.get("id") == Some(&Value::String("r1".into()))).unwrap();
    assert_eq!(
        r1.get("new_name"),
        Some(&Value::String("precious".into())),
        "pre-rename value must survive under the new column name"
    );
    assert!(r1.get("old_name").is_none());

    client
        .batch_execute("SELECT pg_drop_replication_slot('orbit_slot_relabel') FROM pg_replication_slots WHERE slot_name='orbit_slot_relabel'")
        .await
        .ok();
}
