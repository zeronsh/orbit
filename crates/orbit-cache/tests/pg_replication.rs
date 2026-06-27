//! End-to-end test against a **real** PostgreSQL database:
//!
//!   real Postgres  --(logical replication / pgoutput)-->  Orbit decoder
//!   --> OQL SourceChange --> IVM pipeline (filter age>=18) --> materialized view
//!
//! Requires the `orbit-pg` container (Postgres 16, wal_level=logical, trust
//! auth) reachable at 127.0.0.1:5433. See the repo setup. Override with
//! ORBIT_PG_HOST / ORBIT_PG_PORT.

use std::collections::BTreeMap;
use std::rc::Rc;
use std::time::Duration;

use oql::ivm::operator::{Link, OpHandle};
use oql::ivm::{connect, Catch, ColumnType, Filter, Predicate};
use oql::value::Value;

use orbit_cache::pg::{create_publication, create_slot};
use orbit_cache::pg::pgoutput::LogicalEvent;
use orbit_cache::{Replica, ReplicationStream};

use tokio_postgres::NoTls;

fn host() -> String {
    std::env::var("ORBIT_PG_HOST").unwrap_or_else(|_| "127.0.0.1".to_string())
}
fn port() -> u16 {
    std::env::var("ORBIT_PG_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(5433)
}

#[tokio::test]
async fn real_postgres_replication_drives_oql_view() {
    let host = host();
    let port = port();
    let conn_str = format!("host={host} port={port} user=orbit dbname=orbit");

    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("connect to orbit-pg (is the container running on 5433?)");
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("pg connection error: {e}");
        }
    });

    // Fresh, empty table with full old-row capture for updates/deletes.
    client
        .batch_execute(
            "DROP TABLE IF EXISTS app_user;
             CREATE TABLE app_user (id text PRIMARY KEY, name text, age int);
             ALTER TABLE app_user REPLICA IDENTITY FULL;",
        )
        .await
        .unwrap();

    create_publication(&client, "orbit_pub_pgr", &["app_user"])
        .await
        .unwrap();
    let start_lsn = create_slot(&client, "orbit_slot_pgr").await.unwrap();

    // Begin streaming before making any changes.
    let mut stream = ReplicationStream::start(
        &host,
        port,
        "orbit",
        "orbit",
        "orbit_slot_pgr",
        "orbit_pub_pgr",
        start_lsn,
    )
    .await
    .expect("start replication");

    // Build the replica + a materialized OQL query: app_user WHERE age >= 18.
    let mut replica = Replica::new();
    let mut columns = BTreeMap::new();
    columns.insert("id".to_string(), ColumnType::String);
    columns.insert("name".to_string(), ColumnType::String);
    columns.insert("age".to_string(), ColumnType::Number);
    let src = replica.add_table("app_user", columns, vec!["id".into()]);

    let conn = OpHandle::new(connect(&src, vec![("id".to_string(), oql::ast::Direction::Asc)]));
    let adult: Predicate =
        Rc::new(|r| matches!(r.get("age"), Some(Value::Number(a)) if *a >= 18.0));
    let filter = Filter::new(conn, adult);
    let filter_h = OpHandle::new(filter);
    let catch = Catch::new(filter_h.input.clone());
    let catch_link: Link = catch.clone();
    filter_h.set_output(catch_link);

    // Mutate the real database.
    client
        .batch_execute(
            "INSERT INTO app_user VALUES ('u1','Alice',30);
             INSERT INTO app_user VALUES ('u2','Bob',15);
             INSERT INTO app_user VALUES ('u3','Carol',20);
             UPDATE app_user SET age = 18 WHERE id = 'u2';
             DELETE FROM app_user WHERE id = 'u3';",
        )
        .await
        .unwrap();

    // Consume 3 inserts + 1 update + 1 delete = 5 data events.
    let mut data_events = 0;
    while data_events < 5 {
        let (_lsn, ev) = tokio::time::timeout(Duration::from_secs(10), stream.next_event())
            .await
            .expect("timed out waiting for replication events")
            .expect("replication error");
        let is_data = matches!(
            ev,
            LogicalEvent::Insert { .. } | LogicalEvent::Update { .. } | LogicalEvent::Delete { .. }
        );
        replica.apply(ev);
        if is_data {
            data_events += 1;
        }
    }

    // The materialized view should now hold exactly the adults: Alice(30) and
    // the now-adult Bob(18); Carol was deleted.
    let view = catch.borrow().fetch();
    let mut got: Vec<(String, f64)> = view
        .iter()
        .map(|n| {
            let id = match n.row.get("id") {
                Some(Value::String(s)) => s.clone(),
                _ => panic!("expected string id"),
            };
            let age = match n.row.get("age") {
                Some(Value::Number(a)) => *a,
                _ => panic!("expected number age"),
            };
            (id, age)
        })
        .collect();
    got.sort_by(|a, b| a.0.cmp(&b.0));

    assert_eq!(
        got,
        vec![("u1".to_string(), 30.0), ("u2".to_string(), 18.0)],
        "filtered view should contain only adults after live replication"
    );

    // Sanity-check the incremental change stream: Bob arrived via the update
    // (filter add-split), Carol left via the delete.
    let changes = catch.borrow_mut().take_changes();
    let added_bob = changes.iter().any(|c| matches!(c, oql::ivm::Change::Add(n) if n.row.get("id") == Some(&Value::String("u2".into()))));
    let removed_carol = changes.iter().any(|c| matches!(c, oql::ivm::Change::Remove(n) if n.row.get("id") == Some(&Value::String("u3".into()))));
    assert!(added_bob, "Bob should have been added when his age hit 18");
    assert!(removed_carol, "Carol should have been removed on delete");

    // Clean up the slot so re-runs start fresh.
    client
        .batch_execute(
            "SELECT pg_drop_replication_slot('orbit_slot_pgr') \
             FROM pg_replication_slots WHERE slot_name = 'orbit_slot_pgr'",
        )
        .await
        .ok();
}
