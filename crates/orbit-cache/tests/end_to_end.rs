//! Capstone: the full Orbit server data path against **real Postgres**.
//!
//!   Postgres INSERT/UPDATE  --logical replication-->  replica
//!     --> build_pipeline(`task WHERE done = false`)  --IVM-->  change stream
//!     --> view-sync  -->  wire `RowPatchOp`s (what a poke would carry)
//!
//! Requires the `orbit-pg` container on 127.0.0.1:5433 (see STATUS.md).

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
use orbit_cache::{changes_to_patches, Replica};
use orbit_protocol::RowPatchOp;

use tokio_postgres::NoTls;

struct ReplicaProvider<'a>(&'a Replica);
impl<'a> SourceProvider for ReplicaProvider<'a> {
    fn get_source(&self, table: &str) -> Option<Rc<RefCell<oql::ivm::MemorySource>>> {
        self.0.source(table)
    }
}

#[tokio::test]
async fn full_server_path_real_postgres() {
    let host = std::env::var("ORBIT_PG_HOST").unwrap_or_else(|_| "127.0.0.1".into());
    let port: u16 = std::env::var("ORBIT_PG_PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(5433);
    let conn_str = format!("host={host} port={port} user=orbit dbname=orbit");

    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await.expect("connect orbit-pg");
    tokio::spawn(async move {
        let _ = connection.await;
    });

    client
        .batch_execute(
            "DROP TABLE IF EXISTS task;
             CREATE TABLE task (id text PRIMARY KEY, title text, done boolean);
             ALTER TABLE task REPLICA IDENTITY FULL;",
        )
        .await
        .unwrap();
    create_publication(&client, "orbit_pub_e2e", &["task"]).await.unwrap();
    let start_lsn = create_slot(&client, "orbit_slot_e2e").await.unwrap();
    let mut stream = orbit_cache::ReplicationStream::start(
        &host, port, "orbit", "orbit", "orbit_slot_e2e", "orbit_pub_e2e", start_lsn,
    )
    .await
    .unwrap();

    // Replica + the materialized query `task WHERE done = false`.
    let mut replica = Replica::new();
    let mut cols = BTreeMap::new();
    cols.insert("id".to_string(), oql::ivm::ColumnType::String);
    cols.insert("title".to_string(), oql::ivm::ColumnType::String);
    cols.insert("done".to_string(), oql::ivm::ColumnType::Boolean);
    let _ = replica.add_table("task", cols, vec!["id".into()]);

    let provider = ReplicaProvider(&replica);
    let ast = Query::table("task")
        .where_("done", oql::ast::SimpleOperator::Eq, false)
        .order_by("id", Direction::Asc)
        .build();
    let top = build_pipeline(&ast, &provider);
    let catch = Catch::new(top.input.clone());
    let catch_link: Link = catch.clone();
    top.set_output(catch_link);
    let schema = catch.borrow().get_schema();

    // Mutate Postgres: t1/t3 are open (done=false), t2 starts done then reopens.
    client
        .batch_execute(
            "INSERT INTO task VALUES ('t1','a',false);
             INSERT INTO task VALUES ('t2','b',true);
             INSERT INTO task VALUES ('t3','c',false);
             UPDATE task SET done = false WHERE id = 't2';",
        )
        .await
        .unwrap();

    // Consume 3 inserts + 1 update.
    let mut data = 0;
    while data < 4 {
        let (_lsn, ev) = tokio::time::timeout(Duration::from_secs(10), stream.next_event())
            .await
            .expect("timeout")
            .expect("repl error");
        if matches!(
            ev,
            LogicalEvent::Insert { .. } | LogicalEvent::Update { .. } | LogicalEvent::Delete { .. }
        ) {
            data += 1;
        }
        replica.apply(ev);
    }

    // The accumulated change stream → wire row patches.
    let changes = catch.borrow_mut().take_changes();
    let patches = changes_to_patches(&changes, &schema);

    let put_ids: Vec<String> = patches
        .iter()
        .filter_map(|p| match p {
            RowPatchOp::Put { table_name, value } if table_name == "task" => match value.get("id") {
                Some(Value::String(s)) => Some(s.clone()),
                _ => None,
            },
            _ => None,
        })
        .collect();

    // t1 and t3 were inserted open; t2 became open via the update (filter
    // add-split). t2's `done=true` insert was filtered out (no patch).
    assert!(put_ids.contains(&"t1".to_string()));
    assert!(put_ids.contains(&"t3".to_string()));
    assert!(put_ids.contains(&"t2".to_string()));

    // Final materialized view: exactly the three open tasks.
    let mut view_ids: Vec<String> = catch
        .borrow()
        .fetch()
        .iter()
        .map(|n| match n.row.get("id") {
            Some(Value::String(s)) => s.clone(),
            _ => unreachable!(),
        })
        .collect();
    view_ids.sort();
    assert_eq!(view_ids, vec!["t1", "t2", "t3"]);

    client
        .batch_execute(
            "SELECT pg_drop_replication_slot('orbit_slot_e2e') FROM pg_replication_slots WHERE slot_name='orbit_slot_e2e'",
        )
        .await
        .ok();
}
