//! The IVM engine running over a **SQLite-backed** replica source (port of
//! Zero's `zqlite` TableSource): queries materialize and update incrementally,
//! and rows persist to SQLite (durable across "restart").

use std::collections::BTreeMap;

use oql::ast::{Direction, SimpleOperator};
use oql::ivm::operator::Link;
use oql::ivm::{Catch, ColumnType};
use oql::value::Value;
use oql::{build_pipeline, Query};
use orbit_cache::sqlite_source::source_push;
use orbit_cache::{ReplicaBackend, SqliteProvider, SqliteReplica, SqliteSource};
use orbit_cache::LogicalEvent;

fn row(pairs: &[(&str, Value)]) -> oql::value::Row {
    pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
}
fn cols() -> BTreeMap<String, ColumnType> {
    let mut c = BTreeMap::new();
    c.insert("id".to_string(), ColumnType::String);
    c.insert("done".to_string(), ColumnType::Boolean);
    c
}

#[test]
fn ivm_over_sqlite_source() {
    let src = SqliteSource::new("task", cols(), vec!["id".into()]);
    src.borrow().insert_initial(&row(&[("id", "t1".into()), ("done", false.into())]));
    src.borrow().insert_initial(&row(&[("id", "t2".into()), ("done", true.into())]));

    let mut provider = SqliteProvider::new();
    provider.add(src.clone());

    // Query: task WHERE done = false.
    let ast = Query::table("task")
        .where_("done", SimpleOperator::Eq, false)
        .order_by("id", Direction::Asc)
        .build();
    let top = build_pipeline(&ast, &provider);
    let catch = Catch::new(top.input.clone());
    let link: Link = catch.clone();
    top.set_output(link);

    // Initial: only t1 (open).
    let ids: Vec<Value> = catch.borrow().fetch().iter().map(|n| n.row["id"].clone()).collect();
    assert_eq!(ids, vec!["t1".into()]);

    // Live insert of an open task -> incremental Add + persisted.
    source_push(&src, oql::ivm::SourceChange::Add(row(&[("id", "t3".into()), ("done", false.into())])));
    let changes = catch.borrow_mut().take_changes();
    assert!(changes.iter().any(|c| matches!(c, oql::ivm::Change::Add(n) if n.row["id"] == "t3".into())));
    let ids: Vec<Value> = catch.borrow().fetch().iter().map(|n| n.row["id"].clone()).collect();
    assert_eq!(ids, vec!["t1".into(), "t3".into()]);

    // Mark t1 done -> filter edit-split drops it.
    source_push(
        &src,
        oql::ivm::SourceChange::Edit {
            row: row(&[("id", "t1".into()), ("done", true.into())]),
            old_row: row(&[("id", "t1".into()), ("done", false.into())]),
        },
    );
    let _ = catch.borrow_mut().take_changes();
    let ids: Vec<Value> = catch.borrow().fetch().iter().map(|n| n.row["id"].clone()).collect();
    assert_eq!(ids, vec!["t3".into()]);
}

#[test]
fn sqlite_replica_backend_applies_events() {
    // The SqliteReplica backend: apply replication events + serve queries over it.
    let mut replica = SqliteReplica::in_memory();
    replica.add_table(
        "task",
        vec![("id".into(), ColumnType::String), ("done".into(), ColumnType::Boolean)],
        vec!["id".into()],
    );

    let ast = Query::table("task")
        .where_("done", SimpleOperator::Eq, false)
        .order_by("id", Direction::Asc)
        .build();
    let top = build_pipeline(&ast, &replica);
    let catch = Catch::new(top.input.clone());
    let link: Link = catch.clone();
    top.set_output(link);

    let ids = |c: &std::rc::Rc<std::cell::RefCell<Catch>>| -> Vec<String> {
        c.borrow().fetch().iter().map(|n| match n.row.get("id") {
            Some(Value::String(s)) => s.clone(),
            _ => unreachable!(),
        }).collect()
    };

    // Insert two tasks (one done) via replication events.
    replica.apply(LogicalEvent::Insert { table: "task".into(), row: row(&[("id", "t1".into()), ("done", false.into())]) });
    replica.apply(LogicalEvent::Insert { table: "task".into(), row: row(&[("id", "t2".into()), ("done", true.into())]) });
    assert_eq!(ids(&catch), vec!["t1"]);

    // Idempotent: re-applying the same insert (snapshot overlap) must not dup.
    replica.apply(LogicalEvent::Insert { table: "task".into(), row: row(&[("id", "t1".into()), ("done", false.into())]) });
    assert_eq!(ids(&catch), vec!["t1"]);

    // Update t1 -> done -> drops out of the filter.
    replica.apply(LogicalEvent::Update {
        table: "task".into(),
        row: row(&[("id", "t1".into()), ("done", true.into())]),
        old_row: Some(row(&[("id", "t1".into()), ("done", false.into())])),
    });
    assert!(ids(&catch).is_empty());
}

#[test]
fn sqlite_replica_persists_across_reopen() {
    let dir = std::env::temp_dir();
    let path = dir.join(format!("orbit_replica_test_{}.db", std::process::id()));
    let _ = std::fs::remove_file(&path);

    // First "process": write rows.
    {
        let conn = rusqlite::Connection::open(&path).unwrap();
        let src = SqliteSource::with_connection(conn, "task", cols(), vec!["id".into()]);
        source_push(&src, oql::ivm::SourceChange::Add(row(&[("id", "p1".into()), ("done", false.into())])));
        source_push(&src, oql::ivm::SourceChange::Add(row(&[("id", "p2".into()), ("done", true.into())])));
    }

    // Second "process": reopen the same file; the rows are still there.
    {
        let conn = rusqlite::Connection::open(&path).unwrap();
        let src = SqliteSource::with_connection(conn, "task", cols(), vec!["id".into()]);
        let mut provider = SqliteProvider::new();
        provider.add(src.clone());
        let ast = Query::table("task").order_by("id", Direction::Asc).build();
        let top = build_pipeline(&ast, &provider);
        let catch = Catch::new(top.input.clone());
        let link: Link = catch.clone();
        top.set_output(link);
        let ids: Vec<Value> = catch.borrow().fetch().iter().map(|n| n.row["id"].clone()).collect();
        assert_eq!(ids, vec!["p1".into(), "p2".into()], "rows persisted across reopen");
    }

    let _ = std::fs::remove_file(&path);
}
