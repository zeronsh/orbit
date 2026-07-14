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
    src.borrow().insert_initial(&row(&[("id", "t1".into()), ("done", false.into())])).expect("insert");
    src.borrow().insert_initial(&row(&[("id", "t2".into()), ("done", true.into())])).expect("insert");

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
    source_push(&src, oql::ivm::SourceChange::Add(row(&[("id", "t3".into()), ("done", false.into())]))).unwrap();
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
    ).unwrap();
    let _ = catch.borrow_mut().take_changes();
    let ids: Vec<Value> = catch.borrow().fetch().iter().map(|n| n.row["id"].clone()).collect();
    assert_eq!(ids, vec!["t3".into()]);
}

#[test]
fn dropping_sqlite_pipeline_disconnects_source() {
    let src = SqliteSource::new("task", cols(), vec!["id".into()]);
    let mut provider = SqliteProvider::new();
    provider.add(src.clone());
    let ast = Query::table("task").order_by("id", Direction::Asc).build();

    let catch = {
        let top = build_pipeline(&ast, &provider);
        let catch = Catch::new(top.input.clone());
        let link: Link = catch.clone();
        top.set_output(link);
        catch
    };

    assert_eq!(src.borrow().connection_count(), 1);
    drop(catch);
    assert_eq!(src.borrow().connection_count(), 0);
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
    replica.apply(LogicalEvent::Insert { table: "task".into(), row: row(&[("id", "t1".into()), ("done", false.into())]) }).unwrap();
    replica.apply(LogicalEvent::Insert { table: "task".into(), row: row(&[("id", "t2".into()), ("done", true.into())]) }).unwrap();
    assert_eq!(ids(&catch), vec!["t1"]);

    // Idempotent: re-applying the same insert (snapshot overlap) must not dup.
    replica.apply(LogicalEvent::Insert { table: "task".into(), row: row(&[("id", "t1".into()), ("done", false.into())]) }).unwrap();
    assert_eq!(ids(&catch), vec!["t1"]);

    // Update t1 -> done -> drops out of the filter.
    replica.apply(LogicalEvent::Update {
        table: "task".into(),
        row: row(&[("id", "t1".into()), ("done", true.into())]),
        old_row: Some(row(&[("id", "t1".into()), ("done", false.into())])),
    }).unwrap();
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
        source_push(&src, oql::ivm::SourceChange::Add(row(&[("id", "p1".into()), ("done", false.into())]))).unwrap();
        source_push(&src, oql::ivm::SourceChange::Add(row(&[("id", "p2".into()), ("done", true.into())]))).unwrap();
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

// --- Durability: transactions, watermark, fresh-sync phantom clearing --------

fn item_cols() -> Vec<(String, ColumnType)> {
    vec![("id".to_string(), ColumnType::String), ("n".to_string(), ColumnType::Number)]
}

/// A multi-table upstream transaction commits atomically; an uncommitted one
/// (crash before Commit) rolls back on reopen — no torn half-transaction.
#[test]
fn durable_transactions_are_atomic_across_tables_and_crashes() {
    let dir = std::env::temp_dir().join(format!("orbit_sqlite_txn_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);

    {
        let mut replica = SqliteReplica::durable(&dir);
        replica.add_table("a", item_cols(), vec!["id".into()]);
        replica.add_table("b", item_cols(), vec!["id".into()]);

        // Txn 1: rows in BOTH tables, committed with watermark 10.
        replica.begin_txn().unwrap();
        replica.apply(LogicalEvent::Insert { table: "a".into(), row: row(&[("id", "a1".into()), ("n", 1.0.into())]) }).unwrap();
        replica.apply(LogicalEvent::Insert { table: "b".into(), row: row(&[("id", "b1".into()), ("n", 1.0.into())]) }).unwrap();
        replica.commit_txn(10, 0).unwrap();

        // Txn 2: applied but NEVER committed — the "crash" is dropping the
        // replica (and its connection) with the transaction open.
        replica.begin_txn().unwrap();
        replica.apply(LogicalEvent::Insert { table: "a".into(), row: row(&[("id", "a2".into()), ("n", 2.0.into())]) }).unwrap();
        replica.apply(LogicalEvent::Insert { table: "b".into(), row: row(&[("id", "b2".into()), ("n", 2.0.into())]) }).unwrap();
    }

    // Reopen: txn 1 present in both tables, txn 2 fully rolled back, watermark = 10.
    let mut replica = SqliteReplica::durable(&dir);
    let a = replica.add_table("a", item_cols(), vec!["id".into()]);
    let b = replica.add_table("b", item_cols(), vec!["id".into()]);
    assert_eq!(replica.resume_watermark(), Some(10), "watermark from the committed txn");
    let a_rows = a.borrow().all_rows();
    let b_rows = b.borrow().all_rows();
    assert_eq!(a_rows.len(), 1, "a: only the committed row survives: {a_rows:?}");
    assert_eq!(b_rows.len(), 1, "b: only the committed row survives: {b_rows:?}");
    assert_eq!(a_rows[0]["id"], "a1".into());
    assert_eq!(b_rows[0]["id"], "b1".into());

    let _ = std::fs::remove_dir_all(&dir);
}

/// `start_fresh` drops rows a previous run persisted (they may have been deleted
/// upstream while this replica was offline — initial sync only upserts, so
/// without the clear they'd survive as phantoms) and resets the watermark.
#[test]
fn start_fresh_clears_stale_rows_and_watermark() {
    let dir = std::env::temp_dir().join(format!("orbit_sqlite_fresh_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);

    {
        let mut replica = SqliteReplica::durable(&dir);
        replica.add_table("t", item_cols(), vec!["id".into()]);
        replica.begin_txn().unwrap();
        replica.apply(LogicalEvent::Insert { table: "t".into(), row: row(&[("id", "stale".into()), ("n", 1.0.into())]) }).unwrap();
        replica.commit_txn(7, 0).unwrap();
    }

    // "Restart" that decides on a fresh sync (e.g. watermark policy change):
    let mut replica = SqliteReplica::durable(&dir);
    let t = replica.add_table("t", item_cols(), vec!["id".into()]);
    assert_eq!(replica.resume_watermark(), Some(7));
    replica.start_fresh();
    assert_eq!(replica.resume_watermark(), None, "watermark cleared");
    assert!(t.borrow().all_rows().is_empty(), "stale rows cleared (no phantoms)");

    // Re-seed as initial sync would, then verify only the new state exists.
    replica.begin_txn().unwrap();
    replica.seed("t", row(&[("id", "current".into()), ("n", 2.0.into())])).expect("seed");
    replica.commit_txn(0, 0).unwrap();
    let rows = t.borrow().all_rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["id"], "current".into());
    assert_eq!(replica.resume_watermark(), None, "lsn 0 = still no replayable watermark");

    let _ = std::fs::remove_dir_all(&dir);
}

// --- Cluster resume: pos watermark, snapshot backups, schema migration -------

/// `commit_txn(lsn, pos)` records both watermarks atomically; `resume_pos`
/// reads the change-stream position back; `start_fresh` clears it.
#[test]
fn pos_watermark_roundtrip_and_start_fresh() {
    let dir = std::env::temp_dir().join(format!("orbit_sqlite_pos_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);

    {
        let mut replica = SqliteReplica::durable(&dir);
        replica.add_table("t", item_cols(), vec!["id".into()]);
        replica.begin_txn().unwrap();
        replica.apply(LogicalEvent::Insert { table: "t".into(), row: row(&[("id", "x".into()), ("n", 1.0.into())]) }).unwrap();
        replica.commit_txn(42, 777).unwrap();
    }

    let mut replica = SqliteReplica::durable(&dir);
    replica.add_table("t", item_cols(), vec!["id".into()]);
    assert_eq!(replica.resume_watermark(), Some(42));
    assert_eq!(replica.resume_pos(), Some(777));

    replica.start_fresh();
    assert_eq!(replica.resume_pos(), None, "start_fresh clears pos too");

    let _ = std::fs::remove_dir_all(&dir);
}

/// `backup_to` (VACUUM INTO) takes a consistent copy while the source
/// connection stays open: single file, passes quick_check, carries rows and
/// the (lsn, pos) watermark.
#[tokio::test]
async fn backup_to_copies_rows_and_watermark_while_open() {
    let dir = std::env::temp_dir().join(format!("orbit_sqlite_backup_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let dest = std::env::temp_dir().join(format!("orbit_sqlite_backup_out_{}.db", std::process::id()));
    let _ = std::fs::remove_file(&dest);

    let mut replica = SqliteReplica::durable(&dir);
    replica.add_table("t", item_cols(), vec!["id".into()]);
    replica.begin_txn().unwrap();
    for i in 0..50 {
        replica.apply(LogicalEvent::Insert {
            table: "t".into(),
            row: row(&[("id", format!("r{i}").as_str().into()), ("n", (i as f64).into())]),
        }).unwrap();
    }
    replica.commit_txn(9, 123).unwrap();

    // Source connection still open (replica alive) during the backup.
    SqliteReplica::backup_to(replica.db_path().unwrap().to_owned(), dest.clone()).await.unwrap();

    let copy = rusqlite::Connection::open(&dest).unwrap();
    let ok: String = copy.query_row("PRAGMA quick_check", [], |r| r.get(0)).unwrap();
    assert_eq!(ok, "ok");
    let count: i64 = copy.query_row("SELECT count(*) FROM t", [], |r| r.get(0)).unwrap();
    assert_eq!(count, 50);
    let (lsn, pos): (i64, i64) = copy
        .query_row("SELECT lsn, pos FROM orbit_replication_state WHERE id = 1", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .unwrap();
    assert_eq!((lsn, pos), (9, 123), "watermark travels with the snapshot");
    assert!(!dest.with_extension("db-wal").exists(), "single-file output");

    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_file(&dest);
}

/// A replica file created before the `pos` column existed migrates in place.
#[test]
fn pre_pos_schema_migrates_in_place() {
    let dir = std::env::temp_dir().join(format!("orbit_sqlite_migrate_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    // Old-format file: replication state without `pos`.
    {
        let conn = rusqlite::Connection::open(dir.join("replica.db")).unwrap();
        conn.execute_batch(
            "CREATE TABLE orbit_replication_state (
                 id  INTEGER PRIMARY KEY CHECK (id = 1),
                 lsn INTEGER NOT NULL
             );
             INSERT INTO orbit_replication_state (id, lsn) VALUES (1, 55);",
        )
        .unwrap();
    }

    let mut replica = SqliteReplica::durable(&dir);
    replica.add_table("t", item_cols(), vec!["id".into()]);
    assert_eq!(replica.resume_watermark(), Some(55), "old lsn survives migration");
    assert_eq!(replica.resume_pos(), None, "migrated pos defaults to 0 (= None)");
    replica.begin_txn().unwrap();
    replica.commit_txn(56, 900).unwrap();
    assert_eq!(replica.resume_pos(), Some(900));

    let _ = std::fs::remove_dir_all(&dir);
}

/// A `Relation` event (upstream DDL) reconciles the physical SQLite table:
/// dropped columns disappear from stored rows — even when a fetch has lazily
/// created a secondary index over the doomed column (SQLite refuses to drop an
/// indexed column, so the reconcile must drop indexes first). Added columns
/// appear in subsequently-replicated rows. Mirrors the in-memory `Replica`.
#[test]
fn relation_event_reconciles_sqlite_table() {
    let mut replica = SqliteReplica::in_memory();
    let src = replica.add_table(
        "gadget",
        vec![
            ("id".to_string(), ColumnType::String),
            ("a".to_string(), ColumnType::String),
            ("b".to_string(), ColumnType::String),
        ],
        vec!["id".into()],
    );

    replica.apply(LogicalEvent::Insert {
        table: "gadget".into(),
        row: row(&[("id", "g1".into()), ("a", "x".into()), ("b", "y".into())]),
    }).unwrap();

    // A filtered query over `b` — its connect lazily creates an index on `b`,
    // which would block DROP COLUMN without the reconcile's index sweep.
    {
        let mut provider = SqliteProvider::new();
        provider.add(src.clone());
        let ast = Query::table("gadget")
            .where_("b", SimpleOperator::Eq, "y")
            .order_by("id", Direction::Asc)
            .build();
        let top = build_pipeline(&ast, &provider);
        let catch = Catch::new(top.input.clone());
        let link: Link = catch.clone();
        top.set_output(link);
        assert_eq!(catch.borrow().fetch().len(), 1);
    }

    // Upstream DDL: DROP COLUMN b, ADD COLUMN c.
    replica.apply(LogicalEvent::Relation {
        table: "gadget".into(),
        columns: vec![
            ("id".to_string(), ColumnType::String),
            ("a".to_string(), ColumnType::String),
            ("c".to_string(), ColumnType::String),
        ],
    }).unwrap();

    // A row using the new shape replicates cleanly.
    replica.apply(LogicalEvent::Insert {
        table: "gadget".into(),
        row: row(&[("id", "g2".into()), ("a", "z".into()), ("c", "new".into())]),
    }).unwrap();

    let rows = src.borrow().all_rows();
    assert_eq!(rows.len(), 2);
    for r in &rows {
        assert!(r.get("b").is_none(), "column b dropped from {:?}", r.get("id"));
        assert!(r.get("a").is_some());
    }
    let g2 = rows.iter().find(|r| r["id"] == "g2".into()).unwrap();
    assert_eq!(g2.get("c"), Some(&Value::String("new".into())));
}
