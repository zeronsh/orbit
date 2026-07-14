//! Tier 0.5 regression: a durable replica that resumes from a watermark must
//! BACKFILL tables newly added to the config — previously it skipped initial
//! sync entirely and the new table silently served empty history.

use oql::ivm::ColumnType;
use oql::value::Value;
use orbit_cache::replica::ReplicaBackend;
use orbit_cache::run::{backfill_missing_tables, ServerConfig, TableConfig};
use orbit_cache::sqlite_source::SqliteReplica;
use tokio_postgres::NoTls;

fn host() -> String {
    std::env::var("ORBIT_PG_HOST").unwrap_or_else(|_| "127.0.0.1".to_string())
}
fn port() -> u16 {
    std::env::var("ORBIT_PG_PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(5433)
}

fn cfg(tables: Vec<TableConfig>) -> ServerConfig {
    ServerConfig {
        host: host(),
        port: port(),
        user: "orbit".into(),
        database: "orbit".into(),
        password: None,
        tls: orbit_cache::pg::PgTlsMode::Disable,
        tables,
        publication: "orbit_pub_bf".into(),
        slot: "orbit_slot_bf".into(),
        listen_addr: "127.0.0.1:0".into(),
        mutate_url: None,
        query_url: None,
        api_key: None,
        forward_cookies: false,
    }
}

fn tbl(name: &str) -> TableConfig {
    TableConfig {
        name: name.into(),
        columns: vec![("id".into(), ColumnType::String), ("n".into(), ColumnType::Number)],
        primary_key: vec!["id".into()],
    }
}

#[tokio::test]
async fn added_table_is_backfilled_on_watermark_resume() {
    let conn_str = format!("host={} port={} user=orbit dbname=orbit", host(), port());
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(
            "DROP TABLE IF EXISTS bf_a; DROP TABLE IF EXISTS bf_b;
             CREATE TABLE bf_a (id text PRIMARY KEY, n int);
             CREATE TABLE bf_b (id text PRIMARY KEY, n int);
             INSERT INTO bf_a VALUES ('a1', 1);
             INSERT INTO bf_b VALUES ('b1', 10), ('b2', 20);",
        )
        .await
        .unwrap();

    let dir = std::env::temp_dir().join(format!("orbit_bf_test_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);

    // --- First boot: only bf_a configured; sync it and record a watermark.
    {
        let mut replica = SqliteReplica::durable(&dir);
        replica.add_table("bf_a", tbl("bf_a").columns, vec!["id".into()]);
        replica.begin_txn().unwrap();
        orbit_cache::pg::initial_sync_backend(&client, &replica, "bf_a").await.unwrap();
        replica.mark_synced("bf_a").unwrap();
        replica.commit_txn(1234, 77).unwrap();
    }

    // --- Second boot: bf_b added to the config. The replica resumes from its
    // watermark; backfill must seed bf_b's pre-existing rows and PRESERVE the
    // watermark.
    let mut replica = SqliteReplica::durable(&dir);
    replica.add_table("bf_a", tbl("bf_a").columns, vec!["id".into()]);
    replica.add_table("bf_b", tbl("bf_b").columns, vec!["id".into()]);
    assert_eq!(replica.resume_watermark(), Some(1234));
    let synced = replica.synced_tables().unwrap();
    assert!(synced.contains("bf_a"));
    assert!(!synced.contains("bf_b"), "bf_b must be detected as unsynced");

    backfill_missing_tables(&client, &replica, &cfg(vec![tbl("bf_a"), tbl("bf_b")]))
        .await
        .unwrap();

    let rows = replica.source("bf_b").unwrap().borrow().all_rows();
    assert_eq!(rows.len(), 2, "pre-existing rows of the added table must be backfilled");
    assert_eq!(rows[0].get("id"), Some(&Value::String("b1".into())));
    assert_eq!(rows[1].get("n"), Some(&Value::Number(20.0)));

    // Watermark/pos unchanged: backfill doesn't advance replication.
    assert_eq!(replica.resume_watermark(), Some(1234));
    assert_eq!(replica.resume_pos(), Some(77));
    // And it's now registered — a third boot won't re-backfill.
    assert!(replica.synced_tables().unwrap().contains("bf_b"));

    // Idempotent: running again is a no-op (no duplicate work, no errors).
    backfill_missing_tables(&client, &replica, &cfg(vec![tbl("bf_a"), tbl("bf_b")]))
        .await
        .unwrap();
    assert_eq!(replica.source("bf_b").unwrap().borrow().all_rows().len(), 2);

    let _ = std::fs::remove_dir_all(&dir);
}

/// Pre-registry replica files (older versions) must count their existing
/// physical tables as synced — the upgrade must NOT trigger a full re-seed,
/// and must still detect genuinely-new tables.
#[tokio::test]
async fn pre_registry_files_migrate_without_reseeding() {
    let dir = std::env::temp_dir().join(format!("orbit_bf_migrate_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);

    // Simulate an old-version file: table + watermark, NO registry.
    {
        std::fs::create_dir_all(&dir).unwrap();
        let conn = rusqlite::Connection::open(dir.join("replica.db")).unwrap();
        conn.execute_batch(
            "CREATE TABLE old_t (id TEXT PRIMARY KEY, n INTEGER);
             INSERT INTO old_t VALUES ('x', 1);
             CREATE TABLE orbit_replication_state (
                 id INTEGER PRIMARY KEY CHECK (id = 1),
                 lsn INTEGER NOT NULL,
                 pos INTEGER NOT NULL DEFAULT 0
             );
             INSERT INTO orbit_replication_state VALUES (1, 999, 5);",
        )
        .unwrap();
    }

    let mut replica = SqliteReplica::durable(&dir);
    replica.add_table(
        "old_t",
        vec![("id".into(), ColumnType::String), ("n".into(), ColumnType::Number)],
        vec!["id".into()],
    );
    replica.add_table(
        "new_t",
        vec![("id".into(), ColumnType::String)],
        vec!["id".into()],
    );
    let synced = replica.synced_tables().unwrap();
    assert!(synced.contains("old_t"), "pre-existing table counts as synced after upgrade");
    assert!(!synced.contains("new_t"), "genuinely-new table is still detected");

    let _ = std::fs::remove_dir_all(&dir);
}
