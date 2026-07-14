//! TOAST + TRUNCATE + type-fidelity regression tests against a **real**
//! PostgreSQL (the `orbit-pg` container on 127.0.0.1:5433).
//!
//! The canonical TOAST failure (audit Tier 0.1): `UPDATE t SET counter =
//! counter + 1` on a row with a megabyte `text` column. Postgres ships the
//! unchanged TOASTed column as `'u'` (not present); before the merge fix every
//! replica backend NULLed the column. Zero regression-tests this with 1MB
//! columns (`change-source.toasted-values.pg.test.ts`); these tests cover the
//! same shape for BOTH Orbit backends and BOTH replica identities.

use std::collections::BTreeMap;
use std::time::Duration;

use oql::ivm::ColumnType;
use oql::value::Value;

use orbit_cache::pg::pgoutput::LogicalEvent;
use orbit_cache::pg::{create_publication, create_slot};
use orbit_cache::replica::ReplicaBackend;
use orbit_cache::sqlite_source::SqliteReplica;
use orbit_cache::{Replica, ReplicationStream};

use tokio_postgres::NoTls;

fn host() -> String {
    std::env::var("ORBIT_PG_HOST").unwrap_or_else(|_| "127.0.0.1".to_string())
}
fn port() -> u16 {
    std::env::var("ORBIT_PG_PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(5433)
}

async fn connect() -> tokio_postgres::Client {
    let conn_str = format!("host={} port={} user=orbit dbname=orbit", host(), port());
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("connect to orbit-pg (is the container running on 5433?)");
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("pg connection error: {e}");
        }
    });
    client
}

async fn drop_slot(client: &tokio_postgres::Client, slot: &str) {
    client
        .batch_execute(&format!(
            "SELECT pg_drop_replication_slot('{slot}') \
             FROM pg_replication_slots WHERE slot_name = '{slot}'"
        ))
        .await
        .ok();
}

/// Pump events from `stream` into `backend` until `pred` holds against the
/// backend (checked at every commit), or panic after `timeout`.
async fn pump_until<B: ReplicaBackend>(
    stream: &mut ReplicationStream,
    backend: &B,
    timeout: Duration,
    mut pred: impl FnMut(&B) -> bool,
) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let remain = deadline.saturating_duration_since(tokio::time::Instant::now());
        let (_lsn, ev) = tokio::time::timeout(remain, stream.next_event())
            .await
            .expect("timed out waiting for replication events")
            .expect("replication error");
        let at_commit = matches!(ev, LogicalEvent::Commit);
        backend.apply(ev).expect("apply");
        if at_commit && pred(backend) {
            return;
        }
    }
}

fn lookup_in_memory(replica: &Replica, table: &str, id: &str) -> Option<oql::value::Row> {
    let mut key = oql::value::Row::new();
    key.insert("id", Value::String(id.into()));
    replica.source(table).unwrap().borrow().lookup(&key)
}

fn lookup_sqlite(replica: &SqliteReplica, table: &str, id: &str) -> Option<oql::value::Row> {
    let mut key = oql::value::Row::new();
    key.insert("id", Value::String(id.into()));
    replica.source(table).unwrap().borrow().lookup(&key)
}

/// The TOAST regression, parameterized over backend + replica identity.
async fn toast_update_preserves_unchanged_column(replica_identity: &str, sqlite: bool) {
    let tag = format!(
        "toast_{}_{}",
        if sqlite { "sq" } else { "mem" },
        replica_identity.to_lowercase()
    );
    let client = connect().await;
    let table = format!("toast_t_{tag}");
    client
        .batch_execute(&format!(
            "DROP TABLE IF EXISTS {table};
             CREATE TABLE {table} (id text PRIMARY KEY, big text, counter int8);
             ALTER TABLE {table} REPLICA IDENTITY {replica_identity};"
        ))
        .await
        .unwrap();

    let publication = format!("orbit_pub_{tag}");
    let slot = format!("orbit_slot_{tag}");
    drop_slot(&client, &slot).await;
    create_publication(&client, &publication, &[&table]).await.unwrap();
    let start_lsn = create_slot(&client, &slot).await.unwrap();

    let mut stream =
        ReplicationStream::start(&host(), port(), "orbit", "orbit", &slot, &publication, start_lsn)
            .await
            .expect("start replication");

    // Both backends share the ReplicaBackend trait; build the requested one.
    let columns: Vec<(String, ColumnType)> = vec![
        ("id".into(), ColumnType::String),
        ("big".into(), ColumnType::String),
        ("counter".into(), ColumnType::Number),
    ];

    // >1MB forces out-of-line TOAST storage even after compression (use
    // random-ish incompressible-ish content: repeated distinct chunks).
    let big: String = (0..40_000).map(|i| format!("{i:x}")).collect();
    assert!(big.len() > 150_000, "payload must be TOASTed");

    client
        .execute(
            &format!("INSERT INTO {table} VALUES ($1, $2, 1)"),
            &[&"r1", &big.as_str()],
        )
        .await
        .unwrap();
    // The canonical failing case: an UPDATE that never touches `big`.
    client
        .execute(&format!("UPDATE {table} SET counter = counter + 1 WHERE id = 'r1'"), &[])
        .await
        .unwrap();

    let check = |row: Option<oql::value::Row>| {
        let row = row.expect("row must exist");
        assert_eq!(
            row.get("counter"),
            Some(&Value::Number(2.0)),
            "update must have applied"
        );
        assert_eq!(
            row.get("big"),
            Some(&Value::String(big.clone())),
            "unchanged TOAST column must survive the update (was NULLed before the merge fix)"
        );
    };

    if sqlite {
        let mut replica = SqliteReplica::in_memory();
        replica.add_table(&table, columns, vec!["id".into()]);
        pump_until(&mut stream, &replica, Duration::from_secs(15), |r| {
            lookup_sqlite(r, &table, "r1")
                .is_some_and(|row| row.get("counter") == Some(&Value::Number(2.0)))
        })
        .await;
        check(lookup_sqlite(&replica, &table, "r1"));
    } else {
        let mut replica = Replica::new();
        let cols: BTreeMap<String, ColumnType> = columns.into_iter().collect();
        replica.add_table(&table, cols, vec!["id".into()]);
        pump_until(&mut stream, &replica, Duration::from_secs(15), |r| {
            lookup_in_memory(r, &table, "r1")
                .is_some_and(|row| row.get("counter") == Some(&Value::Number(2.0)))
        })
        .await;
        check(lookup_in_memory(&replica, &table, "r1"));
    }

    drop_slot(&client, &slot).await;
}

#[tokio::test]
async fn toast_in_memory_replica_identity_full() {
    toast_update_preserves_unchanged_column("FULL", false).await;
}

#[tokio::test]
async fn toast_in_memory_replica_identity_default() {
    toast_update_preserves_unchanged_column("DEFAULT", false).await;
}

#[tokio::test]
async fn toast_sqlite_replica_identity_full() {
    toast_update_preserves_unchanged_column("FULL", true).await;
}

#[tokio::test]
async fn toast_sqlite_replica_identity_default() {
    toast_update_preserves_unchanged_column("DEFAULT", true).await;
}

/// TOAST columns must also survive when the PK itself changes under
/// REPLICA IDENTITY FULL (the old row is keyed differently than the new).
#[tokio::test]
async fn toast_survives_pk_change() {
    let client = connect().await;
    client
        .batch_execute(
            "DROP TABLE IF EXISTS toast_pkc;
             CREATE TABLE toast_pkc (id text PRIMARY KEY, big text);
             ALTER TABLE toast_pkc REPLICA IDENTITY FULL;",
        )
        .await
        .unwrap();
    drop_slot(&client, "orbit_slot_toast_pkc").await;
    create_publication(&client, "orbit_pub_toast_pkc", &["toast_pkc"]).await.unwrap();
    let start_lsn = create_slot(&client, "orbit_slot_toast_pkc").await.unwrap();
    let mut stream = ReplicationStream::start(
        &host(), port(), "orbit", "orbit",
        "orbit_slot_toast_pkc", "orbit_pub_toast_pkc", start_lsn,
    )
    .await
    .unwrap();

    let mut replica = Replica::new();
    let mut cols = BTreeMap::new();
    cols.insert("id".to_string(), ColumnType::String);
    cols.insert("big".to_string(), ColumnType::String);
    replica.add_table("toast_pkc", cols, vec!["id".into()]);

    let big: String = (0..40_000).map(|i| format!("{i:x}")).collect();
    client
        .execute("INSERT INTO toast_pkc VALUES ($1, $2)", &[&"a", &big.as_str()])
        .await
        .unwrap();
    client
        .execute("UPDATE toast_pkc SET id = 'b' WHERE id = 'a'", &[])
        .await
        .unwrap();

    pump_until(&mut stream, &replica, Duration::from_secs(15), |r| {
        lookup_in_memory(r, "toast_pkc", "b").is_some()
    })
    .await;
    assert!(lookup_in_memory(&replica, "toast_pkc", "a").is_none(), "old key must be gone");
    let row = lookup_in_memory(&replica, "toast_pkc", "b").unwrap();
    assert_eq!(row.get("big"), Some(&Value::String(big)));

    drop_slot(&client, "orbit_slot_toast_pkc").await;
}

/// An upstream TRUNCATE must remove every replicated row (previously it was
/// silently ignored → stale rows forever).
#[tokio::test]
async fn truncate_clears_replica() {
    let client = connect().await;
    client
        .batch_execute(
            "DROP TABLE IF EXISTS trunc_t;
             CREATE TABLE trunc_t (id text PRIMARY KEY, n int);
             ALTER TABLE trunc_t REPLICA IDENTITY FULL;",
        )
        .await
        .unwrap();
    drop_slot(&client, "orbit_slot_trunc").await;
    create_publication(&client, "orbit_pub_trunc", &["trunc_t"]).await.unwrap();
    let start_lsn = create_slot(&client, "orbit_slot_trunc").await.unwrap();
    let mut stream = ReplicationStream::start(
        &host(), port(), "orbit", "orbit", "orbit_slot_trunc", "orbit_pub_trunc", start_lsn,
    )
    .await
    .unwrap();

    let mut replica = SqliteReplica::in_memory();
    replica.add_table(
        "trunc_t",
        vec![("id".into(), ColumnType::String), ("n".into(), ColumnType::Number)],
        vec!["id".into()],
    );

    client
        .batch_execute(
            "INSERT INTO trunc_t VALUES ('a', 1), ('b', 2), ('c', 3);
             TRUNCATE trunc_t;
             INSERT INTO trunc_t VALUES ('d', 4);",
        )
        .await
        .unwrap();

    pump_until(&mut stream, &replica, Duration::from_secs(15), |r| {
        lookup_sqlite(r, "trunc_t", "d").is_some()
    })
    .await;

    let rows = replica.source("trunc_t").unwrap().borrow().all_rows();
    let ids: Vec<String> = rows
        .iter()
        .map(|r| match r.get("id") {
            Some(Value::String(s)) => s.clone(),
            other => panic!("bad id {other:?}"),
        })
        .collect();
    assert_eq!(ids, vec!["d".to_string()], "truncate must clear pre-existing rows");

    drop_slot(&client, "orbit_slot_trunc").await;
}

/// Type fidelity: values seeded by initial sync and values arriving via the
/// stream must decode IDENTICALLY, with real types (timestamps → epoch ms,
/// jsonb → json, arrays → json, big int8 exact, numeric parsed).
#[tokio::test]
async fn snapshot_and_stream_decode_identically() {
    let client = connect().await;
    client
        .batch_execute(
            "DROP TABLE IF EXISTS typ_t;
             CREATE TABLE typ_t (
                 id text PRIMARY KEY,
                 big_id int8,
                 price numeric(12,2),
                 at timestamptz,
                 day date,
                 meta jsonb,
                 tags text[],
                 nums int4[],
                 blob bytea,
                 ok bool
             );
             ALTER TABLE typ_t REPLICA IDENTITY FULL;",
        )
        .await
        .unwrap();

    const INSERT: &str = "INSERT INTO typ_t VALUES (
        $1,
        9007199254740993,
        19.99,
        '2026-07-14 12:00:00.5+00',
        '2026-07-14',
        '{\"a\": 1, \"b\": [true, null]}',
        ARRAY['x','y z'],
        ARRAY[1,2,3],
        '\\xdeadbeef',
        true
    )";

    // Row 'snap' exists BEFORE the slot → arrives via initial sync.
    client.execute(INSERT, &[&"snap"]).await.unwrap();

    drop_slot(&client, "orbit_slot_typ").await;
    create_publication(&client, "orbit_pub_typ", &["typ_t"]).await.unwrap();
    let start_lsn = create_slot(&client, "orbit_slot_typ").await.unwrap();
    let mut stream = ReplicationStream::start(
        &host(), port(), "orbit", "orbit", "orbit_slot_typ", "orbit_pub_typ", start_lsn,
    )
    .await
    .unwrap();

    let columns: Vec<(String, ColumnType)> = vec![
        ("id".into(), ColumnType::String),
        ("big_id".into(), ColumnType::Number),
        ("price".into(), ColumnType::Number),
        ("at".into(), ColumnType::Number),
        ("day".into(), ColumnType::Number),
        ("meta".into(), ColumnType::Json),
        ("tags".into(), ColumnType::Json),
        ("nums".into(), ColumnType::Json),
        ("blob".into(), ColumnType::String),
        ("ok".into(), ColumnType::Boolean),
    ];
    let mut replica = SqliteReplica::in_memory();
    replica.add_table("typ_t", columns, vec!["id".into()]);

    // Initial sync (snapshot path).
    orbit_cache::pg::initial_sync_backend(&client, &replica, "typ_t").await.unwrap();

    // Row 'live' arrives via the stream.
    client.execute(INSERT, &[&"live"]).await.unwrap();
    pump_until(&mut stream, &replica, Duration::from_secs(15), |r| {
        lookup_sqlite(r, "typ_t", "live").is_some()
    })
    .await;

    let snap = lookup_sqlite(&replica, "typ_t", "snap").unwrap();
    let live = lookup_sqlite(&replica, "typ_t", "live").unwrap();

    // 2026-07-14 12:00:00.500 UTC in epoch ms.
    let expect_at = 1_784_030_400_500.0_f64;
    let expect_day = 1_783_987_200_000.0_f64; // 2026-07-14 00:00 UTC

    for (name, row) in [("snap", &snap), ("live", &live)] {
        assert_eq!(row.get("big_id"), Some(&Value::Int(9_007_199_254_740_993)), "{name} big_id");
        assert_eq!(row.get("price"), Some(&Value::Number(19.99)), "{name} price");
        assert_eq!(row.get("at"), Some(&Value::Number(expect_at)), "{name} at");
        assert_eq!(row.get("day"), Some(&Value::Number(expect_day)), "{name} day");
        assert_eq!(
            row.get("meta"),
            Some(&Value::Json(serde_json::json!({"a": 1, "b": [true, null]}))),
            "{name} meta"
        );
        assert_eq!(
            row.get("tags"),
            Some(&Value::Json(serde_json::json!(["x", "y z"]))),
            "{name} tags"
        );
        assert_eq!(
            row.get("nums"),
            Some(&Value::Json(serde_json::json!([1, 2, 3]))),
            "{name} nums"
        );
        assert_eq!(row.get("blob"), Some(&Value::String("\\xdeadbeef".into())), "{name} blob");
        assert_eq!(row.get("ok"), Some(&Value::Bool(true)), "{name} ok");
    }

    // The two paths must agree exactly, column for column.
    let strip_id = |r: &oql::value::Row| {
        let mut r = r.clone();
        r.remove("id");
        r
    };
    assert_eq!(strip_id(&snap), strip_id(&live), "snapshot and stream decode must be identical");

    drop_slot(&client, "orbit_slot_typ").await;
}
