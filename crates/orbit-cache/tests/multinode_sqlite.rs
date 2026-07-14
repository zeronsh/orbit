//! Multinode end-to-end over the **SQLite replica + streamed SQLite-file
//! snapshots** (`run_replicator_sqlite` / `run_view_syncer_sqlite`): one
//! replicator + two view-syncers, each with its own durable replica dir. One
//! view-syncer starts BEFORE the replicator (exercising the wait-for-snapshot
//! loop) and one after (exercising restore-from-file). A Postgres INSERT must
//! reach clients on both.
//!
//! Also: restore short-circuiting (a durable view-syncer restart must NOT
//! re-download the snapshot), corrupt-snapshot rejection, and local
//! invalidation.
//!
//! Requires the `orbit-pg` container on 127.0.0.1:5433 (see STATUS.md).

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use oql::ast::Direction;
use oql::ivm::ColumnType;
use oql::value::Value;
use oql::Query;
use orbit_cache::{
    run_replicator_sqlite, run_view_syncer_sqlite, LocalObjectStore, MutatorRegistry, ObjectStore,
    QueryRegistry, ServerConfig, SnapshotStrategy, SqliteClusterConfig, SqliteSnapshots,
    TableConfig,
};
use orbit_protocol::{ChangeDesiredQueriesBody, Downstream, QueriesPatchOp, RowPatchOp, Upstream};
use tokio_postgres::NoTls;
use tokio_tungstenite::tungstenite::Message;

type Ws = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

const CS_ADDR: &str = "127.0.0.1:39721";
const WS_A: &str = "127.0.0.1:39722";
const WS_B: &str = "127.0.0.1:39723";
const SLOT: &str = "orbit_mns_slot";
const PUB: &str = "orbit_mns_pub";

fn cfg(listen: &str) -> ServerConfig {
    ServerConfig {
        host: "127.0.0.1".into(),
        port: std::env::var("ORBIT_PG_PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(5433),
        user: "orbit".into(),
        database: "orbit".into(),
        password: None,
        tls: orbit_cache::PgTlsMode::Disable,
        tables: vec![TableConfig {
            name: "mns_item".into(),
            columns: vec![("id".into(), ColumnType::String), ("n".into(), ColumnType::Number)],
            primary_key: vec!["id".into()],
        }],
        publication: PUB.into(),
        slot: SLOT.into(),
        listen_addr: listen.into(),
        mutate_url: None,
        query_url: None,
        api_key: None,
        forward_cookies: false,
    }
}

fn spawn_node<F, Fut>(make: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = anyhow::Result<()>>,
{
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        if let Err(e) = rt.block_on(make()) {
            eprintln!("NODE EXITED WITH ERROR: {e:#}");
        } else {
            eprintln!("NODE EXITED (ok)");
        }
    });
}

async fn next_down(ws: &mut Ws) -> Downstream {
    loop {
        match tokio::time::timeout(Duration::from_secs(15), ws.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => return serde_json::from_str(&t).unwrap(),
            Ok(Some(Ok(_))) => continue,
            other => panic!("ws closed/timeout: {other:?}"),
        }
    }
}

async fn connect_subscribe(addr: &str) -> Ws {
    let mut ws = None;
    for _ in 0..150 {
        if let Ok((s, _)) = tokio_tungstenite::connect_async(format!("ws://{addr}/")).await {
            ws = Some(s);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let mut ws = ws.expect("connect to view-syncer");
    assert!(matches!(next_down(&mut ws).await, Downstream::Connected(_)));
    let ast = Query::table("mns_item").order_by("id", Direction::Asc).build();
    ws.send(Message::Text(
        serde_json::to_string(&Upstream::ChangeDesiredQueries(ChangeDesiredQueriesBody {
            desired_queries_patch: vec![QueriesPatchOp::Put { hash: "h1".into(), ast: Some(ast), name: None, args: None, ttl: None }],
            traceparent: None,
        }))
        .unwrap(),
    ))
    .await
    .unwrap();
    assert!(matches!(next_down(&mut ws).await, Downstream::PokeStart(_)));
    ws
}

async fn wait_for_id(ws: &mut Ws, id: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        assert!(std::time::Instant::now() < deadline, "timed out waiting for {id}");
        let d = next_down(ws).await;
        if let Downstream::PokePart(p) = d {
            let has = p
                .rows_patch
                .unwrap_or_default()
                .iter()
                .any(|op| matches!(op, RowPatchOp::Put { value, .. } if value.get("id") == Some(&Value::String(id.into()))));
            if has {
                return;
            }
        }
    }
}

fn sqlite_cfg(dir: &std::path::Path) -> SqliteClusterConfig {
    SqliteClusterConfig::new(dir)
}

#[tokio::test]
async fn mutation_reaches_clients_on_two_sqlite_view_syncers() {
    let host = "127.0.0.1";
    let pg_port: u16 =
        std::env::var("ORBIT_PG_PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(5433);
    let conn_str = format!("host={host} port={pg_port} user=orbit dbname=orbit");
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await.expect("connect orbit-pg");
    tokio::spawn(async move {
        let _ = connection.await;
    });

    client
        .batch_execute(&format!(
            "SELECT pg_drop_replication_slot('{SLOT}') FROM pg_replication_slots WHERE slot_name='{SLOT}';
             DROP TABLE IF EXISTS mns_item;
             DROP TABLE IF EXISTS orbit_change_log_{SLOT};
             CREATE TABLE mns_item (id text PRIMARY KEY, n int);
             ALTER TABLE mns_item REPLICA IDENTITY FULL;",
        ))
        .await
        .unwrap();

    let base = std::env::temp_dir().join(format!("orbit-mns-{}", std::process::id()));
    std::fs::remove_dir_all(&base).ok();
    let store_dir = base.join("store");
    let repl_dir = base.join("replicator");
    let vs_a_dir = base.join("vs-a");
    let vs_b_dir = base.join("vs-b");

    // View-syncer A starts FIRST — it must wait for the replicator's snapshot
    // (the wait-for-snapshot loop) instead of erroring out.
    {
        let (store_dir, vs_a_dir) = (store_dir.clone(), vs_a_dir.clone());
        spawn_node(move || async move {
            run_view_syncer_sqlite(
                cfg(WS_A),
                LocalObjectStore::new(&store_dir),
                CS_ADDR.into(),
                MutatorRegistry::new(),
                QueryRegistry::new(),
                sqlite_cfg(&vs_a_dir),
            )
            .await
        });
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Replicator: durable SQLite replica + streamed file snapshots.
    {
        let (store_dir, repl_dir) = (store_dir.clone(), repl_dir.clone());
        spawn_node(move || async move {
            run_replicator_sqlite(
                cfg("127.0.0.1:39720"),
                LocalObjectStore::new(&store_dir),
                CS_ADDR.into(),
                Duration::from_secs(60),
                sqlite_cfg(&repl_dir),
            )
            .await
        });
    }

    // View-syncer B starts after the snapshot exists → restore-from-file path.
    {
        let (store_dir, vs_b_dir) = (store_dir.clone(), vs_b_dir.clone());
        spawn_node(move || async move {
            run_view_syncer_sqlite(
                cfg(WS_B),
                LocalObjectStore::new(&store_dir),
                CS_ADDR.into(),
                MutatorRegistry::new(),
                QueryRegistry::new(),
                sqlite_cfg(&vs_b_dir),
            )
            .await
        });
    }

    let mut a = connect_subscribe(WS_A).await;
    let mut b = connect_subscribe(WS_B).await;

    client.batch_execute("INSERT INTO mns_item VALUES ('i1', 1)").await.unwrap();

    wait_for_id(&mut a, "i1").await;
    wait_for_id(&mut b, "i1").await;

    client
        .batch_execute(&format!(
            "SELECT pg_drop_replication_slot('{SLOT}') FROM pg_replication_slots WHERE slot_name='{SLOT}'"
        ))
        .await
        .ok();
    std::fs::remove_dir_all(&base).ok();
}

/// An `ObjectStore` decorator that counts streamed downloads.
struct CountingStore {
    inner: LocalObjectStore,
    gets: std::sync::atomic::AtomicUsize,
}

impl ObjectStore for &CountingStore {
    async fn put(&self, key: &str, bytes: Vec<u8>) -> anyhow::Result<()> {
        self.inner.put(key, bytes).await
    }
    async fn get(&self, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
        self.inner.get(key).await
    }
    async fn put_stream(
        &self,
        key: &str,
        data: orbit_cache::objectstore::ByteStream,
        part_size: usize,
    ) -> anyhow::Result<()> {
        self.inner.put_stream(key, data, part_size).await
    }
    async fn get_stream(&self, key: &str) -> anyhow::Result<Option<orbit_cache::objectstore::ByteStream>> {
        self.gets.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner.get_stream(key).await
    }
}

/// Build a snapshot object by writing a small durable replica and backing it up
/// through the strategy.
async fn write_snapshot_via_strategy<O: ObjectStore>(store: &O, work: &std::path::Path, pos: u64) {
    use orbit_cache::{LogicalEvent, ReplicaBackend, SqliteReplica};
    let mut replica = SqliteReplica::durable(work);
    replica.add_table(
        "mns_item",
        vec![("id".into(), ColumnType::String), ("n".into(), ColumnType::Number)],
        vec!["id".into()],
    );
    replica.begin_txn().unwrap();
    let row: oql::value::Row =
        [("id".to_string(), Value::String("s1".into())), ("n".to_string(), Value::Number(1.0))]
            .into_iter()
            .collect();
    replica.apply(LogicalEvent::Insert { table: "mns_item".into(), row }).unwrap();
    replica.commit_txn(1, pos).unwrap();
    let strat = SqliteSnapshots { cfg: SqliteClusterConfig::new(work) };
    strat.write(store, &replica, pos).await.unwrap();
}

#[tokio::test]
async fn restore_short_circuits_snapshot_download() {
    let base = std::env::temp_dir().join(format!("orbit-mns-short-{}", std::process::id()));
    std::fs::remove_dir_all(&base).ok();
    std::fs::create_dir_all(&base).ok();

    let store = CountingStore {
        inner: LocalObjectStore::new(base.join("store")),
        gets: std::sync::atomic::AtomicUsize::new(0),
    };
    write_snapshot_via_strategy(&&store, &base.join("writer"), 41).await;

    let node_dir = base.join("node");
    let strat = SqliteSnapshots { cfg: SqliteClusterConfig::new(&node_dir) };

    // First boot: downloads (1 streamed GET) and restores at the file's pos.
    let (replica, pos) = strat.restore(&&store, &cfg(WS_A)).await.unwrap();
    assert_eq!(pos, 41);
    assert_eq!(store.gets.load(std::sync::atomic::Ordering::Relaxed), 1);

    // The node applies further txns, recording its own progress.
    use orbit_cache::ReplicaBackend;
    replica.begin_txn().unwrap();
    replica.commit_txn(0, 45).unwrap();
    drop(replica);

    // Restart: the local replica.db short-circuits — same strategy, ZERO new
    // downloads, resumes at the locally-recorded pos.
    let strat2 = SqliteSnapshots { cfg: SqliteClusterConfig::new(&node_dir) };
    let (_replica, pos2) = strat2.restore(&&store, &cfg(WS_A)).await.unwrap();
    assert_eq!(pos2, 45, "resumes from locally-recorded progress");
    assert_eq!(
        store.gets.load(std::sync::atomic::Ordering::Relaxed),
        1,
        "restart must not re-download the snapshot"
    );

    // invalidate_local drops the file → the next restore downloads again.
    strat2.invalidate_local();
    assert!(!node_dir.join("replica.db").exists());
    let strat3 = SqliteSnapshots { cfg: SqliteClusterConfig::new(&node_dir) };
    let (_replica, pos3) = strat3.restore(&&store, &cfg(WS_A)).await.unwrap();
    assert_eq!(pos3, 41, "fresh restore comes from the stored snapshot");
    assert_eq!(store.gets.load(std::sync::atomic::Ordering::Relaxed), 2);

    std::fs::remove_dir_all(&base).ok();
}

#[tokio::test]
async fn corrupt_snapshot_is_rejected_then_recovers() {
    let base = std::env::temp_dir().join(format!("orbit-mns-corrupt-{}", std::process::id()));
    std::fs::remove_dir_all(&base).ok();
    std::fs::create_dir_all(&base).ok();

    let store = LocalObjectStore::new(base.join("store"));
    // Garbage where the snapshot should be.
    store.put("snapshot/latest.db", b"this is not a sqlite file".to_vec()).await.unwrap();

    let node_dir = base.join("node");
    let strat = std::rc::Rc::new(SqliteSnapshots { cfg: SqliteClusterConfig::new(&node_dir) });

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let restore = {
                let strat = strat.clone();
                let store = LocalObjectStore::new(base.join("store"));
                let cfg = cfg(WS_A);
                tokio::task::spawn_local(async move { strat.restore(&store, &cfg).await })
            };
            // The garbage object must be rejected (validation), not renamed in.
            tokio::time::sleep(Duration::from_millis(700)).await;
            assert!(!restore.is_finished(), "corrupt snapshot must not restore");
            assert!(!node_dir.join("replica.db").exists(), "garbage never renamed into place");

            // A valid snapshot appears → restore completes with its pos + rows.
            write_snapshot_via_strategy(&store, &base.join("writer"), 7).await;
            let (replica, pos) =
                tokio::time::timeout(Duration::from_secs(10), restore).await.unwrap().unwrap().unwrap();
            assert_eq!(pos, 7);
            let rows = replica.source("mns_item").unwrap().borrow().all_rows();
            assert_eq!(rows.len(), 1);
        })
        .await;

    std::fs::remove_dir_all(&base).ok();
}
