//! The durable change-log lets a view-syncer resume by *delta* even after its
//! resume point has been evicted from the in-memory ring (or lost on a replicator
//! restart) — instead of re-restoring the whole replica. Needs Postgres (orbit-pg
//! on :5433).

use std::sync::Arc;
use std::time::Duration;

use orbit_cache::changelog::PgChangeLog;
use orbit_cache::{ChangeMsg, ChangeStreamClient, ChangeStreamServer, LogicalEvent};
use tokio_postgres::NoTls;

#[tokio::test]
async fn resume_from_durable_log_after_ring_eviction() {
    let conn_str = "host=127.0.0.1 port=5433 user=orbit dbname=orbit";
    let (client, conn) = tokio_postgres::connect(conn_str, NoTls).await.expect("connect orbit-pg");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    client.batch_execute("DROP TABLE IF EXISTS orbit_change_log_test").await.unwrap();

    let log = Arc::new(
        PgChangeLog::open(conn_str.to_string(), "orbit_change_log_test".to_string())
            .await
            .unwrap(),
    );
    // Ring capacity 2: only the two newest positions stay in memory; older resume
    // points must be served from the durable log.
    let server = ChangeStreamServer::new_with_log(2, 0, Some(log.clone()));
    let addr = "127.0.0.1:47750";
    {
        let server = server.clone();
        tokio::spawn(async move {
            let _ = server.serve(addr).await;
        });
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Publish 5 commits → pos 1..=5. The ring keeps {4,5} (floor=3); the log keeps all.
    for i in 0..5u64 {
        server.publish(1000 + i, LogicalEvent::Commit);
    }
    // Let the async writer flush to Postgres.
    tokio::time::sleep(Duration::from_millis(900)).await;

    // A view-syncer resuming at pos 1 — evicted from the ring — still receives the
    // delta 2,3,4,5 (2,3 bridged from the log, 4,5 from the ring), never a Reset.
    let mut c = ChangeStreamClient::connect(addr, 1).await.unwrap();
    let mut got = Vec::new();
    for _ in 0..4 {
        match tokio::time::timeout(Duration::from_secs(5), c.next()).await.unwrap().unwrap() {
            Some(ChangeMsg::Change { pos, .. }) => got.push(pos),
            Some(ChangeMsg::Reset) => panic!("got Reset; expected a delta from the durable log"),
            None => break,
        }
    }
    assert_eq!(got, vec![2, 3, 4, 5], "resume@1 bridges log (2,3) + ring (4,5)");
}
