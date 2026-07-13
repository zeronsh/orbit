//! The durable change-log is byte-bounded: when Postgres stalls, `append`
//! parks (backpressure) instead of growing an unbounded in-memory queue, and
//! resumes as soon as the writer flushes. Reads are byte-capped so a resume
//! bridge never materialises 50K fat rows at once. Needs Postgres (orbit-pg
//! on :5433).

use std::sync::Arc;
use std::time::Duration;

use orbit_cache::changelog::{ChangeLogConfig, PgChangeLog};
use orbit_cache::{
    ChangeMsg, ChangeStreamClient, ChangeStreamConfig, ChangeStreamServer, LogicalEvent, PgTlsMode,
};
use tokio_postgres::NoTls;

fn conn_str() -> String {
    let host = std::env::var("ORBIT_PG_HOST").unwrap_or_else(|_| "127.0.0.1".into());
    let port: u16 =
        std::env::var("ORBIT_PG_PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(5433);
    format!("host={host} port={port} user=orbit dbname=orbit")
}

async fn pg() -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::connect(&conn_str(), NoTls).await.expect("connect orbit-pg");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    client
}

fn fat_event(payload: usize) -> Arc<LogicalEvent> {
    let mut row = oql::value::Row::new();
    row.insert("id", oql::value::Value::Number(1.0));
    row.insert("body", oql::value::Value::String("z".repeat(payload)));
    Arc::new(LogicalEvent::Insert { table: "t".into(), row })
}

/// A stalled Postgres must park `append` at the byte budget — not grow RAM —
/// and release it once the stall clears and the writer flushes.
#[tokio::test]
async fn append_backpressures_on_stalled_pg_and_recovers() {
    let table = "orbit_change_log_bp_test";
    let admin = pg().await;
    admin.batch_execute(&format!("DROP TABLE IF EXISTS {table}")).await.unwrap();

    let cfg = ChangeLogConfig {
        queue_events: 1024,
        queue_bytes: 4 * 1024, // fits ~one 3 KiB event
        max_batch_events: 1024,
        max_batch_bytes: 4 << 20,
    };
    let log = PgChangeLog::open_with(cfg, conn_str(), table.to_string(), PgTlsMode::Disable)
        .await
        .unwrap();
    let stats = log.stats();

    // Stall the log table: the writer's INSERT blocks on this lock.
    let locker = pg().await;
    locker
        .batch_execute(&format!("BEGIN; LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE;"))
        .await
        .unwrap();

    // First fat event: fits the budget, gets picked up by the writer, whose
    // flush now blocks — so its permits are NOT returned.
    log.append(1, 100, fat_event(3 * 1024)).await;
    assert!(stats.queued_events.load(std::sync::atomic::Ordering::Relaxed) <= 1);

    // Second fat event: budget exhausted → append must park (backpressure).
    let second = log.append(2, 101, fat_event(3 * 1024));
    let parked = tokio::time::timeout(Duration::from_millis(300), second).await;
    assert!(parked.is_err(), "append should park while the log is stalled at the byte budget");

    // Clear the stall: the blocked flush completes, permits return, and a
    // fresh append (same pos 2) goes straight through.
    locker.batch_execute("ROLLBACK").await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), log.append(2, 101, fat_event(3 * 1024)))
        .await
        .expect("append should complete after the stall clears");

    // Everything drains: durable watermark advances, queue counters go to 0.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while log.durable_lsn() < 101 {
        assert!(tokio::time::Instant::now() < deadline, "writer never drained");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(stats.queued_events.load(std::sync::atomic::Ordering::Relaxed), 0);
    assert_eq!(stats.queued_bytes.load(std::sync::atomic::Ordering::Relaxed), 0);
}

/// Byte-triggered ring eviction behaves exactly like count-triggered eviction:
/// a resume point evicted by the byte cap is bridged by delta from the durable
/// log — never a Reset.
#[tokio::test]
async fn byte_evicted_resume_bridges_from_durable_log() {
    let table = "orbit_change_log_bytes_evict_test";
    let admin = pg().await;
    admin.batch_execute(&format!("DROP TABLE IF EXISTS {table}")).await.unwrap();

    let log = Arc::new(
        PgChangeLog::open(conn_str(), table.to_string(), PgTlsMode::Disable).await.unwrap(),
    );
    // Byte cap ~2 fat events; count cap generous — eviction is byte-driven.
    let cfg = ChangeStreamConfig { max_events: 1024, max_bytes: 5 * 1024, broadcast_cap: 64 };
    let server = ChangeStreamServer::with_config(cfg, 0, Some(log.clone()));
    let addr = "127.0.0.1:47751";
    {
        let server = server.clone();
        tokio::spawn(async move {
            let _ = server.serve(addr).await;
        });
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    // 5 fat commits → pos 1..=5; the ring can only hold ~2 of them.
    for i in 0..5u64 {
        let mut row = oql::value::Row::new();
        row.insert("body", oql::value::Value::String("y".repeat(2 * 1024)));
        server.publish(1000 + i, LogicalEvent::Insert { table: "t".into(), row }).await;
    }
    // Let the async writer flush to Postgres so the bridge can read the delta.
    tokio::time::sleep(Duration::from_millis(900)).await;

    let mut c = ChangeStreamClient::connect(addr, 1).await.unwrap();
    let mut got = Vec::new();
    for _ in 0..4 {
        match tokio::time::timeout(Duration::from_secs(5), c.next()).await.unwrap().unwrap() {
            Some(ChangeMsg::Change { pos, .. }) => got.push(pos),
            Some(ChangeMsg::Reset) => panic!("got Reset; expected a delta bridged from the log"),
            None => break,
        }
    }
    assert_eq!(got, vec![2, 3, 4, 5], "byte-evicted resume@1 must bridge the full delta");
}

/// `read_after` must stop at the byte cap (partial contiguous page), always
/// return at least one row, and let a paging loop retrieve the full delta.
#[tokio::test]
async fn read_after_respects_byte_cap() {
    let table = "orbit_change_log_bytes_test";
    let admin = pg().await;
    admin.batch_execute(&format!("DROP TABLE IF EXISTS {table}")).await.unwrap();

    let log = PgChangeLog::open(conn_str(), table.to_string(), PgTlsMode::Disable).await.unwrap();
    for i in 1..=10u64 {
        log.append(i, 1000 + i, fat_event(2 * 1024)).await; // ~2 KiB JSON each
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while log.durable_lsn() < 1010 {
        assert!(tokio::time::Instant::now() < deadline, "writer never flushed");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Cap ~3 events' worth: a partial, contiguous prefix.
    let (_, page) = log.read_after(0, 1000, 6 * 1024).await.unwrap();
    assert!(!page.is_empty() && page.len() < 10, "expected a byte-capped partial page, got {}", page.len());
    for (i, (pos, _)) in page.iter().enumerate() {
        assert_eq!(*pos, i as u64 + 1, "page must be contiguous from pos 1");
    }

    // Cap smaller than a single row: still returns exactly one row (progress).
    let (_, one) = log.read_after(0, 1000, 16).await.unwrap();
    assert_eq!(one.len(), 1, "must return at least one row past an oversized event");

    // A paging loop over the byte cap retrieves the whole delta contiguously.
    let mut last = 0u64;
    let mut total = 0;
    loop {
        let (_, page) = log.read_after(last, 1000, 6 * 1024).await.unwrap();
        if page.is_empty() {
            break;
        }
        for (pos, _) in &page {
            assert_eq!(*pos, last + 1);
            last = *pos;
            total += 1;
        }
    }
    assert_eq!(total, 10);
}
