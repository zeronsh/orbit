//! Readiness end-to-end (real Postgres): a booting server answers 503 on
//! `/ready`, flips to 200 once initial sync completed and the WS listener is
//! bound, and serves Prometheus text on `/metrics`.
//!
//! Requires the `orbit-pg` container on 127.0.0.1:5433 (see STATUS.md).

use std::time::Duration;

use oql::ivm::ColumnType;
use orbit_cache::{run_server, MutatorRegistry, ServerConfig, TableConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_postgres::NoTls;

const WS: &str = "127.0.0.1:39750";
const METRICS: &str = "127.0.0.1:39751";
const SLOT: &str = "orbit_ready_slot";
const PUB: &str = "orbit_ready_pub";

async fn http_get(addr: &str, path: &str) -> Option<String> {
    let mut s = tokio::net::TcpStream::connect(addr).await.ok()?;
    s.write_all(format!("GET {path} HTTP/1.1\r\nHost: x\r\n\r\n").as_bytes()).await.ok()?;
    let mut buf = String::new();
    s.read_to_string(&mut buf).await.ok()?;
    Some(buf)
}

#[tokio::test]
async fn ready_flips_after_boot_and_metrics_render() {
    std::env::set_var("ORBIT_METRICS_LISTEN", METRICS);

    let pg_port: u16 =
        std::env::var("ORBIT_PG_PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(5433);
    let conn_str = format!("host=127.0.0.1 port={pg_port} user=orbit dbname=orbit");
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await.expect("connect orbit-pg");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!(
            "SELECT pg_drop_replication_slot('{SLOT}') FROM pg_replication_slots WHERE slot_name='{SLOT}';
             DROP TABLE IF EXISTS ready_item;
             CREATE TABLE ready_item (id text PRIMARY KEY, n int);
             ALTER TABLE ready_item REPLICA IDENTITY FULL;
             INSERT INTO ready_item VALUES ('a', 1);",
        ))
        .await
        .unwrap();

    std::thread::spawn(|| {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let cfg = ServerConfig {
            host: "127.0.0.1".into(),
            port: std::env::var("ORBIT_PG_PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(5433),
            user: "orbit".into(),
            database: "orbit".into(),
            password: None,
            tls: orbit_cache::PgTlsMode::Disable,
            tables: vec![TableConfig {
                name: "ready_item".into(),
                columns: vec![("id".into(), ColumnType::String), ("n".into(), ColumnType::Number)],
                primary_key: vec!["id".into()],
            }],
            publication: PUB.into(),
            slot: SLOT.into(),
            listen_addr: WS.into(),
            mutate_url: None,
            query_url: None,
            api_key: None,
            forward_cookies: false,
        };
        if let Err(e) = rt.block_on(run_server(cfg, MutatorRegistry::new())) {
            eprintln!("SERVER EXITED WITH ERROR: {e:#}");
        }
    });

    // The metrics endpoint comes up FIRST (before PG connect/sync) and must
    // report unready — or, if boot won the race, ready with the WS bound.
    let mut saw_unready = false;
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    let ready = loop {
        assert!(std::time::Instant::now() < deadline, "metrics endpoint never became ready");
        if let Some(resp) = http_get(METRICS, "/ready").await {
            if resp.starts_with("HTTP/1.1 503") {
                saw_unready = true;
            } else if resp.starts_with("HTTP/1.1 200") {
                break resp;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert!(ready.contains("ready"));
    // Boot takes a PG round-trip + publication/slot/sync — the first poll
    // virtually always lands during it; tolerate the (rare) fast-boot race.
    if !saw_unready {
        eprintln!("note: boot won the race; 503 window not observed");
    }

    // Once ready, the WS port accepts and /metrics renders.
    tokio::net::TcpStream::connect(WS).await.expect("WS listener bound when ready");
    let metrics = http_get(METRICS, "/metrics").await.unwrap();
    assert!(metrics.contains("orbit_ready{role=\"single-node\"} 1"));
    assert!(metrics.contains("# TYPE orbit_poke_bytes_total counter"));
    let live = http_get(METRICS, "/live").await.unwrap();
    assert!(live.starts_with("HTTP/1.1 200"));

    client
        .batch_execute(&format!(
            "SELECT pg_drop_replication_slot('{SLOT}') FROM pg_replication_slots WHERE slot_name='{SLOT}'"
        ))
        .await
        .ok();
}
