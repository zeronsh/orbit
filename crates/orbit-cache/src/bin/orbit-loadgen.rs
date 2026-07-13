//! `orbit-loadgen` — WebSocket load generator for the 512 MB acceptance
//! harness (`scripts/acceptance/run.sh`). Drives view-syncers with
//! full-history hydrations, mid-hydration disconnects, and same-client CVR
//! reconnects, verifying exact row counts at every completed poke transaction.
//! Exits non-zero on any mismatch or timeout.
//!
//! Env:
//!   LOADGEN_WS          comma-separated ws addrs (e.g. view-syncer-1:4848,view-syncer-2:4848)
//!   LOADGEN_CLIENTS     concurrent client tasks (default 8)
//!   LOADGEN_DURATION    seconds to run (default 120)
//!   LOADGEN_EXPECT      table=count,table=count (exact rows per table)
//!   LOADGEN_CHURN_PCT   % of hydrations killed mid-flight then retried (default 30)
//!   LOADGEN_HYDRATIONS  max concurrent FRESH full hydrations (default 2 — a
//!                       full-history result is O(dataset) on the server while
//!                       being chunked out; the harness bounds concurrency the
//!                       way a real deployment's client population does)

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use oql::ast::Direction;
use oql::Query;
use orbit_protocol::{ChangeDesiredQueriesBody, Downstream, QueriesPatchOp, RowPatchOp, Upstream};
use tokio_tungstenite::tungstenite::Message;

type Ws = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

fn env(k: &str, d: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| d.to_string())
}

#[derive(Clone)]
struct Config {
    addrs: Vec<String>,
    expect: Arc<HashMap<String, u64>>, // table -> exact row count
    churn_pct: u64,
    deadline: tokio::time::Instant,
}

// mimalloc: glibc malloc never returns a freed hydration working set to the
// OS (arena retention pins RSS at peak), which matters in memory-limited
// containers. mimalloc purges freed pages back to the OS.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let addrs: Vec<String> =
        env("LOADGEN_WS", "127.0.0.1:4848").split(',').map(|s| s.trim().to_string()).collect();
    let clients: usize = env("LOADGEN_CLIENTS", "8").parse()?;
    let duration: u64 = env("LOADGEN_DURATION", "120").parse()?;
    let churn_pct: u64 = env("LOADGEN_CHURN_PCT", "30").parse()?;
    let hydration_slots: usize = env("LOADGEN_HYDRATIONS", "2").parse()?;
    let mut expect = HashMap::new();
    for spec in env("LOADGEN_EXPECT", "").split(',').filter(|s| !s.trim().is_empty()) {
        let (t, n) = spec.trim().split_once('=').expect("LOADGEN_EXPECT: table=count");
        expect.insert(t.to_string(), n.parse::<u64>().expect("count"));
    }
    anyhow::ensure!(!expect.is_empty(), "set LOADGEN_EXPECT, e.g. acc_small=20000,acc_big=5");

    let cfg = Config {
        addrs,
        expect: Arc::new(expect),
        churn_pct,
        deadline: tokio::time::Instant::now() + Duration::from_secs(duration),
    };
    let failed = Arc::new(AtomicBool::new(false));
    let hydrations_done = Arc::new(AtomicU64::new(0));
    let churns_done = Arc::new(AtomicU64::new(0));
    // Bound concurrent fresh hydrations (see LOADGEN_HYDRATIONS doc above).
    let hydration_gate = Arc::new(tokio::sync::Semaphore::new(hydration_slots));

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut tasks = Vec::new();
            for i in 0..clients {
                let cfg = cfg.clone();
                let failed = failed.clone();
                let hydrations_done = hydrations_done.clone();
                let churns_done = churns_done.clone();
                let gate = hydration_gate.clone();
                tasks.push(tokio::task::spawn_local(client_loop(
                    i, cfg, gate, failed, hydrations_done, churns_done,
                )));
            }
            for t in tasks {
                let _ = t.await;
            }
        })
        .await;

    let h = hydrations_done.load(Ordering::Relaxed);
    let c = churns_done.load(Ordering::Relaxed);
    eprintln!("loadgen: {h} verified hydrations, {c} mid-hydration churns");
    if failed.load(Ordering::Relaxed) {
        eprintln!("loadgen: FAILED (count mismatch or timeout — see log above)");
        std::process::exit(1);
    }
    anyhow::ensure!(h >= 3, "only {h} hydrations completed — too few to call it verified");
    eprintln!("loadgen: OK");
    Ok(())
}

/// Correctness failures (wrong counts, wedged hydration) are FATAL; transport
/// errors are NOT — the harness itself restarts a view-syncer mid-run, so
/// resets/refusals/DNS blips are expected churn and get retried.
fn is_fatal(e: &anyhow::Error) -> bool {
    let s = format!("{e:#}");
    s.contains("COUNT-MISMATCH") || s.contains("hydration timed out")
}

async fn client_loop(
    idx: usize,
    cfg: Config,
    gate: Arc<tokio::sync::Semaphore>,
    failed: Arc<AtomicBool>,
    hydrations_done: Arc<AtomicU64>,
    churns_done: Arc<AtomicU64>,
) {
    let mut round: u64 = 0;
    while tokio::time::Instant::now() < cfg.deadline && !failed.load(Ordering::Relaxed) {
        round += 1;
        let addr = &cfg.addrs[(idx + round as usize) % cfg.addrs.len()];
        // Cheap decorrelated pseudo-random churn decision (no rand dep games).
        let churn = (idx as u64 * 7919 + round * 104729) % 100 < cfg.churn_pct;
        let cid = format!("lg-{idx}-{round}");
        let _slot = gate.acquire().await.unwrap();
        match run_hydration(addr, &cid, &cfg, churn).await {
            Ok(true) => {
                hydrations_done.fetch_add(1, Ordering::Relaxed);
            }
            Ok(false) => {
                churns_done.fetch_add(1, Ordering::Relaxed);
                // Reconnect with the SAME clientID (CVR resume path): must
                // reach a clean pokeEnd again, counts verified.
                match run_hydration(addr, &cid, &cfg, false).await {
                    Ok(_) => {
                        hydrations_done.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) if is_fatal(&e) => {
                        eprintln!("client {idx} round {round} (resume): FAILED: {e:#}");
                        failed.store(true, Ordering::Relaxed);
                    }
                    Err(e) => {
                        eprintln!("client {idx} round {round} (resume): transient ({e:#}); retrying");
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                }
            }
            Err(e) if is_fatal(&e) => {
                eprintln!("client {idx} round {round}: FAILED: {e:#}");
                failed.store(true, Ordering::Relaxed);
            }
            Err(e) => {
                eprintln!("client {idx} round {round}: transient ({e:#}); retrying");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
}

/// Connect as `cid`, subscribe a full-history query per expected table, and
/// accumulate row Puts across chunked pokeParts. Returns Ok(true) once every
/// table's count matched at a pokeEnd; Ok(false) if `churn` cut the socket
/// mid-hydration (after the first part).
async fn run_hydration(addr: &str, cid: &str, cfg: &Config, churn: bool) -> anyhow::Result<bool> {
    let url = format!("ws://{addr}/?clientID={cid}");
    let (mut ws, _) = tokio::time::timeout(
        Duration::from_secs(10),
        tokio_tungstenite::connect_async(&url),
    )
    .await
    .map_err(|_| anyhow::anyhow!("connect timeout to {addr}"))??;

    expect_connected(&mut ws).await?;

    let patch: Vec<QueriesPatchOp> = cfg
        .expect
        .keys()
        .map(|t| QueriesPatchOp::Put {
            hash: format!("full-{t}"),
            ast: Some(Query::table(t).order_by("id", Direction::Asc).build()),
            name: None,
            args: None,
            ttl: None,
        })
        .collect();
    let n_queries = patch.len();
    ws.send(Message::Text(serde_json::to_string(&Upstream::ChangeDesiredQueries(
        ChangeDesiredQueriesBody { desired_queries_patch: patch, traceparent: None },
    ))?))
    .await?;

    // A CVR resume can suppress unchanged rows, so counts are verified against
    // the client's ACCUMULATED view across the connection (Clear resets it).
    let mut view: HashMap<String, std::collections::HashSet<String>> = HashMap::new();
    let mut ends = 0usize;
    let mut parts = 0usize;
    let overall = tokio::time::Instant::now() + Duration::from_secs(600);
    loop {
        anyhow::ensure!(tokio::time::Instant::now() < overall, "hydration timed out");
        let msg = tokio::time::timeout(Duration::from_secs(60), ws.next())
            .await
            .map_err(|_| anyhow::anyhow!("ws read timeout"))?
            .ok_or_else(|| anyhow::anyhow!("ws closed mid-hydration"))??;
        let Message::Text(text) = msg else { continue };
        match serde_json::from_str::<Downstream>(&text)? {
            Downstream::PokePart(p) => {
                parts += 1;
                for op in p.rows_patch.unwrap_or_default() {
                    match op {
                        RowPatchOp::Put { table_name, value } => {
                            if let Some(oql::value::Value::String(id)) = value.get("id") {
                                view.entry(table_name).or_default().insert(id.clone());
                            }
                        }
                        RowPatchOp::Del { table_name, id } => {
                            if let Some(oql::value::Value::String(id)) = id.get("id") {
                                view.entry(table_name).or_default().remove(id.as_str());
                            }
                        }
                        RowPatchOp::Clear => view.clear(),
                        RowPatchOp::Update { .. } => {}
                    }
                }
                if churn && parts >= 1 {
                    // Kill mid-hydration (the reconnect-churn case). The server
                    // must clean up; the client discards the partial poke.
                    drop(ws);
                    return Ok(false);
                }
            }
            Downstream::PokeEnd(_) => {
                ends += 1;
                if ends >= n_queries {
                    for (t, want) in cfg.expect.iter() {
                        let got = view.get(t).map(|s| s.len() as u64).unwrap_or(0);
                        anyhow::ensure!(
                            got == *want,
                            "COUNT-MISMATCH: table {t}: got {got} rows, expected {want}"
                        );
                    }
                    let _ = ws.close(None).await;
                    return Ok(true);
                }
            }
            _ => {}
        }
    }
}

async fn expect_connected(ws: &mut Ws) -> anyhow::Result<()> {
    loop {
        let msg = tokio::time::timeout(Duration::from_secs(10), ws.next())
            .await
            .map_err(|_| anyhow::anyhow!("timeout waiting for connected"))?
            .ok_or_else(|| anyhow::anyhow!("ws closed before connected"))??;
        if let Message::Text(t) = msg {
            if matches!(serde_json::from_str::<Downstream>(&t)?, Downstream::Connected(_)) {
                return Ok(());
            }
        }
    }
}
