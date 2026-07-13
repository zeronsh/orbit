//! Observability: a per-node metrics registry, a Prometheus text renderer, and
//! a tiny hand-rolled HTTP responder serving `/metrics`, `/ready`, and `/live`.
//!
//! Zero dependencies by design — this repo already hand-rolls its TCP change
//! stream and WebSocket handshake, everything runs on a current-thread
//! `LocalSet` (no hyper/axum multi-thread assumptions), and the exporter needs
//! only gauges/counters. The registry is `Arc`-shared, never global: benches
//! and tests run several nodes in one process and must not merge their metrics.
//!
//! Readiness (`/ready`, 200/503) is component-based: a node flips ready when
//! every component of its role has completed (snapshot restored, change-stream
//! caught up, listener bound, …). Crash-only exits need no unready path — a
//! dead process refuses connections, which health checks treat correctly.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

/// The node role, labelled on every metric line.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Role {
    SingleNode,
    Replicator,
    ViewSyncer,
}

impl Role {
    fn as_str(&self) -> &'static str {
        match self {
            Role::SingleNode => "single-node",
            Role::Replicator => "replicator",
            Role::ViewSyncer => "view-syncer",
        }
    }
}

/// A component of readiness. Components not applicable to a role are marked
/// ready at construction (see [`Metrics::new`]).
#[derive(Clone, Copy, Debug)]
pub enum ReadyComponent {
    /// Snapshot restored (view-syncer) / initial sync + first snapshot written
    /// (replicator) / initial sync done (single-node).
    Restored = 0,
    /// Change-stream caught up to the replicator's head (view-syncer only).
    CaughtUp = 1,
    /// The serving listener is bound (WS listener; change-stream listener for
    /// the replicator).
    ListenerBound = 2,
}

/// Per-node metrics registry: plain atomics, shared via `Arc`, updated from
/// `LocalSet` tasks and rendered on demand.
pub struct Metrics {
    pub role: Role,
    ready: AtomicBool,
    ready_components: [AtomicBool; 3],

    // Replica.
    pub replica_rows: AtomicU64,
    pub replica_logical_bytes: AtomicU64,
    pub replica_sqlite_file_bytes: AtomicU64,
    pub replica_pos: AtomicU64,

    // Snapshots.
    pub snapshot_bytes: AtomicU64,
    pub snapshot_restore_peak_rss_bytes: AtomicU64,

    // Change stream (replicator).
    pub change_ring_entries: AtomicU64,
    pub change_ring_bytes: AtomicU64,
    pub change_stream_seq: AtomicU64,
    pub change_stream_subscribers: AtomicU64,

    // Durable change log (replicator).
    pub changelog_queue_depth: AtomicU64,
    pub changelog_queue_bytes: AtomicU64,

    // Serving (view-syncer / single-node).
    pub connected_clients: AtomicU64,
    pub active_queries: AtomicU64,
    pub matched_rows: AtomicU64,
    pub hydration_bytes_total: AtomicU64,
    pub poke_bytes_total: AtomicU64,
    pub poke_parts_total: AtomicU64,
    pub pokes_total: AtomicU64,
}

impl Metrics {
    pub fn new(role: Role) -> Arc<Metrics> {
        let m = Metrics {
            role,
            ready: AtomicBool::new(false),
            ready_components: Default::default(),
            replica_rows: Default::default(),
            replica_logical_bytes: Default::default(),
            replica_sqlite_file_bytes: Default::default(),
            replica_pos: Default::default(),
            snapshot_bytes: Default::default(),
            snapshot_restore_peak_rss_bytes: Default::default(),
            change_ring_entries: Default::default(),
            change_ring_bytes: Default::default(),
            change_stream_seq: Default::default(),
            change_stream_subscribers: Default::default(),
            changelog_queue_depth: Default::default(),
            changelog_queue_bytes: Default::default(),
            connected_clients: Default::default(),
            active_queries: Default::default(),
            matched_rows: Default::default(),
            hydration_bytes_total: Default::default(),
            poke_bytes_total: Default::default(),
            pokes_total: Default::default(),
            poke_parts_total: Default::default(),
        };
        // CaughtUp only gates the view-syncer (the other roles ARE the source
        // of truth once synced).
        if role != Role::ViewSyncer {
            m.ready_components[ReadyComponent::CaughtUp as usize].store(true, Ordering::Release);
        }
        Arc::new(m)
    }

    /// Mark a readiness component complete; `ready` flips once all are.
    pub fn mark_ready(&self, c: ReadyComponent) {
        self.ready_components[c as usize].store(true, Ordering::Release);
        let all = self.ready_components.iter().all(|c| c.load(Ordering::Acquire));
        if all && !self.ready.swap(true, Ordering::AcqRel) {
            eprintln!("{}: READY", self.role.as_str());
        }
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    /// Render in Prometheus text exposition format. Process gauges (RSS) are
    /// sampled at render time.
    pub fn render_prometheus(&self) -> String {
        let role = self.role.as_str();
        let mut out = String::with_capacity(2048);
        let mut gauge = |name: &str, help: &str, v: u64| {
            out.push_str(&format!(
                "# HELP {name} {help}\n# TYPE {name} {ty}\n{name}{{role=\"{role}\"}} {v}\n",
                ty = if name.ends_with("_total") { "counter" } else { "gauge" },
            ));
        };
        gauge("orbit_ready", "1 once startup (restore, catch-up, bind) completed", self.is_ready() as u64);
        gauge("orbit_replica_rows", "Rows held by the local replica", self.replica_rows.load(Ordering::Relaxed));
        gauge("orbit_replica_logical_bytes", "Estimated logical bytes of replicated rows (in-memory replica)", self.replica_logical_bytes.load(Ordering::Relaxed));
        gauge("orbit_replica_sqlite_file_bytes", "Size of the SQLite replica database", self.replica_sqlite_file_bytes.load(Ordering::Relaxed));
        gauge("orbit_replica_pos", "Applied change-stream position", self.replica_pos.load(Ordering::Relaxed));
        gauge("orbit_snapshot_bytes", "Size of the last written/restored snapshot", self.snapshot_bytes.load(Ordering::Relaxed));
        gauge("orbit_snapshot_restore_peak_rss_bytes", "Peak RSS observed during snapshot restore", self.snapshot_restore_peak_rss_bytes.load(Ordering::Relaxed));
        gauge("orbit_change_ring_entries", "Events in the change-stream ring", self.change_ring_entries.load(Ordering::Relaxed));
        gauge("orbit_change_ring_bytes", "Estimated bytes in the change-stream ring", self.change_ring_bytes.load(Ordering::Relaxed));
        gauge("orbit_change_stream_seq", "Last published change-stream sequence", self.change_stream_seq.load(Ordering::Relaxed));
        gauge("orbit_change_stream_subscribers", "Live change-stream subscribers", self.change_stream_subscribers.load(Ordering::Relaxed));
        gauge("orbit_changelog_queue_depth", "Durable change-log queued events", self.changelog_queue_depth.load(Ordering::Relaxed));
        gauge("orbit_changelog_queue_bytes", "Durable change-log queued estimated bytes", self.changelog_queue_bytes.load(Ordering::Relaxed));
        gauge("orbit_connected_clients", "Connected WebSocket clients", self.connected_clients.load(Ordering::Relaxed));
        gauge("orbit_active_queries", "Materialized client queries", self.active_queries.load(Ordering::Relaxed));
        gauge("orbit_matched_rows", "Rows referenced by client views", self.matched_rows.load(Ordering::Relaxed));
        gauge("orbit_hydration_bytes_total", "Serialized bytes sent in initial-subscribe pokes", self.hydration_bytes_total.load(Ordering::Relaxed));
        gauge("orbit_poke_bytes_total", "Serialized bytes sent in all pokes", self.poke_bytes_total.load(Ordering::Relaxed));
        gauge("orbit_poke_parts_total", "pokePart frames sent", self.poke_parts_total.load(Ordering::Relaxed));
        gauge("orbit_pokes_total", "Poke transactions sent", self.pokes_total.load(Ordering::Relaxed));
        if let Some(rss) = rss_bytes() {
            gauge("orbit_process_rss_bytes", "Resident set size (VmRSS)", rss);
        }
        out
    }
}

thread_local! {
    /// The node's metrics registry, scoped to its serving thread. Every Orbit
    /// node runs its whole serving path (accept loop + connections + pumps) on
    /// ONE current-thread `LocalSet` thread — so a thread-local is exactly
    /// node-scoped, and in-process multi-node tests/benches (one thread per
    /// node) never merge registries. Set once at role startup.
    static NODE_METRICS: std::cell::RefCell<Option<Arc<Metrics>>> = const { std::cell::RefCell::new(None) };
}

/// Install `m` as this serving thread's registry (call from the role's inner
/// fn, on the `LocalSet` thread, before serving).
pub fn set_node_metrics(m: Arc<Metrics>) {
    NODE_METRICS.with(|n| *n.borrow_mut() = Some(m));
}

/// The serving thread's registry, if one was installed.
pub fn node_metrics() -> Option<Arc<Metrics>> {
    NODE_METRICS.with(|n| n.borrow().clone())
}

/// Resident set size from `/proc/self/status` (`VmRSS`). Linux-only — Orbit
/// nodes run in containers; returns `None` elsewhere (macOS dev machines).
#[cfg(target_os = "linux")]
pub fn rss_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|l| l.starts_with("VmRSS:"))?;
    let kb: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kb * 1024)
}

#[cfg(not(target_os = "linux"))]
pub fn rss_bytes() -> Option<u64> {
    None
}

/// The metrics/health listen address from `ORBIT_METRICS_LISTEN`. `None` when
/// unset or empty/`off` — library entry points serve metrics only when asked
/// (tests and benches run several nodes per process; they must not fight over
/// one port). The binaries default the env to `0.0.0.0:9090`.
pub fn metrics_listen_from_env() -> Option<String> {
    match std::env::var("ORBIT_METRICS_LISTEN") {
        Ok(v) if !v.trim().is_empty() && v.trim() != "off" => Some(v.trim().to_string()),
        _ => None,
    }
}

/// Serve `GET /metrics` (Prometheus text), `GET /ready` (200 once startup
/// completed, else 503), and `GET /live` (always 200) on `addr`. HTTP/1.0
/// style: read the request line, respond, close. Runs on the `LocalSet`.
pub async fn serve_metrics(addr: String, m: Arc<Metrics>) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    eprintln!("metrics listening on {addr} (/metrics /ready /live)");
    loop {
        let (sock, _) = listener.accept().await?;
        let m = m.clone();
        tokio::spawn(async move {
            let _ = handle_http(sock, &m).await;
        });
    }
}

async fn handle_http(sock: tokio::net::TcpStream, m: &Metrics) -> anyhow::Result<()> {
    let (r, mut w) = sock.into_split();
    let mut reader = BufReader::new(r);
    let mut line = String::new();
    tokio::time::timeout(std::time::Duration::from_secs(5), reader.read_line(&mut line)).await??;
    let path = line.split_whitespace().nth(1).unwrap_or("/");
    let (status, content_type, body) = match path.split('?').next().unwrap_or("/") {
        "/metrics" => ("200 OK", "text/plain; version=0.0.4", m.render_prometheus()),
        "/ready" => {
            if m.is_ready() {
                ("200 OK", "text/plain", "ready\n".to_string())
            } else {
                ("503 Service Unavailable", "text/plain", "starting\n".to_string())
            }
        }
        "/live" => ("200 OK", "text/plain", "live\n".to_string()),
        _ => ("404 Not Found", "text/plain", "not found\n".to_string()),
    };
    let resp = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    w.write_all(resp.as_bytes()).await?;
    w.shutdown().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_has_ready_and_counters() {
        let m = Metrics::new(Role::ViewSyncer);
        m.poke_bytes_total.store(1234, Ordering::Relaxed);
        let text = m.render_prometheus();
        assert!(text.contains("orbit_ready{role=\"view-syncer\"} 0"));
        assert!(text.contains("# TYPE orbit_poke_bytes_total counter"));
        assert!(text.contains("orbit_poke_bytes_total{role=\"view-syncer\"} 1234"));
    }

    #[test]
    fn readiness_requires_all_components() {
        let m = Metrics::new(Role::ViewSyncer);
        m.mark_ready(ReadyComponent::Restored);
        m.mark_ready(ReadyComponent::ListenerBound);
        assert!(!m.is_ready(), "view-syncer not ready before catch-up");
        m.mark_ready(ReadyComponent::CaughtUp);
        assert!(m.is_ready());

        // Non-view-syncer roles don't wait for CaughtUp.
        let r = Metrics::new(Role::Replicator);
        r.mark_ready(ReadyComponent::Restored);
        r.mark_ready(ReadyComponent::ListenerBound);
        assert!(r.is_ready());
    }

    #[tokio::test]
    async fn http_responder_serves_ready_transitions() {
        let m = Metrics::new(Role::SingleNode);
        let addr = "127.0.0.1:39790";
        {
            let m = m.clone();
            tokio::spawn(serve_metrics(addr.to_string(), m));
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        async fn get(addr: &str, path: &str) -> String {
            use tokio::io::AsyncReadExt;
            let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
            s.write_all(format!("GET {path} HTTP/1.1\r\nHost: x\r\n\r\n").as_bytes()).await.unwrap();
            let mut buf = String::new();
            s.read_to_string(&mut buf).await.unwrap();
            buf
        }

        assert!(get(addr, "/ready").await.starts_with("HTTP/1.1 503"));
        assert!(get(addr, "/live").await.starts_with("HTTP/1.1 200"));
        m.mark_ready(ReadyComponent::Restored);
        m.mark_ready(ReadyComponent::ListenerBound);
        m.mark_ready(ReadyComponent::CaughtUp);
        assert!(get(addr, "/ready").await.starts_with("HTTP/1.1 200"));
        let metrics = get(addr, "/metrics").await;
        assert!(metrics.contains("orbit_ready{role=\"single-node\"} 1"));
    }
}
