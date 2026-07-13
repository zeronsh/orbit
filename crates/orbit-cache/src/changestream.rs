//! The change-stream: the **replicator** owns the single Postgres slot and
//! broadcasts its decoded [`LogicalEvent`]s (tagged with the WAL LSN) to any
//! number of **view-syncer** nodes, so they never open their own slot. A
//! view-syncer resumes from the LSN it last applied; if that point is no longer
//! retained in the replicator's ring buffer, the server replies [`ChangeMsg::Reset`]
//! and the view-syncer re-restores from the latest object-store snapshot.
//!
//! Memory: events are shared as `Arc<LogicalEvent>` between the ring, the
//! durable-log queue, and the broadcast channel (no deep clones), and the ring
//! is bounded by **total estimated bytes** (primary) as well as event count
//! (secondary). Aggressive byte eviction is safe: resume points that fall out
//! of the ring are served by delta from the durable change-log.
//!
//! Wire format: newline-delimited JSON. The client first sends one line with its
//! resume LSN; the server then streams [`ChangeMsg`]s.

use crate::LogicalEvent;
use anyhow::Result;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;

/// A message streamed from replicator to view-syncer. `pos` is the replicator's
/// monotonic per-event sequence number (NOT the WAL LSN — several events share an
/// LSN within a transaction, so the LSN can't order/resume at event granularity).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum ChangeMsg {
    Change { pos: u64, event: LogicalEvent },
    /// The requested resume point is no longer retained — re-snapshot and retry.
    Reset,
}

/// Server-side serialization mirror of [`ChangeMsg`] that borrows the event
/// instead of owning it, so Arc-shared events go onto the wire without a deep
/// clone. serde's externally-tagged encoding makes this byte-identical to
/// `ChangeMsg` (pinned by a round-trip test below).
#[derive(serde::Serialize)]
enum ChangeMsgOut<'a> {
    Change { pos: u64, event: &'a LogicalEvent },
    Reset,
}

/// Ring + broadcast tuning. Defaults come from `ORBIT_CHANGE_*` env vars via
/// [`ChangeStreamConfig::from_env`].
#[derive(Clone, Copy, Debug)]
pub struct ChangeStreamConfig {
    /// Ring count cap (secondary bound).
    pub max_events: usize,
    /// Ring byte cap (primary bound, estimated event bytes).
    pub max_bytes: usize,
    /// Broadcast channel slots (live fan-out lag window).
    pub broadcast_cap: usize,
}

impl Default for ChangeStreamConfig {
    fn default() -> Self {
        ChangeStreamConfig {
            max_events: 65_536,
            max_bytes: 64 << 20, // 64 MiB
            broadcast_cap: 16_384,
        }
    }
}

impl ChangeStreamConfig {
    /// Read `ORBIT_CHANGE_RING_CAPACITY` / `ORBIT_CHANGE_RING_BYTES` /
    /// `ORBIT_BROADCAST_CAP`, falling back to defaults.
    pub fn from_env() -> Self {
        fn env_usize(name: &str, default: usize) -> usize {
            match std::env::var(name) {
                Ok(v) => v.trim().parse().unwrap_or_else(|_| {
                    eprintln!("change-stream: ignoring unparsable {name}={v:?}");
                    default
                }),
                Err(_) => default,
            }
        }
        let d = ChangeStreamConfig::default();
        ChangeStreamConfig {
            max_events: env_usize("ORBIT_CHANGE_RING_CAPACITY", d.max_events),
            max_bytes: env_usize("ORBIT_CHANGE_RING_BYTES", d.max_bytes),
            broadcast_cap: env_usize("ORBIT_BROADCAST_CAP", d.broadcast_cap),
        }
    }
}

struct Ring {
    /// `(pos, event, estimated bytes)` — the size is carried so eviction can
    /// maintain `bytes` without re-walking events.
    buf: VecDeque<(u64, Arc<LogicalEvent>, usize)>,
    /// Running sum of the `estimated bytes` field over `buf`.
    bytes: usize,
    /// Monotonic sequence assigned to the last published event.
    seq: u64,
    /// Highest position evicted from the ring; a resume `<` this requires the
    /// durable-log bridge (or a re-snapshot).
    floor: u64,
}

/// The replicator's change broadcaster: a byte-bounded ring of recent changes
/// plus a live fan-out to connected view-syncers.
pub struct ChangeStreamServer {
    tx: broadcast::Sender<(u64, Arc<LogicalEvent>)>,
    ring: Mutex<Ring>,
    cap: usize,
    max_bytes: usize,
    /// Optional durable change-log. When present, evicted resume points are served
    /// by delta from it instead of forcing a re-restore, and every change is
    /// appended (with byte-budget backpressure) for cross-restart durability.
    log: Option<Arc<crate::changelog::PgChangeLog>>,
}

impl ChangeStreamServer {
    pub fn new(cap: usize) -> Arc<Self> {
        Self::new_at(cap, 0)
    }

    /// Like [`new`], but start the sequence at `start_seq` instead of 0. The
    /// replicator passes the latest snapshot watermark here so the change-stream
    /// position is *continuous across process restarts* — otherwise a restart
    /// resets `seq` to 0 and every view-syncer's resume point becomes "in the
    /// future", silently dropping new changes. `floor = start_seq` so resume
    /// points older than the snapshot correctly trigger a re-restore.
    pub fn new_at(cap: usize, start_seq: u64) -> Arc<Self> {
        Self::new_with_log(cap, start_seq, None)
    }

    /// As [`new_at`], but backed by a durable change-log (the replicator's path).
    pub fn new_with_log(
        cap: usize,
        start_seq: u64,
        log: Option<Arc<crate::changelog::PgChangeLog>>,
    ) -> Arc<Self> {
        let cfg = ChangeStreamConfig { max_events: cap, ..Default::default() };
        Self::with_config(cfg, start_seq, log)
    }

    /// Fully-configured constructor (see [`ChangeStreamConfig`]).
    pub fn with_config(
        cfg: ChangeStreamConfig,
        start_seq: u64,
        log: Option<Arc<crate::changelog::PgChangeLog>>,
    ) -> Arc<Self> {
        // The broadcast channel only covers LIVE fan-out lag — tokio pre-allocates
        // the full ring up front. With Arc'd events each slot is a 16-byte tuple
        // (plus slot bookkeeping), so 16K slots ≈ ~1.3 MB pre-allocated. While an
        // event is still in the history ring its broadcast slot pins the *same*
        // allocation, so broadcast retention is nearly free up to the ring's
        // window; capping it at or below the ring count keeps uniquely-pinned
        // memory bounded. A subscriber that lags past this gets `Lagged` →
        // `Reset` → resumes from the durable log or re-restores.
        let (tx, _) = broadcast::channel(cfg.broadcast_cap.min(cfg.max_events.max(64)).max(64));
        Arc::new(ChangeStreamServer {
            tx,
            ring: Mutex::new(Ring {
                buf: VecDeque::new(),
                bytes: 0,
                seq: start_seq,
                floor: start_seq,
            }),
            cap: cfg.max_events,
            max_bytes: cfg.max_bytes,
            log,
        })
    }

    /// Record + broadcast a change, assigning it the next sequence number, and
    /// append it to the durable log (awaiting when the log's byte budget is
    /// full — the intended backpressure that parks the replication pump; see
    /// the changelog module doc). `lsn` is the change's WAL position, stored so
    /// the replicator can resume + dedup across restarts. Called by the
    /// replication pump per event. The event is shared, not cloned: one
    /// allocation serves the ring, the log queue, and every subscriber.
    pub async fn publish(&self, lsn: u64, event: LogicalEvent) {
        let event = Arc::new(event);
        let est = event.estimated_bytes();
        let pos = {
            let mut r = self.ring.lock().unwrap();
            r.seq += 1;
            let pos = r.seq;
            r.buf.push_back((pos, Arc::clone(&event), est));
            r.bytes += est;
            // Byte cap (primary) + count cap (secondary). `len() > 1` keeps a
            // just-published oversized event servable until the next publish
            // (otherwise floor == seq on every publish of a big event).
            //
            // Note: an evicted pos may not be durably flushed to the change-log
            // yet (the log writer is async) — a resumer bridging right then
            // finds a gap and Resets. That race predates byte eviction; byte-
            // bounded log batches keep the flush latency small.
            while r.buf.len() > 1 && (r.buf.len() > self.cap || r.bytes > self.max_bytes) {
                if let Some((evicted, _, sz)) = r.buf.pop_front() {
                    r.bytes -= sz;
                    r.floor = r.floor.max(evicted);
                }
            }
            pos
        };
        if let Some(log) = &self.log {
            log.append(pos, lsn, Arc::clone(&event)).await;
        }
        let _ = self.tx.send((pos, event)); // ok if no subscribers
    }

    /// The sequence number of the last published event (for snapshot watermarks).
    pub fn current_seq(&self) -> u64 {
        self.ring.lock().unwrap().seq
    }

    /// Current ring occupancy as `(events, estimated_bytes)` (for metrics).
    pub fn ring_stats(&self) -> (usize, usize) {
        let r = self.ring.lock().unwrap();
        (r.buf.len(), r.bytes)
    }

    /// Accept view-syncer connections forever.
    pub async fn serve(self: Arc<Self>, addr: &str) -> Result<()> {
        let listener = TcpListener::bind(addr).await?;
        eprintln!("change-stream listening on {addr}");
        loop {
            let (sock, _) = listener.accept().await?;
            let me = self.clone();
            tokio::spawn(async move {
                if let Err(e) = me.handle(sock).await {
                    eprintln!("change-stream client ended: {e:#}");
                }
            });
        }
    }

    async fn handle(&self, sock: TcpStream) -> Result<()> {
        let (r, mut w) = sock.into_split();
        let mut reader = BufReader::new(r);
        let mut line = String::new();
        reader.read_line(&mut line).await?;
        let resume: u64 = line.trim().parse().unwrap_or(0);

        // Subscribe before reading the ring so no change slips through the gap.
        let mut rx = self.tx.subscribe();
        let (backlog, floor, seq) = {
            let r = self.ring.lock().unwrap();
            (
                // Arc clones: 24 bytes + a refcount bump per entry, not a deep copy.
                r.buf
                    .iter()
                    .filter(|(l, _, _)| *l > resume)
                    .map(|(l, e, _)| (*l, Arc::clone(e)))
                    .collect::<Vec<_>>(),
                r.floor,
                r.seq,
            )
        };

        // Ahead of our sequence → the replicator restarted with a lower seq, so this
        // resume point is "in the future" and can't be served. Re-restore.
        if resume > seq {
            send(&mut w, &ChangeMsgOut::Reset).await?;
            return Ok(());
        }

        let mut last = resume;

        // Resume point evicted from the in-memory ring (or the ring is empty after a
        // restart): bridge (resume, floor] from the durable change-log so the
        // view-syncer resumes by *delta* instead of re-restoring the whole replica.
        // Any hole / pruned-past / too-far-behind falls through to a Reset.
        if resume < floor {
            // Paginated: bridge any gap the log retains (up to LOG_RETENTION),
            // not just one page — a syncer 200K+ behind used to Reset even
            // though its delta was durably available. Pages are capped by rows
            // AND bytes; a byte-cut short page just re-queries from `last`.
            const PAGE: i64 = 50_000;
            const PAGE_BYTES: usize = 8 << 20; // 8 MiB of stored JSON per page
            if let Some(log) = self.log.as_ref() {
                'bridge: while last < floor {
                    let (_min, events) = log.read_after(last, PAGE, PAGE_BYTES).await?;
                    if events.is_empty() {
                        break; // nothing more in the log → Reset check below
                    }
                    for (pos, event) in events {
                        if pos > floor {
                            break 'bridge; // the ring backlog takes over from here
                        }
                        if pos != last + 1 {
                            break 'bridge; // hole / unreadable entry → Reset
                        }
                        send(&mut w, &ChangeMsgOut::Change { pos, event: &event }).await?;
                        last = pos;
                    }
                }
            }
            if last < floor {
                // couldn't bridge to the ring (no log / pruned / hole / too far behind)
                send(&mut w, &ChangeMsgOut::Reset).await?;
                return Ok(());
            }
        }

        // Ring backlog: events in (last, seq].
        for (pos, event) in backlog {
            if pos > last {
                send(&mut w, &ChangeMsgOut::Change { pos, event: &event }).await?;
                last = pos;
            }
        }
        loop {
            match rx.recv().await {
                Ok((pos, event)) if pos > last => {
                    send(&mut w, &ChangeMsgOut::Change { pos, event: &event }).await?;
                    last = pos;
                }
                Ok(_) => {} // already covered by the backlog
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    send(&mut w, &ChangeMsgOut::Reset).await?;
                    return Ok(());
                }
                Err(broadcast::error::RecvError::Closed) => return Ok(()),
            }
        }
    }
}

async fn send(w: &mut OwnedWriteHalf, msg: &ChangeMsgOut<'_>) -> Result<()> {
    let mut s = serde_json::to_string(msg)?;
    s.push('\n');
    w.write_all(s.as_bytes()).await?;
    Ok(())
}

/// View-syncer side: a connection to the replicator's change-stream.
pub struct ChangeStreamClient {
    reader: BufReader<OwnedReadHalf>,
    _write: OwnedWriteHalf, // kept alive to hold the socket open
}

impl ChangeStreamClient {
    /// Connect and request changes after `resume_lsn`.
    pub async fn connect(addr: &str, resume_lsn: u64) -> Result<Self> {
        let sock = TcpStream::connect(addr).await?;
        let (r, mut w) = sock.into_split();
        w.write_all(format!("{resume_lsn}\n").as_bytes()).await?;
        Ok(ChangeStreamClient { reader: BufReader::new(r), _write: w })
    }

    /// Next message, or `None` when the replicator closes the connection.
    pub async fn next(&mut self) -> Result<Option<ChangeMsg>> {
        let mut line = String::new();
        if self.reader.read_line(&mut line).await? == 0 {
            return Ok(None);
        }
        Ok(Some(serde_json::from_str(line.trim())?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `ChangeMsgOut` must stay byte-identical on the wire to `ChangeMsg` —
    /// it exists only to avoid deep-cloning Arc-shared events at serialization.
    #[test]
    fn change_msg_out_wire_format_matches_change_msg() {
        let ev = LogicalEvent::Commit;
        let out = serde_json::to_string(&ChangeMsgOut::Change { pos: 7, event: &ev }).unwrap();
        let owned: ChangeMsg = serde_json::from_str(&out).unwrap();
        match owned {
            ChangeMsg::Change { pos, event } => {
                assert_eq!(pos, 7);
                assert_eq!(event, LogicalEvent::Commit);
            }
            _ => panic!("wrong variant"),
        }
        assert_eq!(
            serde_json::to_string(&ChangeMsgOut::Reset).unwrap(),
            serde_json::to_string(&ChangeMsg::Reset).unwrap()
        );
        // Pin the exact wire shape too, so a serde attribute change can't
        // silently break cross-version node compatibility.
        assert_eq!(out, r#"{"Change":{"pos":7,"event":"Commit"}}"#);
    }

    fn fat_insert(payload: usize) -> LogicalEvent {
        let mut row = oql::value::Row::new();
        row.insert("id", oql::value::Value::Number(1.0));
        row.insert("body", oql::value::Value::String("x".repeat(payload)));
        LogicalEvent::Insert { table: "t".into(), row }
    }

    #[tokio::test]
    async fn byte_cap_evicts_and_advances_floor() {
        let cfg = ChangeStreamConfig {
            max_events: 1_000,
            max_bytes: 8 * 1024, // tiny: a few 1 KiB events
            broadcast_cap: 64,
        };
        let s = ChangeStreamServer::with_config(cfg, 0, None);
        for i in 0..32 {
            s.publish(i, fat_insert(1024)).await;
        }
        let (events, bytes) = s.ring_stats();
        assert!(bytes <= 8 * 1024 + 2048, "ring bytes {bytes} exceed cap+one-event");
        assert!(events < 32, "no eviction happened");
        let floor = { s.ring.lock().unwrap().floor };
        assert!(floor > 0, "floor did not advance on byte eviction");
    }

    #[tokio::test]
    async fn oversized_event_stays_until_next_publish() {
        let cfg = ChangeStreamConfig { max_events: 1_000, max_bytes: 1024, broadcast_cap: 64 };
        let s = ChangeStreamServer::with_config(cfg, 0, None);
        // One event far bigger than the whole byte cap: must stay resident
        // (len > 1 guard) so it can still be served.
        s.publish(1, fat_insert(64 * 1024)).await;
        assert_eq!(s.ring_stats().0, 1);
        // The next publish evicts it.
        s.publish(2, fat_insert(16)).await;
        assert_eq!(s.ring_stats().0, 1);
        let floor = { s.ring.lock().unwrap().floor };
        assert_eq!(floor, 1);
    }
}
