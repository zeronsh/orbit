//! The change-stream: the **replicator** owns the single Postgres slot and
//! broadcasts its decoded [`LogicalEvent`]s (tagged with the WAL LSN) to any
//! number of **view-syncer** nodes, so they never open their own slot. A
//! view-syncer resumes from the LSN it last applied; if that point is no longer
//! retained in the replicator's ring buffer, the server replies [`ChangeMsg::Reset`]
//! and the view-syncer re-restores from the latest object-store snapshot.
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

struct Ring {
    buf: VecDeque<(u64, LogicalEvent)>,
    /// Monotonic sequence assigned to the last published event.
    seq: u64,
    /// Highest position evicted from the ring; a resume `<` this requires a
    /// re-snapshot.
    floor: u64,
}

/// The replicator's change broadcaster: a bounded ring of recent changes plus a
/// live fan-out to connected view-syncers.
pub struct ChangeStreamServer {
    tx: broadcast::Sender<(u64, LogicalEvent)>,
    ring: Mutex<Ring>,
    cap: usize,
    /// Optional durable change-log. When present, evicted resume points are served
    /// by delta from it instead of forcing a re-restore, and every change is
    /// appended (non-blocking) for cross-restart durability.
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
        // The broadcast channel only covers LIVE fan-out lag — tokio pre-allocates
        // the full ring up front, so sizing it to the (1M) history `cap` wastes
        // ~100MB of empty buffer. A subscriber that lags past this gets `Lagged` →
        // `Reset` → resumes from the durable log or the in-memory ring backlog (the
        // `VecDeque`, which grows on demand and keeps the full `cap` history for
        // free). 64K live lag is plenty.
        const BROADCAST_CAP: usize = 1 << 16;
        let (tx, _) = broadcast::channel(BROADCAST_CAP.min(cap.max(64)));
        Arc::new(ChangeStreamServer {
            tx,
            ring: Mutex::new(Ring { buf: VecDeque::new(), seq: start_seq, floor: start_seq }),
            cap,
            log,
        })
    }

    /// Record + broadcast a change, assigning it the next sequence number, and
    /// (non-blocking) append it to the durable log. `lsn` is the change's WAL
    /// position, stored so the replicator can resume + dedup across restarts.
    /// Called by the replication pump per event.
    pub fn publish(&self, lsn: u64, event: LogicalEvent) {
        let pos = {
            let mut r = self.ring.lock().unwrap();
            r.seq += 1;
            let pos = r.seq;
            r.buf.push_back((pos, event.clone()));
            while r.buf.len() > self.cap {
                if let Some((evicted, _)) = r.buf.pop_front() {
                    r.floor = r.floor.max(evicted);
                }
            }
            pos
        };
        if let Some(log) = &self.log {
            log.append(pos, lsn, event.clone()); // off the hot path: just a channel send
        }
        let _ = self.tx.send((pos, event)); // ok if no subscribers
    }

    /// The sequence number of the last published event (for snapshot watermarks).
    pub fn current_seq(&self) -> u64 {
        self.ring.lock().unwrap().seq
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
                r.buf.iter().filter(|(l, _)| *l > resume).cloned().collect::<Vec<_>>(),
                r.floor,
                r.seq,
            )
        };

        // Ahead of our sequence → the replicator restarted with a lower seq, so this
        // resume point is "in the future" and can't be served. Re-restore.
        if resume > seq {
            send(&mut w, &ChangeMsg::Reset).await?;
            return Ok(());
        }

        let mut last = resume;

        // Resume point evicted from the in-memory ring (or the ring is empty after a
        // restart): bridge (resume, floor] from the durable change-log so the
        // view-syncer resumes by *delta* instead of re-restoring the whole replica.
        // Any hole / pruned-past / too-far-behind falls through to a Reset.
        if resume < floor {
            const MAX_CATCHUP: i64 = 200_000;
            if let Some(log) = self.log.as_ref() {
                let (_min, events) = log.read_after(resume, MAX_CATCHUP).await?;
                for (pos, event) in events {
                    if pos > floor {
                        break; // the ring backlog takes over from here
                    }
                    if pos != last + 1 {
                        break; // hole in the log → fall through to Reset
                    }
                    send(&mut w, &ChangeMsg::Change { pos, event }).await?;
                    last = pos;
                }
            }
            if last < floor {
                // couldn't bridge to the ring (no log / pruned / hole / too far behind)
                send(&mut w, &ChangeMsg::Reset).await?;
                return Ok(());
            }
        }

        // Ring backlog: events in (last, seq].
        for (pos, event) in backlog {
            if pos > last {
                send(&mut w, &ChangeMsg::Change { pos, event }).await?;
                last = pos;
            }
        }
        loop {
            match rx.recv().await {
                Ok((pos, event)) if pos > last => {
                    send(&mut w, &ChangeMsg::Change { pos, event }).await?;
                    last = pos;
                }
                Ok(_) => {} // already covered by the backlog
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    send(&mut w, &ChangeMsg::Reset).await?;
                    return Ok(());
                }
                Err(broadcast::error::RecvError::Closed) => return Ok(()),
            }
        }
    }
}

async fn send(w: &mut OwnedWriteHalf, msg: &ChangeMsg) -> Result<()> {
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
