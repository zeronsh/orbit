//! Change-stream resume semantics across a replicator restart.
//!
//! Regression for the multinode "works then reverts" bug: the change-stream `seq`
//! used to reset to 0 on replicator restart, so a view-syncer resuming from an
//! older snapshot watermark had a resume point "in the future" and every new
//! change was silently dropped (`pos > last` filtered them out). The fixes:
//!   - `new_at` continues the sequence from the snapshot watermark across restarts,
//!   - `serve` emits `Reset` when a resume point is *ahead* of our sequence (a
//!     restart we can't serve), not only when it's too old.

use std::time::Duration;

use orbit_cache::{ChangeMsg, ChangeStreamClient, ChangeStreamServer, LogicalEvent};

#[tokio::test]
async fn change_stream_resume_survives_restart() {
    // A replicator that restarted and resumed its sequence at snapshot watermark 62
    // (positions continue, they don't reset to 0).
    let server = ChangeStreamServer::new_at(64, 62);
    let addr = "127.0.0.1:47731";
    {
        let server = server.clone();
        tokio::spawn(async move {
            let _ = server.serve(addr).await;
        });
    }
    tokio::time::sleep(Duration::from_millis(150)).await;

    // A change after the restart gets the *next* position (63), not 1. (The lsn arg
    // only matters when a durable log is attached; this server has none.)
    server.publish(1000, LogicalEvent::Commit);

    // (a) Resuming exactly at the snapshot watermark receives the new change — no
    //     reset, no dropped update. This is the path that used to lose everything.
    let mut at = ChangeStreamClient::connect(addr, 62).await.unwrap();
    match at.next().await.unwrap() {
        Some(ChangeMsg::Change { pos, event }) => {
            assert_eq!(pos, 63, "resume@62 should receive the next event at 63");
            assert_eq!(event, LogicalEvent::Commit);
        }
        other => panic!("expected Change@63, got {other:?}"),
    }

    // (b) Resuming older than the snapshot (below the floor) → re-restore.
    let mut behind = ChangeStreamClient::connect(addr, 50).await.unwrap();
    assert!(
        matches!(behind.next().await.unwrap(), Some(ChangeMsg::Reset)),
        "resume@50 (< floor 62) must Reset"
    );

    // (c) Resuming ahead of our sequence (watermark from a session that advanced
    //     past where this instance resumed) → re-restore. Without the `resume > seq`
    //     check this returned nothing and the view-syncer went silently deaf.
    let mut ahead = ChangeStreamClient::connect(addr, 100).await.unwrap();
    assert!(
        matches!(ahead.next().await.unwrap(), Some(ChangeMsg::Reset)),
        "resume@100 (> seq) must Reset"
    );
}
