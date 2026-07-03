---
"@zeronsh/orbit": patch
---

Durability + robustness review pass (two adversarial audits against Zero's
invariants): real transactions and crash-consistent resume for the durable
replica, ack-after-durable WAL discipline, pipeline lifecycle without leaks,
consumed (not poison) mutation errors, and a staleness gate for cross-node
resume.

**Durable SQLite replica is now actually durable.**
- All tables share ONE database (`dir/replica.db`); each upstream Postgres
  transaction applies inside a single SQLite transaction — a crash rolls back
  instead of persisting a torn half-transaction (previously: per-table DB files
  + autocommit per event).
- The commit LSN is recorded inside that same transaction as a resume
  watermark; on boot a durable replica resumes from the slot (skipping
  re-delivered transactions) instead of re-running a full initial sync.
- A fresh initial sync clears the replica first: rows deleted upstream while
  offline no longer survive as phantoms (the sync only upserts). The whole
  sync is one transaction.

**WAL acknowledgement follows durable commits, not receipt.** The replication
stream acks only the consumer-confirmed position: the SQLite replica confirms
per committed transaction; the multinode replicator confirms the change-log
writer's durably-flushed watermark. The change-log writer now retries failed
batches instead of dropping them — a dropped batch was a silent,
contiguous-looking hole that delta-resuming view-syncers skipped over
(divergence). Corrupt/unreadable log entries now truncate the read (→ Reset)
instead of becoming silent no-ops with valid positions.

**Pipeline lifecycle: weak output links.** Operators own their inputs; outputs
are now `Weak`. Dropping a query's terminal unravels the whole chain, and
sources prune dead connections on push (reusing their slots). Previously every
churned query leaked its full pipeline forever — receiving every future
change, growing CPU and memory without bound. Removing a query also now emits
`del` patches for rows no other query provides (previously the refcounts
leaked, permanently suppressing later genuine deletes of shared rows).

**Mutations fail loud, not poison.** A failing or unknown mutation on the
direct-write path is consumed (lastMutationID advances) with a `MutationFailed`
error to the client — previously any PG error killed the socket, and the
client's replay made it a permanent reconnect storm; an unknown mutator was
silently dropped while still acked. Number literals are range-checked
(no silent `1e20 → i64::MAX` corruption; non-finite → NULL).

**Cross-node resume hardening.** The CVR records the stream position its view
reflects and is loaded in a single statement (no rows/version tear under
concurrent writers). A node whose replica is behind a connecting client's view
waits briefly for catch-up, then falls back to a full resync — never the fast
delta (which could suppress rows against a stale replica). Stale ephemeral
CVRs are garbage-collected after 7 days. View-syncers apply change-stream
transactions atomically (hydrations can no longer observe torn
mid-transaction state), page through the durable log on catch-up instead of
resetting past one page, and exit after repeated decode failures at a frozen
watermark instead of reconnect-looping while serving ever-staler data.

**Misc hardening:** release builds abort on panic (a panicked shard task
restarts the process instead of silently serving stale data); the app-endpoint
forwarder has timeouts; snapshot writes fsync before rename and use unique
temp names (concurrent writers during deploy overlap); replication frames are
length-sanity-checked; the client survives a rejected `auth()` (previously
stuck permanently with no reconnect); `lastMutationID`s are seeded from
Postgres at boot so pre-restart mutations still ack to reconnecting clients.
