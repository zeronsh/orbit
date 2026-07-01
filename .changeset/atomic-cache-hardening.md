---
"@zeronsh/orbit": patch
---

Client cache + sync hardening pass: the IndexedDB cache is now crash-atomic,
single-writer across tabs, and cheaper; a mutation-id durability race is closed;
single-process servers can no longer serve silently-stale data.

- **Atomic (and faster) cache flush.** `KV` gains an optional `batch(ops)`;
  `IDBKV` implements it as ONE IndexedDB transaction and the store's flush now
  uses it — the whole flush (resync clear + rows + pending mutations + cookie)
  commits all-or-nothing. The cookie can no longer be ahead of the rows at *any*
  crash point, the row set can't tear mid-flush, and a large poke costs one
  IndexedDB commit instead of one transaction per key. Custom `KV`s without
  `batch` keep the previous carefully-ordered sequential path.

- **Multi-tab safety (Web Locks single-writer).** Two tabs used to share the
  restored `clientID` and write the same IndexedDB keyspace with no coordination —
  last-writer-wins could persist a cookie covering rows only the other tab wrote,
  and both tabs drove the same server-side client view. Now the first tab takes a
  Web Lock and owns persistence; later tabs run memory-only with their own fresh
  `clientID` (a full server sync — correct, just uncached). The lock releases on
  `close()` or tab death, so a reload elects a new leader. Environments without
  Web Locks are unchanged.

- **Mutation ids are durable before the push is sent.** The `nextMutationID`
  high-water mark was persisted fire-and-forget while the push went out
  immediately; if the tab died after the server recorded the id but before the
  write committed, a reload could reuse the id for a different mutation — which
  the server silently drops as already-processed. The push now goes out only
  after the id write is durable.

- **`IDBKV.entries(prefix)` scans only the prefix's key range** instead of
  materializing the whole store, making hydrate proportional to what it reads.

- **Single-process servers exit instead of freezing on replication errors.**
  `run_server`/`run_server_sharded` used to stop their replication pump on error
  and keep serving stale data forever. They now exit (crash-only, same policy as
  the view-syncer's Reset handling) so an orchestrator restart re-syncs fresh.
  The multinode replicator/view-syncer already reconnect and are unchanged.

Also normalizes a literal NUL byte in `store.ts` to its `\u0000` escape
(identical runtime value; the raw byte made tooling treat the file as binary).
