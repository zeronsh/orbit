---
"@zeronsh/orbit": patch
---

Correctness/robustness fixes from a differential audit against Zero (`zql`), plus
regression coverage. No API changes.

- **Client string ordering now matches the server (UTF-8 / code-point order).**
  `compareValues` compared strings with JS `<` (UTF-16 code units), which mis-orders
  non-BMP characters (emoji, supplementary CJK) relative to the Rust server's
  `compare_utf8` (byte order) — so `orderBy` and range filters (`<`,`<=`,`>`,`>=`)
  could disagree with the authoritative result until the next poke. It now iterates
  code points. (Mirrors Zero PR #6088.)

- **A too-large "poison" mutation no longer reconnect-loops forever.** On WebSocket
  close 1009 (message too big) the client dropped straight into its reconnect/resend
  loop, re-sending the oversized (persisted) mutation every time and wedging the whole
  queue. It now drops the offending mutation and reports it via `onError`. (Zero #5982.)

- **The replicator recovers from a silently-dead Postgres stream.** The logical-
  replication read had no inbound-liveness bound, so a half-open connection (acks
  flowing into the void) hung forever. An idle read-timeout now surfaces the stall so
  the existing reconnect-and-resume path takes over. (Zero #6047.)

- **The direct-write mutation path deduplicates re-delivered mutations** (skips ids at
  or below the client's recorded `lastMutationID`) so a reconnect replay can't
  double-apply non-idempotent ops.

Also verified (via a 5,800-scenario fuzz sweep generated from Zero and ground-truthed
against SQLite) that Orbit's query engine matches Zero everywhere except a Zero bug in
nested correlated `EXISTS(… NOT EXISTS …)`, where Orbit is the SQL-correct side; a
regression test locks that behavior in.
