---
"@zeronsh/orbit": patch
---

Server: commit the per-client CVR (rows + version) atomically, and make schema
init concurrency-safe.

`commit_client_view` previously issued the row upserts, row deletes, and the
version write as three separate autocommit statements. A crash or a concurrent
checkpoint between them could leave `orbit_cvr_clients.version` inconsistent with
`orbit_cvr_client_rows` — the server-side mirror of the "cookie ahead of rows"
divergence: a reconnect reporting that version takes the fast delta path, which
trusts the stored rows to match. It is now a single CTE (one implicit, all-or-
nothing Postgres transaction), so the stored `(rows, version)` can never tear. An
explicit transaction is unusable here because all client connections share one
pooled `tokio_postgres::Client`; a single statement keeps atomicity without
capturing other tasks' pipelined queries.

`ensure_schema` now serializes its `CREATE TABLE IF NOT EXISTS` block behind a
database-wide advisory lock. `CREATE TABLE IF NOT EXISTS` is not concurrency-safe
(two nodes booting together against a fresh database race on `pg_type` and one
fails with a duplicate-key error); the lock lets exactly one connection create the
schema while the rest wait and then find it present.
