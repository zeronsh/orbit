---
"@zeronsh/orbit": patch
---

Server: Postgres connections now support TLS and password auth.

The `orbit-server`/`orbit-node` binaries (and the `ghcr.io/zeronsh/orbit-server`
image) can now connect to managed Postgres (Railway/Neon/Supabase). Configure via
`DATABASE_URL` (`postgres://user:pass@host:port/db?sslmode=require`), or the
discrete `ORBIT_PG_PASSWORD`/`PGPASSWORD` + `ORBIT_PG_SSLMODE`/`PGSSLMODE`
(`disable` | `require` | `verify-full`). Both the SQL connections and the logical
replication stream are secured. No TS API change (server/binary only).
