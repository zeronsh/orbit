# Hosting Orbit

The Orbit server is a single Rust binary (`orbit-server`) that connects to Postgres
(logical replication), keeps an in-memory/SQLite replica, and serves clients over a
WebSocket — plus a cluster variant (`orbit-node`) for the replicator / view-syncer
split. Both ship in one image: **`ghcr.io/zeronsh/orbit-server`**.

## Quick start (Docker Compose)

Brings up Postgres (with `wal_level=logical`) + the server:

```bash
ORBIT_TABLES=issue:id,comment:id docker compose -f deploy/docker-compose.yml up
# build locally instead of pulling the image:
ORBIT_TABLES=issue:id,comment:id docker compose -f deploy/docker-compose.yml up --build
```

The server listens on `:4848`. Point your client at `ws://localhost:4848`.

## Run the image directly

```bash
docker run --rm -p 4848:4848 \
  -e ORBIT_PG_HOST=your-db-host -e ORBIT_PG_PORT=5432 \
  -e ORBIT_PG_USER=orbit -e ORBIT_PG_DB=orbit \
  -e ORBIT_LISTEN=0.0.0.0:4848 \
  -e ORBIT_TABLES=issue:id,comment:id \
  ghcr.io/zeronsh/orbit-server:latest
```

Run the cluster node role instead by overriding the command with `orbit-node`.

## Environment variables

| Var | Default | Purpose |
| --- | --- | --- |
| `DATABASE_URL` | _(none)_ | Full `postgres://user:pass@host:port/db?sslmode=…` URL (managed PG). Overrides the `ORBIT_PG_*` vars below. |
| `ORBIT_PG_HOST` | `127.0.0.1` | Postgres host |
| `ORBIT_PG_PORT` | `5433` | Postgres port |
| `ORBIT_PG_USER` | `orbit` | Postgres user |
| `ORBIT_PG_DB` | `orbit` | Postgres database |
| `ORBIT_PG_PASSWORD` | _(none)_ | Postgres password (or `PGPASSWORD`); omit for trust auth |
| `ORBIT_PG_SSLMODE` | `disable` | TLS mode (or `PGSSLMODE`): `disable` \| `require` \| `verify-full` |
| `ORBIT_LISTEN` | `127.0.0.1:4848` | WebSocket bind address — **set `0.0.0.0:4848` in a container** |
| `ORBIT_TABLES` | _(required)_ | Comma-separated `table:pkColumn` specs to replicate + serve |
| `ORBIT_REPLICA` | `memory` | `memory` or `sqlite` (durable) |
| `ORBIT_REPLICA_DIR` | _(none)_ | Directory for the SQLite replica when `ORBIT_REPLICA=sqlite` |
| `ORBIT_MUTATE_URL` | _(none)_ | App endpoint for custom mutators (Zero-style forwarding) |
| `ORBIT_QUERY_URL` | _(none)_ | App endpoint for custom (named) queries |
| `ORBIT_API_KEY` | _(none)_ | Sent as `X-Api-Key` to the forwarding endpoints |
| `ORBIT_FORWARD_COOKIES` | _(unset)_ | If set, forward the client `Cookie` header |

## Postgres requirements

Orbit uses logical replication, so Postgres must run with:

```
wal_level=logical, max_wal_senders>=10, max_replication_slots>=10
```

and the replicated tables need `REPLICA IDENTITY FULL` so updates/deletes carry the
full old row. The compose file's Postgres is preconfigured; for a managed Postgres
(Neon, RDS, Supabase…) enable logical replication per the provider's docs.

## Hosting targets

- **Any container host** (Fly.io, Railway, Render, ECS, Cloud Run): deploy the
  `ghcr.io/zeronsh/orbit-server` image, set the env vars above, expose `4848`.
- **Cluster** (separate replicator + view-syncers with object-store snapshots): run
  the image with the `orbit-node` command; see `Dockerfile.node` and the multinode docs.

The image is built + published by the `docker` job in
[`.github/workflows/release.yml`](../.github/workflows/release.yml) on every push to
`main` (`latest` + sha, plus `v<version>` on a release).
