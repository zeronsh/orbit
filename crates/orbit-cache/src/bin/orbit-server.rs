//! The `orbit-server` binary: runs the integrated Orbit sync server.
//!
//! Configuration via env vars:
//!   DATABASE_URL — a full `postgres://user:pass@host:port/db?sslmode=…` URL
//!     (managed PG); takes precedence over the discrete vars below,
//!   ORBIT_PG_HOST (default 127.0.0.1), ORBIT_PG_PORT (5433),
//!   ORBIT_PG_USER (orbit), ORBIT_PG_DB (orbit),
//!   ORBIT_PG_PASSWORD (or PGPASSWORD) — for password-authed (managed) Postgres,
//!   ORBIT_PG_SSLMODE (or PGSSLMODE) — disable (default) | require | verify-full,
//!   ORBIT_LISTEN (127.0.0.1:4848),
//!   ORBIT_TABLES — comma-separated `table:pkcol` specs (columns are discovered
//!     as text; e.g. `issue:id,comment:id`).
//!
//! For richer column typing, embed and configure [`orbit_cache::ServerConfig`]
//! directly. This binary keeps every column as text/JSON-friendly by default.

use oql::ivm::ColumnType;
use orbit_cache::{run_server, run_server_sqlite, MutatorRegistry, ServerConfig, TableConfig};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    // Version is kept in lockstep with the `@zeronsh/orbit` npm package + the
    // `ghcr.io/zeronsh/orbit-server` image (see scripts/sync-versions.mjs).
    eprintln!("orbit-server v{}", env!("CARGO_PKG_VERSION"));

    // Connection params: DATABASE_URL (managed PG) if set, else ORBIT_PG_* +
    // ORBIT_PG_PASSWORD/PGPASSWORD + ORBIT_PG_SSLMODE/PGSSLMODE.
    let orbit_cache::pg::tls::PgConnInfo { host, port, user, database, password, tls } =
        orbit_cache::pg::tls::PgConnInfo::from_env(5433, "orbit", "orbit")?;
    let listen_addr = env("ORBIT_LISTEN", "127.0.0.1:4848");
    let tables_spec = env("ORBIT_TABLES", "");

    // Discover columns at runtime from information_schema for each configured
    // table (typed String/Number/Boolean/Json) before starting.
    let probe_conn_str = orbit_cache::pg::tls::conn_str(&host, port, &user, &database, password.as_deref());
    let (probe, probe_driver) = orbit_cache::pg::tls::connect(&probe_conn_str, tls).await?;
    tokio::spawn(probe_driver);

    let mut tables = Vec::new();
    for spec in tables_spec.split(',').filter(|s| !s.trim().is_empty()) {
        let mut parts = spec.split(':');
        let name = parts.next().unwrap().trim().to_string();
        let pk = parts.next().unwrap_or("id").trim().to_string();
        let columns = discover_columns(&probe, &name).await?;
        if columns.is_empty() {
            anyhow::bail!(
                "table `{name}` not found in database `{database}` on {host}:{port}. \
                 Create it first — e.g. `docker exec -i orbit-pg psql -U {user} -d {database} < apps/demo/postgres/01-init.sql` \
                 (or `cd apps/demo && docker compose up -d` for a fresh Postgres)."
            );
        }
        tables.push(TableConfig {
            name,
            columns,
            primary_key: vec![pk],
        });
    }

    if tables.is_empty() {
        eprintln!("set ORBIT_TABLES, e.g. ORBIT_TABLES=issue:id,comment:id");
        std::process::exit(1);
    }

    let cfg = ServerConfig {
        host,
        port,
        user,
        database,
        password,
        tls,
        tables,
        publication: "orbit_pub".to_string(),
        slot: "orbit_slot".to_string(),
        listen_addr,
        // Forward custom mutators/queries to your app's API endpoints (Zero-style).
        mutate_url: std::env::var("ORBIT_MUTATE_URL").ok(),
        query_url: std::env::var("ORBIT_QUERY_URL").ok(),
        api_key: std::env::var("ORBIT_API_KEY").ok(),
        forward_cookies: std::env::var("ORBIT_FORWARD_COOKIES").is_ok(),
    };

    // ORBIT_REPLICA=sqlite (+ optional ORBIT_REPLICA_DIR for durability) selects
    // the SQLite-backed replica; default is in-memory. ORBIT_REPLICA_CACHE_MB /
    // ORBIT_REPLICA_MMAP_MB tune the SQLite page cache / mmap budget.
    match env("ORBIT_REPLICA", "memory").as_str() {
        "sqlite" => {
            let dir = std::env::var("ORBIT_REPLICA_DIR").ok().map(std::path::PathBuf::from);
            let opts = orbit_cache::SqliteReplicaOpts {
                cache_mb: std::env::var("ORBIT_REPLICA_CACHE_MB").ok().and_then(|v| v.parse().ok()),
                mmap_mb: std::env::var("ORBIT_REPLICA_MMAP_MB").ok().and_then(|v| v.parse().ok()),
            };
            run_server_sqlite(cfg, MutatorRegistry::new(), dir, opts).await
        }
        _ => run_server(cfg, MutatorRegistry::new()).await,
    }
}

async fn discover_columns(
    client: &tokio_postgres::Client,
    table: &str,
) -> anyhow::Result<Vec<(String, ColumnType)>> {
    let rows = client
        .query(
            "SELECT column_name, data_type FROM information_schema.columns \
             WHERE table_name = $1 ORDER BY ordinal_position",
            &[&table],
        )
        .await?;
    Ok(rows
        .iter()
        .map(|r| {
            let name: String = r.get(0);
            let ty: String = r.get(1);
            (name, map_pg_type(&ty))
        })
        .collect())
}

fn map_pg_type(data_type: &str) -> ColumnType {
    match data_type {
        "integer" | "bigint" | "smallint" | "numeric" | "real" | "double precision" => {
            ColumnType::Number
        }
        "boolean" => ColumnType::Boolean,
        "json" | "jsonb" => ColumnType::Json,
        _ => ColumnType::String,
    }
}

fn env(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}
