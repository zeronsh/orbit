//! The `orbit-server` binary: runs the integrated Orbit sync server.
//!
//! Configuration via env vars:
//!   ORBIT_PG_HOST (default 127.0.0.1), ORBIT_PG_PORT (5433),
//!   ORBIT_PG_USER (orbit), ORBIT_PG_DB (orbit),
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
    let host = env("ORBIT_PG_HOST", "127.0.0.1");
    let port: u16 = env("ORBIT_PG_PORT", "5433").parse().unwrap_or(5433);
    let user = env("ORBIT_PG_USER", "orbit");
    let database = env("ORBIT_PG_DB", "orbit");
    let listen_addr = env("ORBIT_LISTEN", "127.0.0.1:4848");
    let tables_spec = env("ORBIT_TABLES", "");

    // Discover columns at runtime from information_schema for each configured
    // table (typed String/Number/Boolean/Json) before starting.
    let (probe, probe_conn) = tokio_postgres::connect(
        &format!("host={host} port={port} user={user} dbname={database}"),
        tokio_postgres::NoTls,
    )
    .await?;
    tokio::spawn(async move {
        let _ = probe_conn.await;
    });

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
    // the SQLite-backed replica; default is in-memory.
    match env("ORBIT_REPLICA", "memory").as_str() {
        "sqlite" => {
            let dir = std::env::var("ORBIT_REPLICA_DIR").ok().map(std::path::PathBuf::from);
            run_server_sqlite(cfg, MutatorRegistry::new(), dir).await
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
