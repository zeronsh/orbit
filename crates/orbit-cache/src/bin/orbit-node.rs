//! `orbit-node` — a multinode cluster node, role chosen by `ORBIT_ROLE`:
//!   • `replicator`  — owns the Postgres slot, snapshots to the object store (S3/
//!     Tigris), and serves the change-stream.
//!   • `view-syncer` — restores from the object store, follows the change-stream,
//!     and serves WebSocket clients (forwarding mutations/queries to the app).
//!
//! Built with `--features s3` (uses `S3ObjectStore::from_env`). Env:
//!   ORBIT_ROLE                  replicator | view-syncer   (default view-syncer)
//!   ORBIT_PG_HOST/PORT/USER/DB / DATABASE_URL parts
//!   ORBIT_TABLES                e.g. user:id,issue:id,comment:id (columns discovered)
//!   ORBIT_CHANGE_STREAM_ADDR    replicator: bind addr (e.g. [::]:4000);
//!                               view-syncer: connect addr (e.g. replicator.railway.internal:4000)
//!   ORBIT_LISTEN                view-syncer WS bind (e.g. [::]:$PORT)
//!   ORBIT_MUTATE_URL/QUERY_URL  app push/query endpoints (view-syncer)
//!   ORBIT_SNAPSHOT_INTERVAL     replicator snapshot cadence, seconds (default 30)
//!   ORBIT_BUCKET, AWS_ENDPOINT_URL, AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY

use std::time::Duration;

use oql::ivm::ColumnType;
use orbit_cache::{
    run_replicator, run_view_syncer, MutatorRegistry, QueryRegistry, S3ObjectStore, ServerConfig,
    TableConfig,
};

fn env(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let role = env("ORBIT_ROLE", "view-syncer");
    let host = env("ORBIT_PG_HOST", "127.0.0.1");
    let port: u16 = env("ORBIT_PG_PORT", "5432").parse().unwrap_or(5432);
    let user = env("ORBIT_PG_USER", "postgres");
    let database = env("ORBIT_PG_DB", "railway");
    let tables_spec = env("ORBIT_TABLES", "");

    // Log immediately so a stuck boot is observable (otherwise the first output is
    // only after Postgres connects — a silent hang looks like a dead container).
    eprintln!("orbit-node: starting role={role} pg={host}:{port}/{database} tables=[{tables_spec}]");

    // Discover column types from information_schema for each configured table.
    // Railway's private network takes a few seconds to initialize after a container
    // starts; connecting immediately can hang forever. Retry with a per-attempt
    // timeout until Postgres is reachable.
    let conn_str = format!(
        "host={host} port={port} user={user} dbname={database} password={}",
        env("ORBIT_PG_PASSWORD", "")
    );
    let probe = loop {
        match tokio::time::timeout(
            Duration::from_secs(5),
            tokio_postgres::connect(&conn_str, tokio_postgres::NoTls),
        )
        .await
        {
            Ok(Ok((client, conn))) => {
                tokio::spawn(async move { let _ = conn.await; });
                eprintln!("orbit-node: connected to Postgres");
                break client;
            }
            Ok(Err(e)) => eprintln!("orbit-node: pg connect failed ({e}); retrying in 2s"),
            Err(_) => eprintln!("orbit-node: pg connect timed out (private net warming up?); retrying in 2s"),
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    };

    let mut tables = Vec::new();
    for spec in tables_spec.split(',').filter(|s| !s.trim().is_empty()) {
        let mut parts = spec.split(':');
        let name = parts.next().unwrap().trim().to_string();
        let pk = parts.next().unwrap_or("id").trim().to_string();
        let columns = discover_columns(&probe, &name).await?;
        anyhow::ensure!(!columns.is_empty(), "table `{name}` not found in `{database}`");
        tables.push(TableConfig { name, columns, primary_key: vec![pk] });
    }
    anyhow::ensure!(!tables.is_empty(), "set ORBIT_TABLES, e.g. user:id,issue:id,comment:id");

    let cfg = ServerConfig {
        host,
        port,
        user,
        database,
        tables,
        publication: env("ORBIT_PUBLICATION", "orbit_pub"),
        slot: env("ORBIT_SLOT", "orbit_slot"),
        listen_addr: env("ORBIT_LISTEN", "[::]:4848"),
        mutate_url: std::env::var("ORBIT_MUTATE_URL").ok(),
        query_url: std::env::var("ORBIT_QUERY_URL").ok(),
        api_key: std::env::var("ORBIT_API_KEY").ok(),
        forward_cookies: std::env::var("ORBIT_FORWARD_COOKIES").is_ok(),
    };

    let store = S3ObjectStore::from_env()?;
    let cs_addr = env("ORBIT_CHANGE_STREAM_ADDR", "[::]:4000");

    match role.as_str() {
        "replicator" => {
            let secs: u64 = env("ORBIT_SNAPSHOT_INTERVAL", "30").parse().unwrap_or(30);
            eprintln!("orbit-node: REPLICATOR, change-stream on {cs_addr}, snapshot every {secs}s");
            run_replicator(cfg, store, cs_addr, Duration::from_secs(secs)).await
        }
        _ => {
            eprintln!("orbit-node: VIEW-SYNCER, following {cs_addr}, serving WS on {}", cfg.listen_addr);
            run_view_syncer(cfg, store, cs_addr, MutatorRegistry::new(), QueryRegistry::new()).await
        }
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
            let ct = match ty.as_str() {
                "integer" | "bigint" | "smallint" | "numeric" | "real" | "double precision" => ColumnType::Number,
                "boolean" => ColumnType::Boolean,
                "json" | "jsonb" => ColumnType::Json,
                _ => ColumnType::String,
            };
            (name, ct)
        })
        .collect())
}
