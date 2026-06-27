//! The todo example's sync server (Postgres-backed).
//!
//! Custom mutators and custom queries live in the **TypeScript client** as typed
//! `defineMutator` / `defineQuery` definitions (see `examples/todo/src/orbit.ts`).
//! A mutator's `tx.mutate.*` calls are recorded into CRUD ops and a query def
//! produces an AST; both are sent to this server, which persists them to Postgres
//! (the source of truth) and streams changes back to every client. So the server
//! just needs the table — no per-mutator handlers.
//!
//! Run Postgres first (`examples/todo/ docker compose up -d`), then:
//!   cargo run -p orbit-cache --example todo_server
//!
//! Env overrides: ORBIT_PG_HOST/PORT/USER/DB, ORBIT_LISTEN.

use oql::ivm::ColumnType;
use orbit_cache::{run_server, MutatorRegistry, ServerConfig, TableConfig};

fn env(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let cfg = ServerConfig {
        host: env("ORBIT_PG_HOST", "127.0.0.1"),
        port: env("ORBIT_PG_PORT", "5433").parse().unwrap_or(5433),
        user: env("ORBIT_PG_USER", "orbit"),
        database: env("ORBIT_PG_DB", "orbit"),
        tables: vec![TableConfig {
            name: "todo".into(),
            columns: vec![
                ("id".into(), ColumnType::String),
                ("text".into(), ColumnType::String),
                ("completed".into(), ColumnType::Boolean),
                ("created".into(), ColumnType::Number),
                ("owner".into(), ColumnType::String),
            ],
            primary_key: vec!["id".into()],
        }],
        publication: "orbit_pub".into(),
        slot: "orbit_slot".into(),
        listen_addr: env("ORBIT_LISTEN", "127.0.0.1:4848"),
        // Forward custom mutators/queries to the app's /api routes (served by the
        // Vite dev server — see examples/todo/src/api.ts), attaching the client's
        // auth so the app authenticates + adds context.
        mutate_url: Some(env("ORBIT_MUTATE_URL", "http://127.0.0.1:5173/api/push")),
        query_url: Some(env("ORBIT_QUERY_URL", "http://127.0.0.1:5173/api/query")),
        api_key: std::env::var("ORBIT_API_KEY").ok(),
        forward_cookies: true,
    };

    eprintln!("todo server — schema-typed mutators/queries are defined in the TS client");
    run_server(cfg, MutatorRegistry::new()).await
}
