//! The mutagen: applies client CRUD mutations to the **upstream** Postgres.
//!
//! Port of `zero-cache`'s `services/mutagen`. Mutations write to Postgres; the
//! changes then flow back to clients through logical replication — so a client's
//! own write is confirmed via the normal sync path (write-through, not a
//! separate optimistic channel here).
//!
//! Values are emitted as escaped SQL literals rather than bound parameters: the
//! engine already holds strongly-typed [`Value`]s, and literals sidestep
//! per-column Rust↔Postgres type matching. Identifiers and string values are
//! escaped.

use anyhow::{bail, Result};
use oql::value::{Row, Value};
use orbit_protocol::{CrudOp, Mutation};
use tokio_postgres::Client;

/// Apply a single mutation to Postgres.
pub async fn apply_mutation(client: &Client, mutation: &Mutation) -> Result<()> {
    match mutation {
        Mutation::Crud { args, .. } => {
            for arg in args {
                for op in &arg.ops {
                    apply_crud_op(client, op).await?;
                }
            }
            Ok(())
        }
        Mutation::Custom { name, .. } => {
            bail!("custom mutator {name:?} not yet supported")
        }
    }
}

/// Apply a single CRUD op to Postgres.
pub async fn apply_crud_op(client: &Client, op: &CrudOp) -> Result<()> {
    let sql = match op {
        CrudOp::Insert { table_name, value, .. } => insert_sql(table_name, value, &[]),
        CrudOp::Upsert { table_name, primary_key, value } => {
            insert_sql(table_name, value, primary_key)
        }
        CrudOp::Update { table_name, primary_key, value } => {
            update_sql(table_name, primary_key, value)
        }
        CrudOp::Delete { table_name, primary_key, value } => {
            delete_sql(table_name, primary_key, value)
        }
    };
    client.batch_execute(&sql).await?;
    Ok(())
}

fn insert_sql(table: &str, value: &Row, upsert_pk: &[String]) -> String {
    let cols: Vec<&str> = value.keys().collect();
    let col_list = cols.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
    let val_list = cols
        .iter()
        .map(|c| sql_literal(value.get(*c).unwrap_or(&Value::Null)))
        .collect::<Vec<_>>()
        .join(", ");
    let mut sql = format!(
        "INSERT INTO {} ({}) VALUES ({})",
        quote_ident(table),
        col_list,
        val_list
    );
    if !upsert_pk.is_empty() {
        let conflict = upsert_pk.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ");
        let updates: Vec<String> = cols
            .iter()
            .filter(|c| !upsert_pk.iter().any(|pk| pk.as_str() == **c))
            .map(|c| format!("{0} = EXCLUDED.{0}", quote_ident(c)))
            .collect();
        if updates.is_empty() {
            sql.push_str(&format!(" ON CONFLICT ({conflict}) DO NOTHING"));
        } else {
            sql.push_str(&format!(
                " ON CONFLICT ({conflict}) DO UPDATE SET {}",
                updates.join(", ")
            ));
        }
    }
    sql
}

fn update_sql(table: &str, primary_key: &[String], value: &Row) -> String {
    let assignments: Vec<String> = value
        .iter()
        .filter(|(c, _)| !primary_key.iter().any(|pk| pk == *c))
        .map(|(c, v)| format!("{} = {}", quote_ident(c), sql_literal(v)))
        .collect();
    format!(
        "UPDATE {} SET {} WHERE {}",
        quote_ident(table),
        assignments.join(", "),
        pk_where(primary_key, value)
    )
}

fn delete_sql(table: &str, primary_key: &[String], value: &Row) -> String {
    format!(
        "DELETE FROM {} WHERE {}",
        quote_ident(table),
        pk_where(primary_key, value)
    )
}

fn pk_where(primary_key: &[String], value: &Row) -> String {
    primary_key
        .iter()
        .map(|k| format!("{} = {}", quote_ident(k), sql_literal(value.get(k).unwrap_or(&Value::Null))))
        .collect::<Vec<_>>()
        .join(" AND ")
}

/// Quote a SQL identifier, escaping embedded double-quotes.
fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// Render a [`Value`] as a SQL literal.
fn sql_literal(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => if *b { "true" } else { "false" }.to_string(),
        Value::Number(n) => {
            if n.fract() == 0.0 && n.is_finite() {
                format!("{}", *n as i64)
            } else {
                format!("{n}")
            }
        }
        Value::String(s) => quote_string(s),
        Value::Json(j) => format!("{}::jsonb", quote_string(&j.to_string())),
    }
}

fn quote_string(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}
