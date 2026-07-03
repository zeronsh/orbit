//! Runtime differential checker for the **SQLite-backed source**: replays a
//! golden JSON file (same shape as `oql`'s `scenario_check`, produced from
//! Zero) through [`SqliteReplica`] pipelines and compares materialized
//! snapshots. This certifies that the SQL pushdown (WHERE / cursor / ORDER BY /
//! LIMIT / indexes) is behaviorally identical to both the in-memory engine and
//! Zero itself.
//!
//!   cargo run --release --example sqlite_scenario_check -p orbit-cache -- <golden.json>

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use oql::ast::Ast;
use oql::ivm::operator::Link;
use oql::ivm::{Catch, ColumnType, Node, SourceChange};
use oql::value::Row;
use orbit_cache::sqlite_source::{source_push, SqliteReplica};
use serde::Deserialize;

#[derive(Deserialize)]
struct Golden {
    name: String,
    tables: HashMap<String, GTable>,
    ast: serde_json::Value,
    pushes: Vec<GPush>,
    snapshots: Vec<serde_json::Value>,
}
#[derive(Deserialize)]
struct GTable {
    columns: HashMap<String, GCol>,
    pk: Vec<String>,
    rows: Vec<serde_json::Value>,
}
#[derive(Deserialize)]
struct GCol {
    #[serde(rename = "type")]
    ty: String,
}
#[derive(Deserialize)]
struct GPush {
    table: String,
    op: String,
    row: serde_json::Value,
    #[serde(default, rename = "oldRow")]
    old_row: Option<serde_json::Value>,
}

fn col_type(s: &str) -> ColumnType {
    match s {
        "number" => ColumnType::Number,
        "boolean" => ColumnType::Boolean,
        "json" => ColumnType::Json,
        "null" => ColumnType::Null,
        _ => ColumnType::String,
    }
}

fn json_to_row(v: &serde_json::Value) -> Row {
    serde_json::from_value(v.clone()).expect("row")
}

fn norm(node: &Node) -> serde_json::Value {
    let row = serde_json::to_value(&*node.row).unwrap();
    let mut rels = serde_json::Map::new();
    for (k, children) in &node.relationships {
        if k.starts_with("zsubq_") {
            continue;
        }
        rels.insert(k.clone(), serde_json::Value::Array(children.iter().map(norm).collect()));
    }
    serde_json::json!({ "row": row, "rels": serde_json::Value::Object(rels) })
}

fn snapshot(catch: &Rc<RefCell<Catch>>) -> serde_json::Value {
    serde_json::Value::Array(catch.borrow().fetch().iter().map(norm).collect())
}

fn run_scenario(g: &Golden) -> Result<(), String> {
    let mut replica = SqliteReplica::in_memory();
    for (name, t) in &g.tables {
        let cols: Vec<(String, ColumnType)> =
            t.columns.iter().map(|(k, c)| (k.clone(), col_type(&c.ty))).collect();
        let src = replica.add_table(name, cols, t.pk.clone());
        for r in &t.rows {
            src.borrow().insert_initial(&json_to_row(r));
        }
    }

    let ast: Ast = serde_json::from_value(g.ast.clone()).map_err(|e| format!("parse ast: {e}"))?;
    let top = oql::build_pipeline(&ast, &replica);
    let catch = Catch::new(top.input.clone());
    let link: Link = catch.clone();
    top.set_output(link);

    let mut snaps = vec![snapshot(&catch)];
    for p in &g.pushes {
        let src = replica.source(&p.table).expect("source");
        let row = json_to_row(&p.row);
        match p.op.as_str() {
            "add" => source_push(&src, SourceChange::Add(row)),
            "remove" => source_push(&src, SourceChange::Remove(row)),
            "edit" => source_push(
                &src,
                SourceChange::Edit { row, old_row: json_to_row(p.old_row.as_ref().expect("oldRow")) },
            ),
            other => return Err(format!("unknown op {other}")),
        }
        catch.borrow_mut().take_changes();
        snaps.push(snapshot(&catch));
    }
    if snaps.len() != g.snapshots.len() {
        return Err(format!("snapshot count {} != {}", snaps.len(), g.snapshots.len()));
    }
    for (i, (got, want)) in snaps.iter().zip(g.snapshots.iter()).enumerate() {
        if got != want {
            return Err(format!("snapshot {i} differs:\n  sqlite: {got}\n  zero:   {want}"));
        }
    }
    Ok(())
}

fn main() {
    let path = std::env::args().nth(1).expect("usage: sqlite_scenario_check <golden.json>");
    let text = std::fs::read_to_string(&path).expect("read golden file");
    let goldens: Vec<Golden> = serde_json::from_str(&text).expect("parse golden");
    let prev = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let mut passed = 0;
    let mut failures = Vec::new();
    for g in &goldens {
        let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run_scenario(g)));
        match res {
            Ok(Ok(())) => passed += 1,
            Ok(Err(msg)) => failures.push(format!("{}: {msg}", g.name)),
            Err(_) => failures.push(format!("{}: PANIC", g.name)),
        }
    }
    std::panic::set_hook(prev);
    println!("{}/{} scenarios matched Zero (via SQLite source)", passed, goldens.len());
    for f in failures.iter().take(5) {
        println!("FAIL {f}");
    }
    if !failures.is_empty() {
        std::process::exit(1);
    }
}
