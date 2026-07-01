//! Runtime differential checker: replay a golden JSON file (array of scenarios,
//! same shape as tests/golden/*.json, produced by mono/orbit-golden/gen*.ts or
//! oracle.ts) through Orbit and report which scenarios diverge from Zero's
//! embedded snapshots. Unlike the `zql_differential` test (which `include_str!`s
//! the goldens at compile time), this reads the file at runtime — so a fuzz /
//! minimization loop can rewrite the JSON and re-run without recompiling.
//!
//!   cargo run --release --example scenario_check -p oql -- <golden.json>
//!
//! Exit code is nonzero if any scenario diverges. NOTE: a divergence is not
//! automatically an Orbit bug — the current Zero `zql` engine has known bugs in
//! nested correlated EXISTS/NOT EXISTS (see tests/nested_exists_correctness.rs),
//! where Orbit is the SQL-correct side. Ground-truth any new divergence class.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;

use oql::ast::Ast;
use oql::ivm::operator::Link;
use oql::ivm::{source_push, Catch, ColumnType, MemorySource, Node, SourceChange};
use oql::value::Row;
use oql::{build_pipeline, SourceProvider};
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

struct Provider(HashMap<String, Rc<RefCell<MemorySource>>>);
impl SourceProvider for Provider {
    fn get_source(&self, t: &str) -> Option<Rc<RefCell<MemorySource>>> {
        self.0.get(t).cloned()
    }
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
    let mut map = HashMap::new();
    for (name, t) in &g.tables {
        let cols: BTreeMap<String, ColumnType> =
            t.columns.iter().map(|(k, c)| (k.clone(), col_type(&c.ty))).collect();
        let src = MemorySource::new(name, cols, t.pk.clone());
        for r in &t.rows {
            src.borrow_mut().insert_initial(json_to_row(r));
        }
        map.insert(name.clone(), src);
    }
    let provider = Provider(map);
    let ast: Ast = serde_json::from_value(g.ast.clone()).map_err(|e| format!("parse ast: {e}"))?;
    let top = build_pipeline(&ast, &provider);
    let catch = Catch::new(top.input.clone());
    let link: Link = catch.clone();
    top.set_output(link);

    let mut snaps = vec![snapshot(&catch)];
    for p in &g.pushes {
        let src = provider.0.get(&p.table).expect("source");
        let row = json_to_row(&p.row);
        match p.op.as_str() {
            "add" => source_push(src, SourceChange::Add(row)),
            "remove" => source_push(src, SourceChange::Remove(row)),
            "edit" => source_push(
                src,
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
            return Err(format!("snapshot {i} differs:\n  orbit: {got}\n  zero:  {want}"));
        }
    }
    Ok(())
}

fn main() {
    let path = std::env::args().nth(1).expect("usage: scenario_check <golden.json>");
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
    println!("{}/{} scenarios matched Zero", passed, goldens.len());
    for f in &failures {
        println!("FAIL {f}");
    }
    if !failures.is_empty() {
        std::process::exit(1);
    }
}
