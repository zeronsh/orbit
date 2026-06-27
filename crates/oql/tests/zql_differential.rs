//! Differential parity test against Zero's **real** `zql` engine.
//!
//! `crates/oql/tests/golden/zql_golden.json` is produced by running Zero's own
//! `zql` (via `mono/orbit-golden/gen.ts`) over a set of query scenarios,
//! recording the materialized result after the initial load and after each
//! mutation. This test replays the identical scenarios through Orbit's engine
//! and asserts byte-identical materialized snapshots — a cross-engine parity
//! certification that is independent of change-stream granularity.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;

use oql::ast::Ast;
use oql::ivm::operator::Link;
use oql::ivm::{source_push, Catch, ColumnType, MemorySource, Node, SourceChange};
use oql::value::Row;
use oql::{build_pipeline, resolve_static_params, SourceProvider};
use serde::Deserialize;

#[derive(Deserialize)]
struct Golden {
    name: String,
    tables: HashMap<String, GTable>,
    ast: serde_json::Value,
    pushes: Vec<GPush>,
    snapshots: Vec<serde_json::Value>,
    /// Read-permission auth data; when present the ast's `static` params are
    /// resolved against it before the pipeline is built.
    #[serde(default, rename = "authData")]
    auth_data: Option<serde_json::Value>,
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

/// Normalize a node tree to the same JSON shape the generator emits. Hidden
/// `zsubq_*` relationships (EXISTS internals) are excluded from the comparison.
fn norm(node: &Node) -> serde_json::Value {
    let row = serde_json::to_value(&*node.row).unwrap();
    let mut rels = serde_json::Map::new();
    for (k, children) in &node.relationships {
        if k.starts_with("zsubq_") {
            continue;
        }
        rels.insert(
            k.clone(),
            serde_json::Value::Array(children.iter().map(norm).collect()),
        );
    }
    serde_json::json!({ "row": row, "rels": serde_json::Value::Object(rels) })
}

fn snapshot(catch: &Rc<RefCell<Catch>>) -> serde_json::Value {
    serde_json::Value::Array(catch.borrow().fetch().iter().map(norm).collect())
}

/// Replay one scenario through Orbit; `Err` on any snapshot mismatch.
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

    let mut ast: Ast = serde_json::from_value(g.ast.clone()).map_err(|e| format!("parse ast: {e}"))?;
    // Read-permission scenarios carry `authData`; resolve the ast's `static`
    // parameters against it (the same step orbit-cache does) before building.
    if let Some(auth) = &g.auth_data {
        ast = resolve_static_params(&ast, auth);
    }
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
                SourceChange::Edit {
                    row,
                    old_row: json_to_row(p.old_row.as_ref().expect("oldRow")),
                },
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
            return Err(format!(
                "snapshot {i} differs:\n  orbit: {got}\n  zero:  {want}"
            ));
        }
    }
    Ok(())
}

fn run_all(goldens: &[Golden]) -> (usize, Vec<String>) {
    // Silence panic spew; we convert panics to per-scenario failures.
    let prev = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let mut passed = 0;
    let mut failures = Vec::new();
    for g in goldens {
        let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run_scenario(g)));
        match res {
            Ok(Ok(())) => passed += 1,
            Ok(Err(msg)) => failures.push(format!("{}: {msg}", g.name)),
            Err(_) => failures.push(format!("{}: PANIC", g.name)),
        }
    }
    std::panic::set_hook(prev);
    (passed, failures)
}

#[test]
fn orbit_matches_zero_zql_engine() {
    let goldens: Vec<Golden> =
        serde_json::from_str(include_str!("golden/zql_golden.json")).expect("parse golden");
    let (passed, failures) = run_all(&goldens);
    eprintln!("hand-written: {passed}/{} matched Zero", goldens.len());
    assert!(failures.is_empty(), "mismatches:\n{}", failures.join("\n"));
}

#[test]
fn orbit_matches_zero_fuzz() {
    let goldens: Vec<Golden> =
        serde_json::from_str(include_str!("golden/zql_fuzz_golden.json")).expect("parse fuzz golden");
    let (passed, failures) = run_all(&goldens);
    eprintln!("fuzz: {passed}/{} matched Zero", goldens.len());
    // Report a capped sample of failures for triage.
    let shown: Vec<_> = failures.iter().take(8).cloned().collect();
    assert!(
        failures.is_empty(),
        "{} fuzz mismatches (showing {}):\n{}",
        failures.len(),
        shown.len(),
        shown.join("\n")
    );
}

#[test]
fn orbit_matches_zero_permissions() {
    let goldens: Vec<Golden> = serde_json::from_str(include_str!("golden/zql_permissions_golden.json"))
        .expect("parse permissions golden");
    let (passed, failures) = run_all(&goldens);
    eprintln!("permissions (authData): {passed}/{} matched Zero", goldens.len());
    let shown: Vec<_> = failures.iter().take(8).cloned().collect();
    assert!(
        failures.is_empty(),
        "{} permission mismatches (showing {}):\n{}",
        failures.len(),
        shown.len(),
        shown.join("\n")
    );
}

#[test]
fn orbit_matches_zero_related() {
    let goldens: Vec<Golden> = serde_json::from_str(include_str!("golden/zql_related_golden.json"))
        .expect("parse related golden");
    let (passed, failures) = run_all(&goldens);
    eprintln!("related/exists: {passed}/{} matched Zero", goldens.len());
    let shown: Vec<_> = failures.iter().take(8).cloned().collect();
    assert!(
        failures.is_empty(),
        "{} related/exists mismatches (showing {}):\n{}",
        failures.len(),
        shown.len(),
        shown.join("\n")
    );
}
