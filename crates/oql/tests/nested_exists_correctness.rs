//! Absolute-correctness regression for nested correlated EXISTS.
//!
//! Query: `forager WHERE EXISTS(forager f2 : f2.epic = p.pillbox
//!            AND NOT EXISTS(gradient g : g.stitcher = f2.pillbox))`
//! with one self-matching row (pillbox == epic == "a"), then add a gradient row
//! whose `stitcher` does NOT equal "a".
//!
//! The inner `NOT EXISTS(gradient WHERE stitcher = "a")` is unaffected by adding a
//! gradient with `stitcher = "d"`, so the parent must STAY in the result. Verified
//! against SQLite (the engine Zero itself syncs from):
//!   after INSERT gradient(stitcher='d') → k4 present; only stitcher='a' drops it.
//!
//! This is NOT a differential-vs-Zero test: the current Zero `zql` engine has a bug
//! here (it drops the parent on ANY gradient add, ignoring the correlation), so
//! this asserts the SQL-correct answer directly to protect Orbit's correct behavior.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use oql::ast::Ast;
use oql::ivm::operator::Link;
use oql::ivm::{source_push, Catch, ColumnType, MemorySource, Node, SourceChange};
use oql::value::Row;
use oql::{build_pipeline, SourceProvider};

struct Provider(std::collections::HashMap<String, Rc<RefCell<MemorySource>>>);
impl SourceProvider for Provider {
    fn get_source(&self, t: &str) -> Option<Rc<RefCell<MemorySource>>> {
        self.0.get(t).cloned()
    }
}

fn row(v: serde_json::Value) -> Row {
    serde_json::from_value(v).unwrap()
}

fn ids(catch: &Rc<RefCell<Catch>>) -> Vec<String> {
    fn id(n: &Node) -> String {
        match n.row.get("co-producer") {
            Some(oql::value::Value::String(s)) => s.clone(),
            _ => String::new(),
        }
    }
    catch.borrow().fetch().iter().map(id).collect()
}

#[test]
fn nested_exists_ignores_noncorrelated_child_add() {
    let ast_json = serde_json::json!({
        "table": "forager",
        "orderBy": [["co-producer", "asc"]],
        "where": {
            "type": "correlatedSubquery", "op": "EXISTS",
            "related": {
                "correlation": {"parentField": ["pillbox"], "childField": ["epic"]},
                "subquery": {
                    "table": "forager", "alias": "zsubq_forager",
                    "where": {
                        "type": "correlatedSubquery", "op": "NOT EXISTS",
                        "related": {
                            "correlation": {"parentField": ["pillbox"], "childField": ["stitcher"]},
                            "subquery": {"table": "gradient", "alias": "zsubq_gradient"}
                        }
                    }
                }
            }
        }
    });

    let forager_cols: BTreeMap<String, ColumnType> = [
        ("co-producer", ColumnType::String),
        ("hope", ColumnType::Null),
        ("pillbox", ColumnType::String),
        ("epic", ColumnType::String),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect();
    let gradient_cols: BTreeMap<String, ColumnType> = [
        ("fork", ColumnType::Number),
        ("stitcher", ColumnType::String),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect();

    let forager = MemorySource::new("forager", forager_cols, vec!["co-producer".into(), "hope".into()]);
    forager
        .borrow_mut()
        .insert_initial(row(serde_json::json!({"co-producer": "k4", "hope": null, "pillbox": "a", "epic": "a"})));
    let gradient = MemorySource::new("gradient", gradient_cols, vec!["fork".into()]);

    let mut map = std::collections::HashMap::new();
    map.insert("forager".to_string(), forager);
    map.insert("gradient".to_string(), gradient.clone());
    let provider = Provider(map);

    let ast: Ast = serde_json::from_value(ast_json).unwrap();
    let top = build_pipeline(&ast, &provider);
    let catch = Catch::new(top.input.clone());
    let link: Link = catch.clone();
    top.set_output(link);

    // Initial: k4 self-matches (pillbox == epic == "a"); gradient empty → NOT EXISTS true.
    assert_eq!(ids(&catch), vec!["k4".to_string()], "initial load includes k4");

    // Add a gradient row whose stitcher != "a" — the correlated NOT EXISTS(stitcher="a")
    // is unaffected, so k4 must STAY (matches SQLite; Zero wrongly drops it).
    source_push(&gradient, SourceChange::Add(row(serde_json::json!({"fork": 5, "stitcher": "d"}))));
    catch.borrow_mut().take_changes();
    assert_eq!(
        ids(&catch),
        vec!["k4".to_string()],
        "adding gradient(stitcher='d') must NOT drop k4 (correlation needs stitcher='a')"
    );

    // Now add the correlated gradient(stitcher="a") → NOT EXISTS flips false → k4 drops.
    source_push(&gradient, SourceChange::Add(row(serde_json::json!({"fork": 6, "stitcher": "a"}))));
    catch.borrow_mut().take_changes();
    assert!(ids(&catch).is_empty(), "adding gradient(stitcher='a') drops k4");
}
