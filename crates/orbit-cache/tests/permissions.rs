//! Read-permissions end-to-end at the engine level: a per-table rule using
//! `authData` restricts a materialized query to the authorized rows.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;

use oql::ast::{Condition, ParameterAnchor, ParameterField, SimpleOperator, ValuePosition};
use oql::ivm::operator::Link;
use oql::ivm::{source_push, Catch, ColumnType, MemorySource, SourceChange};
use oql::value::Value;
use oql::{build_pipeline, Query, SourceProvider};
use orbit_cache::Permissions;

struct Sources(HashMap<String, Rc<RefCell<MemorySource>>>);
impl SourceProvider for Sources {
    fn get_source(&self, table: &str) -> Option<Rc<RefCell<MemorySource>>> {
        self.0.get(table).cloned()
    }
}

fn row(pairs: &[(&str, Value)]) -> oql::value::Row {
    pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
}

#[test]
fn read_rule_restricts_to_authorized_rows() {
    let doc = MemorySource::new("doc", {
        let mut c = BTreeMap::new();
        c.insert("id".to_string(), ColumnType::String);
        c.insert("owner".to_string(), ColumnType::String);
        c
    }, vec!["id".into()]);
    // Two docs for u1, one for u2.
    doc.borrow_mut().insert_initial(row(&[("id", "d1".into()), ("owner", "u1".into())]));
    doc.borrow_mut().insert_initial(row(&[("id", "d2".into()), ("owner", "u2".into())]));
    doc.borrow_mut().insert_initial(row(&[("id", "d3".into()), ("owner", "u1".into())]));

    let mut map = HashMap::new();
    map.insert("doc".to_string(), Rc::clone(&doc));
    let sources = Sources(map);

    // Permission: owner == authData.userId
    let mut perms = Permissions::new();
    perms.allow_read(
        "doc",
        Condition::Simple {
            op: SimpleOperator::Eq,
            left: ValuePosition::Column { name: "owner".into() },
            right: ValuePosition::Static {
                anchor: ParameterAnchor::AuthData,
                field: ParameterField::Single("userId".into()),
            },
        },
    );

    // Client query: all docs, ordered. Permissions restrict it to the user's.
    let query = Query::table("doc").order_by("id", oql::ast::Direction::Asc).build();
    let auth = serde_json::json!({ "userId": "u1" });
    let authorized = perms.apply(&query, &auth);

    let top = build_pipeline(&authorized, &sources);
    let catch = Catch::new(top.input.clone());
    let link: Link = catch.clone();
    top.set_output(link);

    let ids: Vec<String> = catch.borrow().fetch().iter().map(|n| match n.row.get("id") {
        Some(Value::String(s)) => s.clone(),
        _ => unreachable!(),
    }).collect();
    assert_eq!(ids, vec!["d1", "d3"], "only u1's docs are visible");

    // A new doc for u2 must NOT appear; a new doc for u1 must.
    source_push(&doc, SourceChange::Add(row(&[("id", "d4".into()), ("owner", "u2".into())])));
    source_push(&doc, SourceChange::Add(row(&[("id", "d5".into()), ("owner", "u1".into())])));
    let changes = catch.borrow_mut().take_changes();
    assert!(changes.iter().any(|c| matches!(c, oql::ivm::Change::Add(n) if n.row["id"] == "d5".into())));
    assert!(!changes.iter().any(|c| matches!(c, oql::ivm::Change::Add(n) if n.row["id"] == "d4".into())));
}
