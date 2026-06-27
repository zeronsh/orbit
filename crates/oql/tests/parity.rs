//! Parity tests mirroring edge cases from Zero's `zql` test suite (the parity
//! oracle): the subtle semantics of comparison, filtering, edit-splitting,
//! joins, and take that are easy to get wrong.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use oql::ast::{Direction as AstDir, System};
use oql::ivm::operator::{Link, OpHandle};
use oql::ivm::{
    connect, source_push, Catch, Change, ColumnType, Filter, Join, MemorySource, Node, Predicate,
    SourceChange,
};
use oql::value::{compare_values, values_equal, Value};
use std::cmp::Ordering;

fn row(pairs: &[(&str, Value)]) -> oql::value::Row {
    pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
}
fn cols(names: &[&str]) -> BTreeMap<String, ColumnType> {
    names.iter().map(|n| (n.to_string(), ColumnType::String)).collect()
}
fn asc(f: &str) -> Vec<(String, AstDir)> {
    vec![(f.to_string(), AstDir::Asc)]
}
fn ids(nodes: &[Node]) -> Vec<Value> {
    nodes.iter().map(|n| n.row["id"].clone()).collect()
}

// ---- compareValues / valuesEqual (data.test.ts) ----------------------------

#[test]
fn compare_values_semantics() {
    // null sorts first; equal numbers; bool false<true; utf8 strings.
    assert_eq!(compare_values(&Value::Null, &1.into()), Ordering::Less);
    assert_eq!(compare_values(&Value::Null, &Value::Null), Ordering::Equal);
    assert_eq!(compare_values(&false.into(), &true.into()), Ordering::Less);
    // multi-byte: "Z" (0x5A) < "a" (0x61) in UTF-8/byte order.
    assert_eq!(compare_values(&"Z".into(), &"a".into()), Ordering::Less);
    // join semantics: null never equals null.
    assert!(!values_equal(&Value::Null, &Value::Null));
}

// ---- Filter edit splitting (filter.test.ts / maybe-split) ------------------

fn filter_pipe(pred: Predicate) -> (Rc<RefCell<MemorySource>>, Rc<RefCell<Catch>>) {
    let src = MemorySource::new("t", cols(&["id", "n"]), vec!["id".into()]);
    let conn = OpHandle::new(connect(&src, asc("id")));
    let filter = Filter::new(conn, pred);
    let fh = OpHandle::new(filter);
    let catch = Catch::new(fh.input.clone());
    let link: Link = catch.clone();
    fh.set_output(link);
    (src, catch)
}

#[test]
fn filter_edit_all_four_quadrants() {
    let pred: Predicate = Rc::new(|r| matches!(r.get("n"), Some(Value::Number(x)) if *x > 0.0));
    let (src, catch) = filter_pipe(pred);
    src.borrow_mut().insert_initial(row(&[("id", "a".into()), ("n", 1.into())])); // passes
    src.borrow_mut().insert_initial(row(&[("id", "b".into()), ("n", (-1).into())])); // fails
    let _ = catch.borrow().fetch();

    // pass -> pass : Edit
    source_push(&src, SourceChange::Edit { row: row(&[("id", "a".into()), ("n", 2.into())]), old_row: row(&[("id", "a".into()), ("n", 1.into())]) });
    assert!(matches!(catch.borrow_mut().take_changes().as_slice(), [Change::Edit { .. }]));

    // pass -> fail : Remove
    source_push(&src, SourceChange::Edit { row: row(&[("id", "a".into()), ("n", (-5).into())]), old_row: row(&[("id", "a".into()), ("n", 2.into())]) });
    assert!(matches!(catch.borrow_mut().take_changes().as_slice(), [Change::Remove(_)]));

    // fail -> pass : Add
    source_push(&src, SourceChange::Edit { row: row(&[("id", "b".into()), ("n", 9.into())]), old_row: row(&[("id", "b".into()), ("n", (-1).into())]) });
    assert!(matches!(catch.borrow_mut().take_changes().as_slice(), [Change::Add(_)]));

    // fail -> fail : nothing
    source_push(&src, SourceChange::Edit { row: row(&[("id", "a".into()), ("n", (-2).into())]), old_row: row(&[("id", "a".into()), ("n", (-5).into())]) });
    assert!(catch.borrow_mut().take_changes().is_empty());
}

// ---- Join: child add hits multiple parents (join.push.test.ts) -------------

#[test]
fn join_child_add_fans_to_all_matching_parents() {
    let issues = MemorySource::new("issue", cols(&["id"]), vec!["id".into()]);
    // Two parents that share the same join key are not possible (PK), so use a
    // non-PK join: parent.group == child.group, many parents per group.
    let p = MemorySource::new("p", cols(&["id", "g"]), vec!["id".into()]);
    let c = MemorySource::new("c", cols(&["id", "g"]), vec!["id".into()]);
    drop(issues);

    let join = Join::new(
        OpHandle::new(connect(&p, asc("id"))),
        OpHandle::new(connect(&c, asc("id"))),
        vec!["g".into()],
        vec!["g".into()],
        "children",
        false,
        System::Client,
    );
    let jh = OpHandle::new(join);
    let catch = Catch::new(jh.input.clone());
    let link: Link = catch.clone();
    jh.set_output(link);

    p.borrow_mut().insert_initial(row(&[("id", "p1".into()), ("g", "x".into())]));
    p.borrow_mut().insert_initial(row(&[("id", "p2".into()), ("g", "x".into())]));
    let _ = catch.borrow().fetch();

    // Add a child in group x -> both p1 and p2 get a child change.
    source_push(&c, SourceChange::Add(row(&[("id", "c1".into()), ("g", "x".into())])));
    let changes = catch.borrow_mut().take_changes();
    let child_parents: Vec<Value> = changes
        .iter()
        .filter_map(|ch| match ch {
            Change::Child { node, .. } => Some(node.row["id"].clone()),
            _ => None,
        })
        .collect();
    assert_eq!(child_parents, vec!["p1".into(), "p2".into()]);
}

// ---- Take: edge cases (take.push.test.ts) ----------------------------------

fn take_pipe(limit: usize) -> (Rc<RefCell<MemorySource>>, Rc<RefCell<Catch>>) {
    let src = MemorySource::new("t", cols(&["id"]), vec!["id".into()]);
    let conn = OpHandle::new(connect(&src, asc("id")));
    let take = oql::ivm::Take::new(conn, limit);
    let th = OpHandle::new(take);
    let catch = Catch::new(th.input.clone());
    let link: Link = catch.clone();
    th.set_output(link);
    (src, catch)
}

#[test]
fn take_limit_zero_emits_nothing() {
    let (src, catch) = take_pipe(0);
    src.borrow_mut().insert_initial(row(&[("id", "a".into())]));
    assert!(catch.borrow().fetch().is_empty());
    source_push(&src, SourceChange::Add(row(&[("id", "b".into())])));
    assert!(catch.borrow_mut().take_changes().is_empty());
}

#[test]
fn take_limit_larger_than_data() {
    let (src, catch) = take_pipe(10);
    src.borrow_mut().insert_initial(row(&[("id", "a".into())]));
    let _ = catch.borrow().fetch();
    source_push(&src, SourceChange::Add(row(&[("id", "b".into())])));
    // No eviction; b is simply added.
    let changes = catch.borrow_mut().take_changes();
    assert!(changes.iter().any(|c| matches!(c, Change::Add(n) if n.row["id"] == "b".into())));
    assert_eq!(ids(&catch.borrow().fetch()), vec!["a".into(), "b".into()]);
}

#[test]
fn take_add_beyond_limit_is_ignored() {
    let (src, catch) = take_pipe(2);
    for id in ["a", "b"] {
        src.borrow_mut().insert_initial(row(&[("id", id.into())]));
    }
    let _ = catch.borrow().fetch(); // window: a, b
    // "z" sorts after the window; adding it changes nothing.
    source_push(&src, SourceChange::Add(row(&[("id", "z".into())])));
    assert!(catch.borrow_mut().take_changes().is_empty());
    assert_eq!(ids(&catch.borrow().fetch()), vec!["a".into(), "b".into()]);
}

#[test]
fn take_edit_within_window_updates_value() {
    let src = MemorySource::new("t", cols(&["id", "v"]), vec!["id".into()]);
    let conn = OpHandle::new(connect(&src, asc("id")));
    let take = oql::ivm::Take::new(conn, 2);
    let th = OpHandle::new(take);
    let catch = Catch::new(th.input.clone());
    let link: Link = catch.clone();
    th.set_output(link);
    src.borrow_mut().insert_initial(row(&[("id", "a".into()), ("v", "1".into())]));
    src.borrow_mut().insert_initial(row(&[("id", "b".into()), ("v", "2".into())]));
    let _ = catch.borrow().fetch();

    // Edit a's value (id unchanged -> stays in window, value updated).
    source_push(&src, SourceChange::Edit {
        row: row(&[("id", "a".into()), ("v", "99".into())]),
        old_row: row(&[("id", "a".into()), ("v", "1".into())]),
    });
    let _ = catch.borrow_mut().take_changes();
    let nodes = catch.borrow().fetch();
    let a = nodes.iter().find(|n| n.row["id"] == "a".into()).unwrap();
    assert_eq!(a.row["v"], "99".into());
}

#[test]
fn join_child_remove_leaves_parent_with_empty_relationship() {
    let p = MemorySource::new("p", cols(&["id"]), vec!["id".into()]);
    let c = MemorySource::new("c", cols(&["id", "pid"]), vec!["id".into()]);
    let join = Join::new(
        OpHandle::new(connect(&p, asc("id"))),
        OpHandle::new(connect(&c, asc("id"))),
        vec!["id".into()],
        vec!["pid".into()],
        "children",
        false,
        System::Client,
    );
    let jh = OpHandle::new(join);
    let catch = Catch::new(jh.input.clone());
    let link: Link = catch.clone();
    jh.set_output(link);

    p.borrow_mut().insert_initial(row(&[("id", "p1".into())]));
    c.borrow_mut().insert_initial(row(&[("id", "c1".into()), ("pid", "p1".into())]));
    let _ = catch.borrow().fetch();

    // Remove the only child -> a child change; parent remains, relationship empty.
    source_push(&c, SourceChange::Remove(row(&[("id", "c1".into()), ("pid", "p1".into())])));
    let changes = catch.borrow_mut().take_changes();
    assert!(changes.iter().any(|ch| matches!(ch, Change::Child { node, .. } if node.row["id"] == "p1".into())));
    let nodes = catch.borrow().fetch();
    assert_eq!(ids(&nodes), vec!["p1".into()]);
    assert!(nodes[0].relationships["children"].is_empty());
}

#[test]
fn compound_primary_key_add_remove() {
    // PK is (region, id); operations must key on both columns.
    let src = MemorySource::new("t", cols(&["region", "id", "v"]), vec!["region".into(), "id".into()]);
    let conn = OpHandle::new(connect(&src, vec![("region".to_string(), AstDir::Asc), ("id".to_string(), AstDir::Asc)]));
    let catch = Catch::new(conn.input.clone());
    let catch_link: Link = catch.clone();
    conn.set_output(catch_link);

    src.borrow_mut().insert_initial(row(&[("region", "us".into()), ("id", "1".into()), ("v", "a".into())]));
    src.borrow_mut().insert_initial(row(&[("region", "eu".into()), ("id", "1".into()), ("v", "b".into())]));
    let _ = catch.borrow().fetch();

    // Two rows share id="1" but differ by region — both must be present.
    let got: Vec<(Value, Value)> = catch.borrow().fetch().iter()
        .map(|n| (n.row["region"].clone(), n.row["id"].clone())).collect();
    assert_eq!(got, vec![("eu".into(), "1".into()), ("us".into(), "1".into())]);

    // Remove the (us,1) row — must not touch (eu,1).
    source_push(&src, SourceChange::Remove(row(&[("region", "us".into()), ("id", "1".into()), ("v", "a".into())])));
    let changes = catch.borrow_mut().take_changes();
    assert!(changes.iter().any(|c| matches!(c, Change::Remove(n) if n.row["region"] == "us".into())));
    let got: Vec<Value> = catch.borrow().fetch().iter().map(|n| n.row["region"].clone()).collect();
    assert_eq!(got, vec!["eu".into()]);
}

#[test]
fn take_remove_inside_window_pulls_in_next() {
    let (src, catch) = take_pipe(2);
    for id in ["a", "b", "c"] {
        src.borrow_mut().insert_initial(row(&[("id", id.into())]));
    }
    let _ = catch.borrow().fetch(); // window: a, b
    source_push(&src, SourceChange::Remove(row(&[("id", "a".into())])));
    // a removed, c pulled in.
    let changes = catch.borrow_mut().take_changes();
    assert!(changes.iter().any(|c| matches!(c, Change::Remove(n) if n.row["id"] == "a".into())));
    assert!(changes.iter().any(|c| matches!(c, Change::Add(n) if n.row["id"] == "c".into())));
    assert_eq!(ids(&catch.borrow().fetch()), vec!["b".into(), "c".into()]);
}
