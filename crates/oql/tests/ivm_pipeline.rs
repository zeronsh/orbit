//! End-to-end IVM pipeline tests: build a pipeline, hydrate via `fetch`, then
//! push source changes and assert the emitted change stream — the same shape of
//! test Zero uses for its `zql` operators (the parity oracle).

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use oql::ast::Direction as AstDir;
use oql::ivm::operator::{Link, OpHandle};
use oql::ivm::{
    connect, source_push, Catch, Change, Filter, Join, MemorySource, Node, Predicate, SourceChange,
    Take,
};
use oql::value::Value;

// ---- helpers ---------------------------------------------------------------

fn row(pairs: &[(&str, Value)]) -> oql::value::Row {
    pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
}

fn cols(names: &[&str]) -> BTreeMap<String, oql::ivm::ColumnType> {
    names
        .iter()
        .map(|n| (n.to_string(), oql::ivm::ColumnType::String))
        .collect()
}

fn asc(field: &str) -> Vec<(String, AstDir)> {
    vec![(field.to_string(), AstDir::Asc)]
}

/// Names of the rows a Catch currently sees, in order.
fn ids(nodes: &[Node]) -> Vec<Value> {
    nodes.iter().map(|n| n.row.get("id").cloned().unwrap()).collect()
}

// ---- source + filter -------------------------------------------------------

#[test]
fn source_fetch_returns_sorted_rows() {
    let src = MemorySource::new("t", cols(&["id"]), vec!["id".into()]);
    for id in ["c", "a", "b"] {
        src.borrow_mut().insert_initial(row(&[("id", id.into())]));
    }
    let conn = connect(&src, asc("id"));
    let catch = Catch::new(conn.clone());
    assert_eq!(ids(&catch.borrow().fetch()), vec!["a".into(), "b".into(), "c".into()]);
}

#[test]
fn filter_push_add_and_edit_split() {
    let src = MemorySource::new("t", cols(&["id", "n"]), vec!["id".into()]);
    src.borrow_mut().insert_initial(row(&[("id", "a".into()), ("n", 5.into())]));
    let conn = OpHandle::new(connect(&src, asc("id")));

    // predicate: n > 3
    let pred: Predicate = Rc::new(|r| matches!(r.get("n"), Some(Value::Number(x)) if *x > 3.0));
    let filter = Filter::new(conn, pred);
    let filter_h = OpHandle::new(filter);
    let catch = Catch::new(filter_h.input.clone());
    let catch_link: Link = catch.clone();
    filter_h.set_output(catch_link);

    // Initially "a" (n=5) passes.
    assert_eq!(ids(&catch.borrow().fetch()), vec!["a".into()]);

    // Add "b" with n=1 -> filtered out (no change).
    source_push(&src, SourceChange::Add(row(&[("id", "b".into()), ("n", 1.into())])));
    assert!(catch.borrow_mut().take_changes().is_empty());

    // Add "c" with n=10 -> passes (Add change).
    source_push(&src, SourceChange::Add(row(&[("id", "c".into()), ("n", 10.into())])));
    let changes = catch.borrow_mut().take_changes();
    assert_eq!(changes.len(), 1);
    assert!(matches!(&changes[0], Change::Add(n) if n.row["id"] == "c".into()));

    // Edit "a" from n=5 (passes) to n=2 (fails) -> split into Remove.
    source_push(
        &src,
        SourceChange::Edit {
            row: row(&[("id", "a".into()), ("n", 2.into())]),
            old_row: row(&[("id", "a".into()), ("n", 5.into())]),
        },
    );
    let changes = catch.borrow_mut().take_changes();
    assert_eq!(changes.len(), 1);
    assert!(matches!(&changes[0], Change::Remove(n) if n.row["id"] == "a".into()));
}

// ---- join ------------------------------------------------------------------

#[allow(clippy::type_complexity)]
fn build_issue_comments() -> (
    Rc<RefCell<MemorySource>>,
    Rc<RefCell<MemorySource>>,
    Rc<RefCell<Catch>>,
) {
    let issues = MemorySource::new("issue", cols(&["id"]), vec!["id".into()]);
    let comments = MemorySource::new("comment", cols(&["id", "issueID"]), vec!["id".into()]);

    let issue_conn = connect(&issues, asc("id"));
    let comment_conn = connect(&comments, asc("id"));

    let join = Join::new(
        OpHandle::new(issue_conn),
        OpHandle::new(comment_conn),
        vec!["id".into()],
        vec!["issueID".into()],
        "comments",
        false,
    );
    let join_h = OpHandle::new(join);
    let catch = Catch::new(join_h.input.clone());
    let catch_link: Link = catch.clone();
    join_h.set_output(catch_link);
    (issues, comments, catch)
}

#[test]
fn join_fetch_materializes_child_relationship() {
    let (issues, comments, catch) = build_issue_comments();
    issues.borrow_mut().insert_initial(row(&[("id", "i1".into())]));
    issues.borrow_mut().insert_initial(row(&[("id", "i2".into())]));
    comments
        .borrow_mut()
        .insert_initial(row(&[("id", "c1".into()), ("issueID", "i1".into())]));
    comments
        .borrow_mut()
        .insert_initial(row(&[("id", "c2".into()), ("issueID", "i1".into())]));

    let nodes = catch.borrow().fetch();
    assert_eq!(ids(&nodes), vec!["i1".into(), "i2".into()]);
    // i1 has two comments, i2 has none.
    let i1 = &nodes[0];
    let i1_comments = &i1.relationships["comments"];
    assert_eq!(ids(i1_comments), vec!["c1".into(), "c2".into()]);
    assert!(nodes[1].relationships["comments"].is_empty());
}

#[test]
fn join_push_child_add_emits_child_change_with_overlay() {
    let (issues, comments, catch) = build_issue_comments();
    issues.borrow_mut().insert_initial(row(&[("id", "i1".into())]));
    // Hydrate.
    let _ = catch.borrow().fetch();

    // Add a comment for i1.
    source_push(
        &comments,
        SourceChange::Add(row(&[("id", "c1".into()), ("issueID", "i1".into())])),
    );
    let changes = catch.borrow_mut().take_changes();
    assert_eq!(changes.len(), 1, "one child change for the matching parent");
    match &changes[0] {
        Change::Child {
            node,
            relationship_name,
            change,
        } => {
            assert_eq!(node.row["id"], "i1".into());
            assert_eq!(relationship_name, "comments");
            // The parent node's relationship reflects the post-change state
            // (overlay): the new comment is present.
            assert_eq!(ids(&node.relationships["comments"]), vec!["c1".into()]);
            assert!(matches!(change.as_ref(), Change::Add(n) if n.row["id"] == "c1".into()));
        }
        other => panic!("expected Child change, got {other:?}"),
    }
}

#[test]
fn join_push_parent_add_includes_existing_children() {
    let (issues, comments, catch) = build_issue_comments();
    comments
        .borrow_mut()
        .insert_initial(row(&[("id", "c1".into()), ("issueID", "i9".into())]));
    let _ = catch.borrow().fetch();

    // Add issue i9 -> it should arrive already carrying its comment.
    source_push(&issues, SourceChange::Add(row(&[("id", "i9".into())])));
    let changes = catch.borrow_mut().take_changes();
    assert_eq!(changes.len(), 1);
    match &changes[0] {
        Change::Add(node) => {
            assert_eq!(node.row["id"], "i9".into());
            assert_eq!(ids(&node.relationships["comments"]), vec!["c1".into()]);
        }
        other => panic!("expected Add, got {other:?}"),
    }
}

// ---- take ------------------------------------------------------------------

#[test]
fn take_limits_and_maintains_window_on_push() {
    let src = MemorySource::new("t", cols(&["id"]), vec!["id".into()]);
    for id in ["a", "b", "c"] {
        src.borrow_mut().insert_initial(row(&[("id", id.into())]));
    }
    let conn = connect(&src, asc("id"));
    let take = Take::new(OpHandle::new(conn), 2);
    let take_h = OpHandle::new(take);
    let catch = Catch::new(take_h.input.clone());
    let catch_link: Link = catch.clone();
    take_h.set_output(catch_link);

    // Window is first 2: a, b.
    assert_eq!(ids(&catch.borrow().fetch()), vec!["a".into(), "b".into()]);

    // Add "aa" (sorts between a and b): enters window, "b" falls out.
    source_push(&src, SourceChange::Add(row(&[("id", "aa".into())])));
    let changes = catch.borrow_mut().take_changes();
    // Expect b removed and aa added (order: removes then adds).
    assert!(changes.iter().any(|c| matches!(c, Change::Remove(n) if n.row["id"] == "b".into())));
    assert!(changes.iter().any(|c| matches!(c, Change::Add(n) if n.row["id"] == "aa".into())));
    assert_eq!(ids(&catch.borrow().fetch()), vec!["a".into(), "aa".into()]);

    // Remove "a": "aa" stays, "b" re-enters.
    source_push(&src, SourceChange::Remove(row(&[("id", "a".into())])));
    let changes = catch.borrow_mut().take_changes();
    assert!(changes.iter().any(|c| matches!(c, Change::Remove(n) if n.row["id"] == "a".into())));
    assert!(changes.iter().any(|c| matches!(c, Change::Add(n) if n.row["id"] == "b".into())));
    assert_eq!(ids(&catch.borrow().fetch()), vec!["aa".into(), "b".into()]);
}

/// Dropping a pipeline's terminal must unravel the whole chain and stop the
/// source from pushing into it: outputs are weak links, so a dropped query
/// can't leak its operators or receive future changes (the pre-fix behavior
/// grew CPU + memory with every churned query, forever).
#[test]
fn dropped_pipeline_is_pruned_from_the_source() {
    use oql::ivm::{connect, source_push, Catch, ColumnType, Filter, MemorySource, Predicate, SourceChange};
    use oql::ivm::operator::{Link, OpHandle};
    use oql::value::Value;
    use std::rc::Rc;

    let mut cols = std::collections::BTreeMap::new();
    cols.insert("id".to_string(), ColumnType::Number);
    let src = MemorySource::new("t", cols, vec!["id".into()]);

    // Build a query pipeline, then DROP it (query GC / view destroy).
    let weak_catch;
    {
        let conn = OpHandle::new(connect(&src, vec![("id".to_string(), oql::ast::Direction::Asc)]));
        let pred: Predicate = Rc::new(|_| true);
        let fh = OpHandle::new(Filter::new(conn, pred));
        let catch = Catch::new(fh.input.clone());
        let link: Link = catch.clone();
        fh.set_output(link);
        catch.borrow().fetch();
        weak_catch = Rc::downgrade(&catch);
    } // catch dropped here

    assert!(weak_catch.upgrade().is_none(), "dropping the terminal frees the chain (no Rc cycle)");

    // Pushes after the drop must not panic and must not resurrect anything.
    let row: oql::value::Row = [("id".to_string(), Value::from(1.0))].into_iter().collect();
    source_push(&src, SourceChange::Add(row));

    // A NEW pipeline still works (and reuses the pruned slot).
    let conn = OpHandle::new(connect(&src, vec![("id".to_string(), oql::ast::Direction::Asc)]));
    let pred: Predicate = Rc::new(|_| true);
    let fh = OpHandle::new(Filter::new(conn, pred));
    let catch = Catch::new(fh.input.clone());
    let link: Link = catch.clone();
    fh.set_output(link);
    assert_eq!(catch.borrow().fetch().len(), 1);
    let row2: oql::value::Row = [("id".to_string(), Value::from(2.0))].into_iter().collect();
    source_push(&src, SourceChange::Add(row2));
    assert_eq!(catch.borrow().fetch().len(), 2, "new pipeline receives pushes");
}
