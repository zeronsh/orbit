//! Tests for the fluent query builder + pipeline builder (AST → IVM pipeline).

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;

use oql::ast::{Direction, SimpleOperator};
use oql::ivm::operator::Link;
use oql::ivm::{source_push, Catch, ColumnType, MemorySource, Node, SourceChange};
use oql::value::Value;
use oql::{build_pipeline, correlation, Query, SourceProvider};

struct Sources {
    map: HashMap<String, Rc<RefCell<MemorySource>>>,
}
impl SourceProvider for Sources {
    fn get_source(&self, table: &str) -> Option<Rc<RefCell<MemorySource>>> {
        self.map.get(table).cloned()
    }
}

fn row(pairs: &[(&str, Value)]) -> oql::value::Row {
    pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
}
fn str_cols(names: &[&str]) -> BTreeMap<String, ColumnType> {
    names.iter().map(|n| (n.to_string(), ColumnType::String)).collect()
}
fn ids(nodes: &[Node]) -> Vec<Value> {
    nodes.iter().map(|n| n.row["id"].clone()).collect()
}

fn issue_and_comment_sources() -> Sources {
    let issue = MemorySource::new("issue", str_cols(&["id", "open"]), vec!["id".into()]);
    let comment = MemorySource::new("comment", str_cols(&["id", "issueID"]), vec!["id".into()]);
    let mut map = HashMap::new();
    map.insert("issue".to_string(), issue);
    map.insert("comment".to_string(), comment);
    Sources { map }
}

/// Build a pipeline from an AST, attach a Catch, return (catch, top handle).
fn materialize(ast: &oql::ast::Ast, provider: &dyn SourceProvider) -> Rc<RefCell<Catch>> {
    let top = build_pipeline(ast, provider);
    let catch = Catch::new(top.input.clone());
    let catch_link: Link = catch.clone();
    top.set_output(catch_link);
    catch
}

#[test]
fn where_and_limit() {
    let sources = issue_and_comment_sources();
    let issue = sources.get_source("issue").unwrap();
    for (id, open) in [("a", "true"), ("b", "false"), ("c", "true"), ("d", "true")] {
        issue
            .borrow_mut()
            .insert_initial(row(&[("id", id.into()), ("open", open.into())]));
    }

    let ast = Query::table("issue")
        .where_("open", SimpleOperator::Eq, "true")
        .order_by("id", Direction::Asc)
        .limit(2)
        .build();

    let catch = materialize(&ast, &sources);
    // open=true rows are a, c, d; limited to first 2 -> a, c.
    assert_eq!(ids(&catch.borrow().fetch()), vec!["a".into(), "c".into()]);
}

#[test]
fn related_join_with_builder() {
    let sources = issue_and_comment_sources();
    let issue = sources.get_source("issue").unwrap();
    let comment = sources.get_source("comment").unwrap();
    issue.borrow_mut().insert_initial(row(&[("id", "i1".into()), ("open", "true".into())]));
    issue.borrow_mut().insert_initial(row(&[("id", "i2".into()), ("open", "true".into())]));
    comment
        .borrow_mut()
        .insert_initial(row(&[("id", "c1".into()), ("issueID", "i1".into())]));

    let ast = Query::table("issue")
        .related(
            "comments",
            correlation(&["id"], &["issueID"]),
            Query::table("comment"),
        )
        .order_by("id", Direction::Asc)
        .build();

    let catch = materialize(&ast, &sources);
    let nodes = catch.borrow().fetch();
    assert_eq!(ids(&nodes), vec!["i1".into(), "i2".into()]);
    assert_eq!(ids(&nodes[0].relationships["comments"]), vec!["c1".into()]);
    assert!(nodes[1].relationships["comments"].is_empty());

    // Live: add a comment to i2.
    source_push(
        &comment,
        SourceChange::Add(row(&[("id", "c2".into()), ("issueID", "i2".into())])),
    );
    let changes = catch.borrow_mut().take_changes();
    assert_eq!(changes.len(), 1);
    assert!(matches!(&changes[0], oql::ivm::Change::Child { node, relationship_name, .. }
        if node.row["id"] == "i2".into() && relationship_name == "comments"));
}

#[test]
fn not_exists_selects_parents_without_children() {
    let sources = issue_and_comment_sources();
    let issue = sources.get_source("issue").unwrap();
    let comment = sources.get_source("comment").unwrap();
    for id in ["i1", "i2", "i3"] {
        issue.borrow_mut().insert_initial(row(&[("id", id.into()), ("open", "true".into())]));
    }
    comment.borrow_mut().insert_initial(row(&[("id", "c1".into()), ("issueID", "i2".into())]));

    // issue WHERE NOT EXISTS(comments) -> i1, i3
    let ast = Query::table("issue")
        .where_exists(correlation(&["id"], &["issueID"]), Query::table("comment"), true)
        .order_by("id", Direction::Asc)
        .build();
    let catch = materialize(&ast, &sources);
    assert_eq!(ids(&catch.borrow().fetch()), vec!["i1".into(), "i3".into()]);

    // Add a comment to i1 -> it drops out of NOT EXISTS.
    source_push(&comment, SourceChange::Add(row(&[("id", "c2".into()), ("issueID", "i1".into())])));
    let changes = catch.borrow_mut().take_changes();
    assert!(changes.iter().any(|c| matches!(c, oql::ivm::Change::Remove(n) if n.row["id"] == "i1".into())));
    assert_eq!(ids(&catch.borrow().fetch()), vec!["i3".into()]);
}

#[test]
fn nested_related_two_levels() {
    // issue -> comments -> author
    let issue = MemorySource::new("issue", str_cols(&["id"]), vec!["id".into()]);
    let comment = MemorySource::new("comment", str_cols(&["id", "issueID", "authorID"]), vec!["id".into()]);
    let user = MemorySource::new("user", str_cols(&["id", "name"]), vec!["id".into()]);
    issue.borrow_mut().insert_initial(row(&[("id", "i1".into())]));
    comment.borrow_mut().insert_initial(row(&[("id", "c1".into()), ("issueID", "i1".into()), ("authorID", "u1".into())]));
    user.borrow_mut().insert_initial(row(&[("id", "u1".into()), ("name", "Ada".into())]));
    let mut map = HashMap::new();
    map.insert("issue".to_string(), issue);
    map.insert("comment".to_string(), comment);
    map.insert("user".to_string(), user);
    let sources = Sources { map };

    let ast = Query::table("issue")
        .related(
            "comments",
            correlation(&["id"], &["issueID"]),
            Query::table("comment").related(
                "author",
                correlation(&["authorID"], &["id"]),
                Query::table("user"),
            ),
        )
        .order_by("id", Direction::Asc)
        .build();

    let catch = materialize(&ast, &sources);
    let nodes = catch.borrow().fetch();
    let comments = &nodes[0].relationships["comments"];
    assert_eq!(ids(comments), vec!["c1".into()]);
    let author = &comments[0].relationships["author"];
    assert_eq!(author[0].row["name"], "Ada".into());
}

#[test]
fn related_limit_is_per_parent() {
    let sources = issue_and_comment_sources();
    let issue = sources.get_source("issue").unwrap();
    let comment = sources.get_source("comment").unwrap();
    for id in ["i1", "i2"] {
        issue.borrow_mut().insert_initial(row(&[("id", id.into()), ("open", "true".into())]));
    }
    // i1 has 3 comments, i2 has 1.
    for (cid, iid) in [("c1", "i1"), ("c2", "i1"), ("c3", "i1"), ("c4", "i2")] {
        comment.borrow_mut().insert_initial(row(&[("id", cid.into()), ("issueID", iid.into())]));
    }

    // issue.related('comments', comment LIMIT 2)  -> 2 comments per issue.
    let ast = Query::table("issue")
        .related(
            "comments",
            correlation(&["id"], &["issueID"]),
            Query::table("comment").order_by("id", Direction::Asc).limit(2),
        )
        .order_by("id", Direction::Asc)
        .build();

    let catch = materialize(&ast, &sources);
    let nodes = catch.borrow().fetch();
    // i1: first 2 of its 3 comments; i2: its 1 comment.
    assert_eq!(ids(&nodes[0].relationships["comments"]), vec!["c1".into(), "c2".into()]);
    assert_eq!(ids(&nodes[1].relationships["comments"]), vec!["c4".into()]);
}

#[test]
fn where_exists_filters_parents_with_relationship() {
    let sources = issue_and_comment_sources();
    let issue = sources.get_source("issue").unwrap();
    let comment = sources.get_source("comment").unwrap();
    for id in ["i1", "i2", "i3"] {
        issue
            .borrow_mut()
            .insert_initial(row(&[("id", id.into()), ("open", "true".into())]));
    }
    // Only i1 has a comment initially.
    comment
        .borrow_mut()
        .insert_initial(row(&[("id", "c1".into()), ("issueID", "i1".into())]));

    // issue WHERE EXISTS (comment where comment.issueID = issue.id)
    let ast = Query::table("issue")
        .where_exists(
            correlation(&["id"], &["issueID"]),
            Query::table("comment"),
            false,
        )
        .order_by("id", Direction::Asc)
        .build();

    let catch = materialize(&ast, &sources);
    assert_eq!(ids(&catch.borrow().fetch()), vec!["i1".into()]);

    // Add a comment for i2 -> i2 now passes EXISTS (Add).
    source_push(
        &comment,
        SourceChange::Add(row(&[("id", "c2".into()), ("issueID", "i2".into())])),
    );
    let changes = catch.borrow_mut().take_changes();
    assert!(changes.iter().any(|c| matches!(c, oql::ivm::Change::Add(n) if n.row["id"] == "i2".into())));
    assert_eq!(ids(&catch.borrow().fetch()), vec!["i1".into(), "i2".into()]);

    // Remove i1's only comment -> i1 no longer passes (Remove).
    source_push(&comment, SourceChange::Remove(row(&[("id", "c1".into()), ("issueID", "i1".into())])));
    let changes = catch.borrow_mut().take_changes();
    assert!(changes.iter().any(|c| matches!(c, oql::ivm::Change::Remove(n) if n.row["id"] == "i1".into())));
    assert_eq!(ids(&catch.borrow().fetch()), vec!["i2".into()]);
}

#[test]
fn where_or_with_exists() {
    use oql::ast::{Condition, CorrelatedSubquery, Correlation, ExistsOp, ValuePosition};
    let sources = issue_and_comment_sources();
    let issue = sources.get_source("issue").unwrap();
    let comment = sources.get_source("comment").unwrap();
    for id in ["i1", "i2", "i3"] {
        issue.borrow_mut().insert_initial(row(&[("id", id.into()), ("open", "true".into())]));
    }
    // Only i2 has a comment.
    comment.borrow_mut().insert_initial(row(&[("id", "c1".into()), ("issueID", "i2".into())]));

    // WHERE id = 'i3' OR EXISTS(comments)  -> i2 (exists), i3 (id match)
    let exists_cond = Condition::CorrelatedSubquery {
        related: CorrelatedSubquery {
            correlation: Correlation {
                parent_field: vec!["id".into()],
                child_field: vec!["issueID".into()],
            },
            subquery: Box::new(Query::table("comment").build()),
            system: None,
            hidden: None,
        },
        op: ExistsOp::Exists,
        flip: None,
        scalar: None,
    };
    let or = Condition::Or {
        conditions: vec![
            Condition::Simple {
                op: SimpleOperator::Eq,
                left: ValuePosition::Column { name: "id".into() },
                right: ValuePosition::Literal { value: "i3".into() },
            },
            exists_cond,
        ],
    };
    let ast = Query::table("issue").where_cond(or).order_by("id", Direction::Asc).build();
    let catch = materialize(&ast, &sources);
    assert_eq!(ids(&catch.borrow().fetch()), vec!["i2".into(), "i3".into()]);

    // Add a comment to i1 -> now i1 also matches (EXISTS).
    source_push(&comment, SourceChange::Add(row(&[("id", "c2".into()), ("issueID", "i1".into())])));
    let changes = catch.borrow_mut().take_changes();
    assert!(changes.iter().any(|c| matches!(c, oql::ivm::Change::Add(n) if n.row["id"] == "i1".into())));
    assert_eq!(ids(&catch.borrow().fetch()), vec!["i1".into(), "i2".into(), "i3".into()]);
}

#[test]
fn where_or_and_combinations() {
    use oql::ast::{Condition, ValuePosition};
    let sources = issue_and_comment_sources();
    let issue = sources.get_source("issue").unwrap();
    for (id, open) in [("a", "true"), ("b", "false"), ("c", "true")] {
        issue.borrow_mut().insert_initial(row(&[("id", id.into()), ("open", open.into())]));
    }

    // WHERE id = 'a' OR open = 'false'  -> a, b
    let or_cond = Condition::Or {
        conditions: vec![
            Condition::Simple {
                op: SimpleOperator::Eq,
                left: ValuePosition::Column { name: "id".into() },
                right: ValuePosition::Literal { value: "a".into() },
            },
            Condition::Simple {
                op: SimpleOperator::Eq,
                left: ValuePosition::Column { name: "open".into() },
                right: ValuePosition::Literal { value: "false".into() },
            },
        ],
    };
    let ast = Query::table("issue")
        .where_cond(or_cond)
        .order_by("id", Direction::Asc)
        .build();
    let catch = materialize(&ast, &sources);
    assert_eq!(ids(&catch.borrow().fetch()), vec!["a".into(), "b".into()]);
}

#[test]
fn filter_operator_matrix() {
    use oql::ast::{Condition, LiteralPrimitive, LiteralValue, ValuePosition};
    let n = MemorySource::new("n", str_cols(&["id", "v"]), vec!["id".into()]);
    for (id, v) in [("a", 1), ("b", 2), ("c", 3), ("d", 4), ("e", 5)] {
        n.borrow_mut().insert_initial(row(&[("id", id.into()), ("v", v.into())]));
    }
    let mut map = HashMap::new();
    map.insert("n".to_string(), n.clone());
    let sources = Sources { map };

    let run = |q: oql::ast::Ast| -> Vec<Value> {
        let catch = materialize(&q, &sources);
        let r = ids(&catch.borrow().fetch());
        r
    };

    // Comparisons.
    assert_eq!(run(Query::table("n").where_("v", SimpleOperator::Eq, 3).order_by("id", Direction::Asc).build()), vec!["c".into()]);
    assert_eq!(run(Query::table("n").where_("v", SimpleOperator::Ne, 3).order_by("id", Direction::Asc).build()), vec!["a".into(), "b".into(), "d".into(), "e".into()]);
    assert_eq!(run(Query::table("n").where_("v", SimpleOperator::Lt, 3).order_by("id", Direction::Asc).build()), vec!["a".into(), "b".into()]);
    assert_eq!(run(Query::table("n").where_("v", SimpleOperator::Le, 3).order_by("id", Direction::Asc).build()), vec!["a".into(), "b".into(), "c".into()]);
    assert_eq!(run(Query::table("n").where_("v", SimpleOperator::Gt, 3).order_by("id", Direction::Asc).build()), vec!["d".into(), "e".into()]);
    assert_eq!(run(Query::table("n").where_("v", SimpleOperator::Ge, 3).order_by("id", Direction::Asc).build()), vec!["c".into(), "d".into(), "e".into()]);

    // IN / NOT IN.
    let in_cond = Condition::Simple {
        op: SimpleOperator::In,
        left: ValuePosition::Column { name: "v".into() },
        right: ValuePosition::Literal { value: LiteralValue::Array(vec![LiteralPrimitive::Number(2.0), LiteralPrimitive::Number(4.0)]) },
    };
    assert_eq!(run(Query::table("n").where_cond(in_cond).order_by("id", Direction::Asc).build()), vec!["b".into(), "d".into()]);
}

#[test]
fn filter_like_and_null_semantics() {
    let t = MemorySource::new("t", str_cols(&["id", "name"]), vec!["id".into()]);
    t.borrow_mut().insert_initial(row(&[("id", "1".into()), ("name", "apple".into())]));
    t.borrow_mut().insert_initial(row(&[("id", "2".into()), ("name", "Apricot".into())]));
    t.borrow_mut().insert_initial(row(&[("id", "3".into()), ("name", Value::Null)]));
    let mut map = HashMap::new();
    map.insert("t".to_string(), t.clone());
    let sources = Sources { map };
    let run = |q: oql::ast::Ast| -> Vec<Value> {
        let catch = materialize(&q, &sources);
        let r = ids(&catch.borrow().fetch());
        r
    };

    // LIKE is case-sensitive; ILIKE is not.
    assert_eq!(run(Query::table("t").where_("name", SimpleOperator::Like, "ap%").order_by("id", Direction::Asc).build()), vec!["1".into()]);
    assert_eq!(run(Query::table("t").where_("name", SimpleOperator::ILike, "ap%").order_by("id", Direction::Asc).build()), vec!["1".into(), "2".into()]);
    // A comparison with a null lhs yields false (the null-name row is excluded).
    assert_eq!(run(Query::table("t").where_("name", SimpleOperator::Like, "%").order_by("id", Direction::Asc).build()), vec!["1".into(), "2".into()]);
}

#[test]
fn where_operators_round_trip_through_predicate() {
    let sources = issue_and_comment_sources();
    let issue = sources.get_source("issue").unwrap();
    // Reuse "open" column to store names for LIKE/IN testing.
    for name in ["apple", "apricot", "banana"] {
        issue
            .borrow_mut()
            .insert_initial(row(&[("id", name.into()), ("open", name.into())]));
    }

    // LIKE 'ap%'
    let ast = Query::table("issue")
        .where_("open", SimpleOperator::Like, "ap%")
        .order_by("id", Direction::Asc)
        .build();
    let catch = materialize(&ast, &sources);
    assert_eq!(ids(&catch.borrow().fetch()), vec!["apple".into(), "apricot".into()]);
}
