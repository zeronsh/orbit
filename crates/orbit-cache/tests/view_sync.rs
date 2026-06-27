//! Tests that the IVM change stream is correctly flattened into wire row
//! patches (the view-syncer bridge).

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;

use oql::ast::Direction;
use oql::ivm::operator::Link;
use oql::ivm::{source_push, Catch, ColumnType, MemorySource, SourceChange};
use oql::value::Value;
use oql::{build_pipeline, correlation, Query, SourceProvider};

use orbit_cache::{changes_to_patches, initial_patches};
use orbit_protocol::RowPatchOp;

struct Sources(HashMap<String, Rc<RefCell<MemorySource>>>);
impl SourceProvider for Sources {
    fn get_source(&self, table: &str) -> Option<Rc<RefCell<MemorySource>>> {
        self.0.get(table).cloned()
    }
}

fn row(pairs: &[(&str, Value)]) -> oql::value::Row {
    pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
}
fn str_cols(names: &[&str]) -> BTreeMap<String, ColumnType> {
    names.iter().map(|n| (n.to_string(), ColumnType::String)).collect()
}

#[test]
fn initial_and_incremental_patches() {
    let issue = MemorySource::new("issue", str_cols(&["id"]), vec!["id".into()]);
    let comment = MemorySource::new("comment", str_cols(&["id", "issueID"]), vec!["id".into()]);
    issue.borrow_mut().insert_initial(row(&[("id", "i1".into())]));
    comment
        .borrow_mut()
        .insert_initial(row(&[("id", "c1".into()), ("issueID", "i1".into())]));

    let mut map = HashMap::new();
    map.insert("issue".to_string(), Rc::clone(&issue));
    map.insert("comment".to_string(), Rc::clone(&comment));
    let sources = Sources(map);

    let ast = Query::table("issue")
        .related("comments", correlation(&["id"], &["issueID"]), Query::table("comment"))
        .order_by("id", Direction::Asc)
        .build();

    let top = build_pipeline(&ast, &sources);
    let catch = Catch::new(top.input.clone());
    let catch_link: Link = catch.clone();
    top.set_output(catch_link);

    // Initial poke: a put for issue i1 and its comment c1.
    let schema = catch.borrow().get_schema();
    let nodes = catch.borrow().fetch();
    let patches = initial_patches(&nodes, &schema);
    assert_eq!(patches.len(), 2);
    assert!(patches.iter().any(|p| matches!(p, RowPatchOp::Put { table_name, value }
        if table_name == "issue" && value.get("id") == Some(&Value::String("i1".into())))));
    assert!(patches.iter().any(|p| matches!(p, RowPatchOp::Put { table_name, value }
        if table_name == "comment" && value.get("id") == Some(&Value::String("c1".into())))));

    // Incremental: add a new comment -> a single put under the comment table.
    source_push(
        &comment,
        SourceChange::Add(row(&[("id", "c2".into()), ("issueID", "i1".into())])),
    );
    let changes = catch.borrow_mut().take_changes();
    let patches = changes_to_patches(&changes, &schema);
    assert_eq!(patches.len(), 1);
    assert!(matches!(&patches[0], RowPatchOp::Put { table_name, value }
        if table_name == "comment" && value.get("id") == Some(&Value::String("c2".into()))));

    // Remove the issue -> dels for issue and (cascaded) its comments.
    source_push(&issue, SourceChange::Remove(row(&[("id", "i1".into())])));
    let changes = catch.borrow_mut().take_changes();
    let patches = changes_to_patches(&changes, &schema);
    assert!(patches.iter().any(|p| matches!(p, RowPatchOp::Del { table_name, id }
        if table_name == "issue" && id.get("id") == Some(&Value::String("i1".into())))));
    // Both comments cascade as dels.
    let comment_dels = patches
        .iter()
        .filter(|p| matches!(p, RowPatchOp::Del { table_name, .. } if table_name == "comment"))
        .count();
    assert_eq!(comment_dels, 2, "removing the issue cascades dels for its comments");
}
