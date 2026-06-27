//! The [`Filter`] operator: a stateless row predicate.
//!
//! Port of `zql/src/ivm/filter.ts` + `filter-push.ts` +
//! `maybe-split-and-push-edit-change.ts`. An edit whose predicate result
//! changes is split into an add or a remove.

use super::node::{Change, Changes, Node};
use super::operator::{FetchRequest, Input, Link, OpHandle, Operator};
use super::schema::Schema;
use crate::value::Row;
use smallvec::smallvec;
use std::cell::RefCell;
use std::rc::Rc;

/// A boxed row predicate.
pub type Predicate = Rc<dyn Fn(&Row) -> bool>;

/// Filters rows through a (pure) predicate.
pub struct Filter {
    input: Rc<RefCell<dyn Input>>,
    predicate: Predicate,
    output: Option<Link>,
}

impl Filter {
    pub fn new(input: OpHandle, predicate: Predicate) -> Rc<RefCell<Filter>> {
        let filter = Rc::new(RefCell::new(Filter {
            input: input.input.clone(),
            predicate,
            output: None,
        }));
        input.set_output(filter.clone());
        filter
    }
}

impl Input for Filter {
    fn get_schema(&self) -> Rc<Schema> {
        self.input.borrow().get_schema()
    }
    fn fetch(&self, req: &FetchRequest) -> Vec<Node> {
        self.input
            .borrow()
            .fetch(req)
            .into_iter()
            .filter(|n| (self.predicate)(&n.row))
            .collect()
    }
}

impl Operator for Filter {
    fn push(&mut self, change: Change) -> Changes {
        filter_push(change, &self.predicate)
    }
    fn output(&self) -> Option<Link> {
        self.output.clone()
    }
    fn set_output(&mut self, out: Link) {
        self.output = Some(out);
    }
}

/// Apply `predicate` to a change, splitting edits as needed.
///
/// Mirrors `filterPush` + `maybeSplitAndPushEditChange`.
pub fn filter_push(change: Change, predicate: &Predicate) -> Changes {
    match change {
        Change::Add(node) => {
            if predicate(&node.row) {
                smallvec![Change::Add(node)]
            } else {
                Changes::new()
            }
        }
        Change::Remove(node) => {
            if predicate(&node.row) {
                smallvec![Change::Remove(node)]
            } else {
                Changes::new()
            }
        }
        Change::Child {
            node,
            relationship_name,
            change,
        } => {
            // The child change passes only if the parent row passes the filter.
            if predicate(&node.row) {
                smallvec![Change::Child {
                    node,
                    relationship_name,
                    change,
                }]
            } else {
                Changes::new()
            }
        }
        Change::Edit { node, old_node } => {
            let old_matches = predicate(&old_node.row);
            let new_matches = predicate(&node.row);
            match (old_matches, new_matches) {
                (true, true) => smallvec![Change::Edit { node, old_node }],
                (true, false) => smallvec![Change::Remove(old_node)],
                (false, true) => smallvec![Change::Add(node)],
                (false, false) => Changes::new(),
            }
        }
    }
}
