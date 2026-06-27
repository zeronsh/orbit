//! [`CondFilter`]: a node-level filter whose predicate may inspect a node's
//! (eagerly materialized) relationships — generalizing both [`Filter`] (row
//! predicate) and [`Exists`] (relationship presence).
//!
//! This is how Orbit handles arbitrary boolean combinations of simple
//! predicates and `EXISTS` — including `OR` over subqueries — without Zero's
//! `FanOut`/`FanIn`: each `EXISTS` is materialized as a hidden relationship by a
//! preceding [`Join`](super::join), and the predicate evaluates presence of
//! those relationships alongside row predicates.
//!
//! Like [`Exists`](super::exists) it recomputes its passing set on push and
//! diffs (relationship changes can flip membership). Parents are compared by row
//! identity, so hidden-relationship churn produces no spurious patches.
//!
//! [`Filter`]: super::filter::Filter
//! [`Exists`]: super::exists::Exists

use super::node::{Change, Changes, Node};
use super::operator::{FetchRequest, Input, Link, OpHandle, Operator};
use super::schema::Schema;
use crate::value::{Row, Value};
use smallvec::smallvec;
use std::cell::RefCell;
use std::collections::BTreeSet;
use std::rc::Rc;

/// A predicate over a whole node (row + relationships).
pub type NodePredicate = Rc<dyn Fn(&Node) -> bool>;

pub struct CondFilter {
    input: Rc<RefCell<dyn Input>>,
    predicate: NodePredicate,
    primary_key: Vec<String>,
    /// Primary keys currently passing the predicate (`None` until hydrated).
    passing: RefCell<Option<BTreeSet<Vec<Value>>>>,
    output: Option<Link>,
}

impl CondFilter {
    pub fn new(input: OpHandle, predicate: NodePredicate) -> Rc<RefCell<CondFilter>> {
        let primary_key = input.input.borrow().get_schema().primary_key.clone();
        let cf = Rc::new(RefCell::new(CondFilter {
            input: input.input.clone(),
            predicate,
            primary_key,
            passing: RefCell::new(None),
            output: None,
        }));
        input.set_output(cf.clone());
        cf
    }

    fn pk_of(&self, row: &Row) -> Vec<Value> {
        self.primary_key
            .iter()
            .map(|k| row.get(k).cloned().unwrap_or(Value::Null))
            .collect()
    }

    /// Build the passing-pk set from the input (used to hydrate on first touch).
    fn hydrate(&self) -> BTreeSet<Vec<Value>> {
        self.input
            .borrow()
            .fetch(&FetchRequest::default())
            .into_iter()
            .filter(|n| (self.predicate)(n))
            .map(|n| self.pk_of(&n.row))
            .collect()
    }
}

impl Input for CondFilter {
    fn get_schema(&self) -> Rc<Schema> {
        self.input.borrow().get_schema()
    }

    fn fetch(&self, req: &FetchRequest) -> Vec<Node> {
        let nodes: Vec<Node> = self
            .input
            .borrow()
            .fetch(req)
            .into_iter()
            .filter(|n| (self.predicate)(n))
            .collect();
        if self.passing.borrow().is_none() {
            *self.passing.borrow_mut() = Some(self.hydrate());
        }
        nodes
    }
}

impl Operator for CondFilter {
    /// Incremental: only the changed node's membership can flip.
    fn push(&mut self, change: Change) -> Changes {
        if self.passing.borrow().is_none() {
            let h = self.hydrate();
            *self.passing.borrow_mut() = Some(h);
        }
        let mut guard = self.passing.borrow_mut();
        let set = guard.as_mut().unwrap();
        match change {
            Change::Add(node) => {
                if (self.predicate)(&node) {
                    set.insert(self.pk_of(&node.row));
                    smallvec![Change::Add(node)]
                } else {
                    Changes::new()
                }
            }
            Change::Remove(node) => {
                if set.remove(&self.pk_of(&node.row)) {
                    smallvec![Change::Remove(node)]
                } else {
                    Changes::new()
                }
            }
            Change::Edit { node, old_node } => {
                let pk = self.pk_of(&node.row);
                let was = set.contains(&pk);
                let now = (self.predicate)(&node);
                if was && now {
                    smallvec![Change::Edit { node, old_node }]
                } else if was {
                    set.remove(&pk);
                    smallvec![Change::Remove(old_node)]
                } else if now {
                    set.insert(pk);
                    smallvec![Change::Add(node)]
                } else {
                    Changes::new()
                }
            }
            Change::Child { node, relationship_name, change } => {
                let pk = self.pk_of(&node.row);
                let was = set.contains(&pk);
                let now = (self.predicate)(&node);
                if was && now {
                    smallvec![Change::Child { node, relationship_name, change }]
                } else if was {
                    set.remove(&pk);
                    smallvec![Change::Remove(node)]
                } else if now {
                    set.insert(pk);
                    smallvec![Change::Add(node)]
                } else {
                    Changes::new()
                }
            }
        }
    }

    fn output(&self) -> Option<Link> {
        self.output.clone()
    }
    fn set_output(&mut self, out: Link) {
        self.output = Some(out);
    }
}
