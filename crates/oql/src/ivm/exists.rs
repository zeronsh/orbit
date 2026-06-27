//! The [`Exists`] operator: filters parent rows by whether a (hidden) related
//! subquery has matching rows — i.e. `whereExists` / `EXISTS` / `NOT EXISTS`.
//!
//! Port of `zql/src/ivm/exists.ts`. It sits above a hidden [`Join`](super::join)
//! that materializes the relationship; Exists keeps only parents whose
//! relationship is non-empty (or empty, for `NOT EXISTS`).
//!
//! Strategy mirrors [`Take`](super::take): recompute the passing set on each
//! push and diff against the previous set. Because the relationship is hidden
//! (not synced), membership Add/Remove is the only output needed; changes to the
//! hidden relationship of a still-passing parent produce no patch. Parents are
//! compared by row identity, so hidden-relationship churn is ignored.

use super::node::{Change, Changes, Node};
use super::operator::{FetchRequest, Input, Link, OpHandle, Operator};
use super::schema::Schema;
use crate::value::{Row, Value};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

pub struct Exists {
    input: Rc<RefCell<dyn Input>>,
    relationship_name: String,
    negated: bool,
    primary_key: Vec<String>,
    /// Currently-passing parents, keyed by PK. `None` until hydrated.
    passing: RefCell<Option<BTreeMap<Vec<Value>, Node>>>,
    output: Option<Link>,
}

impl Exists {
    pub fn new(input: OpHandle, relationship_name: impl Into<String>, negated: bool) -> Rc<RefCell<Exists>> {
        let primary_key = input.input.borrow().get_schema().primary_key.clone();
        let exists = Rc::new(RefCell::new(Exists {
            input: input.input.clone(),
            relationship_name: relationship_name.into(),
            negated,
            primary_key,
            passing: RefCell::new(None),
            output: None,
        }));
        input.set_output(exists.clone());
        exists
    }

    fn passes(&self, node: &Node) -> bool {
        let non_empty = node
            .relationships
            .get(&self.relationship_name)
            .map(|c| !c.is_empty())
            .unwrap_or(false);
        non_empty != self.negated
    }

    fn pk_of(&self, row: &Row) -> Vec<Value> {
        self.primary_key
            .iter()
            .map(|k| row.get(k).cloned().unwrap_or(Value::Null))
            .collect()
    }

    fn compute_passing(&self) -> BTreeMap<Vec<Value>, Node> {
        self.input
            .borrow()
            .fetch(&FetchRequest::default())
            .into_iter()
            .filter(|n| self.passes(n))
            .map(|n| (self.pk_of(&n.row), n))
            .collect()
    }
}

impl Input for Exists {
    fn get_schema(&self) -> Rc<Schema> {
        self.input.borrow().get_schema()
    }

    fn fetch(&self, req: &FetchRequest) -> Vec<Node> {
        let nodes: Vec<Node> = self
            .input
            .borrow()
            .fetch(req)
            .into_iter()
            .filter(|n| self.passes(n))
            .collect();
        if self.passing.borrow().is_none() {
            *self.passing.borrow_mut() = Some(
                nodes
                    .iter()
                    .cloned()
                    .map(|n| (self.pk_of(&n.row), n))
                    .collect(),
            );
        }
        nodes
    }
}

impl Operator for Exists {
    fn push(&mut self, _change: Change) -> Changes {
        let new_passing = self.compute_passing();
        let old_passing = self.passing.borrow_mut().take().unwrap_or_default();

        let mut out = Changes::new();
        // Removed from the passing set, or row changed (re-emit).
        for (pk, old) in &old_passing {
            match new_passing.get(pk) {
                None => out.push(Change::Remove(old.clone())),
                Some(new) if new.row != old.row => out.push(Change::Remove(old.clone())),
                _ => {}
            }
        }
        // Newly passing, or row changed (re-emit).
        for (pk, new) in &new_passing {
            match old_passing.get(pk) {
                None => out.push(Change::Add(new.clone())),
                Some(old) if old.row != new.row => out.push(Change::Add(new.clone())),
                _ => {}
            }
        }

        *self.passing.borrow_mut() = Some(new_passing);
        out
    }

    fn output(&self) -> Option<Link> {
        self.output.clone()
    }
    fn set_output(&mut self, out: Link) {
        self.output = Some(out);
    }
}
