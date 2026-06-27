//! The [`Join`] operator: a hierarchical (non-flattening) join.
//!
//! Port of `zql/src/ivm/join.ts`. Zero's join outputs hierarchical data: each
//! parent node gains a child *relationship* (a list of matching child nodes)
//! rather than producing a flattened cartesian product.
//!
//! Two inputs (parent, child) feed one Join. Because the [`Operator`] trait has
//! a single `push`, each input is wired to a small adapter port
//! ([`JoinParentPort`] / [`JoinChildPort`]) that forwards to
//! [`Join::push_parent`] / [`Join::push_child`].
//!
//! Child relationships are materialized **eagerly** during `fetch`/`push`. This
//! relies on the [`MemorySource`](super::source) overlay being active during a
//! push so child fetches observe the post-change state — replacing Zero's lazy
//! `generateWithOverlay` bookkeeping.

use super::node::{Change, Changes, Node, RowRef};
use super::operator::{FetchRequest, Input, Link, OpHandle, Operator};
use super::schema::Schema;
use crate::ast::System;
use crate::ivm::constraint::{build_join_constraint, row_equals_for_compound_key};
use smallvec::{smallvec, SmallVec};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

pub struct Join {
    parent: Rc<RefCell<dyn Input>>,
    child: Rc<RefCell<dyn Input>>,
    parent_key: Vec<String>,
    child_key: Vec<String>,
    relationship_name: String,
    output: Option<Link>,
    schema: Rc<Schema>,
}

impl Join {
    /// Build a Join and wire its two inputs to forwarding ports. Returns the
    /// Join handle (usable as both [`Input`] for downstream fetch and
    /// [`Operator`] for `set_output`).
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        parent: OpHandle,
        child: OpHandle,
        parent_key: Vec<String>,
        child_key: Vec<String>,
        relationship_name: impl Into<String>,
        hidden: bool,
        system: System,
    ) -> Rc<RefCell<Join>> {
        assert_eq!(
            parent_key.len(),
            child_key.len(),
            "parentKey and childKey must have the same length"
        );
        let relationship_name = relationship_name.into();

        let parent_schema = parent.input.borrow().get_schema();
        let child_schema = child.input.borrow().get_schema();
        let mut child_schema_for_rel = (*child_schema).clone();
        child_schema_for_rel.is_hidden = hidden;
        child_schema_for_rel.system = system;
        let mut schema = (*parent_schema).clone();
        schema
            .relationships
            .insert(relationship_name.clone(), Rc::new(child_schema_for_rel));

        let join = Rc::new(RefCell::new(Join {
            parent: parent.input.clone(),
            child: child.input.clone(),
            parent_key,
            child_key,
            relationship_name,
            output: None,
            schema: Rc::new(schema),
        }));

        // Wire inputs to forwarding ports.
        let parent_port: Link = Rc::new(RefCell::new(JoinParentPort {
            join: Rc::clone(&join),
        }));
        let child_port: Link = Rc::new(RefCell::new(JoinChildPort {
            join: Rc::clone(&join),
        }));
        parent.set_output(parent_port);
        child.set_output(child_port);

        join
    }

    /// Wrap a parent node, adding this join's child relationship (eagerly
    /// materialized from the child input).
    fn process_parent_node(&self, parent_row: &RowRef, parent_rels: &BTreeMap<String, Vec<Node>>) -> Node {
        let mut relationships = parent_rels.clone();
        let children = match build_join_constraint(parent_row, &self.parent_key, &self.child_key) {
            Some(constraint) => self.child.borrow().fetch(&FetchRequest::constrained(constraint)),
            None => Vec::new(),
        };
        relationships.insert(self.relationship_name.clone(), children);
        Node {
            row: parent_row.clone(),
            relationships,
        }
    }

    fn push_parent(&mut self, change: Change) -> Changes {
        match change {
            Change::Add(node) => smallvec![Change::Add(self.process_parent_node(&node.row, &node.relationships))],
            Change::Remove(node) => {
                smallvec![Change::Remove(self.process_parent_node(&node.row, &node.relationships))]
            }
            Change::Child {
                node,
                relationship_name,
                change,
            } => smallvec![Change::Child {
                node: self.process_parent_node(&node.row, &node.relationships),
                relationship_name,
                change,
            }],
            Change::Edit { node, old_node } => {
                if row_equals_for_compound_key(&old_node.row, &node.row, &self.parent_key) {
                    smallvec![Change::Edit {
                        node: self.process_parent_node(&node.row, &node.relationships),
                        old_node: self.process_parent_node(&old_node.row, &old_node.relationships),
                    }]
                } else {
                    // The edit moves the parent to a different join key, so its
                    // matching child set changes. Split into remove (old) + add
                    // (new) — the same final state, expressed as the two changes
                    // the downstream operators + view expect.
                    smallvec![
                        Change::Remove(self.process_parent_node(&old_node.row, &old_node.relationships)),
                        Change::Add(self.process_parent_node(&node.row, &node.relationships)),
                    ]
                }
            }
        }
    }

    fn push_child(&mut self, change: Change) -> Changes {
        // The child rows whose parents are affected. For an edit that changes the
        // join key, BOTH the old parents (lose this child) and the new parents
        // (gain it) must be re-materialized; the two key values are disjoint.
        let mut child_rows: SmallVec<[RowRef; 2]> = smallvec![change.node().row.clone()];
        if let Change::Edit { node, old_node } = &change {
            if !row_equals_for_compound_key(&old_node.row, &node.row, &self.child_key) {
                child_rows = smallvec![node.row.clone(), old_node.row.clone()];
            }
        }
        let mut out = Changes::new();
        for child_row in &child_rows {
            let constraint = match build_join_constraint(child_row, &self.child_key, &self.parent_key) {
                Some(c) => c,
                None => continue,
            };
            for parent_node in self.parent.borrow().fetch(&FetchRequest::constrained(constraint)) {
                out.push(Change::Child {
                    node: self.process_parent_node(&parent_node.row, &parent_node.relationships),
                    relationship_name: self.relationship_name.clone(),
                    change: Box::new(change.clone()),
                });
            }
        }
        out
    }
}

impl Input for Join {
    fn get_schema(&self) -> Rc<Schema> {
        Rc::clone(&self.schema)
    }
    fn fetch(&self, req: &FetchRequest) -> Vec<Node> {
        self.parent
            .borrow()
            .fetch(req)
            .into_iter()
            .map(|n| self.process_parent_node(&n.row, &n.relationships))
            .collect()
    }
}

impl Operator for Join {
    fn push(&mut self, _change: Change) -> Changes {
        unreachable!("Join is pushed via its parent/child ports, not directly")
    }
    fn output(&self) -> Option<Link> {
        self.output.clone()
    }
    fn set_output(&mut self, out: Link) {
        self.output = Some(out);
    }
}

/// Adapter: forwards an input's push to [`Join::push_parent`].
struct JoinParentPort {
    join: Rc<RefCell<Join>>,
}
impl Operator for JoinParentPort {
    fn push(&mut self, change: Change) -> Changes {
        self.join.borrow_mut().push_parent(change)
    }
    fn output(&self) -> Option<Link> {
        self.join.borrow().output.clone()
    }
    fn set_output(&mut self, _out: Link) {
        unreachable!("a join port's output follows the join's output")
    }
}

/// Adapter: forwards an input's push to [`Join::push_child`].
struct JoinChildPort {
    join: Rc<RefCell<Join>>,
}
impl Operator for JoinChildPort {
    fn push(&mut self, change: Change) -> Changes {
        self.join.borrow_mut().push_child(change)
    }
    fn output(&self) -> Option<Link> {
        self.join.borrow().output.clone()
    }
    fn set_output(&mut self, _out: Link) {
        unreachable!("a join port's output follows the join's output")
    }
}
