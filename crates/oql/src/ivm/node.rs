//! The change stream: [`Node`], [`Change`], and [`SourceChange`].
//!
//! Port of `zql/src/ivm/change.ts`, `change-type.ts`, `data.ts` (Node), and
//! `source.ts` (SourceChange).
//!
//! ## Eager relationships
//!
//! Zero models relationships lazily (`Record<string, () => Stream<Node>>`). We
//! materialize them eagerly (`BTreeMap<String, Vec<Node>>`). Laziness in Zero is
//! a responsiveness/perf optimization; eager evaluation is semantically
//! equivalent and lets us materialize child streams *while the source overlay is
//! still active during a push*, which is exactly when they must be read.

use crate::value::{Row, Value};
use smallvec::SmallVec;
use std::collections::BTreeMap;
use std::ops::{Deref, Index};
use std::rc::Rc;

/// A reference-counted, shareable row. Cloning is a refcount bump, so rows flow
/// through the pipeline (source → operators → changes) without deep `BTreeMap`
/// clones. Derefs to [`Row`] and supports `row["col"]` indexing, so call sites
/// read like a plain row.
#[derive(Debug, Clone, PartialEq)]
pub struct RowRef(pub Rc<Row>);

impl RowRef {
    pub fn new(row: Row) -> Self {
        RowRef(Rc::new(row))
    }
    /// Clone out an owned [`Row`] (only needed at boundaries that require
    /// ownership, e.g. building wire patches).
    pub fn to_row(&self) -> Row {
        (*self.0).clone()
    }
}

impl Deref for RowRef {
    type Target = Row;
    fn deref(&self) -> &Row {
        &self.0
    }
}

impl Index<&str> for RowRef {
    type Output = Value;
    fn index(&self, key: &str) -> &Value {
        &self.0[key]
    }
}

/// Discriminant matching Zero's `ChangeType` enum (ADD=0, REMOVE=1, EDIT=2,
/// CHILD=3). Kept for parity / wire purposes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ChangeType {
    Add = 0,
    Remove = 1,
    Edit = 2,
    Child = 3,
}

/// A row flowing through the pipeline, plus its (eagerly materialized)
/// relationships.
#[derive(Debug, Clone, PartialEq)]
pub struct Node {
    pub row: RowRef,
    /// Child relationships keyed by relationship name. Ordered for determinism.
    pub relationships: BTreeMap<String, Vec<Node>>,
}

impl Node {
    /// A node with no relationships (wraps `row` in a fresh `Rc`).
    pub fn new(row: Row) -> Self {
        Node {
            row: RowRef::new(row),
            relationships: BTreeMap::new(),
        }
    }

    /// A node sharing an existing reference-counted row (no clone).
    pub fn from_rc(row: Rc<Row>) -> Self {
        Node {
            row: RowRef(row),
            relationships: BTreeMap::new(),
        }
    }

    pub fn with_relationships(row: Row, relationships: BTreeMap<String, Vec<Node>>) -> Self {
        Node {
            row: RowRef::new(row),
            relationships,
        }
    }
}

/// An incremental change flowing downstream through `push`.
///
/// Mirrors `zql/src/ivm/change.ts`.
#[derive(Debug, Clone, PartialEq)]
pub enum Change {
    /// A node (and all its children) added to the result.
    Add(Node),
    /// A node (and all its children) removed from the result.
    Remove(Node),
    /// The node's row is unchanged but a descendant changed. `node`'s
    /// relationships reflect the change; `change` specifies the descendant
    /// change at `relationship_name`.
    Child {
        node: Node,
        relationship_name: String,
        change: Box<Change>,
    },
    /// The row changed. If not split into remove+add, `node` and `old_node` have
    /// identical relationships and only the row differs.
    Edit { node: Node, old_node: Node },
}

/// The changes an operator emits from a single push. Inline-stores the common
/// 0–1 case, so a per-row update doesn't heap-allocate at each operator hop.
pub type Changes = SmallVec<[Change; 1]>;

impl Change {
    /// The (new) node carried by this change.
    pub fn node(&self) -> &Node {
        match self {
            Change::Add(n) | Change::Remove(n) => n,
            Change::Child { node, .. } => node,
            Change::Edit { node, .. } => node,
        }
    }

    pub fn change_type(&self) -> ChangeType {
        match self {
            Change::Add(_) => ChangeType::Add,
            Change::Remove(_) => ChangeType::Remove,
            Change::Edit { .. } => ChangeType::Edit,
            Change::Child { .. } => ChangeType::Child,
        }
    }
}

/// A change applied to a [`Source`](crate::ivm::source). Carries bare rows (no
/// relationships, since sources are leaves).
///
/// Mirrors `SourceChange` in `zql/src/ivm/source.ts`.
#[derive(Debug, Clone, PartialEq)]
pub enum SourceChange {
    Add(Row),
    Remove(Row),
    Edit { row: Row, old_row: Row },
}
