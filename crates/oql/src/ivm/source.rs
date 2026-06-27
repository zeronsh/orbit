//! [`MemorySource`]: the in-memory root data source of a pipeline.
//!
//! Port of `zql/src/ivm/memory-source.ts`. Faithfully reproduces the **overlay**
//! mechanism: during a push, the source presents a post-change view to any
//! downstream `fetch` (the changed row is spliced in / out) *before* the change
//! is committed to storage. This is what makes stateful operators (Take, Exists)
//! and joins see a consistent view mid-push.
//!
//! Simplifications vs. Zero (correctness-preserving):
//! * No per-connection filter pushdown — filters are applied by a separate
//!   [`Filter`](super::filter::Filter) operator. (Equivalent results.)
//! * No split-edit at the source. Edits propagate as edits; the
//!   [`Filter`](super::filter::Filter) operator still splits edits across its
//!   predicate.
//!
//! Performance:
//! * Rows are stored as `Rc<Row>` so they're shared between primary storage,
//!   secondary indexes, and the overlay — cloning a row reference is a refcount
//!   bump, not a deep `BTreeMap` clone.
//! * **Secondary indexes**: a constrained fetch (the join-key lookup that
//!   dominates joins) consults a lazily-built, incrementally-maintained index
//!   keyed by the constraint columns — an O(log) bucket lookup instead of an
//!   O(N) scan. (This is the Rust analog of Zero's per-sort `BTreeSet` indexes.)

use super::node::{Change, Node, SourceChange};
use super::operator::{deliver, Basis, FetchRequest, Input, Link, Operator};
use super::schema::{ColumnType, Schema};
use crate::ast::Ordering as AstOrdering;
use crate::ivm::constraint::{constraint_matches_row, Constraint};
use crate::value::{values_equal, values_identical, Comparator, Direction, Ordering2, Row, Value};
use smallvec::SmallVec;
use std::cell::RefCell;
use std::cmp::Ordering as CmpOrdering;
use std::collections::BTreeMap;
use std::rc::Rc;

/// A key tuple (primary key or constraint values). Inline-stores a single
/// column (the common single-column PK / join key) with no heap allocation;
/// compound keys spill to the heap. Kept narrow (1) because `Value` is wide.
/// Keeping `data` a `BTreeMap` keyed by this tuple means rows iterate in PK
/// order, so a fetch ordered by the PK (the common case) sorts in ~O(n).
type KeyVec = SmallVec<[Value; 1]>;

/// A constraint-column-set secondary index: constraint values → matching rows.
type SecondaryIndex = BTreeMap<KeyVec, Vec<Rc<Row>>>;

/// An in-flight change held during a push, with rows already shared via `Rc` so
/// the overlay, the delivered node, and storage all reference one allocation.
#[derive(Clone)]
enum OverlayChange {
    Add(Rc<Row>),
    Remove(Rc<Row>),
    Edit { row: Rc<Row>, old: Rc<Row> },
}

impl OverlayChange {
    /// Move a [`SourceChange`]'s rows into `Rc`s (no deep clone).
    fn from_change(change: SourceChange) -> OverlayChange {
        match change {
            SourceChange::Add(row) => OverlayChange::Add(Rc::new(row)),
            SourceChange::Remove(row) => OverlayChange::Remove(Rc::new(row)),
            SourceChange::Edit { row, old_row } => {
                OverlayChange::Edit { row: Rc::new(row), old: Rc::new(old_row) }
            }
        }
    }

    /// The downstream change to deliver (nodes share the overlay's `Rc`s).
    fn to_change(&self) -> Change {
        match self {
            OverlayChange::Add(rc) => Change::Add(Node::from_rc(Rc::clone(rc))),
            OverlayChange::Remove(rc) => Change::Remove(Node::from_rc(Rc::clone(rc))),
            OverlayChange::Edit { row, old } => Change::Edit {
                node: Node::from_rc(Rc::clone(row)),
                old_node: Node::from_rc(Rc::clone(old)),
            },
        }
    }
}

/// A connection (one downstream consumer) of a [`MemorySource`].
struct Connection {
    output: Option<Link>,
    last_pushed_epoch: u64,
    schema: Rc<Schema>,
    /// Precomputed ordering + forward comparator (avoids rebuilding per fetch).
    order: Ordering2,
    cmp: Comparator,
}

/// An in-memory table source.
pub struct MemorySource {
    table_name: String,
    columns: BTreeMap<String, ColumnType>,
    /// Primary key in *declared* order (drives order-by tie-breaks).
    primary_key: Vec<String>,
    /// Rows keyed by primary-key tuple, in PK order. `Rc<Row>` so the same row
    /// is shared with secondary indexes without duplication.
    data: BTreeMap<KeyVec, Rc<Row>>,
    /// Lazily-built secondary indexes, keyed by the constraint column set.
    indexes: RefCell<BTreeMap<Vec<String>, SecondaryIndex>>,
    connections: Vec<Connection>,
    overlay: Option<(u64, OverlayChange)>,
    push_epoch: u64,
}

impl MemorySource {
    pub fn new(
        table_name: impl Into<String>,
        columns: BTreeMap<String, ColumnType>,
        primary_key: Vec<String>,
    ) -> Rc<RefCell<MemorySource>> {
        Rc::new(RefCell::new(MemorySource {
            table_name: table_name.into(),
            columns,
            primary_key,
            data: BTreeMap::new(),
            indexes: RefCell::new(BTreeMap::new()),
            connections: Vec::new(),
            overlay: None,
            push_epoch: 0,
        }))
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn primary_key(&self) -> &[String] {
        &self.primary_key
    }

    fn pk_of(&self, row: &Row) -> KeyVec {
        key_for(&self.primary_key, row)
    }

    /// Insert a row directly (initial data load, not a push). Bypasses change
    /// propagation but keeps any existing indexes consistent.
    pub fn insert_initial(&mut self, row: Row) {
        let rc = Rc::new(row);
        let key = self.pk_of(&rc);
        self.data.insert(key, Rc::clone(&rc));
        self.index_insert(&rc);
    }

    /// Look up the stored row matching the primary key of `key_row`.
    pub fn lookup(&self, key_row: &Row) -> Option<Row> {
        self.data.get(&self.pk_of(key_row)).map(|rc| (**rc).clone())
    }

    /// Reconcile to a new column set (a DDL schema change). Columns no longer
    /// present are dropped from every stored row; added columns simply appear in
    /// subsequently-replicated rows. Invalidates secondary indexes.
    pub fn reconcile_columns(&mut self, new_columns: &[(String, ColumnType)]) {
        let rebuilt: Vec<Rc<Row>> = self
            .data
            .values()
            .map(|rc| {
                let mut r = (**rc).clone();
                r.retain(|k, _| new_columns.iter().any(|(n, _)| n.as_str() == k));
                Rc::new(r)
            })
            .collect();
        self.data = rebuilt.into_iter().map(|rc| (self.pk_of(&rc), rc)).collect();
        self.columns = new_columns.iter().map(|(n, t)| (n.clone(), *t)).collect();
        self.indexes.borrow_mut().clear();
    }

    /// All stored rows (in primary-key order) — for snapshotting the replica.
    pub fn all_rows(&self) -> Vec<Row> {
        self.data.values().map(|rc| (**rc).clone()).collect()
    }

    /// Number of stored rows.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    fn build_order(&self, sort: &AstOrdering) -> Ordering2 {
        sort.iter()
            .map(|(f, d)| {
                (
                    f.clone(),
                    match d {
                        crate::ast::Direction::Asc => Direction::Asc,
                        crate::ast::Direction::Desc => Direction::Desc,
                    },
                )
            })
            .collect()
    }

    /// Base rows matching `constraint` via a secondary index (built lazily and
    /// maintained incrementally). Returns shared row references.
    fn bucket(&self, constraint: &Constraint) -> Vec<Rc<Row>> {
        let cols: Vec<String> = constraint.keys().cloned().collect();
        let vals: KeyVec = cols.iter().map(|k| constraint.get(k).cloned().unwrap_or(Value::Null)).collect();
        let mut indexes = self.indexes.borrow_mut();
        let index = indexes.entry(cols.clone()).or_insert_with(|| {
            let mut m = SecondaryIndex::default();
            for rc in self.data.values() {
                m.entry(key_for(&cols, rc)).or_default().push(Rc::clone(rc));
            }
            m
        });
        index.get(&vals).cloned().unwrap_or_default()
    }

    fn index_insert(&mut self, rc: &Rc<Row>) {
        for (cols, index) in self.indexes.get_mut().iter_mut() {
            index.entry(key_for(cols, rc)).or_default().push(Rc::clone(rc));
        }
    }

    fn index_remove(&mut self, row: &Row) {
        let pk = self.primary_key.clone();
        for (cols, index) in self.indexes.get_mut().iter_mut() {
            let k = key_for(cols, row);
            if let Some(v) = index.get_mut(&k) {
                v.retain(|x| !pk_eq(&pk, x, row));
                if v.is_empty() {
                    index.remove(&k);
                }
            }
        }
    }

    /// Fetch for connection `conn_idx`, applying overlay, constraint, sort, and
    /// start cursor.
    fn fetch_conn(&self, req: &FetchRequest, conn_idx: usize) -> Vec<Node> {
        let conn = &self.connections[conn_idx];

        // Base rows: an index bucket if constrained, else all rows.
        let mut rows: Vec<Rc<Row>> = match &req.constraint {
            Some(c) => self.bucket(c),
            None => self.data.values().map(Rc::clone).collect(),
        };

        // Overlay (only if this connection is at the current push epoch). Rows
        // from the bucket already satisfy the constraint; overlay additions are
        // re-checked against it.
        if let Some((epoch, change)) = &self.overlay {
            if conn.last_pushed_epoch >= *epoch {
                let matches = |r: &Row| req.constraint.as_ref().is_none_or(|c| constraint_matches_row(c, r));
                match change {
                    OverlayChange::Add(rc) => {
                        if matches(rc) {
                            rows.push(Rc::clone(rc));
                        }
                    }
                    OverlayChange::Remove(rc) => rows.retain(|x| !pk_eq(&self.primary_key, x, rc)),
                    OverlayChange::Edit { row, old } => {
                        rows.retain(|x| !pk_eq(&self.primary_key, x, old));
                        if matches(row) {
                            rows.push(Rc::clone(row));
                        }
                    }
                }
            }
        }

        // Sort by the connection's order (reverse only when requested).
        if req.reverse {
            let cmp = Comparator::new(conn.order.clone(), true);
            rows.sort_by(|a, b| cmp.compare(a, b));
            sort_start(&mut rows, req, &cmp);
        } else {
            rows.sort_by(|a, b| conn.cmp.compare(a, b));
            sort_start(&mut rows, req, &conn.cmp);
        }

        rows.into_iter().map(Node::from_rc).collect()
    }

    fn validate_precondition(&self, change: &SourceChange) {
        match change {
            SourceChange::Add(r) => {
                debug_assert!(
                    !self.data.contains_key(&self.pk_of(r)),
                    "MemorySource: row already exists on add"
                );
            }
            SourceChange::Remove(r) => {
                debug_assert!(
                    self.data.contains_key(&self.pk_of(r)),
                    "MemorySource: row not found on remove"
                );
            }
            SourceChange::Edit { old_row, .. } => {
                debug_assert!(
                    self.data.contains_key(&self.pk_of(old_row)),
                    "MemorySource: old row not found on edit"
                );
            }
        }
    }

    /// Commit an overlay change to storage + indexes, reusing its shared `Rc`s.
    fn write_change(&mut self, change: &OverlayChange) {
        match change {
            OverlayChange::Add(rc) => {
                let key = self.pk_of(rc);
                self.data.insert(key, Rc::clone(rc));
                self.index_insert(rc);
            }
            OverlayChange::Remove(rc) => {
                let key = self.pk_of(rc);
                self.data.remove(&key);
                self.index_remove(rc);
            }
            OverlayChange::Edit { row, old } => {
                let old_key = self.pk_of(old);
                self.data.remove(&old_key);
                self.index_remove(old);
                let key = self.pk_of(row);
                self.data.insert(key, Rc::clone(row));
                self.index_insert(row);
            }
        }
    }
}

/// Build the key tuple for `cols` from `row`.
fn key_for(cols: &[String], row: &Row) -> KeyVec {
    cols.iter().map(|c| row.get(c).cloned().unwrap_or(Value::Null)).collect()
}

/// Primary-key equality (PK values are never null; identity comparison).
fn pk_eq(primary_key: &[String], a: &Row, b: &Row) -> bool {
    primary_key.iter().all(|k| {
        values_identical(a.get(k).unwrap_or(&Value::Null), b.get(k).unwrap_or(&Value::Null))
    })
}

/// Apply a `start` cursor to already-sorted rows.
fn sort_start(rows: &mut Vec<Rc<Row>>, req: &FetchRequest, cmp: &Comparator) {
    if let Some(start) = &req.start {
        let pos = rows.iter().position(|r| {
            let c = cmp.compare(r, &start.row);
            match start.basis {
                Basis::At => c != CmpOrdering::Less,
                Basis::After => c == CmpOrdering::Greater,
            }
        });
        match pos {
            Some(p) => {
                rows.drain(..p);
            }
            None => rows.clear(),
        }
    }
}

/// Connect a new consumer to the source. Returns a handle that is both an
/// [`Input`] (fetch) and an [`Operator`] (so the builder can `set_output` to
/// wire the source's downstream).
pub fn connect(
    src: &Rc<RefCell<MemorySource>>,
    sort: AstOrdering,
) -> Rc<RefCell<SourceConnection>> {
    let conn_idx;
    {
        let mut s = src.borrow_mut();
        let order = s.build_order(&sort);
        let cmp = Comparator::new(order.clone(), false);
        let schema = Rc::new(Schema::leaf(
            s.table_name.clone(),
            s.columns.clone(),
            s.primary_key.clone(),
            Some(sort),
            cmp.clone(),
        ));
        conn_idx = s.connections.len();
        s.connections.push(Connection {
            output: None,
            last_pushed_epoch: 0,
            schema,
            order,
            cmp,
        });
    }
    Rc::new(RefCell::new(SourceConnection {
        source: Rc::clone(src),
        conn_idx,
    }))
}

/// Apply a [`SourceChange`] and propagate it to all connected outputs.
///
/// Mirrors `MemorySource.genPush`: validate, set the overlay, push the base
/// change to each connection (downstream fetches see the overlay), then clear
/// the overlay and commit to storage.
pub fn source_push(src: &Rc<RefCell<MemorySource>>, change: SourceChange) {
    src.borrow().validate_precondition(&change);

    // Move the change's rows into shared `Rc`s once; the overlay, the delivered
    // nodes, and storage all reference the same allocation (no deep clones).
    let overlay = OverlayChange::from_change(change);

    let epoch = {
        let mut s = src.borrow_mut();
        s.push_epoch += 1;
        let e = s.push_epoch;
        s.overlay = Some((e, overlay.clone()));
        e
    };

    let n = src.borrow().connections.len();
    for i in 0..n {
        let output = {
            let mut s = src.borrow_mut();
            s.connections[i].last_pushed_epoch = epoch;
            s.connections[i].output.clone()
        };
        if let Some(output) = output {
            deliver(&output, overlay.to_change());
        }
    }

    let mut s = src.borrow_mut();
    s.overlay = None;
    s.write_change(&overlay);
}

/// A handle to one connection of a [`MemorySource`].
pub struct SourceConnection {
    source: Rc<RefCell<MemorySource>>,
    conn_idx: usize,
}

impl Input for SourceConnection {
    fn get_schema(&self) -> Rc<Schema> {
        Rc::clone(&self.source.borrow().connections[self.conn_idx].schema)
    }
    fn fetch(&self, req: &FetchRequest) -> Vec<Node> {
        self.source.borrow().fetch_conn(req, self.conn_idx)
    }
}

impl Operator for SourceConnection {
    fn push(&mut self, _change: Change) -> super::node::Changes {
        unreachable!("a source connection never receives a push from upstream")
    }
    fn output(&self) -> Option<Link> {
        self.source.borrow().connections[self.conn_idx].output.clone()
    }
    fn set_output(&mut self, out: Link) {
        self.source.borrow_mut().connections[self.conn_idx].output = Some(out);
    }
}

/// `valuesEqual` re-export convenience for source-adjacent code.
#[allow(unused)]
fn _values_equal(a: &Value, b: &Value) -> bool {
    values_equal(a, b)
}
