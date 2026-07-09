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

/// A secondary index for one (constraint columns, connection ordering) pair:
/// constraint values → matching rows, with **each bucket kept sorted** by the
/// ordering's comparator. Maintained incrementally on every write (an
/// upper-bound binary insert — equal sort keys keep arrival order, exactly like
/// the stable sort it replaces), so a constrained fetch returns the bucket
/// as-is instead of re-sorting it: O(k log k) comparator calls per fetch become
/// O(log k) per write. This is what keeps join `push_child` flat as a join
/// key's fan-in grows (the previous per-fetch sort made it quadratic overall).
struct SortedIndex {
    cols: Vec<String>,
    order: Ordering2,
    cmp: Comparator,
    map: BTreeMap<KeyVec, Vec<Rc<Row>>>,
}

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
    /// Weak: the pipeline owns itself top-down (Catch → … → SourceConnection);
    /// when a query is dropped the chain unravels and this upgrade fails, at
    /// which point the push loop prunes the connection. Keeps query churn from
    /// leaking dead pipelines that would receive every future change forever.
    output: Option<super::operator::WeakLink>,
    active_pos: usize,
    last_pushed_epoch: u64,
    schema: Rc<Schema>,
    /// Precomputed ordering + forward comparator (avoids rebuilding per fetch).
    order: Ordering2,
    cmp: Comparator,
    /// Whether `order` covers every primary-key column (a TOTAL order — no two
    /// distinct rows compare equal). Gates the unstable partial-select fast path
    /// for limited fetches, which is only deterministic without ties.
    order_total: bool,
    /// Whether `order` is exactly the primary key, ascending. Then `data`'s
    /// iteration order already matches and the adaptive sort is ~O(n), so the
    /// partial-select fast path would be a pessimization.
    order_is_pk_asc: bool,
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
    /// Lazily-built secondary indexes, one per (constraint columns, ordering)
    /// pair in use. A handful per source, so identity lookup is a linear scan.
    indexes: RefCell<Vec<SortedIndex>>,
    /// Stable reusable slots plus a dense live-slot index: teardown is eager,
    /// and push cost is proportional to active queries only.
    connections: Vec<Option<Connection>>,
    active_connections: Vec<usize>,
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
            indexes: RefCell::new(Vec::new()),
            connections: Vec::new(),
            active_connections: Vec::new(),
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

    /// Number of live query connections attached to this source.
    pub fn connection_count(&self) -> usize {
        self.active_connections.len()
    }

    fn disconnect(&mut self, conn_idx: usize) {
        let Some(connection) = self.connections[conn_idx].take() else {
            return;
        };
        let active_pos = connection.active_pos;
        debug_assert_eq!(self.active_connections[active_pos], conn_idx);
        self.active_connections.swap_remove(active_pos);
        if let Some(&moved_idx) = self.active_connections.get(active_pos) {
            self.connections[moved_idx].as_mut().unwrap().active_pos = active_pos;
        }
        while self.connections.last().is_some_and(Option::is_none) {
            self.connections.pop();
        }
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

    /// Base rows matching `constraint` in `order`, via a secondary index that is
    /// built lazily and maintained incrementally **pre-sorted** — the returned
    /// bucket needs no per-fetch sort. Returns shared row references.
    fn bucket(&self, constraint: &Constraint, order: &Ordering2, cmp: &Comparator) -> Vec<Rc<Row>> {
        let cols: Vec<String> = constraint.keys().cloned().collect();
        let vals: KeyVec = cols.iter().map(|k| constraint.get(k).cloned().unwrap_or(Value::Null)).collect();
        let mut indexes = self.indexes.borrow_mut();
        let index = match indexes.iter().position(|i| i.cols == cols && &i.order == order) {
            Some(i) => &indexes[i],
            None => {
                let mut map: BTreeMap<KeyVec, Vec<Rc<Row>>> = BTreeMap::new();
                for rc in self.data.values() {
                    map.entry(key_for(&cols, rc)).or_default().push(Rc::clone(rc));
                }
                // Stable sort of insertion-ordered buckets: equal sort keys keep
                // arrival order — the invariant incremental inserts preserve.
                for bucket in map.values_mut() {
                    bucket.sort_by(|a, b| cmp.compare(a, b));
                }
                indexes.push(SortedIndex {
                    cols,
                    order: order.clone(),
                    cmp: cmp.clone(),
                    map,
                });
                indexes.last().unwrap()
            }
        };
        index.map.get(&vals).cloned().unwrap_or_default()
    }

    fn index_insert(&mut self, rc: &Rc<Row>) {
        for index in self.indexes.get_mut().iter_mut() {
            let bucket = index.map.entry(key_for(&index.cols, rc)).or_default();
            insert_sorted(bucket, rc, &index.cmp);
        }
    }

    fn index_remove(&mut self, row: &Row) {
        let pk = self.primary_key.clone();
        for index in self.indexes.get_mut().iter_mut() {
            let k = key_for(&index.cols, row);
            if let Some(v) = index.map.get_mut(&k) {
                v.retain(|x| !pk_eq(&pk, x, row));
                if v.is_empty() {
                    index.map.remove(&k);
                }
            }
        }
    }

    /// Fetch for connection `conn_idx`, applying overlay, constraint, sort, and
    /// start cursor.
    fn fetch_conn(&self, req: &FetchRequest, conn_idx: usize) -> Vec<Node> {
        let conn = self.connections[conn_idx]
            .as_ref()
            .expect("source connection slot was disconnected");
        let overlay = self
            .overlay
            .as_ref()
            .filter(|(epoch, _)| conn.last_pushed_epoch >= *epoch)
            .map(|(_, change)| change);

        let mut rows: Vec<Rc<Row>>;
        if let Some(c) = &req.constraint {
            // Constrained: the index bucket is ALREADY sorted by this connection's
            // order (maintained incrementally), so no per-fetch sort. The overlay
            // is spliced in sort position: an upper-bound insert lands equal sort
            // keys after existing ones — exactly where the stable sort used to put
            // a row appended at the end. Overlay additions are re-checked against
            // the constraint; bucket rows satisfy it by construction.
            rows = self.bucket(c, &conn.order, &conn.cmp);
            if let Some(change) = overlay {
                match change {
                    OverlayChange::Add(rc) => {
                        if constraint_matches_row(c, rc) {
                            insert_sorted(&mut rows, rc, &conn.cmp);
                        }
                    }
                    OverlayChange::Remove(rc) => rows.retain(|x| !pk_eq(&self.primary_key, x, rc)),
                    OverlayChange::Edit { row, old } => {
                        rows.retain(|x| !pk_eq(&self.primary_key, x, old));
                        if constraint_matches_row(c, row) {
                            insert_sorted(&mut rows, row, &conn.cmp);
                        }
                    }
                }
            }
            if req.reverse {
                // Stable sort with the reversed comparator (as before): on the
                // ascending input this is a cheap adaptive pass, and it keeps
                // arrival order among equal sort keys (a plain `reverse()` would
                // flip it).
                let cmp = Comparator::new(conn.order.clone(), true);
                rows.sort_by(|a, b| cmp.compare(a, b));
                sort_start(&mut rows, req, &cmp);
            } else {
                sort_start(&mut rows, req, &conn.cmp);
            }
            if let Some(k) = req.limit {
                rows.truncate(k);
            }
        } else {
            // Unconstrained: `data` iterates in PK order, so the common
            // PK-ordered fetch is a near-O(n) adaptive sort. Deliberately not
            // index-maintained: hydrates are one-shot, while maintaining a
            // whole-table sorted copy would tax every push (see filter bench).
            rows = self.data.values().map(Rc::clone).collect();
            if let Some(change) = overlay {
                match change {
                    OverlayChange::Add(rc) => rows.push(Rc::clone(rc)),
                    OverlayChange::Remove(rc) => rows.retain(|x| !pk_eq(&self.primary_key, x, rc)),
                    OverlayChange::Edit { row, old } => {
                        rows.retain(|x| !pk_eq(&self.primary_key, x, old));
                        rows.push(Rc::clone(row));
                    }
                }
            }
            if req.reverse {
                let cmp = Comparator::new(conn.order.clone(), true);
                rows.sort_by(|a, b| cmp.compare(a, b));
                sort_start(&mut rows, req, &cmp);
                if let Some(k) = req.limit {
                    rows.truncate(k);
                }
            } else if let Some(k) = req.limit.filter(|k| {
                conn.order_total && !conn.order_is_pk_asc && req.start.is_none() && *k < rows.len()
            }) {
                // Partial select: only the top `k` rows are returned, so don't
                // sort the rest. Unstable selection is deterministic here because
                // the order is TOTAL (no two distinct rows compare equal). Worth
                // it only for non-PK orders — PK-ordered input is already sorted.
                rows.select_nth_unstable_by(k, |a, b| conn.cmp.compare(a, b));
                rows.truncate(k);
                rows.sort_by(|a, b| conn.cmp.compare(a, b));
            } else {
                rows.sort_by(|a, b| conn.cmp.compare(a, b));
                sort_start(&mut rows, req, &conn.cmp);
                if let Some(k) = req.limit {
                    rows.truncate(k);
                }
            }
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

/// Insert `rc` into `rows` (sorted ascending by `cmp`) at its **upper bound** —
/// after any rows with an equal sort key. This matches the stable sort it
/// replaces: a row appended and then stably sorted also lands after its equals,
/// so incremental maintenance and full sorting produce identical bucket order.
fn insert_sorted(rows: &mut Vec<Rc<Row>>, rc: &Rc<Row>, cmp: &Comparator) {
    let pos = rows.partition_point(|x| cmp.compare(x, rc) != CmpOrdering::Greater);
    rows.insert(pos, Rc::clone(rc));
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
        let order_total = s.primary_key.iter().all(|k| order.iter().any(|(f, _)| f == k));
        let order_is_pk_asc = order.len() == s.primary_key.len()
            && order
                .iter()
                .zip(s.primary_key.iter())
                .all(|((f, d), k)| f == k && *d == Direction::Asc);
        let connection = Connection {
            output: None,
            active_pos: s.active_connections.len(),
            last_pushed_epoch: 0,
            schema,
            order,
            cmp,
            order_total,
            order_is_pk_asc,
        };
        conn_idx = match s.connections.iter().position(Option::is_none) {
            Some(idx) => {
                s.connections[idx] = Some(connection);
                idx
            }
            None => {
                s.connections.push(Some(connection));
                s.connections.len() - 1
            }
        };
        s.active_connections.push(conn_idx);
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

    let n = src.borrow().active_connections.len();
    for active_pos in 0..n {
        let output = {
            let mut s = src.borrow_mut();
            let conn_idx = s.active_connections[active_pos];
            let conn = s.connections[conn_idx].as_mut().unwrap();
            conn.last_pushed_epoch = epoch;
            conn.output.as_ref().and_then(std::rc::Weak::upgrade)
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
        let source = self.source.borrow();
        Rc::clone(
            &source.connections[self.conn_idx]
                .as_ref()
                .expect("source connection slot was disconnected")
                .schema,
        )
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
        self.source.borrow().connections[self.conn_idx]
            .as_ref()
            .and_then(|conn| conn.output.as_ref())
            .and_then(std::rc::Weak::upgrade)
    }
    fn set_output(&mut self, out: Link) {
        self.source.borrow_mut().connections[self.conn_idx]
            .as_mut()
            .expect("source connection slot was disconnected")
            .output = Some(Rc::downgrade(&out));
    }
}

impl Drop for SourceConnection {
    fn drop(&mut self) {
        self.source.borrow_mut().disconnect(self.conn_idx);
    }
}

/// `valuesEqual` re-export convenience for source-adjacent code.
#[allow(unused)]
fn _values_equal(a: &Value, b: &Value) -> bool {
    values_equal(a, b)
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;
    use crate::ast::Direction as AstDirection;
    use crate::ivm::operator::OpHandle;
    use crate::ivm::{Catch, Filter, Join, Predicate, Take};

    fn string_columns(names: &[&str]) -> BTreeMap<String, ColumnType> {
        names.iter().map(|name| (name.to_string(), ColumnType::String)).collect()
    }

    fn ascending(column: &str) -> AstOrdering {
        vec![(column.to_string(), AstDirection::Asc)]
    }

    #[test]
    fn dropping_terminal_disconnects_and_reuses_source_slot() {
        let source = MemorySource::new("item", string_columns(&["id"]), vec!["id".into()]);
        let build = || {
            let connection = OpHandle::new(connect(&source, ascending("id")));
            let predicate: Predicate = Rc::new(|_| true);
            let filter = OpHandle::new(Filter::new(connection, predicate));
            let top = OpHandle::new(Take::new(filter, 10));
            let catch = Catch::new(Rc::clone(&top.input));
            top.set_output(catch.clone());
            catch
        };

        let first = build();
        let second = build();
        assert_eq!(source.borrow().connections.len(), 2);
        drop(first);
        assert_eq!(source.borrow().connection_count(), 1);

        let replacement = build();
        assert_eq!(source.borrow().connection_count(), 2);
        assert_eq!(source.borrow().connections.len(), 2, "the vacant slot is reused");

        drop(second);
        drop(replacement);
        assert_eq!(source.borrow().connection_count(), 0);
        assert!(source.borrow().connections.is_empty());
    }

    #[test]
    fn dropping_join_terminal_releases_both_branches_and_ports() {
        let parent = MemorySource::new("parent", string_columns(&["id"]), vec!["id".into()]);
        let child = MemorySource::new(
            "child",
            string_columns(&["id", "parent_id"]),
            vec!["id".into()],
        );

        let (catch, weak_join) = {
            let parent_connection = OpHandle::new(connect(&parent, ascending("id")));
            let child_connection = OpHandle::new(connect(&child, ascending("id")));
            let join = Join::new(
                parent_connection,
                child_connection,
                vec!["id".into()],
                vec!["parent_id".into()],
                "children",
                false,
            );
            let weak_join = Rc::downgrade(&join);
            let top = OpHandle::new(join);
            let catch = Catch::new(Rc::clone(&top.input));
            top.set_output(catch.clone());
            (catch, weak_join)
        };

        assert_eq!(parent.borrow().connection_count(), 1);
        assert_eq!(child.borrow().connection_count(), 1);
        drop(catch);
        assert!(weak_join.upgrade().is_none());
        assert_eq!(parent.borrow().connection_count(), 0);
        assert_eq!(child.borrow().connection_count(), 0);
    }
}
