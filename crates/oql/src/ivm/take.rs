//! The [`Take`] operator: limits the result to the first `limit` rows in sort
//! order (SQL `LIMIT`), optionally **partitioned** (a separate limit per group —
//! e.g. "3 comments per issue" in a related subquery).
//!
//! Port of `zql/src/ivm/take.ts`, with a different state model chosen for
//! robustness (Zero's bound-based state machine has needed repeated bug fixes;
//! recompute-and-diff hasn't): each partition keeps a **capped** prefix of its
//! rows in sort order — `limit` visible rows plus slack — and every change
//! re-diffs the emitted top-`limit` window against the previous one.
//!
//! The cap is what makes pushes cheap: state never exceeds `2·limit + 16` rows
//! per partition, so
//! * a change that sorts beyond the cap of a full partition is a no-op after a
//!   single comparison (the overwhelmingly common case for a limit query over a
//!   large table),
//! * in-cap churn costs O(cap) on the small capped vec, and
//! * only when removals drain the slack below `limit` (rare — the slack absorbs
//!   `limit + 16` net removals) does the partition refetch from the input.
//!   Refetches are bounded (`limit` hint) where the input chain is exact, and
//!   hit the source's sorted index for partitioned (join-correlated) takes.

use super::node::{Change, Changes, Node};
use super::operator::{FetchRequest, Input, Link, OpHandle, Operator};
use super::schema::Schema;
use crate::ivm::constraint::Constraint;
use crate::value::{Comparator, Row, Value};
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::rc::Rc;

/// One partition's capped, sorted row prefix.
struct Partition {
    /// The first `≤ cap` rows of the partition, in sort order.
    arr: Vec<Node>,
    /// Whether rows may exist beyond `arr` (knowledge was discarded at the cap).
    /// Invariant: `!maybe_more` ⇒ `arr` is the complete partition.
    maybe_more: bool,
}

pub struct Take {
    input: Rc<RefCell<dyn Input>>,
    limit: usize,
    /// Max rows tracked per partition: `limit` visible + slack that absorbs
    /// removals before a refetch is needed.
    cap: usize,
    primary_key: Vec<String>,
    /// If set, apply `limit` per distinct value of these columns.
    partition_key: Option<Vec<String>>,
    /// Comparator for the input's sort order (positions rows within a partition).
    cmp: Comparator,
    /// Whether the input chain below never filters rows (source [+ 1:1 joins]
    /// only), so a `limit` fetch hint is sound. Set by the builder.
    exact_input: bool,
    /// Capped rows per partition. `None` until hydrated.
    partitions: RefCell<Option<BTreeMap<Vec<Value>, Partition>>>,
    /// Last-emitted top-`limit` window per partition (for diffing).
    windows: RefCell<BTreeMap<Vec<Value>, Vec<Node>>>,
    output: Option<super::operator::WeakLink>,
}

impl Take {
    pub fn new(input: OpHandle, limit: usize) -> Rc<RefCell<Take>> {
        Self::partitioned(input, limit, None, false)
    }

    pub fn partitioned(
        input: OpHandle,
        limit: usize,
        partition_key: Option<Vec<String>>,
        exact_input: bool,
    ) -> Rc<RefCell<Take>> {
        let schema = input.input.borrow().get_schema();
        let take = Rc::new(RefCell::new(Take {
            input: input.input.clone(),
            limit,
            cap: limit.saturating_mul(2).saturating_add(16),
            primary_key: schema.primary_key.clone(),
            partition_key,
            cmp: schema.compare_rows.clone(),
            exact_input,
            partitions: RefCell::new(None),
            windows: RefCell::new(BTreeMap::new()),
            output: None,
        }));
        input.set_output(take.clone());
        take
    }

    fn partition_of(&self, row: &Row) -> Vec<Value> {
        match &self.partition_key {
            Some(keys) => keys.iter().map(|k| row.get(k).cloned().unwrap_or(Value::Null)).collect(),
            None => Vec::new(),
        }
    }

    /// Build per-partition capped rows + windows from the input (first touch).
    fn ensure(&self) {
        if self.partitions.borrow().is_some() {
            return;
        }
        // Bound the fetch only for exact, unpartitioned inputs (a partitioned
        // take needs every partition's rows, which a global bound would cut).
        let req = FetchRequest {
            limit: (self.exact_input && self.partition_key.is_none()).then_some(self.cap),
            ..FetchRequest::default()
        };
        let mut parts: BTreeMap<Vec<Value>, Partition> = BTreeMap::new();
        // input.fetch is in sort order, so per-partition order is preserved.
        for node in self.input.borrow().fetch(&req) {
            let entry = parts
                .entry(self.partition_of(&node.row))
                .or_insert_with(|| Partition { arr: Vec::new(), maybe_more: false });
            if entry.arr.len() < self.cap {
                entry.arr.push(node);
            } else {
                entry.maybe_more = true;
            }
        }
        // A bounded fetch that filled the cap may have been cut by the bound.
        if req.limit.is_some() {
            if let Some(p) = parts.get_mut(&Vec::new()) {
                if p.arr.len() == self.cap {
                    p.maybe_more = true;
                }
            }
        }
        let mut wins = self.windows.borrow_mut();
        for (p, part) in &parts {
            wins.insert(p.clone(), part.arr.iter().take(self.limit).cloned().collect());
        }
        *self.partitions.borrow_mut() = Some(parts);
    }

    /// Refetch one partition from the input (post-change state via the source
    /// overlay), restoring the capped prefix after slack underflow.
    fn refetch(&self, p: &[Value]) -> Partition {
        let req = match &self.partition_key {
            Some(keys) => {
                let constraint: Constraint =
                    keys.iter().cloned().zip(p.iter().cloned()).collect();
                FetchRequest { limit: Some(self.cap), ..FetchRequest::constrained(constraint) }
            }
            None => FetchRequest {
                limit: self.exact_input.then_some(self.cap),
                ..FetchRequest::default()
            },
        };
        let mut arr: Vec<Node> = self.input.borrow().fetch(&req);
        let mut maybe_more = false;
        if arr.len() > self.cap {
            arr.truncate(self.cap);
            maybe_more = true;
        } else if req.limit.is_some() && arr.len() == self.cap {
            maybe_more = true;
        }
        Partition { arr, maybe_more }
    }

    /// Apply `f` to a partition's capped rows, refetch on slack underflow, then
    /// diff its top-`limit` window against the previously emitted one.
    fn mutate_partition<F: FnOnce(&mut Partition, &Take)>(
        &self,
        p: Vec<Value>,
        f: F,
        out: &mut Changes,
    ) {
        let new_win: Vec<Node> = {
            let mut parts = self.partitions.borrow_mut();
            let part = parts
                .as_mut()
                .unwrap()
                .entry(p.clone())
                .or_insert_with(|| Partition { arr: Vec::new(), maybe_more: false });
            f(part, self);
            if part.arr.len() < self.limit && part.maybe_more {
                // Slack drained below the window: restore from the input (which
                // reflects the post-change state during a push).
                *part = self.refetch(&p);
            }
            part.arr.iter().take(self.limit).cloned().collect()
        };
        let mut wins = self.windows.borrow_mut();
        let old_win = wins.remove(&p).unwrap_or_default();
        diff_window(&old_win, &new_win, &self.primary_key, out);
        wins.insert(p, new_win);
    }

    /// True iff a row sorting like `row` lies strictly beyond a FULL capped
    /// prefix (so it can neither be tracked nor affect the window): fast no-op.
    fn beyond_full_cap(&self, part: Option<&Partition>, row: &Row) -> bool {
        match part {
            Some(p) if p.arr.len() == self.cap => {
                self.cmp.compare(&p.arr[self.cap - 1].row, row) == Ordering::Less
            }
            _ => false,
        }
    }
}

impl Input for Take {
    fn get_schema(&self) -> Rc<Schema> {
        self.input.borrow().get_schema()
    }

    fn fetch(&self, req: &FetchRequest) -> Vec<Node> {
        self.ensure();
        if req.constraint.is_none() && req.start.is_none() && !req.reverse {
            // Serve from state: per-partition (or the single) top-`limit` window.
            return self
                .partitions
                .borrow()
                .as_ref()
                .unwrap()
                .values()
                .flat_map(|part| part.arr.iter().take(self.limit).cloned())
                .collect();
        }
        // Constrained / cursored fetches: the input applies constraint/sort/
        // start; bound and truncate to the per-partition limit.
        let mut sub = req.clone();
        sub.limit = Some(self.limit.min(sub.limit.unwrap_or(usize::MAX)));
        let mut nodes = self.input.borrow().fetch(&sub);
        nodes.truncate(self.limit);
        nodes
    }
}

impl Operator for Take {
    fn push(&mut self, change: Change) -> Changes {
        self.ensure();
        let mut out = Changes::new();
        match change {
            Change::Add(node) => {
                let p = self.partition_of(&node.row);
                {
                    let parts = self.partitions.borrow();
                    let part = parts.as_ref().unwrap().get(&p);
                    if self.beyond_full_cap(part, &node.row) {
                        // Sorts beyond a full capped prefix: untrackable, and the
                        // window is unaffected. Record that rows exist beyond.
                        drop(parts);
                        self.partitions
                            .borrow_mut()
                            .as_mut()
                            .unwrap()
                            .get_mut(&p)
                            .unwrap()
                            .maybe_more = true;
                        return out;
                    }
                }
                self.mutate_partition(
                    p,
                    |part, take| {
                        insert_sorted(&mut part.arr, node, &take.cmp);
                        if part.arr.len() > take.cap {
                            part.arr.pop();
                            part.maybe_more = true;
                        }
                    },
                    &mut out,
                );
            }
            Change::Remove(node) => {
                let p = self.partition_of(&node.row);
                {
                    let parts = self.partitions.borrow();
                    let part = parts.as_ref().unwrap().get(&p);
                    if self.beyond_full_cap(part, &node.row) {
                        return out; // untracked row beyond the cap: window unaffected
                    }
                }
                self.mutate_partition(
                    p,
                    |part, take| remove_by_pk(&mut part.arr, &node.row, &take.primary_key),
                    &mut out,
                );
            }
            Change::Child { node, .. } => {
                let p = self.partition_of(&node.row);
                self.mutate_partition(
                    p,
                    |part, take| replace_by_pk(&mut part.arr, node, &take.primary_key),
                    &mut out,
                );
            }
            Change::Edit { node, old_node } => {
                let po = self.partition_of(&old_node.row);
                let pn = self.partition_of(&node.row);
                if po == pn {
                    {
                        let parts = self.partitions.borrow();
                        let part = parts.as_ref().unwrap().get(&po);
                        if self.beyond_full_cap(part, &old_node.row)
                            && self.beyond_full_cap(part, &node.row)
                        {
                            return out; // both sides beyond the cap: no-op
                        }
                    }
                    self.mutate_partition(
                        po,
                        |part, take| {
                            remove_by_pk(&mut part.arr, &old_node.row, &take.primary_key);
                            insert_sorted(&mut part.arr, node, &take.cmp);
                            if part.arr.len() > take.cap {
                                part.arr.pop();
                                part.maybe_more = true;
                            }
                        },
                        &mut out,
                    );
                } else {
                    self.mutate_partition(
                        po,
                        |part, take| remove_by_pk(&mut part.arr, &old_node.row, &take.primary_key),
                        &mut out,
                    );
                    self.mutate_partition(
                        pn,
                        |part, take| {
                            insert_sorted(&mut part.arr, node, &take.cmp);
                            if part.arr.len() > take.cap {
                                part.arr.pop();
                                part.maybe_more = true;
                            }
                        },
                        &mut out,
                    );
                }
            }
        }
        out
    }

    fn output(&self) -> Option<Link> {
        self.output.as_ref().and_then(std::rc::Weak::upgrade)
    }
    fn set_output(&mut self, out: Link) {
        self.output = Some(Rc::downgrade(&out));
    }
}

fn key_of(pk: &[String], row: &Row) -> Vec<Value> {
    pk.iter().map(|k| row.get(k).cloned().unwrap_or(Value::Null)).collect()
}

/// Insert `node` at its sorted position (total order via `cmp`).
fn insert_sorted(arr: &mut Vec<Node>, node: Node, cmp: &Comparator) {
    let (mut lo, mut hi) = (0usize, arr.len());
    while lo < hi {
        let mid = (lo + hi) / 2;
        if cmp.compare(&arr[mid].row, &node.row) == Ordering::Less {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    arr.insert(lo, node);
}

fn remove_by_pk(arr: &mut Vec<Node>, row: &Row, pk: &[String]) {
    let k = key_of(pk, row);
    if let Some(i) = arr.iter().position(|n| key_of(pk, &n.row) == k) {
        arr.remove(i);
    }
}

fn replace_by_pk(arr: &mut [Node], node: Node, pk: &[String]) {
    let k = key_of(pk, &node.row);
    if let Some(i) = arr.iter().position(|n| key_of(pk, &n.row) == k) {
        arr[i] = node;
    }
}

/// Diff two windows by primary key + full-node equality into add/remove changes.
fn diff_window(old: &[Node], new: &[Node], pk: &[String], out: &mut Changes) {
    let old_map: BTreeMap<Vec<Value>, &Node> = old.iter().map(|n| (key_of(pk, &n.row), n)).collect();
    let new_map: BTreeMap<Vec<Value>, &Node> = new.iter().map(|n| (key_of(pk, &n.row), n)).collect();
    for (k, o) in &old_map {
        match new_map.get(k) {
            None => out.push(Change::Remove((*o).clone())),
            Some(n) if n != o => out.push(Change::Remove((*o).clone())),
            _ => {}
        }
    }
    for (k, n) in &new_map {
        match old_map.get(k) {
            None => out.push(Change::Add((*n).clone())),
            Some(o) if o != n => out.push(Change::Add((*n).clone())),
            _ => {}
        }
    }
}
