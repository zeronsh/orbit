//! The [`Take`] operator: limits the result to the first `limit` rows in sort
//! order (SQL `LIMIT`), optionally **partitioned** (a separate limit per group —
//! e.g. "3 comments per issue" in a related subquery).
//!
//! Port of `zql/src/ivm/take.ts`. Incrementally maintained: each partition keeps
//! its rows in sort order, and a push touches only the affected partition(s) and
//! re-diffs that partition's top-`limit` window — O(log n + limit) per change,
//! not a full recompute.

use super::node::{Change, Changes, Node};
use super::operator::{FetchRequest, Input, Link, OpHandle, Operator};
use super::schema::Schema;
use crate::value::{Comparator, Row, Value};
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::rc::Rc;

pub struct Take {
    input: Rc<RefCell<dyn Input>>,
    limit: usize,
    primary_key: Vec<String>,
    /// If set, apply `limit` per distinct value of these columns.
    partition_key: Option<Vec<String>>,
    /// Comparator for the input's sort order (positions rows within a partition).
    cmp: Comparator,
    /// All rows per partition, kept in sort order. `None` until hydrated.
    partitions: RefCell<Option<BTreeMap<Vec<Value>, Vec<Node>>>>,
    /// Last-emitted top-`limit` window per partition (for diffing).
    windows: RefCell<BTreeMap<Vec<Value>, Vec<Node>>>,
    output: Option<Link>,
}

impl Take {
    pub fn new(input: OpHandle, limit: usize) -> Rc<RefCell<Take>> {
        Self::partitioned(input, limit, None)
    }

    pub fn partitioned(
        input: OpHandle,
        limit: usize,
        partition_key: Option<Vec<String>>,
    ) -> Rc<RefCell<Take>> {
        let schema = input.input.borrow().get_schema();
        let take = Rc::new(RefCell::new(Take {
            input: input.input.clone(),
            limit,
            primary_key: schema.primary_key.clone(),
            partition_key,
            cmp: schema.compare_rows.clone(),
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

    /// Build per-partition sorted rows + windows from the input (first touch).
    fn ensure(&self) {
        if self.partitions.borrow().is_some() {
            return;
        }
        let mut parts: BTreeMap<Vec<Value>, Vec<Node>> = BTreeMap::new();
        // input.fetch is in sort order, so per-partition order is preserved.
        for node in self.input.borrow().fetch(&FetchRequest::default()) {
            parts.entry(self.partition_of(&node.row)).or_default().push(node);
        }
        let mut wins = self.windows.borrow_mut();
        for (p, arr) in &parts {
            wins.insert(p.clone(), arr.iter().take(self.limit).cloned().collect());
        }
        *self.partitions.borrow_mut() = Some(parts);
    }

    /// Apply `f` to a partition's sorted rows, then diff its top-`limit` window.
    fn mutate_partition<F: FnOnce(&mut Vec<Node>)>(&self, p: Vec<Value>, f: F, out: &mut Changes) {
        let new_win: Vec<Node> = {
            let mut parts = self.partitions.borrow_mut();
            let arr = parts.as_mut().unwrap().entry(p.clone()).or_default();
            f(arr);
            arr.iter().take(self.limit).cloned().collect()
        };
        let mut wins = self.windows.borrow_mut();
        let old_win = wins.remove(&p).unwrap_or_default();
        diff_window(&old_win, &new_win, &self.primary_key, out);
        wins.insert(p, new_win);
    }
}

impl Input for Take {
    fn get_schema(&self) -> Rc<Schema> {
        self.input.borrow().get_schema()
    }

    fn fetch(&self, req: &FetchRequest) -> Vec<Node> {
        self.ensure();
        if req.constraint.is_some() || self.partition_key.is_none() {
            // The input applies constraint/sort/start; limit the result.
            let mut nodes = self.input.borrow().fetch(req);
            nodes.truncate(self.limit);
            nodes
        } else {
            self.partitions
                .borrow()
                .as_ref()
                .unwrap()
                .values()
                .flat_map(|arr| arr.iter().take(self.limit).cloned())
                .collect()
        }
    }
}

impl Operator for Take {
    fn push(&mut self, change: Change) -> Changes {
        self.ensure();
        let mut out = Changes::new();
        match change {
            Change::Add(node) => {
                let p = self.partition_of(&node.row);
                self.mutate_partition(p, |arr| insert_sorted(arr, node, &self.cmp), &mut out);
            }
            Change::Remove(node) => {
                let p = self.partition_of(&node.row);
                self.mutate_partition(p, |arr| remove_by_pk(arr, &node.row, &self.primary_key), &mut out);
            }
            Change::Child { node, .. } => {
                let p = self.partition_of(&node.row);
                self.mutate_partition(p, |arr| replace_by_pk(arr, node, &self.primary_key), &mut out);
            }
            Change::Edit { node, old_node } => {
                let po = self.partition_of(&old_node.row);
                let pn = self.partition_of(&node.row);
                if po == pn {
                    self.mutate_partition(
                        po,
                        |arr| {
                            remove_by_pk(arr, &old_node.row, &self.primary_key);
                            insert_sorted(arr, node, &self.cmp);
                        },
                        &mut out,
                    );
                } else {
                    self.mutate_partition(po, |arr| remove_by_pk(arr, &old_node.row, &self.primary_key), &mut out);
                    self.mutate_partition(pn, |arr| insert_sorted(arr, node, &self.cmp), &mut out);
                }
            }
        }
        out
    }

    fn output(&self) -> Option<Link> {
        self.output.clone()
    }
    fn set_output(&mut self, out: Link) {
        self.output = Some(out);
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
