//! Bridge from the OQL IVM change stream to the wire protocol's row patches.
//!
//! This is the core of Zero's view-syncer: a materialized query's
//! [`Change`]s (Add/Remove/Edit/Child over a hierarchical [`Node`] tree) become
//! flat [`RowPatchOp`]s keyed by table name, which the client reassembles into
//! the query result tree.
//!
//! Hierarchical nodes are flattened by walking each node's relationships using
//! the [`Schema`]'s relationship → child-schema map (which carries the child
//! table name). Hidden relationships (junction edges) are flattened through:
//! their row is not emitted, but their children are.

use oql::ivm::{Change, Node, RowRef, Schema};
use oql::value::{Row, Value};
use orbit_protocol::{RowPatchOp, RowsPatch};
use std::collections::{BTreeMap, HashMap};

/// A row's identity in a persisted client view: `(table, json(primary-key))`.
pub type RowKey = (String, String);

/// A client's materialized view: row identity → canonical JSON of the row value.
/// This is the persisted CVR payload (what the client currently holds), so a
/// reconnect — to *any* node — can be served as a delta instead of a full resync.
pub type ClientView = HashMap<RowKey, String>;

/// Per-connection row reference counts + a cheap handle to each row's value — the
/// core of a CVR. A row may be synced by several queries, so a `del` is only sent
/// once the last query drops it. The hot path is JSON-free: rows are keyed by their
/// primary-key *values* (`Value` is `Ord`) and the value is held as a refcounted
/// [`RowRef`] (an `Rc` bump). JSON is produced only when the view is checkpointed
/// or diffed on reconnect — both off the per-mutation path.
#[derive(Default)]
pub struct RowRefs {
    rows: BTreeMap<(String, Vec<Value>), RowEntry>,
    /// Primary-key column names per table, to rebuild named ids at checkpoint time.
    pks: HashMap<String, Vec<String>>,
}

struct RowEntry {
    count: u32,
    row: RowRef,
}

impl RowRefs {
    pub fn new() -> Self {
        RowRefs::default()
    }
    fn register_pk(&mut self, table: &str, pk: &[String]) {
        if !self.pks.contains_key(table) {
            self.pks.insert(table.to_string(), pk.to_vec());
        }
    }
    /// Increment a key's refcount, recording the current row; true if first (0 → 1).
    fn incr(&mut self, key: (String, Vec<Value>), row: RowRef) -> bool {
        match self.rows.get_mut(&key) {
            Some(e) => {
                e.count += 1;
                e.row = row;
                false
            }
            None => {
                self.rows.insert(key, RowEntry { count: 1, row });
                true
            }
        }
    }
    /// Decrement; returns true if the last reference was dropped (1 → 0).
    fn decr(&mut self, key: &(String, Vec<Value>)) -> bool {
        if let Some(e) = self.rows.get_mut(key) {
            e.count -= 1;
            if e.count == 0 {
                self.rows.remove(key);
                return true;
            }
        }
        false
    }
    /// Update the stored value for an existing key (edit-in-place).
    fn touch(&mut self, key: &(String, Vec<Value>), row: RowRef) {
        if let Some(e) = self.rows.get_mut(key) {
            e.row = row;
        }
    }
    /// The client's current view (row identity → value JSON) for CVR checkpointing.
    /// Serializes here — never on the per-mutation hot path.
    pub fn view(&self) -> ClientView {
        let mut out = HashMap::with_capacity(self.rows.len());
        for ((table, vals), e) in &self.rows {
            let Some(pk) = self.pks.get(table) else { continue };
            let id: Row = pk.iter().cloned().zip(vals.iter().cloned()).collect();
            let id_json = serde_json::to_string(&id).unwrap_or_default();
            let val_json = serde_json::to_string(&*e.row).unwrap_or_default();
            out.insert((table.clone(), id_json), val_json);
        }
        out
    }
}

/// Like [`initial_patches`] but ref-counted (see [`RowRefs`]).
pub fn initial_patches_dedup(nodes: &[Node], schema: &Schema, refs: &mut RowRefs) -> RowsPatch {
    let mut out = Vec::new();
    for node in nodes {
        node_puts_dedup(node, schema, refs, &mut out, None);
    }
    out
}

/// Like [`initial_patches_dedup`] but for a *reconnecting* client whose prior view
/// is `prior`: suppresses puts for rows the client already holds with the same
/// value. After all queries are added, call [`resume_deletes`] to drop rows the
/// client had that no current query provides. This is the cross-node delta resume.
pub fn resume_patches_dedup(
    nodes: &[Node],
    schema: &Schema,
    refs: &mut RowRefs,
    prior: &ClientView,
) -> RowsPatch {
    let mut out = Vec::new();
    for node in nodes {
        node_puts_dedup(node, schema, refs, &mut out, Some(prior));
    }
    out
}

/// Deletions for rows in `prior` the resumed view no longer contains (rebuilt from
/// `refs`). Runs only on reconnect, so the serialization in [`RowRefs::view`] is fine.
pub fn resume_deletes(prior: &ClientView, refs: &RowRefs) -> RowsPatch {
    let current = refs.view();
    prior
        .keys()
        .filter(|k| !current.contains_key(*k))
        .map(|(table, id_json)| RowPatchOp::Del {
            table_name: table.clone(),
            id: serde_json::from_str(id_json).unwrap_or_else(|_| Row::new()),
        })
        .collect()
}

/// Like [`changes_to_patches`] but ref-counted (see [`RowRefs`]).
pub fn changes_to_patches_dedup(changes: &[Change], schema: &Schema, refs: &mut RowRefs) -> RowsPatch {
    let mut out = Vec::new();
    for change in changes {
        change_to_patches_dedup(change, schema, refs, &mut out);
    }
    out
}

fn change_to_patches_dedup(change: &Change, schema: &Schema, refs: &mut RowRefs, out: &mut RowsPatch) {
    match change {
        Change::Add(node) => node_puts_dedup(node, schema, refs, out, None),
        Change::Remove(node) => node_dels_dedup(node, schema, refs, out),
        Change::Edit { node, .. } => {
            if !schema.is_hidden {
                let key = (schema.table_name.clone(), pk_values(&node.row, &schema.primary_key));
                refs.touch(&key, node.row.clone());
                out.push(RowPatchOp::Put { table_name: schema.table_name.clone(), value: node.row.to_row() });
            }
        }
        Change::Child { relationship_name, change, .. } => {
            if let Some(child_schema) = schema.relationships.get(relationship_name) {
                change_to_patches_dedup(change, child_schema, refs, out);
            }
        }
    }
}

fn node_puts_dedup(
    node: &Node,
    schema: &Schema,
    refs: &mut RowRefs,
    out: &mut RowsPatch,
    prior: Option<&ClientView>,
) {
    if !schema.is_hidden {
        let table = &schema.table_name;
        let pk = &schema.primary_key;
        refs.register_pk(table, pk);
        refs.incr((table.clone(), pk_values(&node.row, pk)), node.row.clone());
        // On resume only (prior present), suppress the put if the client already
        // holds this exact value. The JSON here is off the hot path.
        let suppress = match prior {
            Some(p) => {
                let id_json = serde_json::to_string(&pk_id(&node.row, pk)).unwrap_or_default();
                let val_json = serde_json::to_string(&node.row.to_row()).unwrap_or_default();
                p.get(&(table.clone(), id_json)) == Some(&val_json)
            }
            None => false,
        };
        if !suppress {
            out.push(RowPatchOp::Put { table_name: table.clone(), value: node.row.to_row() });
        }
    }
    for (rel_name, children) in &node.relationships {
        if let Some(child_schema) = schema.relationships.get(rel_name) {
            for child in children {
                node_puts_dedup(child, child_schema, refs, out, prior);
            }
        }
    }
}

fn node_dels_dedup(node: &Node, schema: &Schema, refs: &mut RowRefs, out: &mut RowsPatch) {
    if !schema.is_hidden {
        let key = (schema.table_name.clone(), pk_values(&node.row, &schema.primary_key));
        if refs.decr(&key) {
            out.push(RowPatchOp::Del {
                table_name: schema.table_name.clone(),
                id: pk_id(&node.row, &schema.primary_key),
            });
        }
    }
    for (rel_name, children) in &node.relationships {
        if let Some(child_schema) = schema.relationships.get(rel_name) {
            for child in children {
                node_dels_dedup(child, child_schema, refs, out);
            }
        }
    }
}

/// The primary-key column *values* in pk order — a JSON-free row key for the hot path.
fn pk_values(row: &Row, primary_key: &[String]) -> Vec<Value> {
    primary_key.iter().map(|k| row.get(k).cloned().unwrap_or(Value::Null)).collect()
}

/// Convert the full current result set into `put` patches (the initial poke).
pub fn initial_patches(nodes: &[Node], schema: &Schema) -> RowsPatch {
    let mut out = Vec::new();
    for node in nodes {
        node_puts(node, schema, &mut out);
    }
    out
}

/// Convert a batch of incremental changes into row patches.
pub fn changes_to_patches(changes: &[Change], schema: &Schema) -> RowsPatch {
    let mut out = Vec::new();
    for change in changes {
        change_to_patches(change, schema, &mut out);
    }
    out
}

/// Convert one change into row patches, appending to `out`.
pub fn change_to_patches(change: &Change, schema: &Schema, out: &mut RowsPatch) {
    match change {
        Change::Add(node) => node_puts(node, schema, out),
        Change::Remove(node) => node_dels(node, schema, out),
        Change::Edit { node, .. } => {
            // The row identity may have changed; emit the new row as a put. (A
            // future refinement can emit `update` with a merge for efficiency.)
            if !schema.is_hidden {
                out.push(RowPatchOp::Put {
                    table_name: schema.table_name.clone(),
                    value: node.row.to_row(),
                });
            }
        }
        Change::Child {
            relationship_name,
            change,
            ..
        } => {
            if let Some(child_schema) = schema.relationships.get(relationship_name) {
                change_to_patches(change, child_schema, out);
            }
        }
    }
}

/// Emit `put`s for a node and (recursively) its related child nodes.
fn node_puts(node: &Node, schema: &Schema, out: &mut RowsPatch) {
    if !schema.is_hidden {
        out.push(RowPatchOp::Put {
            table_name: schema.table_name.clone(),
            value: node.row.to_row(),
        });
    }
    for (rel_name, children) in &node.relationships {
        if let Some(child_schema) = schema.relationships.get(rel_name) {
            for child in children {
                node_puts(child, child_schema, out);
            }
        }
    }
}

/// Emit `del`s for a node and (recursively) its related child nodes.
fn node_dels(node: &Node, schema: &Schema, out: &mut RowsPatch) {
    if !schema.is_hidden {
        out.push(RowPatchOp::Del {
            table_name: schema.table_name.clone(),
            id: pk_id(&node.row, &schema.primary_key),
        });
    }
    for (rel_name, children) in &node.relationships {
        if let Some(child_schema) = schema.relationships.get(rel_name) {
            for child in children {
                node_dels(child, child_schema, out);
            }
        }
    }
}

/// Extract a row containing only the primary-key columns (a row "id").
fn pk_id(row: &Row, primary_key: &[String]) -> Row {
    primary_key
        .iter()
        .map(|k| (k.clone(), row.get(k).cloned().unwrap_or(Value::Null)))
        .collect()
}
