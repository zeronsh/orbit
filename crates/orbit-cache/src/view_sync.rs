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

use oql::ivm::{Change, Node, Schema};
use oql::value::{Row, Value};
use orbit_protocol::{RowPatchOp, RowsPatch};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;

/// A row's identity in a persisted client view: `(table, json(primary-key))`.
pub type RowKey = (String, String);

/// A client's materialized view: row identity → SHA-256 of the canonical row
/// JSON. A fingerprint is enough to decide whether a reconnect already has the
/// exact value, without retaining every client's full row payload in memory and
/// Postgres (message bodies can be tens of megabytes per client).
pub type ClientView = HashMap<RowKey, String>;

pub(crate) fn fingerprint_json(json: &str) -> String {
    format!("{:x}", Sha256::digest(json.as_bytes()))
}

pub(crate) fn is_fingerprint(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}

fn fingerprint_row(row: &Row) -> String {
    let json = serde_json::to_vec(row).unwrap_or_default();
    format!("{:x}", Sha256::digest(json))
}

/// Per-connection row reference counts + a fixed-size fingerprint of each row's
/// value — the core of a CVR. A row may be synced by several queries, so a `del`
/// is only sent once the last query drops it. Rows are keyed by their
/// primary-key *values* (`Value` is `Ord`); the value is stored as its SHA-256
/// fingerprint, computed when the row (or a new value for it) is registered.
///
/// Why a fingerprint and not the `RowRef` itself: pinning the `Rc<Row>` per
/// connection holds O(view bytes) heap PER CLIENT on backends whose fetched
/// rows are fresh allocations (SQLite replica — a 200 MB view × N clients was
/// an OOM), and made every checkpoint re-serialize + re-hash the entire view.
/// Hashing once per row *change* is strictly cheaper than hashing the whole
/// view once per checkpoint second.
#[derive(Default)]
pub struct RowRefs {
    /// table → (pk values → entry). Nested (rather than a flat
    /// `(String, Vec<Value>)` key) so the hot path looks the table up by `&str` —
    /// no per-event table-name `String` clone.
    tables: BTreeMap<String, BTreeMap<Vec<Value>, RowEntry>>,
    /// Primary-key column names per table, to rebuild named ids at checkpoint time.
    pks: HashMap<String, Vec<String>>,
}

struct RowEntry {
    count: u32,
    /// SHA-256 hex of the row's JSON at last registration (see [`fingerprint_row`]).
    fingerprint: String,
}

impl RowRefs {
    pub fn new() -> Self {
        RowRefs::default()
    }

    /// Rows currently referenced by this client's views (for metrics).
    pub fn len(&self) -> usize {
        self.tables.values().map(|t| t.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn register_pk(&mut self, table: &str, pk: &[String]) {
        if !self.pks.contains_key(table) {
            self.pks.insert(table.to_string(), pk.to_vec());
        }
    }
    /// Increment a key's refcount, recording the current row's fingerprint;
    /// true if first (0 → 1).
    fn incr(&mut self, table: &str, vals: Vec<Value>, row: &Row) -> bool {
        let fingerprint = fingerprint_row(row);
        // Table lookup by &str: allocates the table key only on first sight.
        let rows = match self.tables.get_mut(table) {
            Some(r) => r,
            None => self.tables.entry(table.to_string()).or_default(),
        };
        match rows.get_mut(&vals) {
            Some(e) => {
                e.count += 1;
                e.fingerprint = fingerprint;
                false
            }
            None => {
                rows.insert(vals, RowEntry { count: 1, fingerprint });
                true
            }
        }
    }
    /// Decrement; returns true if the last reference was dropped (1 → 0).
    fn decr(&mut self, table: &str, vals: &[Value]) -> bool {
        if let Some(rows) = self.tables.get_mut(table) {
            if let Some(e) = rows.get_mut(vals) {
                e.count -= 1;
                if e.count == 0 {
                    rows.remove(vals);
                    return true;
                }
            }
        }
        false
    }
    /// Update the stored value for an existing key (edit-in-place).
    fn touch(&mut self, table: &str, vals: &[Value], row: &Row) {
        if let Some(e) = self.tables.get_mut(table).and_then(|rows| rows.get_mut(vals)) {
            e.fingerprint = fingerprint_row(row);
        }
    }
    /// The client's current view (row identity → value fingerprint) for CVR
    /// checkpointing. Fingerprints were computed as rows changed, so this only
    /// rebuilds the identity keys — O(view rows), no row payloads touched.
    pub fn view(&self) -> ClientView {
        let mut out = HashMap::new();
        for (table, rows) in &self.tables {
            let Some(pk) = self.pks.get(table) else { continue };
            for (vals, e) in rows {
                let id: Row = pk.iter().cloned().zip(vals.iter().cloned()).collect();
                let id_json = serde_json::to_string(&id).unwrap_or_default();
                out.insert((table.clone(), id_json), e.fingerprint.clone());
            }
        }
        out
    }
}

/// Count the rows a hydration result would materialize (top-level nodes plus
/// all related children). Used for the per-query result cap BEFORE the
/// refcounted patch build mutates any client state.
pub fn count_result_rows(nodes: &[Node]) -> usize {
    fn walk(node: &Node) -> usize {
        1 + node
            .relationships
            .iter()
            .flat_map(|(_, children)| children.iter())
            .map(walk)
            .sum::<usize>()
    }
    nodes.iter().map(walk).sum()
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

/// Retraction patches for an entire query being dropped: decrement every row it
/// contributed and emit `del`s for rows no other live query still provides.
/// Without this, removing a query (TTL GC, view destroy) leaks its refcounts —
/// and a later genuine upstream DELETE of a shared row decrements to a nonzero
/// count and never reaches the client (a phantom row until full resync).
pub fn retract_patches_dedup(nodes: &[Node], schema: &Schema, refs: &mut RowRefs) -> RowsPatch {
    let mut out = Vec::new();
    for node in nodes {
        node_dels_dedup(node, schema, refs, &mut out);
    }
    out
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
                refs.touch(&schema.table_name, &pk_values(&node.row, &schema.primary_key), &node.row.0);
                out.push(RowPatchOp::Put { table_name: schema.table_name.clone(), value: Rc::clone(&node.row.0) });
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
        refs.incr(table, pk_values(&node.row, pk), &node.row.0);
        // On resume only (prior present), suppress the put if the client already
        // holds this exact value. The JSON here is off the hot path.
        let suppress = match prior {
            Some(p) => {
                let id_json = serde_json::to_string(&pk_id(&node.row, pk)).unwrap_or_default();
                let fingerprint = fingerprint_row(&node.row);
                p.get(&(table.clone(), id_json)) == Some(&fingerprint)
            }
            None => false,
        };
        if !suppress {
            out.push(RowPatchOp::Put { table_name: table.clone(), value: Rc::clone(&node.row.0) });
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
    if !schema.is_hidden
        && refs.decr(&schema.table_name, &pk_values(&node.row, &schema.primary_key))
    {
        out.push(RowPatchOp::Del {
            table_name: schema.table_name.clone(),
            id: pk_id(&node.row, &schema.primary_key),
        });
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
                    value: Rc::clone(&node.row.0),
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
            value: Rc::clone(&node.row.0),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_view_keeps_fixed_size_fingerprints_not_row_payloads() {
        let content = "x".repeat(1_000_000);
        let mut row = Row::new();
        row.insert("id", Value::String("m1".into()));
        row.insert("content", Value::String(content.clone()));

        let mut refs = RowRefs::new();
        refs.register_pk("messages", &["id".into()]);
        refs.incr("messages", vec![Value::String("m1".into())], &row);

        let view = refs.view();
        let fingerprint = view.values().next().unwrap();
        assert_eq!(fingerprint.len(), 64);
        assert!(is_fingerprint(fingerprint));
        assert!(!fingerprint.contains(&content));
    }

    #[test]
    fn fingerprint_changes_with_row_value() {
        assert_ne!(fingerprint_json("{\"id\":1}"), fingerprint_json("{\"id\":2}"));
        assert_eq!(fingerprint_json("{\"id\":1}"), fingerprint_json("{\"id\":1}"));
    }
}
