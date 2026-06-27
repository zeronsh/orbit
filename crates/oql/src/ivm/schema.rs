//! [`Schema`]: metadata describing the nodes an operator outputs.
//!
//! Port of `SourceSchema` in `zql/src/ivm/schema.ts`.

use crate::ast::{Ordering, System};
use crate::value::Comparator;
use std::collections::BTreeMap;
use std::rc::Rc;

/// A column's logical type. Mirrors `SchemaValue` (the parts the engine cares
/// about). Wire/schema-builder details live in `orbit-schema`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ColumnType {
    String,
    Number,
    Boolean,
    Json,
    Null,
}

/// Information about the nodes output by an operator.
#[derive(Clone)]
pub struct Schema {
    pub table_name: String,
    pub columns: BTreeMap<String, ColumnType>,
    pub primary_key: Vec<String>,
    /// Child relationships, keyed by relationship name.
    pub relationships: BTreeMap<String, Rc<Schema>>,
    pub is_hidden: bool,
    pub system: System,
    /// Comparator establishing the order rows are emitted in.
    pub compare_rows: Comparator,
    /// The ordering rows are emitted in, if any. `None` means unordered.
    pub sort: Option<Ordering>,
}

impl Schema {
    /// Build a leaf (source) schema.
    pub fn leaf(
        table_name: impl Into<String>,
        columns: BTreeMap<String, ColumnType>,
        primary_key: Vec<String>,
        sort: Option<Ordering>,
        compare_rows: Comparator,
    ) -> Self {
        Schema {
            table_name: table_name.into(),
            columns,
            primary_key,
            relationships: BTreeMap::new(),
            is_hidden: false,
            system: System::Client,
            compare_rows,
            sort,
        }
    }
}
