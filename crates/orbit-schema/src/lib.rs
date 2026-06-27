//! # orbit-schema
//!
//! Schema definition types and a builder. Rust port of Zero's `zero-schema`.
//!
//! The TypeScript client keeps its type-safe builder (`table().columns().
//! primaryKey()`, `relationships()`); this crate is the runtime/server-side
//! representation of the same schema, plus conversions the engine needs:
//! the OQL column-type map (to build sources) and the wire `ClientSchema`.

use oql::ivm::ColumnType;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// The value types a column can hold (Zero's `ValueType`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ValueType {
    String,
    Number,
    Boolean,
    Null,
    Json,
}

impl ValueType {
    pub fn to_column_type(self) -> ColumnType {
        match self {
            ValueType::String => ColumnType::String,
            ValueType::Number => ColumnType::Number,
            ValueType::Boolean => ColumnType::Boolean,
            ValueType::Null => ColumnType::Null,
            ValueType::Json => ColumnType::Json,
        }
    }
}

/// A column definition.
#[derive(Debug, Clone, PartialEq)]
pub struct SchemaValue {
    pub value_type: ValueType,
    pub optional: bool,
    /// Server (Postgres) column name, if it differs from the client name.
    pub server_name: Option<String>,
}

impl SchemaValue {
    fn new(value_type: ValueType) -> Self {
        SchemaValue {
            value_type,
            optional: false,
            server_name: None,
        }
    }
    /// Mark the column optional (`type | undefined`).
    pub fn optional(mut self) -> Self {
        self.optional = true;
        self
    }
    /// Map to a differently-named server column.
    pub fn from(mut self, server_name: impl Into<String>) -> Self {
        self.server_name = Some(server_name.into());
        self
    }
}

// Column constructors mirroring the TS API.
pub fn string() -> SchemaValue {
    SchemaValue::new(ValueType::String)
}
pub fn number() -> SchemaValue {
    SchemaValue::new(ValueType::Number)
}
pub fn boolean() -> SchemaValue {
    SchemaValue::new(ValueType::Boolean)
}
pub fn json() -> SchemaValue {
    SchemaValue::new(ValueType::Json)
}
/// `enumeration<T>()` in Zero is a string-typed column.
pub fn enumeration() -> SchemaValue {
    SchemaValue::new(ValueType::String)
}

/// A table definition.
#[derive(Debug, Clone, PartialEq)]
pub struct Table {
    pub name: String,
    pub columns: BTreeMap<String, SchemaValue>,
    pub primary_key: Vec<String>,
}

impl Table {
    pub fn new(name: impl Into<String>) -> Self {
        Table {
            name: name.into(),
            columns: BTreeMap::new(),
            primary_key: Vec::new(),
        }
    }
    pub fn column(mut self, name: impl Into<String>, value: SchemaValue) -> Self {
        self.columns.insert(name.into(), value);
        self
    }
    pub fn primary_key(mut self, cols: &[&str]) -> Self {
        self.primary_key = cols.iter().map(|s| s.to_string()).collect();
        self
    }

    /// The OQL column-type map used to build a source for this table.
    pub fn column_types(&self) -> BTreeMap<String, ColumnType> {
        self.columns
            .iter()
            .map(|(n, v)| (n.clone(), v.value_type.to_column_type()))
            .collect()
    }
}

/// Relationship cardinality.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Cardinality {
    One,
    Many,
}

/// A relationship from a source table to a destination table.
#[derive(Debug, Clone, PartialEq)]
pub struct Relationship {
    pub name: String,
    pub source_field: Vec<String>,
    pub dest_field: Vec<String>,
    pub dest_table: String,
    pub cardinality: Cardinality,
}

impl Relationship {
    pub fn one(
        name: impl Into<String>,
        source_field: &[&str],
        dest_field: &[&str],
        dest_table: impl Into<String>,
    ) -> Self {
        Relationship {
            name: name.into(),
            source_field: source_field.iter().map(|s| s.to_string()).collect(),
            dest_field: dest_field.iter().map(|s| s.to_string()).collect(),
            dest_table: dest_table.into(),
            cardinality: Cardinality::One,
        }
    }
    pub fn many(
        name: impl Into<String>,
        source_field: &[&str],
        dest_field: &[&str],
        dest_table: impl Into<String>,
    ) -> Self {
        Relationship {
            cardinality: Cardinality::Many,
            ..Relationship::one(name, source_field, dest_field, dest_table)
        }
    }
}

/// A complete schema: tables + relationships keyed by source table.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Schema {
    pub tables: BTreeMap<String, Table>,
    pub relationships: BTreeMap<String, Vec<Relationship>>,
}

impl Schema {
    pub fn builder() -> SchemaBuilder {
        SchemaBuilder::default()
    }

    /// The wire `ClientSchema` (sent in `initConnection`): tables → columns →
    /// `{type}` + `primaryKey`.
    pub fn client_schema(&self) -> serde_json::Value {
        let mut tables = serde_json::Map::new();
        for (name, table) in &self.tables {
            let mut columns = serde_json::Map::new();
            for (col, val) in &table.columns {
                columns.insert(
                    col.clone(),
                    serde_json::json!({ "type": val.value_type }),
                );
            }
            tables.insert(
                name.clone(),
                serde_json::json!({
                    "columns": columns,
                    "primaryKey": table.primary_key,
                }),
            );
        }
        serde_json::json!({ "tables": tables })
    }
}

/// Builder for [`Schema`].
#[derive(Default)]
pub struct SchemaBuilder {
    schema: Schema,
}

impl SchemaBuilder {
    pub fn table(mut self, table: Table) -> Self {
        self.schema.tables.insert(table.name.clone(), table);
        self
    }
    pub fn relationship(mut self, source_table: impl Into<String>, rel: Relationship) -> Self {
        self.schema
            .relationships
            .entry(source_table.into())
            .or_default()
            .push(rel);
        self
    }
    pub fn build(self) -> Schema {
        self.schema
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sample() -> Schema {
        Schema::builder()
            .table(
                Table::new("user")
                    .column("id", string())
                    .column("name", string().optional())
                    .primary_key(&["id"]),
            )
            .table(
                Table::new("post")
                    .column("id", string())
                    .column("userID", string())
                    .column("views", number())
                    .primary_key(&["id"]),
            )
            .relationship("user", Relationship::many("posts", &["id"], &["userID"], "post"))
            .build()
    }

    #[test]
    fn builds_tables_and_relationships() {
        let s = sample();
        assert_eq!(s.tables["user"].primary_key, vec!["id".to_string()]);
        assert!(s.tables["user"].columns["name"].optional);
        assert_eq!(s.relationships["user"][0].cardinality, Cardinality::Many);
        assert_eq!(s.relationships["user"][0].dest_table, "post");
    }

    #[test]
    fn column_types_for_source() {
        let s = sample();
        let types = s.tables["post"].column_types();
        assert_eq!(types["views"], ColumnType::Number);
        assert_eq!(types["id"], ColumnType::String);
    }

    #[test]
    fn client_schema_wire_shape() {
        let s = sample();
        let cs = s.client_schema();
        assert_eq!(cs["tables"]["user"]["primaryKey"], json!(["id"]));
        assert_eq!(cs["tables"]["user"]["columns"]["id"]["type"], "string");
        assert_eq!(cs["tables"]["post"]["columns"]["views"]["type"], "number");
    }
}
