//! [`Schema`]: metadata describing the nodes an operator outputs.
//!
//! Port of `SourceSchema` in `zql/src/ivm/schema.ts`.

use crate::ast::Ordering;
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

/// Convert a stored [`Value`](crate::value::Value) to a column's (new) logical
/// type — the replica-side analog of the table rewrite Postgres performs on
/// `ALTER TABLE … ALTER COLUMN … TYPE` (which logical replication does NOT
/// re-send rows for). Zero does a tmp-column copy/convert
/// (`processUpdateColumn`); this is the same best-effort conversion applied to
/// the stored rows in place. Unconvertible values become `Null` (upstream PG
/// would have rejected the ALTER if its own cast failed, so a failure here is
/// a semantics mismatch, not data).
pub fn convert_column_value(v: &crate::value::Value, to: ColumnType) -> crate::value::Value {
    use crate::value::Value;
    if matches!(v, Value::Null) {
        return Value::Null;
    }
    match to {
        ColumnType::Null => Value::Null,
        ColumnType::String => match v {
            Value::String(_) => v.clone(),
            Value::Bool(b) => Value::String(if *b { "true" } else { "false" }.into()),
            Value::Int(i) => Value::String(i.to_string()),
            Value::Number(n) => {
                if n.fract() == 0.0 && n.is_finite() && *n >= i64::MIN as f64 && *n <= i64::MAX as f64
                {
                    Value::String((*n as i64).to_string())
                } else {
                    Value::String(n.to_string())
                }
            }
            Value::Json(j) => Value::String(j.to_string()),
            Value::Null => Value::Null,
        },
        ColumnType::Number => match v {
            Value::Number(_) | Value::Int(_) => v.clone(),
            Value::Bool(b) => Value::Number(if *b { 1.0 } else { 0.0 }),
            Value::String(s) => s
                .parse::<i64>()
                .map(Value::int)
                .or_else(|_| s.parse::<f64>().map(Value::Number))
                .unwrap_or(Value::Null),
            _ => Value::Null,
        },
        ColumnType::Boolean => match v {
            Value::Bool(_) => v.clone(),
            Value::Number(n) => Value::Bool(*n != 0.0),
            Value::Int(i) => Value::Bool(*i != 0),
            Value::String(s) => Value::Bool(matches!(
                s.as_str(),
                "t" | "true" | "TRUE" | "yes" | "on" | "1"
            )),
            _ => Value::Null,
        },
        ColumnType::Json => match v {
            // Scalars are already valid JSON values.
            Value::Json(_) | Value::Bool(_) | Value::Number(_) | Value::Int(_) => v.clone(),
            // Matches PG text→jsonb: parse when it IS json, else a JSON string.
            Value::String(s) => serde_json::from_str::<serde_json::Value>(s)
                .map(crate::value::Value::from_json)
                .unwrap_or_else(|_| Value::Json(serde_json::Value::String(s.clone()))),
            Value::Null => Value::Null,
        },
    }
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
            compare_rows,
            sort,
        }
    }
}
