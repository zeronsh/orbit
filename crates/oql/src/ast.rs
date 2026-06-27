//! The OQL AST: the serializable, wire-format representation of a query.
//!
//! Faithful port of `zero-protocol/src/ast.ts`. The serde representation is kept
//! byte-compatible with Zero's JSON so the existing TypeScript client and the
//! wire protocol continue to work unchanged.

use serde::{Deserialize, Serialize};

/// Prefix used for subquery aliases (`zsubq_` in Zero; kept identical for wire
/// compatibility).
pub const SUBQ_PREFIX: &str = "zsubq_";

/// Sort direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Direction {
    Asc,
    Desc,
}

/// A single ordering element: `[field, direction]`.
pub type OrderPart = (String, Direction);
/// Multi-column ordering.
pub type Ordering = Vec<OrderPart>;

/// Which "system" is responsible for a node being in the query. Data produced by
/// the `permissions` system is never synced to clients.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum System {
    Permissions,
    Client,
    Test,
}

/// The comparison operators usable in a [`SimpleCondition`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SimpleOperator {
    #[serde(rename = "=")]
    Eq,
    #[serde(rename = "!=")]
    Ne,
    #[serde(rename = "IS")]
    Is,
    #[serde(rename = "IS NOT")]
    IsNot,
    #[serde(rename = "<")]
    Lt,
    #[serde(rename = ">")]
    Gt,
    #[serde(rename = "<=")]
    Le,
    #[serde(rename = ">=")]
    Ge,
    #[serde(rename = "LIKE")]
    Like,
    #[serde(rename = "NOT LIKE")]
    NotLike,
    #[serde(rename = "ILIKE")]
    ILike,
    #[serde(rename = "NOT ILIKE")]
    NotILike,
    #[serde(rename = "IN")]
    In,
    #[serde(rename = "NOT IN")]
    NotIn,
}

/// A scalar literal value usable on the right side of a condition. Mirrors
/// `LiteralValue` (string | number | boolean | null | array of primitives).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum LiteralValue {
    Null,
    Bool(bool),
    Number(f64),
    String(String),
    /// Array of primitives, used by `IN` / `NOT IN`.
    Array(Vec<LiteralPrimitive>),
}

/// A primitive inside a literal array.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum LiteralPrimitive {
    Bool(bool),
    Number(f64),
    String(String),
}

/// A position in a condition: a literal, a column reference, or a runtime
/// parameter. Internally tagged by `type`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ValuePosition {
    #[serde(rename = "literal")]
    Literal { value: LiteralValue },
    #[serde(rename = "column")]
    Column { name: String },
    #[serde(rename = "static")]
    Static {
        anchor: ParameterAnchor,
        field: ParameterField,
    },
}

/// Source of an injected static parameter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ParameterAnchor {
    #[serde(rename = "authData")]
    AuthData,
    #[serde(rename = "preMutationRow")]
    PreMutationRow,
}

/// A parameter field path: a single field or a path of fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ParameterField {
    Single(String),
    Path(Vec<String>),
}

/// A compound key: at least one column. Modeled as a `Vec` with a runtime
/// non-empty invariant (Zero models it as `[string, ...string[]]`).
pub type CompoundKey = Vec<String>;

/// Correlation between a parent query and a correlated subquery (equality on
/// `parentField == childField`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Correlation {
    #[serde(rename = "parentField")]
    pub parent_field: CompoundKey,
    #[serde(rename = "childField")]
    pub child_field: CompoundKey,
}

/// A correlated subquery (a join / related hop).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CorrelatedSubquery {
    pub correlation: Correlation,
    pub subquery: Box<Ast>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub system: Option<System>,
    /// When true, this hop is not included in the output view but its children
    /// are (used to hide junction edges).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub hidden: Option<bool>,
}

/// `EXISTS` / `NOT EXISTS` operator for a correlated-subquery condition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExistsOp {
    #[serde(rename = "EXISTS")]
    Exists,
    #[serde(rename = "NOT EXISTS")]
    NotExists,
}

/// A `WHERE` condition. Internally tagged by `type`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Condition {
    #[serde(rename = "simple")]
    Simple {
        op: SimpleOperator,
        left: ValuePosition,
        /// `null` is excluded (Zero has no IS/IS NOT-with-null literal path).
        right: ValuePosition,
    },
    #[serde(rename = "and")]
    And { conditions: Vec<Condition> },
    #[serde(rename = "or")]
    Or { conditions: Vec<Condition> },
    #[serde(rename = "correlatedSubquery")]
    CorrelatedSubquery {
        related: CorrelatedSubquery,
        op: ExistsOp,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        flip: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        scalar: Option<bool>,
    },
}

/// A pagination bound (the `start` of a query).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Bound {
    pub row: crate::value::Row,
    pub exclusive: bool,
}

/// The top-level query AST.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Ast {
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub schema: Option<String>,
    pub table: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub alias: Option<String>,
    #[serde(rename = "where", skip_serializing_if = "Option::is_none", default)]
    pub where_: Option<Condition>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub related: Option<Vec<CorrelatedSubquery>>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub start: Option<Bound>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub limit: Option<u64>,
    #[serde(rename = "orderBy", skip_serializing_if = "Option::is_none", default)]
    pub order_by: Option<Ordering>,
}

// serde needs the `where` field to serialize as `"where"`, not `"where_"`.
// We can't name a Rust field `where` (reserved), so apply the rename via attr.
// (Applied here rather than inline to keep the struct readable.)
impl Ast {
    pub fn new(table: impl Into<String>) -> Self {
        Ast {
            schema: None,
            table: table.into(),
            alias: None,
            where_: None,
            related: None,
            start: None,
            limit: None,
            order_by: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn simple_condition_roundtrips_to_zero_wire_format() {
        let cond = Condition::Simple {
            op: SimpleOperator::Eq,
            left: ValuePosition::Column {
                name: "id".to_string(),
            },
            right: ValuePosition::Literal {
                value: LiteralValue::String("abc".to_string()),
            },
        };
        let v = serde_json::to_value(&cond).unwrap();
        assert_eq!(
            v,
            json!({
                "type": "simple",
                "op": "=",
                "left": {"type": "column", "name": "id"},
                "right": {"type": "literal", "value": "abc"}
            })
        );
        let back: Condition = serde_json::from_value(v).unwrap();
        assert_eq!(back, cond);
    }

    #[test]
    fn ast_with_orderby_and_limit() {
        let mut ast = Ast::new("issue");
        ast.order_by = Some(vec![("created".to_string(), Direction::Desc)]);
        ast.limit = Some(10);
        let v = serde_json::to_value(&ast).unwrap();
        assert_eq!(
            v,
            json!({
                "table": "issue",
                "orderBy": [["created", "desc"]],
                "limit": 10
            })
        );
    }

    #[test]
    fn in_operator_with_array_literal() {
        let cond = Condition::Simple {
            op: SimpleOperator::In,
            left: ValuePosition::Column {
                name: "status".to_string(),
            },
            right: ValuePosition::Literal {
                value: LiteralValue::Array(vec![
                    LiteralPrimitive::String("open".to_string()),
                    LiteralPrimitive::String("closed".to_string()),
                ]),
            },
        };
        let v = serde_json::to_value(&cond).unwrap();
        assert_eq!(v["op"], "IN");
        assert_eq!(v["right"]["value"], json!(["open", "closed"]));
    }
}
