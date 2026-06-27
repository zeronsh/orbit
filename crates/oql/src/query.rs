//! A small fluent query builder that produces an [`Ast`].
//!
//! This mirrors the shape of Zero's TypeScript query API
//! (`.where()`, `.related()`, `.order_by()`, `.limit()`, `.start()`,
//! `.one()`). On the client the type-safe TS API is retained unchanged; this
//! Rust builder exists for server-side query construction and tests. Multiple
//! `.where()` calls are ANDed together, matching the TS API.

use crate::ast::{
    Ast, Bound, Condition, CorrelatedSubquery, Correlation, Direction, ExistsOp, LiteralValue,
    SimpleOperator, ValuePosition,
};
use crate::value::Row;

/// Fluent builder over an [`Ast`].
#[derive(Debug, Clone)]
pub struct Query {
    ast: Ast,
}

impl Query {
    /// Start a query against `table`.
    pub fn table(table: impl Into<String>) -> Query {
        Query {
            ast: Ast::new(table),
        }
    }

    /// Add a simple `field op value` condition, ANDed with any existing WHERE.
    pub fn where_(mut self, field: impl Into<String>, op: SimpleOperator, value: impl Into<LiteralValue>) -> Query {
        let cond = Condition::Simple {
            op,
            left: ValuePosition::Column { name: field.into() },
            right: ValuePosition::Literal {
                value: value.into(),
            },
        };
        self.ast.where_ = Some(and_combine(self.ast.where_.take(), cond));
        self
    }

    /// Add an arbitrary condition (e.g. from an expression builder), ANDed in.
    pub fn where_cond(mut self, cond: Condition) -> Query {
        self.ast.where_ = Some(and_combine(self.ast.where_.take(), cond));
        self
    }

    /// Add an `EXISTS` correlated-subquery condition over a related table.
    pub fn where_exists(
        mut self,
        correlation: Correlation,
        subquery: Query,
        negated: bool,
    ) -> Query {
        let cond = Condition::CorrelatedSubquery {
            related: CorrelatedSubquery {
                correlation,
                subquery: Box::new(subquery.build()),
                system: None,
                hidden: None,
            },
            op: if negated {
                ExistsOp::NotExists
            } else {
                ExistsOp::Exists
            },
            flip: None,
            scalar: None,
        };
        self.ast.where_ = Some(and_combine(self.ast.where_.take(), cond));
        self
    }

    /// Add a related subquery (a join) named `name`.
    pub fn related(mut self, name: impl Into<String>, correlation: Correlation, mut subquery: Query) -> Query {
        let name = name.into();
        subquery.ast.alias = Some(name);
        let related = self.ast.related.get_or_insert_with(Vec::new);
        related.push(CorrelatedSubquery {
            correlation,
            subquery: Box::new(subquery.build()),
            system: None,
            hidden: None,
        });
        self
    }

    pub fn order_by(mut self, field: impl Into<String>, dir: Direction) -> Query {
        self.ast
            .order_by
            .get_or_insert_with(Vec::new)
            .push((field.into(), dir));
        self
    }

    pub fn limit(mut self, limit: u64) -> Query {
        self.ast.limit = Some(limit);
        self
    }

    /// `LIMIT 1` (the TS `.one()` modifier returns a single row / undefined).
    pub fn one(mut self) -> Query {
        self.ast.limit = Some(1);
        self
    }

    pub fn start(mut self, row: Row, exclusive: bool) -> Query {
        self.ast.start = Some(Bound { row, exclusive });
        self
    }

    /// Finish building and return the AST.
    pub fn build(self) -> Ast {
        self.ast
    }
}

/// Convenience: a `(parent_field, child_field)` correlation.
pub fn correlation(parent_field: &[&str], child_field: &[&str]) -> Correlation {
    Correlation {
        parent_field: parent_field.iter().map(|s| s.to_string()).collect(),
        child_field: child_field.iter().map(|s| s.to_string()).collect(),
    }
}

fn and_combine(existing: Option<Condition>, new: Condition) -> Condition {
    match existing {
        None => new,
        Some(Condition::And { mut conditions }) => {
            conditions.push(new);
            Condition::And { conditions }
        }
        Some(other) => Condition::And {
            conditions: vec![other, new],
        },
    }
}

// Ergonomic `Into<LiteralValue>` conversions for `.where_`.
impl From<&str> for LiteralValue {
    fn from(s: &str) -> Self {
        LiteralValue::String(s.to_string())
    }
}
impl From<String> for LiteralValue {
    fn from(s: String) -> Self {
        LiteralValue::String(s)
    }
}
impl From<f64> for LiteralValue {
    fn from(n: f64) -> Self {
        LiteralValue::Number(n)
    }
}
impl From<i64> for LiteralValue {
    fn from(n: i64) -> Self {
        LiteralValue::Number(n as f64)
    }
}
impl From<i32> for LiteralValue {
    fn from(n: i32) -> Self {
        LiteralValue::Number(n as f64)
    }
}
impl From<bool> for LiteralValue {
    fn from(b: bool) -> Self {
        LiteralValue::Bool(b)
    }
}
