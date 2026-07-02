//! Pipeline builder + predicate compiler: turn an [`Ast`] into a live IVM
//! pipeline of operators.
//!
//! Port of `zql/src/builder/builder.ts` (`buildPipeline`) and
//! `zql/src/builder/filter.ts` (`createPredicate`).

use crate::ast::{
    Ast, Condition, CorrelatedSubquery, Direction, ExistsOp, LiteralPrimitive, LiteralValue,
    SimpleOperator, ValuePosition,
};
use crate::ivm::filter::Predicate;
use crate::ivm::node::Node;
use crate::ivm::operator::OpHandle;
use crate::ivm::{connect, CondFilter, Filter, Join, MemorySource, NodePredicate, Take};
use crate::value::{compare_values, values_identical, Row, Value};
use std::cell::RefCell;
use std::cmp::Ordering as CmpOrdering;
use std::rc::Rc;

/// Supplies the source backing each table referenced by an AST.
///
/// The default implementations cover the common [`MemorySource`] case: implement
/// just [`get_source`](SourceProvider::get_source). A source backed by something
/// else (e.g. SQLite) instead overrides [`primary_key`](SourceProvider::primary_key)
/// and [`connect`](SourceProvider::connect) and leaves `get_source` returning
/// `None`.
pub trait SourceProvider {
    /// The [`MemorySource`] for `table`, if this provider is memory-backed.
    fn get_source(&self, _table: &str) -> Option<Rc<RefCell<MemorySource>>> {
        None
    }

    /// The primary key of `table`.
    fn primary_key(&self, table: &str) -> Option<Vec<String>> {
        self.get_source(table).map(|s| s.borrow().primary_key().to_vec())
    }

    /// Connect a fresh consumer to `table` ordered by `sort`, returning its
    /// operator handle.
    fn connect(&self, table: &str, sort: crate::ast::Ordering) -> Option<OpHandle> {
        self.get_source(table).map(|s| OpHandle::new(connect(&s, sort)))
    }
}

/// Build a live pipeline for `ast` and return its top operator handle.
///
/// The shape is: source → (start cursor) → (filter / exists for WHERE) →
/// (joins for each `related`) → (take for `limit`).
pub fn build_pipeline(ast: &Ast, provider: &dyn SourceProvider) -> OpHandle {
    build_pipeline_partitioned(ast, provider, None)
}

/// Like [`build_pipeline`], but a top-level `limit` is partitioned by
/// `partition_key` (used for related subqueries: a limit *per parent*).
fn build_pipeline_partitioned(
    ast: &Ast,
    provider: &dyn SourceProvider,
    partition_key: Option<Vec<String>>,
) -> OpHandle {
    let pk = provider
        .primary_key(&ast.table)
        .unwrap_or_else(|| panic!("no source registered for table {:?}", ast.table));
    let order = complete_ordering(ast.order_by.as_ref(), &pk);

    let mut current = provider
        .connect(&ast.table, order)
        .unwrap_or_else(|| panic!("no source registered for table {:?}", ast.table));

    // `start` cursor: keep only rows at/after the bound in the query's sort
    // order (the Skip operator).
    if let Some(bound) = &ast.start {
        current = OpHandle::new(crate::ivm::skip(current, bound.row.clone(), bound.exclusive));
    }

    if let Some(cond) = &ast.where_ {
        if condition_has_exists(cond) {
            // Materialize every EXISTS as a hidden relationship, then filter on
            // the whole boolean condition at the node level (handles AND/OR mixes
            // of predicates and EXISTS — no FanOut/FanIn needed).
            let mut joins = Vec::new();
            let mut counter = 0usize;
            let resolved = resolve_condition(cond, &mut joins, &mut counter);
            for (rel_name, related) in joins {
                let child = build_pipeline(&related.subquery, provider);
                let join = Join::new(
                    current,
                    child,
                    related.correlation.parent_field.clone(),
                    related.correlation.child_field.clone(),
                    rel_name,
                    true, // hidden
                );
                current = OpHandle::new(join);
            }
            current = OpHandle::new(CondFilter::new(current, node_predicate(resolved)));
        } else {
            current = OpHandle::new(Filter::new(current, create_predicate(cond)));
        }
    }

    if let Some(related) = &ast.related {
        for sub in related {
            // A `limit` inside the subquery is per-parent: partition by the
            // correlation's child field.
            let child = build_pipeline_partitioned(
                &sub.subquery,
                provider,
                Some(sub.correlation.child_field.clone()),
            );
            let rel_name = relationship_name(sub);
            // Shallow Child-change parents are sound only when nothing above this
            // join reads them: a `limit` at this level puts a Take above (it
            // stores Child parents for later window emission), so stay deep. The
            // hidden EXISTS joins (built in the `where` branch below a
            // CondFilter) are constructed with Join::new — always deep.
            let shallow = ast.limit.is_none();
            let join = Join::with_shallow_child_parents(
                current,
                child,
                sub.correlation.parent_field.clone(),
                sub.correlation.child_field.clone(),
                rel_name,
                sub.hidden.unwrap_or(false),
                shallow,
            );
            current = OpHandle::new(join);
        }
    }

    if let Some(limit) = ast.limit {
        // The `limit` fetch hint is sound only when nothing below the Take
        // filters rows: no `start` cursor (Skip), no `where` (Filter/CondFilter),
        // and no related joins in between (they're 1:1 on parents, but keep the
        // gate conservative). Then a bounded fetch returns exactly the head the
        // Take would have kept anyway.
        let exact_input =
            ast.start.is_none() && ast.where_.is_none() && ast.related.is_none();
        current = OpHandle::new(Take::partitioned(current, limit as usize, partition_key, exact_input));
    }

    current
}

/// The output relationship name for a related subquery (its alias, minus the
/// `zsubq_` prefix, falling back to the table name).
fn relationship_name(sub: &crate::ast::CorrelatedSubquery) -> String {
    match &sub.subquery.alias {
        Some(a) => a
            .strip_prefix(crate::ast::SUBQ_PREFIX)
            .unwrap_or(a)
            .to_string(),
        None => sub.subquery.table.clone(),
    }
}

/// Append any primary-key columns missing from `order_by` (ascending), so the
/// ordering is total. Mirrors `completeOrdering`.
pub fn complete_ordering(
    order_by: Option<&crate::ast::Ordering>,
    primary_key: &[String],
) -> crate::ast::Ordering {
    let mut order: crate::ast::Ordering = order_by.cloned().unwrap_or_default();
    for k in primary_key {
        if !order.iter().any(|(f, _)| f == k) {
            order.push((k.clone(), Direction::Asc));
        }
    }
    order
}

/// A WHERE condition with every `EXISTS` replaced by a reference to the hidden
/// relationship that materializes it.
enum ResolvedCond {
    Simple(Condition),
    And(Vec<ResolvedCond>),
    Or(Vec<ResolvedCond>),
    Exists { rel_name: String, negated: bool },
}

/// Walk a condition, emitting a hidden join (into `joins`) for every `EXISTS`
/// and returning a [`ResolvedCond`] that references those relationships by name.
fn resolve_condition(
    cond: &Condition,
    joins: &mut Vec<(String, CorrelatedSubquery)>,
    counter: &mut usize,
) -> ResolvedCond {
    match cond {
        Condition::Simple { .. } => ResolvedCond::Simple(cond.clone()),
        Condition::And { conditions } => ResolvedCond::And(
            conditions.iter().map(|c| resolve_condition(c, joins, counter)).collect(),
        ),
        Condition::Or { conditions } => ResolvedCond::Or(
            conditions.iter().map(|c| resolve_condition(c, joins, counter)).collect(),
        ),
        Condition::CorrelatedSubquery { related, op, .. } => {
            // Name the hidden join's relationship by the subquery alias (as Zero
            // does), falling back to a generated unique name.
            let rel_name = related
                .subquery
                .alias
                .clone()
                .unwrap_or_else(|| format!("{}exists_{}", crate::ast::SUBQ_PREFIX, *counter));
            *counter += 1;
            joins.push((rel_name.clone(), related.clone()));
            ResolvedCond::Exists {
                rel_name,
                negated: matches!(op, ExistsOp::NotExists),
            }
        }
    }
}

/// Build a node predicate from a resolved condition: rows are checked by the
/// usual predicate; `EXISTS` references check the presence of the materialized
/// hidden relationship on the node.
fn node_predicate(resolved: ResolvedCond) -> NodePredicate {
    std::rc::Rc::new(move |node: &Node| eval_resolved(&resolved, node))
}

fn eval_resolved(rc: &ResolvedCond, node: &Node) -> bool {
    match rc {
        ResolvedCond::Simple(c) => eval_condition(c, &node.row),
        ResolvedCond::And(v) => v.iter().all(|r| eval_resolved(r, node)),
        ResolvedCond::Or(v) => v.iter().any(|r| eval_resolved(r, node)),
        ResolvedCond::Exists { rel_name, negated } => {
            let present = node
                .relationships
                .get(rel_name)
                .map(|c| !c.is_empty())
                .unwrap_or(false);
            present != *negated
        }
    }
}

fn condition_has_exists(cond: &Condition) -> bool {
    match cond {
        Condition::CorrelatedSubquery { .. } => true,
        Condition::And { conditions } | Condition::Or { conditions } => {
            conditions.iter().any(condition_has_exists)
        }
        Condition::Simple { .. } => false,
    }
}

/// Compile a [`Condition`] into a row [`Predicate`].
pub fn create_predicate(cond: &Condition) -> Predicate {
    let cond = cond.clone();
    Rc::new(move |row: &Row| eval_condition(&cond, row))
}

/// Evaluate a condition against a row. Mirrors `createPredicate` semantics:
/// for all operators except `IS`/`IS NOT`, a null left-hand side yields false.
pub fn eval_condition(cond: &Condition, row: &Row) -> bool {
    match cond {
        Condition::And { conditions } => conditions.iter().all(|c| eval_condition(c, row)),
        Condition::Or { conditions } => conditions.iter().any(|c| eval_condition(c, row)),
        Condition::CorrelatedSubquery { .. } => {
            unreachable!("EXISTS conditions are handled by the Exists operator, not predicates")
        }
        Condition::Simple { op, left, right } => eval_simple(*op, left, right, row),
    }
}

fn eval_simple(op: SimpleOperator, left: &ValuePosition, right: &ValuePosition, row: &Row) -> bool {
    let lhs = value_at(left, row);

    // IS / IS NOT operate even on null and use strict identity.
    if matches!(op, SimpleOperator::Is | SimpleOperator::IsNot) {
        let rhs = scalar_literal(right);
        let identical = match (&lhs, &rhs) {
            (Some(l), Some(r)) => values_identical(l, r),
            _ => false,
        };
        return match op {
            SimpleOperator::Is => identical,
            SimpleOperator::IsNot => !identical,
            _ => unreachable!(),
        };
    }

    // Non-IS operators with a null right-hand literal are always false
    // (matches Zero's `createPredicate`: `=`/`!=`/`<`/… vs NULL never matches).
    if matches!(right, ValuePosition::Literal { value: LiteralValue::Null }) {
        return false;
    }

    // All other operators: null/undefined lhs => false.
    let lhs = match lhs {
        Some(Value::Null) | None => return false,
        Some(v) => v,
    };

    match op {
        SimpleOperator::In | SimpleOperator::NotIn => {
            let set = array_literal(right);
            let present = set.iter().any(|v| values_identical(v, &lhs));
            if matches!(op, SimpleOperator::In) {
                present
            } else {
                !present
            }
        }
        SimpleOperator::Like | SimpleOperator::NotLike | SimpleOperator::ILike
        | SimpleOperator::NotILike => {
            let pattern = match scalar_literal(right) {
                Some(Value::String(s)) => s,
                _ => return false,
            };
            let text = match &lhs {
                Value::String(s) => s.clone(),
                _ => return false,
            };
            let ci = matches!(op, SimpleOperator::ILike | SimpleOperator::NotILike);
            let m = like_match(&text, &pattern, ci);
            if matches!(op, SimpleOperator::Like | SimpleOperator::ILike) {
                m
            } else {
                !m
            }
        }
        _ => {
            let rhs = match scalar_literal(right) {
                Some(v) => v,
                None => return false,
            };
            match op {
                SimpleOperator::Eq => values_identical(&lhs, &rhs),
                SimpleOperator::Ne => !values_identical(&lhs, &rhs),
                SimpleOperator::Lt => compare_values(&lhs, &rhs) == CmpOrdering::Less,
                SimpleOperator::Le => compare_values(&lhs, &rhs) != CmpOrdering::Greater,
                SimpleOperator::Gt => compare_values(&lhs, &rhs) == CmpOrdering::Greater,
                SimpleOperator::Ge => compare_values(&lhs, &rhs) != CmpOrdering::Less,
                _ => unreachable!("handled above"),
            }
        }
    }
}

/// Resolve a value position to a concrete value for the given row. Returns
/// `None` when the column is absent.
fn value_at(pos: &ValuePosition, row: &Row) -> Option<Value> {
    match pos {
        ValuePosition::Column { name } => row.get(name).cloned(),
        ValuePosition::Literal { value } => Some(literal_scalar(value)),
    }
}

fn scalar_literal(pos: &ValuePosition) -> Option<Value> {
    match pos {
        ValuePosition::Literal { value } => Some(literal_scalar(value)),
        // The AST forbids a column on the right of a simple condition.
        ValuePosition::Column { .. } => None,
    }
}

fn array_literal(pos: &ValuePosition) -> Vec<Value> {
    match pos {
        ValuePosition::Literal {
            value: LiteralValue::Array(items),
        } => items
            .iter()
            .map(|p| match p {
                LiteralPrimitive::Bool(b) => Value::Bool(*b),
                LiteralPrimitive::Number(n) => Value::Number(*n),
                LiteralPrimitive::String(s) => Value::String(s.clone()),
            })
            .collect(),
        _ => Vec::new(),
    }
}

fn literal_scalar(lit: &LiteralValue) -> Value {
    match lit {
        LiteralValue::Null => Value::Null,
        LiteralValue::Bool(b) => Value::Bool(*b),
        LiteralValue::Number(n) => Value::Number(*n),
        LiteralValue::String(s) => Value::String(s.clone()),
        // An array used as a scalar isn't meaningful; represent as JSON.
        LiteralValue::Array(_) => Value::Null,
    }
}

/// SQL `LIKE` matcher supporting `%` (any run) and `_` (any single char).
/// `case_insensitive` implements `ILIKE`. Backslash escapes are not handled.
pub fn like_match(text: &str, pattern: &str, case_insensitive: bool) -> bool {
    let (text, pattern) = if case_insensitive {
        (text.to_lowercase(), pattern.to_lowercase())
    } else {
        (text.to_string(), pattern.to_string())
    };
    let t: Vec<char> = text.chars().collect();
    let p: Vec<char> = pattern.chars().collect();

    // Iterative wildcard match with backtracking on `%`.
    let (mut ti, mut pi) = (0usize, 0usize);
    let (mut star_pi, mut star_ti): (Option<usize>, usize) = (None, 0);
    while ti < t.len() {
        if pi < p.len() && (p[pi] == '_' || p[pi] == t[ti]) {
            ti += 1;
            pi += 1;
        } else if pi < p.len() && p[pi] == '%' {
            star_pi = Some(pi);
            star_ti = ti;
            pi += 1;
        } else if let Some(spi) = star_pi {
            pi = spi + 1;
            star_ti += 1;
            ti = star_ti;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '%' {
        pi += 1;
    }
    pi == p.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn like_basic() {
        assert!(like_match("hello world", "hello%", false));
        assert!(like_match("hello", "h_llo", false));
        assert!(!like_match("hello", "h_llo_", false));
        assert!(like_match("HELLO", "hello", true));
        assert!(like_match("abcXYZ", "%xyz", true));
        assert!(!like_match("abc", "%xyz", true));
    }
}
