//! [`Constraint`]: a set of column-equality requirements used to drive
//! constrained fetches (the join-key lookup `(col_a, col_b, ...) = (...)`).
//!
//! Port of `zql/src/ivm/constraint.ts`. Note constraint matching uses
//! [`values_equal`] (join semantics: `null` never matches).

use crate::value::{values_equal, Row, Value};
use std::collections::BTreeMap;

/// A conjunction of column-equality requirements.
pub type Constraint = BTreeMap<String, Value>;

/// Does `row` satisfy every key in `constraint`?
pub fn constraint_matches_row(constraint: &Constraint, row: &Row) -> bool {
    for (key, val) in constraint {
        let row_val = row.get(key).unwrap_or(&Value::Null);
        if !values_equal(row_val, val) {
            return false;
        }
    }
    true
}

/// Two constraints are compatible if shared keys have equal values.
pub fn constraints_are_compatible(left: &Constraint, right: &Constraint) -> bool {
    for (key, lval) in left {
        if let Some(rval) = right.get(key) {
            if !values_equal(lval, rval) {
                return false;
            }
        }
    }
    true
}

/// Does the constraint's key set exactly equal the primary key?
pub fn constraint_matches_primary_key(constraint: &Constraint, primary: &[String]) -> bool {
    if constraint.len() != primary.len() {
        return false;
    }
    // BTreeMap iterates keys in sorted order; primary key is stored sorted.
    let mut sorted_primary = primary.to_vec();
    sorted_primary.sort();
    constraint.keys().zip(sorted_primary.iter()).all(|(a, b)| a == b)
}

/// Build a [`Constraint`] mapping each `to_keys[i]` to `from_row[from_keys[i]]`.
///
/// Mirrors `buildJoinConstraint` (join-utils.ts): used to look up matching rows
/// on the other side of a join. Returns `None` if any source value is null
/// (join semantics: null never matches).
pub fn build_join_constraint(
    from_row: &Row,
    from_keys: &[String],
    to_keys: &[String],
) -> Option<Constraint> {
    debug_assert_eq!(from_keys.len(), to_keys.len());
    let mut constraint = BTreeMap::new();
    for (fk, tk) in from_keys.iter().zip(to_keys.iter()) {
        let v = from_row.get(fk).unwrap_or(&Value::Null);
        if matches!(v, Value::Null) {
            // null never joins.
            return None;
        }
        constraint.insert(tk.clone(), v.clone());
    }
    Some(constraint)
}

/// Are two rows equal on the given compound key (join semantics)?
///
/// Mirrors `rowEqualsForCompoundKey` (join-utils.ts).
pub fn row_equals_for_compound_key(a: &Row, b: &Row, key: &[String]) -> bool {
    for k in key {
        let av = a.get(k).unwrap_or(&Value::Null);
        let bv = b.get(k).unwrap_or(&Value::Null);
        if !values_equal(av, bv) {
            return false;
        }
    }
    true
}

/// Does `parent_row[parent_key]` equal `child_row[child_key]` (join match)?
///
/// Mirrors `isJoinMatch` (join-utils.ts).
pub fn is_join_match(
    parent_row: &Row,
    parent_key: &[String],
    child_row: &Row,
    child_key: &[String],
) -> bool {
    for (pk, ck) in parent_key.iter().zip(child_key.iter()) {
        let pv = parent_row.get(pk).unwrap_or(&Value::Null);
        let cv = child_row.get(ck).unwrap_or(&Value::Null);
        if !values_equal(pv, cv) {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(pairs: &[(&str, Value)]) -> Row {
        pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
    }

    #[test]
    fn constraint_match_basic() {
        let mut c = Constraint::new();
        c.insert("id".to_string(), 1.into());
        assert!(constraint_matches_row(&c, &row(&[("id", 1.into())])));
        assert!(!constraint_matches_row(&c, &row(&[("id", 2.into())])));
    }

    #[test]
    fn null_never_matches_constraint() {
        let mut c = Constraint::new();
        c.insert("id".to_string(), Value::Null);
        // values_equal(null, null) == false
        assert!(!constraint_matches_row(&c, &row(&[("id", Value::Null)])));
    }

    #[test]
    fn join_constraint_null_returns_none() {
        let r = row(&[("a", Value::Null)]);
        assert!(build_join_constraint(&r, &["a".into()], &["b".into()]).is_none());
    }
}
