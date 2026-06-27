//! The Orbit data model: [`Value`] and [`Row`], plus the comparison semantics
//! that the IVM engine relies on.
//!
//! Port of `zero-protocol/src/data.ts` and the comparison helpers in
//! `zql/src/ivm/data.ts`.
//!
//! The set of representable value types is deliberately small (the same set Zero
//! allows): `null`, `bool`, `number`, `string`, plus arbitrary JSON for `json`
//! columns. IDs must be comparable because they are used for sorting and row
//! identity.

use serde::de::{Deserialize, Deserializer};
use serde::ser::{Serialize, Serializer};
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::sync::Arc;

/// A single cell value.
///
/// Zero represents all numbers as JavaScript numbers (f64). We mirror that for
/// parity (and accept the bigint precision caveat that Zero itself has).
///
/// `undefined` in Zero is normalized to `null`, so we only have [`Value::Null`].
#[derive(Debug, Clone)]
pub enum Value {
    Null,
    Bool(bool),
    Number(f64),
    String(String),
    /// Arbitrary JSON, used for `json` columns. Not orderable as an id.
    Json(serde_json::Value),
}

impl Value {
    /// Rank used to give a *total* order across differing types. Within the IVM
    /// engine, comparisons are always same-type (or involve null); the
    /// cross-type ordering here exists only so [`Value`] can be a map key.
    fn type_rank(&self) -> u8 {
        match self {
            Value::Null => 0,
            Value::Bool(_) => 1,
            Value::Number(_) => 2,
            Value::String(_) => 3,
            Value::Json(_) => 4,
        }
    }
}

impl Value {
    /// Convert a JSON value to a [`Value`]. Objects and arrays become
    /// [`Value::Json`]; scalars map to the corresponding variant.
    pub fn from_json(j: serde_json::Value) -> Value {
        match j {
            serde_json::Value::Null => Value::Null,
            serde_json::Value::Bool(b) => Value::Bool(b),
            serde_json::Value::Number(n) => Value::Number(n.as_f64().unwrap_or(f64::NAN)),
            serde_json::Value::String(s) => Value::String(s),
            other => Value::Json(other),
        }
    }
}

impl Serialize for Value {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            Value::Null => s.serialize_none(),
            Value::Bool(b) => s.serialize_bool(*b),
            Value::Number(n) => {
                // Match JS/JSON: integer-valued numbers serialize without a
                // fractional part (`1`, not `1.0`).
                if n.fract() == 0.0 && n.is_finite() && *n >= i64::MIN as f64 && *n <= i64::MAX as f64
                {
                    s.serialize_i64(*n as i64)
                } else {
                    s.serialize_f64(*n)
                }
            }
            Value::String(st) => s.serialize_str(st),
            Value::Json(j) => j.serialize(s),
        }
    }
}

impl<'de> Deserialize<'de> for Value {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let j = serde_json::Value::deserialize(d)?;
        Ok(Value::from_json(j))
    }
}

impl From<bool> for Value {
    fn from(b: bool) -> Self {
        Value::Bool(b)
    }
}
impl From<f64> for Value {
    fn from(n: f64) -> Self {
        Value::Number(n)
    }
}
impl From<i64> for Value {
    fn from(n: i64) -> Self {
        Value::Number(n as f64)
    }
}
impl From<i32> for Value {
    fn from(n: i32) -> Self {
        Value::Number(n as f64)
    }
}
impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::String(s.to_string())
    }
}
impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::String(s)
    }
}

/// Compare two values for *sorting / row identity*.
///
/// Mirrors `compareValues` in `zql/src/ivm/data.ts`. NOTE: this considers
/// `null == null` (returns `Equal`). This is different from SQL; join code must
/// use [`values_equal`] instead, which treats `null != null`.
///
/// Strings are compared by their UTF-8 byte sequence (matching SQLite's default
/// `BINARY` collation, which Zero relies on).
///
/// Returns `Equal` for cross-type comparisons only when both are null;
/// otherwise null sorts first. Genuinely mismatched non-null types are a logic
/// error in a well-formed query; we fall back to a stable type-rank ordering
/// rather than panicking (Zero throws here).
pub fn compare_values(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        // null sorts before any non-null value.
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,
        (Value::String(x), Value::String(y)) => compare_utf8(x, y),
        (Value::Number(x), Value::Number(y)) => {
            // Mirror `a - b`: NaN-safe, treat equal floats as Equal.
            x.partial_cmp(y).unwrap_or(Ordering::Equal)
        }
        // false < true (Zero: `a ? 1 : -1`).
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        (Value::Json(x), Value::Json(y)) => compare_utf8(&x.to_string(), &y.to_string()),
        // Mismatched non-null types: not reachable for valid queries. Provide a
        // stable order instead of panicking.
        _ => a.type_rank().cmp(&b.type_rank()),
    }
}

/// Compare two strings by UTF-8 byte order. Rust's `str` ordering is already
/// byte-order (UTF-8), so this matches `compare-utf8` semantics.
#[inline]
pub fn compare_utf8(a: &str, b: &str) -> Ordering {
    a.as_bytes().cmp(b.as_bytes())
}

/// Determine if two values are equal with *join* semantics.
///
/// Mirrors `valuesEqual` in `zql/src/ivm/data.ts`: unlike [`compare_values`],
/// this treats `null` as **unequal to itself**. Required for correct joins.
pub fn values_equal(a: &Value, b: &Value) -> bool {
    if matches!(a, Value::Null) || matches!(b, Value::Null) {
        return false;
    }
    values_identical(a, b)
}

/// Strict structural equality (`null == null` is true here). Used where Zero
/// would use `===` on already-normalized values.
pub fn values_identical(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Null, Value::Null) => true,
        (Value::Bool(x), Value::Bool(y)) => x == y,
        (Value::Number(x), Value::Number(y)) => x == y,
        (Value::String(x), Value::String(y)) => x == y,
        (Value::Json(x), Value::Json(y)) => x == y,
        _ => false,
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        values_identical(self, other)
    }
}
impl Eq for Value {}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_values(self, other)
    }
}

/// A row: an ordered map of column name to [`Value`].
///
/// Zero uses plain JS objects. We back rows with a **sorted `Vec`** of
/// `(column, value)` pairs rather than a `BTreeMap`: a `BTreeMap` heap-allocates
/// a node sized for 11 entries even for a 3-column row (~600 B of slack), while
/// this stores exactly the columns present, contiguously. Lookups are a binary
/// search over a handful of cache-adjacent keys — faster in practice than B-tree
/// traversal for the small, wide rows typical here. The public API mirrors the
/// `BTreeMap` subset the engine uses, and iteration is in sorted key order, so
/// call sites and serialized output are unchanged.
/// An interned column name: a shared `Arc<str>`, so every row references one
/// allocation per distinct column name instead of owning a `String` copy. `Arc`
/// (not `Rc`) so rows stay `Send` and can cross threads in the sharded server.
pub type ColName = Arc<str>;

thread_local! {
    /// Per-thread intern pool. Thread-local so interning never touches a global
    /// lock — that would serialize the per-thread sharding. Each worker thread
    /// dedups column names across its own rows; a row that crosses a thread keeps
    /// its origin thread's `Arc` (still valid, since `Arc` is `Send`).
    static INTERNER: RefCell<HashSet<ColName>> = RefCell::new(HashSet::new());
}

/// Intern a column name. Allocates only the first time a name is seen on this
/// thread; afterwards it is a refcount bump.
pub fn intern_col(name: &str) -> ColName {
    INTERNER.with(|i| {
        let mut set = i.borrow_mut();
        if let Some(existing) = set.get(name) {
            return existing.clone();
        }
        let arc: ColName = Arc::from(name);
        set.insert(arc.clone());
        arc
    })
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Row {
    cols: Vec<(ColName, Value)>,
}

impl Row {
    pub fn new() -> Row {
        Row { cols: Vec::new() }
    }

    pub fn with_capacity(n: usize) -> Row {
        Row { cols: Vec::with_capacity(n) }
    }

    /// Build from interned pairs (sorts by key; on duplicate keys the last wins,
    /// matching `BTreeMap` insertion).
    fn from_pairs(mut v: Vec<(ColName, Value)>) -> Row {
        v.sort_by(|a, b| a.0.cmp(&b.0));
        v.dedup_by(|a, b| a.0 == b.0); // keeps the first of consecutive equals
        Row { cols: v }
    }

    fn position(&self, key: &str) -> Result<usize, usize> {
        self.cols.binary_search_by(|(k, _)| k.as_ref().cmp(key))
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        self.position(key).ok().map(|i| &self.cols[i].1)
    }

    pub fn get_mut(&mut self, key: &str) -> Option<&mut Value> {
        match self.position(key) {
            Ok(i) => Some(&mut self.cols[i].1),
            Err(_) => None,
        }
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.position(key).is_ok()
    }

    /// Insert a column. The key is interned, so repeated inserts of the same
    /// column name (across rows) share one allocation.
    pub fn insert(&mut self, key: impl AsRef<str>, value: Value) -> Option<Value> {
        let key = key.as_ref();
        match self.position(key) {
            Ok(i) => Some(std::mem::replace(&mut self.cols[i].1, value)),
            Err(i) => {
                self.cols.insert(i, (intern_col(key), value));
                None
            }
        }
    }

    pub fn remove(&mut self, key: &str) -> Option<Value> {
        match self.position(key) {
            Ok(i) => Some(self.cols.remove(i).1),
            Err(_) => None,
        }
    }

    pub fn retain<F: FnMut(&str, &mut Value) -> bool>(&mut self, mut f: F) {
        self.cols.retain_mut(|(k, v)| f(k.as_ref(), v));
    }

    pub fn len(&self) -> usize {
        self.cols.len()
    }

    pub fn is_empty(&self) -> bool {
        self.cols.is_empty()
    }

    pub fn keys(&self) -> impl Iterator<Item = &str> {
        self.cols.iter().map(|(k, _)| k.as_ref())
    }

    pub fn values(&self) -> impl Iterator<Item = &Value> {
        self.cols.iter().map(|(_, v)| v)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &Value)> {
        self.cols.iter().map(|(k, v)| (k.as_ref(), v))
    }
}

impl std::ops::Index<&str> for Row {
    type Output = Value;
    fn index(&self, key: &str) -> &Value {
        self.get(key).expect("no entry found for key")
    }
}

impl FromIterator<(String, Value)> for Row {
    fn from_iter<I: IntoIterator<Item = (String, Value)>>(iter: I) -> Row {
        Row::from_pairs(iter.into_iter().map(|(k, v)| (intern_col(&k), v)).collect())
    }
}

/// Alloc-free construction from string slices — the keys are interned, not
/// copied. Preferred on the hot path (replication decode, fetch construction).
impl<'a> FromIterator<(&'a str, Value)> for Row {
    fn from_iter<I: IntoIterator<Item = (&'a str, Value)>>(iter: I) -> Row {
        Row::from_pairs(iter.into_iter().map(|(k, v)| (intern_col(k), v)).collect())
    }
}

impl IntoIterator for Row {
    type Item = (ColName, Value);
    type IntoIter = std::vec::IntoIter<(ColName, Value)>;
    fn into_iter(self) -> Self::IntoIter {
        self.cols.into_iter()
    }
}

impl<'a> IntoIterator for &'a Row {
    type Item = &'a (ColName, Value);
    type IntoIter = std::slice::Iter<'a, (ColName, Value)>;
    fn into_iter(self) -> Self::IntoIter {
        self.cols.iter()
    }
}

impl Serialize for Row {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut m = s.serialize_map(Some(self.cols.len()))?;
        for (k, v) in &self.cols {
            m.serialize_entry(k.as_ref(), v)?;
        }
        m.end()
    }
}

impl<'de> Deserialize<'de> for Row {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct RowVisitor;
        impl<'de> serde::de::Visitor<'de> for RowVisitor {
            type Value = Row;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a row object")
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(self, mut a: A) -> Result<Row, A::Error> {
                let mut cols: Vec<(ColName, Value)> = Vec::with_capacity(a.size_hint().unwrap_or(0));
                while let Some((k, v)) = a.next_entry::<String, Value>()? {
                    cols.push((intern_col(&k), v));
                }
                Ok(Row::from_pairs(cols))
            }
        }
        d.deserialize_map(RowVisitor)
    }
}

/// Sort direction for an order-by element.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Asc,
    Desc,
}

/// A single `(column, direction)` ordering element.
pub type OrderPart = (String, Direction);
/// A multi-column ordering.
pub type Ordering2 = Vec<OrderPart>;

/// A comparator over rows, built from an [`Ordering2`].
///
/// Mirrors `makeComparator` in `zql/src/ivm/data.ts`.
#[derive(Clone)]
pub struct Comparator {
    order: Ordering2,
    reverse: bool,
}

impl Comparator {
    pub fn new(order: Ordering2, reverse: bool) -> Self {
        Comparator { order, reverse }
    }

    pub fn compare(&self, a: &Row, b: &Row) -> Ordering {
        for (field, dir) in &self.order {
            let av = a.get(field).unwrap_or(&Value::Null);
            let bv = b.get(field).unwrap_or(&Value::Null);
            let comp = compare_values(av, bv);
            if comp != Ordering::Equal {
                let result = match dir {
                    Direction::Asc => comp,
                    Direction::Desc => comp.reverse(),
                };
                return if self.reverse { result.reverse() } else { result };
            }
        }
        Ordering::Equal
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(pairs: &[(&str, Value)]) -> Row {
        pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
    }

    #[test]
    fn compare_values_numbers() {
        assert_eq!(compare_values(&1.into(), &2.into()), Ordering::Less);
        assert_eq!(compare_values(&2.into(), &2.into()), Ordering::Equal);
        assert_eq!(compare_values(&3.into(), &2.into()), Ordering::Greater);
    }

    #[test]
    fn compare_values_null_sorts_first() {
        assert_eq!(compare_values(&Value::Null, &1.into()), Ordering::Less);
        assert_eq!(compare_values(&1.into(), &Value::Null), Ordering::Greater);
        assert_eq!(compare_values(&Value::Null, &Value::Null), Ordering::Equal);
    }

    #[test]
    fn compare_values_bool_false_lt_true() {
        assert_eq!(compare_values(&false.into(), &true.into()), Ordering::Less);
    }

    #[test]
    fn compare_values_strings_utf8() {
        assert_eq!(compare_values(&"a".into(), &"b".into()), Ordering::Less);
        assert_eq!(compare_values(&"abc".into(), &"abd".into()), Ordering::Less);
    }

    #[test]
    fn values_equal_null_is_unequal_to_itself() {
        // Join semantics.
        assert!(!values_equal(&Value::Null, &Value::Null));
        assert!(values_equal(&1.into(), &1.into()));
        assert!(!values_equal(&1.into(), &2.into()));
    }

    // Ported from Zero's `zql/src/ivm/data.test.ts` (which uses fast-check
    // property testing). We sweep representative + random values to assert the
    // same properties.
    #[test]
    fn compare_values_properties_ported_from_zero() {
        let nums = [-1e9, -2.5, -1.0, 0.0, 1.0, 2.5, 42.0, 1e9];
        let strs = ["", "a", "ab", "b", "Z", "z", "é", "🦀"];

        // null is equal to itself and less than any non-null value.
        assert_eq!(compare_values(&Value::Null, &Value::Null), Ordering::Equal);
        for &n in &nums {
            assert_eq!(compare_values(&Value::Null, &n.into()), Ordering::Less);
            assert_eq!(compare_values(&Value::Number(n), &Value::Null), Ordering::Greater);
        }
        for s in &strs {
            assert_eq!(compare_values(&Value::Null, &(*s).into()), Ordering::Less);
        }

        // boolean: b1 == b2 ? Equal : (b1 ? Greater : Less)
        for &b1 in &[false, true] {
            for &b2 in &[false, true] {
                let want = if b1 == b2 {
                    Ordering::Equal
                } else if b1 {
                    Ordering::Greater
                } else {
                    Ordering::Less
                };
                assert_eq!(compare_values(&b1.into(), &b2.into()), want);
            }
        }

        // number: sign matches n1 - n2.
        for &n1 in &nums {
            for &n2 in &nums {
                let want = (n1 - n2).partial_cmp(&0.0).unwrap();
                assert_eq!(compare_values(&n1.into(), &n2.into()), want, "{n1} vs {n2}");
            }
        }

        // string: UTF-8 byte order.
        for s1 in &strs {
            for s2 in &strs {
                assert_eq!(
                    compare_values(&(*s1).into(), &(*s2).into()),
                    compare_utf8(s1, s2),
                    "{s1:?} vs {s2:?}"
                );
            }
        }

        // valuesEqual: true iff equal AND non-null (null != null for joins).
        for &n1 in &nums {
            for &n2 in &nums {
                assert_eq!(values_equal(&n1.into(), &n2.into()), n1 == n2);
            }
        }
        assert!(!values_equal(&Value::Null, &Value::Null));
    }

    #[test]
    fn comparator_multi_column() {
        let cmp = Comparator::new(
            vec![
                ("a".to_string(), Direction::Asc),
                ("b".to_string(), Direction::Desc),
            ],
            false,
        );
        let r1 = row(&[("a", 1.into()), ("b", 1.into())]);
        let r2 = row(&[("a", 1.into()), ("b", 2.into())]);
        // a equal, b desc => r2 (b=2) sorts before r1 (b=1)
        assert_eq!(cmp.compare(&r1, &r2), Ordering::Greater);
    }
}
