//! The [`skip`] operator: emit only rows at/after a cursor bound in the source's
//! sort order (cursor-based `OFFSET`, the `start` of a query).
//!
//! Port of `zql/src/ivm/skip.ts`. Skip is a stateless bound predicate, so it is
//! implemented as a [`Filter`] whose predicate compares each row against the
//! bound using the input's row comparator.

use super::filter::{Filter, Predicate};
use super::operator::OpHandle;
use crate::value::Row;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::rc::Rc;

/// Build a Skip operator: keep rows `>= bound` (or `> bound` if `exclusive`) in
/// the input's sort order.
pub fn skip(input: OpHandle, bound: Row, exclusive: bool) -> Rc<RefCell<Filter>> {
    let cmp = input.input.borrow().get_schema().compare_rows.clone();
    let predicate: Predicate = Rc::new(move |row: &Row| {
        let o = cmp.compare(row, &bound);
        if exclusive {
            o == Ordering::Greater
        } else {
            o != Ordering::Less
        }
    });
    Filter::new(input, predicate)
}
