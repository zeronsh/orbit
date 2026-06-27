//! Operator traits, fetch requests, and the push-propagation driver.
//!
//! Port of `zql/src/ivm/operator.ts`, adapted to Rust:
//!
//! * [`Input`] (fetch side) and [`Operator`] (push side) are separate traits,
//!   both accessed through `Rc<RefCell<_>>`.
//! * `push` **returns** the changes an operator emits instead of calling its
//!   output inline. The [`deliver`] driver propagates them *after* the
//!   operator's borrow is released, faithfully reproducing Zero's behavior
//!   (operators may `fetch` from their inputs mid-push) without tripping
//!   Rust's borrow checker.

use super::node::Change;
use super::schema::Schema;
use crate::ivm::constraint::Constraint;
use crate::value::Row;
use std::cell::RefCell;
use std::rc::Rc;

/// Where a `start` cursor begins relative to the given row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Basis {
    /// Inclusive: begin at the first row `>=` `row`.
    At,
    /// Exclusive: begin at the first row `>` `row`.
    After,
}

/// A pagination cursor for [`fetch`](Input::fetch).
#[derive(Debug, Clone)]
pub struct Start {
    pub row: Row,
    pub basis: Basis,
}

/// Parameters for a [`fetch`](Input::fetch).
///
/// `multi_constraints` (the FlippedJoin batched-IN optimization) is omitted for
/// now; the in-memory path is correct without it.
#[derive(Debug, Clone, Default)]
pub struct FetchRequest {
    pub constraint: Option<Constraint>,
    pub start: Option<Start>,
    pub reverse: bool,
}

impl FetchRequest {
    pub fn constrained(constraint: Constraint) -> Self {
        FetchRequest {
            constraint: Some(constraint),
            start: None,
            reverse: false,
        }
    }
}

/// The fetch (pull) side of an operator.
pub trait Input {
    fn get_schema(&self) -> Rc<Schema>;
    /// Fetch nodes matching `req`, in `Schema::sort` order (reversed if
    /// `req.reverse`).
    fn fetch(&self, req: &FetchRequest) -> Vec<super::node::Node>;
}

/// The push side of an operator.
///
/// `push` processes a change arriving from this operator's input and returns
/// the changes to emit to its output. The [`deliver`] driver moves them to
/// [`output`](Operator::output).
pub trait Operator {
    fn push(&mut self, change: Change) -> super::node::Changes;
    /// This operator's downstream push target, if any. `None` for terminals.
    fn output(&self) -> Option<Link>;
    fn set_output(&mut self, out: Link);
}

/// A shared, push-target handle to an operator.
pub type Link = Rc<RefCell<dyn Operator>>;
/// A shared, fetch handle to an input.
pub type InputRef = Rc<RefCell<dyn Input>>;

/// A handle to an operator carrying both its [`Input`] (fetch) and [`Operator`]
/// (push/wire) trait-object views of the *same* underlying object.
///
/// A trait object has one vtable, so a single `Rc<RefCell<dyn Input>>` can't be
/// re-coerced to `dyn Operator`. We instead keep both views, each produced from
/// the concrete `Rc<RefCell<T>>` (cheap clones of the same allocation).
#[derive(Clone)]
pub struct OpHandle {
    pub input: InputRef,
    pub operator: Link,
}

impl OpHandle {
    pub fn new<T: Input + Operator + 'static>(op: Rc<RefCell<T>>) -> Self {
        OpHandle {
            input: op.clone(),
            operator: op,
        }
    }

    /// Wire this operator's downstream push target.
    pub fn set_output(&self, out: Link) {
        self.operator.borrow_mut().set_output(out);
    }
}

/// Propagate `change` into `dest` and recursively to its downstream.
///
/// Each operator's borrow is a short-lived temporary: we take the emitted
/// changes and the downstream link, drop the borrows, then recurse. This is
/// what makes mid-push `fetch` calls (which re-borrow upstream operators)
/// safe — no borrow is held across the recursion.
pub fn deliver(dest: &Link, change: Change) {
    let emitted = dest.borrow_mut().push(change);
    if emitted.is_empty() {
        return;
    }
    let next = dest.borrow().output();
    if let Some(next) = next {
        for c in emitted {
            deliver(&next, c);
        }
    }
    // If there's no downstream, a non-terminal operator emitted changes with
    // nowhere to go. Terminals (Catch) absorb changes in `push` and return
    // empty, so this is unreachable for well-formed pipelines.
}
