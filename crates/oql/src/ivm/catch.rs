//! [`Catch`]: a terminal operator that collects pushed changes, for testing and
//! as the pipeline's output sink.
//!
//! Analogous to Zero's test `Catch`/`SnitchOutput`. Because [`Catch::push`]
//! returns no emitted changes, [`deliver`](super::operator::deliver) stops here.

use super::node::{Change, Node};
use super::operator::{FetchRequest, Input, Link, Operator};
use super::schema::Schema;
use std::cell::RefCell;
use std::rc::Rc;

/// Terminal change collector.
pub struct Catch {
    input: Rc<RefCell<dyn Input>>,
    changes: Vec<Change>,
}

impl Catch {
    pub fn new(input: Rc<RefCell<dyn Input>>) -> Rc<RefCell<Catch>> {
        Rc::new(RefCell::new(Catch {
            input,
            changes: Vec::new(),
        }))
    }

    /// Fetch the full current result set from upstream.
    pub fn fetch(&self) -> Vec<Node> {
        self.input.borrow().fetch(&FetchRequest::default())
    }

    pub fn get_schema(&self) -> Rc<Schema> {
        self.input.borrow().get_schema()
    }

    /// Drain and return the changes collected since the last call.
    pub fn take_changes(&mut self) -> Vec<Change> {
        std::mem::take(&mut self.changes)
    }

    pub fn changes(&self) -> &[Change] {
        &self.changes
    }
}

impl Operator for Catch {
    fn push(&mut self, change: Change) -> super::node::Changes {
        self.changes.push(change);
        super::node::Changes::new()
    }
    fn output(&self) -> Option<Link> {
        None
    }
    fn set_output(&mut self, _out: Link) {}
}
