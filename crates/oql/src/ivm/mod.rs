//! The Orbit IVM (Incremental View Maintenance) engine.
//!
//! Port of `zql/src/ivm`. A pipeline is a graph of operators rooted at one or
//! more [`MemorySource`]s. Data is pulled downward via [`Input::fetch`] and
//! incremental changes are pushed upward via [`Operator::push`], propagated by
//! [`deliver`].

pub mod catch;
pub mod cond_filter;
pub mod constraint;
pub mod exists;
pub mod filter;
pub mod join;
pub mod node;
pub mod operator;
pub mod schema;
pub mod skip;
pub mod source;
pub mod take;

pub use catch::Catch;
pub use cond_filter::{CondFilter, NodePredicate};
pub use constraint::Constraint;
pub use exists::Exists;
pub use filter::{filter_push, Filter, Predicate};
pub use join::Join;
pub use node::{Change, ChangeType, Changes, Node, RowRef, SourceChange};
pub use operator::{deliver, Basis, FetchRequest, Input, InputRef, Link, Operator, Start};
pub use schema::{ColumnType, Schema};
pub use skip::skip;
pub use source::{connect, source_push, MemorySource, SourceConnection};
pub use take::Take;
