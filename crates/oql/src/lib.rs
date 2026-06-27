//! # OQL — Orbit Query Language
//!
//! Rust port of Zero's `zql`: the data model, the serializable AST, and the
//! Incremental View Maintenance (IVM) engine.
//!
//! The engine maintains the results of a query incrementally: a pipeline of
//! [`ivm`] operators is built over one or more [`ivm::MemorySource`]s; full
//! results are pulled via [`ivm::Input::fetch`], and incremental updates flow
//! as [`ivm::Change`]s pushed through the pipeline.

pub mod ast;
pub mod builder;
pub mod ivm;
pub mod query;
pub mod value;

pub use builder::{
    build_pipeline, create_predicate, eval_condition, resolve_cond_params_with_row,
    resolve_static_params, resolve_static_params_with_row, SourceProvider,
};
pub use query::{correlation, Query};
pub use value::{Direction, Row, Value};
