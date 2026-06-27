//! Custom-mutator registry (the server-side analog of Zero's custom mutators).
//!
//! In Zero, custom mutators are user functions that run optimistically on the
//! client and authoritatively on the server. Orbit's server provides a registry
//! of named handlers: each receives the mutation args and read access to the
//! [`Replica`], and returns the [`CrudOp`]s to apply. (Client-side optimistic
//! application stays in the TypeScript client.)

use oql::SourceProvider;
use orbit_protocol::CrudOp;
use std::collections::HashMap;

/// A custom-mutator handler: `(provider, args) -> ops`. The provider gives read
/// access to the replica (any [`SourceProvider`] — in-memory or SQLite-backed).
pub type MutatorFn = Box<dyn Fn(&dyn SourceProvider, &[serde_json::Value]) -> Vec<CrudOp>>;

/// A registry of named custom mutators.
#[derive(Default)]
pub struct MutatorRegistry {
    handlers: HashMap<String, MutatorFn>,
}

impl MutatorRegistry {
    pub fn new() -> Self {
        MutatorRegistry::default()
    }

    /// Register a handler under `name`.
    pub fn register(
        &mut self,
        name: impl Into<String>,
        f: impl Fn(&dyn SourceProvider, &[serde_json::Value]) -> Vec<CrudOp> + 'static,
    ) {
        self.handlers.insert(name.into(), Box::new(f));
    }

    /// Run the handler for `name`, returning the resulting ops (or `None` if no
    /// such mutator is registered).
    pub fn run(
        &self,
        name: &str,
        provider: &dyn SourceProvider,
        args: &[serde_json::Value],
    ) -> Option<Vec<CrudOp>> {
        self.handlers.get(name).map(|f| f(provider, args))
    }

    pub fn contains(&self, name: &str) -> bool {
        self.handlers.contains_key(name)
    }
}
