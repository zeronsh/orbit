//! Custom (named) query registry — the server-side analog of Zero's custom /
//! synced queries.
//!
//! A client may subscribe to a query by **name + args** instead of sending a
//! full AST (the `name`/`args` form of a `QueriesPatchOp::Put`). The server
//! holds the query definitions: each handler turns the args into an [`Ast`],
//! which is then built into a pipeline exactly like a client-supplied AST. This
//! lets the server own/authorize query shapes (mirrors how custom mutators let
//! it own writes).

use oql::ast::Ast;
use std::collections::HashMap;

/// A named-query handler: `args -> AST`.
pub type QueryFn = Box<dyn Fn(&[serde_json::Value]) -> Ast>;

/// A registry of named custom queries.
#[derive(Default)]
pub struct QueryRegistry {
    handlers: HashMap<String, QueryFn>,
}

impl QueryRegistry {
    pub fn new() -> Self {
        QueryRegistry::default()
    }

    /// Register a query under `name`.
    pub fn register(&mut self, name: impl Into<String>, f: impl Fn(&[serde_json::Value]) -> Ast + 'static) {
        self.handlers.insert(name.into(), Box::new(f));
    }

    /// Resolve a named query + args to an AST (or `None` if not registered).
    pub fn resolve(&self, name: &str, args: &[serde_json::Value]) -> Option<Ast> {
        self.handlers.get(name).map(|f| f(args))
    }

    pub fn contains(&self, name: &str) -> bool {
        self.handlers.contains_key(name)
    }
}
