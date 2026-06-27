//! # orbit-protocol
//!
//! The Orbit client/server wire protocol. Rust port of Zero's `zero-protocol`,
//! with serde representations kept byte-compatible with Zero's JSON so the
//! existing TypeScript client can talk to an Orbit server unchanged.
//!
//! Messages are 2-element JSON arrays `["tag", body]`; see [`Downstream`] /
//! [`Upstream`].

pub mod messages;
pub mod mutation;
pub mod patches;

pub use messages::{
    ChangeDesiredQueriesBody, ConnectedBody, Downstream, ErrorBody, ErrorKind, InitConnectionBody,
    PokeEndBody, PokePartBody, PokeStartBody, SchemaVersions, Upstream, Version, PROTOCOL_VERSION,
};
pub use mutation::{CrudArg, CrudOp, Mutation, PushBody, CRUD_MUTATION_NAME};
pub use patches::{QueriesPatch, QueriesPatchOp, RowPatchOp, RowsPatch};
