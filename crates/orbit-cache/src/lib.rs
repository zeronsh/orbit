//! # orbit-cache
//!
//! Orbit's server-side sync engine. Rust port of Zero's `zero-cache`.
//!
//! Currently implemented:
//! * [`pg`] — PostgreSQL logical replication client (wire protocol + pgoutput
//!   decoder) and setup helpers.
//! * [`replica`] — the local materialized replica that turns replication events
//!   into OQL [`oql::ivm::SourceChange`]s.
//!
//! Planned: SQLite-backed replica + change-log, the view-syncer + CVR, and the
//! WebSocket sync protocol server.

pub mod changelog;
pub mod changestream;
pub mod cvr;
pub mod forward;
pub mod handshake;
pub mod objectstore;
pub mod mutagen;
pub mod mutators;
pub mod permissions;
pub mod pg;
pub mod queries;
pub mod replica;
pub mod run;
pub mod server;
pub mod sharded;
pub mod sqlite_source;
pub mod view_sync;

pub use changestream::{ChangeMsg, ChangeStreamClient, ChangeStreamServer};
pub use objectstore::{LocalObjectStore, ObjectStore, ReplicaSnapshot};
#[cfg(feature = "s3")]
pub use objectstore::S3ObjectStore;
pub use forward::{AuthContext, ForwardConfig, Forwarder};
pub use mutagen::{apply_crud_op, apply_mutation};
pub use cvr::{Cvr, CvrStore, PgCvrStore};
pub use mutators::MutatorRegistry;
pub use permissions::{decode_auth, Permissions, WriteOp};
pub use queries::QueryRegistry;
pub use pg::pgoutput::LogicalEvent;
pub use pg::{initial_sync, ReplicationStream};
pub use replica::Replica;
pub use replica::ReplicaBackend;
pub use run::{
    run_replicator, run_server, run_server_full, run_server_sharded, run_server_sqlite,
    run_server_with, run_view_syncer, ServerConfig, TableConfig,
};
pub use sqlite_source::{SqliteProvider, SqliteReplica, SqliteSource};
pub use server::{serve_client, serve_connection, serve_connection_with_mutators};
pub use sharded::{ShardTable, ShardedServer};
pub use view_sync::{changes_to_patches, initial_patches};
