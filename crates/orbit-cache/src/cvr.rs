//! Client View Records (CVR) persistence.
//!
//! A CVR is the server's memory of what a *client group* has: which queries it
//! desires and the last mutation id processed per client. Persisting it across
//! reconnects (port of Zero's `view-syncer/cvr` store) lets a reconnecting
//! client resume — the server already knows its desired queries and won't
//! reprocess mutations it has already applied.
//!
//! Here the store is in-process and shared across connections (`Rc<RefCell<_>>`);
//! a deployment can back it with Postgres without changing the interface.

use crate::view_sync::ClientView;
use std::collections::{HashMap, HashSet};

/// One client group's view record.
#[derive(Debug, Default, Clone)]
pub struct Cvr {
    /// Hashes of the queries this group currently desires.
    pub desired_queries: HashSet<String>,
    /// Highest mutation id processed, per client id in the group.
    pub last_mutation_ids: HashMap<String, u64>,
    /// Monotonic CVR version (the cookie base).
    pub version: u64,
}

/// A store of CVRs keyed by client-group id.
#[derive(Default)]
pub struct CvrStore {
    groups: HashMap<String, Cvr>,
}

impl CvrStore {
    pub fn new() -> Self {
        CvrStore::default()
    }

    fn entry(&mut self, group: &str) -> &mut Cvr {
        self.groups.entry(group.to_string()).or_default()
    }

    /// Record that `mutation_id` from `client_id` was processed. Returns false
    /// if it was already processed (out-of-order / duplicate), so the caller can
    /// skip re-applying it on reconnect.
    pub fn record_mutation(&mut self, group: &str, client_id: &str, mutation_id: u64) -> bool {
        let cvr = self.entry(group);
        let slot = cvr.last_mutation_ids.entry(client_id.to_string()).or_insert(0);
        if mutation_id <= *slot {
            return false;
        }
        *slot = mutation_id;
        true
    }

    pub fn last_mutation_id(&self, group: &str, client_id: &str) -> u64 {
        self.groups
            .get(group)
            .and_then(|c| c.last_mutation_ids.get(client_id).copied())
            .unwrap_or(0)
    }

    pub fn add_query(&mut self, group: &str, hash: &str) {
        self.entry(group).desired_queries.insert(hash.to_string());
    }

    pub fn remove_query(&mut self, group: &str, hash: &str) {
        if let Some(c) = self.groups.get_mut(group) {
            c.desired_queries.remove(hash);
        }
    }

    /// The queries a (re)connecting client group already desires.
    pub fn desired_queries(&self, group: &str) -> Vec<String> {
        self.groups
            .get(group)
            .map(|c| c.desired_queries.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Bump and return the group's CVR version (the next cookie base).
    pub fn bump_version(&mut self, group: &str) -> u64 {
        let cvr = self.entry(group);
        cvr.version += 1;
        cvr.version
    }

    pub fn known_group(&self, group: &str) -> bool {
        self.groups.contains_key(group)
    }
}

/// A Postgres-backed CVR store: the same client-group view records persisted to
/// Postgres so they survive process restarts (port of Zero's `cvr-store`).
pub struct PgCvrStore;

impl PgCvrStore {
    /// Create the CVR tables if absent.
    pub async fn ensure_schema(client: &tokio_postgres::Client) -> anyhow::Result<()> {
        client
            .batch_execute(
                "CREATE TABLE IF NOT EXISTS orbit_cvr_mutations (
                     client_group_id text NOT NULL,
                     client_id text NOT NULL,
                     last_mutation_id bigint NOT NULL,
                     PRIMARY KEY (client_group_id, client_id)
                 );
                 CREATE TABLE IF NOT EXISTS orbit_cvr_queries (
                     client_group_id text NOT NULL,
                     query_hash text NOT NULL,
                     PRIMARY KEY (client_group_id, query_hash)
                 );
                 CREATE TABLE IF NOT EXISTS orbit_cvr_client_rows (
                     client_id text NOT NULL,
                     table_name text NOT NULL,
                     row_id text NOT NULL,
                     row_val text NOT NULL,
                     PRIMARY KEY (client_id, table_name, row_id)
                 );
                 CREATE TABLE IF NOT EXISTS orbit_cvr_clients (
                     client_id text PRIMARY KEY,
                     version bigint NOT NULL
                 );",
            )
            .await?;
        Ok(())
    }

    /// The rows a client currently holds — its materialized view — plus the version
    /// (cookie) that view corresponds to, so a reconnect to *any* node can be served
    /// as a delta **only when the client proves it has this view** (its acked cookie
    /// equals this version). `(table, json(pk)) → json(row)`; version defaults to 0.
    pub async fn load_client_view(
        client: &tokio_postgres::Client,
        client_id: &str,
    ) -> anyhow::Result<(ClientView, u64)> {
        let rows = client
            .query(
                "SELECT table_name, row_id, row_val FROM orbit_cvr_client_rows WHERE client_id = $1",
                &[&client_id],
            )
            .await?;
        let view = rows
            .iter()
            .map(|r| ((r.get::<_, String>(0), r.get::<_, String>(1)), r.get::<_, String>(2)))
            .collect();
        let version = client
            .query_opt("SELECT version FROM orbit_cvr_clients WHERE client_id = $1", &[&client_id])
            .await?
            .map(|r| r.get::<_, i64>(0) as u64)
            .unwrap_or(0);
        Ok((view, version))
    }

    /// Persist a client's view as the delta from `prior` to `current` (upsert new/
    /// changed rows, delete dropped rows) and record `version` — the cookie a client
    /// will report if it holds exactly `current`. Called off the hot path (after a
    /// subscribe + a throttled checkpoint + on clean close), so steady-state mutation
    /// throughput pays no per-poke Postgres write.
    pub async fn commit_client_view(
        client: &tokio_postgres::Client,
        client_id: &str,
        prior: &ClientView,
        current: &ClientView,
        version: u64,
    ) -> anyhow::Result<()> {
        for ((table, id), val) in current {
            if prior.get(&(table.clone(), id.clone())) != Some(val) {
                client
                    .execute(
                        "INSERT INTO orbit_cvr_client_rows (client_id, table_name, row_id, row_val)
                         VALUES ($1, $2, $3, $4)
                         ON CONFLICT (client_id, table_name, row_id)
                         DO UPDATE SET row_val = EXCLUDED.row_val",
                        &[&client_id, table, id, val],
                    )
                    .await?;
            }
        }
        for (table, id) in prior.keys() {
            if !current.contains_key(&(table.clone(), id.clone())) {
                client
                    .execute(
                        "DELETE FROM orbit_cvr_client_rows
                         WHERE client_id = $1 AND table_name = $2 AND row_id = $3",
                        &[&client_id, table, id],
                    )
                    .await?;
            }
        }
        client
            .execute(
                "INSERT INTO orbit_cvr_clients (client_id, version) VALUES ($1, $2)
                 ON CONFLICT (client_id) DO UPDATE SET version = EXCLUDED.version",
                &[&client_id, &(version as i64)],
            )
            .await?;
        Ok(())
    }

    /// Record a processed mutation. Returns false (and writes nothing) if it was
    /// already processed.
    pub async fn record_mutation(
        client: &tokio_postgres::Client,
        group: &str,
        client_id: &str,
        mutation_id: u64,
    ) -> anyhow::Result<bool> {
        // Upsert only if strictly newer.
        let n = client
            .execute(
                "INSERT INTO orbit_cvr_mutations (client_group_id, client_id, last_mutation_id)
                 VALUES ($1, $2, $3)
                 ON CONFLICT (client_group_id, client_id)
                 DO UPDATE SET last_mutation_id = EXCLUDED.last_mutation_id
                 WHERE orbit_cvr_mutations.last_mutation_id < EXCLUDED.last_mutation_id",
                &[&group, &client_id, &(mutation_id as i64)],
            )
            .await?;
        Ok(n > 0)
    }

    pub async fn last_mutation_id(
        client: &tokio_postgres::Client,
        group: &str,
        client_id: &str,
    ) -> anyhow::Result<u64> {
        let row = client
            .query_opt(
                "SELECT last_mutation_id FROM orbit_cvr_mutations
                 WHERE client_group_id = $1 AND client_id = $2",
                &[&group, &client_id],
            )
            .await?;
        Ok(row.map(|r| r.get::<_, i64>(0) as u64).unwrap_or(0))
    }

    pub async fn add_query(
        client: &tokio_postgres::Client,
        group: &str,
        hash: &str,
    ) -> anyhow::Result<()> {
        client
            .execute(
                "INSERT INTO orbit_cvr_queries (client_group_id, query_hash) VALUES ($1, $2)
                 ON CONFLICT DO NOTHING",
                &[&group, &hash],
            )
            .await?;
        Ok(())
    }

    pub async fn desired_queries(
        client: &tokio_postgres::Client,
        group: &str,
    ) -> anyhow::Result<Vec<String>> {
        let rows = client
            .query("SELECT query_hash FROM orbit_cvr_queries WHERE client_group_id = $1", &[&group])
            .await?;
        Ok(rows.iter().map(|r| r.get(0)).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mutation_ids_persist_and_dedup_across_reconnects() {
        let mut store = CvrStore::new();

        // "Connection 1": process mutations 1 and 2 from client c1 in group g.
        assert!(store.record_mutation("g", "c1", 1));
        assert!(store.record_mutation("g", "c1", 2));

        // "Connection 1" drops; "Connection 2" reconnects (same store/group):
        // the last mutation id is remembered, and re-delivered mutations 1/2 are
        // recognized as already processed (not re-applied).
        assert_eq!(store.last_mutation_id("g", "c1"), 2);
        assert!(!store.record_mutation("g", "c1", 1), "duplicate is skipped");
        assert!(!store.record_mutation("g", "c1", 2), "duplicate is skipped");
        assert!(store.record_mutation("g", "c1", 3), "new mutation proceeds");
    }

    #[test]
    fn desired_queries_persist_across_reconnects() {
        let mut store = CvrStore::new();
        store.add_query("g", "h1");
        store.add_query("g", "h2");
        assert!(store.known_group("g"));

        // On reconnect the server can re-establish these without the client
        // re-sending them.
        let mut qs = store.desired_queries("g");
        qs.sort();
        assert_eq!(qs, vec!["h1".to_string(), "h2".to_string()]);

        store.remove_query("g", "h1");
        assert_eq!(store.desired_queries("g"), vec!["h2".to_string()]);
    }

    #[test]
    fn version_is_monotonic_per_group() {
        let mut store = CvrStore::new();
        assert_eq!(store.bump_version("g"), 1);
        assert_eq!(store.bump_version("g"), 2);
        assert_eq!(store.bump_version("other"), 1);
    }
}
