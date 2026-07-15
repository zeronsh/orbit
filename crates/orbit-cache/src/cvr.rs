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
    ///
    /// `CREATE TABLE IF NOT EXISTS` is NOT safe under concurrency: two connections
    /// running it at once (e.g. the replicator and view-syncer booting together
    /// against a fresh database) race on `pg_type` and one fails with a duplicate-key
    /// error. Serialize the whole DDL block behind a database-wide advisory lock so
    /// only one connection creates the schema; the rest wait, then find it present.
    pub async fn ensure_schema(client: &tokio_postgres::Client) -> anyhow::Result<()> {
        // Fixed arbitrary key identifying the CVR-schema init critical section.
        const SCHEMA_LOCK: i64 = 7_412_030_907;
        client.execute("SELECT pg_advisory_lock($1)", &[&SCHEMA_LOCK]).await?;
        let res = Self::create_tables(client).await;
        // Release even if creation failed, so a retry (or another node) isn't blocked.
        let _ = client.execute("SELECT pg_advisory_unlock($1)", &[&SCHEMA_LOCK]).await;
        res
    }

    async fn create_tables(client: &tokio_postgres::Client) -> anyhow::Result<()> {
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
                     version bigint NOT NULL,
                     pos bigint NOT NULL DEFAULT 0,
                     last_seen timestamptz NOT NULL DEFAULT now()
                 );
                 ALTER TABLE orbit_cvr_clients ADD COLUMN IF NOT EXISTS pos bigint NOT NULL DEFAULT 0;
                 ALTER TABLE orbit_cvr_clients ADD COLUMN IF NOT EXISTS last_seen timestamptz NOT NULL DEFAULT now();

                 -- Older releases persisted every full row JSON once per client,
                 -- multiplying large message histories across CVRs. Compact those
                 -- values in-place to the SHA-256 fingerprints newer servers use.
                 UPDATE orbit_cvr_client_rows
                 SET row_val = encode(sha256(convert_to(row_val, 'UTF8')), 'hex')
                 WHERE length(row_val) <> 64 OR row_val !~ '^[0-9a-f]{64}$';",
            )
            .await?;
        Ok(())
    }

    /// The rows a client currently holds — its materialized view — plus the version
    /// (cookie) that view corresponds to, so a reconnect to *any* node can be served
    /// as a delta **only when the client proves it has this view** (its acked cookie
    /// equals this version). `(table, json(pk)) → sha256(row)`; version defaults to 0.
    pub async fn load_client_view(
        client: &tokio_postgres::Client,
        client_id: &str,
    ) -> anyhow::Result<(ClientView, u64, u64)> {
        // ONE statement (a single Postgres snapshot): reading rows and version
        // separately could pair one writer's rows with another's version under a
        // concurrent checkpoint (e.g. a zombie connection on another node) — the
        // fast delta path would then suppress rows the client doesn't hold.
        let rows = client
            .query(
                "SELECT c.version, c.pos, r.table_name, r.row_id, r.row_val
                 FROM orbit_cvr_clients c
                 LEFT JOIN orbit_cvr_client_rows r ON r.client_id = c.client_id
                 WHERE c.client_id = $1",
                &[&client_id],
            )
            .await?;
        let mut view = ClientView::new();
        let mut version = 0u64;
        let mut pos = 0u64;
        for r in &rows {
            version = r.get::<_, i64>(0) as u64;
            pos = r.get::<_, i64>(1) as u64;
            if let (Some(t), Some(id), Some(val)) = (
                r.get::<_, Option<String>>(2),
                r.get::<_, Option<String>>(3),
                r.get::<_, Option<String>>(4),
            ) {
                // Be tolerant of a legacy value written during a rolling upgrade.
                // `ensure_schema` normally compacts all of these before listening.
                let fingerprint = if crate::view_sync::is_fingerprint(&val) {
                    val
                } else {
                    crate::view_sync::fingerprint_json(&val)
                };
                view.insert((t, id), fingerprint);
            }
        }
        Ok((view, version, pos))
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
        pos: u64,
    ) -> anyhow::Result<()> {
        // The row delta (changed/new rows to upsert, dropped rows to delete) and the
        // fingerprint delta and version write must land atomically: a reconnect that
        // reports `version` as its cookie takes the fast delta path, which trusts the
        // stored fingerprints to match that version exactly. If the version could be
        // persisted without (or ahead of) its fingerprints, the delta could suppress
        // rows the client never received. So this is ONE all-or-nothing statement.
        //
        // An explicit BEGIN/COMMIT is deliberately avoided — every client connection
        // shares one pooled `tokio_postgres::Client`, so wrapping statements in a
        // transaction would capture other tasks' concurrently-pipelined queries. A CTE
        // keeps the atomicity within a single statement, immune to that interleaving.
        let mut up_tables: Vec<String> = Vec::new();
        let mut up_ids: Vec<String> = Vec::new();
        let mut up_vals: Vec<String> = Vec::new();
        for ((table, id), val) in current {
            if prior.get(&(table.clone(), id.clone())) != Some(val) {
                up_tables.push(table.clone());
                up_ids.push(id.clone());
                up_vals.push(val.clone());
            }
        }
        let mut del_tables: Vec<String> = Vec::new();
        let mut del_ids: Vec<String> = Vec::new();
        for (table, id) in prior.keys() {
            if !current.contains_key(&(table.clone(), id.clone())) {
                del_tables.push(table.clone());
                del_ids.push(id.clone());
            }
        }
        // Data-modifying CTEs run to completion exactly once regardless of whether the
        // primary query references them, and all share one snapshot — so the upserts,
        // deletes, and version write commit together or not at all.
        client
            .execute(
                "WITH ups AS (
                     INSERT INTO orbit_cvr_client_rows (client_id, table_name, row_id, row_val)
                     SELECT $1, t, i, v FROM UNNEST($2::text[], $3::text[], $4::text[]) AS u(t, i, v)
                     ON CONFLICT (client_id, table_name, row_id)
                     DO UPDATE SET row_val = EXCLUDED.row_val
                 ),
                 dels AS (
                     DELETE FROM orbit_cvr_client_rows r
                     USING UNNEST($5::text[], $6::text[]) AS d(t, i)
                     WHERE r.client_id = $1 AND r.table_name = d.t AND r.row_id = d.i
                 )
                 INSERT INTO orbit_cvr_clients (client_id, version, pos, last_seen)
                 VALUES ($1, $7, $8, now())
                 ON CONFLICT (client_id) DO UPDATE
                 SET version = EXCLUDED.version, pos = EXCLUDED.pos, last_seen = now()",
                &[
                    &client_id,
                    &up_tables,
                    &up_ids,
                    &up_vals,
                    &del_tables,
                    &del_ids,
                    &(version as i64),
                    &(pos as i64),
                ],
            )
            .await?;
        Ok(())
    }

    /// Drop CVRs of clients not seen for `max_age_days` (ephemeral tabs get a
    /// random clientID each session, so without GC their materialized views
    /// accumulate in Postgres forever). A swept client that does return simply
    /// full-resyncs. One statement (CTE) so rows and client entries go together.
    pub async fn gc_stale_clients(
        client: &tokio_postgres::Client,
        max_age_days: i32,
    ) -> anyhow::Result<u64> {
        let n = client
            .execute(
                "WITH stale AS (
                     DELETE FROM orbit_cvr_clients
                     WHERE last_seen < now() - make_interval(days => $1)
                     RETURNING client_id
                 ),
                 -- lastMutationID records of swept clients (previously never
                 -- GC'd — they accumulated forever; audit Tier 2)
                 dead_mutations AS (
                     DELETE FROM orbit_cvr_mutations m USING stale s
                     WHERE m.client_id = s.client_id
                 ),
                 -- desired-query records of groups with no surviving client
                 dead_queries AS (
                     DELETE FROM orbit_cvr_queries q
                     WHERE NOT EXISTS (
                         SELECT 1 FROM orbit_cvr_mutations m
                         WHERE m.client_group_id = q.client_group_id
                     )
                 )
                 DELETE FROM orbit_cvr_client_rows r USING stale s
                 WHERE r.client_id = s.client_id",
                &[&max_age_days],
            )
            .await?;
        Ok(n)
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
