//! CVR persistence in **real Postgres**: the client view record (per-client
//! lastMutationID + desired queries) survives a "restart" (a fresh connection),
//! and already-processed mutations are deduplicated.

use orbit_cache::PgCvrStore;
use std::collections::HashMap;
use tokio_postgres::NoTls;

async fn connect() -> tokio_postgres::Client {
    let host = std::env::var("ORBIT_PG_HOST").unwrap_or_else(|_| "127.0.0.1".into());
    let port: u16 = std::env::var("ORBIT_PG_PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(5433);
    let (client, connection) =
        tokio_postgres::connect(&format!("host={host} port={port} user=orbit dbname=orbit"), NoTls)
            .await
            .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

#[tokio::test]
async fn cvr_persists_across_reconnect() {
    let group = format!("g_{}", std::process::id());

    // "Process 1": record mutations + a desired query.
    {
        let c = connect().await;
        PgCvrStore::ensure_schema(&c).await.unwrap();
        c.execute("DELETE FROM orbit_cvr_mutations WHERE client_group_id=$1", &[&group]).await.unwrap();
        c.execute("DELETE FROM orbit_cvr_queries WHERE client_group_id=$1", &[&group]).await.unwrap();

        assert!(PgCvrStore::record_mutation(&c, &group, "c1", 1).await.unwrap());
        assert!(PgCvrStore::record_mutation(&c, &group, "c1", 2).await.unwrap());
        PgCvrStore::add_query(&c, &group, "h1").await.unwrap();
    }

    // "Process 2": a fresh connection sees the persisted state.
    {
        let c = connect().await;
        assert_eq!(PgCvrStore::last_mutation_id(&c, &group, "c1").await.unwrap(), 2);
        // Re-delivered mutations are recognized as already processed.
        assert!(!PgCvrStore::record_mutation(&c, &group, "c1", 1).await.unwrap());
        assert!(!PgCvrStore::record_mutation(&c, &group, "c1", 2).await.unwrap());
        assert!(PgCvrStore::record_mutation(&c, &group, "c1", 3).await.unwrap());
        assert_eq!(PgCvrStore::desired_queries(&c, &group).await.unwrap(), vec!["h1".to_string()]);

        // cleanup
        c.execute("DELETE FROM orbit_cvr_mutations WHERE client_group_id=$1", &[&group]).await.ok();
        c.execute("DELETE FROM orbit_cvr_queries WHERE client_group_id=$1", &[&group]).await.ok();
    }
}

/// The per-client view (rows) and its version commit **atomically**: an upsert of
/// new/changed rows, deletes of dropped rows, and the version write all land as one
/// statement, so a reconnect that reports the version can never see a torn row set
/// (the server-side half of the "cookie must never be ahead of the rows" invariant).
#[tokio::test]
async fn commit_client_view_upsert_delete_and_version_are_atomic() {
    let cid = format!("cv_{}", std::process::id());
    let c = connect().await;
    PgCvrStore::ensure_schema(&c).await.unwrap();
    c.execute("DELETE FROM orbit_cvr_client_rows WHERE client_id=$1", &[&cid]).await.unwrap();
    c.execute("DELETE FROM orbit_cvr_clients WHERE client_id=$1", &[&cid]).await.unwrap();

    // First commit: two fresh rows at version 5, from an empty prior.
    let empty: HashMap<(String, String), String> = HashMap::new();
    let mut v1: HashMap<(String, String), String> = HashMap::new();
    v1.insert(("todo".into(), "a".into()), "{\"id\":\"a\"}".into());
    v1.insert(("todo".into(), "b".into()), "{\"id\":\"b\"}".into());
    PgCvrStore::commit_client_view(&c, &cid, &empty, &v1, 5, 100).await.unwrap();

    let (loaded, ver, _pos) = PgCvrStore::load_client_view(&c, &cid).await.unwrap();
    assert_eq!(ver, 5);
    assert_eq!(loaded, v1, "both rows persisted, carrying their version");

    // Second commit: update `a`, delete `b`, add `c`, bump to version 7 — atomically.
    let mut v2: HashMap<(String, String), String> = HashMap::new();
    v2.insert(("todo".into(), "a".into()), "{\"id\":\"a\",\"n\":1}".into());
    v2.insert(("todo".into(), "c".into()), "{\"id\":\"c\"}".into());
    PgCvrStore::commit_client_view(&c, &cid, &v1, &v2, 7, 200).await.unwrap();

    let (loaded, ver, _pos) = PgCvrStore::load_client_view(&c, &cid).await.unwrap();
    assert_eq!(ver, 7);
    assert_eq!(loaded, v2, "upsert + delete + version all applied together; no stale `b`");

    // A no-op commit (prior == current) still records the version and drops nothing.
    PgCvrStore::commit_client_view(&c, &cid, &v2, &v2, 9, 300).await.unwrap();
    let (loaded, ver, _pos) = PgCvrStore::load_client_view(&c, &cid).await.unwrap();
    assert_eq!(ver, 9);
    assert_eq!(loaded, v2);

    c.execute("DELETE FROM orbit_cvr_client_rows WHERE client_id=$1", &[&cid]).await.ok();
    c.execute("DELETE FROM orbit_cvr_clients WHERE client_id=$1", &[&cid]).await.ok();
}
