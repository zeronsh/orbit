//! CVR persistence in **real Postgres**: the client view record (per-client
//! lastMutationID + desired queries) survives a "restart" (a fresh connection),
//! and already-processed mutations are deduplicated.

use orbit_cache::PgCvrStore;
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
