//! Verifies the Postgres connection over **TLS + password** — both the regular
//! SQL path ([`orbit_cache::pg::tls::connect`]) and Orbit's raw logical-replication
//! socket ([`ReplicationStream::start_with_tls`]).
//!
//! Gated on `ORBIT_TLS_PG_PORT` (CI's Postgres is trust/no-TLS, so this is skipped
//! there). Run locally against a ssl=on + scram-password Postgres:
//!
//!   # build + run the test database (self-signed cert, user/pass orbit/secret)
//!   docker build -t orbit-tls-pg - <<'DOCKER'
//!   FROM postgres:18
//!   RUN apt-get update && apt-get install -y openssl && mkdir -p /etc/pg-ssl \
//!    && openssl req -new -x509 -days 365 -nodes -subj /CN=localhost \
//!         -out /etc/pg-ssl/server.crt -keyout /etc/pg-ssl/server.key \
//!    && chmod 600 /etc/pg-ssl/server.key && chown postgres /etc/pg-ssl/server.*
//!   CMD ["postgres","-c","ssl=on","-c","ssl_cert_file=/etc/pg-ssl/server.crt",\
//!        "-c","ssl_key_file=/etc/pg-ssl/server.key","-c","wal_level=logical",\
//!        "-c","max_wal_senders=10","-c","max_replication_slots=10"]
//!   DOCKER
//!   docker run -d --name orbit-tls-pg -e POSTGRES_USER=orbit \
//!     -e POSTGRES_PASSWORD=secret -e POSTGRES_DB=orbit -p 5434:5432 orbit-tls-pg
//!   # (append `host replication all all scram-sha-256` to its pg_hba.conf)
//!   ORBIT_TLS_PG_PORT=5434 cargo test -p orbit-cache --test tls_pg -- --nocapture

use std::time::Duration;

use orbit_cache::pg::pgoutput::LogicalEvent;
use orbit_cache::pg::tls::{self, PgConnInfo};
use orbit_cache::pg::{create_publication, create_slot};
use orbit_cache::{PgTlsMode, ReplicationStream};

#[tokio::test]
async fn tls_password_sql_and_replication() {
    let Some(port) = std::env::var("ORBIT_TLS_PG_PORT").ok().and_then(|p| p.parse::<u16>().ok())
    else {
        eprintln!("skipping: set ORBIT_TLS_PG_PORT to a ssl=on + password Postgres (see this file's docs)");
        return;
    };
    let host = std::env::var("ORBIT_TLS_PG_HOST").unwrap_or_else(|_| "127.0.0.1".into());
    let password = std::env::var("ORBIT_TLS_PG_PASSWORD").unwrap_or_else(|_| "secret".into());
    let conn_str = format!("host={host} port={port} user=orbit dbname=orbit password={password}");

    // --- SQL path: connect over TLS with a password and prove it's encrypted. ---
    let (client, driver) = tls::connect(&conn_str, PgTlsMode::Require)
        .await
        .expect("TLS+password SQL connect");
    tokio::spawn(driver);

    let encrypted: bool = client
        .query_one("SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()", &[])
        .await
        .expect("query pg_stat_ssl")
        .get(0);
    assert!(encrypted, "the SQL connection should be TLS-encrypted");

    // --- DATABASE_URL path: parse a postgres:// URL, connect, prove encryption. ---
    let url = format!("postgres://orbit:{password}@{host}:{port}/orbit?sslmode=require");
    let info = PgConnInfo::parse_url(&url).expect("parse DATABASE_URL");
    assert_eq!(info.tls, PgTlsMode::Require);
    assert_eq!(info.password.as_deref(), Some(password.as_str()));
    let (url_client, url_driver) = tls::connect(&info.conn_str(), info.tls)
        .await
        .expect("connect via DATABASE_URL");
    tokio::spawn(url_driver);
    let url_encrypted: bool = url_client
        .query_one("SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()", &[])
        .await
        .expect("query pg_stat_ssl")
        .get(0);
    assert!(url_encrypted, "the DATABASE_URL connection should be TLS-encrypted");

    client
        .batch_execute(
            "DROP TABLE IF EXISTS tls_item;
             CREATE TABLE tls_item (id text PRIMARY KEY, name text);
             ALTER TABLE tls_item REPLICA IDENTITY FULL;",
        )
        .await
        .unwrap();
    create_publication(&client, "orbit_pub_tls", &["tls_item"]).await.unwrap();
    let start_lsn = create_slot(&client, "orbit_slot_tls").await.unwrap();

    // --- Replication path: stream over TLS with a password. ---
    let mut stream = ReplicationStream::start_with_tls(
        &host,
        port,
        "orbit",
        "orbit",
        "orbit_slot_tls",
        "orbit_pub_tls",
        start_lsn,
        Some(&password),
        PgTlsMode::Require,
    )
    .await
    .expect("start TLS replication");

    client.batch_execute("INSERT INTO tls_item VALUES ('a','Alice');").await.unwrap();

    let mut saw_insert = false;
    for _ in 0..20 {
        let (_lsn, ev) = tokio::time::timeout(Duration::from_secs(10), stream.next_event())
            .await
            .expect("timed out waiting for a replication event")
            .expect("replication error");
        if matches!(ev, LogicalEvent::Insert { .. }) {
            saw_insert = true;
            break;
        }
    }
    assert!(saw_insert, "the insert should arrive over the TLS replication stream");

    client
        .batch_execute(
            "SELECT pg_drop_replication_slot('orbit_slot_tls') \
             FROM pg_replication_slots WHERE slot_name = 'orbit_slot_tls'",
        )
        .await
        .ok();
}
