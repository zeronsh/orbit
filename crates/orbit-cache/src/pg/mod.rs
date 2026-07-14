//! PostgreSQL integration: logical replication streaming + setup helpers.

pub mod pgoutput;
pub mod proto;
pub mod tls;

use anyhow::{bail, Result};
use pgoutput::{Decoder, LogicalEvent};
use proto::RawConn;
pub use tls::PgTlsMode;
use tokio_postgres::Client;

/// A live logical-replication stream that yields decoded [`LogicalEvent`]s.
pub struct ReplicationStream {
    conn: RawConn,
    decoder: Decoder,
    /// Highest WAL position seen; reported back in Standby Status Updates.
    last_lsn: u64,
    /// Highest WAL position the consumer has durably applied (see
    /// [`confirm`](Self::confirm)). This — not `last_lsn` — is what standby
    /// status updates acknowledge, so Postgres never prunes WAL the consumer
    /// hasn't safely committed: a crash between receipt and durable apply is
    /// recovered by slot re-delivery instead of becoming a permanent gap.
    confirmed_lsn: u64,
    /// Max time to wait for ANY inbound frame (data OR keepalive) before treating
    /// the stream as dead. A healthy Postgres sends keepalives at least every
    /// `wal_sender_timeout/2` (default 30s), so a stall well beyond that means a
    /// half-open connection — where our acks flow into the void and `read` would
    /// otherwise block forever. Erroring out lets the caller reconnect + resume
    /// from `last_lsn`. (Mirrors Zero's `lastReceivedTime` liveness check, #6047.)
    idle_timeout: std::time::Duration,
}

impl ReplicationStream {
    /// Open a replication connection and begin streaming `slot`/`publication`
    /// from `start_lsn` (plaintext; password from the environment). For TLS or an
    /// explicit password use [`start_with_tls`](Self::start_with_tls).
    pub async fn start(
        host: &str,
        port: u16,
        user: &str,
        database: &str,
        slot: &str,
        publication: &str,
        start_lsn: u64,
    ) -> Result<ReplicationStream> {
        Self::start_with_tls(host, port, user, database, slot, publication, start_lsn, None, PgTlsMode::Disable)
            .await
    }

    /// Like [`start`](Self::start) but with an explicit `password` and TLS `mode`
    /// (for managed Postgres that requires a password and/or `sslmode`).
    #[allow(clippy::too_many_arguments)]
    pub async fn start_with_tls(
        host: &str,
        port: u16,
        user: &str,
        database: &str,
        slot: &str,
        publication: &str,
        start_lsn: u64,
        password: Option<&str>,
        mode: PgTlsMode,
    ) -> Result<ReplicationStream> {
        let mut conn = RawConn::connect_replication(host, port, user, database, password, mode).await?;
        conn.start_replication(slot, publication, start_lsn).await?;
        Ok(ReplicationStream {
            conn,
            decoder: Decoder::new(),
            last_lsn: start_lsn,
            confirmed_lsn: start_lsn,
            idle_timeout: std::time::Duration::from_secs(180),
        })
    }

    /// Highest WAL position (LSN) seen so far. Monotonic across restarts, so it
    /// doubles as the change-stream resume watermark.
    pub fn last_lsn(&self) -> u64 {
        self.last_lsn
    }

    /// Mark WAL up to `lsn` as durably applied by the consumer. Acknowledgements
    /// (keepalive replies and [`ack`](Self::ack)) advance only to this point.
    /// Callers that don't need durable replay (in-memory replicas that re-sync
    /// on boot) confirm every received event; durable consumers confirm after
    /// each committed transaction.
    pub fn confirm(&mut self, lsn: u64) {
        self.confirmed_lsn = self.confirmed_lsn.max(lsn);
    }

    /// Read the next data event (Insert/Update/Delete/Begin/Commit), returning the
    /// event's own WAL position (`wal_start`) — distinct and monotonic per record,
    /// so it's a stable identity for the event across replicator restarts (used to
    /// dedup re-delivered changes and key the durable change-log). Transparently
    /// handles keepalives and skips ignored messages.
    pub async fn next_event(&mut self) -> Result<(u64, LogicalEvent)> {
        loop {
            // Bound the wait on inbound frames: a healthy stream sends data or a
            // keepalive well within `idle_timeout`, so exceeding it means a dead /
            // half-open connection. Error out so the caller reconnects from `last_lsn`
            // rather than blocking here forever.
            let msg = match tokio::time::timeout(self.idle_timeout, self.conn.read_message()).await {
                Ok(res) => res?,
                Err(_) => bail!(
                    "replication stream idle for {:?} (no data or keepalive); treating as dead",
                    self.idle_timeout
                ),
            };
            match msg.tag {
                b'd' => {
                    // CopyData. First byte selects the sub-message.
                    let body = &msg.body;
                    if body.is_empty() {
                        continue;
                    }
                    match body[0] {
                        b'w' => {
                            // XLogData: 'w' + i64 start + i64 end + i64 ts + payload.
                            if body.len() < 25 {
                                bail!("short XLogData frame");
                            }
                            let wal_start = u64::from_be_bytes(body[1..9].try_into().unwrap());
                            let wal_end = u64::from_be_bytes(body[9..17].try_into().unwrap());
                            self.last_lsn = self.last_lsn.max(wal_end);
                            let event = self.decoder.decode(&body[25..])?;
                            if matches!(event, LogicalEvent::Other) {
                                continue;
                            }
                            return Ok((wal_start, event));
                        }
                        b'k' => {
                            // Primary keepalive: 'k' + i64 wal_end + i64 ts + u8 replyRequested.
                            if body.len() < 18 {
                                bail!("short keepalive frame");
                            }
                            let wal_end = u64::from_be_bytes(body[1..9].try_into().unwrap());
                            self.last_lsn = self.last_lsn.max(wal_end);
                            let reply_requested = body[17] == 1;
                            if reply_requested {
                                // Ack only the durably-confirmed position (standby
                                // "flushed" semantics), not merely-received WAL.
                                self.conn.send_standby_status(self.confirmed_lsn, false).await?;
                            }
                            continue;
                        }
                        _ => continue,
                    }
                }
                b'E' => bail!("replication error: server sent ErrorResponse"),
                b'c' => bail!("replication stream ended (CopyDone)"),
                _ => continue,
            }
        }
    }

    /// Acknowledge WAL up to the durably-confirmed position.
    pub async fn ack(&mut self) -> Result<()> {
        let lsn = self.confirmed_lsn;
        self.conn.send_standby_status(lsn, false).await
    }
}

/// Create a publication for the given tables (idempotent).
/// The per-client `lastMutationID` table, written by the app's `PushProcessor`
/// (or the direct-write path) in the same transaction as a mutation's data. It's
/// replicated so the view-syncer can ack a mutation atomically with its rows.
pub const LMID_TABLE: &str = "orbit_client_mutations";

pub async fn create_publication(client: &Client, name: &str, tables: &[&str]) -> Result<()> {
    // Ensure the lastMutationID table exists (the app creates it lazily on first
    // push; it must exist to be added to the publication) and replicate it too.
    client
        .batch_execute(&format!(
            "CREATE TABLE IF NOT EXISTS {LMID_TABLE} \
             (client_id text PRIMARY KEY, last_mutation_id bigint NOT NULL)"
        ))
        .await?;
    client
        .batch_execute(&format!("DROP PUBLICATION IF EXISTS {name}"))
        .await?;
    let mut all: Vec<&str> = tables.to_vec();
    if !all.contains(&LMID_TABLE) {
        all.push(LMID_TABLE);
    }
    let table_list = all
        .iter()
        .map(|t| format!("\"{t}\""))
        .collect::<Vec<_>>()
        .join(", ");
    client
        .batch_execute(&format!("CREATE PUBLICATION {name} FOR TABLE {table_list}"))
        .await?;
    Ok(())
}

/// Resolve the real Postgres type OID of each column by preparing an uncasted
/// `SELECT` (no rows are fetched). The initial-sync SELECTs cast everything to
/// text, which erases type information — this recovers it so snapshot rows are
/// parsed with the SAME per-OID rules as streamed rows ([`pgoutput::parse_value`]).
async fn table_column_oids(
    client: &Client,
    table: &str,
    columns: &[(String, oql::ivm::ColumnType)],
) -> Result<Vec<u32>> {
    let select_cols = columns
        .iter()
        .map(|(c, _)| format!("\"{}\"", c.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!("SELECT {} FROM \"{}\"", select_cols, table.replace('"', "\"\""));
    let stmt = client.prepare(&sql).await?;
    Ok(stmt.columns().iter().map(|c| c.type_().oid()).collect())
}

/// Parse one snapshot text value: per-OID when the OID's natural type agrees
/// with the declared column type (the normal case — timestamps → epoch ms,
/// arrays → json, big int8 → exact), otherwise by the declared type (a user
/// override, e.g. a text column declared `json`).
fn parse_synced_value(s: &str, oid: u32, declared: oql::ivm::ColumnType) -> oql::value::Value {
    if pgoutput::column_type_for_oid(oid) == declared {
        pgoutput::parse_value(s, oid)
    } else {
        parse_text_value(s, declared)
    }
}

/// Seed a replica source with the current contents of its table (the initial
/// snapshot). All columns are selected as text and parsed by the column's
/// real type OID, so this works for any column type without per-type binding.
///
/// Combined with [`Replica::apply`](crate::replica::Replica::apply)'s
/// idempotency, this tolerates the small window between slot creation and the
/// snapshot SELECT (re-delivered changes are deduplicated by primary key).
pub async fn initial_sync(
    client: &Client,
    source: &std::rc::Rc<std::cell::RefCell<oql::ivm::MemorySource>>,
    columns: &[(String, oql::ivm::ColumnType)],
) -> Result<usize> {
    use oql::value::Value;

    let table = source.borrow().table_name().to_string();
    let oids = table_column_oids(client, &table, columns).await?;
    let select_cols = columns
        .iter()
        .map(|(c, _)| format!("\"{0}\"::text AS \"{0}\"", c.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!("SELECT {} FROM \"{}\"", select_cols, table.replace('"', "\"\""));

    // Run the query and collect owned data before touching the (!Send) source.
    let pg_rows = client.query(&sql, &[]).await?;

    let mut count = 0;
    for pg_row in &pg_rows {
        let mut row = oql::value::Row::new();
        for (i, (name, ty)) in columns.iter().enumerate() {
            let text: Option<String> = pg_row.try_get(name.as_str()).ok().flatten();
            let value = match text {
                None => Value::Null,
                Some(s) => parse_synced_value(&s, oids[i], *ty),
            };
            row.insert(name.as_str(), value);
        }
        source.borrow_mut().insert_initial(row);
        count += 1;
    }
    Ok(count)
}

/// Read all rows of `table` as typed [`Row`](oql::value::Row)s (the initial
/// snapshot, collected rather than seeded — used to seed every shard's replica).
pub async fn select_all_rows(
    client: &Client,
    table: &str,
    columns: &[(String, oql::ivm::ColumnType)],
) -> Result<Vec<oql::value::Row>> {
    use oql::value::Value;
    let oids = table_column_oids(client, table, columns).await?;
    let select_cols = columns
        .iter()
        .map(|(c, _)| format!("\"{0}\"::text AS \"{0}\"", c.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!("SELECT {} FROM \"{}\"", select_cols, table.replace('"', "\"\""));
    let pg_rows = client.query(&sql, &[]).await?;
    let mut out = Vec::with_capacity(pg_rows.len());
    for pg_row in &pg_rows {
        let mut row = oql::value::Row::new();
        for (i, (name, ty)) in columns.iter().enumerate() {
            let text: Option<String> = pg_row.try_get(name.as_str()).ok().flatten();
            let value = match text {
                None => Value::Null,
                Some(s) => parse_synced_value(&s, oids[i], *ty),
            };
            row.insert(name.as_str(), value);
        }
        out.push(row);
    }
    Ok(out)
}

/// Initial snapshot into any [`ReplicaBackend`](crate::replica::ReplicaBackend)
/// (in-memory or SQLite). Selects all columns as text and seeds typed rows.
pub async fn initial_sync_backend(
    client: &Client,
    backend: &dyn crate::replica::ReplicaBackend,
    table: &str,
) -> Result<usize> {
    use futures_util::TryStreamExt;
    use oql::value::Value;
    let columns = backend.table_columns(table);
    let oids = table_column_oids(client, table, &columns).await?;
    let select_cols = columns
        .iter()
        .map(|(c, _)| format!("\"{0}\"::text AS \"{0}\"", c.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!("SELECT {} FROM \"{}\"", select_cols, table.replace('"', "\"\""));
    // Stream instead of buffering the whole table (`query` would hold every
    // protocol row in RAM at once): each row is converted and seeded as it
    // arrives, so a durable (disk-backed) replica syncs any table size in O(1)
    // memory — the same reason Zero's initial sync streams COPY data.
    let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = Vec::new();
    let stream = client.query_raw(&sql, params).await?;
    futures_util::pin_mut!(stream);
    let mut count = 0;
    while let Some(pg_row) = stream.try_next().await? {
        let pg_row = &pg_row;
        let mut row = oql::value::Row::new();
        for (i, (name, ty)) in columns.iter().enumerate() {
            let text: Option<String> = pg_row.try_get(name.as_str()).ok().flatten();
            row.insert(
                name.as_str(),
                text.map(|s| parse_synced_value(&s, oids[i], *ty)).unwrap_or(Value::Null),
            );
        }
        backend.seed(table, row)?;
        count += 1;
    }
    Ok(count)
}

fn parse_text_value(s: &str, ty: oql::ivm::ColumnType) -> oql::value::Value {
    use oql::ivm::ColumnType;
    use oql::value::Value;
    match ty {
        ColumnType::String => Value::String(s.to_string()),
        ColumnType::Number => s
            .parse::<f64>()
            .map(Value::Number)
            .unwrap_or_else(|_| Value::String(s.to_string())),
        ColumnType::Boolean => Value::Bool(s == "t" || s == "true"),
        ColumnType::Json => serde_json::from_str(s)
            .map(Value::from_json)
            .unwrap_or_else(|_| Value::String(s.to_string())),
        ColumnType::Null => Value::Null,
    }
}

/// Ensure a logical replication slot (`pgoutput` plugin) exists, returning the LSN
/// at which streaming should begin.
///
/// A slot has a single consumer. We **reuse** an existing slot and never drop or
/// terminate it: during a redeploy two replicators briefly overlap, and dropping
/// or `pg_terminate_backend`-ing each other makes them mutually kill+restart in a
/// loop. Instead the loser simply retries `START_REPLICATION` (see run_replicator)
/// until the departing instance releases the slot.
pub async fn create_slot(client: &Client, slot: &str) -> Result<u64> {
    if let Some(row) = client
        .query_opt(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
            &[&slot],
        )
        .await?
    {
        let lsn: Option<String> = row.get(0);
        return proto::parse_lsn(lsn.as_deref().unwrap_or("0/0"));
    }
    let row = client
        .query_one(
            "SELECT lsn::text FROM pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&slot],
        )
        .await?;
    let lsn_text: String = row.get(0);
    proto::parse_lsn(&lsn_text)
}
