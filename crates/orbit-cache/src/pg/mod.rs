//! PostgreSQL integration: logical replication streaming + setup helpers.

pub mod pgoutput;
pub mod proto;

use anyhow::{bail, Result};
use pgoutput::{Decoder, LogicalEvent};
use proto::RawConn;
use tokio_postgres::Client;

/// A live logical-replication stream that yields decoded [`LogicalEvent`]s.
pub struct ReplicationStream {
    conn: RawConn,
    decoder: Decoder,
    /// Highest WAL position seen; reported back in Standby Status Updates.
    last_lsn: u64,
}

impl ReplicationStream {
    /// Open a replication connection and begin streaming `slot`/`publication`
    /// from `start_lsn`.
    pub async fn start(
        host: &str,
        port: u16,
        user: &str,
        database: &str,
        slot: &str,
        publication: &str,
        start_lsn: u64,
    ) -> Result<ReplicationStream> {
        let mut conn = RawConn::connect_replication(host, port, user, database).await?;
        conn.start_replication(slot, publication, start_lsn).await?;
        Ok(ReplicationStream {
            conn,
            decoder: Decoder::new(),
            last_lsn: start_lsn,
        })
    }

    /// Highest WAL position (LSN) seen so far. Monotonic across restarts, so it
    /// doubles as the change-stream resume watermark.
    pub fn last_lsn(&self) -> u64 {
        self.last_lsn
    }

    /// Read the next data event (Insert/Update/Delete/Begin/Commit), returning the
    /// event's own WAL position (`wal_start`) — distinct and monotonic per record,
    /// so it's a stable identity for the event across replicator restarts (used to
    /// dedup re-delivered changes and key the durable change-log). Transparently
    /// handles keepalives and skips ignored messages.
    pub async fn next_event(&mut self) -> Result<(u64, LogicalEvent)> {
        loop {
            let msg = self.conn.read_message().await?;
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
                                self.conn.send_standby_status(self.last_lsn, false).await?;
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

    /// Acknowledge all received WAL up to the latest position.
    pub async fn ack(&mut self) -> Result<()> {
        let lsn = self.last_lsn;
        self.conn.send_standby_status(lsn, false).await
    }
}

/// Create a publication for the given tables (idempotent).
pub async fn create_publication(client: &Client, name: &str, tables: &[&str]) -> Result<()> {
    client
        .batch_execute(&format!("DROP PUBLICATION IF EXISTS {name}"))
        .await?;
    let table_list = tables
        .iter()
        .map(|t| format!("\"{t}\""))
        .collect::<Vec<_>>()
        .join(", ");
    client
        .batch_execute(&format!("CREATE PUBLICATION {name} FOR TABLE {table_list}"))
        .await?;
    Ok(())
}

/// Seed a replica source with the current contents of its table (the initial
/// snapshot). All columns are selected as text and parsed by the column's
/// declared type, so this works for any column type without per-type binding.
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
        for (name, ty) in columns {
            let text: Option<String> = pg_row.try_get(name.as_str()).ok().flatten();
            let value = match text {
                None => Value::Null,
                Some(s) => parse_text_value(&s, *ty),
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
        for (name, ty) in columns {
            let text: Option<String> = pg_row.try_get(name.as_str()).ok().flatten();
            let value = match text {
                None => Value::Null,
                Some(s) => parse_text_value(&s, *ty),
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
    use oql::value::Value;
    let columns = backend.table_columns(table);
    let select_cols = columns
        .iter()
        .map(|(c, _)| format!("\"{0}\"::text AS \"{0}\"", c.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!("SELECT {} FROM \"{}\"", select_cols, table.replace('"', "\"\""));
    let pg_rows = client.query(&sql, &[]).await?;
    let mut count = 0;
    for pg_row in &pg_rows {
        let mut row = oql::value::Row::new();
        for (name, ty) in &columns {
            let text: Option<String> = pg_row.try_get(name.as_str()).ok().flatten();
            row.insert(name.as_str(), text.map(|s| parse_text_value(&s, *ty)).unwrap_or(Value::Null));
        }
        backend.seed(table, row);
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
