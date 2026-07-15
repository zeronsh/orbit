//! Decoder for the `pgoutput` logical-decoding format (proto version 1).
//!
//! Parses the WAL payload carried inside `XLogData` frames into typed messages,
//! maintaining a relation cache so tuples can be turned into named [`Row`]s.
//!
//! Reference: PostgreSQL "Logical Replication Message Formats".

use anyhow::{bail, Result};
use oql::ivm::ColumnType;
use oql::value::{Row, Value};
use std::collections::HashMap;

/// A column in a relation, as described by a Relation message.
#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub type_oid: u32,
}

/// A cached relation definition.
#[derive(Debug, Clone)]
pub struct Relation {
    pub name: String,
    pub columns: Vec<Column>,
}

/// A decoded logical-replication event (the subset Orbit consumes).
///
/// Serializable so the replicator can stream events to view-syncer nodes (see
/// [`changestream`](crate::changestream)).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum LogicalEvent {
    Begin,
    Commit,
    Insert { table: String, row: Row },
    Update { table: String, row: Row, old_row: Option<Row> },
    Delete { table: String, old_row: Row },
    /// An upstream `TRUNCATE` of the listed tables — every replicated row of
    /// those tables must be removed. (Silently dropping this left stale rows in
    /// every replica forever.)
    Truncate { tables: Vec<String> },
    /// A table's column set (Relation message) — surfaces DDL schema changes.
    /// `renamed_from` is set when the same relation OID previously carried a
    /// different name (an upstream `ALTER TABLE … RENAME TO`): the replica
    /// aliases the new upstream name onto the existing source so replication
    /// keeps flowing to clients subscribed under the old name (previously the
    /// renamed table's events were silently dropped — data loss).
    Relation {
        table: String,
        columns: Vec<(String, ColumnType)>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        renamed_from: Option<String>,
    },
    /// Type / Origin / Message — ignored.
    Other,
}

impl LogicalEvent {
    /// Approximate in-memory footprint in bytes: the inline enum size plus the
    /// heap owned by table names and rows (see [`Row::estimated_bytes`]).
    /// Used to byte-bound the change ring, the durable-log queue, and write
    /// batches — an approximation, not exact accounting.
    pub fn estimated_bytes(&self) -> usize {
        let base = std::mem::size_of::<LogicalEvent>();
        match self {
            LogicalEvent::Begin | LogicalEvent::Commit | LogicalEvent::Other => base,
            LogicalEvent::Insert { table, row } => base + table.len() + row.estimated_bytes(),
            LogicalEvent::Update { table, row, old_row } => {
                base + table.len()
                    + row.estimated_bytes()
                    + old_row.as_ref().map_or(0, Row::estimated_bytes)
            }
            LogicalEvent::Delete { table, old_row } => {
                base + table.len() + old_row.estimated_bytes()
            }
            LogicalEvent::Truncate { tables } => {
                base + tables.iter().map(String::len).sum::<usize>()
            }
            LogicalEvent::Relation { table, columns, renamed_from } => {
                base + table.len()
                    + renamed_from.as_ref().map_or(0, String::len)
                    + columns
                        .iter()
                        .map(|(name, _)| name.len() + std::mem::size_of::<(String, ColumnType)>())
                        .sum::<usize>()
            }
        }
    }
}

// --- Postgres type OIDs (pg_type.dat) ---------------------------------------
const OID_BOOL: u32 = 16;
const OID_BYTEA: u32 = 17;
const OID_CHAR: u32 = 18;
const OID_NAME: u32 = 19;
const OID_INT8: u32 = 20;
const OID_INT2: u32 = 21;
const OID_INT4: u32 = 23;
const OID_TEXT: u32 = 25;
const OID_OID: u32 = 26;
const OID_JSON: u32 = 114;
const OID_FLOAT4: u32 = 700;
const OID_FLOAT8: u32 = 701;
const OID_BPCHAR: u32 = 1042;
const OID_VARCHAR: u32 = 1043;
const OID_DATE: u32 = 1082;
const OID_TIME: u32 = 1083;
const OID_TIMESTAMP: u32 = 1114;
const OID_TIMESTAMPTZ: u32 = 1184;
const OID_TIMETZ: u32 = 1266;
const OID_NUMERIC: u32 = 1700;
const OID_UUID: u32 = 2950;
const OID_JSONB: u32 = 3802;

/// Map a Postgres **array** type OID to its element OID (the common builtins).
/// Returns `None` for non-array (or unknown-array) OIDs.
fn array_elem_oid(oid: u32) -> Option<u32> {
    Some(match oid {
        1000 => OID_BOOL,
        1001 => OID_BYTEA,
        1002 => OID_CHAR,
        1003 => OID_NAME,
        1016 => OID_INT8,
        1005 => OID_INT2,
        1007 => OID_INT4,
        1009 => OID_TEXT,
        1028 => OID_OID,
        199 => OID_JSON,
        1021 => OID_FLOAT4,
        1022 => OID_FLOAT8,
        1014 => OID_BPCHAR,
        1015 => OID_VARCHAR,
        1182 => OID_DATE,
        1183 => OID_TIME,
        1115 => OID_TIMESTAMP,
        1185 => OID_TIMESTAMPTZ,
        1270 => OID_TIMETZ,
        1231 => OID_NUMERIC,
        2951 => OID_UUID,
        3807 => OID_JSONB,
        _ => return None,
    })
}

/// Map a Postgres type OID to an Orbit column type.
///
/// Mirrors the client schema-gen mapping (`packages/orbit/drizzle/type-map.ts`):
/// numbers/dates/times are `number`, json/jsonb/arrays are `json`, everything
/// else is `string`. Keeping the two in sync is what makes the declared client
/// type and the replicated value agree.
pub fn column_type_for_oid(oid: u32) -> ColumnType {
    match oid {
        OID_BOOL => ColumnType::Boolean,
        OID_INT8 | OID_INT2 | OID_INT4 | OID_OID | OID_FLOAT4 | OID_FLOAT8 | OID_NUMERIC
        | OID_DATE | OID_TIME | OID_TIMESTAMP | OID_TIMESTAMPTZ | OID_TIMETZ => ColumnType::Number,
        OID_JSON | OID_JSONB => ColumnType::Json,
        _ if array_elem_oid(oid).is_some() => ColumnType::Json,
        _ => ColumnType::String,
    }
}

/// Holds the relation cache across messages within a connection.
#[derive(Default)]
pub struct Decoder {
    relations: HashMap<u32, Relation>,
    /// The `final_lsn` (commit LSN) of the most recently decoded Begin
    /// message. Knowing a transaction's commit LSN AT ITS BEGIN lets the
    /// replication pumps decide apply/skip upfront and stream events through
    /// bounded memory instead of buffering the whole transaction (audit
    /// Tier 1.2).
    begin_final_lsn: Option<u64>,
}

impl Decoder {
    pub fn new() -> Self {
        Decoder::default()
    }

    /// The commit LSN carried by the most recent Begin message.
    pub fn begin_final_lsn(&self) -> Option<u64> {
        self.begin_final_lsn
    }

    /// Decode one pgoutput message (the body of an XLogData frame).
    pub fn decode(&mut self, data: &[u8]) -> Result<LogicalEvent> {
        if data.is_empty() {
            bail!("empty pgoutput message");
        }
        let mut c = Cursor::new(data);
        let tag = c.u8()?;
        match tag {
            b'B' => {
                // Begin: final_lsn (8) + commit timestamp (8) + xid (4).
                self.begin_final_lsn = c.u64().ok();
                Ok(LogicalEvent::Begin)
            }
            b'C' => Ok(LogicalEvent::Commit),
            b'R' => self.decode_relation(&mut c),
            b'I' => self.decode_insert(&mut c),
            b'U' => self.decode_update(&mut c),
            b'D' => self.decode_delete(&mut c),
            b'T' => self.decode_truncate(&mut c),
            // 'Y' type, 'O' origin, 'M' message.
            _ => Ok(LogicalEvent::Other),
        }
    }

    fn decode_relation(&mut self, c: &mut Cursor) -> Result<LogicalEvent> {
        let rel_id = c.u32()?;
        let _namespace = c.cstr()?;
        let name = c.cstr()?;
        let _replica_identity = c.u8()?;
        let num_columns = c.u16()? as usize;
        let mut columns = Vec::with_capacity(num_columns);
        for _ in 0..num_columns {
            let _flags = c.u8()?;
            let col_name = c.cstr()?;
            let type_oid = c.u32()?;
            let _type_modifier = c.u32()?;
            columns.push(Column {
                name: col_name,
                type_oid,
            });
        }
        let typed: Vec<(String, ColumnType)> = columns
            .iter()
            .map(|col| (col.name.clone(), column_type_for_oid(col.type_oid)))
            .collect();
        // The same relation OID under a NEW name = ALTER TABLE … RENAME TO.
        let renamed_from = self
            .relations
            .get(&rel_id)
            .filter(|prev| prev.name != name)
            .map(|prev| prev.name.clone());
        self.relations.insert(rel_id, Relation { name: name.clone(), columns });
        Ok(LogicalEvent::Relation { table: name, columns: typed, renamed_from })
    }

    fn relation(&self, rel_id: u32) -> Result<&Relation> {
        self.relations
            .get(&rel_id)
            .ok_or_else(|| anyhow::anyhow!("relation {rel_id} not seen before tuple"))
    }

    fn decode_insert(&mut self, c: &mut Cursor) -> Result<LogicalEvent> {
        let rel_id = c.u32()?;
        let kind = c.u8()?; // 'N'
        if kind != b'N' {
            bail!("unexpected insert tuple kind {kind}");
        }
        let rel = self.relation(rel_id)?.clone();
        let row = decode_tuple(c, &rel)?;
        Ok(LogicalEvent::Insert {
            table: rel.name,
            row,
        })
    }

    fn decode_update(&mut self, c: &mut Cursor) -> Result<LogicalEvent> {
        let rel_id = c.u32()?;
        let rel = self.relation(rel_id)?.clone();
        let mut old_row = None;
        let mut old_kind = 0u8;
        let mut kind = c.u8()?;
        if kind == b'K' || kind == b'O' {
            // Key or old-tuple (REPLICA IDENTITY). 'O' = full old row.
            old_kind = kind;
            old_row = Some(decode_tuple(c, &rel)?);
            kind = c.u8()?;
        }
        if kind != b'N' {
            bail!("unexpected update tuple kind {kind}");
        }
        let mut row = decode_tuple(c, &rel)?;
        // Unchanged-TOAST merge (decode side): columns shipped as 'u' are
        // absent from `row`. With REPLICA IDENTITY FULL the 'O' old tuple
        // carries their values — merge them in so the event is complete.
        // ('K' key tuples ship non-key columns as NULL, so merging from them
        // would corrupt; the apply-side merge over the *stored* row covers
        // that case — see `Replica::apply` / `SqliteReplica::apply`.)
        if old_kind == b'O' {
            if let Some(old) = &old_row {
                row.merge_missing_from(old);
            }
        }
        Ok(LogicalEvent::Update {
            table: rel.name,
            row,
            old_row,
        })
    }

    fn decode_delete(&mut self, c: &mut Cursor) -> Result<LogicalEvent> {
        let rel_id = c.u32()?;
        let rel = self.relation(rel_id)?.clone();
        let kind = c.u8()?;
        if kind != b'K' && kind != b'O' {
            bail!(
                "delete for table {} carries no key or old tuple (tuple kind {kind:?}) — \
                 is REPLICA IDENTITY set to NOTHING?",
                rel.name
            );
        }
        let old_row = decode_tuple(c, &rel)?;
        Ok(LogicalEvent::Delete {
            table: rel.name,
            old_row,
        })
    }

    /// Truncate message: u32 relation count, u8 option bits (CASCADE /
    /// RESTART IDENTITY — irrelevant to a replica that mirrors the outcome),
    /// then the relation OIDs.
    fn decode_truncate(&mut self, c: &mut Cursor) -> Result<LogicalEvent> {
        let n = c.u32()? as usize;
        let _options = c.u8()?;
        let mut tables = Vec::with_capacity(n);
        for _ in 0..n {
            let rel_id = c.u32()?;
            // PG sends Relation messages for truncated tables ahead of the
            // Truncate; an unknown id here means the table isn't replicated.
            if let Some(rel) = self.relations.get(&rel_id) {
                tables.push(rel.name.clone());
            }
        }
        Ok(LogicalEvent::Truncate { tables })
    }
}

/// Decode a TupleData into a [`Row`], mapping values by column type.
///
/// A column shipped as `'u'` (unchanged TOASTed value) is **absent** from the
/// returned row — deliberately distinct from an explicit NULL (`'n'`). Callers
/// reconstruct absent columns from the old tuple or the stored row.
fn decode_tuple(c: &mut Cursor, rel: &Relation) -> Result<Row> {
    let n = c.u16()? as usize;
    let mut row = Row::new();
    for i in 0..n {
        let col = rel.columns.get(i);
        let kind = c.u8()?;
        match kind {
            b'n' => {
                if let Some(col) = col {
                    row.insert(col.name.as_str(), Value::Null);
                }
            }
            b'u' => {
                // Unchanged TOASTed value; not present in the stream. Skip
                // (the column stays absent — NOT null — in this row).
            }
            b't' => {
                let len = c.u32()? as usize;
                let bytes = c.take(len)?;
                let text = std::str::from_utf8(bytes)?;
                if let Some(col) = col {
                    row.insert(col.name.as_str(), parse_value(text, col.type_oid));
                }
            }
            b'b' => {
                // Binary value (sent when the subscription requests binary
                // format). Decode per type instead of crashing the process.
                let len = c.u32()? as usize;
                let bytes = c.take(len)?;
                if let Some(col) = col {
                    let v = binary_value(bytes, col.type_oid).map_err(|e| {
                        anyhow::anyhow!("column {} (oid {}): {e}", col.name, col.type_oid)
                    })?;
                    row.insert(col.name.as_str(), v);
                }
            }
            other => bail!("unknown tuple column kind {other}"),
        }
    }
    Ok(row)
}

/// Map a pgoutput **text-format** value to a [`Value`] using the column's type
/// OID. Shared with the initial-sync path (`pg/mod.rs`), so a row seeded by
/// snapshot and the same row arriving via the stream decode identically.
pub fn parse_value(text: &str, type_oid: u32) -> Value {
    match type_oid {
        OID_BOOL => Value::Bool(text == "t" || text == "true"),
        OID_INT8 | OID_INT2 | OID_INT4 | OID_OID => text
            .parse::<i64>()
            .map(Value::int) // exact: big int8 ids keep all 64 bits
            .unwrap_or_else(|_| Value::String(text.to_string())),
        OID_FLOAT4 | OID_FLOAT8 => text
            .parse::<f64>()
            .map(Value::Number)
            .unwrap_or_else(|_| Value::String(text.to_string())),
        OID_NUMERIC => numeric_value(text),
        OID_JSON | OID_JSONB => serde_json::from_str::<serde_json::Value>(text)
            .map(Value::from_json)
            .unwrap_or_else(|_| Value::String(text.to_string())),
        OID_DATE => parse_pg_date(text)
            .map(|days| Value::Number(days as f64 * 86_400_000.0))
            .unwrap_or_else(|| Value::String(text.to_string())),
        OID_TIME | OID_TIMETZ => parse_pg_time(text)
            .map(|us| Value::Number(us as f64 / 1000.0))
            .unwrap_or_else(|| Value::String(text.to_string())),
        OID_TIMESTAMP | OID_TIMESTAMPTZ => parse_pg_timestamp(text)
            .map(|us| Value::Number(us as f64 / 1000.0))
            .unwrap_or_else(|| Value::String(text.to_string())),
        _ => match array_elem_oid(type_oid) {
            Some(elem) => parse_pg_array(text, elem)
                .map(Value::Json)
                .unwrap_or_else(|| Value::String(text.to_string())),
            // bytea keeps PG's canonical hex text (`\x…`) — stable and
            // round-trippable; uuid/enum/other → string.
            None => Value::String(text.to_string()),
        },
    }
}

/// Parse a `numeric` text value. Integer-valued numerics keep full 64-bit
/// precision ([`Value::int`]); everything else is an f64 (same ceiling as
/// Zero — the wire format is JSON either way). `NaN`/`Infinity` map to their
/// float forms.
fn numeric_value(text: &str) -> Value {
    if let Ok(i) = text.parse::<i64>() {
        return Value::int(i);
    }
    text.parse::<f64>()
        .map(Value::Number)
        .unwrap_or_else(|_| Value::String(text.to_string()))
}

// --- Binary-format decoding ---------------------------------------------------

/// Decode a **binary-format** tuple value (`'b'`). Postgres only sends these
/// when the subscriber asks for `binary 'true'`; handling them anyway turns a
/// process-killing `bail!` into defense in depth.
fn binary_value(bytes: &[u8], type_oid: u32) -> Result<Value> {
    let take = |n: usize| -> Result<&[u8]> {
        if bytes.len() != n {
            bail!("expected {n} bytes, got {}", bytes.len());
        }
        Ok(bytes)
    };
    Ok(match type_oid {
        OID_BOOL => Value::Bool(take(1)?[0] != 0),
        OID_INT2 => Value::Number(i16::from_be_bytes(take(2)?.try_into().unwrap()) as f64),
        OID_INT4 => Value::Number(i32::from_be_bytes(take(4)?.try_into().unwrap()) as f64),
        OID_OID => Value::Number(u32::from_be_bytes(take(4)?.try_into().unwrap()) as f64),
        OID_INT8 => Value::int(i64::from_be_bytes(take(8)?.try_into().unwrap())),
        OID_FLOAT4 => Value::Number(f32::from_be_bytes(take(4)?.try_into().unwrap()) as f64),
        OID_FLOAT8 => Value::Number(f64::from_be_bytes(take(8)?.try_into().unwrap())),
        OID_TEXT | OID_VARCHAR | OID_BPCHAR | OID_NAME | OID_CHAR => {
            Value::String(std::str::from_utf8(bytes)?.to_string())
        }
        OID_BYTEA => Value::String(bytea_hex(bytes)),
        OID_UUID => {
            let b = take(16)?;
            Value::String(format!(
                "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]
            ))
        }
        OID_JSON => serde_json::from_slice::<serde_json::Value>(bytes)
            .map(Value::from_json)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(bytes).into_owned())),
        OID_JSONB => {
            // jsonb binary = 1-byte version prefix, then json text.
            let body = bytes.get(1..).unwrap_or_default();
            serde_json::from_slice::<serde_json::Value>(body)
                .map(Value::from_json)
                .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(body).into_owned()))
        }
        OID_TIMESTAMP | OID_TIMESTAMPTZ => {
            // µs since 2000-01-01 00:00:00 UTC.
            let us = i64::from_be_bytes(take(8)?.try_into().unwrap());
            Value::Number((us as f64 + PG_EPOCH_US as f64) / 1000.0)
        }
        OID_DATE => {
            let days = i32::from_be_bytes(take(4)?.try_into().unwrap()) as i64;
            Value::Number((days + PG_EPOCH_DAYS) as f64 * 86_400_000.0)
        }
        OID_TIME => {
            let us = i64::from_be_bytes(take(8)?.try_into().unwrap());
            Value::Number(us as f64 / 1000.0)
        }
        OID_NUMERIC => numeric_value(&decode_binary_numeric(bytes)?),
        _ => match array_elem_oid(type_oid) {
            Some(elem) => decode_binary_array(bytes, elem)?,
            None => bail!("unsupported binary-format type"),
        },
    })
}

/// PG epoch (2000-01-01) offsets from the Unix epoch.
const PG_EPOCH_DAYS: i64 = 10_957;
const PG_EPOCH_US: i64 = PG_EPOCH_DAYS * 86_400_000_000;

/// Render bytes as PG's canonical bytea hex text (`\x…`), matching the text
/// format so both tuple formats produce identical values.
fn bytea_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(2 + bytes.len() * 2);
    s.push_str("\\x");
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// Decode PG's binary `numeric` into its text form (then parsed like text).
fn decode_binary_numeric(bytes: &[u8]) -> Result<String> {
    if bytes.len() < 8 {
        bail!("short numeric");
    }
    let ndigits = u16::from_be_bytes(bytes[0..2].try_into().unwrap()) as usize;
    let weight = i16::from_be_bytes(bytes[2..4].try_into().unwrap()) as i32;
    let sign = u16::from_be_bytes(bytes[4..6].try_into().unwrap());
    let dscale = u16::from_be_bytes(bytes[6..8].try_into().unwrap()) as usize;
    match sign {
        0xC000 => return Ok("NaN".to_string()),
        0xD000 => return Ok("Infinity".to_string()),
        0xF000 => return Ok("-Infinity".to_string()),
        _ => {}
    }
    if bytes.len() < 8 + ndigits * 2 {
        bail!("short numeric digits");
    }
    let digits: Vec<u16> = (0..ndigits)
        .map(|i| u16::from_be_bytes(bytes[8 + i * 2..10 + i * 2].try_into().unwrap()))
        .collect();
    // Integer part: digit groups with index <= weight (base 10000).
    let mut int_part = String::new();
    for gi in 0..=weight.max(-1) {
        let d = digits.get(gi as usize).copied().unwrap_or(0);
        if int_part.is_empty() {
            int_part.push_str(&d.to_string());
        } else {
            int_part.push_str(&format!("{d:04}"));
        }
    }
    if int_part.is_empty() {
        int_part.push('0');
    }
    // Fraction: groups after the weight, then trimmed/padded to dscale.
    let mut frac = String::new();
    let mut gi = weight + 1;
    while (gi as usize) < ndigits || frac.len() < dscale {
        if gi >= 0 {
            let d = digits.get(gi as usize).copied().unwrap_or(0);
            frac.push_str(&format!("{d:04}"));
        } else {
            frac.push_str("0000");
        }
        gi += 1;
        if frac.len() >= dscale && (gi as usize) >= ndigits {
            break;
        }
    }
    frac.truncate(dscale);
    let sign_str = if sign == 0x4000 { "-" } else { "" };
    Ok(if dscale > 0 {
        format!("{sign_str}{int_part}.{frac:0<dscale$}")
    } else {
        format!("{sign_str}{int_part}")
    })
}

/// Decode a binary array into a JSON array value.
fn decode_binary_array(bytes: &[u8], elem_oid: u32) -> Result<Value> {
    let mut c = Cursor::new(bytes);
    let ndim = c.i32()?;
    let _has_nulls = c.i32()?;
    let _elem_type = c.u32()?;
    if ndim < 0 || ndim > 6 {
        bail!("bad array ndim {ndim}");
    }
    let mut count = if ndim == 0 { 0usize } else { 1usize };
    for _ in 0..ndim {
        let dim = c.i32()?;
        let _lbound = c.i32()?;
        count = count.saturating_mul(dim.max(0) as usize);
    }
    let mut out = Vec::with_capacity(count.min(1024));
    for _ in 0..count {
        let len = c.i32()?;
        if len < 0 {
            out.push(serde_json::Value::Null);
        } else {
            let raw = c.take(len as usize)?;
            out.push(value_to_json(binary_value(raw, elem_oid)?));
        }
    }
    // Multi-dimensional arrays flatten (row-major) — same shape either way for
    // the JSON column type.
    Ok(Value::Json(serde_json::Value::Array(out)))
}

fn value_to_json(v: Value) -> serde_json::Value {
    serde_json::to_value(&v).unwrap_or(serde_json::Value::Null)
}

// --- Text-format array parsing -------------------------------------------------

/// Parse a PG array literal (`{1,2,NULL}`, `{"a","b"}`, `{{1,2},{3,4}}`) into a
/// JSON array, parsing elements as `elem_oid`. Returns `None` on malformed
/// input (caller falls back to the raw string).
fn parse_pg_array(text: &str, elem_oid: u32) -> Option<serde_json::Value> {
    let mut chars = text.trim().chars().peekable();
    // Optional bounds decoration `[1:2]={…}` — skip to the first `{`.
    if chars.peek() == Some(&'[') {
        for ch in chars.by_ref() {
            if ch == '=' {
                break;
            }
        }
    }
    if chars.next()? != '{' {
        return None;
    }
    let v = parse_pg_array_body(&mut chars, elem_oid)?;
    Some(v)
}

/// Parse the body of an array after the opening `{` up to and including the
/// matching `}`.
fn parse_pg_array_body(
    chars: &mut std::iter::Peekable<std::str::Chars>,
    elem_oid: u32,
) -> Option<serde_json::Value> {
    let mut out: Vec<serde_json::Value> = Vec::new();
    loop {
        match chars.peek()? {
            '}' => {
                chars.next();
                return Some(serde_json::Value::Array(out));
            }
            ',' => {
                chars.next();
            }
            '{' => {
                chars.next();
                out.push(parse_pg_array_body(chars, elem_oid)?);
            }
            '"' => {
                chars.next();
                let mut s = String::new();
                loop {
                    match chars.next()? {
                        '\\' => s.push(chars.next()?),
                        '"' => break,
                        ch => s.push(ch),
                    }
                }
                out.push(value_to_json(parse_value(&s, elem_oid)));
            }
            _ => {
                let mut s = String::new();
                while let Some(&ch) = chars.peek() {
                    if ch == ',' || ch == '}' {
                        break;
                    }
                    s.push(ch);
                    chars.next();
                }
                if s == "NULL" {
                    out.push(serde_json::Value::Null);
                } else {
                    out.push(value_to_json(parse_value(&s, elem_oid)));
                }
            }
        }
    }
}

// --- Date/time text parsing ------------------------------------------------------

/// Days from the Unix epoch for a civil date (Howard Hinnant's algorithm;
/// proleptic Gregorian, handles negative years).
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) as i64 + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Parse `YYYY-MM-DD` (with optional ` BC` suffix) into days since the Unix
/// epoch. `infinity`/`-infinity` are unrepresentable → `None`.
fn parse_pg_date(text: &str) -> Option<i64> {
    let text = text.trim();
    let (text, bc) = match text.strip_suffix(" BC") {
        Some(t) => (t, true),
        None => (text, false),
    };
    let mut it = text.splitn(3, '-');
    let y: i64 = it.next()?.parse().ok()?;
    let m: u32 = it.next()?.parse().ok()?;
    let d: u32 = it.next()?.parse().ok()?;
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return None;
    }
    let y = if bc { 1 - y } else { y }; // 1 BC = year 0
    Some(days_from_civil(y, m, d))
}

/// Parse `HH:MM:SS[.ffffff]` into µs since midnight.
fn parse_hms_us(text: &str) -> Option<i64> {
    let mut it = text.splitn(3, ':');
    let h: i64 = it.next()?.parse().ok()?;
    let m: i64 = it.next()?.parse().ok()?;
    let rest = it.next()?;
    let (s_str, frac_str) = match rest.split_once('.') {
        Some((s, f)) => (s, Some(f)),
        None => (rest, None),
    };
    let s: i64 = s_str.parse().ok()?;
    if !(0..=24).contains(&h) || !(0..=59).contains(&m) || !(0..=60).contains(&s) {
        return None;
    }
    let mut us = ((h * 60 + m) * 60 + s) * 1_000_000;
    if let Some(f) = frac_str {
        let digits: String = f.chars().take(6).collect();
        if digits.is_empty() || !digits.chars().all(|c| c.is_ascii_digit()) {
            return None;
        }
        let val: i64 = digits.parse().ok()?;
        us += val * 10_i64.pow(6 - digits.len() as u32);
    }
    Some(us)
}

/// Parse a trailing UTC offset (`+HH`, `-HH:MM`, `+HHMM`) off `text`, returning
/// `(rest, offset_us)`.
fn split_tz_offset(text: &str) -> (&str, i64) {
    // Find a '+'/'-' after the date part (skip the leading year sign position
    // and date hyphens by only looking after the last space or 'T').
    let start = text.rfind(|c| c == ' ' || c == 'T').map(|i| i + 1).unwrap_or(0);
    if let Some(pos) = text[start..].rfind(['+', '-']) {
        let idx = start + pos;
        let (rest, tz) = text.split_at(idx);
        let sign: i64 = if tz.starts_with('-') { -1 } else { 1 };
        let tz = &tz[1..];
        let (h, m) = match tz.split_once(':') {
            Some((h, m)) => (h.parse::<i64>().ok(), m.parse::<i64>().ok()),
            None if tz.len() == 4 => (tz[..2].parse::<i64>().ok(), tz[2..].parse::<i64>().ok()),
            None => (tz.parse::<i64>().ok(), Some(0)),
        };
        if let (Some(h), Some(m)) = (h, m) {
            return (rest, sign * (h * 60 + m) * 60 * 1_000_000);
        }
    }
    (text, 0)
}

/// Parse a PG timestamp / timestamptz text value into µs since the Unix epoch
/// (UTC). Accepts `YYYY-MM-DD[ T]HH:MM:SS[.ffffff][±TZ][ BC]`.
fn parse_pg_timestamp(text: &str) -> Option<i64> {
    let text = text.trim();
    let (text, bc) = match text.strip_suffix(" BC") {
        Some(t) => (t, true),
        None => (text, false),
    };
    let (text, tz_us) = split_tz_offset(text);
    let sep = text.find([' ', 'T'])?;
    let (date_part, time_part) = text.split_at(sep);
    let time_part = &time_part[1..];
    let date_part = if bc { format!("{date_part} BC") } else { date_part.to_string() };
    let days = parse_pg_date(&date_part)?;
    let us = parse_hms_us(time_part)?;
    Some(days * 86_400_000_000 + us - tz_us)
}

/// Parse a PG time / timetz text value into µs since midnight (offset applied
/// for timetz).
fn parse_pg_time(text: &str) -> Option<i64> {
    let (text, tz_us) = split_tz_offset(text.trim());
    Some(parse_hms_us(text)? - tz_us)
}

/// A big-endian byte cursor.
struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Cursor { buf, pos: 0 }
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.pos + n > self.buf.len() {
            bail!("unexpected end of pgoutput message");
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }
    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn i32(&mut self) -> Result<i32> {
        Ok(i32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn cstr(&mut self) -> Result<String> {
        let start = self.pos;
        while self.pos < self.buf.len() && self.buf[self.pos] != 0 {
            self.pos += 1;
        }
        let s = String::from_utf8_lossy(&self.buf[start..self.pos]).into_owned();
        self.pos += 1; // skip NUL
        Ok(s)
    }
}

#[cfg(test)]
mod estimate_tests {
    use super::*;

    #[test]
    fn estimated_bytes_tracks_row_payload() {
        let mut row = Row::new();
        row.insert("id", oql::value::Value::Number(1.0));
        row.insert("body", oql::value::Value::String("y".repeat(50_000)));
        let ev = LogicalEvent::Insert { table: "t".into(), row };
        let est = ev.estimated_bytes();
        assert!(est >= 50_000, "est {est} < payload");
        assert!(est < 55_000, "est {est} unexpectedly large");
        assert!(LogicalEvent::Commit.estimated_bytes() < 200);
    }
}

#[cfg(test)]
mod decode_tests {
    use super::*;

    // --- wire-message builders (mirror PG's pgoutput encoding) --------------

    fn cstr(out: &mut Vec<u8>, s: &str) {
        out.extend_from_slice(s.as_bytes());
        out.push(0);
    }

    fn relation_msg(rel_id: u32, table: &str, cols: &[(&str, u32)]) -> Vec<u8> {
        let mut m = vec![b'R'];
        m.extend_from_slice(&rel_id.to_be_bytes());
        cstr(&mut m, "public");
        cstr(&mut m, table);
        m.push(b'f'); // replica identity
        m.extend_from_slice(&(cols.len() as u16).to_be_bytes());
        for (name, oid) in cols {
            m.push(0); // flags
            cstr(&mut m, name);
            m.extend_from_slice(&oid.to_be_bytes());
            m.extend_from_slice(&0u32.to_be_bytes()); // typmod
        }
        m
    }

    enum Cell<'a> {
        Null,
        Unchanged,
        Text(&'a str),
        Bin(&'a [u8]),
    }

    fn tuple(out: &mut Vec<u8>, cells: &[Cell]) {
        out.extend_from_slice(&(cells.len() as u16).to_be_bytes());
        for c in cells {
            match c {
                Cell::Null => out.push(b'n'),
                Cell::Unchanged => out.push(b'u'),
                Cell::Text(s) => {
                    out.push(b't');
                    out.extend_from_slice(&(s.len() as u32).to_be_bytes());
                    out.extend_from_slice(s.as_bytes());
                }
                Cell::Bin(b) => {
                    out.push(b'b');
                    out.extend_from_slice(&(b.len() as u32).to_be_bytes());
                    out.extend_from_slice(b);
                }
            }
        }
    }

    fn insert_msg(rel_id: u32, cells: &[Cell]) -> Vec<u8> {
        let mut m = vec![b'I'];
        m.extend_from_slice(&rel_id.to_be_bytes());
        m.push(b'N');
        tuple(&mut m, cells);
        m
    }

    fn update_msg(rel_id: u32, old: Option<(u8, &[Cell])>, new: &[Cell]) -> Vec<u8> {
        let mut m = vec![b'U'];
        m.extend_from_slice(&rel_id.to_be_bytes());
        if let Some((kind, cells)) = old {
            m.push(kind);
            tuple(&mut m, cells);
        }
        m.push(b'N');
        tuple(&mut m, new);
        m
    }

    fn decoder_with_relation(cols: &[(&str, u32)]) -> Decoder {
        let mut d = Decoder::new();
        d.decode(&relation_msg(1, "t", cols)).unwrap();
        d
    }

    // --- TOAST ---------------------------------------------------------------

    #[test]
    fn unchanged_toast_column_is_absent_not_null() {
        let mut d = decoder_with_relation(&[("id", OID_TEXT), ("big", OID_TEXT)]);
        let ev = d
            .decode(&update_msg(1, None, &[Cell::Text("k1"), Cell::Unchanged]))
            .unwrap();
        let LogicalEvent::Update { row, .. } = ev else { panic!("not update") };
        assert_eq!(row.get("id"), Some(&Value::String("k1".into())));
        assert!(!row.contains_key("big"), "unchanged TOAST must be absent, got {:?}", row.get("big"));
    }

    #[test]
    fn unchanged_toast_merges_from_full_old_tuple() {
        // REPLICA IDENTITY FULL: 'O' old tuple carries the TOASTed value.
        let mut d = decoder_with_relation(&[("id", OID_TEXT), ("big", OID_TEXT), ("n", OID_INT4)]);
        let big = "x".repeat(100_000);
        let ev = d
            .decode(&update_msg(
                1,
                Some((b'O', &[Cell::Text("k1"), Cell::Text(&big), Cell::Text("1")])),
                &[Cell::Text("k1"), Cell::Unchanged, Cell::Text("2")],
            ))
            .unwrap();
        let LogicalEvent::Update { row, .. } = ev else { panic!("not update") };
        assert_eq!(row.get("big"), Some(&Value::String(big)));
        assert_eq!(row.get("n"), Some(&Value::Number(2.0)));
    }

    #[test]
    fn key_old_tuple_does_not_merge_nulls() {
        // 'K' tuples ship non-key columns as NULL; merging them would corrupt.
        let mut d = decoder_with_relation(&[("id", OID_TEXT), ("big", OID_TEXT)]);
        let ev = d
            .decode(&update_msg(
                1,
                Some((b'K', &[Cell::Text("k1"), Cell::Null])),
                &[Cell::Text("k2"), Cell::Unchanged],
            ))
            .unwrap();
        let LogicalEvent::Update { row, .. } = ev else { panic!("not update") };
        assert!(!row.contains_key("big"), "must stay absent for apply-side stored-row merge");
    }

    #[test]
    fn explicit_null_stays_null() {
        let mut d = decoder_with_relation(&[("id", OID_TEXT), ("big", OID_TEXT)]);
        let ev = d
            .decode(&update_msg(1, None, &[Cell::Text("k1"), Cell::Null]))
            .unwrap();
        let LogicalEvent::Update { row, .. } = ev else { panic!("not update") };
        assert_eq!(row.get("big"), Some(&Value::Null));
    }

    // --- types ----------------------------------------------------------------

    #[test]
    fn jsonb_text_decodes_to_json_value() {
        let mut d = decoder_with_relation(&[("id", OID_TEXT), ("meta", OID_JSONB)]);
        let ev = d
            .decode(&insert_msg(1, &[Cell::Text("k"), Cell::Text(r#"{"a":1}"#)]))
            .unwrap();
        let LogicalEvent::Insert { row, .. } = ev else { panic!() };
        assert_eq!(row.get("meta"), Some(&Value::Json(serde_json::json!({"a":1}))));
    }

    #[test]
    fn int8_beyond_2_53_is_exact() {
        let big = 9_007_199_254_740_993i64; // 2^53 + 1
        let mut d = decoder_with_relation(&[("id", OID_INT8)]);
        let ev = d.decode(&insert_msg(1, &[Cell::Text("9007199254740993")])).unwrap();
        let LogicalEvent::Insert { row, .. } = ev else { panic!() };
        assert_eq!(row.get("id"), Some(&Value::Int(big)));
        // And two adjacent big ids stay distinct (Zero's f64 model collapses them).
        let ev2 = d.decode(&insert_msg(1, &[Cell::Text("9007199254740994")])).unwrap();
        let LogicalEvent::Insert { row: row2, .. } = ev2 else { panic!() };
        assert_ne!(row.get("id"), row2.get("id"));
    }

    #[test]
    fn small_ints_stay_plain_numbers() {
        let mut d = decoder_with_relation(&[("a", OID_INT8), ("b", OID_INT4), ("c", OID_NUMERIC)]);
        let ev = d
            .decode(&insert_msg(1, &[Cell::Text("42"), Cell::Text("-7"), Cell::Text("19.99")]))
            .unwrap();
        let LogicalEvent::Insert { row, .. } = ev else { panic!() };
        assert_eq!(row.get("a"), Some(&Value::Number(42.0)));
        assert_eq!(row.get("b"), Some(&Value::Number(-7.0)));
        assert_eq!(row.get("c"), Some(&Value::Number(19.99)));
    }

    #[test]
    fn timestamps_decode_to_epoch_ms() {
        let mut d = decoder_with_relation(&[
            ("ts", OID_TIMESTAMPTZ),
            ("t2", OID_TIMESTAMP),
            ("d", OID_DATE),
            ("tm", OID_TIME),
        ]);
        let ev = d
            .decode(&insert_msg(
                1,
                &[
                    Cell::Text("2026-07-14 12:00:00.5+00"),
                    Cell::Text("1970-01-02 00:00:00"),
                    Cell::Text("1970-01-11"),
                    Cell::Text("01:00:00.25"),
                ],
            ))
            .unwrap();
        let LogicalEvent::Insert { row, .. } = ev else { panic!() };
        // 2026-07-14 12:00:00.5 UTC
        let expected = (days_from_civil(2026, 7, 14) as f64) * 86_400_000.0 + 12.0 * 3_600_000.0 + 500.0;
        assert_eq!(row.get("ts"), Some(&Value::Number(expected)));
        assert_eq!(row.get("t2"), Some(&Value::Number(86_400_000.0)));
        assert_eq!(row.get("d"), Some(&Value::Number(10.0 * 86_400_000.0)));
        assert_eq!(row.get("tm"), Some(&Value::Number(3_600_250.0)));
    }

    #[test]
    fn timestamp_with_negative_offset() {
        // 05:00:00-03 == 08:00:00 UTC
        let us = parse_pg_timestamp("1970-01-01 05:00:00-03").unwrap();
        assert_eq!(us, 8 * 3_600 * 1_000_000);
        // And a +HH:MM form.
        let us2 = parse_pg_timestamp("1970-01-01 05:30:00+05:30").unwrap();
        assert_eq!(us2, 0);
    }

    #[test]
    fn arrays_decode_to_json() {
        let mut d = decoder_with_relation(&[("xs", 1007), ("ss", 1009)]);
        let ev = d
            .decode(&insert_msg(
                1,
                &[Cell::Text("{1,2,NULL}"), Cell::Text(r#"{"a b","c\"d",NULL}"#)],
            ))
            .unwrap();
        let LogicalEvent::Insert { row, .. } = ev else { panic!() };
        assert_eq!(row.get("xs"), Some(&Value::Json(serde_json::json!([1, 2, null]))));
        assert_eq!(
            row.get("ss"),
            Some(&Value::Json(serde_json::json!(["a b", "c\"d", null])))
        );
    }

    #[test]
    fn nested_array_decodes() {
        let v = parse_pg_array("{{1,2},{3,4}}", OID_INT4).unwrap();
        assert_eq!(v, serde_json::json!([[1, 2], [3, 4]]));
    }

    #[test]
    fn binary_tuples_decode_instead_of_crashing() {
        let mut d = decoder_with_relation(&[
            ("id", OID_INT8),
            ("ok", OID_BOOL),
            ("w", OID_FLOAT8),
            ("s", OID_TEXT),
        ]);
        let ev = d
            .decode(&insert_msg(
                1,
                &[
                    Cell::Bin(&9_007_199_254_740_993i64.to_be_bytes()),
                    Cell::Bin(&[1]),
                    Cell::Bin(&2.5f64.to_be_bytes()),
                    Cell::Bin(b"hello"),
                ],
            ))
            .unwrap();
        let LogicalEvent::Insert { row, .. } = ev else { panic!() };
        assert_eq!(row.get("id"), Some(&Value::Int(9_007_199_254_740_993)));
        assert_eq!(row.get("ok"), Some(&Value::Bool(true)));
        assert_eq!(row.get("w"), Some(&Value::Number(2.5)));
        assert_eq!(row.get("s"), Some(&Value::String("hello".into())));
    }

    #[test]
    fn unknown_binary_type_is_clean_error_not_crash() {
        let mut d = decoder_with_relation(&[("x", 600 /* point */)]);
        let err = d.decode(&insert_msg(1, &[Cell::Bin(&[0; 16])])).unwrap_err();
        assert!(err.to_string().contains("unsupported binary-format type"), "{err}");
    }

    #[test]
    fn binary_numeric_decodes() {
        // 12345.678 = digits [1,2345,6780] weight 1 dscale 3 (base 10000)
        let mut b = Vec::new();
        b.extend_from_slice(&3u16.to_be_bytes()); // ndigits
        b.extend_from_slice(&1i16.to_be_bytes()); // weight
        b.extend_from_slice(&0u16.to_be_bytes()); // sign +
        b.extend_from_slice(&3u16.to_be_bytes()); // dscale
        for d in [1u16, 2345, 6780] {
            b.extend_from_slice(&d.to_be_bytes());
        }
        assert_eq!(decode_binary_numeric(&b).unwrap(), "12345.678");
    }

    // --- TRUNCATE ---------------------------------------------------------------

    #[test]
    fn truncate_decodes_table_names() {
        let mut d = Decoder::new();
        d.decode(&relation_msg(7, "a", &[("id", OID_TEXT)])).unwrap();
        d.decode(&relation_msg(9, "b", &[("id", OID_TEXT)])).unwrap();
        let mut m = vec![b'T'];
        m.extend_from_slice(&2u32.to_be_bytes());
        m.push(0); // options
        m.extend_from_slice(&7u32.to_be_bytes());
        m.extend_from_slice(&9u32.to_be_bytes());
        let ev = d.decode(&m).unwrap();
        assert_eq!(
            ev,
            LogicalEvent::Truncate { tables: vec!["a".into(), "b".into()] }
        );
    }

    #[test]
    fn bytea_binary_matches_text_hex() {
        assert_eq!(bytea_hex(&[0xde, 0xad, 0x01]), "\\xdead01");
    }
}
