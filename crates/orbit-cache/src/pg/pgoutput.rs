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
    /// A table's column set (Relation message) — surfaces DDL schema changes.
    Relation { table: String, columns: Vec<(String, ColumnType)> },
    /// Truncate / Type / Origin / Message — ignored.
    Other,
}

/// Map a Postgres type OID to an Orbit column type.
pub fn column_type_for_oid(oid: u32) -> ColumnType {
    match oid {
        16 => ColumnType::Boolean,
        20 | 21 | 23 | 700 | 701 | 1700 => ColumnType::Number,
        114 | 3802 => ColumnType::Json,
        _ => ColumnType::String,
    }
}

/// Holds the relation cache across messages within a connection.
#[derive(Default)]
pub struct Decoder {
    relations: HashMap<u32, Relation>,
}

impl Decoder {
    pub fn new() -> Self {
        Decoder::default()
    }

    /// Decode one pgoutput message (the body of an XLogData frame).
    pub fn decode(&mut self, data: &[u8]) -> Result<LogicalEvent> {
        if data.is_empty() {
            bail!("empty pgoutput message");
        }
        let mut c = Cursor::new(data);
        let tag = c.u8()?;
        match tag {
            b'B' => Ok(LogicalEvent::Begin),
            b'C' => Ok(LogicalEvent::Commit),
            b'R' => self.decode_relation(&mut c),
            b'I' => self.decode_insert(&mut c),
            b'U' => self.decode_update(&mut c),
            b'D' => self.decode_delete(&mut c),
            // 'T' truncate, 'Y' type, 'O' origin, 'M' message.
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
        self.relations.insert(rel_id, Relation { name: name.clone(), columns });
        Ok(LogicalEvent::Relation { table: name, columns: typed })
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
        let mut kind = c.u8()?;
        if kind == b'K' || kind == b'O' {
            // Key or old-tuple (REPLICA IDENTITY). 'O' = full old row.
            old_row = Some(decode_tuple(c, &rel)?);
            kind = c.u8()?;
        }
        if kind != b'N' {
            bail!("unexpected update tuple kind {kind}");
        }
        let row = decode_tuple(c, &rel)?;
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
            bail!("unexpected delete tuple kind {kind}");
        }
        let old_row = decode_tuple(c, &rel)?;
        Ok(LogicalEvent::Delete {
            table: rel.name,
            old_row,
        })
    }
}

/// Decode a TupleData into a [`Row`], mapping values by column type.
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
                // (the column simply won't be set in this row).
            }
            b't' => {
                let len = c.u32()? as usize;
                let bytes = c.take(len)?;
                let text = std::str::from_utf8(bytes)?;
                if let Some(col) = col {
                    row.insert(col.name.as_str(), parse_value(text, col.type_oid));
                }
            }
            other => bail!("unknown tuple column kind {other}"),
        }
    }
    Ok(row)
}

/// Map a pgoutput text value to a [`Value`] using the column's type OID.
fn parse_value(text: &str, type_oid: u32) -> Value {
    match type_oid {
        16 => Value::Bool(text == "t"), // bool
        20 | 21 | 23 => text // int8, int2, int4
            .parse::<i64>()
            .map(|n| Value::Number(n as f64))
            .unwrap_or_else(|_| Value::String(text.to_string())),
        700 | 701 | 1700 => text // float4, float8, numeric
            .parse::<f64>()
            .map(Value::Number)
            .unwrap_or_else(|_| Value::String(text.to_string())),
        _ => Value::String(text.to_string()),
    }
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
