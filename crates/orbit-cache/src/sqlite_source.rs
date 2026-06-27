//! A SQLite-backed IVM [`Input`] source — the Rust analog of Zero's `zqlite`
//! `TableSource`. Rows live in SQLite (durable, not memory-bound); `fetch` runs
//! a parameterized `SELECT`, and `push` maintains the same in-flight **overlay**
//! as the in-memory source so downstream operators see the post-change state
//! mid-push before the row is committed to SQLite.
//!
//! This makes the replica persistent and able to hold datasets larger than RAM,
//! while reusing the rest of the IVM engine unchanged. Wire it into a query via
//! [`SqliteProvider`].

use oql::ast::{Direction, Ordering as AstOrdering};
use oql::ivm::constraint::constraint_matches_row;
use oql::ivm::operator::{deliver, Basis, FetchRequest, Input, Link, OpHandle, Operator};
use oql::ivm::{Change, ColumnType, Node, Schema, SourceChange};
use oql::value::{values_identical, Comparator, Row, Value};
use oql::SourceProvider;
use rusqlite::Connection;
use std::cell::RefCell;
use std::cmp::Ordering as CmpOrdering;
use std::collections::BTreeMap;
use std::rc::Rc;

struct Conn {
    sort: AstOrdering,
    output: Option<Link>,
    last_pushed_epoch: u64,
    schema: Rc<Schema>,
}

/// A SQLite-backed source for one table.
pub struct SqliteSource {
    table: String,
    columns: BTreeMap<String, ColumnType>,
    primary_key: Vec<String>,
    db: Connection,
    connections: Vec<Conn>,
    overlay: Option<(u64, SourceChange)>,
    push_epoch: u64,
}

impl SqliteSource {
    /// Create an in-memory SQLite-backed source. (Pass a file-backed
    /// `Connection` via [`with_connection`](Self::with_connection) for
    /// durability.)
    pub fn new(
        table: impl Into<String>,
        columns: BTreeMap<String, ColumnType>,
        primary_key: Vec<String>,
    ) -> Rc<RefCell<SqliteSource>> {
        Self::with_connection(Connection::open_in_memory().expect("open sqlite"), table, columns, primary_key)
    }

    pub fn with_connection(
        db: Connection,
        table: impl Into<String>,
        columns: BTreeMap<String, ColumnType>,
        primary_key: Vec<String>,
    ) -> Rc<RefCell<SqliteSource>> {
        let table = table.into();
        // Keep declared primary-key order (it drives order-by tie-breaks).
        let pk = primary_key;
        let col_defs = columns
            .keys()
            .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(", ");
        let pk_defs = pk.iter().map(|c| format!("\"{c}\"")).collect::<Vec<_>>().join(", ");
        db.execute_batch(&format!(
            "CREATE TABLE IF NOT EXISTS \"{table}\" ({col_defs}, PRIMARY KEY ({pk_defs}))"
        ))
        .expect("create replica table");
        Rc::new(RefCell::new(SqliteSource {
            table,
            columns,
            primary_key: pk,
            db,
            connections: Vec::new(),
            overlay: None,
            push_epoch: 0,
        }))
    }

    pub fn table_name(&self) -> &str {
        &self.table
    }
    pub fn primary_key(&self) -> &[String] {
        &self.primary_key
    }

    /// Insert a row directly (initial load), bypassing change propagation.
    pub fn insert_initial(&self, row: &Row) {
        let (sql, params) = self.insert_sql(row);
        self.db.execute(&sql, rusqlite::params_from_iter(params)).expect("insert_initial");
    }

    /// Look up the stored row matching the primary key of `key_row`.
    pub fn lookup(&self, key_row: &Row) -> Option<Row> {
        let cols = self.col_list();
        let select = cols.iter().map(|c| format!("\"{}\"", c.replace('"', "\"\""))).collect::<Vec<_>>().join(", ");
        let sql = format!("SELECT {select} FROM \"{}\" WHERE {}", self.table, self.pk_where());
        let params: Vec<SqlVal> =
            self.primary_key.iter().map(|k| SqlVal(key_row.get(k).cloned().unwrap_or(Value::Null))).collect();
        let col_types: Vec<(String, ColumnType)> = cols.iter().map(|c| (c.clone(), self.columns[c])).collect();
        let mut stmt = self.db.prepare(&sql).ok()?;
        stmt.query_row(rusqlite::params_from_iter(params), |r| {
            let mut row = Row::new();
            for (i, (name, ty)) in col_types.iter().enumerate() {
                row.insert(name.as_str(), read_value(r, i, *ty));
            }
            Ok(row)
        })
        .ok()
    }

    fn col_list(&self) -> Vec<String> {
        self.columns.keys().cloned().collect()
    }

    fn insert_sql(&self, row: &Row) -> (String, Vec<SqlVal>) {
        let cols = self.col_list();
        let placeholders = (1..=cols.len()).map(|i| format!("?{i}")).collect::<Vec<_>>().join(", ");
        let col_idents = cols.iter().map(|c| format!("\"{}\"", c.replace('"', "\"\""))).collect::<Vec<_>>().join(", ");
        let params = cols.iter().map(|c| SqlVal(row.get(c).cloned().unwrap_or(Value::Null))).collect();
        (
            format!("INSERT OR REPLACE INTO \"{}\" ({col_idents}) VALUES ({placeholders})", self.table),
            params,
        )
    }

    fn pk_where(&self) -> String {
        self.primary_key
            .iter()
            .enumerate()
            .map(|(i, c)| format!("\"{}\" = ?{}", c.replace('"', "\"\""), i + 1))
            .collect::<Vec<_>>()
            .join(" AND ")
    }

    fn write_change(&self, change: &SourceChange) {
        match change {
            SourceChange::Add(r) => {
                let (sql, params) = self.insert_sql(r);
                self.db.execute(&sql, rusqlite::params_from_iter(params)).unwrap();
            }
            SourceChange::Remove(r) => {
                let params: Vec<SqlVal> =
                    self.primary_key.iter().map(|k| SqlVal(r.get(k).cloned().unwrap_or(Value::Null))).collect();
                self.db
                    .execute(
                        &format!("DELETE FROM \"{}\" WHERE {}", self.table, self.pk_where()),
                        rusqlite::params_from_iter(params),
                    )
                    .unwrap();
            }
            SourceChange::Edit { row, old_row } => {
                let params: Vec<SqlVal> =
                    self.primary_key.iter().map(|k| SqlVal(old_row.get(k).cloned().unwrap_or(Value::Null))).collect();
                self.db
                    .execute(
                        &format!("DELETE FROM \"{}\" WHERE {}", self.table, self.pk_where()),
                        rusqlite::params_from_iter(params),
                    )
                    .unwrap();
                let (sql, p) = self.insert_sql(row);
                self.db.execute(&sql, rusqlite::params_from_iter(p)).unwrap();
            }
        }
    }

    /// All rows (primary-key order) — for snapshotting the replica.
    pub fn all_rows(&self) -> Vec<Row> {
        let sort: AstOrdering = self.primary_key().iter().map(|k| (k.clone(), Direction::Asc)).collect();
        self.select_all(&sort)
    }

    /// Read all rows (typed by column) ordered by `sort`.
    fn select_all(&self, sort: &AstOrdering) -> Vec<Row> {
        let cols = self.col_list();
        let select = cols.iter().map(|c| format!("\"{}\"", c.replace('"', "\"\""))).collect::<Vec<_>>().join(", ");
        let order = sort
            .iter()
            .map(|(c, d)| {
                let dir = if matches!(d, Direction::Asc) { "ASC" } else { "DESC" };
                format!("\"{}\" {dir}", c.replace('"', "\"\""))
            })
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!("SELECT {select} FROM \"{}\" ORDER BY {order}", self.table);
        let mut stmt = self.db.prepare(&sql).unwrap();
        let col_types: Vec<(String, ColumnType)> = cols.iter().map(|c| (c.clone(), self.columns[c])).collect();
        let rows = stmt
            .query_map([], |r| {
                let mut row = Row::new();
                for (i, (name, ty)) in col_types.iter().enumerate() {
                    row.insert(name.as_str(), read_value(r, i, *ty));
                }
                Ok(row)
            })
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();
        rows
    }

    fn build_schema(&self, sort: &AstOrdering) -> Rc<Schema> {
        let order: oql::value::Ordering2 = sort
            .iter()
            .map(|(f, d)| {
                (f.clone(), match d {
                    Direction::Asc => oql::value::Direction::Asc,
                    Direction::Desc => oql::value::Direction::Desc,
                })
            })
            .collect();
        Rc::new(Schema::leaf(
            self.table.clone(),
            self.columns.clone(),
            self.primary_key.clone(),
            Some(sort.clone()),
            Comparator::new(order, false),
        ))
    }

    fn fetch_conn(&self, req: &FetchRequest, conn_idx: usize) -> Vec<Node> {
        let conn = &self.connections[conn_idx];
        let mut rows = self.select_all(&conn.sort);

        // Overlay (post-change view during a push), gated by epoch.
        if let Some((epoch, change)) = &self.overlay {
            if conn.last_pushed_epoch >= *epoch {
                match change {
                    SourceChange::Add(r) => rows.push(r.clone()),
                    SourceChange::Remove(r) => rows.retain(|x| !self.pk_eq(x, r)),
                    SourceChange::Edit { row, old_row } => {
                        rows.retain(|x| !self.pk_eq(x, old_row));
                        rows.push(row.clone());
                    }
                }
            }
        }

        if let Some(c) = &req.constraint {
            rows.retain(|r| constraint_matches_row(c, r));
        }

        let order: oql::value::Ordering2 = conn
            .sort
            .iter()
            .map(|(f, d)| {
                (f.clone(), match d {
                    Direction::Asc => oql::value::Direction::Asc,
                    Direction::Desc => oql::value::Direction::Desc,
                })
            })
            .collect();
        let cmp = Comparator::new(order, req.reverse);
        rows.sort_by(|a, b| cmp.compare(a, b));

        if let Some(start) = &req.start {
            let pos = rows.iter().position(|r| {
                let c = cmp.compare(r, &start.row);
                match start.basis {
                    Basis::At => c != CmpOrdering::Less,
                    Basis::After => c == CmpOrdering::Greater,
                }
            });
            rows = match pos {
                Some(p) => rows.split_off(p),
                None => Vec::new(),
            };
        }

        rows.into_iter().map(Node::new).collect()
    }

    fn pk_eq(&self, a: &Row, b: &Row) -> bool {
        self.primary_key.iter().all(|k| {
            values_identical(a.get(k).unwrap_or(&Value::Null), b.get(k).unwrap_or(&Value::Null))
        })
    }
}

/// Connect a new consumer; returns a handle usable as both [`Input`] and
/// [`Operator`].
pub fn connect(src: &Rc<RefCell<SqliteSource>>, sort: AstOrdering) -> Rc<RefCell<SqliteConnection>> {
    let conn_idx;
    {
        let mut s = src.borrow_mut();
        let schema = s.build_schema(&sort);
        conn_idx = s.connections.len();
        s.connections.push(Conn { sort, output: None, last_pushed_epoch: 0, schema });
    }
    Rc::new(RefCell::new(SqliteConnection { source: Rc::clone(src), conn_idx }))
}

/// Apply a change and propagate it to connected outputs (overlay then commit).
pub fn source_push(src: &Rc<RefCell<SqliteSource>>, change: SourceChange) {
    let epoch = {
        let mut s = src.borrow_mut();
        s.push_epoch += 1;
        let e = s.push_epoch;
        s.overlay = Some((e, change.clone()));
        e
    };
    let n = src.borrow().connections.len();
    for i in 0..n {
        let output = {
            let mut s = src.borrow_mut();
            s.connections[i].last_pushed_epoch = epoch;
            s.connections[i].output.clone()
        };
        if let Some(output) = output {
            deliver(&output, base_change(&change));
        }
    }
    let mut s = src.borrow_mut();
    s.overlay = None;
    s.write_change(&change);
}

fn base_change(change: &SourceChange) -> Change {
    match change {
        SourceChange::Add(r) => Change::Add(Node::new(r.clone())),
        SourceChange::Remove(r) => Change::Remove(Node::new(r.clone())),
        SourceChange::Edit { row, old_row } => Change::Edit {
            node: Node::new(row.clone()),
            old_node: Node::new(old_row.clone()),
        },
    }
}

/// A connection handle for [`SqliteSource`].
pub struct SqliteConnection {
    source: Rc<RefCell<SqliteSource>>,
    conn_idx: usize,
}

impl Input for SqliteConnection {
    fn get_schema(&self) -> Rc<Schema> {
        Rc::clone(&self.source.borrow().connections[self.conn_idx].schema)
    }
    fn fetch(&self, req: &FetchRequest) -> Vec<Node> {
        self.source.borrow().fetch_conn(req, self.conn_idx)
    }
}

impl Operator for SqliteConnection {
    fn push(&mut self, _change: Change) -> oql::ivm::Changes {
        unreachable!("a source connection never receives a push from upstream")
    }
    fn output(&self) -> Option<Link> {
        self.source.borrow().connections[self.conn_idx].output.clone()
    }
    fn set_output(&mut self, out: Link) {
        self.source.borrow_mut().connections[self.conn_idx].output = Some(out);
    }
}

/// A [`SourceProvider`] over a set of [`SqliteSource`]s.
#[derive(Default)]
pub struct SqliteProvider {
    sources: std::collections::HashMap<String, Rc<RefCell<SqliteSource>>>,
}

impl SqliteProvider {
    pub fn new() -> Self {
        SqliteProvider::default()
    }
    pub fn add(&mut self, src: Rc<RefCell<SqliteSource>>) {
        let name = src.borrow().table_name().to_string();
        self.sources.insert(name, src);
    }
    pub fn source(&self, table: &str) -> Option<Rc<RefCell<SqliteSource>>> {
        self.sources.get(table).cloned()
    }
}

impl SourceProvider for SqliteProvider {
    fn primary_key(&self, table: &str) -> Option<Vec<String>> {
        self.sources.get(table).map(|s| s.borrow().primary_key().to_vec())
    }
    fn connect(&self, table: &str, sort: AstOrdering) -> Option<OpHandle> {
        self.sources.get(table).map(|s| OpHandle::new(connect(s, sort)))
    }
}

/// A SQLite-backed replica: a [`ReplicaBackend`](crate::replica::ReplicaBackend)
/// holding a [`SqliteSource`] per table. Pass a directory to make it durable.
#[derive(Default)]
pub struct SqliteReplica {
    sources: std::collections::HashMap<String, Rc<RefCell<SqliteSource>>>,
    columns: std::collections::HashMap<String, Vec<(String, ColumnType)>>,
    dir: Option<std::path::PathBuf>,
}

impl SqliteReplica {
    /// An in-memory replica (each table in its own in-memory database).
    pub fn in_memory() -> Self {
        SqliteReplica::default()
    }

    /// A durable replica storing each table at `dir/<table>.db`.
    pub fn durable(dir: impl Into<std::path::PathBuf>) -> Self {
        SqliteReplica { dir: Some(dir.into()), ..Default::default() }
    }

    pub fn add_table(
        &mut self,
        name: &str,
        columns: Vec<(String, ColumnType)>,
        primary_key: Vec<String>,
    ) -> Rc<RefCell<SqliteSource>> {
        let conn = match &self.dir {
            Some(dir) => {
                std::fs::create_dir_all(dir).ok();
                Connection::open(dir.join(format!("{name}.db"))).expect("open sqlite file")
            }
            None => Connection::open_in_memory().expect("open sqlite"),
        };
        let col_map: BTreeMap<String, ColumnType> = columns.iter().cloned().collect();
        let src = SqliteSource::with_connection(conn, name, col_map, primary_key);
        self.columns.insert(name.to_string(), columns);
        self.sources.insert(name.to_string(), Rc::clone(&src));
        src
    }

    pub fn source(&self, name: &str) -> Option<Rc<RefCell<SqliteSource>>> {
        self.sources.get(name).cloned()
    }
}

impl SourceProvider for SqliteReplica {
    fn primary_key(&self, table: &str) -> Option<Vec<String>> {
        self.sources.get(table).map(|s| s.borrow().primary_key().to_vec())
    }
    fn connect(&self, table: &str, sort: AstOrdering) -> Option<OpHandle> {
        self.sources.get(table).map(|s| OpHandle::new(connect(s, sort)))
    }
}

impl crate::replica::ReplicaBackend for SqliteReplica {
    fn apply(&self, event: crate::LogicalEvent) {
        use crate::LogicalEvent as E;
        match event {
            E::Insert { table, row } => {
                if let Some(src) = self.sources.get(&table) {
                    let existing = src.borrow().lookup(&row);
                    match existing {
                        None => source_push(src, SourceChange::Add(row)),
                        Some(old) => source_push(src, SourceChange::Edit { row, old_row: old }),
                    }
                }
            }
            E::Delete { table, old_row } => {
                if let Some(src) = self.sources.get(&table) {
                    let stored = src.borrow().lookup(&old_row);
                    if let Some(stored) = stored {
                        source_push(src, SourceChange::Remove(stored));
                    }
                }
            }
            E::Update { table, row, old_row } => {
                if let Some(src) = self.sources.get(&table) {
                    let key = old_row.as_ref().unwrap_or(&row);
                    let existing = src.borrow().lookup(key);
                    match existing {
                        Some(old) => source_push(src, SourceChange::Edit { row, old_row: old }),
                        None => source_push(src, SourceChange::Add(row)),
                    }
                }
            }
            // SQLite tables have a fixed schema created up front; a dropped
            // column simply stops being written/selected. (ALTER of the SQLite
            // replica table on DDL is a future refinement.)
            E::Relation { .. } | E::Begin | E::Commit | E::Other => {}
        }
    }
    fn seed(&self, table: &str, row: Row) {
        if let Some(src) = self.sources.get(table) {
            src.borrow().insert_initial(&row);
        }
    }
    fn table_columns(&self, table: &str) -> Vec<(String, ColumnType)> {
        self.columns.get(table).cloned().unwrap_or_default()
    }
    fn snapshot(&self) -> Vec<(String, Vec<Row>)> {
        self.sources
            .iter()
            .map(|(name, src)| (name.clone(), src.borrow().all_rows()))
            .collect()
    }
}

/// Newtype so we can implement `ToSql` for [`Value`].
struct SqlVal(Value);

impl rusqlite::ToSql for SqlVal {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        use rusqlite::types::{ToSqlOutput, Value as SqliteValue};
        Ok(match &self.0 {
            Value::Null => ToSqlOutput::Owned(SqliteValue::Null),
            Value::Bool(b) => ToSqlOutput::Owned(SqliteValue::Integer(if *b { 1 } else { 0 })),
            Value::Number(n) => {
                if n.fract() == 0.0 && n.is_finite() {
                    ToSqlOutput::Owned(SqliteValue::Integer(*n as i64))
                } else {
                    ToSqlOutput::Owned(SqliteValue::Real(*n))
                }
            }
            Value::String(s) => ToSqlOutput::Owned(SqliteValue::Text(s.clone())),
            Value::Json(j) => ToSqlOutput::Owned(SqliteValue::Text(j.to_string())),
        })
    }
}

fn read_value(row: &rusqlite::Row, idx: usize, ty: ColumnType) -> Value {
    use rusqlite::types::ValueRef;
    match row.get_ref(idx) {
        Ok(ValueRef::Null) | Err(_) => Value::Null,
        Ok(v) => match ty {
            ColumnType::Boolean => Value::Bool(v.as_i64().map(|i| i != 0).unwrap_or(false)),
            ColumnType::Number => match v {
                ValueRef::Integer(i) => Value::Number(i as f64),
                ValueRef::Real(f) => Value::Number(f),
                ValueRef::Text(t) => std::str::from_utf8(t)
                    .ok()
                    .and_then(|s| s.parse::<f64>().ok())
                    .map(Value::Number)
                    .unwrap_or(Value::Null),
                _ => Value::Null,
            },
            ColumnType::Json => v
                .as_str()
                .ok()
                .and_then(|s| serde_json::from_str(s).ok())
                .map(Value::from_json)
                .unwrap_or(Value::Null),
            ColumnType::String => v.as_str().map(|s| Value::String(s.to_string())).unwrap_or(Value::Null),
            ColumnType::Null => Value::Null,
        },
    }
}
