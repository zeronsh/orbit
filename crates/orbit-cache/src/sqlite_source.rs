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
    /// Superset-safe pushable subset of the query's WHERE condition — applied
    /// to FETCH SQL only (pushes still flow unfiltered; the pipeline's Filter
    /// re-applies the full predicate). See `pushable_subset`.
    filter: Option<oql::ast::Condition>,
    /// Weak (see the memory source): dead downstream pipelines are pruned by
    /// the push loop instead of receiving every future change forever.
    output: Option<oql::ivm::operator::WeakLink>,
    active_pos: usize,
    last_pushed_epoch: u64,
    schema: Rc<Schema>,
}

/// A SQLite-backed source for one table.
pub struct SqliteSource {
    table: String,
    columns: BTreeMap<String, ColumnType>,
    primary_key: Vec<String>,
    /// Shared with every other table of the same replica: ONE database, so a
    /// multi-table upstream transaction commits (or rolls back) atomically.
    db: Rc<Connection>,
    connections: Vec<Option<Conn>>,
    active_connections: Vec<usize>,
    overlay: Option<(u64, SourceChange)>,
    push_epoch: u64,
    /// Names of secondary indexes already created (lazily, per fetch shape), so
    /// the hot path doesn't re-issue `CREATE INDEX IF NOT EXISTS` DDL.
    created_indexes: RefCell<std::collections::HashSet<String>>,
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
        Self::with_shared(Rc::new(db), table, columns, primary_key)
    }

    /// Create a source over a connection shared with other tables of the same
    /// replica (one database file → atomic multi-table transactions).
    pub fn with_shared(
        db: Rc<Connection>,
        table: impl Into<String>,
        columns: BTreeMap<String, ColumnType>,
        primary_key: Vec<String>,
    ) -> Rc<RefCell<SqliteSource>> {
        let table = table.into();
        // Replica-appropriate settings (same as Zero's zqlite replica): WAL for
        // concurrent-read-friendly durability (no-op on in-memory databases),
        // NORMAL synchronous (safe under WAL), and a prepared-statement cache so
        // the per-push/per-fetch statements never re-parse SQL.
        let _ = db.pragma_update(None, "journal_mode", "WAL");
        let _ = db.pragma_update(None, "synchronous", "NORMAL");
        db.set_prepared_statement_cache_capacity(64);
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
            active_connections: Vec::new(),
            overlay: None,
            push_epoch: 0,
            created_indexes: RefCell::new(std::collections::HashSet::new()),
        }))
    }

    pub fn table_name(&self) -> &str {
        &self.table
    }
    pub fn primary_key(&self) -> &[String] {
        &self.primary_key
    }

    /// Reconcile the physical table to an upstream column set (DDL surfaced by
    /// a Relation message). Added columns are `ALTER TABLE … ADD COLUMN`
    /// (NULL-filled, matching the in-memory backend where they appear only in
    /// subsequently-replicated rows). Removed columns are dropped — after
    /// dropping any lazily-created secondary indexes (SQLite refuses to drop an
    /// indexed column); primary-key columns are never dropped (warn instead).
    pub fn reconcile_columns(&mut self, new_columns: &[(String, ColumnType)]) {
        let mut added: Vec<&(String, ColumnType)> =
            new_columns.iter().filter(|(n, _)| !self.columns.contains_key(n)).collect();
        let mut removed: Vec<String> = self
            .columns
            .keys()
            .filter(|c| !new_columns.iter().any(|(n, _)| n == *c))
            .cloned()
            .collect();
        // RENAME pairing (see the in-memory reconcile): one out + one in of
        // the same type → ALTER TABLE … RENAME COLUMN, values preserved.
        if removed.len() == 1
            && added.len() == 1
            && self.columns.get(&removed[0]) == Some(&added[0].1)
            && !self.primary_key.iter().any(|k| *k == removed[0])
        {
            let (from, to) = (removed[0].clone(), added[0].0.clone());
            eprintln!("replica DDL: treating column {from} -> {to} as a RENAME (values preserved)");
            for idx in self.created_indexes.borrow_mut().drain() {
                let _ = self.db.execute_batch(&format!("DROP INDEX IF EXISTS {}", ident(&idx)));
            }
            let sql = format!(
                "ALTER TABLE {} RENAME COLUMN {} TO {}",
                ident(&self.table),
                ident(&from),
                ident(&to)
            );
            if let Err(e) = self.db.execute_batch(&sql) {
                eprintln!("replica DDL: rename column {from} -> {to} failed: {e}");
            } else {
                // Keep the logical column map in step with the physical rename
                // NOW — the shared early-return below fires when the rename was
                // the only change.
                if let Some(ty) = self.columns.remove(&from) {
                    self.columns.insert(to.clone(), ty);
                }
                added.clear();
                removed.clear();
            }
        }
        // Columns whose TYPE changed while keeping the name: Postgres rewrites
        // the table on `ALTER COLUMN … TYPE` but never re-sends the rows, so
        // the stored values must be converted in place (audit Tier 0.4 —
        // previously invisible: reconcile compared names only).
        let changed: Vec<(String, ColumnType, ColumnType)> = new_columns
            .iter()
            .filter_map(|(n, t)| {
                self.columns.get(n).filter(|old| *old != t).map(|old| (n.clone(), *old, *t))
            })
            .collect();
        if added.is_empty() && removed.is_empty() && changed.is_empty() {
            return;
        }
        for (name, _) in &added {
            let sql = format!("ALTER TABLE {} ADD COLUMN {}", ident(&self.table), ident(name));
            if let Err(e) = self.db.execute_batch(&sql) {
                eprintln!("replica DDL: add column {name} to {} failed: {e}", self.table);
            }
        }
        if !removed.is_empty() || !changed.is_empty() {
            // Secondary indexes may cover a doomed/retyped column; drop them
            // all (they are lazily re-created per fetch shape).
            for idx in self.created_indexes.borrow_mut().drain() {
                let _ = self.db.execute_batch(&format!("DROP INDEX IF EXISTS {}", ident(&idx)));
            }
        }
        for name in &removed {
            if self.primary_key.iter().any(|k| k == name) {
                eprintln!(
                    "replica DDL: refusing to drop primary-key column {name} of {}",
                    self.table
                );
                continue;
            }
            let sql = format!("ALTER TABLE {} DROP COLUMN {}", ident(&self.table), ident(name));
            if let Err(e) = self.db.execute_batch(&sql) {
                eprintln!("replica DDL: drop column {name} from {} failed: {e}", self.table);
            }
        }
        // Convert stored values of retyped columns: read with the OLD type,
        // convert, write with the NEW type's binding. Runs inside the
        // surrounding replication transaction (Relation events are applied
        // between Begin/Commit), so a crash mid-rewrite rolls back atomically.
        for (name, old_ty, new_ty) in &changed {
            let convert = |db: &Connection| -> anyhow::Result<()> {
                let select =
                    format!("SELECT rowid, {} FROM {}", ident(name), ident(&self.table));
                let mut read = db.prepare(&select)?;
                let rows: Vec<(i64, Value)> = read
                    .query_map([], |r| {
                        Ok((r.get::<_, i64>(0)?, read_value(r, 1, *old_ty)))
                    })?
                    .filter_map(|r| r.ok())
                    .collect();
                let update = format!(
                    "UPDATE {} SET {} = ?1 WHERE rowid = ?2",
                    ident(&self.table),
                    ident(name)
                );
                let mut write = db.prepare(&update)?;
                for (rowid, v) in rows {
                    let converted = oql::ivm::schema::convert_column_value(&v, *new_ty);
                    // Match `param`'s JSON-column encoding for the NEW type.
                    let bound = match (new_ty, &converted) {
                        (_, Value::Null) => SqlVal(Value::Null),
                        (ColumnType::Json, _) => SqlVal(Value::String(
                            serde_json::to_string(&converted).unwrap_or_default(),
                        )),
                        _ => SqlVal(converted),
                    };
                    write.execute(rusqlite::params![bound, rowid])?;
                }
                Ok(())
            };
            if let Err(e) = convert(&self.db) {
                eprintln!(
                    "replica DDL: converting column {name} of {} from {old_ty:?} to {new_ty:?} failed: {e}",
                    self.table
                );
            }
        }
        self.columns = new_columns.iter().map(|(n, t)| (n.clone(), *t)).collect();
        // PK columns must stay known even if upstream stopped reporting them
        // (we refused to drop them above).
        for k in &self.primary_key {
            self.columns.entry(k.clone()).or_insert(ColumnType::String);
        }
    }

    /// Number of live query connections attached to this source.
    pub fn connection_count(&self) -> usize {
        self.active_connections.len()
    }

    fn disconnect(&mut self, conn_idx: usize) {
        let Some(connection) = self.connections[conn_idx].take() else {
            return;
        };
        let active_pos = connection.active_pos;
        debug_assert_eq!(self.active_connections[active_pos], conn_idx);
        self.active_connections.swap_remove(active_pos);
        if let Some(&moved_idx) = self.active_connections.get(active_pos) {
            self.connections[moved_idx].as_mut().unwrap().active_pos = active_pos;
        }
        while self.connections.last().is_some_and(Option::is_none) {
            self.connections.pop();
        }
    }

    /// Insert a row directly (initial load), bypassing change propagation.
    pub fn insert_initial(&self, row: &Row) -> anyhow::Result<()> {
        let (sql, params) = self.insert_sql(row);
        self.db
            .prepare_cached(&sql)?
            .execute(rusqlite::params_from_iter(params))?;
        Ok(())
    }

    /// Look up the stored row matching the primary key of `key_row`.
    pub fn lookup(&self, key_row: &Row) -> Option<Row> {
        let cols = self.col_list();
        let select = cols.iter().map(|c| format!("\"{}\"", c.replace('"', "\"\""))).collect::<Vec<_>>().join(", ");
        let sql = format!("SELECT {select} FROM \"{}\" WHERE {}", self.table, self.pk_where());
        let params: Vec<SqlVal> =
            self.primary_key.iter().map(|k| self.param(k, key_row.get(k).cloned().unwrap_or(Value::Null))).collect();
        let col_types: Vec<(String, ColumnType)> = cols.iter().map(|c| (c.clone(), self.columns[c])).collect();
        let mut stmt = self.db.prepare_cached(&sql).ok()?;
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

    /// Bind `v` for column `col`, respecting the column's declared type. JSON
    /// columns store the JSON-serialized TEXT of the value so any JSON primitive
    /// (bool/number/string/object) round-trips exactly — a native binding would
    /// collapse `true` and `1` into the same INTEGER. SQL NULL stays NULL.
    fn param(&self, col: &str, v: Value) -> SqlVal {
        match (self.columns.get(col), &v) {
            (_, Value::Null) => SqlVal(Value::Null),
            (Some(ColumnType::Json), _) => {
                SqlVal(Value::String(serde_json::to_string(&v).unwrap_or_default()))
            }
            _ => SqlVal(v),
        }
    }

    fn insert_sql(&self, row: &Row) -> (String, Vec<SqlVal>) {
        let cols = self.col_list();
        let placeholders = (1..=cols.len()).map(|i| format!("?{i}")).collect::<Vec<_>>().join(", ");
        let col_idents = cols.iter().map(|c| format!("\"{}\"", c.replace('"', "\"\""))).collect::<Vec<_>>().join(", ");
        let params =
            cols.iter().map(|c| self.param(c, row.get(c).cloned().unwrap_or(Value::Null))).collect();
        (
            format!("INSERT OR REPLACE INTO \"{}\" ({col_idents}) VALUES ({placeholders})", self.table),
            params,
        )
    }

    fn pk_where(&self) -> String {
        // `IS` (not `=`): row identity treats NULL as matching NULL
        // (`values_identical`), and a primary-key component can be NULL. SQLite's
        // `IS` works with bound parameters and stays index-sargable.
        self.primary_key
            .iter()
            .enumerate()
            .map(|(i, c)| format!("\"{}\" IS ?{}", c.replace('"', "\"\""), i + 1))
            .collect::<Vec<_>>()
            .join(" AND ")
    }

    /// Persist a change to SQLite. Errors propagate (no panic): the caller
    /// rolls back the surrounding replication transaction and halts cleanly —
    /// a torn half-write must never be committed under a watermark.
    fn write_change(&self, change: &SourceChange) -> anyhow::Result<()> {
        match change {
            SourceChange::Add(r) => {
                let (sql, params) = self.insert_sql(r);
                self.db.prepare_cached(&sql)?.execute(rusqlite::params_from_iter(params))?;
            }
            SourceChange::Remove(r) => {
                let params: Vec<SqlVal> =
                    self.primary_key.iter().map(|k| self.param(k, r.get(k).cloned().unwrap_or(Value::Null))).collect();
                let sql = format!("DELETE FROM \"{}\" WHERE {}", self.table, self.pk_where());
                self.db.prepare_cached(&sql)?.execute(rusqlite::params_from_iter(params))?;
            }
            SourceChange::Edit { row, old_row } => {
                let params: Vec<SqlVal> =
                    self.primary_key.iter().map(|k| self.param(k, old_row.get(k).cloned().unwrap_or(Value::Null))).collect();
                let sql = format!("DELETE FROM \"{}\" WHERE {}", self.table, self.pk_where());
                self.db.prepare_cached(&sql)?.execute(rusqlite::params_from_iter(params))?;
                let (sql, p) = self.insert_sql(row);
                self.db.prepare_cached(&sql)?.execute(rusqlite::params_from_iter(p))?;
            }
        }
        Ok(())
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
        let mut stmt = self.db.prepare_cached(&sql).unwrap();
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

    /// Fetch with FULL SQL pushdown — constraint (`WHERE =`), cursor (a
    /// sargable lexicographic bound, null-aware to match `compare_values`'
    /// nulls-first order), `ORDER BY` (directions flipped for reverse), and
    /// `LIMIT` all execute inside SQLite against a lazily-created covering
    /// index. No table scan, no in-memory filter/sort. The in-flight overlay row
    /// is spliced into the (already-sorted) result; with a `LIMIT`, one extra
    /// row is fetched while an overlay is active so a spliced-out row can be
    /// backfilled.
    fn fetch_conn(&self, req: &FetchRequest, conn_idx: usize) -> Vec<Node> {
        let conn = self.connections[conn_idx]
            .as_ref()
            .expect("SQLite source connection slot was disconnected");
        let overlay_active = matches!(&self.overlay, Some((epoch, _)) if conn.last_pushed_epoch >= *epoch);

        // Effective per-column directions (reverse flips each).
        let eff: Vec<(String, Direction)> = conn
            .sort
            .iter()
            .map(|(f, d)| {
                let dir = match (d, req.reverse) {
                    (Direction::Asc, false) | (Direction::Desc, true) => Direction::Asc,
                    _ => Direction::Desc,
                };
                (f.clone(), dir)
            })
            .collect();

        self.ensure_index(req.constraint.as_ref(), &conn.sort);

        // SELECT list + WHERE.
        let cols = self.col_list();
        let select = cols.iter().map(|c| ident(c)).collect::<Vec<_>>().join(", ");
        let mut wheres: Vec<String> = Vec::new();
        let mut params: Vec<SqlVal> = Vec::new();
        if let Some(c) = &req.constraint {
            for (k, v) in c.iter() {
                if matches!(v, Value::Null) {
                    // Join semantics: a null key never matches anything
                    // (`values_equal(null, null) == false`, like SQL `= NULL`).
                    wheres.push("0".to_string());
                } else {
                    params.push(self.param(k, v.clone()));
                    wheres.push(format!("{} = ?{}", ident(k), params.len()));
                }
            }
        }
        if let Some(start) = &req.start {
            let conv = |col: &str, v: Value| self.param(col, v);
            wheres.push(start_bound_sql(&eff, &start.row, start.basis, &mut params, &conv));
        }
        // WHERE pushdown: filter inside SQLite instead of materializing the
        // whole table into RAM and filtering above (audit Tier 2). Superset-
        // safe by construction, and the pipeline Filter re-checks every row.
        if let Some(cond) = &conn.filter {
            if let Some(sql) = pushed_where_sql(cond, &mut params, &|col, v| self.param(col, v)) {
                wheres.push(sql);
            }
        }
        let order = eff
            .iter()
            .map(|(c, d)| format!("{} {}", ident(c), if matches!(d, Direction::Asc) { "ASC" } else { "DESC" }))
            .collect::<Vec<_>>()
            .join(", ");
        let mut sql = format!("SELECT {select} FROM {}", ident(&self.table));
        if !wheres.is_empty() {
            sql.push_str(&format!(" WHERE {}", wheres.join(" AND ")));
        }
        sql.push_str(&format!(" ORDER BY {order}"));
        // With an active overlay a Remove/Edit can drop one fetched row, so read
        // one extra to backfill the window.
        let fetch_limit = req.limit.map(|k| if overlay_active { k + 1 } else { k });
        if let Some(k) = fetch_limit {
            sql.push_str(&format!(" LIMIT {k}"));
        }

        let col_types: Vec<(String, ColumnType)> = cols.iter().map(|c| (c.clone(), self.columns[c])).collect();
        let mut stmt = self.db.prepare_cached(&sql).unwrap();
        let mut rows: Vec<Row> = stmt
            .query_map(rusqlite::params_from_iter(params), |r| {
                let mut row = Row::new();
                for (i, (name, ty)) in col_types.iter().enumerate() {
                    row.insert(name.as_str(), read_value(r, i, *ty));
                }
                Ok(row)
            })
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();

        // Overlay splice: present the post-change view without re-sorting. The
        // spliced-in row must satisfy the constraint AND the start bound; rows
        // from SQL already do.
        if overlay_active {
            let (_, change) = self.overlay.as_ref().unwrap();
            let ord2: oql::value::Ordering2 = eff
                .iter()
                .map(|(f, d)| {
                    (f.clone(), match d {
                        Direction::Asc => oql::value::Direction::Asc,
                        Direction::Desc => oql::value::Direction::Desc,
                    })
                })
                .collect();
            let cmp = Comparator::new(ord2, false);
            let admits = |r: &Row| {
                let c_ok = req.constraint.as_ref().is_none_or(|c| constraint_matches_row(c, r));
                let s_ok = req.start.as_ref().is_none_or(|start| {
                    let c = cmp.compare(r, &start.row);
                    match start.basis {
                        Basis::At => c != CmpOrdering::Less,
                        Basis::After => c == CmpOrdering::Greater,
                    }
                });
                c_ok && s_ok
            };
            match change {
                SourceChange::Add(r) => {
                    if admits(r) {
                        insert_sorted_row(&mut rows, r.clone(), &cmp);
                    }
                }
                SourceChange::Remove(r) => rows.retain(|x| !self.pk_eq(x, r)),
                SourceChange::Edit { row, old_row } => {
                    rows.retain(|x| !self.pk_eq(x, old_row));
                    if admits(row) {
                        insert_sorted_row(&mut rows, row.clone(), &cmp);
                    }
                }
            }
        }
        if let Some(k) = req.limit {
            rows.truncate(k);
        }

        rows.into_iter().map(Node::new).collect()
    }

    /// Create (once) a covering index for this fetch shape: the equality
    /// constraint columns first, then the sort columns with their directions —
    /// so `WHERE k = ? ORDER BY s LIMIT n` is an index seek, not a scan+sort.
    fn ensure_index(&self, constraint: Option<&oql::ivm::constraint::Constraint>, sort: &AstOrdering) {
        let ccols: Vec<&String> = constraint.map(|c| c.keys().collect()).unwrap_or_default();
        // The PK index already serves unconstrained PK-ordered fetches.
        if ccols.is_empty() {
            let pk_prefix = sort.len() <= self.primary_key.len()
                && sort
                    .iter()
                    .zip(self.primary_key.iter())
                    .all(|((f, d), k)| f == k && matches!(d, Direction::Asc));
            if pk_prefix {
                return;
            }
        }
        let mut name = format!("orbit_idx_{}", self.table);
        let mut cols_sql: Vec<String> = Vec::new();
        for c in &ccols {
            name.push_str(&format!("_{c}"));
            cols_sql.push(ident(c));
        }
        for (f, d) in sort {
            let dir = if matches!(d, Direction::Asc) { "ASC" } else { "DESC" };
            name.push_str(&format!("_{f}{}", if matches!(d, Direction::Asc) { "a" } else { "d" }));
            cols_sql.push(format!("{} {dir}", ident(f)));
        }
        let name: String = name.chars().map(|ch| if ch.is_alphanumeric() || ch == '_' { ch } else { '_' }).collect();
        if self.created_indexes.borrow().contains(&name) {
            return;
        }
        let sql = format!(
            "CREATE INDEX IF NOT EXISTS {} ON {} ({})",
            ident(&name),
            ident(&self.table),
            cols_sql.join(", ")
        );
        self.db.execute_batch(&sql).expect("create index");
        self.created_indexes.borrow_mut().insert(name);
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
    connect_filtered(src, sort, None)
}

/// [`connect`] with a WHERE condition offered for fetch pushdown.
pub fn connect_filtered(
    src: &Rc<RefCell<SqliteSource>>,
    sort: AstOrdering,
    cond: Option<&oql::ast::Condition>,
) -> Rc<RefCell<SqliteConnection>> {
    let conn_idx;
    {
        let mut s = src.borrow_mut();
        let schema = s.build_schema(&sort);
        let filter = cond.and_then(|c| pushable_subset(c, &s.columns));
        let connection = Conn {
            sort,
            filter,
            output: None,
            active_pos: s.active_connections.len(),
            last_pushed_epoch: 0,
            schema,
        };
        conn_idx = match s.connections.iter().position(Option::is_none) {
            Some(idx) => {
                s.connections[idx] = Some(connection);
                idx
            }
            None => {
                s.connections.push(Some(connection));
                s.connections.len() - 1
            }
        };
        s.active_connections.push(conn_idx);
    }
    Rc::new(RefCell::new(SqliteConnection { source: Rc::clone(src), conn_idx }))
}

/// Apply a change and propagate it to connected outputs (overlay then commit).
/// A storage error propagates AFTER the overlay is cleared (pipelines saw a
/// change that won't commit — the caller must roll back the surrounding
/// replication transaction and halt, never commit the watermark over it).
pub fn source_push(src: &Rc<RefCell<SqliteSource>>, change: SourceChange) -> anyhow::Result<()> {
    let epoch = {
        let mut s = src.borrow_mut();
        s.push_epoch += 1;
        let e = s.push_epoch;
        s.overlay = Some((e, change.clone()));
        e
    };
    let n = src.borrow().active_connections.len();
    // Convert ONCE and share: a `Node`'s row is an `Rc<Row>`, so cloning the
    // template per connection bumps a refcount instead of deep-copying the
    // row (mirrors `MemorySource::source_push`'s OverlayChange sharing).
    // Deep-copying per connection made every replicated row cost
    // O(connected clients) heap while it sat in their Catch buffers awaiting
    // the next flush — a 130 MB replication burst × 6 clients was ~800 MB.
    let template = base_change(&change);
    for active_pos in 0..n {
        let output = {
            let mut s = src.borrow_mut();
            let conn_idx = s.active_connections[active_pos];
            let conn = s.connections[conn_idx].as_mut().unwrap();
            conn.last_pushed_epoch = epoch;
            conn.output.as_ref().and_then(std::rc::Weak::upgrade)
        };
        if let Some(output) = output {
            deliver(&output, template.clone());
        }
    }
    let mut s = src.borrow_mut();
    s.overlay = None;
    s.write_change(&change)
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
        let source = self.source.borrow();
        Rc::clone(
            &source.connections[self.conn_idx]
                .as_ref()
                .expect("SQLite source connection slot was disconnected")
                .schema,
        )
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
        self.source.borrow().connections[self.conn_idx]
            .as_ref()
            .and_then(|conn| conn.output.as_ref())
            .and_then(std::rc::Weak::upgrade)
    }
    fn set_output(&mut self, out: Link) {
        self.source.borrow_mut().connections[self.conn_idx]
            .as_mut()
            .expect("SQLite source connection slot was disconnected")
            .output = Some(Rc::downgrade(&out));
    }
}

impl Drop for SqliteConnection {
    fn drop(&mut self) {
        self.source.borrow_mut().disconnect(self.conn_idx);
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
    fn connect_filtered(
        &self,
        table: &str,
        sort: AstOrdering,
        cond: Option<&oql::ast::Condition>,
    ) -> Option<OpHandle> {
        self.sources.get(table).map(|s| OpHandle::new(connect_filtered(s, sort, cond)))
    }
}

/// A SQLite-backed replica: a [`ReplicaBackend`](crate::replica::ReplicaBackend)
/// holding a [`SqliteSource`] per table — **all tables in ONE database** (a
/// single file for [`durable`](Self::durable)), so an upstream transaction
/// spanning tables commits atomically and the replication watermark lives in
/// the same database as the rows it describes.
pub struct SqliteReplica {
    sources: std::collections::HashMap<String, Rc<RefCell<SqliteSource>>>,
    columns: std::collections::HashMap<String, Vec<(String, ColumnType)>>,
    conn: Rc<Connection>,
    /// The database file path (`None` for in-memory replicas).
    path: Option<std::path::PathBuf>,
    /// Upstream-name → source-key aliases created by upstream
    /// `ALTER TABLE … RENAME TO` (see the in-memory replica's field).
    aliases: RefCell<std::collections::HashMap<String, String>>,
}

/// Tuning knobs for a [`SqliteReplica`]'s connection. Defaults leave SQLite's
/// own defaults in place (~2 MB page cache, no mmap).
#[derive(Clone, Copy, Debug, Default)]
pub struct SqliteReplicaOpts {
    /// Page-cache size in MiB (`PRAGMA cache_size = -(mb * 1024)`, the
    /// negative-KiB form). This is THE steady-state memory knob for a
    /// SQLite-backed node: resident base-row memory becomes O(page cache),
    /// not O(dataset).
    pub cache_mb: Option<u64>,
    /// Memory-map budget in MiB (`PRAGMA mmap_size`). Mapped pages are
    /// page-cache-backed file mappings the OS can reclaim under pressure —
    /// they don't count against a cgroup the way anonymous memory does.
    pub mmap_mb: Option<u64>,
}

impl SqliteReplica {
    /// An in-memory replica (all tables in one in-memory database).
    pub fn in_memory() -> Self {
        Self::in_memory_with(&SqliteReplicaOpts::default())
    }

    /// [`in_memory`](Self::in_memory) with connection tuning.
    pub fn in_memory_with(opts: &SqliteReplicaOpts) -> Self {
        Self::with_connection(Connection::open_in_memory().expect("open sqlite"), None, opts)
    }

    /// A durable replica: one database file at `dir/replica.db`.
    pub fn durable(dir: impl Into<std::path::PathBuf>) -> Self {
        Self::durable_with(dir, &SqliteReplicaOpts::default())
    }

    /// [`durable`](Self::durable) with connection tuning.
    pub fn durable_with(dir: impl Into<std::path::PathBuf>, opts: &SqliteReplicaOpts) -> Self {
        let dir = dir.into();
        std::fs::create_dir_all(&dir).ok();
        let path = dir.join("replica.db");
        Self::with_connection(
            Connection::open(&path).expect("open sqlite file"),
            Some(path),
            opts,
        )
    }

    /// The database file path (`None` for in-memory replicas). Snapshot
    /// backups ([`backup_to`](Self::backup_to)) need this.
    pub fn db_path(&self) -> Option<&std::path::Path> {
        self.path.as_deref()
    }

    /// Set `PRAGMA wal_autocheckpoint` (0 disables). Incremental WAL-shipping
    /// backups take manual control of checkpoints: the main db file must stay
    /// byte-stable between generation rolls so only the WAL carries changes.
    pub fn set_wal_autocheckpoint(&self, frames: i64) -> anyhow::Result<()> {
        self.conn.pragma_update(None, "wal_autocheckpoint", frames)?;
        Ok(())
    }

    /// Run `PRAGMA wal_checkpoint(TRUNCATE)`: fold the whole WAL into the main
    /// file and reset the WAL to empty. Errors if the checkpoint could not
    /// complete (e.g. a concurrent reader pinned the WAL).
    pub fn checkpoint_truncate(&self) -> anyhow::Result<()> {
        let (busy, _log, _ckpt): (i64, i64, i64) = self.conn.query_row(
            "PRAGMA wal_checkpoint(TRUNCATE)",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;
        anyhow::ensure!(busy == 0, "wal_checkpoint(TRUNCATE) blocked (busy)");
        Ok(())
    }

    fn with_connection(
        conn: Connection,
        path: Option<std::path::PathBuf>,
        opts: &SqliteReplicaOpts,
    ) -> Self {
        let _ = conn.pragma_update(None, "journal_mode", "WAL");
        let _ = conn.pragma_update(None, "synchronous", "NORMAL");
        if let Some(mb) = opts.cache_mb {
            // Negative value = KiB (page-count form depends on page size).
            let _ = conn.pragma_update(None, "cache_size", -((mb as i64) * 1024));
        }
        if let Some(mb) = opts.mmap_mb {
            let _ = conn.pragma_update(None, "mmap_size", (mb as i64) << 20);
        }
        conn.set_prepared_statement_cache_capacity(128);
        // The replication watermark: the commit LSN of the last upstream
        // transaction fully applied to THIS database plus the change-stream
        // position of that commit, written inside that same transaction (see
        // `commit_txn`) so they can never disagree with the rows. `pos` makes a
        // copied snapshot file self-describing (the restore point travels WITH
        // the data) and lets a durable view-syncer resume by delta.
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS orbit_replication_state (
                 id  INTEGER PRIMARY KEY CHECK (id = 1),
                 lsn INTEGER NOT NULL,
                 pos INTEGER NOT NULL DEFAULT 0
             )",
        )
        .expect("create replication state table");
        // Migrate pre-`pos` files in place (ignore "duplicate column name").
        let _ = conn.execute_batch(
            "ALTER TABLE orbit_replication_state ADD COLUMN pos INTEGER NOT NULL DEFAULT 0",
        );
        // Which tables have been fully initial-synced. Consulted on a
        // watermark resume so tables newly added to the config get backfilled
        // instead of silently starting empty (audit Tier 0.5).
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS orbit_synced_tables (name TEXT PRIMARY KEY)",
        )
        .expect("create synced-tables registry");
        // Migration: a pre-registry file that already carries a watermark was
        // fully synced by an older version — count its physical tables as
        // synced so the upgrade doesn't trigger a full re-seed.
        let has_watermark: bool = conn
            .query_row(
                "SELECT lsn > 0 OR pos > 0 FROM orbit_replication_state WHERE id = 1",
                [],
                |r| r.get(0),
            )
            .unwrap_or(false);
        let registry_empty: bool = conn
            .query_row("SELECT count(*) = 0 FROM orbit_synced_tables", [], |r| r.get(0))
            .unwrap_or(true);
        if has_watermark && registry_empty {
            let _ = conn.execute_batch(
                "INSERT OR IGNORE INTO orbit_synced_tables (name)
                 SELECT name FROM sqlite_master
                 WHERE type = 'table' AND name NOT LIKE 'orbit_%' AND name NOT LIKE 'sqlite_%'",
            );
        }
        SqliteReplica {
            sources: std::collections::HashMap::new(),
            columns: std::collections::HashMap::new(),
            conn: Rc::new(conn),
            path,
            aliases: RefCell::new(std::collections::HashMap::new()),
        }
    }

    /// Take a consistent point-in-time copy of the database at `src` into the
    /// file at `dest` using `VACUUM INTO` on a fresh connection. Under WAL
    /// this does not block the writer; the output is a single compacted file
    /// (no `-wal`/`-shm` sidecars) carrying `orbit_replication_state`
    /// (`lsn`, `pos`) — i.e. a self-describing snapshot. Runs on a blocking
    /// thread so the serving `LocalSet` isn't stalled.
    ///
    /// An associated fn (not `&self`) because `SqliteReplica` is `!Send`.
    pub async fn backup_to(src: std::path::PathBuf, dest: std::path::PathBuf) -> anyhow::Result<()> {
        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            let _ = std::fs::remove_file(&dest); // VACUUM INTO refuses to overwrite
            let conn = Connection::open_with_flags(
                &src,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            )?;
            conn.busy_timeout(std::time::Duration::from_secs(5))?;
            let dest_str = dest.to_str().ok_or_else(|| anyhow::anyhow!("non-utf8 dest path"))?;
            conn.execute("VACUUM INTO ?1", [dest_str])?;
            Ok(())
        })
        .await
        .map_err(|e| anyhow::anyhow!("backup task panicked: {e}"))?
    }

    pub fn add_table(
        &mut self,
        name: &str,
        columns: Vec<(String, ColumnType)>,
        primary_key: Vec<String>,
    ) -> Rc<RefCell<SqliteSource>> {
        let col_map: BTreeMap<String, ColumnType> = columns.iter().cloned().collect();
        let src = SqliteSource::with_shared(Rc::clone(&self.conn), name, col_map, primary_key);
        self.columns.insert(name.to_string(), columns);
        self.sources.insert(name.to_string(), Rc::clone(&src));
        src
    }

    pub fn source(&self, name: &str) -> Option<Rc<RefCell<SqliteSource>>> {
        self.sources.get(name).cloned()
    }

    /// Resolve an upstream table name to its source key (following a RENAME
    /// alias when the name isn't a source itself).
    fn resolve_key(&self, table: &str) -> String {
        if self.sources.contains_key(table) {
            return table.to_string();
        }
        self.aliases.borrow().get(table).cloned().unwrap_or_else(|| table.to_string())
    }
}

impl SourceProvider for SqliteReplica {
    fn primary_key(&self, table: &str) -> Option<Vec<String>> {
        self.sources.get(table).map(|s| s.borrow().primary_key().to_vec())
    }
    fn connect(&self, table: &str, sort: AstOrdering) -> Option<OpHandle> {
        self.sources.get(table).map(|s| OpHandle::new(connect(s, sort)))
    }
    fn connect_filtered(
        &self,
        table: &str,
        sort: AstOrdering,
        cond: Option<&oql::ast::Condition>,
    ) -> Option<OpHandle> {
        self.sources.get(table).map(|s| OpHandle::new(connect_filtered(s, sort, cond)))
    }
}

impl crate::replica::ReplicaBackend for SqliteReplica {
    fn apply(&self, event: crate::LogicalEvent) -> anyhow::Result<()> {
        use crate::LogicalEvent as E;
        match event {
            E::Insert { table, row } => {
                if let Some(src) = self.sources.get(&self.resolve_key(&table)) {
                    let existing = src.borrow().lookup(&row);
                    match existing {
                        None => source_push(src, SourceChange::Add(row))?,
                        Some(old) => source_push(src, SourceChange::Edit { row, old_row: old })?,
                    }
                }
            }
            E::Delete { table, old_row } => {
                if let Some(src) = self.sources.get(&self.resolve_key(&table)) {
                    let stored = src.borrow().lookup(&old_row);
                    if let Some(stored) = stored {
                        source_push(src, SourceChange::Remove(stored))?;
                    }
                }
            }
            E::Update { table, mut row, old_row } => {
                if let Some(src) = self.sources.get(&self.resolve_key(&table)) {
                    let key = old_row.as_ref().unwrap_or(&row);
                    let existing = src.borrow().lookup(key);
                    match existing {
                        Some(old) => {
                            // Unchanged-TOAST merge (apply side): columns the
                            // stream omitted ('u') are absent from `row`; fill
                            // them from the stored row. Without this the Edit's
                            // DELETE + re-INSERT binds them as explicit NULL.
                            row.merge_missing_from(&old);
                            source_push(src, SourceChange::Edit { row, old_row: old })?
                        }
                        None => source_push(src, SourceChange::Add(row))?,
                    }
                }
            }
            E::Truncate { tables } => {
                for table in tables {
                    if let Some(src) = self.sources.get(&self.resolve_key(&table)) {
                        // Remove every row THROUGH the pipelines so subscribed
                        // queries and client caches converge (not just storage).
                        let rows = src.borrow().all_rows();
                        for row in rows {
                            source_push(src, SourceChange::Remove(row))?;
                        }
                    }
                }
            }
            E::Relation { table, columns, renamed_from } => {
                if let Some(from) = renamed_from {
                    let key = self.resolve_key(&from);
                    if !self.sources.contains_key(&table) && self.sources.contains_key(&key) {
                        eprintln!(
                            "upstream renamed table {from} -> {table}; aliasing so clients subscribed to {key} keep receiving changes"
                        );
                        self.aliases.borrow_mut().insert(table.clone(), key);
                    }
                }
                // Mirror the in-memory replica: reconcile the physical table to
                // the upstream column set (DDL). `self.columns` stays at the
                // boot-time declaration — exact parity with the in-memory
                // backend, whose `Replica.columns` also goes stale; it is only
                // used for the initial-sync SELECT.
                if let Some(src) = self.sources.get(&self.resolve_key(&table)) {
                    src.borrow_mut().reconcile_columns(&columns);
                }
            }
            E::Begin | E::Commit | E::Other => {}
        }
        Ok(())
    }
    fn seed(&self, table: &str, row: Row) -> anyhow::Result<()> {
        if let Some(src) = self.sources.get(table) {
            src.borrow().insert_initial(&row)?;
        }
        Ok(())
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

    fn begin_txn(&self) -> anyhow::Result<()> {
        // One SQLite transaction per upstream transaction: a crash mid-apply
        // rolls back on reopen instead of persisting a torn half-transaction.
        // A failed BEGIN must propagate: applying in autocommit would tear.
        self.conn.execute_batch("BEGIN IMMEDIATE")?;
        Ok(())
    }

    fn commit_txn(&self, lsn: u64, pos: u64) -> anyhow::Result<()> {
        // The watermark commits atomically WITH the rows it describes. A
        // failure here must propagate — acking WAL past an uncommitted
        // watermark would lose the transaction on restart.
        self.conn
            .prepare_cached(
                "INSERT INTO orbit_replication_state (id, lsn, pos) VALUES (1, ?1, ?2)
                 ON CONFLICT (id) DO UPDATE SET lsn = excluded.lsn, pos = excluded.pos",
            )?
            .execute([lsn as i64, pos as i64])?;
        self.conn.execute_batch("COMMIT")?;
        Ok(())
    }

    fn rollback_txn(&self) {
        let _ = self.conn.execute_batch("ROLLBACK");
    }

    fn clear_table(&self, table: &str) -> anyhow::Result<()> {
        if self.sources.contains_key(table) {
            self.conn
                .execute_batch(&format!("DELETE FROM {}", ident(table)))?;
        }
        Ok(())
    }

    fn synced_tables(&self) -> Option<std::collections::HashSet<String>> {
        let mut stmt = self.conn.prepare_cached("SELECT name FROM orbit_synced_tables").ok()?;
        let set = stmt
            .query_map([], |r| r.get::<_, String>(0))
            .ok()?
            .filter_map(|r| r.ok())
            .collect();
        Some(set)
    }

    fn mark_synced(&self, table: &str) -> anyhow::Result<()> {
        self.conn
            .prepare_cached("INSERT OR IGNORE INTO orbit_synced_tables (name) VALUES (?1)")?
            .execute([table])?;
        Ok(())
    }

    fn resume_watermark(&self) -> Option<u64> {
        self.conn
            .query_row("SELECT lsn FROM orbit_replication_state WHERE id = 1", [], |r| {
                r.get::<_, i64>(0)
            })
            .ok()
            .filter(|l| *l > 0)
            .map(|l| l as u64)
    }

    fn resume_pos(&self) -> Option<u64> {
        self.conn
            .query_row("SELECT pos FROM orbit_replication_state WHERE id = 1", [], |r| {
                r.get::<_, i64>(0)
            })
            .ok()
            .filter(|p| *p > 0)
            .map(|p| p as u64)
    }

    fn metrics_sample(&self) -> crate::replica::ReplicaSample {
        let mut s = crate::replica::ReplicaSample::default();
        // File size (incl. WAL) — the disk footprint. Page stats would need a
        // query per sample; the file is the operative number for capacity.
        if let Some(path) = self.db_path() {
            for suffix in ["", "-wal"] {
                let mut p = path.to_path_buf().into_os_string();
                p.push(suffix);
                if let Ok(md) = std::fs::metadata(std::path::PathBuf::from(p)) {
                    s.file_bytes += md.len();
                }
            }
        } else if let Ok((page_count, page_size)) =
            self.conn.query_row("SELECT * FROM pragma_page_count(), pragma_page_size()", [], |r| {
                Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?))
            })
        {
            s.file_bytes = (page_count * page_size) as u64;
        }
        s
    }

    fn start_fresh(&self) {
        // Atomically drop everything stale before a fresh initial sync: the
        // sync only upserts, so rows deleted upstream while this replica was
        // offline would otherwise survive as phantoms.
        let _ = self.conn.execute_batch("BEGIN IMMEDIATE");
        for src in self.sources.values() {
            let table = src.borrow().table_name().to_string();
            let sql = format!("DELETE FROM \"{}\"", table.replace('"', "\"\""));
            let _ = self.conn.execute_batch(&sql);
        }
        let _ = self.conn.execute_batch("DELETE FROM orbit_replication_state");
        let _ = self.conn.execute_batch("DELETE FROM orbit_synced_tables");
        let _ = self.conn.execute_batch("COMMIT");
    }
}

/// Reduce a WHERE condition to the subset that can be pushed into SQLite
/// SQL **without ever excluding a row the OQL predicate would admit** (the
/// superset rule — the pipeline Filter re-applies the full predicate, so SQL
/// may only ever admit MORE):
/// * simple `col <op> literal` for `=`, `!=`, `<`, `<=`, `>`, `>=`, `IN`,
///   `NOT IN`, and `IS` — on columns whose declared type is not `Json`
///   (JSON's TEXT encoding differs from literal bindings) and with non-null
///   literals (OQL: any non-`IS` op vs NULL is constant false; SQL agrees,
///   but we simply skip). `IS NOT` is skipped: OQL admits absent-column rows
///   that SQL's `IS NOT NULL` would exclude.
/// * `LIKE`/`ILIKE` are skipped (SQLite LIKE is ASCII-case-insensitive).
/// * `AND`: any pushable subset of its children (dropping a child widens).
/// * `OR`: only when EVERY child is fully pushable (dropping one would
///   narrow the union — an exclusion violation).
/// * `EXISTS` subqueries: never pushed.
fn pushable_subset(
    cond: &oql::ast::Condition,
    columns: &BTreeMap<String, ColumnType>,
) -> Option<oql::ast::Condition> {
    use oql::ast::{Condition as C, LiteralValue, SimpleOperator as Op, ValuePosition as VP};
    match cond {
        C::Simple { op, left, right } => {
            let VP::Column { name } = left else { return None };
            match columns.get(name) {
                None | Some(ColumnType::Json) => return None,
                _ => {}
            }
            let VP::Literal { value } = right else { return None };
            match op {
                Op::Like | Op::NotLike | Op::ILike | Op::NotILike | Op::IsNot => None,
                Op::Is => match value {
                    // `IS NULL` is safe: SQL admits absent-column rows OQL
                    // rejects (superset), never the reverse.
                    _ => Some(cond.clone()),
                },
                _ => match value {
                    LiteralValue::Null => None, // OQL: constant false; skip
                    LiteralValue::Array(_) if matches!(op, Op::In | Op::NotIn) => {
                        Some(cond.clone())
                    }
                    LiteralValue::Array(_) => None,
                    _ => Some(cond.clone()),
                },
            }
        }
        C::And { conditions } => {
            let kept: Vec<oql::ast::Condition> =
                conditions.iter().filter_map(|c| pushable_subset(c, columns)).collect();
            if kept.is_empty() {
                None
            } else {
                Some(C::And { conditions: kept })
            }
        }
        C::Or { conditions } => {
            let kept: Vec<oql::ast::Condition> =
                conditions.iter().filter_map(|c| pushable_subset(c, columns)).collect();
            if kept.len() == conditions.len() {
                Some(C::Or { conditions: kept })
            } else {
                None
            }
        }
        C::CorrelatedSubquery { .. } => None,
    }
}

fn literal_to_value(v: &oql::ast::LiteralValue) -> Value {
    use oql::ast::LiteralValue as LV;
    match v {
        LV::Null => Value::Null,
        LV::Bool(b) => Value::Bool(*b),
        LV::Number(n) => Value::Number(*n),
        LV::String(s) => Value::String(s.clone()),
        LV::Array(_) => Value::Null, // handled by the IN path
    }
}

fn literal_prim_to_value(v: &oql::ast::LiteralPrimitive) -> Value {
    use oql::ast::LiteralPrimitive as LP;
    match v {
        LP::Bool(b) => Value::Bool(*b),
        LP::Number(n) => Value::Number(*n),
        LP::String(s) => Value::String(s.clone()),
    }
}

/// Render a pushable condition (from [`pushable_subset`]) as SQL, appending
/// its bindings to `params`.
fn pushed_where_sql(
    cond: &oql::ast::Condition,
    params: &mut Vec<SqlVal>,
    conv: &dyn Fn(&str, Value) -> SqlVal,
) -> Option<String> {
    use oql::ast::{Condition as C, LiteralValue, SimpleOperator as Op, ValuePosition as VP};
    match cond {
        C::Simple { op, left, right } => {
            let VP::Column { name } = left else { return None };
            let VP::Literal { value } = right else { return None };
            let col = ident(name);
            match op {
                Op::In | Op::NotIn => {
                    let LiteralValue::Array(items) = value else { return None };
                    if items.is_empty() {
                        // IN () is invalid SQL; OQL: IN [] = false, NOT IN [] =
                        // true for non-null lhs. Render equivalents.
                        return Some(if matches!(op, Op::In) {
                            "0".to_string()
                        } else {
                            format!("{col} IS NOT NULL")
                        });
                    }
                    let mut holes = Vec::with_capacity(items.len());
                    for item in items {
                        params.push(conv(name, literal_prim_to_value(item)));
                        holes.push(format!("?{}", params.len()));
                    }
                    let not = if matches!(op, Op::NotIn) { " NOT" } else { "" };
                    Some(format!("{col}{not} IN ({})", holes.join(", ")))
                }
                Op::Is => {
                    if matches!(value, LiteralValue::Null) {
                        Some(format!("{col} IS NULL"))
                    } else {
                        params.push(conv(name, literal_to_value(value)));
                        Some(format!("{col} IS ?{}", params.len()))
                    }
                }
                Op::Eq | Op::Ne | Op::Lt | Op::Le | Op::Gt | Op::Ge => {
                    let sql_op = match op {
                        Op::Eq => "=",
                        Op::Ne => "<>",
                        Op::Lt => "<",
                        Op::Le => "<=",
                        Op::Gt => ">",
                        Op::Ge => ">=",
                        _ => unreachable!(),
                    };
                    params.push(conv(name, literal_to_value(value)));
                    Some(format!("{col} {sql_op} ?{}", params.len()))
                }
                _ => None,
            }
        }
        C::And { conditions } => {
            let parts: Vec<String> =
                conditions.iter().filter_map(|c| pushed_where_sql(c, params, conv)).collect();
            if parts.is_empty() {
                None
            } else {
                Some(format!("({})", parts.join(" AND ")))
            }
        }
        C::Or { conditions } => {
            let mut parts = Vec::with_capacity(conditions.len());
            for c in conditions {
                // All-or-nothing (guaranteed by pushable_subset).
                parts.push(pushed_where_sql(c, params, conv)?);
            }
            Some(format!("({})", parts.join(" OR ")))
        }
        C::CorrelatedSubquery { .. } => None,
    }
}

/// Quote an identifier for SQLite.
fn ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Insert `row` into `rows` (sorted ascending by `cmp`) at its upper bound —
/// matching where a stable sort would land a freshly appended row.
fn insert_sorted_row(rows: &mut Vec<Row>, row: Row, cmp: &Comparator) {
    let pos = rows.partition_point(|x| cmp.compare(x, &row) != CmpOrdering::Greater);
    rows.insert(pos, row);
}

/// The SQL for a `start` cursor: the sargable lexicographic bound over the
/// effective (reverse-applied) order — `(a > ?) OR (a = ? AND b > ?) OR …`,
/// plus the all-equal branch for `Basis::At`. Comparisons are **null-aware to
/// match `compare_values`** (null sorts first): ascending, "x > NULL" is
/// `x IS NOT NULL` and "x < v" admits NULL; equality uses `IS`. Mirrors Zero's
/// `gatherStartConstraints`.
fn start_bound_sql(
    eff: &[(String, Direction)],
    from: &Row,
    basis: Basis,
    params: &mut Vec<SqlVal>,
    conv: &dyn Fn(&str, Value) -> SqlVal,
) -> String {
    // eq / gt / lt with nulls-first semantics on the EFFECTIVE ascending order.
    let eq = |col: &str, v: &Value, params: &mut Vec<SqlVal>| -> String {
        if matches!(v, Value::Null) {
            format!("{} IS NULL", ident(col))
        } else {
            params.push(conv(col, v.clone()));
            format!("{} = ?{}", ident(col), params.len())
        }
    };
    let after = |col: &str, v: &Value, dir: &Direction, params: &mut Vec<SqlVal>| -> String {
        match dir {
            // Effective ascending: strictly greater. NULL is smallest, so
            // "> NULL" is IS NOT NULL, and "> v" naturally excludes NULL.
            Direction::Asc => {
                if matches!(v, Value::Null) {
                    format!("{} IS NOT NULL", ident(col))
                } else {
                    params.push(conv(col, v.clone()));
                    format!("{} > ?{}", ident(col), params.len())
                }
            }
            // Effective descending: strictly less. Nothing is less than NULL;
            // "< v" must ALSO admit NULL (null sorts last in effective desc).
            Direction::Desc => {
                if matches!(v, Value::Null) {
                    "0".to_string()
                } else {
                    params.push(conv(col, v.clone()));
                    format!("({} < ?{} OR {} IS NULL)", ident(col), params.len(), ident(col))
                }
            }
        }
    };

    let mut branches: Vec<String> = Vec::new();
    for i in 0..eff.len() {
        let mut group: Vec<String> = Vec::new();
        for (j, (col, _)) in eff.iter().enumerate().take(i) {
            let v = from.get(col).cloned().unwrap_or(Value::Null);
            let _ = j;
            group.push(eq(col, &v, params));
        }
        let (col, dir) = &eff[i];
        let v = from.get(col).cloned().unwrap_or(Value::Null);
        group.push(after(col, &v, dir, params));
        branches.push(format!("({})", group.join(" AND ")));
    }
    if matches!(basis, Basis::At) {
        let group: Vec<String> = eff
            .iter()
            .map(|(col, _)| {
                let v = from.get(col).cloned().unwrap_or(Value::Null);
                eq(col, &v, params)
            })
            .collect();
        branches.push(format!("({})", group.join(" AND ")));
    }
    format!("({})", branches.join(" OR "))
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
            // Exact 64-bit integers (beyond f64's 2^53) stay exact in SQLite.
            Value::Int(i) => ToSqlOutput::Owned(SqliteValue::Integer(*i)),
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
                // Normalizing constructor: representable → Number, else exact Int.
                ValueRef::Integer(i) => Value::int(i),
                ValueRef::Real(f) => Value::Number(f),
                ValueRef::Text(t) => std::str::from_utf8(t)
                    .ok()
                    .and_then(|s| s.parse::<f64>().ok())
                    .map(Value::Number)
                    .unwrap_or(Value::Null),
                _ => Value::Null,
            },
            ColumnType::Json => match v {
                ValueRef::Text(t) => std::str::from_utf8(t)
                    .ok()
                    .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok())
                    .map(Value::from_json)
                    .unwrap_or(Value::Null),
                ValueRef::Integer(i) => Value::int(i),
                ValueRef::Real(f) => Value::Number(f),
                _ => Value::Null,
            },
            ColumnType::String => v.as_str().map(|s| Value::String(s.to_string())).unwrap_or(Value::Null),
            ColumnType::Null => Value::Null,
        },
    }
}
