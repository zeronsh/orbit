//! A WebSocket sync server speaking Orbit's (= Zero's) wire protocol.
//!
//! Port of the relevant parts of `zero-cache`'s `workers/connection.ts` +
//! view-syncer streaming. Implements the reactive loop: a client connects,
//! subscribes via `changeDesiredQueries` (carrying query ASTs), receives the
//! initial result as a poke, and then receives **incremental pokes** whenever
//! the replica advances.
//!
//! Replica advances arrive on an `mpsc` channel (in a full deployment this is
//! fed by the replication stream; the channel keeps the `!Send` IVM state and
//! the `Send` change data cleanly separated). Drive this on a current-thread
//! runtime / `LocalSet` — the per-connection IVM pipeline is `!Send`.

use crate::cvr::PgCvrStore;
use crate::forward::{AuthContext, Forwarder};
use crate::mutators::MutatorRegistry;
use crate::queries::QueryRegistry;
use crate::replica::Replica;
use crate::view_sync::{
    changes_to_patches_dedup, initial_patches_dedup, resume_deletes, resume_patches_dedup,
    ClientView, RowRefs,
};
use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use oql::build_pipeline;
use oql::ivm::operator::Link;
use oql::ivm::{source_push, Catch, SourceChange};
use orbit_protocol::{
    ConnectedBody, CrudOp, Downstream, Mutation, PokeEndBody, PokePartBody, PokeStartBody,
    QueriesPatchOp, RowPatchOp, RowsPatch, Upstream,
};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

/// Shared per-client `lastMutationID` map. The replication apply loop records
/// each client's latest id from replicated [`orbit_client_mutations`](crate::pg::LMID_TABLE)
/// rows; each connection reads its own id to ack mutations **atomically with the
/// rows they produced** (same Postgres commit → same tick → same poke), so a
/// client's optimistic overlay is never dropped before its authoritative row
/// arrives — even while another client is writing.
pub type LmidMap = Rc<RefCell<HashMap<String, u64>>>;

/// If `ev` is an `orbit_client_mutations` Insert/Update, record its
/// `client_id -> last_mutation_id` into `lmids`. Returns true if it advanced.
pub fn capture_lmid(ev: &crate::LogicalEvent, lmids: &LmidMap) -> bool {
    use crate::LogicalEvent;
    use oql::value::Value;
    let row = match ev {
        LogicalEvent::Insert { table, row } if table == crate::pg::LMID_TABLE => row,
        LogicalEvent::Update { table, row, .. } if table == crate::pg::LMID_TABLE => row,
        _ => return false,
    };
    let cid = match row.get("client_id") {
        Some(Value::String(s)) => s.clone(),
        _ => return false,
    };
    let id = match row.get("last_mutation_id") {
        Some(Value::Number(n)) => *n as u64,
        // int8 column: values beyond 2^53 arrive as exact Ints.
        Some(Value::Int(i)) => *i as u64,
        _ => return false,
    };
    let mut map = lmids.borrow_mut();
    let slot = map.entry(cid).or_insert(0);
    if id > *slot {
        *slot = id;
        true
    } else {
        false
    }
}

impl oql::SourceProvider for Replica {
    fn get_source(&self, table: &str) -> Option<Rc<RefCell<oql::ivm::MemorySource>>> {
        self.source(table)
    }
}

/// A replica advance: apply `change` to table `table`, then poke subscribers.
pub type ReplicaChange = (String, SourceChange);

/// One materialized, subscribed query on a connection.
struct ActiveQuery {
    hash: String,
    catch: Rc<RefCell<Catch>>,
    schema: Rc<oql::ivm::Schema>,
    /// Tables this query reads (incl. subqueries) — a tick that touched none
    /// of them cannot change the result, so the drain is skipped.
    tables: std::collections::HashSet<String>,
}

impl Drop for ActiveQuery {
    fn drop(&mut self) {
        // Covers every teardown path — explicit Del, clean close, AND error
        // disconnects — so /statz active counts can't drift.
        crate::metrics::record_query_active(&self.hash, -1);
    }
}

/// A replication tick: the tables the committed transaction touched. An empty
/// list means "unknown" — every client conservatively drains. Carrying the
/// table set suppresses no-op wakeups: previously EVERY transaction woke EVERY
/// client task to drain all of its queries (audit Tier 2).
pub type Tick = std::sync::Arc<Vec<String>>;

/// Serve a single WebSocket connection until it closes.
///
/// `replica_changes` delivers upstream changes to apply; after each, every
/// active query is advanced and any resulting row patches are poked to the
/// client.
pub async fn serve_connection<S>(
    ws: WebSocketStream<S>,
    replica: &Replica,
    mut replica_changes: UnboundedReceiver<ReplicaChange>,
) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    serve_connection_with_mutators(ws, replica, &MutatorRegistry::new(), &mut replica_changes).await
}

/// Like [`serve_connection`] but with a custom-mutator registry.
pub async fn serve_connection_with_mutators<S>(
    ws: WebSocketStream<S>,
    replica: &Replica,
    mutators: &MutatorRegistry,
    replica_changes: &mut UnboundedReceiver<ReplicaChange>,
) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (mut tx, mut rx) = ws.split();

    send(&mut tx, &Downstream::Connected(ConnectedBody {
        wsid: "orbit-0".to_string(),
        timestamp: None,
    }))
    .await?;

    let mut active: Vec<ActiveQuery> = Vec::new();
    let mut version: u64 = 0;
    let mut base_cookie: Option<String> = None;
    let mut row_refs = RowRefs::new();
    let queries = QueryRegistry::new();
    let forwarder = Forwarder::new(crate::forward::ForwardConfig::default());
    let auth = AuthContext::default();

    loop {
        tokio::select! {
            msg = rx.next() => {
                let Some(msg) = msg else { break };
                let msg = msg?;
                match msg {
                    Message::Close(_) => break,
                    Message::Ping(p) => tx.send(Message::Pong(p)).await?,
                    Message::Text(text) if !text.trim().is_empty() => {
                        handle_upstream(
                            serde_json::from_str(&text)?,
                            replica,
                            mutators,
                            &queries,
                            &forwarder,
                            &auth,
                            &mut tx,
                            &mut active,
                            &mut version,
                            &mut base_cookie,
                            &mut row_refs,
                        )
                        .await?;
                    }
                    _ => {}
                }
            }
            Some((table, change)) = replica_changes.recv() => {
                if let Some(src) = replica.source(&table) {
                    source_push(&src, change);
                }
                flush_active(&active, &mut tx, &mut version, &mut base_cookie, &mut row_refs, HashMap::new()).await?;
            }
        }
    }

    drop(active);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn handle_upstream<S>(
    up: Upstream,
    replica: &Replica,
    mutators: &MutatorRegistry,
    queries: &QueryRegistry,
    forwarder: &Forwarder,
    auth: &AuthContext,
    tx: &mut SplitSink<WebSocketStream<S>, Message>,
    active: &mut Vec<ActiveQuery>,
    version: &mut u64,
    base_cookie: &mut Option<String>,
    row_refs: &mut RowRefs,
) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let patch = match up {
        Upstream::Ping => {
            send(tx, &Downstream::Pong).await?;
            return Ok(());
        }
        Upstream::Push(push) => {
            // Apply CRUD mutations to the replica and poke subscribers.
            //
            // This is the in-memory write-through path. In a full deployment the
            // mutagen writes to upstream Postgres and the change returns via
            // replication; here the same `SourceChange`s are applied directly so
            // a Postgres-less server is fully reactive over the socket.
            let mut lmids: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
            for m in &push.mutations {
                let ops: Vec<CrudOp> = match m {
                    Mutation::Crud { args, .. } => {
                        args.iter().flat_map(|a| a.ops.iter().cloned()).collect()
                    }
                    Mutation::Custom { name, args, .. } => match mutators.run(name, replica, args) {
                        Some(ops) => ops,
                        None => {
                            // Consumed (lmid still advances, same as the PG path)
                            // but LOUD — an unknown mutator is version skew, not a
                            // no-op.
                            eprintln!(
                                "unknown mutator {name:?} from {} (mutation {}) — consumed with no effect",
                                m.client_id(),
                                m.id()
                            );
                            Vec::new()
                        }
                    },
                };
                for op in &ops {
                    if let Some((table, change)) = crud_to_change(op, replica) {
                        if let Some(src) = replica.source(&table) {
                            source_push(&src, change);
                        }
                    }
                }
                let e = lmids.entry(m.client_id().to_string()).or_insert(0);
                *e = (*e).max(m.id());
            }
            // Ack the mutations together with the rows they just produced (one
            // atomic poke) — the writes above are applied synchronously here, so
            // the rows are already in the replica. Acking separately would drop
            // the client's optimistic overlay a beat before its row lands.
            flush_active(active, tx, version, base_cookie, row_refs, lmids).await?;
            return Ok(());
        }
        Upstream::InitConnection(b) => b.desired_queries_patch,
        Upstream::ChangeDesiredQueries(b) => b.desired_queries_patch,
    };

    // Legacy single-process path: no shared CVR resume.
    subscribe(patch, replica, queries, forwarder, auth, tx, active, version, base_cookie, row_refs, None).await
}

/// Advance every active query and poke any resulting row patches, riding the
/// given `lastMutationID` acks along with them (one atomic poke). Acking with the
/// rows a mutation produced is what keeps the client's optimistic overlay from
/// being dropped before its authoritative row is present.
async fn flush_active<S>(
    active: &[ActiveQuery],
    tx: &mut SplitSink<WebSocketStream<S>, Message>,
    version: &mut u64,
    base_cookie: &mut Option<String>,
    row_refs: &mut RowRefs,
    lmids: HashMap<String, u64>,
) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // Consistency: never drain Catch buffers while a pump is mid-way through
    // STREAMING an oversized transaction (its partial changes are already in
    // the buffers) — a lagged tick would otherwise flush a torn transaction
    // to the client. Free when no streamed txn is in progress.
    let apply_lock = replica_apply_lock();
    let consistent = apply_lock.read().await;
    let started = std::time::Instant::now();
    let mut patches = Vec::new();
    for q in active {
        let changes = q.catch.borrow_mut().take_changes();
        if changes.is_empty() {
            continue;
        }
        let before = patches.len();
        patches.extend(changes_to_patches_dedup(&changes, &q.schema, row_refs));
        crate::metrics::record_query_advance(&q.hash, (patches.len() - before) as u64);
    }
    drop(consistent);
    // Advancement circuit-breaker signal: a giant upstream transaction turns
    // this synchronous drain into a shard-wide stall. The drain itself can't
    // be aborted mid-walk (the pipelines already consumed the push), so the
    // actionable output is detection + attribution (Zero aborts + rehydrates;
    // Orbit's tick-drain happens after apply, so the equivalent recovery is a
    // reconnect — which the client does if the stall exceeds its timeouts).
    let took = started.elapsed();
    let slow = took > slow_hydration_budget();
    crate::metrics::observe_advance(took, slow);
    if slow {
        eprintln!(
            "slow advance: draining {} queries produced {} ops in {took:?} (budget {:?})",
            active.len(),
            patches.len(),
            slow_hydration_budget()
        );
    }
    if !patches.is_empty() || !lmids.is_empty() {
        let lmids = if lmids.is_empty() { None } else { Some(lmids) };
        poke(tx, version, base_cookie, None, lmids, patches).await?;
    }
    Ok(())
}

/// A client's currently-recorded `lastMutationID` (0 if none), for deduplicating
/// re-delivered mutations on the direct-write path.
async fn current_lmid(pg: &tokio_postgres::Client, client_id: &str) -> anyhow::Result<u64> {
    let t = crate::pg::LMID_TABLE;
    let sql = format!("SELECT last_mutation_id FROM {t} WHERE client_id = $1");
    Ok(pg
        .query_opt(&sql, &[&client_id])
        .await?
        .map(|r| r.get::<_, i64>(0) as u64)
        .unwrap_or(0))
}

/// Advance a client's `lastMutationID` in `orbit_client_mutations` (the same table
/// the app's PushProcessor writes), so it replicates back and the client's ack rides
/// with its rows. Used by the direct-write path (no app push endpoint configured).
async fn advance_lmid(pg: &tokio_postgres::Client, client_id: &str, id: u64) -> anyhow::Result<()> {
    let t = crate::pg::LMID_TABLE;
    let sql = format!(
        "INSERT INTO {t} (client_id, last_mutation_id) VALUES ($1, $2) \
         ON CONFLICT (client_id) DO UPDATE SET last_mutation_id = EXCLUDED.last_mutation_id \
         WHERE {t}.last_mutation_id < EXCLUDED.last_mutation_id",
    );
    pg.execute(&sql, &[&client_id, &(id as i64)]).await?;
    Ok(())
}

/// Convert a CRUD op to a `(table, SourceChange)`, resolving old rows for
/// update/delete/upsert from the replica.
fn crud_to_change(op: &CrudOp, replica: &Replica) -> Option<(String, SourceChange)> {
    match op {
        CrudOp::Insert { table_name, value, .. } => {
            Some((table_name.clone(), SourceChange::Add(value.clone())))
        }
        CrudOp::Upsert { table_name, value, .. } => {
            let src = replica.source(table_name)?;
            let existing = src.borrow().lookup(value);
            Some(match existing {
                Some(old) => (table_name.clone(), SourceChange::Edit { row: value.clone(), old_row: old }),
                None => (table_name.clone(), SourceChange::Add(value.clone())),
            })
        }
        CrudOp::Update { table_name, value, .. } => {
            let src = replica.source(table_name)?;
            let old = src.borrow().lookup(value)?;
            Some((table_name.clone(), SourceChange::Edit { row: value.clone(), old_row: old }))
        }
        CrudOp::Delete { table_name, value, .. } => {
            let src = replica.source(table_name)?;
            let old = src.borrow().lookup(value)?;
            Some((table_name.clone(), SourceChange::Remove(old)))
        }
    }
}

thread_local! {
    /// Per-node hydration admission gate (one serving thread per node, so a
    /// thread-local is node-scoped — same reasoning as `metrics::NODE_METRICS`).
    ///
    /// A fresh hydration materializes its full result (`fetch()` node tree +
    /// `RowsPatch`) before the chunked sends bound the WIRE memory — the
    /// PATCH is still O(result set), and on a SQLite replica those rows are
    /// fresh allocations, not shared `Rc`s. N concurrent full-history
    /// hydrations therefore hold N × result-set bytes at once; this gate
    /// bounds peak memory to `ORBIT_MAX_CONCURRENT_HYDRATIONS` × result
    /// (default 1). Queued clients hydrate as soon as a permit frees — they
    /// wait, they don't fail.
    static HYDRATION_GATE: Rc<tokio::sync::Semaphore> = Rc::new(tokio::sync::Semaphore::new(
        std::env::var("ORBIT_MAX_CONCURRENT_HYDRATIONS")
            .ok()
            .and_then(|v| v.trim().parse().ok())
            .filter(|n| *n > 0)
            .unwrap_or(1),
    ));
}

fn hydration_gate() -> Rc<tokio::sync::Semaphore> {
    HYDRATION_GATE.with(Rc::clone)
}

thread_local! {
    /// Node-wide replica-consistency lock (one serving thread per node).
    ///
    /// Readers: hydration materialize phases and tick flushes. Writer: a
    /// replication pump STREAMING an oversized transaction (audit Tier 1.2 —
    /// transactions larger than the buffer cap apply event-by-event across
    /// socket awaits instead of atomically at Commit). The write hold is what
    /// keeps a mid-transaction replica state invisible: no query and no flush
    /// can observe a state that existed in no Postgres snapshot. `Arc` (not
    /// `Rc`) so pumps can hold an owned write guard across await points.
    static REPLICA_APPLY_LOCK: std::sync::Arc<tokio::sync::RwLock<()>> =
        std::sync::Arc::new(tokio::sync::RwLock::new(()));
}

/// The node's replica-consistency lock (see [`REPLICA_APPLY_LOCK`]).
pub fn replica_apply_lock() -> std::sync::Arc<tokio::sync::RwLock<()>> {
    REPLICA_APPLY_LOCK.with(std::sync::Arc::clone)
}

/// Per-client serving limits (audit Tier 2 — Zero has targetClientRowCount /
/// maxRowsPerTable / perUserMutationLimit; Orbit previously had none).
pub struct ServeLimits {
    /// Max concurrently-active queries per connection
    /// (`ORBIT_MAX_QUERIES_PER_CLIENT`, default 200; 0 = unlimited).
    pub max_queries_per_client: usize,
    /// Max rows one query may materialize (`ORBIT_MAX_ROWS_PER_QUERY`,
    /// default 100_000; 0 = unlimited). Checked before any client state is
    /// touched; an over-limit query is rejected with an error message.
    pub max_rows_per_query: usize,
    /// Sliding-window per-client mutation rate limit
    /// (`ORBIT_MUTATIONS_PER_MINUTE`, default 0 = unlimited).
    pub mutations_per_minute: usize,
    /// Eviction timeout for a poke send onto a stalled socket
    /// (`ORBIT_POKE_TIMEOUT_SECS`, default 120). A client that stops reading
    /// otherwise parks its task (and its view memory) forever.
    pub poke_timeout: std::time::Duration,
}

pub fn serve_limits() -> &'static ServeLimits {
    static LIMITS: std::sync::OnceLock<ServeLimits> = std::sync::OnceLock::new();
    LIMITS.get_or_init(|| {
        let num = |k: &str, d: usize| {
            std::env::var(k).ok().and_then(|v| v.trim().parse().ok()).unwrap_or(d)
        };
        ServeLimits {
            max_queries_per_client: num("ORBIT_MAX_QUERIES_PER_CLIENT", 200),
            max_rows_per_query: num("ORBIT_MAX_ROWS_PER_QUERY", 100_000),
            mutations_per_minute: num("ORBIT_MUTATIONS_PER_MINUTE", 0),
            poke_timeout: std::time::Duration::from_secs(
                num("ORBIT_POKE_TIMEOUT_SECS", 120) as u64
            ),
        }
    })
}

/// Wall-clock budget for one query's synchronous materialize phase before it
/// is reported as a shard-stalling slow hydration (`ORBIT_SLOW_HYDRATION_MS`,
/// default 500ms). The IVM fetch cannot yield mid-walk, so detection +
/// per-query attribution is the actionable signal (Zero's lap-budget analog).
fn slow_hydration_budget() -> std::time::Duration {
    static BUDGET: std::sync::OnceLock<std::time::Duration> = std::sync::OnceLock::new();
    *BUDGET.get_or_init(|| {
        std::time::Duration::from_millis(
            std::env::var("ORBIT_SLOW_HYDRATION_MS")
                .ok()
                .and_then(|v| v.trim().parse().ok())
                .unwrap_or(500),
        )
    })
}

/// Serialized-size cap per `pokePart` frame. `ORBIT_POKE_PART_BYTES`
/// (default 512 KiB, floor 4 KiB). Bounds the peak transient allocation of a
/// poke to O(cap) instead of O(result set): a large hydration becomes many
/// small frames, each awaited onto the socket (real TCP backpressure) before
/// the next is serialized. A single op larger than the cap still travels alone
/// in its own part (the WebSocket `max_message_size` is the hard ceiling).
fn poke_part_cap() -> usize {
    static CAP: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *CAP.get_or_init(|| {
        std::env::var("ORBIT_POKE_PART_BYTES")
            .ok()
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(512 * 1024)
            .max(4 * 1024)
    })
}

/// Rough serialized JSON size of a row (delegates to the byte-footprint
/// estimator in `oql` — close enough for greedy packing).
fn estimate_row_bytes(row: &oql::value::Row) -> usize {
    row.estimated_bytes()
}

/// Rough serialized size of one row-patch op, JSON overhead included.
fn estimate_op_bytes(op: &RowPatchOp) -> usize {
    const OP_OVERHEAD: usize = 48; // {"op":"…","tableName":"…", …} scaffolding
    match op {
        RowPatchOp::Put { table_name, value } => {
            OP_OVERHEAD + table_name.len() + estimate_row_bytes(value)
        }
        RowPatchOp::Update { table_name, id, merge, .. } => {
            OP_OVERHEAD
                + table_name.len()
                + estimate_row_bytes(id)
                + merge.as_ref().map_or(0, |m| m.to_string().len())
        }
        RowPatchOp::Del { table_name, id } => {
            OP_OVERHEAD + table_name.len() + estimate_row_bytes(id)
        }
        RowPatchOp::Clear => OP_OVERHEAD,
    }
}

/// Send one poke (`pokeStart`/`pokePart`s/`pokeEnd`) carrying `rows`, optionally
/// announcing a newly-got query `hash` and/or `lastMutationID` changes.
///
/// `rows` is greedily packed into byte-capped `pokePart` frames (see
/// [`poke_part_cap`]); the first part carries the query/lmid metadata. One
/// `pokeStart`/`pokeEnd` wraps them all — the client accumulates parts and
/// applies atomically at `pokeEnd`, so chunking is invisible to it, and a
/// mid-poke disconnect discards cleanly.
async fn poke<S>(
    tx: &mut SplitSink<WebSocketStream<S>, Message>,
    version: &mut u64,
    base_cookie: &mut Option<String>,
    got_query: Option<String>,
    last_mutation_ids: Option<std::collections::HashMap<String, u64>>,
    rows: RowsPatch,
) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    *version += 1;
    let cookie = format!("{:08}", *version);
    let poke_id = format!("poke-{}", *version);
    let cap = poke_part_cap();
    let metrics = crate::metrics::node_metrics();
    let is_hydration = got_query.is_some();
    let mut sent_bytes = 0usize;
    let mut sent_parts = 0u64;

    sent_bytes += send(tx, &Downstream::PokeStart(PokeStartBody {
        poke_id: poke_id.clone(),
        base_cookie: base_cookie.clone(),
        schema_versions: None,
        timestamp: None,
    }))
    .await?;

    // Metadata (got-query + lmid changes) rides only on the FIRST part —
    // which is sent even when `rows` is empty (lmid-only pokes must ack).
    let mut meta = Some((
        last_mutation_ids,
        got_query.map(|hash| {
            vec![QueriesPatchOp::Put { hash, ttl: None, ast: None, name: None, args: None }]
        }),
    ));

    // Greedy byte-capped packing. Consuming `rows` by value lets each sent
    // chunk's ops (table-name Strings, Rc bumps) drop as we go.
    let mut it = rows.into_iter().peekable();
    loop {
        let mut chunk: RowsPatch = Vec::new();
        let mut chunk_bytes = 0usize;
        while let Some(op) = it.peek() {
            // ~15% fudge for JSON syntax the estimator can't see.
            let est = estimate_op_bytes(op) * 115 / 100;
            if !chunk.is_empty() && chunk_bytes + est > cap {
                break;
            }
            chunk_bytes += est;
            chunk.push(it.next().unwrap());
        }
        let (lmids, got) = meta.take().unwrap_or((None, None));
        sent_bytes += send(tx, &Downstream::PokePart(PokePartBody {
            poke_id: poke_id.clone(),
            last_mutation_id_changes: lmids,
            got_queries_patch: got,
            rows_patch: Some(chunk),
            ..Default::default()
        }))
        .await?;
        sent_parts += 1;
        // `send` awaits the socket flush (backpressure); yield anyway so a
        // fast socket can't let one giant hydration monopolize the LocalSet.
        tokio::task::yield_now().await;
        if it.peek().is_none() {
            break;
        }
    }

    sent_bytes += send(tx, &Downstream::PokeEnd(PokeEndBody {
        poke_id,
        cookie: cookie.clone(),
        cancel: None,
    }))
    .await?;
    *base_cookie = Some(cookie);
    if let Some(m) = &metrics {
        use std::sync::atomic::Ordering::Relaxed;
        m.pokes_total.fetch_add(1, Relaxed);
        m.poke_parts_total.fetch_add(sent_parts, Relaxed);
        m.poke_bytes_total.fetch_add(sent_bytes as u64, Relaxed);
        if is_hydration {
            m.hydration_bytes_total.fetch_add(sent_bytes as u64, Relaxed);
        }
    }
    Ok(())
}

/// Serialize + send one frame; returns its serialized byte length (metrics).
async fn send<S>(
    tx: &mut SplitSink<WebSocketStream<S>, Message>,
    msg: &Downstream,
) -> anyhow::Result<usize>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let s = serde_json::to_string(msg)?;
    let n = s.len();
    // Slow-client eviction: a stalled socket (TCP backpressure never
    // draining) would otherwise park this task — and everything its view
    // holds — forever. Time out and drop the connection instead; the client
    // reconnects and resumes from its CVR.
    match tokio::time::timeout(serve_limits().poke_timeout, tx.send(Message::Text(s))).await {
        Ok(res) => res?,
        Err(_) => anyhow::bail!(
            "client socket stalled for {:?}; evicting slow client",
            serve_limits().poke_timeout
        ),
    }
    Ok(n)
}

/// RAII connection gauges: increments `connected_clients` on connect and
/// unwinds this connection's contribution to every gauge on ANY exit path.
struct ConnGauges {
    m: Option<std::sync::Arc<crate::metrics::Metrics>>,
    active: u64,
    matched: u64,
}

impl ConnGauges {
    fn new() -> Self {
        let m = crate::metrics::node_metrics();
        if let Some(m) = &m {
            m.connected_clients.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        ConnGauges { m, active: 0, matched: 0 }
    }

    /// Reconcile the global gauges with this connection's current counts.
    fn update(&mut self, active: usize, matched: usize) {
        let Some(m) = &self.m else { return };
        use std::sync::atomic::Ordering::Relaxed;
        let (active, matched) = (active as u64, matched as u64);
        if active >= self.active {
            m.active_queries.fetch_add(active - self.active, Relaxed);
        } else {
            m.active_queries.fetch_sub(self.active - active, Relaxed);
        }
        if matched >= self.matched {
            m.matched_rows.fetch_add(matched - self.matched, Relaxed);
        } else {
            m.matched_rows.fetch_sub(self.matched - matched, Relaxed);
        }
        self.active = active;
        self.matched = matched;
    }
}

impl Drop for ConnGauges {
    fn drop(&mut self) {
        if let Some(m) = &self.m {
            use std::sync::atomic::Ordering::Relaxed;
            m.connected_clients.fetch_sub(1, Relaxed);
            m.active_queries.fetch_sub(self.active, Relaxed);
            m.matched_rows.fetch_sub(self.matched, Relaxed);
        }
    }
}

/// Serve a connection in a **multi-client** deployment.
///
/// The shared [`Replica`] is advanced once by the replication pump; each
/// connection is notified via a broadcast `tick` and flushes its own active
/// queries. Mutations (`push`) are written to upstream Postgres via the mutagen
/// (`pg`) and return through replication — so all clients converge through the
/// single Postgres source of truth.
#[allow(clippy::too_many_arguments)]
pub async fn serve_client<S, P>(
    ws: WebSocketStream<S>,
    provider: &P,
    pg: Option<&tokio_postgres::Client>,
    mutators: &MutatorRegistry,
    queries: &QueryRegistry,
    forwarder: &Forwarder,
    auth: &AuthContext,
    initial_queries: Vec<QueriesPatchOp>,
    client_id: Option<String>,
    // The last cookie the client successfully applied (from the connect URL); used
    // to prove it's safe to resume as a delta. `None` (or a mismatch) → full resync.
    client_base_cookie: Option<u64>,
    mut ticks: tokio::sync::broadcast::Receiver<Tick>,
    // Per-client lastMutationIDs, advanced by the replication apply loop from
    // replicated `orbit_client_mutations` rows (see [`capture_lmid`]).
    lmids: &LmidMap,
    // The local replica's change-stream position (updated by the apply pump), or
    // `None` where cross-node staleness can't occur (single-node/tests). Gates
    // serving a client whose persisted view is AHEAD of this replica.
    replica_pos: Option<Rc<std::cell::Cell<u64>>>,
) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
    P: oql::SourceProvider,
{
    let (mut tx, mut rx) = ws.split();
    send(&mut tx, &Downstream::Connected(ConnectedBody { wsid: "orbit-0".into(), timestamp: None })).await?;

    let mut active: Vec<ActiveQuery> = Vec::new();
    let mut version: u64 = 0;
    let mut base_cookie: Option<String> = None;
    let mut row_refs = RowRefs::new();
    let mut gauges = ConnGauges::new();

    // Shared-CVR cross-node resume: if the client identifies itself and we have a
    // Postgres handle, load the view it last held (persisted by whatever node it was
    // on) and the version that view corresponds to. The fast delta path runs ONLY
    // when the client proves it holds that view — its acked cookie equals the stored
    // version; otherwise we full-resync (correct, just not minimal). The wire cookie
    // is a durable per-client poke counter that resumes from the stored version.
    let cvr_on = client_id.is_some() && pg.is_some();
    let mut checkpoint: ClientView = ClientView::new();
    let mut resume_prior: Option<ClientView> = None;
    if cvr_on {
        let (pg, cid) = (pg.unwrap(), client_id.as_deref().unwrap());
        let (view, v, stored_pos) = PgCvrStore::load_client_view(pg, cid).await.unwrap_or_default();
        // Staleness gate: the client's persisted view reflects stream position
        // `stored_pos` (checkpointed by whichever node served it last). If THIS
        // node's replica is behind that — freshly restored from an older
        // snapshot, still catching up — serving now would time-travel the client
        // backwards (authoritative retractions of rows that still exist). Wait
        // for the apply pump to catch up first (Zero: "wait for the next
        // advancement").
        let mut caught_up = true;
        if let Some(pos) = &replica_pos {
            if pos.get() < stored_pos {
                eprintln!(
                    "client {cid}: replica at {} but client view at {stored_pos}; waiting for catch-up",
                    pos.get()
                );
                // Bounded wait: same-lineage lag catches up in moments; a stream
                // whose positions restarted (slot/log recreated) never will —
                // don't deadlock on it. On timeout the stored pos is treated as
                // incomparable: NO fast delta (it could suppress rows the client
                // lacks against a stale replica) — full resync instead, which is
                // an authoritative replacement and converges via later pokes.
                let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
                while pos.get() < stored_pos {
                    match tokio::time::timeout_at(deadline, ticks.recv()).await {
                        Ok(Ok(_)) => {}
                        Ok(Err(_)) => anyhow::bail!("tick channel closed while waiting for catch-up"),
                        Err(_) => {
                            eprintln!(
                                "client {cid}: replica still at {} (< {stored_pos}) after wait; forcing full resync",
                                pos.get()
                            );
                            caught_up = false;
                            break;
                        }
                    }
                }
            }
        }
        version = v;
        checkpoint = view.clone();
        let fast = caught_up && client_base_cookie == Some(v);
        resume_prior = Some(if fast { view } else { ClientView::new() });
    }
    let mut last_ckpt = std::time::Instant::now();
    // Highest lastMutationID already acked to THIS client. On each tick we ack up
    // to the client's current id in `lmids` (advanced by replication from the same
    // commit as the rows), so the ack rides atomically with the rows — and a tick
    // caused by ANOTHER client's write never carries this client's ack.
    let mut last_sent_lmid: u64 = 0;
    // Sliding-window per-client mutation rate limiter (Zero's
    // perUserMutationLimit analog). Timestamps of accepted pushes in the
    // last minute; disabled when the env knob is 0.
    let mut mutation_window: std::collections::VecDeque<std::time::Instant> =
        std::collections::VecDeque::new();

    // Queries supplied during the handshake (Zero client `initConnection`).
    if !initial_queries.is_empty() {
        let resume = resume_prior.take();
        subscribe(initial_queries, provider, queries, forwarder, auth, &mut tx, &mut active, &mut version, &mut base_cookie, &mut row_refs, resume.as_ref()).await?;
        cvr_checkpoint(pg, &client_id, &mut checkpoint, &row_refs, version, replica_pos.as_ref().map(|p| p.get()).unwrap_or(0)).await?;
        gauges.update(active.len(), row_refs.len());
    }

    loop {
        tokio::select! {
            msg = rx.next() => {
                let Some(msg) = msg else { break };
                match msg? {
                    Message::Close(_) => break,
                    Message::Ping(p) => tx.send(Message::Pong(p)).await?,
                    Message::Text(text) if !text.trim().is_empty() => {
                        let up: Upstream = serde_json::from_str(&text)?;
                        match up {
                            Upstream::Ping => {
                                send(&mut tx, &Downstream::Pong).await?;
                            }
                            Upstream::Push(push) => {
                                let limit = serve_limits().mutations_per_minute;
                                if limit > 0 {
                                    let now = std::time::Instant::now();
                                    while mutation_window
                                        .front()
                                        .is_some_and(|t| now.duration_since(*t).as_secs() >= 60)
                                    {
                                        mutation_window.pop_front();
                                    }
                                    if mutation_window.len() + push.mutations.len() > limit {
                                        // Rejected, not consumed: the lastMutationID
                                        // does NOT advance, so the client re-sends
                                        // later — nothing is lost, just deferred.
                                        send(&mut tx, &Downstream::Error(orbit_protocol::ErrorBody {
                                            kind: orbit_protocol::ErrorKind::MutationRateLimited,
                                            message: format!(
                                                "mutation rate limit: {limit}/min per client"
                                            ),
                                        }))
                                        .await?;
                                        continue;
                                    }
                                    for _ in 0..push.mutations.len() {
                                        mutation_window.push_back(now);
                                    }
                                }
                                // The lastMutationID is NOT acked here. It's advanced by the app's
                                // PushProcessor (or the direct-write path below) in `orbit_client_mutations`
                                // — in the same Postgres transaction as the data — and returns via
                                // replication, so flush_active can ack it atomically with the rows it
                                // produced. Acking on receipt would drop the client's optimistic overlay
                                // a round-trip before its authoritative row arrives (the revert flicker).
                                if forwarder.forwards_mutations() {
                                    // Forward to the app's push endpoint (auth attached). It runs
                                    // the mutators with context and writes to Postgres; the change
                                    // returns via replication.
                                    forwarder.push(auth, &push.mutations).await?;
                                } else if let Some(pg) = pg {
                                    // No endpoint configured: write through to Postgres directly, then
                                    // advance the client's lastMutationID (after the rows, so its ack
                                    // never replicates ahead of them).
                                    //
                                    // NOTE: the per-mutation ops and `advance_lmid` are separate
                                    // statements (not one transaction) because every connection shares
                                    // one pooled `pg` client, so an explicit BEGIN/COMMIT would capture
                                    // other tasks' pipelined queries; full atomicity here needs a
                                    // dedicated connection. Dedup, however, we can and must do: a
                                    // reconnect resends unconfirmed pushes, so skip any mutation whose id
                                    // is already recorded (its ops were already applied) — otherwise the
                                    // replay double-applies non-idempotent ops.
                                    for m in &push.mutations {
                                        if m.id() <= current_lmid(pg, m.client_id()).await? {
                                            continue; // already processed (re-delivered on reconnect)
                                        }
                                        // Application errors are CONSUMED per mutation (Zero's
                                        // model): the lastMutationID still advances and an
                                        // `error` message tells the client. Killing the socket
                                        // here would make a permanently-failing mutation a
                                        // poison pill — the client re-sends it on every
                                        // reconnect, forever, wedging everything queued
                                        // behind it. An unknown mutator (version skew) is the
                                        // same case, made LOUD instead of silently dropped.
                                        let applied: anyhow::Result<()> = async {
                                            match m {
                                                orbit_protocol::Mutation::Crud { .. } => {
                                                    crate::mutagen::apply_mutation(pg, m).await
                                                }
                                                orbit_protocol::Mutation::Custom { name, args, .. } => {
                                                    match mutators.run(name, provider, args) {
                                                        Some(ops) => {
                                                            for op in &ops {
                                                                crate::mutagen::apply_crud_op(pg, op).await?;
                                                            }
                                                            Ok(())
                                                        }
                                                        None => Err(anyhow::anyhow!(
                                                            "unknown mutator {name:?} (client/server version skew?)"
                                                        )),
                                                    }
                                                }
                                            }
                                        }
                                        .await;
                                        if let Err(e) = &applied {
                                            eprintln!(
                                                "mutation {} from {} failed (consumed): {e:#}",
                                                m.id(),
                                                m.client_id()
                                            );
                                            send(&mut tx, &Downstream::Error(orbit_protocol::ErrorBody {
                                                kind: orbit_protocol::ErrorKind::MutationFailed,
                                                message: format!("mutation {} failed: {e}", m.id()),
                                            }))
                                            .await?;
                                        }
                                        advance_lmid(pg, m.client_id(), m.id()).await?;
                                    }
                                }
                            }
                            Upstream::InitConnection(b) => {
                                let resume = resume_prior.take();
                                subscribe(b.desired_queries_patch, provider, queries, forwarder, auth, &mut tx, &mut active, &mut version, &mut base_cookie, &mut row_refs, resume.as_ref()).await?;
                                cvr_checkpoint(pg, &client_id, &mut checkpoint, &row_refs, version, replica_pos.as_ref().map(|p| p.get()).unwrap_or(0)).await?;
                                gauges.update(active.len(), row_refs.len());
                            }
                            Upstream::ChangeDesiredQueries(b) => {
                                let resume = resume_prior.take();
                                subscribe(b.desired_queries_patch, provider, queries, forwarder, auth, &mut tx, &mut active, &mut version, &mut base_cookie, &mut row_refs, resume.as_ref()).await?;
                                cvr_checkpoint(pg, &client_id, &mut checkpoint, &row_refs, version, replica_pos.as_ref().map(|p| p.get()).unwrap_or(0)).await?;
                                gauges.update(active.len(), row_refs.len());
                            }
                        }
                    }
                    _ => {}
                }
            }
            tick = ticks.recv() => {
                // Which tables the transaction touched. Lagged/closed (or an
                // empty set) → conservative: drain everything.
                let touched: Option<Tick> = tick.ok();
                // Ack this client's mutations up to the id replication has confirmed
                // (advanced from the same commit as this tick's rows). A tick from
                // another client's write leaves our id unchanged → no ack for us.
                let ack = match client_id.as_deref() {
                    Some(cid) => {
                        let cur = lmids.borrow().get(cid).copied().unwrap_or(0);
                        if cur > last_sent_lmid {
                            last_sent_lmid = cur;
                            HashMap::from([(cid.to_string(), cur)])
                        } else {
                            HashMap::new()
                        }
                    }
                    None => HashMap::new(),
                };
                // No-op wake suppression: if the transaction touched none of
                // this client's tables and there is nothing to ack, skip the
                // drain (and the checkpoint clock) entirely.
                let relevant = match &touched {
                    Some(t) if !t.is_empty() => {
                        active.iter().any(|q| t.iter().any(|tb| q.tables.contains(tb)))
                    }
                    _ => true,
                };
                if !relevant && ack.is_empty() {
                    continue;
                }
                flush_active(&active, &mut tx, &mut version, &mut base_cookie, &mut row_refs, ack).await?;
                gauges.update(active.len(), row_refs.len());
                // Throttled CVR checkpoint (off the per-mutation hot path) so a
                // reconnect to another node resumes from a recent view.
                if cvr_on && last_ckpt.elapsed() >= std::time::Duration::from_secs(1) {
                    cvr_checkpoint(pg, &client_id, &mut checkpoint, &row_refs, version, replica_pos.as_ref().map(|p| p.get()).unwrap_or(0)).await?;
                    last_ckpt = std::time::Instant::now();
                }
            }
        }
    }
    // Persist the final view on a clean disconnect, at the latest cookie — so a
    // reconnect that reports this cookie takes the fast delta path.
    cvr_checkpoint(pg, &client_id, &mut checkpoint, &row_refs, version, replica_pos.as_ref().map(|p| p.get()).unwrap_or(0)).await?;
    drop(active);
    Ok(())
}

/// Persist the client's current view as a delta from the last checkpoint. No-op
/// unless this connection has CVR enabled (a client id + a Postgres handle).
async fn cvr_checkpoint(
    pg: Option<&tokio_postgres::Client>,
    client_id: &Option<String>,
    checkpoint: &mut ClientView,
    row_refs: &RowRefs,
    version: u64,
    pos: u64,
) -> anyhow::Result<()> {
    if let (Some(pg), Some(cid)) = (pg, client_id.as_deref()) {
        let current = row_refs.view();
        PgCvrStore::commit_client_view(pg, cid, checkpoint, &current, version, pos).await?;
        *checkpoint = current;
    }
    Ok(())
}

/// Build pipelines for newly-desired queries and send their initial pokes.
///
/// A `Put` carries either an `ast` (client query) or a `name`+`args` (custom /
/// named query resolved by the [`QueryRegistry`]).
#[allow(clippy::too_many_arguments)]
async fn subscribe<S>(
    patch: Vec<QueriesPatchOp>,
    provider: &dyn oql::SourceProvider,
    queries: &QueryRegistry,
    forwarder: &Forwarder,
    auth: &AuthContext,
    tx: &mut SplitSink<WebSocketStream<S>, Message>,
    active: &mut Vec<ActiveQuery>,
    version: &mut u64,
    base_cookie: &mut Option<String>,
    row_refs: &mut RowRefs,
    // The view this client already holds (from the CVR), present only on the first
    // subscribe of a reconnect: puts for unchanged rows are suppressed, and rows
    // no longer matched are deleted after the batch. This is the cross-node delta.
    resume: Option<&ClientView>,
) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // A full resync (an empty prior view — cookie mismatch or fresh client) is an
    // authoritative REPLACEMENT, not a merge: emit a Clear before rebuilding so a
    // delete the client missed while disconnected (and that the CVR no longer tracks)
    // can't survive as a phantom row in its cache/IndexedDB. A populated prior (delta
    // resume) and mid-session query adds (resume is None) must NOT clear.
    let mut pending_clear = matches!(resume, Some(p) if p.is_empty());
    for op in patch {
        match op {
            QueriesPatchOp::Put { hash, ast, name, args, .. } => {
                // Resolve the query to an AST: an explicit AST, else a named query —
                // transformed by the app's query endpoint (auth attached) if one is
                // configured, otherwise by the local QueryRegistry.
                let ast = match ast {
                    Some(a) => Some(a),
                    None => match name {
                        Some(n) if forwarder.forwards_queries() => {
                            Some(forwarder.transform(auth, &n, args.as_deref().unwrap_or(&[])).await?)
                        }
                        Some(n) => queries.resolve(&n, args.as_deref().unwrap_or(&[])),
                        None => None,
                    },
                };
                let Some(ast) = ast else { continue };
                let limits = serve_limits();
                if limits.max_queries_per_client > 0
                    && active.len() >= limits.max_queries_per_client
                {
                    send(tx, &Downstream::Error(orbit_protocol::ErrorBody {
                        kind: orbit_protocol::ErrorKind::ServerOverloaded,
                        message: format!(
                            "query {hash} rejected: max {} active queries per client",
                            limits.max_queries_per_client
                        ),
                    }))
                    .await?;
                    continue;
                }
                let ast_tables = ast.tables();
                // Admission control: the fetch + patch below materialize the
                // full result — the permit bounds peak node memory during the
                // MATERIALIZE phase to (gate width) × result. The permit is
                // released before the chunked poke: holding it across a
                // slow-socket send let one stalled client head-of-line block
                // every queued hydration on the node (audit Tier 1.5) — the
                // poke drains an already-materialized patch under its own TCP
                // backpressure and needs no admission slot.
                let gate = hydration_gate();
                let hydrating = gate.acquire().await.expect("hydration gate closed");
                // Consistency: wait out any in-progress streamed transaction
                // (held only while a pump streams an oversized txn — free
                // otherwise). Released with `hydrating` before the poke.
                let apply_lock = replica_apply_lock();
                let consistent = apply_lock.read().await;
                let started = std::time::Instant::now();
                let top = build_pipeline(&ast, provider);
                let catch = Catch::new(top.input.clone());
                let link: Link = catch.clone();
                top.set_output(link);
                let schema = catch.borrow().get_schema();
                let nodes = catch.borrow().fetch();
                if limits.max_rows_per_query > 0 {
                    let n = crate::view_sync::count_result_rows(&nodes);
                    if n > limits.max_rows_per_query {
                        // Reject BEFORE the refcounted patch build touches any
                        // client state — nothing to unwind.
                        drop(nodes);
                        drop(consistent);
                        drop(hydrating);
                        send(tx, &Downstream::Error(orbit_protocol::ErrorBody {
                            kind: orbit_protocol::ErrorKind::ServerOverloaded,
                            message: format!(
                                "query {hash} rejected: result has {n} rows (max {})",
                                limits.max_rows_per_query
                            ),
                        }))
                        .await?;
                        continue;
                    }
                }
                let mut rows = match resume {
                    Some(prior) => resume_patches_dedup(&nodes, &schema, row_refs, prior),
                    None => initial_patches_dedup(&nodes, &schema, row_refs),
                };
                // The patches share the nodes' `Rc<Row>`s, so dropping the node
                // tree here frees only its scaffolding — but do it before the
                // (chunked, potentially slow-socket) poke holds it alive.
                drop(nodes);
                drop(consistent);
                drop(hydrating);
                let hydrate_took = started.elapsed();
                if hydrate_took > slow_hydration_budget() {
                    eprintln!(
                        "slow hydration: query {} materialized {} ops in {hydrate_took:?} (budget {:?}) — other clients on this shard stalled for that long",
                        hash,
                        rows.len(),
                        slow_hydration_budget()
                    );
                }
                crate::metrics::observe_hydration(
                    hydrate_took,
                    rows.len() as u64,
                    hydrate_took > slow_hydration_budget(),
                );
                crate::metrics::record_query_hydration(&hash, hydrate_took, rows.len() as u64);
                crate::metrics::record_query_active(&hash, 1);
                // Let queued work (other clients' sends, ticks) interleave
                // between one query's heavy synchronous fetch and the next.
                tokio::task::yield_now().await;
                // Prepend the replacement Clear to the first query's poke so its rows
                // clear+rebuild atomically (no flash of an empty result).
                if pending_clear {
                    rows.insert(0, RowPatchOp::Clear);
                    pending_clear = false;
                }
                poke(tx, version, base_cookie, Some(hash.clone()), None, rows).await?;
                active.push(ActiveQuery { hash, catch, schema, tables: ast_tables });
            }
            QueriesPatchOp::Del { hash } => {
                // Retract the query's rows BEFORE dropping its pipeline: decrement
                // its refcounts and delete rows no other live query provides.
                // Silently dropping it would leak the counts — and permanently
                // suppress future deletes of rows this query shared.
                let mut dels = RowsPatch::new();
                for q in active.iter().filter(|q| q.hash == hash) {
                    let nodes = q.catch.borrow().fetch();
                    dels.extend(crate::view_sync::retract_patches_dedup(&nodes, &q.schema, row_refs));
                }
                if !dels.is_empty() {
                    poke(tx, version, base_cookie, None, None, dels).await?;
                }
                active.retain(|q| q.hash != hash);
            }
            _ => {}
        }
    }
    // Full resync with no queries to repopulate → still wipe the client's stale rows.
    if pending_clear {
        poke(tx, version, base_cookie, None, None, vec![RowPatchOp::Clear]).await?;
    }
    // After resuming all queries, drop rows the client held that no current query
    // provides anymore (computed against the now-populated view).
    if let Some(prior) = resume {
        let dels = resume_deletes(prior, row_refs);
        if !dels.is_empty() {
            poke(tx, version, base_cookie, None, None, dels).await?;
        }
    }
    Ok(())
}
