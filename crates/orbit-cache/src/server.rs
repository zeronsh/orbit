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
use std::rc::Rc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

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
}

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
                flush_active(&active, &mut tx, &mut version, &mut base_cookie, &mut row_refs, &mut std::collections::HashMap::new()).await?;
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
                    Mutation::Custom { name, args, .. } => {
                        mutators.run(name, replica, args).unwrap_or_default()
                    }
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
            // Acknowledge processed mutations, then send the resulting rows.
            if !lmids.is_empty() {
                poke(tx, version, base_cookie, None, Some(lmids), vec![]).await?;
            }
            flush_active(active, tx, version, base_cookie, row_refs, &mut std::collections::HashMap::new()).await?;
            return Ok(());
        }
        Upstream::InitConnection(b) => b.desired_queries_patch,
        Upstream::ChangeDesiredQueries(b) => b.desired_queries_patch,
    };

    // Legacy single-process path: no shared CVR resume.
    subscribe(patch, replica, queries, forwarder, auth, tx, active, version, base_cookie, row_refs, None).await
}

/// Advance every active query and poke any resulting row patches, riding any
/// deferred `lastMutationID` acks along with them.
async fn flush_active<S>(
    active: &[ActiveQuery],
    tx: &mut SplitSink<WebSocketStream<S>, Message>,
    version: &mut u64,
    base_cookie: &mut Option<String>,
    row_refs: &mut RowRefs,
    pending_lmids: &mut std::collections::HashMap<String, u64>,
) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut patches = Vec::new();
    for q in active {
        let changes = q.catch.borrow_mut().take_changes();
        if changes.is_empty() {
            continue;
        }
        patches.extend(changes_to_patches_dedup(&changes, &q.schema, row_refs));
    }
    // Acknowledge mutations together with the rows they produced (one atomic poke),
    // so the client drops its optimistic overlay only once the authoritative row is
    // present — otherwise the early ack reverts the row until replication catches up.
    if !patches.is_empty() || !pending_lmids.is_empty() {
        let lmids = if pending_lmids.is_empty() { None } else { Some(std::mem::take(pending_lmids)) };
        poke(tx, version, base_cookie, None, lmids, patches).await?;
    }
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

/// Send one poke (`pokeStart`/`pokePart`/`pokeEnd`) carrying `rows`, optionally
/// announcing a newly-got query `hash` and/or `lastMutationID` changes.
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

    send(tx, &Downstream::PokeStart(PokeStartBody {
        poke_id: poke_id.clone(),
        base_cookie: base_cookie.clone(),
        schema_versions: None,
        timestamp: None,
    }))
    .await?;
    send(tx, &Downstream::PokePart(PokePartBody {
        poke_id: poke_id.clone(),
        last_mutation_id_changes: last_mutation_ids,
        got_queries_patch: got_query.map(|hash| {
            vec![QueriesPatchOp::Put { hash, ttl: None, ast: None, name: None, args: None }]
        }),
        rows_patch: Some(rows),
        ..Default::default()
    }))
    .await?;
    send(tx, &Downstream::PokeEnd(PokeEndBody {
        poke_id,
        cookie: cookie.clone(),
        cancel: None,
    }))
    .await?;
    *base_cookie = Some(cookie);
    Ok(())
}

async fn send<S>(
    tx: &mut SplitSink<WebSocketStream<S>, Message>,
    msg: &Downstream,
) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    tx.send(Message::Text(serde_json::to_string(msg)?)).await?;
    Ok(())
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
    mut ticks: tokio::sync::broadcast::Receiver<()>,
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
        let (view, v) = PgCvrStore::load_client_view(pg, cid).await.unwrap_or_default();
        version = v;
        checkpoint = view.clone();
        let fast = client_base_cookie == Some(v);
        resume_prior = Some(if fast { view } else { ClientView::new() });
    }
    let mut last_ckpt = std::time::Instant::now();
    // Deferred lastMutationID acks — sent with the rows they produced (flush_active),
    // so the client's optimistic overlay isn't dropped before its row arrives.
    let mut pending_lmids: std::collections::HashMap<String, u64> = std::collections::HashMap::new();

    // Queries supplied during the handshake (Zero client `initConnection`).
    if !initial_queries.is_empty() {
        let resume = resume_prior.take();
        subscribe(initial_queries, provider, queries, forwarder, auth, &mut tx, &mut active, &mut version, &mut base_cookie, &mut row_refs, resume.as_ref()).await?;
        cvr_checkpoint(pg, &client_id, &mut checkpoint, &row_refs, version).await?;
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
                            Upstream::Ping => send(&mut tx, &Downstream::Pong).await?,
                            Upstream::Push(push) => {
                                // Defer the lastMutationID ack: record it, but only send it once the
                                // mutation's rows return via replication (in flush_active). Acking now
                                // would drop the client's optimistic overlay ~30ms before its
                                // authoritative row arrives — the revert flicker.
                                for m in &push.mutations {
                                    let e = pending_lmids.entry(m.client_id().to_string()).or_insert(0);
                                    *e = (*e).max(m.id());
                                }
                                if forwarder.forwards_mutations() {
                                    // Forward to the app's push endpoint (auth attached). It runs
                                    // the mutators with context and writes to Postgres; the change
                                    // returns via replication.
                                    forwarder.push(auth, &push.mutations).await?;
                                } else if let Some(pg) = pg {
                                    // No endpoint configured: write through to Postgres directly.
                                    for m in &push.mutations {
                                        match m {
                                            orbit_protocol::Mutation::Crud { .. } => {
                                                crate::mutagen::apply_mutation(pg, m).await?;
                                            }
                                            orbit_protocol::Mutation::Custom { name, args, .. } => {
                                                if let Some(ops) = mutators.run(name, provider, args) {
                                                    for op in &ops {
                                                        crate::mutagen::apply_crud_op(pg, op).await?;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                // No immediate ack — flush_active acks these on the next replication
                                // tick, atomically with the mutation's rows.
                            }
                            Upstream::InitConnection(b) => {
                                let resume = resume_prior.take();
                                subscribe(b.desired_queries_patch, provider, queries, forwarder, auth, &mut tx, &mut active, &mut version, &mut base_cookie, &mut row_refs, resume.as_ref()).await?;
                                cvr_checkpoint(pg, &client_id, &mut checkpoint, &row_refs, version).await?;
                            }
                            Upstream::ChangeDesiredQueries(b) => {
                                let resume = resume_prior.take();
                                subscribe(b.desired_queries_patch, provider, queries, forwarder, auth, &mut tx, &mut active, &mut version, &mut base_cookie, &mut row_refs, resume.as_ref()).await?;
                                cvr_checkpoint(pg, &client_id, &mut checkpoint, &row_refs, version).await?;
                            }
                        }
                    }
                    _ => {}
                }
            }
            tick = ticks.recv() => {
                // Lagged or closed: still attempt a flush.
                let _ = tick;
                flush_active(&active, &mut tx, &mut version, &mut base_cookie, &mut row_refs, &mut pending_lmids).await?;
                // Throttled CVR checkpoint (off the per-mutation hot path) so a
                // reconnect to another node resumes from a recent view.
                if cvr_on && last_ckpt.elapsed() >= std::time::Duration::from_secs(1) {
                    cvr_checkpoint(pg, &client_id, &mut checkpoint, &row_refs, version).await?;
                    last_ckpt = std::time::Instant::now();
                }
            }
        }
    }
    // Persist the final view on a clean disconnect, at the latest cookie — so a
    // reconnect that reports this cookie takes the fast delta path.
    cvr_checkpoint(pg, &client_id, &mut checkpoint, &row_refs, version).await?;
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
) -> anyhow::Result<()> {
    if let (Some(pg), Some(cid)) = (pg, client_id.as_deref()) {
        let current = row_refs.view();
        PgCvrStore::commit_client_view(pg, cid, checkpoint, &current, version).await?;
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
                let top = build_pipeline(&ast, provider);
                let catch = Catch::new(top.input.clone());
                let link: Link = catch.clone();
                top.set_output(link);
                let schema = catch.borrow().get_schema();
                let nodes = catch.borrow().fetch();
                let mut rows = match resume {
                    Some(prior) => resume_patches_dedup(&nodes, &schema, row_refs, prior),
                    None => initial_patches_dedup(&nodes, &schema, row_refs),
                };
                // Prepend the replacement Clear to the first query's poke so its rows
                // clear+rebuild atomically (no flash of an empty result).
                if pending_clear {
                    rows.insert(0, RowPatchOp::Clear);
                    pending_clear = false;
                }
                poke(tx, version, base_cookie, Some(hash.clone()), None, rows).await?;
                active.push(ActiveQuery { hash, catch, schema });
            }
            QueriesPatchOp::Del { hash } => active.retain(|q| q.hash != hash),
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
