//! Wire-path decomposition benchmark: where does a view-syncer's per-client
//! serving time actually go?
//!
//! The `multinode_bench` fanout number deliberately measures **pure IVM
//! delivery**. A real view-syncer additionally, per client per tick:
//!   1. builds the wire patch (`changes_to_patches_dedup` over the drained
//!      changes, ref-counted per connection), and
//!   2. serializes the poke (`pokeStart`/`pokePart`/`pokeEnd`) to JSON.
//!
//! This bench runs the same fan-out workload three times — engine only,
//! engine+patch, engine+patch+JSON — so the stage costs fall out as deltas
//! (separate loops instead of inline timers keeps timer overhead out of the
//! numbers). WebSocket writes are excluded (network, not CPU).
//!
//!   cargo run --release --example wire_bench -p orbit-cache -- <clients> <changes>

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Instant;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use oql::ast::Direction as AstDir;
use oql::ivm::operator::{Link, OpHandle};
use oql::ivm::{
    connect, source_push, Catch, ColumnType, Filter, MemorySource, Predicate, Schema, SourceChange,
};
use oql::value::{Row, Value};
use orbit_cache::view_sync::{changes_to_patches_dedup, RowRefs};
use orbit_protocol::{Downstream, PokeEndBody, PokePartBody, PokeStartBody, RowsPatch};
use std::collections::BTreeMap;

fn row(pairs: &[(&str, Value)]) -> Row {
    pairs.iter().map(|(k, v)| (*k, v.clone())).collect()
}

struct Client {
    catch: Rc<RefCell<Catch>>,
    schema: Rc<Schema>,
    refs: RowRefs,
    version: u64,
}

type Source = Rc<RefCell<MemorySource>>;

fn build_shard(seed: usize, clients_n: usize) -> (Source, Vec<Client>) {
    let mut cols = BTreeMap::new();
    cols.insert("id".to_string(), ColumnType::Number);
    cols.insert("n".to_string(), ColumnType::Number);
    cols.insert("s".to_string(), ColumnType::String);
    let src = MemorySource::new("item", cols, vec!["id".into()]);
    for i in 0..seed {
        src.borrow_mut().insert_initial(row(&[
            ("id", (i as i64).into()),
            ("n", ((i % 1000) as i64).into()),
            ("s", "x".into()),
        ]));
    }

    let mut clients = Vec::with_capacity(clients_n);
    for _ in 0..clients_n {
        let conn = OpHandle::new(connect(&src, vec![("id".to_string(), AstDir::Asc)]));
        let pred: Predicate =
            Rc::new(|r| matches!(r.get("n"), Some(Value::Number(x)) if *x >= 500.0));
        let filter = Filter::new(conn, pred);
        let fh = OpHandle::new(filter);
        let schema = fh.input.borrow().get_schema();
        let catch = Catch::new(fh.input.clone());
        let link: Link = catch.clone();
        fh.set_output(link);
        catch.borrow().fetch(); // hydrate
        clients.push(Client { catch, schema, refs: RowRefs::new(), version: 0 });
    }
    (src, clients)
}

/// Stage selector: how much of the wire path each loop performs.
#[derive(Clone, Copy, PartialEq)]
enum Stage {
    Engine,
    Patch,
    Json,
}

fn run(src: &Source, clients: &mut [Client], seed: usize, changes: usize, stage: Stage) -> usize {
    let mut sink = 0usize;
    for j in 0..changes {
        source_push(
            src,
            SourceChange::Add(row(&[
                ("id", ((seed + j) as i64).into()),
                ("n", ((j % 1000) as i64).into()),
                ("s", "y".into()),
            ])),
        );
        for c in clients.iter_mut() {
            let drained = c.catch.borrow_mut().take_changes();
            sink += drained.len();
            if stage == Stage::Engine {
                continue;
            }
            let patch: RowsPatch = changes_to_patches_dedup(&drained, &c.schema, &mut c.refs);
            if patch.is_empty() {
                continue; // a real view-syncer skips empty pokes
            }
            sink += patch.len();
            if stage == Stage::Patch {
                continue;
            }
            // JSON: the exact three messages `server.rs::poke` sends.
            c.version += 1;
            let cookie = format!("{:08}", c.version);
            let poke_id = format!("poke-{}", c.version);
            let start = Downstream::PokeStart(PokeStartBody {
                poke_id: poke_id.clone(),
                base_cookie: Some(cookie.clone()),
                schema_versions: None,
                timestamp: None,
            });
            let part = Downstream::PokePart(PokePartBody {
                poke_id: poke_id.clone(),
                rows_patch: Some(patch),
                ..Default::default()
            });
            let end = Downstream::PokeEnd(PokeEndBody { poke_id, cookie, cancel: None });
            sink += serde_json::to_string(&start).unwrap().len();
            sink += serde_json::to_string(&part).unwrap().len();
            sink += serde_json::to_string(&end).unwrap().len();
        }
    }
    sink
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let clients: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(200);
    let changes: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(2000);
    let seed = 1000usize;
    let events = (clients * changes) as f64;

    let mut results = Vec::new();
    for (name, stage) in [
        ("engine (IVM drain only)     ", Stage::Engine),
        ("+ patch build (dedup/refcnt)", Stage::Patch),
        ("+ JSON serialize (3 msgs)   ", Stage::Json),
    ] {
        let (src, mut cs) = build_shard(seed, clients);
        let t = Instant::now();
        let sink = run(&src, &mut cs, seed, changes, stage);
        let el = t.elapsed();
        std::hint::black_box(sink);
        results.push((name, el));
    }

    println!("ORBIT wire-path decomposition: clients={clients} changes={changes}");
    let mut prev = 0.0f64;
    for (name, el) in &results {
        let ms = el.as_secs_f64() * 1e3;
        let rate = events / el.as_secs_f64();
        let delta = ms - prev;
        println!("  {name} {ms:9.1} ms ({rate:12.0} client-events/s)  [stage +{delta:.1} ms]");
        prev = ms;
    }
}
