//! Single-node fan-out throughput + multi-core scaling benchmark.
//!
//! Models a view-syncer's serving job under high fan-out: apply each change to the
//! replica, then advance **every connected client's** query pipeline. The unit is a
//! *client-fan-out* (one change delivered to one client). This is the single-node
//! (`run_server_sharded`) path — N shards in ONE process — NOT a multinode cluster.
//!
//! To compare apples-to-apples with Zero's `zql` engine
//! (`mono/orbit-golden/bench.ts fanout`), the timed loop measures **pure IVM
//! delivery** (drain the changes the pipeline delivered to each client). A real
//! view-syncer additionally builds the wire patch + JSON on BOTH engines; that's
//! excluded here so the number reflects the engine, not the serializer.
//!
//!   cargo run --release --example multinode_bench -p orbit-cache -- <clients_per_shard> <changes> [shards]

use std::rc::Rc;
use std::time::Instant;

// A scalable multithread allocator (the macOS/glibc system malloc serializes
// under the per-core fan-out allocation, capping scaling). Production servers do
// the same; this measures the engine, not the allocator.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use oql::ast::Direction as AstDir;
use oql::ivm::operator::{Link, OpHandle};
use oql::ivm::{
    connect, source_push, Catch, ColumnType, Filter, MemorySource, Predicate, SourceChange,
};
use oql::value::{Row, Value};

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::sync::Arc;

fn row(pairs: &[(&str, Value)]) -> Row {
    pairs.iter().map(|(k, v)| (*k, v.clone())).collect()
}
fn cols(names: &[(&str, ColumnType)]) -> BTreeMap<String, ColumnType> {
    names.iter().map(|(n, t)| (n.to_string(), *t)).collect()
}
fn asc(f: &str) -> Vec<(String, AstDir)> {
    vec![(f.to_string(), AstDir::Asc)]
}

/// One connected client of a view-syncer: a materialized query (its `Catch`
/// accumulates the changes the IVM delivers, which a view-syncer turns into a poke).
struct Client {
    catch: Rc<RefCell<Catch>>,
}

type Source = Rc<RefCell<MemorySource>>;

/// Build a view-syncer shard: its own replica seeded with `seed` rows + `clients_n`
/// connected clients of the same query (each materialized). Connection setup is a
/// one-time cost, so the benchmark builds outside the timed region.
fn build_shard(seed: usize, clients_n: usize) -> (Source, Vec<Client>) {
    let src = MemorySource::new(
        "item",
        cols(&[("id", ColumnType::Number), ("n", ColumnType::Number), ("s", ColumnType::String)]),
        vec!["id".into()],
    );
    for i in 0..seed {
        src.borrow_mut().insert_initial(row(&[
            ("id", (i as i64).into()),
            ("n", ((i % 1000) as i64).into()),
            ("s", "x".into()),
        ]));
    }

    let mut clients = Vec::with_capacity(clients_n);
    for _ in 0..clients_n {
        let conn = OpHandle::new(connect(&src, asc("id")));
        let pred: Predicate = Rc::new(|r| matches!(r.get("n"), Some(Value::Number(x)) if *x >= 500.0));
        let filter = Filter::new(conn, pred);
        let fh = OpHandle::new(filter);
        let catch = Catch::new(fh.input.clone());
        let link: Link = catch.clone();
        fh.set_output(link);
        catch.borrow().fetch(); // hydrate this client
        clients.push(Client { catch });
    }
    (src, clients)
}

/// Steady-state serving: apply `changes` events to the shard's replica and fan
/// each out to every connected client. To match Zero's `fanout` bench exactly,
/// this measures pure IVM delivery — drain the changes the IVM delivered to each
/// client (a real view-syncer additionally builds the wire patch on BOTH engines).
fn apply_changes(src: &Source, clients: &mut [Client], seed: usize, changes: usize) -> usize {
    let mut delivered = 0usize;
    for j in 0..changes {
        source_push(src, SourceChange::Add(row(&[
            ("id", ((seed + j) as i64).into()),
            ("n", ((j % 1000) as i64).into()),
            ("s", "y".into()),
        ])));
        for c in clients.iter_mut() {
            delivered += c.catch.borrow_mut().take_changes().len();
        }
    }
    delivered
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let clients: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(200);
    let changes: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(500);
    let cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
    let shards = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(cores);
    let seed = 1000usize;

    // Single core: one shard serving `clients` clients (steady-state, setup untimed).
    let (src, mut cs) = build_shard(seed, clients);
    let t = Instant::now();
    let acc = apply_changes(&src, &mut cs, seed, changes);
    let st = t.elapsed();
    std::hint::black_box(acc);
    let single_events = (clients * changes) as f64;
    let single_rate = single_events / st.as_secs_f64();

    // Per machine: `shards` shards (one per core), each serving `clients` clients.
    // A barrier starts every shard's steady-state loop together; each reports its
    // own elapsed and we take the slowest (the true wall of the parallel region).
    let barrier = Arc::new(std::sync::Barrier::new(shards));
    let handles: Vec<_> = (0..shards)
        .map(|_| {
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let (src, mut cs) = build_shard(seed, clients);
                barrier.wait();
                let t = Instant::now();
                let p = apply_changes(&src, &mut cs, seed, changes);
                (p, t.elapsed())
            })
        })
        .collect();
    let mut acc2 = 0usize;
    let mut mt = std::time::Duration::ZERO;
    for h in handles {
        let (p, e) = h.join().unwrap();
        acc2 += p;
        mt = mt.max(e);
    }
    std::hint::black_box(acc2);
    let multi_events = (shards * clients * changes) as f64;
    let multi_rate = multi_events / mt.as_secs_f64();

    println!("ORBIT single-node fan-out (sharded): clients/shard={clients} changes={changes} cores={cores} shards={shards}");
    println!(
        "  1 shard  (1 core)        {:>8.1} ms  ({:>12.0} client-fanouts/s, {} clients)",
        st.as_secs_f64() * 1e3,
        single_rate,
        clients
    );
    println!(
        "  {shards} shards (1 process)    {:>8.1} ms  ({:>12.0} client-fanouts/s, {} clients)",
        mt.as_secs_f64() * 1e3,
        multi_rate,
        shards * clients
    );
    println!("  scaling                  {:>8.2}x across {shards} cores", multi_rate / single_rate);
}
