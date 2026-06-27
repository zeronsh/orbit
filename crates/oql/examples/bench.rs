//! Orbit IVM micro-benchmark. Mirrors `mono/orbit-golden/bench.ts` (Zero) so the
//! two engines run the *same* workload. Measures: initial load, hydration
//! (build + first fetch), and incremental push throughput. Peak RSS is captured
//! by running this under `/usr/bin/time -l` (macOS) / `-v` (GNU).
//!
//!   cargo run --release --example bench -p oql -- <filter|join> <N> <M>

use std::collections::BTreeMap;
use std::rc::Rc;
use std::time::Instant;

use oql::ast::Direction as AstDir;
use oql::ivm::operator::{Link, OpHandle};
use oql::ivm::{
    connect, source_push, Catch, ColumnType, Filter, Join, MemorySource, Predicate, SourceChange,
};
use oql::value::{Row, Value};

fn row(pairs: &[(&str, Value)]) -> Row {
    // Interned, alloc-free keys (the production hot path).
    pairs.iter().map(|(k, v)| (*k, v.clone())).collect()
}
fn cols(names: &[(&str, ColumnType)]) -> BTreeMap<String, ColumnType> {
    names.iter().map(|(n, t)| (n.to_string(), *t)).collect()
}
fn asc(f: &str) -> Vec<(String, AstDir)> {
    vec![(f.to_string(), AstDir::Asc)]
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let workload = args.get(1).cloned().unwrap_or_else(|| "filter".into());
    let n: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(100_000);
    let m: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(100_000);

    match workload.as_str() {
        "filter" => filter_bench(n, m),
        "join" => join_bench(n, m),
        // mt <queries> <pushes-per-query> [threads]
        "mt" => mt_bench(n, m, args.get(4).and_then(|s| s.parse().ok())),
        other => eprintln!("unknown workload {other}"),
    }
}

/// One independent query pipeline (source -> filter -> catch), seeded with
/// `seed` rows, then driven by `pushes` incremental adds. Returns the matched
/// count (so the work can't be optimized away). Entirely thread-local: the
/// `Rc`/`RefCell` graph never crosses a thread boundary.
fn run_one_query(seed: usize, pushes: usize) -> usize {
    let src = MemorySource::new(
        "t",
        cols(&[("id", ColumnType::Number), ("n", ColumnType::Number)]),
        vec!["id".into()],
    );
    for i in 0..seed {
        src.borrow_mut()
            .insert_initial(row(&[("id", (i as i64).into()), ("n", ((i % 1000) as i64).into())]));
    }
    let conn = OpHandle::new(connect(&src, asc("id")));
    let pred: Predicate = Rc::new(|r| matches!(r.get("n"), Some(Value::Number(x)) if *x >= 500.0));
    let filter = Filter::new(conn, pred);
    let fh = OpHandle::new(filter);
    let catch = Catch::new(fh.input.clone());
    let link: Link = catch.clone();
    fh.set_output(link);
    let mut count = catch.borrow().fetch().len();
    for j in 0..pushes {
        source_push(
            &src,
            SourceChange::Add(row(&[("id", ((seed + j) as i64).into()), ("n", ((j % 1000) as i64).into())])),
        );
        count += catch.borrow_mut().take_changes().len();
    }
    count
}

/// Throughput scaling: run `queries` independent pipelines (each with `pushes`
/// incremental updates) single-threaded, then sharded across a thread pool.
/// Demonstrates the structural win Zero can't have: independent query pipelines
/// run on every core with no shared state and no locks.
fn mt_bench(queries: usize, pushes: usize, threads: Option<usize>) {
    let cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
    let threads = threads.unwrap_or(cores);
    let seed = 1000usize;
    let total_pushes = (queries * pushes) as f64;

    // Single-threaded baseline.
    let t = Instant::now();
    let mut acc = 0usize;
    for _ in 0..queries {
        acc += run_one_query(seed, pushes);
    }
    let st = t.elapsed();
    std::hint::black_box(acc);

    // Sharded across `threads` OS threads. Each thread builds and owns its own
    // pipelines (thread-local `Rc`); no synchronization on the hot path.
    let t = Instant::now();
    let per = queries.div_ceil(threads);
    let mut handles = Vec::new();
    let mut remaining = queries;
    for _ in 0..threads {
        let q = per.min(remaining);
        remaining -= q;
        handles.push(std::thread::spawn(move || {
            let mut a = 0usize;
            for _ in 0..q {
                a += run_one_query(seed, pushes);
            }
            a
        }));
        if remaining == 0 {
            break;
        }
    }
    let mut acc2 = 0usize;
    for h in handles {
        acc2 += h.join().unwrap();
    }
    let mt = t.elapsed();
    std::hint::black_box(acc2);

    let st_tp = total_pushes / st.as_secs_f64();
    let mt_tp = total_pushes / mt.as_secs_f64();
    println!("ORBIT mt queries={queries} pushes/query={pushes} cores={cores} threads={threads}");
    println!("  single-thread {:>8.1} ms ({:>10.0} pushes/s)", st.as_secs_f64() * 1e3, st_tp);
    println!("  {threads}-thread     {:>8.1} ms ({:>10.0} pushes/s)", mt.as_secs_f64() * 1e3, mt_tp);
    println!("  scaling       {:>8.2}x", mt_tp / st_tp);
}

fn filter_bench(n: usize, m: usize) {
    let src = MemorySource::new(
        "t",
        cols(&[("id", ColumnType::Number), ("n", ColumnType::Number), ("s", ColumnType::String)]),
        vec!["id".into()],
    );

    // Initial load (no connections yet).
    let t = Instant::now();
    for i in 0..n {
        source_push(&src, SourceChange::Add(row(&[
            ("id", (i as i64).into()),
            ("n", ((i % 1000) as i64).into()),
            ("s", "x".into()),
        ])));
    }
    let load = t.elapsed();

    // Build: source -> filter(n >= 500) -> catch.
    let conn = OpHandle::new(connect(&src, asc("id")));
    let pred: Predicate = Rc::new(|r| matches!(r.get("n"), Some(Value::Number(x)) if *x >= 500.0));
    let filter = Filter::new(conn, pred);
    let fh = OpHandle::new(filter);
    let catch = Catch::new(fh.input.clone());
    let link: Link = catch.clone();
    fh.set_output(link);

    let t = Instant::now();
    let count = catch.borrow().fetch().len();
    let hydrate = t.elapsed();

    // Incremental pushes (drain the catch each iteration).
    let t = Instant::now();
    for j in 0..m {
        source_push(&src, SourceChange::Add(row(&[
            ("id", ((n + j) as i64).into()),
            ("n", ((j % 1000) as i64).into()),
            ("s", "y".into()),
        ])));
        catch.borrow_mut().take_changes();
    }
    let push = t.elapsed();

    report("filter", n, m, count, load, hydrate, push);
}

fn join_bench(n: usize, m: usize) {
    let issues = MemorySource::new("issue", cols(&[("id", ColumnType::Number)]), vec!["id".into()]);
    let comments = MemorySource::new(
        "comment",
        cols(&[("id", ColumnType::Number), ("issueID", ColumnType::Number)]),
        vec!["id".into()],
    );

    let t = Instant::now();
    for i in 0..n {
        source_push(&issues, SourceChange::Add(row(&[("id", (i as i64).into())])));
    }
    for i in 0..n {
        source_push(&comments, SourceChange::Add(row(&[
            ("id", (i as i64).into()),
            ("issueID", ((i % n.max(1)) as i64).into()),
        ])));
    }
    let load = t.elapsed();

    let join = Join::new(
        OpHandle::new(connect(&issues, asc("id"))),
        OpHandle::new(connect(&comments, asc("id"))),
        vec!["id".into()],
        vec!["issueID".into()],
        "comments",
        false,
        oql::ast::System::Client,
    );
    let jh = OpHandle::new(join);
    let catch = Catch::new(jh.input.clone());
    let link: Link = catch.clone();
    jh.set_output(link);

    let t = Instant::now();
    let count = catch.borrow().fetch().len();
    let hydrate = t.elapsed();

    let t = Instant::now();
    for j in 0..m {
        source_push(&comments, SourceChange::Add(row(&[
            ("id", ((n + j) as i64).into()),
            ("issueID", ((j % n.max(1)) as i64).into()),
        ])));
        catch.borrow_mut().take_changes();
    }
    let push = t.elapsed();

    report("join", n, m, count, load, hydrate, push);
}

fn report(
    workload: &str,
    n: usize,
    m: usize,
    count: usize,
    load: std::time::Duration,
    hydrate: std::time::Duration,
    push: std::time::Duration,
) {
    println!("ORBIT {workload} N={n} M={m} matched={count}");
    println!("  load     {:>8.1} ms ({:>9.0} rows/s)", load.as_secs_f64() * 1e3, n as f64 / load.as_secs_f64());
    println!("  hydrate  {:>8.1} ms", hydrate.as_secs_f64() * 1e3);
    println!("  push     {:>8.1} ms ({:>9.0} pushes/s)", push.as_secs_f64() * 1e3, m as f64 / push.as_secs_f64());
}
