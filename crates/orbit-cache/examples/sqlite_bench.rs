//! SQLite-source benchmark: the scaling paths a durable replica actually runs —
//! a limit-query hydrate over a large table, join-child (constrained) fetches at
//! fan-in, and steady-state pushes. Mirrors the shapes of `oql/examples/bench.rs`
//! but over [`SqliteReplica`], so SQL pushdown (or the lack of it) is what's
//! measured.
//!
//!   cargo run --release --example sqlite_bench -p orbit-cache -- <N> <M>

use std::rc::Rc;
use std::time::Instant;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use oql::ivm::operator::Link;
use oql::ivm::{Catch, ColumnType};
use oql::value::{Row, Value};
use orbit_cache::sqlite_source::{source_push, SqliteReplica};
use oql::ivm::SourceChange;

fn row(pairs: &[(&str, Value)]) -> Row {
    pairs.iter().map(|(k, v)| (*k, v.clone())).collect()
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let n: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(100_000);
    let m: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(10_000);

    // --- take: `t ORDER BY n LIMIT 100` over N rows -------------------------
    let mut replica = SqliteReplica::in_memory();
    let t_src = replica.add_table(
        "t",
        vec![("id".into(), ColumnType::Number), ("n".into(), ColumnType::Number)],
        vec!["id".into()],
    );
    let t = Instant::now();
    for i in 0..n {
        t_src.borrow().insert_initial(&row(&[
            ("id", (i as i64).into()),
            ("n", (((i * 7919) % 1_000_000) as i64).into()),
        ]));
    }
    let load = t.elapsed();

    let ast: oql::ast::Ast = serde_json::from_value(serde_json::json!({
        "table": "t", "orderBy": [["n", "asc"]], "limit": 100,
    }))
    .unwrap();
    let t = Instant::now();
    let top = oql::build_pipeline(&ast, &replica);
    let catch = Catch::new(top.input.clone());
    let link: Link = catch.clone();
    top.set_output(link);
    let count = catch.borrow().fetch().len();
    let hydrate = t.elapsed();

    let t = Instant::now();
    for j in 0..m {
        source_push(&t_src, SourceChange::Add(row(&[
            ("id", ((n + j) as i64).into()),
            ("n", (((j * 104_729) % 1_000_000) as i64).into()),
        ])));
        catch.borrow_mut().take_changes();
    }
    let push = t.elapsed();
    println!("SQLITE take   N={n} M={m} matched={count}");
    println!("  load     {:8.1} ms ({:9.0} rows/s)", load.as_secs_f64() * 1e3, n as f64 / load.as_secs_f64());
    println!("  hydrate  {:8.1} ms", hydrate.as_secs_f64() * 1e3);
    println!("  push     {:8.1} ms ({:9.0} pushes/s)", push.as_secs_f64() * 1e3, m as f64 / push.as_secs_f64());

    // --- join: issue related comments, fan-in n_issues=100 ------------------
    let mut replica = SqliteReplica::in_memory();
    let issues = replica.add_table("issue", vec![("id".into(), ColumnType::Number)], vec!["id".into()]);
    let comments = replica.add_table(
        "comment",
        vec![("id".into(), ColumnType::Number), ("issueID".into(), ColumnType::Number)],
        vec!["id".into()],
    );
    let n_issues = 100usize;
    let t = Instant::now();
    for i in 0..n_issues {
        issues.borrow().insert_initial(&row(&[("id", (i as i64).into())]));
    }
    for i in 0..n {
        comments.borrow().insert_initial(&row(&[
            ("id", (i as i64).into()),
            ("issueID", ((i % n_issues) as i64).into()),
        ]));
    }
    let jload = t.elapsed();

    let ast: oql::ast::Ast = serde_json::from_value(serde_json::json!({
        "table": "issue",
        "orderBy": [["id", "asc"]],
        "related": [{
            "correlation": {"parentField": ["id"], "childField": ["issueID"]},
            "subquery": {"table": "comment", "alias": "comments", "orderBy": [["id", "asc"]]}
        }]
    }))
    .unwrap();
    let t = Instant::now();
    let top = oql::build_pipeline(&ast, &replica);
    let catch = Catch::new(top.input.clone());
    let link: Link = catch.clone();
    top.set_output(link);
    let jcount = catch.borrow().fetch().len();
    let jhydrate = t.elapsed();

    let t = Instant::now();
    for j in 0..m {
        source_push(&comments, SourceChange::Add(row(&[
            ("id", ((n + j) as i64).into()),
            ("issueID", ((j % n_issues) as i64).into()),
        ])));
        catch.borrow_mut().take_changes();
    }
    let jpush = t.elapsed();
    println!("SQLITE join   N={n} M={m} parents={jcount} (fan-in ~{})", n / n_issues);
    println!("  load     {:8.1} ms ({:9.0} rows/s)", jload.as_secs_f64() * 1e3, n as f64 / jload.as_secs_f64());
    println!("  hydrate  {:8.1} ms", jhydrate.as_secs_f64() * 1e3);
    println!("  push     {:8.1} ms ({:9.0} pushes/s)", jpush.as_secs_f64() * 1e3, m as f64 / jpush.as_secs_f64());
}
