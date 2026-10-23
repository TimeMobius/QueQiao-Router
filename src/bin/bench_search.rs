//! Read-only, in-process comparison of the shipped V7 and current V8 search SQL.
use std::io;
use std::time::Instant;

use queqiao_router::db::engine::{Db, DbConn, OpenOptions, Value};
use serde_json::json;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Row, SqlitePool};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
#[path = "bench_search/plan.rs"]
mod plans;
use plans::{plan, Case, Plan, Workload};

struct Args {
    source: String,
    target: String,
    iters: usize,
    filter: Option<String>,
}

fn args() -> Result<Args> {
    let mut input = std::env::args().skip(1);
    let (mut source, mut target, mut filter) = (None, None, None);
    let mut iters = 5;
    while let Some(flag) = input.next() {
        let value = input
            .next()
            .ok_or_else(|| io::Error::other("missing argument value"))?;
        match flag.as_str() {
            "--source" => source = Some(value),
            "--target" => target = Some(value),
            "--iters" => iters = value.parse()?,
            "--case" => filter = Some(value),
            _ => return Err(io::Error::other(format!("unknown argument: {flag}")).into()),
        }
    }
    if iters == 0 {
        return Err(io::Error::other("--iters must be positive").into());
    }
    Ok(Args {
        source: source.ok_or_else(|| io::Error::other("--source is required"))?,
        target: target.ok_or_else(|| io::Error::other("--target is required"))?,
        iters,
        filter,
    })
}

async fn source_query(pool: &SqlitePool, plan: &Plan) -> Result<Vec<i64>> {
    let mut query = sqlx::query(&plan.sql);
    for value in &plan.params {
        query = match value {
            Value::Text(value) => query.bind(value),
            Value::Integer(value) => query.bind(*value),
            Value::Real(value) => query.bind(*value),
            Value::Blob(value) => query.bind(value),
            Value::Null => query.bind(Option::<String>::None),
        };
    }
    let rows = query.fetch_all(pool).await?;
    let mut output = Vec::with_capacity(rows.len());
    for row in rows {
        output.push(row.try_get(0)?);
    }
    Ok(output)
}

async fn target_query(conn: &DbConn, plan: &Plan) -> Result<Vec<i64>> {
    let rows = conn.query(&plan.sql, plan.params.clone()).await?;
    let mut output = Vec::with_capacity(rows.len());
    for row in rows {
        output.push(row.get_i64(0)?);
    }
    Ok(output)
}

fn stats(samples: &[f64]) -> Result<serde_json::Value> {
    let mut sorted = samples.to_vec();
    sorted.sort_by(f64::total_cmp);
    let median = sorted
        .get(sorted.len() / 2)
        .ok_or_else(|| io::Error::other("no samples"))?;
    Ok(json!({"samples_ms": samples, "median_ms": median, "max_ms": sorted.last()}))
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = args()?;
    let source = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(&args.source)
                .read_only(true),
        )
        .await?;
    let db = Db::open(
        &args.target,
        OpenOptions {
            read_only: true,
            ..OpenOptions::default()
        },
    )
    .await?;
    let target = db.connect()?;
    let version: String = sqlx::query_scalar("SELECT sqlite_version()")
        .fetch_one(&source)
        .await?;
    let max_ms: i64 = sqlx::query_scalar("SELECT MAX(TimeMs) FROM records")
        .fetch_one(&source)
        .await?;
    let model: String = sqlx::query_scalar("SELECT Model FROM records WHERE Model IS NOT NULL AND Model != '' ORDER BY id DESC LIMIT 1").fetch_one(&source).await?;
    println!(
        "{}",
        json!({"metadata": {"source": args.source, "target": args.target, "sqlite_version": version, "turso_version": "0.8.3", "iterations": args.iters, "max_time_ms": max_ms, "model_prefix": model, "warmup_runs": 1, "timing": "prepare + execute + fetch all + decode first column; HTTP and JSON rendering excluded"}})
    );
    let mut cases: Vec<Case<'_>> = [
        "模",
        "的",
        "模型",
        "错误",
        "用户",
        "图片",
        "数据库",
        "请求头",
        "错误码",
        "上下文",
        "函数调用",
        "系统提示",
        "数据库连接",
        "模型调用",
        "鹊桥中文检索不存在甲乙丙",
        "模型API",
        "数据库 连接",
        "数据库 错误码",
    ]
    .into_iter()
    .map(|q| Case {
        name: q,
        q,
        days: None,
        status: None,
        model: None,
    })
    .collect();
    cases.extend([
        Case {
            name: "数据库/最近1天",
            q: "数据库",
            days: Some(1),
            status: None,
            model: None,
        },
        Case {
            name: "数据库/最近7天",
            q: "数据库",
            days: Some(7),
            status: None,
            model: None,
        },
        Case {
            name: "模型/最近7天",
            q: "模型",
            days: Some(7),
            status: None,
            model: None,
        },
        Case {
            name: "数据库/200",
            q: "数据库",
            days: None,
            status: Some(200),
            model: None,
        },
        Case {
            name: "数据库/模型前缀",
            q: "数据库",
            days: None,
            status: None,
            model: Some(&model),
        },
    ]);
    for case in cases {
        if args
            .filter
            .as_ref()
            .is_some_and(|filter| !case.name.contains(filter))
        {
            continue;
        }
        for workload in [Workload::Count, Workload::List, Workload::CappedCount] {
            let sp = plan(&case, workload, false, max_ms);
            let tp = plan(&case, workload, true, max_ms);
            let start = Instant::now();
            let expected_source = source_query(&source, &sp).await?;
            let source_warmup = start.elapsed().as_secs_f64() * 1000.0;
            let start = Instant::now();
            let expected_target = target_query(&target, &tp).await?;
            let target_warmup = start.elapsed().as_secs_f64() * 1000.0;
            let (mut st, mut tt) = (Vec::new(), Vec::new());
            for index in 0..args.iters {
                if index % 2 == 0 {
                    let start = Instant::now();
                    if source_query(&source, &sp).await? != expected_source {
                        return Err(io::Error::other("unstable source results").into());
                    }
                    st.push(start.elapsed().as_secs_f64() * 1000.0);
                }
                let start = Instant::now();
                if target_query(&target, &tp).await? != expected_target {
                    return Err(io::Error::other("unstable target results").into());
                }
                tt.push(start.elapsed().as_secs_f64() * 1000.0);
                if index % 2 != 0 {
                    let start = Instant::now();
                    if source_query(&source, &sp).await? != expected_source {
                        return Err(io::Error::other("unstable source results").into());
                    }
                    st.push(start.elapsed().as_secs_f64() * 1000.0);
                }
            }
            println!(
                "{}",
                json!({"case": case.name, "q": case.q, "workload": format!("{workload:?}"), "source": stats(&st)?, "target": stats(&tt)?, "source_first_ms": source_warmup, "target_first_ms": target_warmup, "source_result": expected_source, "target_result": expected_target, "results_equal": expected_source == expected_target, "source_sql": sp.sql, "target_sql": tp.sql})
            );
        }
    }
    Ok(())
}
