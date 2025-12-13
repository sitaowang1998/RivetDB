mod cluster;
mod workload;

use std::fs::{File, create_dir_all};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use serde::Serialize;
use tracing_subscriber::EnvFilter;

use workload::{KillPlan, KillTarget, RunMeasurement, WorkloadConfig};

#[derive(Parser, Debug)]
#[command(name = "rivetdb-benchmark")]
struct Args {
    /// Label for this run (used in output).
    #[arg(long, default_value = "run")]
    label: String,

    /// Number of read operations to issue.
    #[arg(long, default_value_t = 0)]
    reads: usize,

    /// Number of write operations to issue.
    #[arg(long, default_value_t = 0)]
    writes: usize,

    /// Number of commits to perform (commit_every is derived).
    #[arg(long, default_value_t = 1)]
    commits: usize,

    /// Number of parallel client workers to run.
    #[arg(long, default_value_t = 5)]
    threads: usize,

    /// Directory to write per-experiment CSV files.
    #[arg(long, default_value = "benchmark/reports/csv")]
    csv_dir: PathBuf,

    /// Optional root directory to place disk storage (e.g. mounted SSD).
    #[arg(long)]
    storage_root: Option<PathBuf>,

    /// Seed deterministic data before running (recommended when reads > 0).
    #[arg(long, default_value_t = false)]
    seed_data: bool,

    /// Optional op index at which to kill/restart a node.
    #[arg(long)]
    kill_at: Option<usize>,

    /// Kill target (`leader` or `node`).
    #[arg(long, value_enum, default_value_t = KillTargetArg::Leader)]
    kill_target: KillTargetArg,

    /// Node id to kill when using `--kill-target node`.
    #[arg(long, default_value_t = 1)]
    kill_node_id: u64,

    /// Delay in ms before restarting a killed node.
    #[arg(long, default_value_t = 250)]
    restart_delay_ms: u64,

    /// Number of times to run each experiment (trimmed to the middle 5 when >=7).
    #[arg(long, default_value_t = 7)]
    runs: usize,
}

#[derive(Serialize)]
struct RunSnapshot {
    run: usize,
    duration_ms: u128,
    ops: usize,
    reads: usize,
    writes: usize,
    commits: usize,
    commit_every: usize,
    ops_per_sec: f64,
}

#[derive(Serialize)]
struct TrimmedStats {
    runs_used: usize,
    mean_ms: f64,
    ops_per_sec: f64,
    durations_ms: Vec<u128>,
}

#[derive(Serialize)]
struct ExperimentReport {
    name: String,
    description: String,
    runs: Vec<RunSnapshot>,
    trimmed: TrimmedStats,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")),
        )
        .init();

    let kill = args.kill_at.map(|at| KillPlan {
        at_op: at,
        target: args.kill_target.to_target(args.kill_node_id),
        restart_delay: Duration::from_millis(args.restart_delay_ms),
    });

    let workload = WorkloadConfig {
        read_ops: args.reads,
        write_ops: args.writes,
        commits: args.commits,
        threads: args.threads,
        requires_seed: args.seed_data,
        kill,
    };

    println!("=== {} ===", args.label);
    let mut runs = Vec::new();
    for run_idx in 0..args.runs {
        let measurement = workload::run_workload(
            &workload,
            run_idx,
            args.storage_root.as_deref(),
            &args.label,
        )
        .await?;
        println!(
            "  run {:>2}: {:>8.2?} ({:.1} ops/s, reads={}, writes={}, commits={})",
            run_idx + 1,
            measurement.duration,
            measurement.ops as f64 / measurement.duration.as_secs_f64(),
            measurement.reads,
            measurement.writes,
            measurement.commits
        );
        runs.push(measurement);
    }

    let report = summarize(&args.label, &runs);
    print_trimmed(&args.label, &report.trimmed);
    write_experiment_csv(&args.csv_dir, &args.label, &report.runs, &report.trimmed)?;

    Ok(())
}

fn summarize(label: &str, runs: &[RunMeasurement]) -> ExperimentReport {
    let run_snapshots = runs
        .iter()
        .enumerate()
        .map(|(idx, run)| RunSnapshot {
            run: idx + 1,
            duration_ms: run.duration.as_millis(),
            ops: run.ops,
            reads: run.reads,
            writes: run.writes,
            commits: run.commits,
            commit_every: run.commit_every,
            ops_per_sec: run.ops as f64 / run.duration.as_secs_f64(),
        })
        .collect::<Vec<_>>();

    ExperimentReport {
        name: label.to_string(),
        description: String::new(),
        runs: run_snapshots,
        trimmed: trim_runs(runs),
    }
}

fn trim_runs(runs: &[RunMeasurement]) -> TrimmedStats {
    if runs.is_empty() {
        return TrimmedStats {
            runs_used: 0,
            mean_ms: 0.0,
            ops_per_sec: 0.0,
            durations_ms: Vec::new(),
        };
    }

    let mut ordered = runs
        .iter()
        .map(|run| {
            (
                run.duration.as_millis(),
                run.ops as f64 / run.duration.as_secs_f64(),
            )
        })
        .collect::<Vec<_>>();
    ordered.sort_by_key(|(d, _)| *d);

    let slice: Vec<(u128, f64)> = if ordered.len() > 2 {
        ordered[1..ordered.len() - 1].to_vec()
    } else {
        ordered
    };

    let durations_ms = slice.iter().map(|(d, _)| *d).collect::<Vec<_>>();
    let mean_ms = durations_ms.iter().copied().sum::<u128>() as f64 / (durations_ms.len() as f64);
    let ops_per_sec = slice.iter().map(|(_, ops)| *ops).sum::<f64>() / (slice.len() as f64);

    TrimmedStats {
        runs_used: slice.len(),
        mean_ms,
        ops_per_sec,
        durations_ms,
    }
}

fn print_trimmed(_name: &str, stats: &TrimmedStats) {
    println!(
        "  trimmed (middle {}): mean {:.2} ms, {:.1} ops/s",
        stats.runs_used, stats.mean_ms, stats.ops_per_sec
    );
    println!("  middle durations (ms): {:?}", stats.durations_ms);
    println!();
}

fn write_experiment_csv(
    dir: &Path,
    name: &str,
    runs: &[RunSnapshot],
    trimmed: &TrimmedStats,
) -> Result<()> {
    create_dir_all(dir)?;
    let path = dir.join(format!("{name}.csv"));
    let mut file = File::create(&path)?;
    writeln!(
        file,
        "kind,run,duration_ms,ops,reads,writes,commits,commit_every,ops_per_sec"
    )?;

    for run in runs {
        writeln!(
            file,
            "run,{},{},{},{},{},{},{},{}",
            run.run,
            run.duration_ms,
            run.ops,
            run.reads,
            run.writes,
            run.commits,
            run.commit_every,
            run.ops_per_sec
        )?;
    }

    if !trimmed.durations_ms.is_empty() {
        let ops = runs.first().map(|r| r.ops).unwrap_or(0);
        writeln!(
            file,
            "trimmed_mean,,{:.2},{ops},,,,{:.3}",
            trimmed.mean_ms, trimmed.ops_per_sec
        )?;
        writeln!(
            file,
            "trimmed_durations,,\"{}\",,,,,",
            trimmed
                .durations_ms
                .iter()
                .map(|d| d.to_string())
                .collect::<Vec<_>>()
                .join(";")
        )?;
    }

    println!("    wrote {}", path.display());
    Ok(())
}
#[derive(Copy, Clone, Debug, clap::ValueEnum)]
enum KillTargetArg {
    Leader,
    Node,
}

impl KillTargetArg {
    fn to_target(self, node_id: u64) -> KillTarget {
        match self {
            KillTargetArg::Leader => KillTarget::Leader,
            KillTargetArg::Node => KillTarget::Node(node_id),
        }
    }
}
