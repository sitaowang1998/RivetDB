use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rivetdb::{ClientConfig, ClientError, RivetClient};
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::{info, warn};

use crate::cluster::BenchmarkCluster;

const SEED_KEY_COUNT: usize = 200;
const SEED_BATCH_SIZE: usize = 50;
const VALUE_SIZE_BYTES: usize = 128;

#[derive(Clone, Debug)]
pub struct WorkloadConfig {
    pub read_ops: usize,
    pub write_ops: usize,
    pub commits: usize,
    pub threads: usize,
    pub requires_seed: bool,
    pub kill: Option<KillPlan>,
}

#[derive(Clone, Debug)]
pub struct KillPlan {
    pub at_op: usize,
    pub target: KillTarget,
    pub restart_delay: Duration,
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub enum KillTarget {
    Leader,
    Node(u64),
}

#[derive(Clone, Debug)]
pub struct RunMeasurement {
    pub duration: Duration,
    pub ops: usize,
    pub reads: usize,
    pub writes: usize,
    pub commits: usize,
    pub commit_every: usize,
    pub redirects: usize,
    pub failures: usize,
}

pub async fn run_workload(
    plan: &WorkloadConfig,
    run_idx: usize,
    storage_root: Option<&std::path::Path>,
    experiment: &str,
) -> Result<RunMeasurement> {
    if plan.commits == 0 {
        return Err(anyhow!("commits must be greater than zero"));
    }

    let total_ops = plan.read_ops + plan.write_ops;
    if total_ops == 0 {
        return Err(anyhow!("total operations must be greater than zero"));
    }

    let commit_every = total_ops.div_ceil(plan.commits).max(1);

    let cluster = Arc::new(Mutex::new(
        BenchmarkCluster::start(3, storage_root, experiment, run_idx).await?,
    ));

    let result = run_workload_inner(plan, run_idx, cluster.clone(), total_ops, commit_every).await;

    if let Err(err) = cluster.lock().await.shutdown().await {
        warn!(error = %err, "failed to shutdown cluster after workload");
    }

    result
}

async fn run_workload_inner(
    plan: &WorkloadConfig,
    run_idx: usize,
    cluster: Arc<Mutex<BenchmarkCluster>>,
    total_ops: usize,
    commit_every: usize,
) -> Result<RunMeasurement> {
    {
        let cluster = cluster.lock().await;
        cluster
            .wait_for_leader(Duration::from_secs(5))
            .await
            .context("leader election before workload")?;
    }

    let generator = Arc::new(Mutex::new(OperationGenerator::new(
        plan.read_ops,
        plan.write_ops,
        run_idx as u64,
    )));
    let ops_claimed = Arc::new(AtomicUsize::new(0));
    let ops_completed = Arc::new(AtomicUsize::new(0));
    let read_count = Arc::new(AtomicUsize::new(0));
    let write_count = Arc::new(AtomicUsize::new(0));
    let commit_count = Arc::new(AtomicUsize::new(0));
    let redirect_count = Arc::new(AtomicUsize::new(0));
    let failure_count = Arc::new(AtomicUsize::new(0));
    let kill_triggered = Arc::new(AtomicBool::new(false));
    let kill_lock = Arc::new(Mutex::new(()));

    if plan.requires_seed {
        seed_sample_data(&cluster, &redirect_count, &failure_count).await?;
    }

    let start = Instant::now();

    let threads = plan.threads.max(1);
    let mut handles = Vec::with_capacity(threads);
    for _ in 0..threads {
        let generator = generator.clone();
        let cluster = cluster.clone();
        let ops_claimed = ops_claimed.clone();
        let ops_completed = ops_completed.clone();
        let read_count = read_count.clone();
        let write_count = write_count.clone();
        let commit_count = commit_count.clone();
        let redirect_count = redirect_count.clone();
        let failure_count = failure_count.clone();
        let kill_plan = plan.kill.clone();
        let kill_triggered = kill_triggered.clone();
        let kill_lock = kill_lock.clone();

        let handle = tokio::spawn(async move {
            loop {
                let start_idx = ops_claimed.fetch_add(commit_every, Ordering::SeqCst);
                if start_idx >= total_ops {
                    break;
                }
                let batch_size = (total_ops - start_idx).min(commit_every);

                if let Some(ref kill) = kill_plan
                    && !kill_triggered.load(Ordering::SeqCst)
                    && start_idx >= kill.at_op
                {
                    let _guard = kill_lock.lock().await;
                    if !kill_triggered.load(Ordering::SeqCst) && start_idx >= kill.at_op {
                        info!(target = ?kill.target, op = start_idx, "injecting failure");
                        kill_triggered.store(true, Ordering::SeqCst);
                        kill_node(&cluster, kill).await?;
                    }
                }

                let mut batch = Vec::with_capacity(batch_size);
                {
                    let mut op_gen = generator.lock().await;
                    for i in 0..batch_size {
                        batch.push(op_gen.next(start_idx + i));
                    }
                }

                let batch_reads = batch
                    .iter()
                    .filter(|op| matches!(op, Operation::Read(_)))
                    .count();
                let batch_writes = batch.len() - batch_reads;

                execute_transaction(&cluster, &batch, &redirect_count, &failure_count).await?;
                read_count.fetch_add(batch_reads, Ordering::SeqCst);
                write_count.fetch_add(batch_writes, Ordering::SeqCst);
                commit_count.fetch_add(1, Ordering::SeqCst);
                ops_completed.fetch_add(batch_size, Ordering::SeqCst);
            }
            Ok::<(), anyhow::Error>(())
        });
        handles.push(handle);
    }

    for handle in handles {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => return Err(err),
            Err(join_err) => return Err(anyhow!("worker panicked: {join_err}")),
        }
    }

    let completed = ops_completed.load(Ordering::SeqCst);
    if completed != total_ops {
        warn!(
            completed,
            expected = total_ops,
            "workload completed a different number of operations than planned"
        );
    }

    let duration = start.elapsed();

    Ok(RunMeasurement {
        duration,
        ops: total_ops,
        reads: read_count.load(Ordering::SeqCst),
        writes: write_count.load(Ordering::SeqCst),
        commits: commit_count.load(Ordering::SeqCst),
        commit_every,
        redirects: redirect_count.load(Ordering::SeqCst),
        failures: failure_count.load(Ordering::SeqCst),
    })
}

async fn seed_sample_data(
    cluster: &Arc<Mutex<BenchmarkCluster>>,
    redirects: &Arc<AtomicUsize>,
    failures: &Arc<AtomicUsize>,
) -> Result<()> {
    let mut remaining = SEED_KEY_COUNT;
    let mut cursor = 0;

    while remaining > 0 {
        let batch = remaining.min(SEED_BATCH_SIZE);
        let operations = (0..batch)
            .map(|_| {
                let key = seed_key(cursor);
                cursor += 1;
                Operation::Write(key, sample_value(cursor))
            })
            .collect::<Vec<_>>();

        execute_transaction(cluster, &operations, redirects, failures).await?;
        remaining -= batch;
    }

    Ok(())
}

async fn kill_node(cluster: &Arc<Mutex<BenchmarkCluster>>, plan: &KillPlan) -> Result<()> {
    let mut cluster = cluster.lock().await;
    let target_id = match plan.target {
        KillTarget::Leader => cluster
            .leader_id()
            .await?
            .ok_or_else(|| anyhow!("no leader to kill"))?,
        KillTarget::Node(id) => id,
    };

    cluster.kill_node(target_id).await?;
    sleep(plan.restart_delay).await;
    cluster.restart_node(target_id).await?;
    cluster
        .wait_for_leader(Duration::from_secs(10))
        .await
        .context("leader election after restart")?;
    Ok(())
}

async fn execute_transaction(
    cluster: &Arc<Mutex<BenchmarkCluster>>,
    ops: &[Operation],
    redirects: &Arc<AtomicUsize>,
    failures: &Arc<AtomicUsize>,
) -> Result<()> {
    let mut attempts = 0;
    let mut last_redirect_endpoint: Option<String> = None;
    const MAX_ATTEMPTS: usize = 500;
    loop {
        attempts += 1;
        if attempts > MAX_ATTEMPTS {
            failures.fetch_add(1, Ordering::SeqCst);
            return Err(anyhow!(
                "transaction exceeded max attempts ({MAX_ATTEMPTS}), last seen redirect/failure"
            ));
        }
        let endpoint = preferred_endpoint(cluster, last_redirect_endpoint.as_deref()).await?;
        let client = RivetClient::connect(ClientConfig::new(endpoint.clone())).await;
        let client = match client {
            Ok(client) => client,
            Err(err) => {
                if attempts % 5 == 0 {
                    warn!(
                        endpoint = endpoint,
                        attempt = attempts,
                        "connect failed: {err}"
                    );
                }
                sleep(Duration::from_millis(100)).await;
                continue;
            }
        };

        match run_txn(&client, ops).await {
            Ok(()) => return Ok(()),
            Err(err) => {
                if is_redirectable(&err) {
                    redirects.fetch_add(1, Ordering::SeqCst);
                    last_redirect_endpoint = Some(endpoint);
                    sleep(Duration::from_millis(50)).await;
                    continue;
                }

                if attempts % 5 == 0 {
                    warn!(
                        attempt = attempts,
                        "retrying transaction after error: {err}"
                    );
                }

                sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

async fn run_txn(client: &RivetClient, ops: &[Operation]) -> Result<(), ClientError> {
    let txn = client.begin_transaction("benchmark").await?;

    for op in ops {
        match op {
            Operation::Read(key) => {
                let result = txn.get(key.clone()).await?;
                if result.is_none() {
                    return Err(ClientError::OperationFailed {
                        operation: "get",
                        message: format!("missing key {key}"),
                    });
                }
            }
            Operation::Write(key, value) => {
                txn.put(key.clone(), value.clone()).await?;
            }
        }
    }

    txn.commit().await?;
    Ok(())
}

fn is_redirectable(err: &ClientError) -> bool {
    match err {
        ClientError::OperationFailed { message, .. } => {
            message.to_ascii_lowercase().contains("catching up")
                || message
                    .to_ascii_lowercase()
                    .contains("retry against another node")
                || message.to_ascii_lowercase().contains("missing key")
        }
        ClientError::Rpc(status) => status.code() == tonic::Code::Unavailable,
        _ => false,
    }
}

async fn preferred_endpoint(
    cluster: &Arc<Mutex<BenchmarkCluster>>,
    exclude: Option<&str>,
) -> Result<String> {
    const ATTEMPTS: usize = 50;
    const DELAY_MS: u64 = 100;

    for _ in 0..ATTEMPTS {
        let endpoint = {
            let cluster = cluster.lock().await;
            if let Some(leader) = cluster.leader_endpoint().await {
                Some(leader)
            } else {
                let endpoints = cluster.endpoints();
                if endpoints.is_empty() {
                    None
                } else if let Some(ex) = exclude {
                    endpoints
                        .into_iter()
                        .find(|ep| ep != ex)
                        .or_else(|| cluster.any_endpoint())
                } else {
                    cluster.any_endpoint()
                }
            }
        };
        if let Some(endpoint) = endpoint {
            return Ok(endpoint);
        }
        sleep(Duration::from_millis(DELAY_MS)).await;
    }

    Err(anyhow!(
        "no endpoint available after waiting {} ms",
        ATTEMPTS as u64 * DELAY_MS
    ))
}

#[derive(Clone, Debug)]
enum Operation {
    Read(String),
    Write(String, Vec<u8>),
}

struct OperationGenerator {
    remaining_reads: usize,
    remaining_writes: usize,
    rng: StdRng,
    write_index: usize,
}

impl OperationGenerator {
    fn new(read_ops: usize, write_ops: usize, seed: u64) -> Self {
        Self {
            remaining_reads: read_ops,
            remaining_writes: write_ops,
            rng: StdRng::seed_from_u64(seed_from(read_ops as f32, seed)),
            write_index: 0,
        }
    }

    fn next(&mut self, op_index: usize) -> Operation {
        if self.remaining_reads > 0 && (self.remaining_writes == 0 || self.rng.r#gen::<f32>() < 0.5)
        {
            self.remaining_reads -= 1;
            Operation::Read(seed_key(op_index % SEED_KEY_COUNT))
        } else {
            self.remaining_writes = self.remaining_writes.saturating_sub(1);
            self.next_write(op_index)
        }
    }

    fn next_write(&mut self, op_index: usize) -> Operation {
        let key = format!("write-{op_index}-{counter}", counter = self.write_index);
        self.write_index += 1;
        Operation::Write(key, sample_value(self.write_index))
    }
}

fn seed_key(index: usize) -> String {
    format!("seed-{index:04}")
}

fn sample_value(offset: usize) -> Vec<u8> {
    vec![(offset % 251) as u8; VALUE_SIZE_BYTES]
}

fn seed_from(read_ratio: f32, run_idx: u64) -> u64 {
    let mut hasher = DefaultHasher::new();
    read_ratio.to_bits().hash(&mut hasher);
    run_idx.hash(&mut hasher);
    hasher.finish()
}
