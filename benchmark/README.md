# RivetDB Benchmarks

This directory contains a small benchmark harness for RivetDB. The executable
spins up a fresh 3-node cluster per run, seeds deterministic data when reads are
requested, and talks to the cluster via the Rust client. Storage uses on-disk
state (temp dirs by default) so restarts replay correctly; pass a mounted SSD
path if you want to place data elsewhere.

## Running the runner directly

`rivetdb-benchmark` accepts knobs for the basic workload:

```
cargo run --release --manifest-path benchmark/Cargo.toml -- \
  --label read_1000_c25 \
  --reads 1000 --writes 0 --commits 25 \
  --threads 5 \
  --runs 7 \
  --seed-data \
  --storage-root /mnt/ssd/rivetdb \
  --csv-dir benchmark/reports/csv
```

Key flags:
- `--label` names the experiment and the CSV file.
- `--reads/--writes/--commits` set operation counts; commit frequency is derived.
- `--threads` controls parallel clients (default 5).
- `--seed-data` preloads deterministic keys so reads have something to fetch.
- `--storage-root` points on-disk storage at a specific directory (e.g. SSD).
- `--kill-*` flags allow injecting a kill/restart during the run.

## Helper scripts

Scripts live in `benchmark/scripts` and all ultimately call `run_case.sh`. They
run each experiment 7 times and record the middle 5 in per-test CSV files.

- `read_suite.sh` — 1000 reads, commits {10,25,50,75,100}, seeds data.
- `write_suite.sh` — 1000 writes, commits {10,25,50,75,100}.
- `mixed_1000_ops_10_commits.sh` — 1000 ops, 10 commits, read ratios 0..100 step 10.
- `mixed_100_ops_100_commits.sh` — 100 ops, 100 commits, read ratios 0..100 step 10.
- `kill_restart_reads.sh` — kill/restart the leader mid-run (1000 reads, 100 commits).
- `kill_restart_writes.sh` — kill/restart the leader mid-run (1000 writes, 100 commits).
- `run_experiment.sh <script>` — compatibility shim to call any script above.
- `thread_scaling_suite.sh` — thread-count sweep (1..5) for 1000 reads, 1000 writes, and 500/500 mixed at 100 commits.

### Script configuration

Environment variables that apply to all scripts:
- `THREADS` (default 5) sets parallel client workers.
- `RUNS` (default 7) controls iteration count; the runner trims to the middle 5.
- `CSV_DIR` (default `benchmark/reports/csv`) changes the report destination.
- `STORAGE_ROOT` points to a directory for on-disk storage (e.g. SSD mount).
- `RUST_LOG` overrides logging (default `error,openraft=error` to keep the console clean).

Example: `STORAGE_ROOT=/mnt/ssd THREADS=8 RUNS=9 ./benchmark/scripts/read_suite.sh`.

CSV output is written per experiment under the configured `CSV_DIR`. Each run is
also printed to stdout along with the trimmed mean of the middle five runs.
