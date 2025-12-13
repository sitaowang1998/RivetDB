# RivetDB Benchmarks

This directory contains a small benchmark harness for RivetDB. It spins up a
fresh in-process 3-node cluster for each run, seeds deterministic data for any
read-heavy tests, and exercises the RPC surface via the Rust client. Each node
uses an on-disk data directory (temp-backed by default) so restarts replay
state. You can point the storage at a mounted SSD with `--storage-root`.

## Experiments

The runner encodes the following scenarios (each runs 7 times by default and
reports the middle 5):

- 1000 reads; commit every 10 and 100 operations.
- 1000 writes; commit every 10 and 100 operations.
- Mixed workload; 1000 operations, 10 commits; read ratios 25%, 50%, 75%.
- Mixed workload; 100 operations, 100 commits; read ratios 25%, 50%, 75%.
- Kill and restart a server midway through 1000 reads (100 commits).
- Kill and restart a server midway through 1000 writes (100 commits).

## Running

Use the scripts to run experiments (each run executes one experiment 7 times and
takes the middle 5):

```bash
# run a single experiment
./benchmark/scripts/run_experiment.sh <name> [env:STORAGE_ROOT=/mnt/ssd] [env:RUNS=7]

# run all experiments
# (pick the script for the scenario you want)
```

Available scripts (call with `--help` for options):

- `read_1000_commit10.sh`
- `read_1000_commit100.sh`
- `write_1000_commit10.sh`
- `write_1000_commit100.sh`
- `mixed_1000_ops_10_commits_read25.sh|read50.sh|read75.sh`
- `mixed_100_ops_100_commits_read25.sh|read50.sh|read75.sh`
- `kill_restart_reads.sh`
- `kill_restart_writes.sh`

You can set `STORAGE_ROOT=/path/on/ssd` to point on-disk storage at a mounted
SSD, `CSV_DIR` to change where CSVs land, and `RUNS` to override iteration
count.

Results: per-experiment CSV files under `benchmark/reports/csv/`, one file per
test. Use `--csv-dir <path>` to redirect where CSVs land.
