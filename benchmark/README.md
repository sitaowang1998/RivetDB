# RivetDB Benchmarks

This directory contains a small benchmark harness for RivetDB. It spins up a
fresh in-process 3-node cluster for each run, seeds deterministic data for any
read-heavy tests, and exercises the RPC surface via the Rust client. Each node
uses an on-disk data directory (temp-backed) so restarts replay state, and the
directories are cleaned up after each experiment run.

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

From the repository root:

```bash
./benchmark/scripts/run_all.sh
```

This builds in release mode, executes every experiment 7 times, prints trimmed
results to the console, and writes per-experiment CSV files under
`benchmark/reports/csv/`, one file per test.

Use `--runs N` to change the iteration count, `--experiment <name>` to filter to
a subset (names are listed in the console), or `--csv-dir <path>` to redirect
where CSVs land.
