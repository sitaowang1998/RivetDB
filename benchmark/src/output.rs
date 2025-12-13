use serde::Serialize;

#[derive(Serialize)]
pub struct OutputRow {
    pub label: String,
    pub run: usize,
    pub duration_ms: u128,
    pub ops: usize,
    pub reads: usize,
    pub writes: usize,
    pub commits: usize,
    pub commit_every: usize,
    pub ops_per_sec: f64,
}
