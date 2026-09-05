//! Candidate cache policies under identical sustained, zero-latency workloads.
use std::sync::Arc;
use std::time::{Duration, Instant};
use waltier::{CachePolicy, Entry, Lsn, MemoryStore, Options, WalApp, WalError, WalTier};

struct Count;
impl WalApp for Count {
    type State = u64;
    fn init(&self) -> u64 {
        0
    }
    fn apply(&self, state: &mut u64, _: Lsn, _: &[u8]) {
        *state += 1;
    }
    fn restore(&self, bytes: &[u8]) -> Result<u64, WalError> {
        Ok(u64::from_le_bytes(
            bytes
                .try_into()
                .map_err(|_| WalError::App("bad count".into()))?,
        ))
    }
    fn compact(&self, base: Option<&[u8]>, entries: &[Entry]) -> Result<Vec<u8>, WalError> {
        Ok(
            (base.map(|b| self.restore(b)).transpose()?.unwrap_or(0) + entries.len() as u64)
                .to_le_bytes()
                .to_vec(),
        )
    }
}
fn percentile(times: &[Duration], percent: usize) -> f64 {
    times[(times.len() * percent).div_ceil(100) - 1].as_secs_f64() * 1e3
}
fn run(policy: CachePolicy, entry_bytes: usize) {
    let dir = tempfile::tempdir().unwrap();
    let mut options = Options::new(dir.path());
    options.cache_policy = policy;
    let mut wal = WalTier::open(Arc::new(MemoryStore::new()), Count, options).unwrap();
    let mut times = Vec::with_capacity(192);
    let start = Instant::now();
    for _ in 0..12 {
        for _ in 0..16 {
            let entries = vec![vec![0; entry_bytes]; 64];
            let started = Instant::now();
            wal.write_batch(entries).unwrap();
            times.push(started.elapsed());
        }
        assert!(wal.compact_now());
        wal.flush().unwrap();
    }
    assert_eq!(*wal.state(), 12_288);
    wal.close().unwrap();
    let elapsed = start.elapsed();
    times.sort();
    println!(
        "cache_{policy:?}_entry{entry_bytes}: p50_ms={:.4} p99_ms={:.4} total_s={:.4} entries_s={:.1}",
        percentile(&times, 50),
        percentile(&times, 99),
        elapsed.as_secs_f64(),
        12_288.0 / elapsed.as_secs_f64()
    );
}
fn main() {
    for bytes in [64, 4096] {
        for policy in [
            CachePolicy::Disabled,
            CachePolicy::EveryCommit,
            CachePolicy::OnFlush,
        ] {
            run(policy, bytes);
        }
    }
}
