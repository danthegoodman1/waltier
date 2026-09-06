//! Repeatable comparison against the reviewed baseline. Reports acknowledgement
//! latency separately from total time, which includes explicit maintenance/close.
//! See PERFORMANCE.md for the workload and baseline reproduction commands.
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};
use tempfile::TempDir;
use waltier::sim::{Latency, SimStats, SimStore};
use waltier::{Entry, Lsn, MemoryStore, Options, WalApp, WalError, WalTier};

struct Count;
impl WalApp for Count {
    type State = u64;
    fn init(&self) -> u64 {
        0
    }
    fn apply(&self, state: &mut u64, _: Lsn, _: &[u8]) {
        *state += 1;
    }
    fn restore(&self, snapshot: &[u8]) -> Result<u64, WalError> {
        Ok(u64::from_le_bytes(snapshot.try_into().map_err(|_| {
            WalError::App("bad counter snapshot".into())
        })?))
    }
    fn compact(&self, base: Option<&[u8]>, entries: &[Entry]) -> Result<Vec<u8>, WalError> {
        let count = base
            .map(|bytes| self.restore(bytes))
            .transpose()?
            .unwrap_or(0);
        Ok((count + entries.len() as u64).to_le_bytes().to_vec())
    }
}
fn store(rtt_ms: u64) -> Arc<SimStore> {
    Arc::new(
        SimStore::new(Arc::new(MemoryStore::new())).with_latency(Latency {
            rtt: Duration::from_millis(rtt_ms),
            bytes_per_sec: if rtt_ms == 0 { 0 } else { 100_000_000 },
            jitter: 0.0,
        }),
    )
}
fn percentile(times: &[Duration], p: usize) -> f64 {
    times[(times.len() * p).div_ceil(100).saturating_sub(1)].as_secs_f64() * 1000.0
}
fn report(
    label: &str,
    mut times: Vec<Duration>,
    elapsed: Duration,
    entries: usize,
    before: SimStats,
    after: SimStats,
) {
    times.sort();
    println!(
        "{label}: commits={} entries={entries} p50_ms={:.4} p99_ms={:.4} total_s={:.4} entries_s={:.1} uploaded_bytes_per_entry={:.1} gets={} puts={} deletes={}",
        times.len(),
        percentile(&times, 50),
        percentile(&times, 99),
        elapsed.as_secs_f64(),
        entries as f64 / elapsed.as_secs_f64(),
        (after.bytes_uploaded - before.bytes_uploaded) as f64 / entries as f64,
        after.gets - before.gets,
        after.puts - before.puts,
        after.deletes - before.deletes,
    );
}
fn install_latency() {
    let store = store(15);
    let dir = TempDir::new().unwrap();
    let mut wal = WalTier::open(store.clone(), Count, Options::new(dir.path())).unwrap();
    wal.write(vec![0; 64]).unwrap();
    assert!(wal.compact_now());
    let _ = wal.wait_for_compaction();
    assert!(wal.has_pending_fold());
    wal.flush().unwrap();
    let start = Instant::now();
    let before = store.stats();
    let mut times = Vec::new();
    for _ in 0..20 {
        wal.write(vec![0; 64]).unwrap();
        assert!(wal.compact_now());
        let _ = wal.wait_for_compaction();
        assert!(wal.has_pending_fold());
        let t = Instant::now();
        wal.write(vec![0; 64]).unwrap();
        times.push(t.elapsed());
    }
    assert_eq!(*wal.state(), 41);
    wal.close().unwrap();
    report(
        "fold_install_rtt15",
        times,
        start.elapsed(),
        40,
        before,
        store.stats(),
    );
}
fn sustained(batch: usize, rtt_ms: u64, entry_bytes: usize) {
    sustained_folds(batch, rtt_ms, entry_bytes, 12);
}

fn sustained_folds(batch: usize, rtt_ms: u64, entry_bytes: usize, folds: usize) {
    let store = store(rtt_ms);
    let dir = TempDir::new().unwrap();
    let mut wal = WalTier::open(store.clone(), Count, Options::new(dir.path())).unwrap();
    let start = Instant::now();
    let before = store.stats();
    let mut times = Vec::new();
    // Sixteen commits per fold: every version sees the same schedule.
    for _ in 0..folds {
        for _ in 0..16 {
            let entries = vec![vec![0; entry_bytes]; batch];
            let t = Instant::now();
            wal.write_batch(entries).unwrap();
            times.push(t.elapsed());
        }
        assert!(wal.compact_now());
        let _ = wal.wait_for_compaction();
        assert!(wal.has_pending_fold());
        wal.flush().unwrap();
    }
    let entries = folds * 16 * batch;
    assert_eq!(*wal.state(), entries as u64);
    wal.close().unwrap();
    let label = if folds != 12 {
        format!("extended_batch{batch}_rtt{rtt_ms}")
    } else if entry_bytes == 64 {
        format!("steady_batch{batch}_rtt{rtt_ms}")
    } else {
        format!("steady_batch{batch}_rtt{rtt_ms}_entry{entry_bytes}")
    };
    report(
        &label,
        times,
        start.elapsed(),
        entries,
        before,
        store.stats(),
    );
}

struct HeldCount {
    entered: Arc<Barrier>,
    release: Arc<Barrier>,
}
impl WalApp for HeldCount {
    type State = u64;
    fn init(&self) -> u64 {
        Count.init()
    }
    fn apply(&self, state: &mut u64, lsn: Lsn, entry: &[u8]) {
        Count.apply(state, lsn, entry);
    }
    fn restore(&self, bytes: &[u8]) -> Result<u64, WalError> {
        Count.restore(bytes)
    }
    fn compact(&self, base: Option<&[u8]>, entries: &[Entry]) -> Result<Vec<u8>, WalError> {
        self.entered.wait();
        self.release.wait();
        Count.compact(base, entries)
    }
}
fn compaction_lag() {
    let store = store(1);
    let dir = TempDir::new().unwrap();
    let entered = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let mut wal = WalTier::open(
        store.clone(),
        HeldCount {
            entered: entered.clone(),
            release: release.clone(),
        },
        Options::new(dir.path()),
    )
    .unwrap();
    wal.write_batch(vec![vec![0; 64]; 64]).unwrap();
    let start = Instant::now();
    let before = store.stats();
    let mut times = Vec::with_capacity(192);
    for _ in 0..12 {
        assert!(wal.compact_now());
        entered.wait();
        // A fixed amount of lag, independent of thread timing: this entire
        // cohort is accepted while the previous prefix is still being folded.
        for _ in 0..16 {
            let entries = vec![vec![0; 64]; 64];
            let started = Instant::now();
            wal.write_batch(entries).unwrap();
            times.push(started.elapsed());
        }
        release.wait();
        let _ = wal.wait_for_compaction();
        assert!(wal.has_pending_fold());
        wal.flush().unwrap();
    }
    assert_eq!(*wal.state(), 12_288 + 64);
    wal.close().unwrap();
    report(
        "lag_batch64_rtt1",
        times,
        start.elapsed(),
        12_288,
        before,
        store.stats(),
    );
}
fn main() {
    if std::env::args().nth(1).as_deref() == Some("--small") {
        // Extend the otherwise ~10 ms zero-latency sample for regression work.
        sustained_folds(64, 0, 64, 120);
        sustained(64, 1, 64);
        return;
    }
    install_latency();
    for batch in [1, 8, 64, 256] {
        sustained(batch, 1, 64);
    }
    sustained(64, 0, 64);
    sustained(64, 1, 4096);
    compaction_lag();
    sustained_folds(64, 0, 64, 120);
}
