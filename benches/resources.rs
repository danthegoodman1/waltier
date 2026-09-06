//! Heap retention and compaction-start cost; run separately from latency cases.
//! Counts requested Rust heap bytes, excluding allocator overhead and thread stacks.
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Barrier};
use std::time::Instant;

use waltier::{Entry, Lsn, MemoryStore, Options, Replica, WalApp, WalError, WalTier};

struct Meter;
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
fn allocated(bytes: usize) {
    let live = LIVE.fetch_add(bytes, SeqCst) + bytes;
    PEAK.fetch_max(live, SeqCst);
}
// Delegate the allocator contract unchanged; counters allocate no memory.
unsafe impl GlobalAlloc for Meter {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            allocated(layout.size());
        }
        ptr
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            allocated(layout.size());
        }
        ptr
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe {
            System.dealloc(ptr, layout);
        }
        LIVE.fetch_sub(layout.size(), SeqCst);
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let next = unsafe { System.realloc(ptr, layout, size) };
        if !next.is_null() {
            if size >= layout.size() {
                allocated(size - layout.size());
            } else {
                LIVE.fetch_sub(layout.size() - size, SeqCst);
            }
        }
        next
    }
}
#[global_allocator]
static ALLOCATOR: Meter = Meter;

struct Count {
    snapshot_bytes: usize,
    barriers: Option<(Arc<Barrier>, Arc<Barrier>)>,
}
impl WalApp for Count {
    type State = u64;
    fn init(&self) -> u64 {
        0
    }
    fn apply(&self, state: &mut u64, _: Lsn, _: &[u8]) {
        *state += 1;
    }
    fn restore(&self, snapshot: &[u8]) -> Result<u64, WalError> {
        Ok(u64::from_le_bytes(
            snapshot
                .get(..8)
                .ok_or_else(|| WalError::App("short snapshot".into()))?
                .try_into()
                .unwrap(),
        ))
    }
    fn compact(&self, base: Option<&[u8]>, entries: &[Entry]) -> Result<Vec<u8>, WalError> {
        if let Some((entered, release)) = &self.barriers {
            entered.wait();
            release.wait();
        }
        let count = base
            .map(|bytes| self.restore(bytes))
            .transpose()?
            .unwrap_or(0)
            + entries.len() as u64;
        let mut bytes = vec![0; self.snapshot_bytes];
        bytes[..8].copy_from_slice(&count.to_le_bytes());
        Ok(bytes)
    }
}
fn retained_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(MemoryStore::new());
    let mut wal = WalTier::open(
        store.clone(),
        Count {
            snapshot_bytes: 16 << 20,
            barriers: None,
        },
        Options::new(dir.path()),
    )
    .unwrap();
    wal.write_batch(vec![vec![0; 64]; 1024]).unwrap();
    let before = LIVE.load(SeqCst);
    PEAK.store(before, SeqCst);
    assert!(wal.compact_now());
    let _ = wal.wait_for_compaction();
    assert!(wal.has_pending_fold());
    let retained = LIVE.load(SeqCst).saturating_sub(before);
    let peak = PEAK.load(SeqCst).saturating_sub(before);
    wal.flush().unwrap();
    let cold_dir = tempfile::tempdir().unwrap();
    let start = Instant::now();
    let replica = Replica::open(
        store,
        Count {
            snapshot_bytes: 16 << 20,
            barriers: None,
        },
        Options::new(cold_dir.path()),
    )
    .unwrap();
    let catchup = start.elapsed();
    assert_eq!(*replica.state(), 1024);
    println!(
        "snapshot16m: pending_heap_delta={retained} compaction_peak_delta={peak} cold_open_ms={:.4}",
        catchup.as_secs_f64() * 1000.0
    );
    wal.close().unwrap();
}
fn compaction_start() {
    let dir = tempfile::tempdir().unwrap();
    let entered = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let mut wal = WalTier::open(
        Arc::new(MemoryStore::new()),
        Count {
            snapshot_bytes: 8,
            barriers: Some((entered.clone(), release.clone())),
        },
        Options::new(dir.path()),
    )
    .unwrap();
    wal.write_batch(vec![vec![0; 4096]; 4096]).unwrap();
    let before = LIVE.load(SeqCst);
    PEAK.store(before, SeqCst);
    let start = Instant::now();
    assert!(wal.compact_now());
    let elapsed = start.elapsed();
    entered.wait();
    let held = LIVE.load(SeqCst).saturating_sub(before);
    let peak = PEAK.load(SeqCst).saturating_sub(before);
    release.wait();
    let _ = wal.wait_for_compaction();
    assert!(wal.has_pending_fold());
    wal.close().unwrap();
    println!(
        "live16m: start_ms={:.4} held_heap_delta={held} startup_peak_delta={peak}",
        elapsed.as_secs_f64() * 1000.0
    );
}
fn main() {
    // Separate processes isolate allocator history when diagnosing timings.
    match std::env::args().nth(1).as_deref() {
        None => {
            retained_snapshot();
            compaction_start();
        }
        Some("snapshot") => retained_snapshot(),
        Some("startup") => compaction_start(),
        Some(other) => panic!("unknown resource case: {other}"),
    }
}
