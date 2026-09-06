//! Diagnostic only: decompose cold replica open without changing WalTier.
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use waltier::{
    CondGet, CondPut, Entry, Lsn, MemoryStore, ObjectStore, Options, Replica, StoreError, Stored,
    WalApp, WalError, WalTier,
};

struct Meter;
static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);
static DEALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static DEALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);
unsafe impl GlobalAlloc for Meter {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            ALLOCATIONS.fetch_add(1, Relaxed);
            ALLOCATED_BYTES.fetch_add(layout.size() as u64, Relaxed);
        }
        pointer
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            ALLOCATIONS.fetch_add(1, Relaxed);
            ALLOCATED_BYTES.fetch_add(layout.size() as u64, Relaxed);
        }
        pointer
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe {
            System.dealloc(pointer, layout);
        }
        DEALLOCATIONS.fetch_add(1, Relaxed);
        DEALLOCATED_BYTES.fetch_add(layout.size() as u64, Relaxed);
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let next = unsafe { System.realloc(pointer, layout, size) };
        if !next.is_null() {
            ALLOCATIONS.fetch_add(1, Relaxed);
            ALLOCATED_BYTES.fetch_add(size as u64, Relaxed);
            DEALLOCATIONS.fetch_add(1, Relaxed);
            DEALLOCATED_BYTES.fetch_add(layout.size() as u64, Relaxed);
        }
        next
    }
}
#[global_allocator]
static ALLOCATOR: Meter = Meter;

#[derive(Clone, Copy)]
struct Observation {
    time: Instant,
    cpu: i32,
    allocations: u64,
    allocated_bytes: u64,
    deallocations: u64,
    deallocated_bytes: u64,
    minor_faults: i64,
    major_faults: i64,
    user_us: i64,
    system_us: i64,
    voluntary_switches: i64,
    involuntary_switches: i64,
}
impl Observation {
    fn now() -> Self {
        let time = Instant::now();
        // All measured stages execute on the opening thread. The compactor
        // has been joined before measurements begin.
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
        assert_eq!(
            unsafe { libc::getrusage(libc::RUSAGE_THREAD, usage.as_mut_ptr()) },
            0
        );
        let usage = unsafe { usage.assume_init() };
        Self {
            time,
            // Linux diagnostic only; sched_getcpu has no pointer arguments.
            cpu: unsafe { libc::sched_getcpu() },
            allocations: ALLOCATIONS.load(Relaxed),
            allocated_bytes: ALLOCATED_BYTES.load(Relaxed),
            deallocations: DEALLOCATIONS.load(Relaxed),
            deallocated_bytes: DEALLOCATED_BYTES.load(Relaxed),
            minor_faults: usage.ru_minflt,
            major_faults: usage.ru_majflt,
            user_us: usage.ru_utime.tv_sec * 1_000_000 + usage.ru_utime.tv_usec,
            system_us: usage.ru_stime.tv_sec * 1_000_000 + usage.ru_stime.tv_usec,
            voluntary_switches: usage.ru_nvcsw,
            involuntary_switches: usage.ru_nivcsw,
        }
    }
}
#[derive(Default)]
struct Stages {
    enabled: bool,
    get_start: Option<Observation>,
    get_end: Option<Observation>,
    restore_start: Option<Observation>,
}
struct TimingStore {
    inner: Arc<MemoryStore>,
    stages: Arc<Mutex<Stages>>,
}
impl ObjectStore for TimingStore {
    // NEW_API_METHODS
    fn get(&self, key: &str) -> Result<Option<Stored>, StoreError> {
        if key.starts_with("snap/") && self.stages.lock().unwrap().enabled {
            let start = Observation::now();
            let result = self.inner.get(key);
            let end = Observation::now();
            let mut stages = self.stages.lock().unwrap();
            assert!(
                stages.get_start.is_none(),
                "expected exactly one snapshot GET"
            );
            stages.get_start = Some(start);
            stages.get_end = Some(end);
            result
        } else {
            self.inner.get(key)
        }
    }
    fn get_if_changed(&self, key: &str, etag: Option<&str>) -> Result<CondGet, StoreError> {
        self.inner.get_if_changed(key, etag)
    }
    fn put_if_match(
        &self,
        key: &str,
        etag: Option<&str>,
        data: &[u8],
    ) -> Result<CondPut, StoreError> {
        self.inner.put_if_match(key, etag, data)
    }
    fn put(&self, key: &str, data: &[u8]) -> Result<String, StoreError> {
        self.inner.put(key, data)
    }
    fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key)
    }
}
struct Count {
    stages: Arc<Mutex<Stages>>,
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
        let entry = Observation::now();
        let mut stages = self.stages.lock().unwrap();
        if stages.enabled {
            assert!(stages.restore_start.is_none());
            stages.restore_start = Some(entry);
        }
        Ok(u64::from_le_bytes(snapshot[..8].try_into().unwrap()))
    }
    fn compact(&self, base: Option<&[u8]>, entries: &[Entry]) -> Result<Vec<u8>, WalError> {
        assert!(base.is_none(), "diagnostic prepares one fold");
        let mut snapshot = vec![0; 16 << 20];
        snapshot[..8].copy_from_slice(&(entries.len() as u64).to_le_bytes());
        Ok(snapshot)
    }
}
fn report(label: &str, from: Observation, to: Observation) {
    println!(
        "{label}: elapsed_ms={:.6} cpu_start={} cpu_end={} allocations={} allocated_bytes={} deallocations={} deallocated_bytes={} minor_faults={} major_faults={} user_us={} system_us={} voluntary_switches={} involuntary_switches={}",
        to.time.duration_since(from.time).as_secs_f64() * 1000.0,
        from.cpu,
        to.cpu,
        to.allocations - from.allocations,
        to.allocated_bytes - from.allocated_bytes,
        to.deallocations - from.deallocations,
        to.deallocated_bytes - from.deallocated_bytes,
        to.minor_faults - from.minor_faults,
        to.major_faults - from.major_faults,
        to.user_us - from.user_us,
        to.system_us - from.system_us,
        to.voluntary_switches - from.voluntary_switches,
        to.involuntary_switches - from.involuntary_switches
    );
}
fn main() {
    let write_dir = tempfile::tempdir().unwrap();
    let stages = Arc::new(Mutex::new(Stages::default()));
    let raw = Arc::new(MemoryStore::new());
    let store = Arc::new(TimingStore {
        inner: raw.clone(),
        stages: stages.clone(),
    });
    let mut wal = WalTier::open(
        store.clone(),
        Count {
            stages: stages.clone(),
        },
        Options::new(write_dir.path()),
    )
    .unwrap();
    wal.write_batch(vec![vec![0; 64]; 1024]).unwrap();
    assert!(wal.compact_now());
    let _ = wal.wait_for_compaction();
    assert!(wal.has_pending_fold());
    wal.flush().unwrap();
    let cold_dir = tempfile::tempdir().unwrap();
    // An optional separate clone before the timer reveals allocator/source-page
    // warmup effects; it does not turn the replica's empty cache into a hit.
    if std::env::args().any(|arg| arg == "--prime-get") {
        let key = raw
            .keys()
            .into_iter()
            .find(|key| key.starts_with("snap/"))
            .unwrap();
        drop(raw.get(&key).unwrap().unwrap());
    }
    stages.lock().unwrap().enabled = true;
    let start = Observation::now();
    let replica = Replica::open(
        store,
        Count {
            stages: stages.clone(),
        },
        Options::new(cold_dir.path()),
    )
    .unwrap();
    let end = Observation::now();
    assert_eq!(*replica.state(), 1024);
    let stages = stages.lock().unwrap();
    let get_start = stages.get_start.unwrap();
    let get_end = stages.get_end.unwrap();
    let restore = stages.restore_start.unwrap();
    // Capture all observations before printing so reporting cannot alter counts.
    report("cold_total", start, end);
    report("before_snapshot_get", start, get_start);
    report("snapshot_get", get_start, get_end);
    report("snapshot_cache", get_end, restore);
    report("after_restore", restore, end);
    // The prepared snapshot must be cached in all three variants.
    assert_eq!(std::fs::read_dir(cold_dir.path()).unwrap().count(), 2);
    drop(stages);
    wal.close().unwrap();
}
