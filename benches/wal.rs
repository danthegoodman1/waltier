//! Local benchmarks against a simulated S3.
//!
//! Run: `cargo bench --bench wal -- [--rtt-ms 15] [--mbps 100] [--writes 200] [--quick]`
//!
//! The store is a MemoryStore behind `sim::SimStore`, which sleeps
//! `rtt + bytes/bandwidth` (with 20% jitter) per operation. Latency numbers
//! are per `write()` call — the CAS PUT is the commit point, so this is
//! commit latency.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tempfile::TempDir;
use waltier::sim::{Latency, SimStore};
use waltier::{
    Entry, Lsn, MemoryStore, ObjectStore, Options, Reconcile, Replica, WalApp, WalError, WalStats,
    WalTier,
};

/// KV app with fixed-size blob values: entry = u32 key + payload.
struct BlobKv {
    compact_at: u64,
}

type Map = HashMap<u32, Vec<u8>>;

impl WalApp for BlobKv {
    type State = Map;

    fn init(&self) -> Map {
        Map::new()
    }

    fn apply(&self, state: &mut Map, _lsn: Lsn, entry: &[u8]) {
        let key = u32::from_le_bytes(entry[..4].try_into().unwrap());
        state.insert(key, entry[4..].to_vec());
    }

    fn restore(&self, snapshot: &[u8]) -> Result<Map, WalError> {
        let mut map = Map::new();
        let mut pos = 0;
        while pos < snapshot.len() {
            let key = u32::from_le_bytes(snapshot[pos..pos + 4].try_into().unwrap());
            let len = u32::from_le_bytes(snapshot[pos + 4..pos + 8].try_into().unwrap()) as usize;
            map.insert(key, snapshot[pos + 8..pos + 8 + len].to_vec());
            pos += 8 + len;
        }
        Ok(map)
    }

    fn compact(&self, base: Option<&[u8]>, entries: &[Entry]) -> Result<Vec<u8>, WalError> {
        let mut map = base.map(|b| self.restore(b)).transpose()?.unwrap_or_default();
        for e in entries {
            self.apply(&mut map, e.lsn, &e.data);
        }
        let mut out = Vec::new();
        for (key, value) in &map {
            out.extend_from_slice(&key.to_le_bytes());
            out.extend_from_slice(&(value.len() as u32).to_le_bytes());
            out.extend_from_slice(value);
        }
        Ok(out)
    }

    fn should_compact(&self, stats: &WalStats) -> bool {
        stats.live_entries >= self.compact_at
    }

    fn reconcile(&self, _state: &Map, _pending: &[u8]) -> Reconcile {
        Reconcile::Retry
    }
}

fn entry_for(key: u32, payload: usize) -> Vec<u8> {
    let mut e = key.to_le_bytes().to_vec();
    e.resize(4 + payload, 0xAB);
    e
}

struct Cfg {
    latency: Latency,
    writes: usize,
}

fn percentile(sorted: &[Duration], p: usize) -> Duration {
    sorted[(sorted.len() - 1) * p / 100]
}

fn report_latency(label: &str, mut timings: Vec<Duration>, total: Duration) {
    timings.sort();
    let n = timings.len();
    println!(
        "  {label}: {n} ops in {:.2}s → {:.1} ops/s | p50={:.1?} p90={:.1?} p99={:.1?} max={:.1?}",
        total.as_secs_f64(),
        n as f64 / total.as_secs_f64(),
        percentile(&timings, 50),
        percentile(&timings, 90),
        percentile(&timings, 99),
        timings[n - 1],
    );
}

fn report_store(store: &SimStore) {
    let s = store.stats();
    println!(
        "  store: {} puts ({} CAS-lost), {} gets, {} 304s, {:.2} MB up, {:.2} MB down",
        s.puts,
        s.cas_failures,
        s.gets,
        s.not_modified,
        s.bytes_uploaded as f64 / 1e6,
        s.bytes_downloaded as f64 / 1e6,
    );
}

fn sim_store(latency: Latency) -> Arc<SimStore> {
    Arc::new(SimStore::new(Arc::new(MemoryStore::new())).with_latency(latency))
}

/// Steady-state writes with periodic folds riding on them.
fn bench_writes(cfg: &Cfg, payload: usize, compact_at: u64) {
    let store = sim_store(cfg.latency);
    let dir = TempDir::new().unwrap();
    let s: Arc<dyn ObjectStore> = store.clone();
    let mut wal = WalTier::open(s, BlobKv { compact_at }, Options::new(dir.path())).unwrap();

    let mut timings = Vec::with_capacity(cfg.writes);
    let started = Instant::now();
    for i in 0..cfg.writes {
        let entry = entry_for(i as u32 % 1024, payload);
        let t = Instant::now();
        wal.write(entry).unwrap();
        timings.push(t.elapsed());
    }
    let total = started.elapsed();

    println!("write: entry={payload}B, compact every {compact_at}");
    report_latency("writes", timings, total);
    let stats = wal.stats();
    println!(
        "  wal: tip={:?}, image={} B, live entries={}",
        stats.tip, stats.image_bytes, stats.live_entries
    );
    report_store(&store);
    wal.close().unwrap();
}

/// No compaction: shows per-write cost growing with the image.
fn bench_image_growth(cfg: &Cfg, payload: usize) {
    let store = sim_store(cfg.latency);
    let dir = TempDir::new().unwrap();
    let s: Arc<dyn ObjectStore> = store.clone();
    let mut wal =
        WalTier::open(s, BlobKv { compact_at: u64::MAX }, Options::new(dir.path())).unwrap();

    let n = cfg.writes;
    let mut timings = Vec::with_capacity(n);
    for i in 0..n {
        let entry = entry_for(i as u32, payload);
        let t = Instant::now();
        wal.write(entry).unwrap();
        timings.push(t.elapsed());
    }
    let decile = n / 10;
    let mean = |slice: &[Duration]| {
        slice.iter().sum::<Duration>() / slice.len() as u32
    };
    println!("image growth: entry={payload}B, no compaction, {n} writes");
    println!(
        "  mean write latency: first 10% = {:.1?}, last 10% = {:.1?}; final image = {} B",
        mean(&timings[..decile]),
        mean(&timings[n - decile..]),
        wal.stats().image_bytes,
    );
}

/// Cold open (download snapshot) vs warm open (etag-validated disk cache).
fn bench_open(cfg: &Cfg) {
    let store = sim_store(cfg.latency);
    let dir = TempDir::new().unwrap();
    let s: Arc<dyn ObjectStore> = store.clone();
    let mut wal =
        WalTier::open(s, BlobKv { compact_at: u64::MAX }, Options::new(dir.path())).unwrap();
    let blob = 1 << 20;
    for key in 0..8u32 {
        wal.write(entry_for(key, blob)).unwrap();
    }
    assert!(wal.compact_now());
    assert!(wal.wait_for_compaction());
    wal.flush().unwrap();
    let snapshot_bytes = 8 * (blob + 8);
    drop(wal);

    let cold_dir = TempDir::new().unwrap();
    let t = Instant::now();
    let s: Arc<dyn ObjectStore> = store.clone();
    let wal = WalTier::open(s, BlobKv { compact_at: u64::MAX }, Options::new(cold_dir.path()))
        .unwrap();
    let cold = t.elapsed();
    drop(wal);

    let t = Instant::now();
    let s: Arc<dyn ObjectStore> = store.clone();
    let wal = WalTier::open(s, BlobKv { compact_at: u64::MAX }, Options::new(cold_dir.path()))
        .unwrap();
    let warm = t.elapsed();
    drop(wal);

    println!("open with a ~{:.0} MB snapshot:", snapshot_bytes as f64 / 1e6);
    println!("  cold (empty cache) = {cold:.1?}, warm (cached, etag-validated) = {warm:.1?}");
}

/// Caught-up polling (304s) vs catching up after new writes.
fn bench_replica(cfg: &Cfg) {
    let store = sim_store(cfg.latency);
    let dw = TempDir::new().unwrap();
    let s: Arc<dyn ObjectStore> = store.clone();
    let mut wal =
        WalTier::open(s, BlobKv { compact_at: u64::MAX }, Options::new(dw.path())).unwrap();
    wal.write(entry_for(0, 64)).unwrap();

    let dr = TempDir::new().unwrap();
    let s: Arc<dyn ObjectStore> = store.clone();
    let mut replica =
        Replica::open(s, BlobKv { compact_at: u64::MAX }, Options::new(dr.path())).unwrap();

    let polls = 20;
    let mut timings = Vec::with_capacity(polls);
    let started = Instant::now();
    for _ in 0..polls {
        let t = Instant::now();
        assert!(!replica.refresh().unwrap());
        timings.push(t.elapsed());
    }
    let total = started.elapsed();
    println!("replica:");
    report_latency("caught-up polls (304)", timings, total);

    for i in 0..10u32 {
        wal.write(entry_for(i, 1024)).unwrap();
    }
    let t = Instant::now();
    assert!(replica.refresh().unwrap());
    println!("  catch-up after 10 writes of 1KB = {:.1?}", t.elapsed());
}

fn main() {
    let mut rtt_ms = 15u64;
    let mut mbps = 100u64;
    let mut writes = 200usize;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--rtt-ms" => rtt_ms = args.next().unwrap().parse().unwrap(),
            "--mbps" => mbps = args.next().unwrap().parse().unwrap(),
            "--writes" => writes = args.next().unwrap().parse().unwrap(),
            "--quick" => {
                rtt_ms = 5;
                writes = 60;
            }
            _ => {} // ignore harness flags like --bench
        }
    }

    println!("== waltier bench: no store latency (raw library cost) ==");
    let local = Cfg { latency: Latency::default(), writes: 2000 };
    bench_writes(&local, 64, 64);

    println!();
    println!("== waltier bench: simulated S3 (rtt {rtt_ms}ms ±20%, {mbps} MB/s) ==");
    let cfg = Cfg { latency: Latency::s3_like(rtt_ms, mbps), writes };
    bench_writes(&cfg, 64, 64);
    bench_writes(&cfg, 4096, 64);
    bench_image_growth(&cfg, 256);
    bench_open(&cfg);
    bench_replica(&cfg);
}
