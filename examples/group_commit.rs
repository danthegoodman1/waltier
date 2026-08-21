//! Group commit over a channel: producers send entries to one writer thread,
//! which drains the channel into `write_batch` calls. Commits are serialized
//! by the etag chain at roughly one per round trip, so batching is what
//! scales entries/sec.
//!
//! Run: `cargo run --release --example group_commit`

use std::sync::Arc;
use std::sync::mpsc;
use std::thread;
use std::time::Instant;

use waltier::sim::{Latency, SimStore};
use waltier::{Lsn, MemoryStore, ObjectStore, Options, Reconcile, WalApp, WalTier};

/// Counts entries; the smallest possible app.
struct Count;

impl WalApp for Count {
    type State = u64;

    fn init(&self) -> u64 {
        0
    }

    fn apply(&self, state: &mut u64, _lsn: Lsn, _entry: &[u8]) {
        *state += 1;
    }

    fn reconcile(&self, _state: &u64, _pending: &[u8]) -> Reconcile {
        Reconcile::Retry
    }
}

const PRODUCERS: usize = 4;
const PER_PRODUCER: usize = 500;
const MAX_BATCH: usize = 256;

fn main() {
    let store: Arc<dyn ObjectStore> = Arc::new(
        SimStore::new(Arc::new(MemoryStore::new())).with_latency(Latency::s3_like(15, 100)),
    );
    let dir = tempfile::tempdir().unwrap();
    let mut wal = WalTier::open(store, Count, Options::new(dir.path())).unwrap();

    let (tx, rx) = mpsc::channel::<Vec<u8>>();
    let producers: Vec<_> = (0..PRODUCERS)
        .map(|p| {
            let tx = tx.clone();
            thread::spawn(move || {
                for i in 0..PER_PRODUCER {
                    tx.send(format!("p{p}-{i}").into_bytes()).unwrap();
                }
            })
        })
        .collect();
    drop(tx);

    // The group-commit loop: block for one entry, then greedily drain the
    // channel up to MAX_BATCH and commit everything in one CAS PUT. A
    // production version would ack each producer (e.g. a per-entry oneshot
    // channel) after write_batch returns.
    let started = Instant::now();
    let mut commits = 0u64;
    while let Ok(first) = rx.recv() {
        let mut batch = vec![first];
        while batch.len() < MAX_BATCH {
            match rx.try_recv() {
                Ok(entry) => batch.push(entry),
                Err(_) => break,
            }
        }
        wal.write_batch(batch).unwrap();
        commits += 1;
    }
    let elapsed = started.elapsed();

    for p in producers {
        p.join().unwrap();
    }
    let entries = *wal.state();
    println!(
        "{entries} entries in {commits} commits over {:.2}s → {:.0} entries/s at {:.0} commits/s",
        elapsed.as_secs_f64(),
        entries as f64 / elapsed.as_secs_f64(),
        commits as f64 / elapsed.as_secs_f64(),
    );
}
