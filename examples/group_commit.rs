//! Bounded group commit with producer receipts after durable batch results.
//! Run: cargo run --release --example group_commit
//!
//! Queued payload lengths are bounded by capacity × maximum entry bytes; the
//! writer holds at most max_batch additional entries. Vec spare capacity,
//! request/channel overhead, and producer-owned data are outside that bound.
//! Producers pipeline a bounded window of receipts. Dropping all submitters
//! drains accepted work before shutdown.

use std::collections::VecDeque;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
    mpsc,
};
use std::thread;
use std::time::Instant;
use waltier::sim::{Latency, SimStore};
use waltier::{
    Lsn, MemoryStore, MutationOutcome, ObjectStore, Options, Reconcile, WalApp, WalError, WalTier,
    WriteError,
};

struct Count;
impl WalApp for Count {
    type State = u64;
    fn init(&self) -> u64 {
        0
    }
    fn apply(&self, state: &mut u64, _: Lsn, _: &[u8]) {
        *state += 1;
    }
    fn reconcile(&self, _: &u64, _: &[u8]) -> Reconcile {
        Reconcile::Retry
    }
}

type Receipt = mpsc::Receiver<Result<Lsn, Arc<WriteError>>>;
struct Request {
    entry: Vec<u8>,
    result: mpsc::Sender<Result<Lsn, Arc<WriteError>>>,
}
#[derive(Debug)]
struct SubmissionError {
    entry: Vec<u8>,
    reason: &'static str,
}
impl std::fmt::Display for SubmissionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} ({} byte entry returned)",
            self.reason,
            self.entry.len()
        )
    }
}
impl std::error::Error for SubmissionError {}

#[derive(Clone)]
struct Submitter {
    tx: mpsc::SyncSender<Request>,
    stopped: Arc<AtomicBool>,
    max_entry_bytes: usize,
}
impl Submitter {
    fn prepare(&self, entry: Vec<u8>) -> Result<(Request, Receipt), SubmissionError> {
        if entry.len() > self.max_entry_bytes {
            return Err(SubmissionError {
                entry,
                reason: "entry too large",
            });
        }
        if self.stopped.load(Ordering::Acquire) {
            return Err(SubmissionError {
                entry,
                reason: "writer stopped",
            });
        }
        let (result, receipt) = mpsc::channel();
        Ok((Request { entry, result }, receipt))
    }

    /// Blocks for queue capacity, not for durability; await the returned receipt.
    fn submit(&self, entry: Vec<u8>) -> Result<Receipt, SubmissionError> {
        let (request, receipt) = self.prepare(entry)?;
        self.tx.send(request).map_err(|error| SubmissionError {
            entry: error.0.entry,
            reason: "writer stopped",
        })?;
        Ok(receipt)
    }

    /// An admission check suitable for callers that must not block for capacity.
    fn try_submit(&self, entry: Vec<u8>) -> Result<Receipt, SubmissionError> {
        let (request, receipt) = self.prepare(entry)?;
        self.tx.try_send(request).map_err(|error| match error {
            mpsc::TrySendError::Full(request) => SubmissionError {
                entry: request.entry,
                reason: "queue full",
            },
            mpsc::TrySendError::Disconnected(request) => SubmissionError {
                entry: request.entry,
                reason: "writer stopped",
            },
        })?;
        Ok(receipt)
    }
}
fn queue(capacity: usize, max_entry_bytes: usize) -> (Submitter, mpsc::Receiver<Request>) {
    assert!(capacity > 0 && max_entry_bytes > 0);
    let (tx, rx) = mpsc::sync_channel(capacity);
    (
        Submitter {
            tx,
            stopped: Arc::new(AtomicBool::new(false)),
            max_entry_bytes,
        },
        rx,
    )
}

#[derive(Debug)]
struct Report {
    acknowledged: u64,
    commits: u64,
    failed: u64,
}
fn run_writer(
    mut wal: WalTier<Count>,
    rx: mpsc::Receiver<Request>,
    stopped: Arc<AtomicBool>,
    max_batch: usize,
) -> Result<Report, WalError> {
    assert!(max_batch > 0);
    let mut report = Report {
        acknowledged: 0,
        commits: 0,
        failed: 0,
    };
    while let Ok(first) = rx.recv() {
        let mut entries = vec![first.entry];
        let mut receipts = vec![first.result];
        while entries.len() < max_batch {
            let Ok(request) = rx.try_recv() else { break };
            entries.push(request.entry);
            receipts.push(request.result);
        }
        let result = if stopped.load(Ordering::Acquire) {
            Err(WriteError {
                entries,
                source: WalError::App(
                    "group writer stopped after an earlier append failure".into(),
                ),
                outcome: MutationOutcome::NotApplied,
            })
        } else {
            report.commits += 1;
            wal.write_batch(entries)
        };
        match result {
            Ok(range) => {
                // This adapter is for independent entries and the Count app's
                // Retry reconciliation, so it never changes batch cardinality.
                assert_eq!(range.end - range.start, receipts.len() as u64);
                report.acknowledged += receipts.len() as u64;
                for (lsn, receipt) in range.zip(receipts) {
                    let _ = receipt.send(Ok(lsn));
                }
            }
            Err(error) => {
                // Preserve the uncertain batch and do not retry it. Reject
                // further admissions and report queued work as NotApplied.
                stopped.store(true, Ordering::Release);
                report.failed += receipts.len() as u64;
                let error = Arc::new(error);
                for receipt in receipts {
                    let _ = receipt.send(Err(error.clone()));
                }
            }
        }
    }
    wal.close()?;
    Ok(report)
}

const PRODUCERS: usize = 4;
const PER_PRODUCER: usize = 500;
const MAX_BATCH: usize = 256;
const MAX_ENTRY_BYTES: usize = 1024;
const QUEUE_CAPACITY: usize = 512;
const RECEIPT_WINDOW: usize = 128;
fn main() {
    let store: Arc<dyn ObjectStore> = Arc::new(
        SimStore::new(Arc::new(MemoryStore::new())).with_latency(Latency::s3_like(15, 100)),
    );
    let wal = WalTier::open(store, Count, Options::default()).unwrap();
    let (submitter, rx) = queue(QUEUE_CAPACITY, MAX_ENTRY_BYTES);
    let stopped = submitter.stopped.clone();
    let started = Instant::now();
    let worker = thread::spawn(move || run_writer(wal, rx, stopped, MAX_BATCH));
    let producers: Vec<_> = (0..PRODUCERS)
        .map(|producer| {
            let submitter = submitter.clone();
            thread::spawn(move || {
                let mut pending = VecDeque::new();
                for index in 0..PER_PRODUCER {
                    let entry = format!("p{producer}-{index}").into_bytes();
                    // Prefer immediate admission; waiting for queue capacity keeps
                    // the producer's own memory bounded when the store is slow.
                    let receipt = match submitter.try_submit(entry) {
                        Ok(receipt) => receipt,
                        Err(error) if error.reason == "queue full" => {
                            submitter.submit(error.entry).unwrap()
                        }
                        Err(error) => panic!("{error}"),
                    };
                    pending.push_back(receipt);
                    if pending.len() == RECEIPT_WINDOW {
                        pending.pop_front().unwrap().recv().unwrap().unwrap();
                    }
                }
                for receipt in pending {
                    receipt.recv().unwrap().unwrap();
                }
            })
        })
        .collect();
    drop(submitter);
    for producer in producers {
        producer.join().unwrap();
    }
    let report = worker.join().unwrap().unwrap();
    assert_eq!(report.failed, 0);
    let elapsed = started.elapsed().as_secs_f64();
    println!(
        "{} acknowledged entries in {} commits over {elapsed:.2}s: {:.0} entries/s",
        report.acknowledged,
        report.commits,
        report.acknowledged as f64 / elapsed
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, atomic::AtomicUsize};
    use std::time::Duration;
    use waltier::{CondPut, Replica, StoreError, Stored};

    struct HeldStore {
        inner: MemoryStore,
        started: mpsc::Sender<()>,
        release: Mutex<mpsc::Receiver<()>>,
        calls: AtomicUsize,
        unknown: bool,
    }
    impl ObjectStore for HeldStore {
        fn get(&self, key: &str) -> Result<Option<Stored>, StoreError> {
            self.inner.get(key)
        }
        fn put_if_match(
            &self,
            key: &str,
            etag: Option<&str>,
            data: &[u8],
        ) -> Result<CondPut, StoreError> {
            if key == "wal" && etag.is_some() && self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                self.started.send(()).unwrap();
                self.release
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap();
                let result = self.inner.put_if_match(key, etag, data)?;
                if self.unknown {
                    return Err(StoreError::new("accepted PUT reply lost"));
                }
                return Ok(result);
            }
            self.inner.put_if_match(key, etag, data)
        }
        fn put(&self, key: &str, data: &[u8]) -> Result<String, StoreError> {
            self.inner.put(key, data)
        }
        fn delete(&self, key: &str) -> Result<(), StoreError> {
            self.inner.delete(key)
        }
    }
    fn held(unknown: bool) -> (Arc<HeldStore>, mpsc::Receiver<()>, mpsc::Sender<()>) {
        let (started, notification) = mpsc::channel();
        let (release, wait) = mpsc::channel();
        (
            Arc::new(HeldStore {
                inner: MemoryStore::new(),
                started,
                release: Mutex::new(wait),
                calls: AtomicUsize::new(0),
                unknown,
            }),
            notification,
            release,
        )
    }

    #[test]
    fn blocked_cas_withholds_receipts_bounds_queue_and_drains_after_producer_shutdown() {
        let (store, started, release) = held(false);
        let wal = WalTier::open(store.clone(), Count, Options::default()).unwrap();
        let (submitter, rx) = queue(2, 4);
        let stopped = submitter.stopped.clone();
        let first = submitter.submit(b"aaaa".to_vec()).unwrap();
        let worker = thread::spawn(move || run_writer(wal, rx, stopped, 2));
        started.recv_timeout(Duration::from_secs(5)).unwrap();
        let second = submitter.try_submit(b"bb".to_vec()).unwrap();
        let third = submitter.try_submit(b"cc".to_vec()).unwrap();
        let full = submitter.try_submit(b"d".to_vec()).unwrap_err();
        assert_eq!(full.reason, "queue full");
        assert_eq!(full.entry, b"d");
        let oversized = submitter.submit(b"12345".to_vec()).unwrap_err();
        assert_eq!(oversized.reason, "entry too large");
        for receipt in [&first, &second, &third] {
            assert!(matches!(receipt.try_recv(), Err(mpsc::TryRecvError::Empty)));
        }
        drop(submitter); // Drains already accepted work, even during blocked CAS.
        release.send(()).unwrap();
        for (lsn, receipt) in [first, second, third].into_iter().enumerate() {
            assert_eq!(
                receipt
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap()
                    .unwrap(),
                lsn as u64
            );
        }
        let report = worker.join().unwrap().unwrap();
        assert_eq!(report.acknowledged, 3);
        assert_eq!(report.commits, 2);
        let reader = Replica::open(store, Count, Options::default()).unwrap();
        assert_eq!(*reader.state(), 3);
    }

    #[test]
    fn uncertain_batch_is_reported_once_and_queued_entries_are_not_applied() {
        let (store, started, release) = held(true);
        let wal = WalTier::open(store.clone(), Count, Options::default()).unwrap();
        let (submitter, rx) = queue(2, 4);
        let stopped = submitter.stopped.clone();
        let first = submitter.submit(b"a".to_vec()).unwrap();
        let worker = thread::spawn(move || run_writer(wal, rx, stopped, 2));
        started.recv_timeout(Duration::from_secs(5)).unwrap();
        let second = submitter.submit(b"b".to_vec()).unwrap();
        release.send(()).unwrap();
        let uncertain = first
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap_err();
        assert_eq!(uncertain.outcome, MutationOutcome::Unknown);
        assert_eq!(uncertain.entries, [b"a".to_vec()]);
        let queued = second
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap_err();
        assert_eq!(queued.outcome, MutationOutcome::NotApplied);
        assert_eq!(queued.entries, [b"b".to_vec()]);
        let rejected = submitter.submit(b"c".to_vec()).unwrap_err();
        assert_eq!(rejected.reason, "writer stopped");
        assert_eq!(rejected.entry, b"c");
        drop(submitter);
        let report = worker.join().unwrap().unwrap();
        assert_eq!(report.failed, 2);
        assert_eq!(store.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            *Replica::open(store, Count, Options::default())
                .unwrap()
                .state(),
            1
        );
    }

    #[test]
    fn closed_receiver_returns_unaccepted_entry_to_producer() {
        let (submitter, receiver) = queue(1, 4);
        drop(receiver);
        let error = submitter.submit(b"a".to_vec()).unwrap_err();
        assert_eq!(error.entry, b"a");
        assert_eq!(error.reason, "writer stopped");
    }
}
