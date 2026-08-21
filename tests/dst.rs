#![cfg(feature = "sim")]
//! Deterministic simulation tests.
//!
//! Each run is driven entirely by a seeded RNG: it interleaves writes,
//! refreshes, flushes, compactions, and crash/reopen cycles across two
//! writers and a replica sharing one store, optionally with injected store
//! faults (clean and ambiguous failures). Compactions are waited for
//! immediately after they spawn, so every run is a deterministic function of
//! its seed; the racy window that matters — a completed fold pending
//! installation while other instances write — stays open across steps.
//!
//! The app under test records the entire history of (lsn, entry) in both its
//! state and its snapshots, so an oracle replica on the raw store can check,
//! after every step: the committed history only ever grows, every live
//! instance's state is an exact prefix of it, `apply` is called exactly once
//! per LSN in order (asserted inside the app), and no snapshot object leaks
//! or vanishes.
//!
//! Reproduce a failure with `DST_SEED=<seed>`; `DST_TRACE=1` prints each op;
//! `DST_STEPS` / `DST_SEEDS` scale the runs.

use std::sync::Arc;

use tempfile::TempDir;
use waltier::sim::{Faults, Rng, SimStore};
use waltier::{
    Entry, Lsn, MemoryStore, ObjectStore, Options, Reconcile, Replica, WalApp, WalError, WalStats,
    WalTier,
};

type History = Vec<(Lsn, Vec<u8>)>;

#[derive(Clone, Copy, Debug)]
enum Mode {
    Abort,
    Retry,
    Replace,
}

#[derive(Clone, Copy)]
struct HistoryApp {
    mode: Mode,
    compact_at: u64,
}

fn encode_history(h: &History) -> Vec<u8> {
    let mut out = Vec::new();
    for (lsn, data) in h {
        out.extend_from_slice(&lsn.to_le_bytes());
        out.extend_from_slice(&(data.len() as u32).to_le_bytes());
        out.extend_from_slice(data);
    }
    out
}

fn decode_history(bytes: &[u8]) -> Result<History, WalError> {
    let mut h = History::new();
    let mut pos = 0;
    while pos < bytes.len() {
        let corrupt = || WalError::App("bad history snapshot".into());
        let lsn = u64::from_le_bytes(bytes.get(pos..pos + 8).ok_or_else(corrupt)?.try_into().unwrap());
        let len = u32::from_le_bytes(
            bytes.get(pos + 8..pos + 12).ok_or_else(corrupt)?.try_into().unwrap(),
        ) as usize;
        let data = bytes.get(pos + 12..pos + 12 + len).ok_or_else(corrupt)?.to_vec();
        assert_eq!(lsn, h.len() as u64, "snapshot history must be contiguous from 0");
        h.push((lsn, data));
        pos += 12 + len;
    }
    Ok(h)
}

impl WalApp for HistoryApp {
    type State = History;

    fn init(&self) -> History {
        History::new()
    }

    fn apply(&self, state: &mut History, lsn: Lsn, entry: &[u8]) {
        let expected = state.last().map(|(l, _)| l + 1).unwrap_or(0);
        assert_eq!(lsn, expected, "apply must be called exactly once per LSN, in order");
        state.push((lsn, entry.to_vec()));
    }

    fn restore(&self, snapshot: &[u8]) -> Result<History, WalError> {
        decode_history(snapshot)
    }

    fn compact(&self, base: Option<&[u8]>, entries: &[Entry]) -> Result<Vec<u8>, WalError> {
        let mut h = base.map(decode_history).transpose()?.unwrap_or_default();
        for e in entries {
            self.apply(&mut h, e.lsn, &e.data);
        }
        Ok(encode_history(&h))
    }

    fn should_compact(&self, stats: &WalStats) -> bool {
        stats.live_entries >= self.compact_at
    }

    fn reconcile(&self, _state: &History, pending: &[u8]) -> Reconcile {
        match self.mode {
            Mode::Abort => Reconcile::Abort,
            Mode::Retry => Reconcile::Retry,
            Mode::Replace => Reconcile::Replace([b"R:", pending].concat()),
        }
    }
}

const WRITERS: usize = 2;
const REPLICAS: usize = 1;
const FAULT_PLAN: Faults = Faults { fail_clean: 0.06, fail_ambiguous: 0.04 };

struct Slot<T> {
    instance: Option<T>,
    dir: TempDir,
}

struct Sim {
    seed: u64,
    rng: Rng,
    raw: Arc<MemoryStore>,
    store: Arc<SimStore>,
    app: HistoryApp,
    writers: Vec<Slot<WalTier<HistoryApp>>>,
    replicas: Vec<Slot<Replica<HistoryApp>>>,
    oracle: Replica<HistoryApp>,
    _oracle_dir: TempDir,
    observed: History,
    max_snapshot_lsn: Option<Lsn>,
    expected_orphans: usize,
    faults: bool,
    trace: bool,
    folds_observed: u64,
    payloads: u64,
}

impl Sim {
    fn new(seed: u64, faults: bool) -> Self {
        let mut rng = Rng::new(seed);
        let mode = match rng.below(3) {
            0 => Mode::Abort,
            1 => Mode::Retry,
            _ => Mode::Replace,
        };
        let compact_at = match rng.below(3) {
            0 => u64::MAX, // manual compact_now only
            _ => 2 + rng.below(6),
        };
        let app = HistoryApp { mode, compact_at };
        let raw = Arc::new(MemoryStore::new());
        let store = Arc::new(
            SimStore::new(raw.clone()).with_faults(rng.next_u64(), Faults::default()),
        );

        let mut writers = Vec::new();
        for _ in 0..WRITERS {
            let dir = TempDir::new().unwrap();
            let s: Arc<dyn ObjectStore> = store.clone();
            let w = WalTier::open(s, app, Options::new(dir.path())).unwrap();
            writers.push(Slot { instance: Some(w), dir });
        }
        let mut replicas = Vec::new();
        for _ in 0..REPLICAS {
            let dir = TempDir::new().unwrap();
            let s: Arc<dyn ObjectStore> = store.clone();
            let r = Replica::open(s, app, Options::new(dir.path())).unwrap();
            replicas.push(Slot { instance: Some(r), dir });
        }
        let oracle_dir = TempDir::new().unwrap();
        let s: Arc<dyn ObjectStore> = raw.clone();
        let oracle = Replica::open(s, app, Options::new(oracle_dir.path())).unwrap();

        if faults {
            store.set_faults(FAULT_PLAN);
        }
        Self {
            seed,
            rng,
            raw,
            store,
            app,
            writers,
            replicas,
            oracle,
            _oracle_dir: oracle_dir,
            observed: History::new(),
            max_snapshot_lsn: None,
            expected_orphans: 0,
            faults,
            trace: std::env::var("DST_TRACE").is_ok(),
            folds_observed: 0,
            payloads: 0,
        }
    }

    fn log(&self, step: usize, msg: &str) {
        if self.trace {
            eprintln!("[seed {} step {step}] {msg}", self.seed);
        }
    }

    fn payload(&mut self) -> Vec<u8> {
        self.payloads += 1;
        let mut p = format!("s{}-p{}", self.seed, self.payloads).into_bytes();
        p.resize(p.len() + self.rng.below(48) as usize, b'.');
        p
    }

    /// Only `WalError::Store` may surface, and only while faults are on.
    fn expect_clean<T>(&self, result: Result<T, WalError>, what: &str) {
        if let Err(e) = result {
            assert!(
                self.faults && matches!(e, WalError::Store(_)),
                "seed {}: unexpected {what} error: {e}",
                self.seed
            );
        }
    }

    /// Compactions run to completion immediately so runs stay deterministic;
    /// the pending-fold window stays open across steps regardless.
    fn settle(&mut self, wi: usize) {
        let w = self.writers[wi].instance.as_mut().unwrap();
        if w.compaction_running() {
            w.wait_for_compaction();
        }
        if !self.faults {
            if let Some(e) = w.last_compaction_error() {
                assert!(
                    e.contains("is gone"),
                    "seed {}: unexpected compaction failure: {e}",
                    self.seed
                );
            }
        }
    }

    /// True when the writer slot holds a live instance (reopening it first if
    /// it was crashed; reopening may fail under faults).
    fn writer_alive(&mut self, wi: usize, step: usize) -> bool {
        if self.writers[wi].instance.is_some() {
            return true;
        }
        let s: Arc<dyn ObjectStore> = self.store.clone();
        match WalTier::open(s, self.app, Options::new(self.writers[wi].dir.path())) {
            Ok(w) => {
                self.log(step, &format!("writer {wi} reopened"));
                self.writers[wi].instance = Some(w);
                true
            }
            Err(e) => {
                self.expect_clean::<()>(Err(e), "writer reopen");
                false
            }
        }
    }

    fn step(&mut self, step: usize) {
        let wi = self.rng.below(WRITERS as u64) as usize;
        let ri = self.rng.below(REPLICAS as u64) as usize;
        match self.rng.below(100) {
            0..55 => {
                if self.writer_alive(wi, step) {
                    let entry = self.payload();
                    let resubmit = self.rng.chance(0.5);
                    let w = self.writers[wi].instance.as_mut().unwrap();
                    match w.write(entry.clone()) {
                        Ok(lsn) => self.log(step, &format!("writer {wi} wrote lsn {lsn}")),
                        Err(WalError::Conflict { entry: back }) => {
                            assert!(
                                matches!(self.app.mode, Mode::Abort),
                                "seed {}: only Abort mode may surface Conflict here",
                                self.seed
                            );
                            assert_eq!(back, entry, "Conflict must hand the entry back");
                            self.log(step, &format!("writer {wi} conflict"));
                            if resubmit {
                                let w = self.writers[wi].instance.as_mut().unwrap();
                                let second = w.write(back);
                                if !self.faults {
                                    second.expect("resubmit after refresh must succeed");
                                } else {
                                    self.expect_clean(second, "resubmit");
                                }
                            }
                        }
                        Err(e) => self.expect_clean::<()>(Err(e), "write"),
                    }
                    self.settle(wi);
                }
            }
            55..63 => {
                if self.writer_alive(wi, step) {
                    let r = self.writers[wi].instance.as_mut().unwrap().flush();
                    self.expect_clean(r, "flush");
                    self.log(step, &format!("writer {wi} flush"));
                }
            }
            63..70 => {
                if self.writer_alive(wi, step) {
                    let r = self.writers[wi].instance.as_mut().unwrap().refresh();
                    self.expect_clean(r, "refresh");
                    self.log(step, &format!("writer {wi} refresh"));
                }
            }
            70..78 => {
                if self.writer_alive(wi, step) {
                    let started = self.writers[wi].instance.as_mut().unwrap().compact_now();
                    self.log(step, &format!("writer {wi} compact_now → {started}"));
                    self.settle(wi);
                }
            }
            78..88 => {
                let slot = &mut self.replicas[ri];
                match &mut slot.instance {
                    Some(r) => {
                        let changed = r.refresh();
                        self.expect_clean(changed, "replica refresh");
                        self.log(step, &format!("replica {ri} refresh"));
                    }
                    None => {
                        let s: Arc<dyn ObjectStore> = self.store.clone();
                        match Replica::open(s, self.app, Options::new(slot.dir.path())) {
                            Ok(r) => {
                                slot.instance = Some(r);
                                self.log(step, &format!("replica {ri} reopened"));
                            }
                            Err(e) => self.expect_clean::<()>(Err(e), "replica reopen"),
                        }
                    }
                }
            }
            88..94 => {
                if let Some(w) = self.writers[wi].instance.take() {
                    assert!(!w.compaction_running(), "settle keeps this false at step boundaries");
                    if w.has_pending_fold() {
                        // Its uploaded-but-uninstalled snapshot is now an orphan.
                        self.expected_orphans += 1;
                    }
                    drop(w);
                    if self.rng.chance(0.5) {
                        self.writers[wi].dir = TempDir::new().unwrap(); // cold restart
                    }
                    self.log(step, &format!("writer {wi} crashed"));
                }
            }
            _ => {
                if self.replicas[ri].instance.take().is_some() {
                    if self.rng.chance(0.5) {
                        self.replicas[ri].dir = TempDir::new().unwrap();
                    }
                    self.log(step, &format!("replica {ri} crashed"));
                }
            }
        }
        self.check(step);
    }

    /// Invariants, checked after every step via an oracle replica reading the
    /// raw (fault-free) store.
    fn check(&mut self, step: usize) {
        let seed = self.seed;
        self.oracle.refresh().expect("oracle refresh must always succeed");
        let now = self.oracle.state();
        assert!(
            now.len() >= self.observed.len() && now[..self.observed.len()] == self.observed[..],
            "seed {seed} step {step}: committed history must be append-only"
        );
        self.observed = now.clone();

        let stats = self.oracle.stats();
        assert_eq!(
            stats.tip,
            self.observed.last().map(|(l, _)| *l),
            "seed {seed} step {step}: tip must match history"
        );
        if let Some(lsn) = stats.snapshot_lsn {
            assert!(
                self.max_snapshot_lsn.is_none_or(|m| lsn >= m),
                "seed {seed} step {step}: snapshot_lsn went backward"
            );
            if self.max_snapshot_lsn != Some(lsn) {
                self.folds_observed += 1;
            }
            self.max_snapshot_lsn = Some(lsn);
        }

        let prefix_of_observed = |state: &History, who: &str| {
            assert!(
                state.len() <= self.observed.len()
                    && state[..] == self.observed[..state.len()],
                "seed {seed} step {step}: {who} state must be a prefix of the committed history"
            );
        };
        for (i, slot) in self.writers.iter().enumerate() {
            if let Some(w) = &slot.instance {
                prefix_of_observed(w.state(), &format!("writer {i}"));
                assert_eq!(w.tip(), w.state().last().map(|(l, _)| *l));
            }
        }
        for (i, slot) in self.replicas.iter().enumerate() {
            if let Some(r) = &slot.instance {
                prefix_of_observed(r.state(), &format!("replica {i}"));
            }
        }

        // With no faults, snapshot objects are exactly accounted for:
        // the referenced one, live pending folds, and crash orphans.
        if !self.faults {
            let snaps =
                self.raw.keys().iter().filter(|k| k.starts_with("snap/")).count();
            let pending = self
                .writers
                .iter()
                .filter(|s| s.instance.as_ref().is_some_and(|w| w.has_pending_fold()))
                .count();
            let referenced = usize::from(stats.snapshot_lsn.is_some());
            assert_eq!(
                snaps,
                referenced + pending + self.expected_orphans,
                "seed {seed} step {step}: snapshot object leak or loss"
            );
        }
    }

    /// Turn faults off, bring every instance back, and prove the log is
    /// intact and still writable.
    fn finish(mut self, steps: usize) -> u64 {
        self.store.set_faults(Faults::default());
        for wi in 0..WRITERS {
            assert!(self.writer_alive(wi, steps), "reopen must succeed once faults stop");
            let w = self.writers[wi].instance.as_mut().unwrap();
            w.refresh().unwrap();
            w.flush().unwrap();
            let entry = self.payload();
            let w = self.writers[wi].instance.as_mut().unwrap();
            match w.write(entry) {
                Ok(_) => {}
                Err(WalError::Conflict { entry }) => {
                    // Abort mode: the refresh already reconciled; resubmit.
                    self.writers[wi].instance.as_mut().unwrap().write(entry).unwrap();
                }
                Err(e) => panic!("seed {}: final write failed: {e}", self.seed),
            }
            self.settle(wi);
            self.check(steps + wi + 1);
        }

        // A cold bootstrap from nothing but the store sees the same history.
        let dir = TempDir::new().unwrap();
        let s: Arc<dyn ObjectStore> = self.raw.clone();
        let fresh = Replica::open(s, self.app, Options::new(dir.path())).unwrap();
        assert_eq!(fresh.state()[..], self.observed[..], "seed {}", self.seed);
        self.folds_observed
    }
}

fn run(seed: u64, faults: bool) -> u64 {
    println!("dst: seed {seed} faults={faults}");
    let steps: usize =
        std::env::var("DST_STEPS").ok().and_then(|s| s.parse().ok()).unwrap_or(250);
    let mut sim = Sim::new(seed, faults);
    for i in 0..steps {
        sim.step(i);
    }
    sim.finish(steps)
}

fn seed_list(base: u64) -> Vec<u64> {
    if let Ok(s) = std::env::var("DST_SEED") {
        return vec![s.parse().expect("DST_SEED must be a number")];
    }
    let count: u64 = std::env::var("DST_SEEDS").ok().and_then(|s| s.parse().ok()).unwrap_or(40);
    (base..base + count).collect()
}

#[test]
fn dst_interleavings() {
    let folds: u64 = seed_list(0).into_iter().map(|s| run(s, false)).sum();
    assert!(folds > 0, "the sims never exercised compaction");
}

#[test]
fn dst_with_faults() {
    let folds: u64 = seed_list(10_000).into_iter().map(|s| run(s, true)).sum();
    assert!(folds > 0, "the sims never exercised compaction");
}
