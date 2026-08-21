//! Simulation support: a deterministic RNG and [`SimStore`], an
//! [`ObjectStore`] wrapper that injects faults and latency. Built for
//! deterministic simulation tests and local benchmarks — of WalTier itself
//! and of apps built on it.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::StoreError;
use crate::store::{CondGet, CondPut, ObjectStore, Stored};

/// SplitMix64: small, seedable, deterministic. Same seed, same sequence.
#[derive(Clone)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `0..n`. `n` must be nonzero.
    pub fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }

    /// Uniform in `[0, 1)`.
    pub fn float(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// True with probability `p`.
    pub fn chance(&mut self, p: f64) -> bool {
        p > 0.0 && self.float() < p
    }
}

/// Fault probabilities per store operation.
#[derive(Debug, Clone, Copy, Default)]
pub struct Faults {
    /// Clean failure: the op reports an error and nothing happened.
    pub fail_clean: f64,
    /// Ambiguous failure (mutations only): the op executes, then reports an
    /// error — the S3 "timeout after the write landed" case.
    pub fail_ambiguous: f64,
}

/// Latency model: a fixed round trip plus transfer time for the payload.
#[derive(Debug, Clone, Copy)]
pub struct Latency {
    pub rtt: Duration,
    /// Transfer rate; 0 means unlimited.
    pub bytes_per_sec: u64,
    /// Multiplies each delay by a uniform factor in `[1, 1 + jitter]`.
    pub jitter: f64,
}

impl Default for Latency {
    fn default() -> Self {
        Self {
            rtt: Duration::ZERO,
            bytes_per_sec: 0,
            jitter: 0.0,
        }
    }
}

impl Latency {
    /// A rough S3-like profile: `rtt_ms` round trip, `mbps` MB/s transfer,
    /// 20% jitter.
    pub fn s3_like(rtt_ms: u64, mbps: u64) -> Self {
        Self {
            rtt: Duration::from_millis(rtt_ms),
            bytes_per_sec: mbps * 1_000_000,
            jitter: 0.2,
        }
    }
}

/// Counters of everything that passed through a [`SimStore`].
#[derive(Debug, Clone, Copy, Default)]
pub struct SimStats {
    pub gets: u64,
    pub not_modified: u64,
    pub puts: u64,
    pub cas_failures: u64,
    pub deletes: u64,
    pub bytes_uploaded: u64,
    pub bytes_downloaded: u64,
    pub faults_injected: u64,
}

/// Wraps any [`ObjectStore`] with seeded fault injection, a latency model,
/// and operation counters. With everything at its default this is a
/// transparent passthrough.
pub struct SimStore {
    inner: Arc<dyn ObjectStore>,
    rng: Mutex<Rng>,
    faults: Mutex<Faults>,
    /// One-shot targeted ambiguous failure: the next mutation whose key
    /// contains this string executes and then reports an error.
    targeted: Mutex<Option<String>>,
    latency: Latency,
    counters: Mutex<SimStats>,
}

impl SimStore {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            rng: Mutex::new(Rng::new(0)),
            faults: Mutex::new(Faults::default()),
            targeted: Mutex::new(None),
            latency: Latency::default(),
            counters: Mutex::new(SimStats::default()),
        }
    }

    pub fn with_faults(mut self, seed: u64, faults: Faults) -> Self {
        self.rng = Mutex::new(Rng::new(seed));
        self.faults = Mutex::new(faults);
        self
    }

    pub fn with_latency(mut self, latency: Latency) -> Self {
        self.latency = latency;
        self
    }

    /// Replace the fault probabilities, e.g. to end a fault phase.
    pub fn set_faults(&self, faults: Faults) {
        *self.faults.lock().unwrap() = faults;
    }

    /// Arm a one-shot ambiguous failure for the next mutation whose key
    /// contains `key_part`: it executes, then reports an error.
    pub fn fail_next_mutation_ambiguously(&self, key_part: &str) {
        *self.targeted.lock().unwrap() = Some(key_part.to_string());
    }

    pub fn stats(&self) -> SimStats {
        *self.counters.lock().unwrap()
    }

    fn count(&self, bump: impl FnOnce(&mut SimStats)) {
        bump(&mut self.counters.lock().unwrap());
    }

    fn pause(&self, transfer_bytes: usize) {
        let l = self.latency;
        if l.rtt.is_zero() && (l.bytes_per_sec == 0 || transfer_bytes == 0) {
            return;
        }
        let mut d = l.rtt;
        if l.bytes_per_sec > 0 {
            d += Duration::from_secs_f64(transfer_bytes as f64 / l.bytes_per_sec as f64);
        }
        if l.jitter > 0.0 {
            d = d.mul_f64(1.0 + self.rng.lock().unwrap().float() * l.jitter);
        }
        std::thread::sleep(d);
    }

    fn roll_clean_fault(&self, what: &str, key: &str) -> Result<(), StoreError> {
        let p = self.faults.lock().unwrap().fail_clean;
        if self.rng.lock().unwrap().chance(p) {
            self.count(|c| c.faults_injected += 1);
            self.pause(0);
            return Err(StoreError(format!("simulated clean failure: {what} {key}")));
        }
        Ok(())
    }

    /// Whether a mutation that just executed should report failure anyway.
    fn roll_ambiguous_fault(&self, key: &str) -> bool {
        let mut targeted = self.targeted.lock().unwrap();
        if targeted
            .as_ref()
            .is_some_and(|part| key.contains(part.as_str()))
        {
            *targeted = None;
            self.count(|c| c.faults_injected += 1);
            return true;
        }
        let p = self.faults.lock().unwrap().fail_ambiguous;
        if self.rng.lock().unwrap().chance(p) {
            self.count(|c| c.faults_injected += 1);
            return true;
        }
        false
    }

    fn ambiguous(what: &str, key: &str) -> StoreError {
        StoreError(format!(
            "simulated ambiguous failure: {what} {key} (op executed)"
        ))
    }
}

impl ObjectStore for SimStore {
    fn get(&self, key: &str) -> Result<Option<Stored>, StoreError> {
        self.roll_clean_fault("get", key)?;
        let found = self.inner.get(key)?;
        let bytes = found.as_ref().map_or(0, |s| s.data.len());
        self.count(|c| {
            c.gets += 1;
            c.bytes_downloaded += bytes as u64;
        });
        self.pause(bytes);
        Ok(found)
    }

    fn get_if_changed(&self, key: &str, etag: Option<&str>) -> Result<CondGet, StoreError> {
        self.roll_clean_fault("get", key)?;
        let got = self.inner.get_if_changed(key, etag)?;
        let bytes = match &got {
            CondGet::Changed(s) => s.data.len(),
            CondGet::NotModified | CondGet::Missing => 0,
        };
        self.count(|c| {
            match &got {
                CondGet::NotModified => c.not_modified += 1,
                CondGet::Changed(_) | CondGet::Missing => c.gets += 1,
            }
            c.bytes_downloaded += bytes as u64;
        });
        self.pause(bytes);
        Ok(got)
    }

    fn put_if_match(
        &self,
        key: &str,
        etag: Option<&str>,
        data: &[u8],
    ) -> Result<CondPut, StoreError> {
        self.roll_clean_fault("put", key)?;
        let outcome = self.inner.put_if_match(key, etag, data)?;
        self.pause(data.len());
        match &outcome {
            CondPut::Ok { .. } => {
                self.count(|c| {
                    c.puts += 1;
                    c.bytes_uploaded += data.len() as u64;
                });
                if self.roll_ambiguous_fault(key) {
                    return Err(Self::ambiguous("put", key));
                }
            }
            CondPut::PreconditionFailed => self.count(|c| c.cas_failures += 1),
        }
        Ok(outcome)
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<String, StoreError> {
        self.roll_clean_fault("put", key)?;
        let etag = self.inner.put(key, data)?;
        self.count(|c| {
            c.puts += 1;
            c.bytes_uploaded += data.len() as u64;
        });
        self.pause(data.len());
        if self.roll_ambiguous_fault(key) {
            return Err(Self::ambiguous("put", key));
        }
        Ok(etag)
    }

    fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.roll_clean_fault("delete", key)?;
        self.inner.delete(key)?;
        self.count(|c| c.deletes += 1);
        self.pause(0);
        if self.roll_ambiguous_fault(key) {
            return Err(Self::ambiguous("delete", key));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemoryStore;

    #[test]
    fn rng_is_deterministic_and_spread() {
        let a: Vec<u64> = {
            let mut r = Rng::new(7);
            (0..5).map(|_| r.next_u64()).collect()
        };
        let b: Vec<u64> = {
            let mut r = Rng::new(7);
            (0..5).map(|_| r.next_u64()).collect()
        };
        assert_eq!(a, b);
        let mut r = Rng::new(1);
        let hits = (0..1000).filter(|_| r.chance(0.3)).count();
        assert!((200..400).contains(&hits), "chance(0.3) hit {hits}/1000");
    }

    #[test]
    fn fault_injection_is_deterministic() {
        let run = || {
            let store = SimStore::new(Arc::new(MemoryStore::new())).with_faults(
                42,
                Faults {
                    fail_clean: 0.3,
                    fail_ambiguous: 0.2,
                },
            );
            (0..50)
                .map(|i| store.put(&format!("k{i}"), b"v").is_ok())
                .collect::<Vec<bool>>()
        };
        let a = run();
        assert_eq!(a, run());
        assert!(a.contains(&true) && a.contains(&false));
    }

    #[test]
    fn ambiguous_fault_executes_the_op() {
        let inner = Arc::new(MemoryStore::new());
        let store = SimStore::new(inner.clone());
        store.fail_next_mutation_ambiguously("wal");
        store.put("other", b"x").unwrap();
        store.put("wal", b"y").unwrap_err();
        assert_eq!(inner.get("wal").unwrap().unwrap().data, b"y");
        store.put("wal", b"z").unwrap();
    }

    #[test]
    fn passthrough_counts_ops() {
        let store = SimStore::new(Arc::new(MemoryStore::new()));
        let etag = store.put("k", b"12345").unwrap();
        store.get("k").unwrap().unwrap();
        assert!(matches!(
            store.get_if_changed("k", Some(&etag)).unwrap(),
            CondGet::NotModified
        ));
        store.delete("k").unwrap();
        let s = store.stats();
        assert_eq!((s.puts, s.gets, s.not_modified, s.deletes), (1, 1, 1, 1));
        assert_eq!((s.bytes_uploaded, s.bytes_downloaded), (5, 5));
        assert_eq!(s.faults_injected, 0);
    }
}
