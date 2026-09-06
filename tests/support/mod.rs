//! Test-only independent storage history and object-lifetime oracle.
//! Shared between settled seeded tests and explicitly overlapped schedules.
#![allow(dead_code)]
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::sync::{Arc, Mutex};
use waltier::{
    CondGet, CondPut, Entry, Lsn, MemoryStore, MutationOutcome, ObjectStore, Reconcile, StoreError,
    Stored, WalApp, WalError, WalStats, WriteError,
};

pub type History = Vec<Vec<u8>>;

/// The application codec intentionally does not use the oracle's parsers.
#[derive(Clone, Copy)]
pub struct HistoryApp {
    pub compact_at: u64,
}
impl WalApp for HistoryApp {
    type State = History;
    fn init(&self) -> History {
        vec![]
    }
    fn apply(&self, state: &mut History, lsn: Lsn, entry: &[u8]) {
        assert_eq!(lsn, state.len() as u64);
        state.push(entry.to_vec());
    }
    fn restore(&self, bytes: &[u8]) -> Result<History, WalError> {
        let mut state = vec![];
        let mut pos = 0;
        while pos < bytes.len() {
            let lsn = u64::from_le_bytes(bytes[pos..pos + 8].try_into().unwrap());
            let len = u32::from_le_bytes(bytes[pos + 8..pos + 12].try_into().unwrap()) as usize;
            self.apply(&mut state, lsn, &bytes[pos + 12..pos + 12 + len]);
            pos += 12 + len;
        }
        Ok(state)
    }
    fn compact(&self, base: Option<&[u8]>, entries: &[Entry]) -> Result<Vec<u8>, WalError> {
        let mut bytes = base.unwrap_or_default().to_vec();
        for entry in entries {
            bytes.extend_from_slice(&entry.lsn.to_le_bytes());
            bytes.extend_from_slice(&(entry.data.len() as u32).to_le_bytes());
            bytes.extend_from_slice(&entry.data);
        }
        Ok(bytes)
    }
    fn should_compact(&self, stats: &WalStats) -> bool {
        stats.live_entries >= self.compact_at
    }
    fn reconcile(&self, _: &History, _: &[u8]) -> Reconcile {
        Reconcile::Retry
    }
}

/// Independent small parser: consume a remaining slice rather than using
/// production WalImage, Replica, or the application's restoration function.
fn take<'a>(remaining: &mut &'a [u8], count: usize) -> Result<&'a [u8], String> {
    if remaining.len() < count {
        return Err("truncated independent fixture".into());
    }
    let (head, tail) = remaining.split_at(count);
    *remaining = tail;
    Ok(head)
}
fn u32_le(bytes: &mut &[u8]) -> Result<usize, String> {
    Ok(u32::from_le_bytes(take(bytes, 4)?.try_into().unwrap()) as usize)
}
fn u64_le(bytes: &mut &[u8]) -> Result<u64, String> {
    Ok(u64::from_le_bytes(take(bytes, 8)?.try_into().unwrap()))
}
#[derive(Clone, Debug)]
pub struct Image {
    pub snapshot: Option<(u64, String)>,
    pub entries: History,
}
pub fn parse_image(mut bytes: &[u8]) -> Result<Image, String> {
    if take(&mut bytes, 4)? != b"WTL1" {
        return Err("bad WTL1 magic".into());
    }
    let snapshot = match take(&mut bytes, 1)?[0] {
        0 => None,
        1 => {
            let lsn = u64_le(&mut bytes)?;
            let length = u32_le(&mut bytes)?;
            let key = std::str::from_utf8(take(&mut bytes, length)?).map_err(|e| e.to_string())?;
            Some((lsn, key.to_owned()))
        }
        _ => return Err("bad snapshot tag".into()),
    };
    let count = u32_le(&mut bytes)?;
    let mut entries = vec![];
    for _ in 0..count {
        let length = u32_le(&mut bytes)?;
        entries.push(take(&mut bytes, length)?.to_vec());
    }
    if !bytes.is_empty() {
        return Err("trailing WTL1 bytes".into());
    }
    Ok(Image { snapshot, entries })
}
pub fn parse_snapshot(mut bytes: &[u8]) -> Result<History, String> {
    let mut history = vec![];
    while !bytes.is_empty() {
        let lsn = u64_le(&mut bytes)?;
        if lsn != history.len() as u64 {
            return Err("non-contiguous snapshot history".into());
        }
        let length = u32_le(&mut bytes)?;
        history.push(take(&mut bytes, length)?.to_vec());
    }
    Ok(history)
}
pub fn grows(before: &History, after: &History) -> Result<(), String> {
    if !after.starts_with(before) {
        return Err("committed history changed or shrank".into());
    }
    Ok(())
}
pub fn check_ack(
    history: &History,
    range: Range<u64>,
    submitted: &[Vec<u8>],
) -> Result<(), String> {
    if range.end.checked_sub(range.start) != Some(submitted.len() as u64)
        || history.get(range.start as usize..range.end as usize) != Some(submitted)
    {
        return Err("acknowledged batch/range is absent, incomplete, or changed".into());
    }
    Ok(())
}
pub fn check_attempt(
    history: &History,
    submitted: &[Vec<u8>],
    outcome: MutationOutcome,
) -> Result<(), String> {
    // Every test submission has unique command IDs and is submitted once.
    let positions: Vec<_> = submitted
        .iter()
        .map(|command| history.iter().position(|e| e == command))
        .collect();
    if positions.iter().all(Option::is_none) {
        return Ok(());
    }
    if outcome == MutationOutcome::NotApplied {
        return Err("rejected batch appeared in history".into());
    }
    let first = positions
        .first()
        .and_then(|p| *p)
        .ok_or("partial uncertain batch")?;
    for (offset, position) in positions.iter().enumerate() {
        if *position != Some(first + offset) {
            return Err("partial or reordered uncertain batch".into());
        }
    }
    // Each uniquely named command occurs once; the adapter never resubmits it.
    for entry in submitted {
        if history
            .iter()
            .filter(|candidate| *candidate == entry)
            .count()
            != 1
        {
            return Err("uncertain batch was duplicated".into());
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Upload {
    InFlight,
    Known,
    Unknown,
    Rejected,
}
#[derive(Clone, Debug)]
struct Candidate {
    alias: usize,
    owner: u8,
    upload: Upload,
    uncertain_install: bool,
}
#[derive(Clone, Copy, Debug, Default)]
struct Owner {
    alive: bool,
    active: bool,
}
#[derive(Default)]
struct Model {
    history: History,
    objects: BTreeMap<String, Vec<u8>>,
    live: Option<String>,
    candidates: BTreeMap<String, Candidate>,
    current: BTreeMap<u8, String>,
    owners: BTreeMap<u8, Owner>,
    submitted: Vec<History>,
    acknowledgements: Vec<(Range<u64>, History)>,
    failures: Vec<(History, MutationOutcome)>,
    trace: Vec<String>,
    installs: usize,
    deletes: usize,
}
#[derive(Clone, Debug)]
pub struct Inventory {
    pub live: Option<String>,
    pub pending: BTreeSet<String>,
    pub orphans: BTreeSet<String>,
    pub uncertain_installs: BTreeSet<String>,
    pub uncertain_uploads: BTreeSet<String>,
}
impl Model {
    fn candidate(&mut self, owner: u8, key: &str) -> &mut Candidate {
        let alias = self.candidates.len();
        self.candidates.entry(key.into()).or_insert(Candidate {
            alias,
            owner,
            upload: Upload::InFlight,
            uncertain_install: false,
        })
    }
    fn label(&self, key: &str) -> String {
        self.candidates.get(key).map_or_else(
            || key.to_owned(),
            |candidate| format!("snapshot{}", candidate.alias),
        )
    }
    fn reconstruct(&self, data: &[u8]) -> Result<(History, Option<String>), String> {
        let image = parse_image(data)?;
        let (mut history, key) = match image.snapshot {
            None => (vec![], None),
            Some((lsn, key)) => {
                let bytes = self
                    .objects
                    .get(&key)
                    .ok_or_else(|| format!("missing live snapshot: {}", self.label(&key)))?;
                let history = parse_snapshot(bytes)?;
                if history.len() as u64 != lsn + 1 {
                    return Err("snapshot LSN disagrees with its independent history".into());
                }
                (history, Some(key))
            }
        };
        history.extend(image.entries);
        Ok((history, key))
    }
    fn deletion_allowed(&self, key: &str) -> Result<(), String> {
        if key == "wal" || self.live.as_deref() == Some(key) {
            return Err("attempted deletion of live WAL/snapshot".into());
        }
        if let Some(candidate) = self.candidates.get(key)
            && let Some(bytes) = self.objects.get(key)
        {
            let owner = self
                .owners
                .get(&candidate.owner)
                .copied()
                .unwrap_or_default();
            let candidate_length = parse_snapshot(bytes)?.len();
            let folded_length = self
                .live
                .as_ref()
                .map(|live| parse_snapshot(&self.objects[live]).map(|history| history.len()))
                .transpose()?
                .unwrap_or(0);
            if owner.alive
                && owner.active
                && self
                    .current
                    .get(&candidate.owner)
                    .is_some_and(|current| current == key)
                && candidate_length > folded_length
            {
                return Err("attempted deletion of a still-installable pending candidate".into());
            }
        }
        Ok(())
    }
    fn inventory(&self) -> Inventory {
        let mut inventory = Inventory {
            live: self.live.clone(),
            pending: BTreeSet::new(),
            orphans: BTreeSet::new(),
            uncertain_installs: BTreeSet::new(),
            uncertain_uploads: BTreeSet::new(),
        };
        for (key, candidate) in &self.candidates {
            if candidate.uncertain_install {
                inventory.uncertain_installs.insert(key.clone());
            }
            if candidate.upload == Upload::Unknown {
                inventory.uncertain_uploads.insert(key.clone());
            }
            if !self.objects.contains_key(key) || self.live.as_ref() == Some(key) {
                continue;
            }
            let owner = self
                .owners
                .get(&candidate.owner)
                .copied()
                .unwrap_or_default();
            if owner.alive && owner.active && self.current.get(&candidate.owner) == Some(key) {
                inventory.pending.insert(key.clone());
            } else {
                inventory.orphans.insert(key.clone());
            }
        }
        inventory
    }
}

/// Place below SimStore so a mutation is observed even if its response is lost.
/// One lock covers the actual mutation and oracle transition, preserving CAS order.
#[derive(Default)]
pub struct RecordingStore {
    raw: MemoryStore,
    model: Mutex<Model>,
}
impl RecordingStore {
    pub fn owner(&self, owner: u8, alive: bool, active: bool) {
        self.model
            .lock()
            .unwrap()
            .owners
            .insert(owner, Owner { alive, active });
    }
    pub fn upload_started(&self, owner: u8, key: &str) {
        let mut model = self.model.lock().unwrap();
        model.candidate(owner, key);
        model.current.insert(owner, key.into());
        model
            .owners
            .entry(owner)
            .or_insert(Owner {
                alive: true,
                active: false,
            })
            .active = true;
        let label = model.label(key);
        model.trace.push(format!("owner{owner} upload {label}"));
    }
    pub fn upload_result(&self, owner: u8, key: &str, result: &Result<CondPut, StoreError>) {
        let mut model = self.model.lock().unwrap();
        let status = match result {
            Ok(CondPut::Ok { .. }) => Upload::Known,
            Err(e) if e.mutation_outcome == MutationOutcome::Unknown => Upload::Unknown,
            _ => Upload::Rejected,
        };
        model.candidate(owner, key).upload = status;
        let label = model.label(key);
        model
            .trace
            .push(format!("{label} upload result {status:?}"));
    }
    pub fn live_snapshot(&self) -> Option<String> {
        self.model.lock().unwrap().live.clone()
    }
    pub fn install_result(
        &self,
        previous: Option<String>,
        data: &[u8],
        result: &Result<CondPut, StoreError>,
    ) {
        if matches!(result, Err(e) if e.mutation_outcome == MutationOutcome::Unknown)
            && let Some((_, key)) = parse_image(data).unwrap().snapshot
            && previous.as_ref() != Some(&key)
        {
            let mut model = self.model.lock().unwrap();
            if let Some(candidate) = model.candidates.get_mut(&key) {
                candidate.uncertain_install = true;
            }
            let label = model.label(&key);
            model.trace.push(format!("uncertain install {label}"));
        }
    }
    pub fn submitting(&self, batch: &History) {
        assert!(!batch.is_empty());
        self.model.lock().unwrap().submitted.push(batch.clone());
    }
    pub fn acknowledge(&self, submitted: History, range: Range<u64>) {
        let mut model = self.model.lock().unwrap();
        check_ack(&model.history, range.clone(), &submitted).unwrap();
        model.trace.push(format!(
            "ack {range:?} {:?}",
            submitted
                .iter()
                .map(|entry| String::from_utf8_lossy(entry))
                .collect::<Vec<_>>()
        ));
        model.acknowledgements.push((range, submitted));
    }
    pub fn failed(&self, submitted: History, error: &WriteError) {
        assert_eq!(
            error.entries, submitted,
            "Retry-only app retains original commands"
        );
        let mut model = self.model.lock().unwrap();
        check_attempt(&model.history, &submitted, error.outcome).unwrap();
        model.trace.push(format!(
            "failure {:?} {:?}",
            error.outcome,
            submitted
                .iter()
                .map(|entry| String::from_utf8_lossy(entry))
                .collect::<Vec<_>>()
        ));
        model.failures.push((submitted, error.outcome));
    }
    pub fn history(&self) -> History {
        self.model.lock().unwrap().history.clone()
    }
    pub fn trace(&self) -> Vec<String> {
        self.model.lock().unwrap().trace.clone()
    }
    pub fn counts(&self) -> (usize, usize, usize, usize) {
        let model = self.model.lock().unwrap();
        (
            model.acknowledgements.len(),
            model
                .failures
                .iter()
                .filter(|(_, o)| *o == MutationOutcome::Unknown)
                .count(),
            model.installs,
            model.deletes,
        )
    }
    pub fn assert_prefix(&self, state: &History) {
        assert!(self.history().starts_with(state));
    }
    pub fn deletion_allowed(&self, key: &str) -> Result<(), String> {
        self.model.lock().unwrap().deletion_allowed(key)
    }
    pub fn inventory(&self) -> Inventory {
        self.model.lock().unwrap().inventory()
    }
    pub fn audit(&self) -> Result<(), String> {
        let model = self.model.lock().unwrap();
        // Compare actual backend bytes/identities, including under injected faults.
        let actual: BTreeMap<_, _> = self
            .raw
            .keys()
            .into_iter()
            .map(|key| {
                let data = self.raw.get(&key).unwrap().unwrap().data;
                (key, data)
            })
            .collect();
        if model
            .live
            .as_ref()
            .is_some_and(|key| !actual.contains_key(key))
        {
            return Err("missing live snapshot in actual backend".into());
        }
        if actual != model.objects {
            return Err("actual object identities/bytes differ from recording".into());
        }
        if let Some(data) = actual.get("wal") {
            let (history, live) = model.reconstruct(data)?;
            if history != model.history || live != model.live {
                return Err("independent reconstruction disagrees with committed history".into());
            }
        }
        for (range, batch) in &model.acknowledgements {
            check_ack(&model.history, range.clone(), batch)?;
        }
        for (batch, outcome) in &model.failures {
            check_attempt(&model.history, batch, *outcome)?;
        }
        let inventory = model.inventory();
        let mut classified = inventory.pending;
        classified.extend(inventory.orphans);
        classified.extend(inventory.live);
        let snapshots: BTreeSet<_> = actual
            .keys()
            .filter(|key| key.starts_with("snap/"))
            .cloned()
            .collect();
        if classified != snapshots {
            return Err("unclassified or missing snapshot identity".into());
        }
        Ok(())
    }
    /// Call only after all handles/compactors have been drained and dropped.
    pub fn offline_sweep(&self) -> usize {
        {
            let model = self.model.lock().unwrap();
            assert!(
                model
                    .owners
                    .values()
                    .all(|owner| !owner.alive && !owner.active),
                "old handles or workers are not drained"
            );
            assert!(
                model
                    .candidates
                    .values()
                    .all(|candidate| candidate.upload != Upload::InFlight),
                "upload remains in flight"
            );
        }
        self.audit().unwrap();
        let inventory = self.inventory();
        assert!(inventory.pending.is_empty());
        let mut orphans: Vec<_> = inventory.orphans.iter().collect();
        {
            let model = self.model.lock().unwrap();
            orphans.sort_by_key(|key| model.candidates[*key].alias);
        }
        for key in orphans {
            self.delete(key).unwrap();
        }
        self.audit().unwrap();
        assert!(self.inventory().orphans.is_empty());
        inventory.orphans.len()
    }
    /// Negative test seam bypasses the recorder to model broken external storage.
    pub fn remove_live_unchecked(&self) {
        let key = self.model.lock().unwrap().live.clone().unwrap();
        self.raw.delete(&key).unwrap();
    }
}
impl ObjectStore for RecordingStore {
    fn cache_namespace(&self) -> Option<String> {
        self.raw.cache_namespace()
    }
    fn get(&self, key: &str) -> Result<Option<Stored>, StoreError> {
        self.raw.get(key)
    }
    fn get_if_changed(&self, key: &str, etag: Option<&str>) -> Result<CondGet, StoreError> {
        self.raw.get_if_changed(key, etag)
    }
    fn put_if_match(
        &self,
        key: &str,
        etag: Option<&str>,
        data: &[u8],
    ) -> Result<CondPut, StoreError> {
        let mut model = self.model.lock().unwrap();
        let result = self.raw.put_if_match(key, etag, data)?;
        if matches!(result, CondPut::Ok { .. }) {
            if key == "wal" {
                let (history, live) = model.reconstruct(data).unwrap();
                grows(&model.history, &history).unwrap();
                let appended = &history[model.history.len()..];
                if !appended.is_empty() {
                    assert!(
                        model.submitted.iter().any(|batch| batch == appended),
                        "CAS contains a partial or unsubmitted batch"
                    );
                    assert!(
                        appended.iter().all(|entry| !model.history.contains(entry)),
                        "a uniquely submitted batch was duplicated"
                    );
                }
                if live != model.live {
                    model.installs += 1;
                }
                let label = live.as_ref().map(|key| model.label(key));
                model
                    .trace
                    .push(format!("CAS history={} live={label:?}", history.len()));
                model.history = history;
                model.live = live;
            } else {
                assert!(key.starts_with("snap/"));
                assert!(
                    !model.objects.contains_key(key),
                    "immutable snapshot overwritten"
                );
                assert!(
                    model.history.starts_with(&parse_snapshot(data).unwrap()),
                    "snapshot changed the committed prefix"
                );
                // Unwrapped writes are allowed only for explicit oracle fixtures.
                model.candidate(255, key);
                let label = model.label(key);
                model.trace.push(format!("published {label}"));
            }
            model.objects.insert(key.into(), data.to_vec());
        }
        Ok(result)
    }
    fn put(&self, _: &str, _: &[u8]) -> Result<String, StoreError> {
        panic!("WAL/snapshots require conditional publication")
    }
    fn delete(&self, key: &str) -> Result<(), StoreError> {
        let mut model = self.model.lock().unwrap();
        model.deletion_allowed(key).unwrap();
        self.raw.delete(key)?;
        model.objects.remove(key);
        model.deletes += 1;
        let label = model.label(key);
        model.trace.push(format!("delete {label}"));
        Ok(())
    }
}

/// Above fault injection, identify candidate ownership and what the caller saw.
pub struct ClientStore {
    pub inner: Arc<dyn ObjectStore>,
    pub recording: Arc<RecordingStore>,
    pub owner: u8,
}
impl ObjectStore for ClientStore {
    fn cache_namespace(&self) -> Option<String> {
        self.inner.cache_namespace()
    }
    fn get(&self, key: &str) -> Result<Option<Stored>, StoreError> {
        self.inner.get(key)
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
        let snapshot = key.starts_with("snap/");
        if snapshot {
            self.recording.upload_started(self.owner, key);
        }
        let previous = if snapshot {
            None
        } else {
            self.recording.live_snapshot()
        };
        let result = self.inner.put_if_match(key, etag, data);
        if snapshot {
            self.recording.upload_result(self.owner, key, &result);
        } else {
            self.recording.install_result(previous, data, &result);
        }
        result
    }
    fn put(&self, key: &str, data: &[u8]) -> Result<String, StoreError> {
        self.inner.put(key, data)
    }
    fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key)
    }
}
