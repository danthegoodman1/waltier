//! Object storage abstraction with conditional writes, plus two local
//! implementations: [`MemoryStore`] for tests and [`FsStore`] for
//! single-process development. The S3 implementation lives in `s3.rs`.

use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::StoreError;

/// An object plus the etag the store assigned to that version of it.
#[derive(Debug, Clone)]
pub struct Stored {
    pub data: Vec<u8>,
    pub etag: String,
}

#[derive(Debug)]
pub enum CondGet {
    Changed(Stored),
    NotModified,
    Missing,
}

#[derive(Debug)]
pub enum CondPut {
    Ok { etag: String },
    PreconditionFailed,
}

/// Minimal object-store surface WalTier needs. Etags are opaque; the store
/// must change them on every successful put.
pub trait ObjectStore: Send + Sync {
    fn get(&self, key: &str) -> Result<Option<Stored>, StoreError>;

    /// Conditional read. `etag: None` behaves like `get`.
    fn get_if_changed(&self, key: &str, etag: Option<&str>) -> Result<CondGet, StoreError> {
        match (self.get(key)?, etag) {
            (None, _) => Ok(CondGet::Missing),
            (Some(s), Some(e)) if s.etag == e => Ok(CondGet::NotModified),
            (Some(s), _) => Ok(CondGet::Changed(s)),
        }
    }

    /// Compare-and-swap put. `etag: Some(e)` succeeds only if the object's
    /// current etag is `e` (If-Match); `etag: None` succeeds only if the
    /// object is absent (If-None-Match: *).
    fn put_if_match(
        &self,
        key: &str,
        etag: Option<&str>,
        data: &[u8],
    ) -> Result<CondPut, StoreError>;

    /// Unconditional put. Used for content that lives at a unique key
    /// (snapshots) and for app payload objects.
    fn put(&self, key: &str, data: &[u8]) -> Result<String, StoreError>;

    /// Delete; missing objects are fine.
    fn delete(&self, key: &str) -> Result<(), StoreError>;
}

/// In-memory store with operation counters, for tests.
#[derive(Default)]
pub struct MemoryStore {
    objects: Mutex<HashMap<String, Stored>>,
    next_etag: AtomicU64,
    full_gets: AtomicU64,
    not_modified_gets: AtomicU64,
    puts: AtomicU64,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }

    fn fresh_etag(&self) -> String {
        format!("\"{}\"", self.next_etag.fetch_add(1, Ordering::SeqCst))
    }

    /// Gets that returned a body.
    pub fn full_gets(&self) -> u64 {
        self.full_gets.load(Ordering::SeqCst)
    }

    /// Conditional gets answered with NotModified.
    pub fn not_modified_gets(&self) -> u64 {
        self.not_modified_gets.load(Ordering::SeqCst)
    }

    /// Successful puts, conditional or not.
    pub fn puts(&self) -> u64 {
        self.puts.load(Ordering::SeqCst)
    }

    /// Sorted keys currently stored.
    pub fn keys(&self) -> Vec<String> {
        let mut keys: Vec<String> = self.objects.lock().unwrap().keys().cloned().collect();
        keys.sort();
        keys
    }
}

impl ObjectStore for MemoryStore {
    fn get(&self, key: &str) -> Result<Option<Stored>, StoreError> {
        let found = self.objects.lock().unwrap().get(key).cloned();
        if found.is_some() {
            self.full_gets.fetch_add(1, Ordering::SeqCst);
        }
        Ok(found)
    }

    fn get_if_changed(&self, key: &str, etag: Option<&str>) -> Result<CondGet, StoreError> {
        let objects = self.objects.lock().unwrap();
        match (objects.get(key), etag) {
            (None, _) => Ok(CondGet::Missing),
            (Some(s), Some(e)) if s.etag == e => {
                self.not_modified_gets.fetch_add(1, Ordering::SeqCst);
                Ok(CondGet::NotModified)
            }
            (Some(s), _) => {
                self.full_gets.fetch_add(1, Ordering::SeqCst);
                Ok(CondGet::Changed(s.clone()))
            }
        }
    }

    fn put_if_match(
        &self,
        key: &str,
        etag: Option<&str>,
        data: &[u8],
    ) -> Result<CondPut, StoreError> {
        let mut objects = self.objects.lock().unwrap();
        let current = objects.get(key).map(|s| s.etag.as_str());
        let matches = match (current, etag) {
            (None, None) => true,
            (Some(c), Some(e)) => c == e,
            _ => false,
        };
        if !matches {
            return Ok(CondPut::PreconditionFailed);
        }
        let new_etag = self.fresh_etag();
        objects.insert(
            key.to_string(),
            Stored {
                data: data.to_vec(),
                etag: new_etag.clone(),
            },
        );
        self.puts.fetch_add(1, Ordering::SeqCst);
        Ok(CondPut::Ok { etag: new_etag })
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<String, StoreError> {
        let etag = self.fresh_etag();
        self.objects.lock().unwrap().insert(
            key.to_string(),
            Stored {
                data: data.to_vec(),
                etag: etag.clone(),
            },
        );
        self.puts.fetch_add(1, Ordering::SeqCst);
        Ok(etag)
    }

    fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.objects.lock().unwrap().remove(key);
        Ok(())
    }
}

/// Directory-backed store for single-process development and examples.
/// CAS is serialized with an in-process lock, so two processes sharing a
/// directory would race; use a real object store for that.
pub struct FsStore {
    root: PathBuf,
    lock: Mutex<()>,
    counter: AtomicU64,
}

impl FsStore {
    pub fn new(root: impl Into<PathBuf>) -> io::Result<Self> {
        let root = root.into();
        fs::create_dir_all(&root)?;
        Ok(Self {
            root,
            lock: Mutex::new(()),
            counter: AtomicU64::new(0),
        })
    }

    fn path(&self, key: &str) -> PathBuf {
        self.root.join(key.replace('/', "__"))
    }

    fn etag_path(&self, key: &str) -> PathBuf {
        self.root.join(format!("{}.etag", key.replace('/', "__")))
    }

    fn fresh_etag(&self) -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        format!(
            "\"{:x}-{}\"",
            nanos,
            self.counter.fetch_add(1, Ordering::SeqCst)
        )
    }

    fn current_etag(&self, key: &str) -> Result<Option<String>, StoreError> {
        match fs::read_to_string(self.etag_path(key)) {
            Ok(s) => Ok(Some(s)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(StoreError(format!("read etag for {key}: {e}"))),
        }
    }

    fn write_object(&self, key: &str, data: &[u8]) -> Result<String, StoreError> {
        let path = self.path(key);
        let tmp = PathBuf::from(format!("{}.tmp", path.to_string_lossy()));
        fs::write(&tmp, data).map_err(|e| StoreError(format!("write {key}: {e}")))?;
        fs::rename(&tmp, &path).map_err(|e| StoreError(format!("rename {key}: {e}")))?;
        let etag = self.fresh_etag();
        fs::write(self.etag_path(key), &etag)
            .map_err(|e| StoreError(format!("write etag for {key}: {e}")))?;
        Ok(etag)
    }
}

impl ObjectStore for FsStore {
    fn get(&self, key: &str) -> Result<Option<Stored>, StoreError> {
        let _guard = self.lock.lock().unwrap();
        let Some(etag) = self.current_etag(key)? else {
            return Ok(None);
        };
        match fs::read(self.path(key)) {
            Ok(data) => Ok(Some(Stored { data, etag })),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(StoreError(format!("read {key}: {e}"))),
        }
    }

    fn put_if_match(
        &self,
        key: &str,
        etag: Option<&str>,
        data: &[u8],
    ) -> Result<CondPut, StoreError> {
        let _guard = self.lock.lock().unwrap();
        let current = self.current_etag(key)?;
        let matches = match (&current, etag) {
            (None, None) => true,
            (Some(c), Some(e)) => c == e,
            _ => false,
        };
        if !matches {
            return Ok(CondPut::PreconditionFailed);
        }
        Ok(CondPut::Ok {
            etag: self.write_object(key, data)?,
        })
    }

    fn put(&self, key: &str, data: &[u8]) -> Result<String, StoreError> {
        let _guard = self.lock.lock().unwrap();
        self.write_object(key, data)
    }

    fn delete(&self, key: &str) -> Result<(), StoreError> {
        let _guard = self.lock.lock().unwrap();
        for path in [self.path(key), self.etag_path(key)] {
            if let Err(e) = fs::remove_file(&path)
                && e.kind() != io::ErrorKind::NotFound
            {
                return Err(StoreError(format!("delete {key}: {e}")));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn exercise(store: &dyn ObjectStore) {
        assert!(store.get("k").unwrap().is_none());

        let CondPut::Ok { etag } = store.put_if_match("k", None, b"v1").unwrap() else {
            panic!("create should succeed on an absent key");
        };
        assert!(matches!(
            store.put_if_match("k", None, b"v2").unwrap(),
            CondPut::PreconditionFailed
        ));
        assert!(matches!(
            store.put_if_match("k", Some("\"wrong\""), b"v2").unwrap(),
            CondPut::PreconditionFailed
        ));

        let got = store.get("k").unwrap().unwrap();
        assert_eq!(got.data, b"v1");
        assert_eq!(got.etag, etag);
        assert!(matches!(
            store.get_if_changed("k", Some(&etag)).unwrap(),
            CondGet::NotModified
        ));

        let CondPut::Ok { etag: etag2 } = store.put_if_match("k", Some(&etag), b"v2").unwrap()
        else {
            panic!("matching CAS should succeed");
        };
        assert_ne!(etag, etag2);
        match store.get_if_changed("k", Some(&etag)).unwrap() {
            CondGet::Changed(s) => assert_eq!(s.data, b"v2"),
            other => panic!("expected Changed, got {other:?}"),
        }

        store.delete("k").unwrap();
        assert!(store.get("k").unwrap().is_none());
        assert!(matches!(
            store.get_if_changed("k", Some(&etag2)).unwrap(),
            CondGet::Missing
        ));
        store.delete("k").unwrap();
    }

    #[test]
    fn memory_store_semantics() {
        exercise(&MemoryStore::new());
    }

    #[test]
    fn fs_store_semantics() {
        let dir = tempfile::tempdir().unwrap();
        exercise(&FsStore::new(dir.path()).unwrap());
    }
}
