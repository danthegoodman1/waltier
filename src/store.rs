//! Object storage abstraction with conditional writes, plus two local
//! implementations: [`MemoryStore`] for tests and [`FsStore`] for
//! single-process development. The S3 implementation lives in `s3.rs`; for
//! operation counters and fault/latency injection, wrap any store in
//! `sim::SimStore`.

use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::StoreError;
use crate::cache::{hex, write_atomic};

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

/// The CAS predicate shared by conditional stores: `expected: None` means
/// create-only (the object must be absent).
pub(crate) fn cas_matches(current: Option<&str>, expected: Option<&str>) -> bool {
    match (current, expected) {
        (None, None) => true,
        (Some(c), Some(e)) => c == e,
        _ => false,
    }
}

pub(crate) fn unique_id() -> io::Result<String> {
    let mut bytes = [0; 16];
    getrandom::fill(&mut bytes).map_err(io::Error::other)?;
    Ok(hex(&bytes))
}

/// Minimal object-store surface WalTier needs. Reads must return coherent
/// data and validators; conditional replacement must be atomic and strongly
/// consistent. A successful PUT is the durable commit point (except the
/// explicitly development-only local stores). ETags are opaque, object-scoped
/// validators: equal validators for one key must imply identical bytes. They
/// may repeat for identical content, as on S3.
///
/// Applications must reserve the WAL and snapshot keys for WalTier. In
/// particular, snapshots are immutable and must never be overwritten or
/// deleted while a writer could still publish a reference to them.
pub trait ObjectStore: Send + Sync {
    /// Stable identity of the backing resource, excluding the object key.
    /// Different backends/resources must never share a namespace even when
    /// their ETags coincide. Return the same value across handles/restarts
    /// only when they address the same storage. Wrappers should forward it.
    /// The default bypasses all cache filesystem operations, including directory
    /// setup and stale-file cleanup.
    fn cache_namespace(&self) -> Option<String> {
        None
    }

    /// Maximum readable/writable object body, if the backend has one. WalTier
    /// caps its image and snapshot budgets to this bound. Wrappers must forward
    /// it. Custom stores must enforce their own allocation and network bounds;
    /// the synchronous trait cannot interrupt a blocked implementation.
    fn max_object_bytes(&self) -> Option<usize> {
        None
    }

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

    /// Unconditional put for app payload objects. WalTier snapshots use
    /// create-only `put_if_match` so key collisions cannot overwrite them.
    fn put(&self, key: &str, data: &[u8]) -> Result<String, StoreError>;

    /// Delete; missing objects are fine.
    fn delete(&self, key: &str) -> Result<(), StoreError>;
}

/// In-memory store for tests.
pub struct MemoryStore {
    objects: Mutex<HashMap<String, Stored>>,
    next_etag: AtomicU64,
    namespace: Option<String>,
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self {
            objects: Mutex::new(HashMap::new()),
            next_etag: AtomicU64::new(0),
            namespace: unique_id().ok().map(|id| format!("memory:{id}")),
        }
    }
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }

    fn fresh_etag(&self) -> String {
        format!("\"{}\"", self.next_etag.fetch_add(1, Ordering::SeqCst))
    }

    /// Sorted keys currently stored.
    pub fn keys(&self) -> Vec<String> {
        let mut keys: Vec<String> = self.objects.lock().unwrap().keys().cloned().collect();
        keys.sort();
        keys
    }
}

impl ObjectStore for MemoryStore {
    fn cache_namespace(&self) -> Option<String> {
        self.namespace.clone()
    }

    fn get(&self, key: &str) -> Result<Option<Stored>, StoreError> {
        Ok(self.objects.lock().unwrap().get(key).cloned())
    }

    fn put_if_match(
        &self,
        key: &str,
        etag: Option<&str>,
        data: &[u8],
    ) -> Result<CondPut, StoreError> {
        let mut objects = self.objects.lock().unwrap();
        if !cas_matches(objects.get(key).map(|s| s.etag.as_str()), etag) {
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
        Ok(etag)
    }

    fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.objects.lock().unwrap().remove(key);
        Ok(())
    }
}

/// Directory-backed store for development and examples. Share a single
/// `Arc<FsStore>`: a held OS file lock rejects a second open of the same root,
/// including from another process. The lock is released when the store drops.
///
/// Each object is one atomically replaced file containing data and validator.
/// This prevents torn publication to concurrent readers, but files/directories
/// are not fsynced: power-loss durability is not promised. The v2 directory
/// layout rejects the old data-plus-`.etag` layout; use a fresh directory.
pub struct FsStore {
    root: PathBuf,
    lock: Mutex<()>,
    _root_lock: fs::File,
    namespace: String,
}

impl FsStore {
    pub fn new(root: impl Into<PathBuf>) -> io::Result<Self> {
        use std::io::{Read, Seek, Write};
        let root = root.into();
        fs::create_dir_all(&root)?;
        let root = fs::canonicalize(root)?;
        let marker = root.join(".waltier-store");
        let mut root_lock = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(marker)?;
        root_lock.try_lock().map_err(io::Error::other)?;
        let mut namespace = String::new();
        root_lock.read_to_string(&mut namespace)?;
        if namespace.is_empty() {
            if fs::read_dir(&root)?
                .any(|entry| entry.map_or(true, |entry| entry.file_name() != ".waltier-store"))
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unrecognized/legacy FsStore directory; use a fresh root",
                ));
            }
            namespace = format!("waltier-fs-v2:{}", unique_id()?);
            root_lock.rewind()?;
            root_lock.write_all(namespace.as_bytes())?;
        } else if !namespace
            .strip_prefix("waltier-fs-v2:")
            .is_some_and(|id| id.len() == 32 && id.bytes().all(|b| b.is_ascii_hexdigit()))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid FsStore identity",
            ));
        }
        // Include the canonical root so copying a directory does not copy its
        // cache identity into an independently mutable store.
        namespace.push(':');
        namespace.push_str(&hex(root.as_os_str().as_encoded_bytes()));
        Ok(Self {
            root,
            lock: Mutex::new(()),
            _root_lock: root_lock,
            namespace,
        })
    }

    fn path(&self, key: &str) -> PathBuf {
        let mut path = self.root.join("objects");
        // Hex is injective; fixed-size chunks respect filename length limits.
        // A separate leaf also distinguishes keys that are prefixes of others.
        for chunk in key.as_bytes().chunks(64) {
            path.push(hex(chunk));
        }
        path.join("object")
    }

    fn read_object(&self, key: &str) -> Result<Option<Stored>, StoreError> {
        let bytes = match fs::read(self.path(key)) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(StoreError::new(format!("read {key}: {e}"))),
        };
        // WFS2 + 32-byte random validator + 8-byte data length + data.
        if bytes.len() < 44
            || &bytes[..4] != b"WFS2"
            || u64::from_le_bytes(bytes[36..44].try_into().unwrap()) != (bytes.len() - 44) as u64
        {
            return Err(StoreError::new(format!("invalid FsStore object: {key}")));
        }
        let etag = String::from_utf8(bytes[4..36].to_vec())
            .map_err(|_| StoreError::new(format!("invalid FsStore validator: {key}")))?;
        Ok(Some(Stored {
            data: bytes[44..].to_vec(),
            etag,
        }))
    }

    fn write_object(&self, key: &str, data: &[u8]) -> Result<String, StoreError> {
        let etag = unique_id().map_err(|e| StoreError::new(format!("create validator: {e}")))?;
        let path = self.path(key);
        fs::create_dir_all(path.parent().expect("object has parent"))
            .map_err(|e| StoreError::new(format!("prepare {key}: {e}")))?;
        write_atomic(
            &path,
            &[
                b"WFS2",
                etag.as_bytes(),
                &(data.len() as u64).to_le_bytes(),
                data,
            ],
        )
        .map_err(|e| StoreError::new(format!("write {key}: {e}")))?;
        Ok(etag)
    }
}

impl ObjectStore for FsStore {
    fn cache_namespace(&self) -> Option<String> {
        Some(self.namespace.clone())
    }

    fn get(&self, key: &str) -> Result<Option<Stored>, StoreError> {
        let _guard = self.lock.lock().unwrap();
        self.read_object(key)
    }

    fn put_if_match(
        &self,
        key: &str,
        etag: Option<&str>,
        data: &[u8],
    ) -> Result<CondPut, StoreError> {
        let _guard = self.lock.lock().unwrap();
        if !cas_matches(
            self.read_object(key)?.as_ref().map(|s| s.etag.as_str()),
            etag,
        ) {
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
        if let Err(e) = fs::remove_file(self.path(key))
            && e.kind() != io::ErrorKind::NotFound
        {
            return Err(StoreError::new(format!("delete {key}: {e}")));
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
    fn fs_object_keys_and_metadata_do_not_alias() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsStore::new(dir.path()).unwrap();
        let long = "x".repeat(1024);
        let keys = [
            "",
            "a/b",
            "a__b",
            "k",
            "k.etag",
            "k.tmp",
            ".waltier-store",
            "../outside",
            "/absolute",
            "objects",
            "é",
            &long,
        ];
        let versions: Vec<_> = keys
            .iter()
            .map(|key| (key, store.put(key, key.as_bytes()).unwrap()))
            .collect();
        for (key, etag) in versions {
            let got = store.get(key).unwrap().unwrap();
            assert_eq!(got.data, key.as_bytes());
            assert_eq!(got.etag, etag);
        }
        store.delete("k.etag").unwrap();
        assert_eq!(store.get("k").unwrap().unwrap().data, b"k");
    }

    #[test]
    fn fs_root_rejects_independent_handles_and_releases_after_drop() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsStore::new(dir.path()).unwrap();
        let etag = store.put("wal", b"history").unwrap();
        let namespace = store.cache_namespace();
        assert!(FsStore::new(dir.path().join(".")).is_err());
        let child = |expected| {
            std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "store::tests::fs_root_child_lock"])
                .env("WALTIER_TEST_LOCK_ROOT", dir.path())
                .env("WALTIER_TEST_LOCK_EXPECT_OPEN", expected)
                .output()
                .unwrap()
        };
        let locked = child("no");
        assert!(
            locked.status.success(),
            "{}",
            String::from_utf8_lossy(&locked.stdout)
        );
        drop(store);
        let released = child("yes");
        assert!(
            released.status.success(),
            "{}",
            String::from_utf8_lossy(&released.stdout)
        );
        let reopened = FsStore::new(dir.path()).unwrap();
        assert_eq!(reopened.cache_namespace(), namespace);
        let got = reopened.get("wal").unwrap().unwrap();
        assert_eq!(got.data, b"history");
        assert_eq!(got.etag, etag);
    }

    #[test]
    fn fs_root_child_lock() {
        let Some(root) = std::env::var_os("WALTIER_TEST_LOCK_ROOT") else {
            return;
        };
        let opened = FsStore::new(root);
        assert_eq!(
            opened.is_ok(),
            std::env::var("WALTIER_TEST_LOCK_EXPECT_OPEN").unwrap() == "yes"
        );
        if let Ok(store) = opened {
            assert_eq!(store.get("wal").unwrap().unwrap().data, b"history");
        }
    }

    #[test]
    fn fs_legacy_roots_are_rejected_without_hiding_history() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("wal"), b"old-image").unwrap();
        fs::write(dir.path().join("wal.etag"), b"old-etag").unwrap();
        assert!(FsStore::new(dir.path()).is_err());
        assert_eq!(fs::read(dir.path().join("wal")).unwrap(), b"old-image");
        assert!(
            FsStore::new(dir.path()).is_err(),
            "failed migration cannot turn into a fresh store"
        );
    }

    #[test]
    fn fs_failed_publication_keeps_existing_objects_coherent() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsStore::new(dir.path()).unwrap();
        let etag = store.put("good", b"history").unwrap();
        // A filesystem obstacle at publication prevents replacing the target;
        // preparing a complete frame does not publish a stray ETag sidecar.
        fs::create_dir_all(store.path("blocked")).unwrap();
        assert!(store.put("blocked", b"candidate").is_err());
        let got = store.get("good").unwrap().unwrap();
        assert_eq!((got.etag, got.data), (etag, b"history".to_vec()));
        fs::remove_dir(store.path("blocked")).unwrap();
        assert!(store.get("blocked").unwrap().is_none());
        assert!(matches!(
            store.put_if_match("blocked", None, b"retry").unwrap(),
            CondPut::Ok { .. }
        ));
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
