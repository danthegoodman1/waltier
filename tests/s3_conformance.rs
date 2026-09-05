#![cfg(feature = "s3")]
//! Opt-in real-service conformance. Normal tests never read credentials or send
//! these requests. On failure, retain the isolated prefix for offline inspection.
use std::collections::BTreeSet;
use std::sync::{Arc, Barrier, Mutex};
use std::time::Duration;
use waltier::{
    CondGet, CondPut, Entry, Lsn, ObjectStore, Options, Replica, S3Config, S3Options, S3Store,
    StoreError, Stored, WalApp, WalError, WalTier,
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
    fn restore(&self, bytes: &[u8]) -> Result<u64, WalError> {
        Ok(u64::from_le_bytes(
            bytes
                .try_into()
                .map_err(|_| WalError::App("bad count snapshot".into()))?,
        ))
    }
    fn compact(&self, base: Option<&[u8]>, entries: &[Entry]) -> Result<Vec<u8>, WalError> {
        let count = base
            .map(|bytes| self.restore(bytes))
            .transpose()?
            .unwrap_or(0);
        Ok((count + entries.len() as u64).to_le_bytes().to_vec())
    }
}
/// Restrict all access to one successfully reserved random prefix and delete
/// only keys whose create-only PUT was confirmed. No automatic Drop cleanup:
/// failures leave evidence, and cannot race a detached worker's late upload.
struct OwnedPrefix {
    inner: S3Store,
    prefix: String,
    created: Mutex<BTreeSet<String>>,
}
impl OwnedPrefix {
    fn check(&self, key: &str) {
        assert!(
            key.starts_with(&self.prefix),
            "request outside reserved conformance prefix"
        );
    }
}
impl ObjectStore for OwnedPrefix {
    fn cache_namespace(&self) -> Option<String> {
        self.inner.cache_namespace()
    }
    fn max_object_bytes(&self) -> Option<usize> {
        self.inner.max_object_bytes()
    }
    fn get(&self, key: &str) -> Result<Option<Stored>, StoreError> {
        self.check(key);
        self.inner.get(key)
    }
    fn get_if_changed(&self, key: &str, etag: Option<&str>) -> Result<CondGet, StoreError> {
        self.check(key);
        self.inner.get_if_changed(key, etag)
    }
    fn put_if_match(
        &self,
        key: &str,
        etag: Option<&str>,
        data: &[u8],
    ) -> Result<CondPut, StoreError> {
        self.check(key);
        if key.contains("/snap/") {
            assert!(etag.is_none(), "snapshot publication must be create-only");
        }
        let result = self.inner.put_if_match(key, etag, data)?;
        if etag.is_none() && matches!(result, CondPut::Ok { .. }) {
            self.created.lock().unwrap().insert(key.into());
        }
        Ok(result)
    }
    fn put(&self, _: &str, _: &[u8]) -> Result<String, StoreError> {
        Err(StoreError::new("conformance objects require conditional creation").not_applied())
    }
    fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.check(key);
        assert!(
            self.created.lock().unwrap().contains(key),
            "test cannot delete an object it did not create"
        );
        self.inner.delete(key)?;
        self.created.lock().unwrap().remove(key);
        Ok(())
    }
}
fn required(name: &str) -> Result<String, std::env::VarError> {
    std::env::var(name)
}

#[test]
#[ignore = "requires explicitly configured isolated S3 test storage; never run by ordinary CI"]
fn real_s3_conditional_publication_and_cold_recovery() -> Result<(), Box<dyn std::error::Error>> {
    let root = required("WALTIER_S3_TEST_PREFIX")?;
    assert!(
        !root.is_empty() && root.ends_with('/'),
        "use a nonempty reserved prefix ending in /"
    );
    let config = S3Config {
        endpoint: required("WALTIER_S3_TEST_ENDPOINT")?,
        region: required("WALTIER_S3_TEST_REGION")?,
        bucket: required("WALTIER_S3_TEST_BUCKET")?,
        access_key: required("WALTIER_S3_TEST_ACCESS_KEY")?,
        secret_key: required("WALTIER_S3_TEST_SECRET_KEY")?,
        path_style: required("WALTIER_S3_TEST_PATH_STYLE")?.parse()?,
    };
    let inner = S3Store::new_with_options(
        config,
        S3Options {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
            max_object_bytes: 1 << 20,
        },
    )?;
    let mut random = [0u8; 16];
    getrandom::fill(&mut random)?;
    let id: String = random.iter().map(|byte| format!("{byte:02x}")).collect();
    let prefix = format!("{root}run-{id}/");
    let marker = format!("{prefix}owner");
    assert!(
        matches!(
            inner.put_if_match(&marker, None, b"WalTier conformance")?,
            CondPut::Ok { .. }
        ),
        "prefix ownership was not acquired"
    );
    eprintln!("S3 conformance reserved prefix: {prefix}");
    let store = Arc::new(OwnedPrefix {
        inner,
        prefix: prefix.clone(),
        created: Mutex::new(BTreeSet::from([marker.clone()])),
    });

    let key = format!("{prefix}condition");
    let CondPut::Ok { etag } = store.put_if_match(&key, None, b"initial")? else {
        panic!("fresh creation failed")
    };
    assert!(matches!(
        store.put_if_match(&key, None, b"overwrite")?,
        CondPut::PreconditionFailed
    ));
    assert!(matches!(
        store.get_if_changed(&key, Some(&etag))?,
        CondGet::NotModified
    ));
    assert_eq!(store.get(&key)?.unwrap().data, b"initial");
    let barrier = Arc::new(Barrier::new(3));
    let results = std::thread::scope(|scope| {
        let handles: Vec<_> = [b"winner-a", b"winner-b"]
            .into_iter()
            .map(|bytes| {
                let store = store.clone();
                let barrier = barrier.clone();
                let etag = etag.clone();
                let key = key.clone();
                scope.spawn(move || {
                    barrier.wait();
                    (bytes, store.put_if_match(&key, Some(&etag), bytes))
                })
            })
            .collect();
        barrier.wait();
        handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>()
    });
    let mut accepted = vec![];
    let mut rejected = 0;
    for (bytes, result) in results {
        match result? {
            CondPut::Ok { etag: changed } => {
                assert_ne!(changed, etag, "changed bytes need a different validator");
                accepted.push(bytes);
            }
            CondPut::PreconditionFailed => rejected += 1,
        }
    }
    assert_eq!((accepted.len(), rejected), (1, 1));
    assert_eq!(store.get(&key)?.unwrap().data, accepted[0]);
    assert!(matches!(
        store.get_if_changed(&key, Some(&etag))?,
        CondGet::Changed(_)
    ));

    let immutable = format!("{prefix}immutable");
    assert!(matches!(
        store.put_if_match(&immutable, None, b"original")?,
        CondPut::Ok { .. }
    ));
    assert!(matches!(
        store.put_if_match(&immutable, None, b"replacement")?,
        CondPut::PreconditionFailed
    ));
    assert_eq!(store.get(&immutable)?.unwrap().data, b"original");

    let wal_prefix = format!("{prefix}log/");
    let opts = || Options {
        prefix: wal_prefix.clone(),
        ..Options::default()
    };
    let mut writer = WalTier::open(store.clone(), Count, opts())?;
    assert_eq!(
        writer.write_batch(vec![b"one".to_vec(), b"two".to_vec(), b"three".to_vec()])?,
        0..3
    );
    assert!(writer.compact_now());
    writer.flush()?; // Joins upload and installs the immutable snapshot.
    assert_eq!(writer.write(b"four")?, 3);
    writer.close()?; // All work drains before cleanup is permitted.
    let cold = Replica::open(store.clone(), Count, opts())?;
    assert_eq!(*cold.state(), 4);
    assert_eq!(cold.stats().snapshot_lsn, Some(2));
    drop(cold);

    // Preserve the reservation until all other confirmed objects are removed.
    let keys = store.created.lock().unwrap().clone();
    for key in keys.iter().filter(|key| *key != &marker) {
        store.delete(key)?;
    }
    store.delete(&marker)?;
    assert!(store.created.lock().unwrap().is_empty());
    Ok(())
}
