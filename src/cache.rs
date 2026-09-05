//! Local-disk tier: a warm-start cache of the WAL image and the current
//! snapshot. Each file is framed with a checksum over its payload, so a torn
//! or truncated one reads back as a miss. The object store holds the durable
//! copy, so a damaged or missing cache only costs an extra download.

use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// Cache-file framing: magic, checksum, payload length, payload.
const MAGIC: &[u8; 4] = b"WTC2";
const HEADER: usize = 4 + 8 + 8;

/// An injective encoding for identity fields and filename components.
pub(crate) fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut out, "{byte:02x}").expect("writing to String cannot fail");
    }
    out
}

/// Atomically publish through an exclusively created sibling temporary file.
/// The file is disposable: this does not promise power-loss durability.
pub(crate) fn write_atomic(path: &Path, parts: &[&[u8]]) -> io::Result<()> {
    write_atomic_with(path, |file| {
        for part in parts {
            file.write_all(part)?;
        }
        Ok(())
    })
}

fn write_atomic_with(
    path: &Path,
    write: impl FnOnce(&mut fs::File) -> io::Result<()>,
) -> io::Result<()> {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let (tmp, mut file) = loop {
        let seq = NEXT.fetch_add(1, Ordering::Relaxed);
        let tmp = parent.join(format!(".waltier-{}-{seq}.tmp", std::process::id()));
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp)
        {
            Ok(file) => break (tmp, file),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e),
        }
    };
    let result = write(&mut file).and_then(|()| {
        drop(file);
        fs::rename(&tmp, path)
    });
    if result.is_err() {
        let _ = fs::remove_file(tmp);
    }
    result
}

/// Catches torn, truncated, and mangled cache files. Not cryptographic.
fn checksum(data: &[u8]) -> u64 {
    const PRIME: u64 = 0x0000_0100_0000_01B3;
    let mut h = 0xcbf2_9ce4_8422_2325_u64 ^ data.len() as u64;
    let mut chunks = data.chunks_exact(8);
    for c in &mut chunks {
        h = (h ^ u64::from_le_bytes(c.try_into().unwrap())).wrapping_mul(PRIME);
        h ^= h >> 29;
    }
    for &b in chunks.remainder() {
        h = (h ^ b as u64).wrapping_mul(PRIME);
    }
    h
}

/// The payload of a cache file, or `None` when the framing or the checksum
/// disagrees with the bytes on disk.
fn read_checked(path: &Path) -> Option<Vec<u8>> {
    let data = fs::read(path).ok()?;
    if data.len() < HEADER || &data[..4] != MAGIC {
        return None;
    }
    let sum = u64::from_le_bytes(data[4..12].try_into().unwrap());
    let len = u64::from_le_bytes(data[12..20].try_into().unwrap());
    let payload = data.get(HEADER..)?;
    if payload.len() as u64 != len || checksum(payload) != sum {
        return None;
    }
    Some(payload.to_vec())
}

fn write_checked(path: &Path, payload: &[u8]) {
    let mut header = Vec::with_capacity(HEADER);
    header.extend_from_slice(MAGIC);
    header.extend_from_slice(&checksum(payload).to_le_bytes());
    header.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    let _ = write_atomic(path, &[&header, payload]);
}

#[derive(Clone)]
pub(crate) struct Cache {
    dir: PathBuf,
    identity: Option<Vec<u8>>,
}

impl Cache {
    pub fn new(dir: &Path, namespace: Option<&str>, wal_key: &str) -> io::Result<Self> {
        fs::create_dir_all(dir)?;
        Ok(Self {
            dir: dir.to_path_buf(),
            identity: namespace
                .map(|ns| format!("{}:{ns}{}:{wal_key}", ns.len(), wal_key.len()).into_bytes()),
        })
    }

    fn wal_path(&self) -> PathBuf {
        self.dir.join("wal.cache")
    }

    fn snap_path(&self, key: &str) -> PathBuf {
        // Filenames are only lookup hints; full identity is checked in the record.
        // A bounded name handles arbitrarily long object keys without ENAMETOOLONG.
        self.dir
            .join(format!("snap-{:016x}", checksum(key.as_bytes())))
    }

    /// The cached WAL image and the etag it was fetched under.
    pub fn load_wal(&self) -> Option<(String, Vec<u8>)> {
        let data = self.load(&self.wal_path(), b"wal")?;
        let n = u16::from_le_bytes([*data.first()?, *data.get(1)?]) as usize;
        let etag = String::from_utf8(data.get(2..2 + n)?.to_vec()).ok()?;
        Some((etag, data[2 + n..].to_vec()))
    }

    pub fn save_wal(&self, etag: &str, image: &[u8]) {
        if etag.len() > u16::MAX as usize {
            return;
        }
        let mut payload = Vec::with_capacity(2 + etag.len() + image.len());
        payload.extend_from_slice(&(etag.len() as u16).to_le_bytes());
        payload.extend_from_slice(etag.as_bytes());
        payload.extend_from_slice(image);
        self.save(&self.wal_path(), b"wal", &payload);
    }

    pub fn load_snapshot(&self, key: &str) -> Option<Vec<u8>> {
        self.load(&self.snap_path(key), key.as_bytes())
    }

    pub fn save_snapshot(&self, key: &str, data: &[u8]) {
        self.save(&self.snap_path(key), key.as_bytes(), data);
    }

    fn load(&self, path: &Path, key: &[u8]) -> Option<Vec<u8>> {
        let identity = self.identity.as_ref()?;
        let data = read_checked(path)?;
        let rest = data.strip_prefix(identity.as_slice())?;
        let key_len = u64::from_le_bytes(rest.get(..8)?.try_into().ok()?);
        if key_len != key.len() as u64 {
            return None;
        }
        Some(rest.get(8..)?.strip_prefix(key)?.to_vec())
    }

    fn save(&self, path: &Path, key: &[u8], data: &[u8]) {
        let Some(identity) = &self.identity else {
            return;
        };
        let mut payload = Vec::with_capacity(identity.len() + 8 + key.len() + data.len());
        payload.extend_from_slice(identity);
        payload.extend_from_slice(&(key.len() as u64).to_le_bytes());
        payload.extend_from_slice(key);
        payload.extend_from_slice(data);
        write_checked(path, &payload);
    }

    pub fn remove_snapshot(&self, key: &str) {
        let _ = fs::remove_file(self.snap_path(key));
    }

    /// Drop every cached snapshot but `keep`. Run on open, so a directory a
    /// previous process left behind does not hold a file per fold forever.
    pub fn retain_snapshot(&self, keep: Option<&str>) {
        let keep = keep.map(|k| self.snap_path(k));
        let Ok(entries) = fs::read_dir(&self.dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let stale = path
                .file_name()
                .is_some_and(|n| n.to_string_lossy().starts_with("snap-"))
                && Some(&path) != keep.as_ref();
            if stale {
                let _ = fs::remove_file(&path);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cache() -> (Cache, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        (Cache::new(dir.path(), Some("test"), "wal").unwrap(), dir)
    }

    #[test]
    fn wal_and_snapshot_roundtrip() {
        let (c, _d) = cache();
        assert_eq!(c.load_wal(), None);
        c.save_wal("\"etag-1\"", b"image bytes");
        assert_eq!(
            c.load_wal(),
            Some(("\"etag-1\"".to_string(), b"image bytes".to_vec()))
        );

        assert_eq!(c.load_snapshot("snap/k"), None);
        c.save_snapshot("snap/k", b"snapshot bytes");
        assert_eq!(c.load_snapshot("snap/k"), Some(b"snapshot bytes".to_vec()));
        c.remove_snapshot("snap/k");
        assert_eq!(c.load_snapshot("snap/k"), None);
    }

    /// Every way a file can be damaged must read back as a miss, never as
    /// bytes the caller would trust.
    #[test]
    fn damaged_files_read_as_misses() {
        let (c, _dir) = cache();
        c.save_wal("\"etag-1\"", b"image bytes");
        c.save_snapshot("snap/k", b"snapshot bytes");
        type Load<'a> = &'a dyn Fn() -> Option<Vec<u8>>;
        let load: [(PathBuf, Load); 2] = [
            (c.wal_path(), &|| c.load_wal().map(|(_, image)| image)),
            (c.snap_path("snap/k"), &|| c.load_snapshot("snap/k")),
        ];

        for (path, load) in load {
            let good = fs::read(&path).unwrap();
            let damaged = [
                good[..good.len() - 1].to_vec(), // truncated tail
                good[..HEADER].to_vec(),         // header only
                good[..HEADER - 1].to_vec(),     // truncated header
                Vec::new(),                      // empty
                [&good[..], b"extra"].concat(),  // trailing bytes
                {
                    let mut d = good.clone(); // a flipped payload bit
                    *d.last_mut().unwrap() ^= 1;
                    d
                },
                {
                    let mut d = good.clone(); // a stale checksum
                    d[4] ^= 1;
                    d
                },
                {
                    let mut d = good.clone(); // wrong magic
                    d[0] = b'X';
                    d
                },
            ];
            for bytes in damaged {
                fs::write(&path, &bytes).unwrap();
                assert_eq!(load(), None, "{path:?} from {} bytes", bytes.len());
            }
            fs::write(&path, &good).unwrap();
            assert!(load().is_some(), "{path:?} must survive being restored");
        }
        assert!(c.load_wal().is_some());
        assert!(c.load_snapshot("snap/k").is_some());
    }

    #[test]
    fn retain_snapshot_sweeps_the_rest() {
        let (c, _d) = cache();
        c.save_wal("\"etag\"", b"image");
        for key in ["snap/a", "snap/b", "snap/c"] {
            c.save_snapshot(key, key.as_bytes());
        }
        c.retain_snapshot(Some("snap/b"));
        assert_eq!(c.load_snapshot("snap/a"), None);
        assert_eq!(c.load_snapshot("snap/b"), Some(b"snap/b".to_vec()));
        assert_eq!(c.load_snapshot("snap/c"), None);
        assert!(c.load_wal().is_some(), "the image cache is untouched");

        c.retain_snapshot(None);
        assert_eq!(c.load_snapshot("snap/b"), None);
        assert!(c.load_wal().is_some());
    }

    #[test]
    fn cache_identity_covers_backend_wal_key_and_snapshot_key() {
        let dir = tempfile::tempdir().unwrap();
        let a = Cache::new(dir.path(), Some("backend-a"), "a/wal").unwrap();
        a.save_wal("same-etag", b"history-a");
        a.save_snapshot("snap/same", b"snapshot-a");
        for (namespace, wal_key) in [
            (Some("backend-b"), "a/wal"),
            (Some("backend-a"), "b/wal"),
            (None, "a/wal"),
        ] {
            let b = Cache::new(dir.path(), namespace, wal_key).unwrap();
            assert_eq!(b.load_wal(), None);
            assert_eq!(b.load_snapshot("snap/same"), None);
        }
        // Even a deliberately misplaced, checksum-valid record must miss.
        fs::copy(a.snap_path("snap/same"), a.snap_path("snap/different")).unwrap();
        assert_eq!(a.load_snapshot("snap/different"), None);
        let unknown = Cache::new(dir.path(), None, "a/wal").unwrap();
        unknown.save_wal("same-etag", b"wrong");
        assert_eq!(a.load_wal().unwrap().1, b"history-a");
    }

    #[test]
    fn shared_cache_writers_never_publish_a_partial_or_foreign_record() {
        let dir = tempfile::tempdir().unwrap();
        std::thread::scope(|scope| {
            for n in 0..8 {
                let dir = dir.path();
                scope.spawn(move || {
                    let key = format!("{n}/wal");
                    let c = Cache::new(dir, Some("backend"), &key).unwrap();
                    let payload = vec![n as u8; 8192];
                    for _ in 0..32 {
                        c.save_wal("same-etag", &payload);
                        c.save_snapshot("same-snapshot-key", &payload);
                        if let Some((_, bytes)) = c.load_wal() {
                            assert_eq!(bytes, payload);
                        }
                        if let Some(bytes) = c.load_snapshot("same-snapshot-key") {
                            assert_eq!(bytes, payload);
                        }
                    }
                });
            }
        });
        assert!(
            fs::read_dir(dir.path())
                .unwrap()
                .all(|e| { !e.unwrap().file_name().to_string_lossy().ends_with(".tmp") })
        );
    }

    #[test]
    fn failed_atomic_preparation_preserves_the_published_version() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("object");
        write_atomic(&path, &[b"old-etag", b"old-data"]).unwrap();
        let error = write_atomic_with(&path, |file| {
            file.write_all(b"new-etag-partial-data")?;
            Err(io::Error::other("injected disk-full during preparation"))
        });
        assert!(error.is_err());
        assert_eq!(fs::read(path).unwrap(), b"old-etagold-data");
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn checksum_separates_content_and_length() {
        assert_ne!(checksum(b""), checksum(b"\0"));
        assert_ne!(checksum(b"abcdefgh"), checksum(b"abcdefgi"));
        assert_ne!(checksum(b"abcdefghi"), checksum(b"abcdefgh"));
        assert_eq!(checksum(b"abcdefghij"), checksum(b"abcdefghij"));
    }
}
