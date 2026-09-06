//! Local-disk tier: a warm-start cache of the WAL image and the current
//! snapshot. Each file is framed with a checksum over its payload, so a torn
//! or truncated one reads back as a miss. The object store holds the durable
//! copy, so a damaged or missing cache only costs an extra download.

use std::fs;
use std::io::{self, Read, Write};
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
    let pid = std::process::id();
    // Reuse the common filename after publication removes it. Exclusive
    // creation still separates overlapping writers and stale temporary files.
    let mut tmp = parent.join(format!(".waltier-{pid}.tmp"));
    let mut file = loop {
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp)
        {
            Ok(file) => break file,
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                let seq = NEXT.fetch_add(1, Ordering::Relaxed);
                tmp = parent.join(format!(".waltier-{pid}-{seq}.tmp"));
            }
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
    checksum_parts(data.len(), &[data])
}

/// Match the WTC2 checksum over concatenated bytes without allocating that
/// concatenation. Carry at most seven bytes across slice boundaries.
fn checksum_parts(total: usize, parts: &[&[u8]]) -> u64 {
    const PRIME: u64 = 0x0000_0100_0000_01B3;
    let mut h = 0xcbf2_9ce4_8422_2325_u64 ^ total as u64;
    let mut tail = [0u8; 8];
    let mut used = 0;
    let word = |h: &mut u64, bytes: &[u8; 8]| {
        *h = (*h ^ u64::from_le_bytes(*bytes)).wrapping_mul(PRIME);
        *h ^= *h >> 29;
    };
    for &part in parts {
        let mut remaining = part;
        if used > 0 {
            let take = (8 - used).min(remaining.len());
            tail[used..used + take].copy_from_slice(&remaining[..take]);
            used += take;
            remaining = &remaining[take..];
            if used == 8 {
                word(&mut h, &tail);
                used = 0;
            }
        }
        let (chunks, remainder) = remaining.as_chunks::<8>();
        for chunk in chunks {
            word(&mut h, chunk);
        }
        tail[used..used + remainder.len()].copy_from_slice(remainder);
        used += remainder.len();
    }
    for &byte in &tail[..used] {
        h = (h ^ byte as u64).wrapping_mul(PRIME);
    }
    h
}

/// The payload of a cache file, or `None` when the framing or the checksum
/// disagrees with the bytes on disk.
fn read_checked(path: &Path, max_payload: usize) -> Option<Vec<u8>> {
    let limit = max_payload.checked_add(HEADER)?;
    let file = fs::File::open(path).ok()?;
    if file.metadata().ok()?.len() > limit as u64 {
        return None;
    }
    // The file can change after metadata; bound the read itself too.
    let mut data = Vec::new();
    file.take(limit as u64 + 1).read_to_end(&mut data).ok()?;
    if data.len() > limit {
        return None;
    }
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

#[derive(Clone)]
pub(crate) struct Cache {
    dir: PathBuf,
    identity: Option<Vec<u8>>,
}

impl Cache {
    pub fn disabled() -> Self {
        Self {
            dir: PathBuf::new(),
            identity: None,
        }
    }

    /// Cache failures are misses, including failure to prepare its directory.
    /// Unknown namespaces must not inspect or modify the filesystem at all.
    pub fn new(dir: &Path, namespace: Option<&str>, wal_key: &str) -> Self {
        let Some(ns) = namespace else {
            return Self::disabled();
        };
        if fs::create_dir_all(dir).is_err() {
            return Self::disabled();
        }
        Self {
            dir: dir.to_path_buf(),
            identity: Some(format!("{}:{ns}{}:{wal_key}", ns.len(), wal_key.len()).into_bytes()),
        }
    }

    pub fn enabled(&self) -> bool {
        self.identity.is_some()
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
    pub fn load_wal(&self, max_bytes: usize) -> Option<(String, Vec<u8>)> {
        self.identity.as_ref()?;
        let data = self.load(
            &self.wal_path(),
            b"wal",
            max_bytes.checked_add(2 + u16::MAX as usize)?,
        )?;
        let n = u16::from_le_bytes([*data.first()?, *data.get(1)?]) as usize;
        let etag = String::from_utf8(data.get(2..2 + n)?.to_vec()).ok()?;
        let image = &data[2 + n..];
        if image.len() > max_bytes {
            return None;
        }
        Some((etag, image.to_vec()))
    }

    pub fn save_wal(&self, etag: &str, image: &[u8]) {
        if self.identity.is_none() {
            return;
        }
        if etag.len() > u16::MAX as usize {
            return;
        }
        self.save(
            &self.wal_path(),
            b"wal",
            &[&(etag.len() as u16).to_le_bytes(), etag.as_bytes()],
            image,
        );
    }

    pub fn load_snapshot(&self, key: &str, max_bytes: usize) -> Option<Vec<u8>> {
        self.identity.as_ref()?;
        self.load(&self.snap_path(key), key.as_bytes(), max_bytes)
    }

    pub fn save_snapshot(&self, key: &str, data: &[u8]) {
        if self.identity.is_none() {
            return;
        }
        self.save(&self.snap_path(key), key.as_bytes(), &[], data);
    }

    fn load(&self, path: &Path, key: &[u8], max_data: usize) -> Option<Vec<u8>> {
        let identity = self.identity.as_ref()?;
        let max_payload = identity
            .len()
            .checked_add(8)?
            .checked_add(key.len())?
            .checked_add(max_data)?;
        let data = read_checked(path, max_payload)?;
        let rest = data.strip_prefix(identity.as_slice())?;
        let key_len = u64::from_le_bytes(rest.get(..8)?.try_into().ok()?);
        if key_len != key.len() as u64 {
            return None;
        }
        Some(rest.get(8..)?.strip_prefix(key)?.to_vec())
    }

    fn save(&self, path: &Path, key: &[u8], fields: &[&[u8]], body: &[u8]) {
        let Some(identity) = &self.identity else {
            return;
        };
        let Some(prefix_len) = [HEADER, identity.len(), 8, key.len()]
            .into_iter()
            .chain(fields.iter().map(|field| field.len()))
            .try_fold(0usize, |total, len| total.checked_add(len))
        else {
            return;
        };
        let Some(payload_len) = (prefix_len - HEADER).checked_add(body.len()) else {
            return;
        };
        // Combine only framing and metadata. Publish with two writes while
        // borrowing the image/snapshot body instead of copying it into a frame.
        let mut prefix = Vec::with_capacity(prefix_len);
        prefix.resize(HEADER, 0);
        prefix.extend_from_slice(identity);
        prefix.extend_from_slice(&(key.len() as u64).to_le_bytes());
        prefix.extend_from_slice(key);
        for field in fields {
            prefix.extend_from_slice(field);
        }
        let sum = checksum_parts(payload_len, &[&prefix[HEADER..], body]);
        prefix[..4].copy_from_slice(MAGIC);
        prefix[4..12].copy_from_slice(&sum.to_le_bytes());
        prefix[12..HEADER].copy_from_slice(&(payload_len as u64).to_le_bytes());
        let _ = write_atomic(path, &[&prefix, body]);
    }

    pub fn remove_snapshot(&self, key: &str) {
        if self.identity.is_none() {
            return;
        }
        let _ = fs::remove_file(self.snap_path(key));
    }

    /// Drop every cached snapshot but `keep`. Run on open, so a directory a
    /// previous process left behind does not hold a file per fold forever.
    pub fn retain_snapshot(&self, keep: Option<&str>) {
        if self.identity.is_none() {
            return;
        }
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
        (Cache::new(dir.path(), Some("test"), "wal"), dir)
    }

    #[test]
    fn wal_and_snapshot_roundtrip() {
        let (c, _d) = cache();
        assert_eq!(c.load_wal(4096), None);
        c.save_wal("\"etag-1\"", b"image bytes");
        assert_eq!(
            c.load_wal(4096),
            Some(("\"etag-1\"".to_string(), b"image bytes".to_vec()))
        );

        assert_eq!(c.load_snapshot("snap/k", 4096), None);
        c.save_snapshot("snap/k", b"snapshot bytes");
        assert_eq!(
            c.load_snapshot("snap/k", 4096),
            Some(b"snapshot bytes".to_vec())
        );
        c.remove_snapshot("snap/k");
        assert_eq!(c.load_snapshot("snap/k", 4096), None);
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
            (c.wal_path(), &|| c.load_wal(4096).map(|(_, image)| image)),
            (c.snap_path("snap/k"), &|| c.load_snapshot("snap/k", 4096)),
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
        assert!(c.load_wal(4096).is_some());
        assert!(c.load_snapshot("snap/k", 4096).is_some());
    }

    #[test]
    fn retain_snapshot_sweeps_the_rest() {
        let (c, _d) = cache();
        c.save_wal("\"etag\"", b"image");
        for key in ["snap/a", "snap/b", "snap/c"] {
            c.save_snapshot(key, key.as_bytes());
        }
        c.retain_snapshot(Some("snap/b"));
        assert_eq!(c.load_snapshot("snap/a", 4096), None);
        assert_eq!(c.load_snapshot("snap/b", 4096), Some(b"snap/b".to_vec()));
        assert_eq!(c.load_snapshot("snap/c", 4096), None);
        assert!(c.load_wal(4096).is_some(), "the image cache is untouched");

        c.retain_snapshot(None);
        assert_eq!(c.load_snapshot("snap/b", 4096), None);
        assert!(c.load_wal(4096).is_some());
    }

    #[test]
    fn cache_identity_covers_backend_wal_key_and_snapshot_key() {
        let dir = tempfile::tempdir().unwrap();
        let a = Cache::new(dir.path(), Some("backend-a"), "a/wal");
        a.save_wal("same-etag", b"history-a");
        a.save_snapshot("snap/same", b"snapshot-a");
        for (namespace, wal_key) in [
            (Some("backend-b"), "a/wal"),
            (Some("backend-a"), "b/wal"),
            (None, "a/wal"),
        ] {
            let b = Cache::new(dir.path(), namespace, wal_key);
            assert_eq!(b.load_wal(4096), None);
            assert_eq!(b.load_snapshot("snap/same", 4096), None);
        }
        // Even a deliberately misplaced, checksum-valid record must miss.
        fs::copy(a.snap_path("snap/same"), a.snap_path("snap/different")).unwrap();
        assert_eq!(a.load_snapshot("snap/different", 4096), None);
        let unknown = Cache::new(dir.path(), None, "a/wal");
        unknown.save_wal("same-etag", b"wrong");
        assert_eq!(a.load_wal(4096).unwrap().1, b"history-a");
    }

    #[test]
    fn shared_cache_writers_never_publish_a_partial_or_foreign_record() {
        let dir = tempfile::tempdir().unwrap();
        std::thread::scope(|scope| {
            for n in 0..8 {
                let dir = dir.path();
                scope.spawn(move || {
                    let key = format!("{n}/wal");
                    let c = Cache::new(dir, Some("backend"), &key);
                    let payload = vec![n as u8; 8192];
                    for _ in 0..32 {
                        c.save_wal("same-etag", &payload);
                        c.save_snapshot("same-snapshot-key", &payload);
                        if let Some((_, bytes)) = c.load_wal(payload.len()) {
                            assert_eq!(bytes, payload);
                        }
                        if let Some(bytes) = c.load_snapshot("same-snapshot-key", payload.len()) {
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
    fn atomic_writers_preserve_an_existing_preferred_temporary_file() {
        let dir = tempfile::tempdir().unwrap();
        let preferred = dir
            .path()
            .join(format!(".waltier-{}.tmp", std::process::id()));
        fs::write(&preferred, b"another writer's temporary bytes").unwrap();
        let path = dir.path().join("object");
        write_atomic(&path, &[b"new version"]).unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"new version");
        assert_eq!(
            fs::read(&preferred).unwrap(),
            b"another writer's temporary bytes"
        );
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 2);
    }

    #[test]
    fn overlapping_atomic_writers_publish_only_their_own_complete_frames() {
        use std::sync::mpsc;
        use std::time::Duration;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("object");
        let (prepared, preparation) = mpsc::channel();
        let (release, released) = mpsc::channel();
        std::thread::scope(|scope| {
            let path = &path;
            let first = scope.spawn(move || {
                write_atomic_with(path, |file| {
                    file.write_all(b"first-")?;
                    prepared.send(()).unwrap();
                    released.recv_timeout(Duration::from_secs(5)).unwrap();
                    file.write_all(b"complete")
                })
            });
            preparation.recv_timeout(Duration::from_secs(5)).unwrap();
            // The first writer owns the preferred name until it is released.
            let second = write_atomic(path, &[b"second-complete"]);
            let published = fs::read(path);
            release.send(()).unwrap();
            first.join().unwrap().unwrap();
            second.unwrap();
            assert_eq!(published.unwrap(), b"second-complete");
        });
        assert_eq!(fs::read(path).unwrap(), b"first-complete");
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[test]
    fn checksum_separates_content_and_length() {
        assert_ne!(checksum(b""), checksum(b"\0"));
        assert_ne!(checksum(b"abcdefgh"), checksum(b"abcdefgi"));
        assert_ne!(checksum(b"abcdefghi"), checksum(b"abcdefgh"));
        assert_eq!(checksum(b"abcdefghij"), checksum(b"abcdefghij"));
    }
    #[test]
    fn streaming_checksum_matches_wtc2_across_every_part_alignment() {
        // Independent legacy checksum spelling anchors format compatibility.
        fn legacy(data: &[u8]) -> u64 {
            let mut hash = 0xcbf2_9ce4_8422_2325_u64 ^ data.len() as u64;
            let (chunks, remainder) = data.as_chunks::<8>();
            for chunk in chunks {
                hash = (hash ^ u64::from_le_bytes(*chunk)).wrapping_mul(0x0000_0100_0000_01B3);
                hash ^= hash >> 29;
            }
            for &byte in remainder {
                hash = (hash ^ byte as u64).wrapping_mul(0x0000_0100_0000_01B3);
            }
            hash
        }
        let bytes: Vec<u8> = (0..97).collect();
        for end in 0..=bytes.len() {
            for split in 0..=end {
                assert_eq!(
                    checksum_parts(end, &[&bytes[..split], &[], &bytes[split..end]]),
                    legacy(&bytes[..end])
                );
            }
            let parts: Vec<&[u8]> = bytes[..end].chunks(3).collect();
            assert_eq!(checksum_parts(end, &parts), legacy(&bytes[..end]));
        }
        let dir = tempfile::tempdir().unwrap();
        for length in 0..16 {
            let cache = Cache::new(dir.path(), Some(&"n".repeat(length)), "unaligned/wal");
            cache.save_wal(&"e".repeat(length), &bytes);
            assert_eq!(cache.load_wal(bytes.len()).unwrap().1, bytes);
            let mut corrupted = fs::read(cache.wal_path()).unwrap();
            *corrupted.last_mut().unwrap() ^= 1;
            fs::write(cache.wal_path(), corrupted).unwrap();
            assert!(cache.load_wal(bytes.len()).is_none());
        }
    }

    #[test]
    fn cache_frames_preserve_wtc2_with_empty_and_large_metadata() {
        fn frame(payload: &[u8]) -> Vec<u8> {
            [
                MAGIC.as_slice(),
                &checksum(payload).to_le_bytes(),
                &(payload.len() as u64).to_le_bytes(),
                payload,
            ]
            .concat()
        }
        for (namespace_len, key_len, etag_len, body_len) in
            [(0, 0, 0, 0), (1, 7, 9, 31), (8192, 16384, 65535, 65536)]
        {
            let dir = tempfile::tempdir().unwrap();
            let namespace = "n".repeat(namespace_len);
            let key = "k".repeat(key_len);
            let etag = "e".repeat(etag_len);
            let body = vec![0x5a; body_len];
            let cache = Cache::new(dir.path(), Some(&namespace), "wal");
            let identity = format!("{namespace_len}:{namespace}3:wal");

            let wal_payload = [
                identity.as_bytes(),
                &3u64.to_le_bytes(),
                b"wal",
                &(etag_len as u16).to_le_bytes(),
                etag.as_bytes(),
                &body,
            ]
            .concat();
            cache.save_wal(&etag, &body);
            assert_eq!(fs::read(cache.wal_path()).unwrap(), frame(&wal_payload));
            assert_eq!(cache.load_wal(body_len), Some((etag, body.clone())));

            let snap_payload = [
                identity.as_bytes(),
                &(key_len as u64).to_le_bytes(),
                key.as_bytes(),
                &body,
            ]
            .concat();
            cache.save_snapshot(&key, &body);
            assert_eq!(
                fs::read(cache.snap_path(&key)).unwrap(),
                frame(&snap_payload)
            );
            assert_eq!(cache.load_snapshot(&key, body_len), Some(body));
        }
    }

    #[test]
    fn byte_budgets_reject_oversized_cache_files_and_allow_exact_bodies() {
        let (c, _dir) = cache();
        c.save_wal("etag", b"four");
        assert_eq!(c.load_wal(4).unwrap().1, b"four");
        assert!(c.load_wal(3).is_none());
        c.save_snapshot("s", b"four");
        assert_eq!(c.load_snapshot("s", 4).unwrap(), b"four");
        assert!(c.load_snapshot("s", 3).is_none());
        // A large sparse file is rejected from its metadata, without reading
        // its body or allocating for its claimed size.
        fs::File::create(c.snap_path("s"))
            .unwrap()
            .set_len(1 << 30)
            .unwrap();
        assert!(c.load_snapshot("s", 4).is_none());
        fs::File::create(c.wal_path())
            .unwrap()
            .set_len(1 << 30)
            .unwrap();
        assert!(c.load_wal(4).is_none());
    }
}
