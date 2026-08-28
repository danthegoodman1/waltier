//! Local-disk tier: a warm-start cache of the WAL image and the current
//! snapshot. Each file is framed with a checksum over its payload, so a torn
//! or truncated one reads back as a miss. The object store holds the durable
//! copy, so a damaged or missing cache only costs an extra download.

use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

/// Cache-file framing: magic, checksum, payload length, payload.
const MAGIC: &[u8; 4] = b"WTC1";
const HEADER: usize = 4 + 8 + 8;

/// Flatten an object key into a single filename component.
pub(crate) fn escape_key(key: &str) -> String {
    key.replace('/', "__")
}

/// Write `parts` to `path` via a temp file and rename, so readers never see
/// a torn file.
pub(crate) fn write_atomic(path: &Path, parts: &[&[u8]]) -> io::Result<()> {
    let tmp = PathBuf::from(format!("{}.tmp", path.to_string_lossy()));
    let mut file = fs::File::create(&tmp)?;
    for part in parts {
        file.write_all(part)?;
    }
    drop(file);
    fs::rename(&tmp, path)
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
}

impl Cache {
    pub fn new(dir: &Path) -> io::Result<Self> {
        fs::create_dir_all(dir)?;
        Ok(Self {
            dir: dir.to_path_buf(),
        })
    }

    fn wal_path(&self) -> PathBuf {
        self.dir.join("wal.cache")
    }

    fn snap_path(&self, key: &str) -> PathBuf {
        self.dir.join(format!("snap-{}", escape_key(key)))
    }

    /// The cached WAL image and the etag it was fetched under.
    pub fn load_wal(&self) -> Option<(String, Vec<u8>)> {
        let data = read_checked(&self.wal_path())?;
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
        write_checked(&self.wal_path(), &payload);
    }

    pub fn load_snapshot(&self, key: &str) -> Option<Vec<u8>> {
        read_checked(&self.snap_path(key))
    }

    pub fn save_snapshot(&self, key: &str, data: &[u8]) {
        write_checked(&self.snap_path(key), data);
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
        (Cache::new(dir.path()).unwrap(), dir)
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
        let (c, dir) = cache();
        c.save_wal("\"etag-1\"", b"image bytes");
        c.save_snapshot("snap/k", b"snapshot bytes");
        type Load<'a> = &'a dyn Fn() -> Option<Vec<u8>>;
        let load: [(&str, Load); 2] = [
            ("wal.cache", &|| c.load_wal().map(|(_, image)| image)),
            ("snap-snap__k", &|| c.load_snapshot("snap/k")),
        ];

        for (name, load) in load {
            let path = dir.path().join(name);
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
                assert_eq!(load(), None, "{name} from {} bytes", bytes.len());
            }
            fs::write(&path, &good).unwrap();
            assert!(load().is_some(), "{name} must survive being restored");
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
    fn checksum_separates_content_and_length() {
        assert_ne!(checksum(b""), checksum(b"\0"));
        assert_ne!(checksum(b"abcdefgh"), checksum(b"abcdefgi"));
        assert_ne!(checksum(b"abcdefghi"), checksum(b"abcdefgh"));
        assert_eq!(checksum(b"abcdefghij"), checksum(b"abcdefghij"));
    }
}
