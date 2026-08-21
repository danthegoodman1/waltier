//! Local-disk tier: a warm-start cache of the WAL image and the current
//! snapshot. All operations are best-effort; the object store holds the
//! durable copy, so a lost or stale cache only costs an extra download.

use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

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
        let data = fs::read(self.wal_path()).ok()?;
        let n = u16::from_le_bytes([*data.first()?, *data.get(1)?]) as usize;
        if data.len() < 2 + n {
            return None;
        }
        let etag = String::from_utf8(data[2..2 + n].to_vec()).ok()?;
        Some((etag, data[2 + n..].to_vec()))
    }

    pub fn save_wal(&self, etag: &str, image: &[u8]) {
        if etag.len() > u16::MAX as usize {
            return;
        }
        let mut header = Vec::with_capacity(2 + etag.len());
        header.extend_from_slice(&(etag.len() as u16).to_le_bytes());
        header.extend_from_slice(etag.as_bytes());
        let _ = write_atomic(&self.wal_path(), &[&header, image]);
    }

    pub fn load_snapshot(&self, key: &str) -> Option<Vec<u8>> {
        fs::read(self.snap_path(key)).ok()
    }

    pub fn save_snapshot(&self, key: &str, data: &[u8]) {
        let _ = write_atomic(&self.snap_path(key), &[data]);
    }

    pub fn remove_snapshot(&self, key: &str) {
        let _ = fs::remove_file(self.snap_path(key));
    }
}
