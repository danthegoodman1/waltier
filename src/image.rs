//! The WAL image: the byte format of the single CAS'd WAL object.
//!
//! Layout (little-endian):
//! - magic `WTL1`
//! - u8 has_snapshot; if 1: u64 snapshot_lsn, u32 key_len, key (utf8)
//! - u64 first_lsn (must equal snapshot_lsn + 1, or 0 with no snapshot)
//! - u32 entry count, then per entry: u32 len, bytes

use std::collections::VecDeque;

use crate::{Lsn, WalError};

const MAGIC: &[u8; 4] = b"WTL1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SnapshotRef {
    pub key: String,
    pub lsn: Lsn,
}

#[derive(Debug, Clone)]
pub(crate) struct WalImage {
    pub snapshot: Option<SnapshotRef>,
    /// Entries appended after the snapshot; entry `i` has LSN `first_lsn() + i`.
    pub entries: VecDeque<Vec<u8>>,
}

impl WalImage {
    pub fn empty() -> Self {
        Self {
            snapshot: None,
            entries: VecDeque::new(),
        }
    }

    pub fn first_lsn(&self) -> Lsn {
        self.snapshot.as_ref().map(|s| s.lsn + 1).unwrap_or(0)
    }

    pub fn next_lsn(&self) -> Lsn {
        self.first_lsn() + self.entries.len() as u64
    }

    pub fn tip(&self) -> Option<Lsn> {
        self.next_lsn().checked_sub(1)
    }

    pub fn entry_bytes(&self) -> u64 {
        self.entries.iter().map(|e| e.len() as u64).sum()
    }

    /// Entries with LSN >= `lsn`, in order.
    pub fn entries_from(&self, lsn: Lsn) -> impl Iterator<Item = (Lsn, &[u8])> {
        let first = self.first_lsn();
        let skip = lsn.saturating_sub(first) as usize;
        self.entries
            .iter()
            .enumerate()
            .skip(skip)
            .map(move |(i, e)| (first + i as u64, e.as_slice()))
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(64 + self.entry_bytes() as usize);
        out.extend_from_slice(MAGIC);
        match &self.snapshot {
            None => out.push(0),
            Some(s) => {
                out.push(1);
                out.extend_from_slice(&s.lsn.to_le_bytes());
                out.extend_from_slice(&(s.key.len() as u32).to_le_bytes());
                out.extend_from_slice(s.key.as_bytes());
            }
        }
        out.extend_from_slice(&self.first_lsn().to_le_bytes());
        out.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());
        for e in &self.entries {
            out.extend_from_slice(&(e.len() as u32).to_le_bytes());
            out.extend_from_slice(e);
        }
        out
    }

    pub fn decode(data: &[u8]) -> Result<Self, WalError> {
        let mut r = Reader { data, pos: 0 };
        if r.take(4)? != MAGIC {
            return Err(WalError::Corrupt("bad magic".into()));
        }
        let snapshot = match r.u8()? {
            0 => None,
            1 => {
                let lsn = r.u64()?;
                let key_len = r.u32()? as usize;
                let key = String::from_utf8(r.take(key_len)?.to_vec())
                    .map_err(|_| WalError::Corrupt("snapshot key is not utf8".into()))?;
                Some(SnapshotRef { key, lsn })
            }
            _ => return Err(WalError::Corrupt("bad snapshot flag".into())),
        };
        let first_lsn = r.u64()?;
        let expected = snapshot.as_ref().map(|s| s.lsn + 1).unwrap_or(0);
        if first_lsn != expected {
            return Err(WalError::Corrupt(format!(
                "first_lsn {first_lsn} disagrees with snapshot (expected {expected})"
            )));
        }
        let count = r.u32()? as usize;
        let mut entries = VecDeque::with_capacity(count.min(r.remaining() / 4 + 1));
        for _ in 0..count {
            let len = r.u32()? as usize;
            entries.push_back(r.take(len)?.to_vec());
        }
        if r.remaining() != 0 {
            return Err(WalError::Corrupt("trailing bytes".into()));
        }
        Ok(Self { snapshot, entries })
    }
}

struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn remaining(&self) -> usize {
        self.data.len() - self.pos
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], WalError> {
        if self.remaining() < n {
            return Err(WalError::Corrupt("truncated".into()));
        }
        let out = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(out)
    }

    fn u8(&mut self) -> Result<u8, WalError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, WalError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64, WalError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_empty() {
        let img = WalImage::empty();
        let decoded = WalImage::decode(&img.encode()).unwrap();
        assert_eq!(decoded.snapshot, None);
        assert!(decoded.entries.is_empty());
        assert_eq!(decoded.first_lsn(), 0);
        assert_eq!(decoded.tip(), None);
    }

    #[test]
    fn roundtrip_full() {
        let mut img = WalImage::empty();
        img.snapshot = Some(SnapshotRef {
            key: "snap/x".into(),
            lsn: 41,
        });
        img.entries.push_back(b"one".to_vec());
        img.entries.push_back(vec![]);
        img.entries.push_back(b"three".to_vec());
        let decoded = WalImage::decode(&img.encode()).unwrap();
        assert_eq!(decoded.snapshot, img.snapshot);
        assert_eq!(decoded.entries, img.entries);
        assert_eq!(decoded.first_lsn(), 42);
        assert_eq!(decoded.tip(), Some(44));
        let got: Vec<(Lsn, &[u8])> = decoded.entries_from(43).collect();
        assert_eq!(got, vec![(43, &b""[..]), (44, &b"three"[..])]);
    }

    #[test]
    fn decode_rejects_garbage() {
        assert!(WalImage::decode(b"nope").is_err());
        assert!(WalImage::decode(b"").is_err());
        let mut ok = WalImage::empty().encode();
        ok.push(0);
        assert!(WalImage::decode(&ok).is_err());
    }
}
