//! The WAL image: the byte format of the single CAS'd WAL object.
//!
//! Layout (little-endian):
//! - magic `WTL1`
//! - u8 has_snapshot; if 1: u64 snapshot_lsn, u32 key_len, key (utf8)
//! - u32 entry count, then per entry: u32 len, bytes
//!
//! Entry LSNs are implicit: `snapshot_lsn + 1` (or 0 with no snapshot) plus
//! the entry's index.

use std::collections::VecDeque;

use crate::{Lsn, WalError};

const MAGIC: &[u8; 4] = b"WTL1";

#[derive(Clone, Copy)]
pub(crate) struct ImageLimits {
    pub bytes: usize,
    pub entries: usize,
}

pub(crate) fn check_limit(
    resource: &'static str,
    actual: usize,
    limit: usize,
) -> Result<(), WalError> {
    if actual > limit {
        Err(WalError::LimitExceeded {
            resource,
            limit,
            actual,
        })
    } else {
        Ok(())
    }
}

fn check_lsn(snapshot: Option<&SnapshotRef>, count: usize) -> Option<Lsn> {
    let first = match snapshot {
        Some(s) => s.lsn.checked_add(1)?,
        None => 0,
    };
    first.checked_add(u64::try_from(count).ok()?)
}

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
        let skip = usize::try_from(lsn.saturating_sub(first)).unwrap_or(usize::MAX);
        self.entries
            .iter()
            .enumerate()
            .skip(skip)
            .map(move |(i, e)| (first + i as u64, e.as_slice()))
    }

    pub fn encode(&self, limits: ImageLimits) -> Result<Vec<u8>, WalError> {
        self.encode_view(None, 0, &[], limits)
    }

    /// Validate the entire candidate before allocation or publication. The
    /// authoritative WTL1 layout is unchanged; narrowing conversions below are
    /// safe only after these checks.
    pub fn encode_view(
        &self,
        snapshot: Option<&SnapshotRef>,
        skip: usize,
        extra: &[Vec<u8>],
        limits: ImageLimits,
    ) -> Result<Vec<u8>, WalError> {
        let snapshot = snapshot.or(self.snapshot.as_ref());
        let Some(count) = self
            .entries
            .len()
            .checked_sub(skip)
            .and_then(|kept| kept.checked_add(extra.len()))
        else {
            return Err(WalError::LsnExhausted);
        };
        check_limit("live entries", count, limits.entries.min(u32::MAX as usize))?;
        if check_lsn(snapshot, count).is_none() {
            return Err(WalError::LsnExhausted);
        }
        let mut size = 9usize;
        if let Some(s) = snapshot {
            check_limit("snapshot key bytes", s.key.len(), u32::MAX as usize)?;
            let Some(next_size) = size
                .checked_add(12)
                .and_then(|n| n.checked_add(s.key.len()))
            else {
                return Err(WalError::LsnExhausted);
            };
            size = next_size;
        }
        let entries = self.entries.iter().skip(skip).chain(extra.iter());
        for e in entries.clone() {
            check_limit("entry bytes", e.len(), u32::MAX as usize)?;
            // Construct errors only on failure. Eager `ok_or` can make the
            // compiler drop a WalError on every successful entry check.
            let Some(next_size) = size.checked_add(4).and_then(|n| n.checked_add(e.len())) else {
                return Err(WalError::LsnExhausted);
            };
            size = next_size;
        }
        check_limit("WAL image bytes", size, limits.bytes)?;
        let mut out = Vec::with_capacity(size);
        out.extend_from_slice(MAGIC);
        match snapshot {
            None => out.push(0),
            Some(s) => {
                out.push(1);
                out.extend_from_slice(&s.lsn.to_le_bytes());
                out.extend_from_slice(&(s.key.len() as u32).to_le_bytes());
                out.extend_from_slice(s.key.as_bytes());
            }
        }
        out.extend_from_slice(&(count as u32).to_le_bytes());
        for e in entries {
            out.extend_from_slice(&(e.len() as u32).to_le_bytes());
            out.extend_from_slice(e);
        }
        Ok(out)
    }

    pub fn decode(data: &[u8], limits: ImageLimits) -> Result<Self, WalError> {
        check_limit("WAL image bytes", data.len(), limits.bytes)?;
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
        let count = r.u32()? as usize;
        if check_lsn(snapshot.as_ref(), count).is_none() {
            return Err(WalError::Corrupt("snapshot/entry LSN overflow".into()));
        }
        if count > r.remaining() / 4 {
            return Err(WalError::Corrupt(
                "entry count exceeds remaining body".into(),
            ));
        }
        check_limit("live entries", count, limits.entries)?;
        let mut entries = VecDeque::with_capacity(count);
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
    const LIMITS: ImageLimits = ImageLimits {
        bytes: 4096,
        entries: 100,
    };

    #[test]
    fn roundtrip_empty() {
        let img = WalImage::empty();
        let decoded = WalImage::decode(&img.encode(LIMITS).unwrap(), LIMITS).unwrap();
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
        let decoded = WalImage::decode(&img.encode(LIMITS).unwrap(), LIMITS).unwrap();
        assert_eq!(decoded.snapshot, img.snapshot);
        assert_eq!(decoded.entries, img.entries);
        assert_eq!(decoded.first_lsn(), 42);
        assert_eq!(decoded.tip(), Some(44));
        let got: Vec<(Lsn, &[u8])> = decoded.entries_from(43).collect();
        assert_eq!(got, vec![(43, &b""[..]), (44, &b"three"[..])]);
    }

    #[test]
    fn encode_view_applies_fold_and_extra() {
        let mut img = WalImage::empty();
        img.entries.push_back(b"a".to_vec());
        img.entries.push_back(b"b".to_vec());
        img.entries.push_back(b"c".to_vec());
        let sr = SnapshotRef {
            key: "snap/k".into(),
            lsn: 1,
        };
        let data = img
            .encode_view(Some(&sr), 2, &[b"dd".to_vec(), b"e".to_vec()], LIMITS)
            .unwrap();
        assert_eq!(data.len(), data.capacity(), "capacity hint must be exact");
        let decoded = WalImage::decode(&data, LIMITS).unwrap();
        assert_eq!(decoded.snapshot, Some(sr));
        assert_eq!(decoded.first_lsn(), 2);
        assert_eq!(
            decoded.entries,
            VecDeque::from(vec![b"c".to_vec(), b"dd".to_vec(), b"e".to_vec()])
        );
    }

    #[test]
    fn decode_rejects_garbage() {
        assert!(WalImage::decode(b"nope", LIMITS).is_err());
        assert!(WalImage::decode(b"", LIMITS).is_err());
        let mut ok = WalImage::empty().encode(LIMITS).unwrap();
        ok.push(0);
        assert!(WalImage::decode(&ok, LIMITS).is_err());
    }
    #[test]
    fn malformed_corpus_and_every_truncation_are_rejected() {
        let mut image = WalImage::empty();
        image.snapshot = Some(SnapshotRef {
            key: "s".into(),
            lsn: 5,
        });
        image.entries.push_back(b"abc".to_vec());
        let valid = image.encode(LIMITS).unwrap();
        for end in 0..valid.len() {
            assert!(
                WalImage::decode(&valid[..end], LIMITS).is_err(),
                "truncation {end}"
            );
        }
        let mut invalid = valid.clone();
        invalid[5..13].copy_from_slice(&u64::MAX.to_le_bytes());
        assert!(matches!(
            WalImage::decode(&invalid, LIMITS),
            Err(WalError::Corrupt(_))
        ));
        invalid[5..13].copy_from_slice(&(u64::MAX - 1).to_le_bytes());
        assert!(matches!(
            WalImage::decode(&invalid, LIMITS),
            Err(WalError::Corrupt(_))
        ));
        let mut impossible_count = b"WTL1\x00".to_vec();
        impossible_count.extend_from_slice(&u32::MAX.to_le_bytes());
        assert!(matches!(
            WalImage::decode(&impossible_count, LIMITS),
            Err(WalError::Corrupt(_))
        ));
        // A corpus of mutations must either fail or preserve exact round-trip bytes.
        for index in 0..valid.len() {
            for byte in [0, 1, 127, 255] {
                let mut mutation = valid.clone();
                mutation[index] = byte;
                if let Ok(decoded) = WalImage::decode(&mutation, LIMITS) {
                    assert_eq!(decoded.encode(LIMITS).unwrap(), mutation);
                }
            }
        }
    }

    #[test]
    fn final_representable_lsn_and_exact_limits_roundtrip() {
        let mut image = WalImage::empty();
        image.snapshot = Some(SnapshotRef {
            key: "s".into(),
            lsn: u64::MAX - 2,
        });
        image.entries.push_back(vec![]);
        let exact = ImageLimits {
            bytes: 26,
            entries: 1,
        };
        let data = image.encode(exact).unwrap();
        let decoded = WalImage::decode(&data, exact).unwrap();
        assert_eq!(decoded.tip(), Some(u64::MAX - 1));
        assert_eq!(decoded.next_lsn(), u64::MAX);
        assert!(matches!(
            image.encode_view(None, 0, &[vec![]], LIMITS),
            Err(WalError::LsnExhausted)
        ));
        assert!(matches!(
            image.encode(ImageLimits { bytes: 25, ..exact }),
            Err(WalError::LimitExceeded { .. })
        ));
        assert!(matches!(
            WalImage::decode(
                &data,
                ImageLimits {
                    entries: 0,
                    ..exact
                }
            ),
            Err(WalError::LimitExceeded { .. })
        ));
    }
}
