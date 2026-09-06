//! A last-write-wins key-value store on WalTier, using the local-filesystem
//! ObjectStore. Run with `cargo run --example kv [data-dir]`.

use std::collections::BTreeMap;
use std::sync::Arc;

use waltier::{Entry, FsStore, Lsn, Options, Reconcile, WalApp, WalError, WalStats, WalTier};

type Map = BTreeMap<String, String>;

struct Kv;

fn encode_map(map: &Map) -> Vec<u8> {
    let mut out = String::new();
    for (k, v) in map {
        out.push_str(k);
        out.push('\t');
        out.push_str(v);
        out.push('\n');
    }
    out.into_bytes()
}

fn decode_map(bytes: &[u8]) -> Result<Map, WalError> {
    let text =
        std::str::from_utf8(bytes).map_err(|_| WalError::App("snapshot is not utf8".into()))?;
    let mut map = Map::new();
    for line in text.lines() {
        let (k, v) = line
            .split_once('\t')
            .ok_or_else(|| WalError::App(format!("bad snapshot line: {line}")))?;
        map.insert(k.to_string(), v.to_string());
    }
    Ok(map)
}

impl WalApp for Kv {
    type State = Map;

    fn init(&self) -> Map {
        Map::new()
    }

    fn apply(&self, state: &mut Map, _lsn: Lsn, entry: &[u8]) {
        let text = String::from_utf8_lossy(entry);
        if let Some(rest) = text.strip_prefix("set ") {
            if let Some((k, v)) = rest.split_once(' ') {
                state.insert(k.to_string(), v.to_string());
            }
        } else if let Some(k) = text.strip_prefix("del ") {
            state.remove(k);
        }
    }

    fn restore(&self, snapshot: &[u8]) -> Result<Map, WalError> {
        decode_map(snapshot)
    }

    fn compact(&self, base: Option<&[u8]>, entries: &[Entry]) -> Result<Vec<u8>, WalError> {
        let mut map = base.map(decode_map).transpose()?.unwrap_or_default();
        for e in entries {
            self.apply(&mut map, e.lsn, &e.data);
        }
        Ok(encode_map(&map))
    }

    fn should_compact(&self, stats: &WalStats) -> bool {
        stats.live_entries >= 4
    }

    // Last-write-wins sets and deletes stay valid whatever landed first.
    fn reconcile(&self, _state: &Map, _pending: &[u8]) -> Reconcile {
        Reconcile::Retry
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "kv-demo-data".to_string());
    let store = Arc::new(FsStore::new(format!("{dir}/objects"))?);
    let mut wal = WalTier::open(store, Kv, Options::new(format!("{dir}/cache")))?;

    println!("opened; state = {:?}", wal.state());
    for (k, v) in [
        ("alpha", "1"),
        ("beta", "2"),
        ("alpha", "3"),
        ("gamma", "4"),
        ("delta", "5"),
    ] {
        let lsn = wal.write(format!("set {k} {v}").into_bytes())?;
        println!("lsn {lsn}: set {k} {v}");
    }

    println!("compaction: {:?}", wal.wait_for_compaction()?);
    println!("flush: {:?}", wal.flush()?);
    println!("state = {:?}", wal.state());
    println!("stats = {:?}", wal.stats());
    Ok(())
}
