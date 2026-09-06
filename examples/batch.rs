//! Rebuild dependent ID allocations together after a competing writer commits.
use std::sync::Arc;
use waltier::{Lsn, MemoryStore, Options, ReconcileBatch, WalApp, WalTier};

struct Allocator;
struct State {
    next_id: u64,
    allocated: Vec<u64>,
}
impl WalApp for Allocator {
    type State = State;
    fn init(&self) -> State {
        State {
            next_id: 0,
            allocated: vec![],
        }
    }
    fn apply(&self, state: &mut State, _: Lsn, entry: &[u8]) {
        let id = u64::from_le_bytes(entry.try_into().unwrap());
        assert_eq!(id, state.next_id);
        state.allocated.push(id);
        state.next_id += 1;
    }
    fn reconcile_batch(&self, state: &State, pending: &[Vec<u8>]) -> ReconcileBatch {
        ReconcileBatch::Replace(allocations(state.next_id, pending.len()))
    }
}
fn allocations(first: u64, count: usize) -> Vec<Vec<u8>> {
    (first..first + count as u64)
        .map(|id| id.to_le_bytes().to_vec())
        .collect()
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(MemoryStore::new());
    let first_cache = tempfile::tempdir()?;
    let second_cache = tempfile::tempdir()?;
    let mut first = WalTier::open(store.clone(), Allocator, Options::new(first_cache.path()))?;
    let mut second = WalTier::open(store, Allocator, Options::new(second_cache.path()))?;

    // Both callers prepared their commands against next_id = 0.
    let pending = allocations(second.state().next_id, 2);
    first.write_batch(allocations(first.state().next_id, 1))?;
    let accepted = second.write_batch(pending)?;
    assert_eq!(accepted, 1..3);
    assert_eq!(second.state().allocated, [0, 1, 2]);
    println!(
        "accepted LSNs {accepted:?}; allocated IDs {:?}",
        second.state().allocated
    );
    second.close()?;
    first.close()?;
    Ok(())
}
