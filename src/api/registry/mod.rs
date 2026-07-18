//! Multi-Raft group registry.
//!
//! Owns N `RaftNode` instances keyed by [`GroupId`], along with the
//! [`StateMachine`] bound to each group. Incoming frames arrive multiplexed
//! on a single transport, are decoded once, and dispatched to the right
//! group by `group_id`.

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};

use crate::{
    GroupId, InboundRaftMessage, RaftError, RaftNode, RaftStorage, RaftTransport, StateMachine,
};

pub(crate) mod batch_scratch;
pub mod driver;
pub mod heartbeats;

pub(crate) use batch_scratch::{BatchScratch, FrameOut};
pub use driver::MultiRaftDriver;

/// Multiplicative hasher for `GroupId` keys: one `imul` + one shift instead
/// of SipHash, keeping the per-frame group route O(1) with a ~2 ns hash.
/// Not DoS-hardened — group ids are operator-assigned, never attacker-chosen.
#[derive(Clone, Copy, Default)]
pub struct GroupIdHasher(u64);

const FIB: u64 = 0x9e37_79b9_7f4a_7c15;

impl Hasher for GroupIdHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = (self.0 ^ u64::from(b)).wrapping_mul(FIB);
        }
        self.0 ^= self.0 >> 32;
    }
    #[inline]
    fn write_u64(&mut self, n: u64) {
        let x = n.wrapping_mul(FIB);
        self.0 = x ^ (x >> 32);
    }
}

pub type GroupIdBuildHasher = BuildHasherDefault<GroupIdHasher>;
pub(crate) type GroupIdMap<V> = HashMap<GroupId, V, GroupIdBuildHasher>;

pub(crate) struct GroupEntry<S, T, SM> {
    pub(crate) node: RaftNode<S, T>,
    pub(crate) sm: SM,
}

pub struct RaftGroupRegistry<S, T, SM>
where
    S: RaftStorage,
    T: RaftTransport,
    SM: StateMachine,
{
    groups: GroupIdMap<GroupEntry<S, T, SM>>,
    /// Reusable scratch buffers for the batched heartbeat tick — see
    /// [`BatchScratch`]. Cleared (not reallocated) at the start of every
    /// `tick_heartbeats` call.
    scratch: BatchScratch,
}

impl<S, T, SM> RaftGroupRegistry<S, T, SM>
where
    S: RaftStorage,
    T: RaftTransport,
    SM: StateMachine,
{
    pub fn new() -> Self {
        Self {
            groups: GroupIdMap::default(),
            scratch: BatchScratch::default(),
        }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            groups: HashMap::with_capacity_and_hasher(cap, GroupIdBuildHasher::default()),
            scratch: BatchScratch::default(),
        }
    }

    /// Insert a new group. Panics-free: returns Err if the id is already used.
    pub fn insert(&mut self, id: GroupId, node: RaftNode<S, T>, sm: SM) -> Result<(), RaftError> {
        if self.groups.contains_key(&id) {
            return Err(RaftError::Protocol(format!("duplicate group id {}", id.0)));
        }
        self.groups.insert(id, GroupEntry { node, sm });
        Ok(())
    }

    pub fn remove(&mut self, id: GroupId) -> Option<(RaftNode<S, T>, SM)> {
        self.groups.remove(&id).map(|entry| (entry.node, entry.sm))
    }

    pub fn get(&self, id: GroupId) -> Option<&RaftNode<S, T>> {
        self.groups.get(&id).map(|entry| &entry.node)
    }

    pub fn get_mut(&mut self, id: GroupId) -> Option<&mut RaftNode<S, T>> {
        self.groups.get_mut(&id).map(|entry| &mut entry.node)
    }

    /// Whole-entry access (node + state machine) for the run-driver's
    /// per-frame route→step path — one O(1) lookup, no double hash.
    pub(crate) fn entry_mut(&mut self, id: GroupId) -> Option<&mut GroupEntry<S, T, SM>> {
        self.groups.get_mut(&id)
    }

    pub fn state_machine(&self, id: GroupId) -> Option<&SM> {
        self.groups.get(&id).map(|entry| &entry.sm)
    }

    pub fn state_machine_mut(&mut self, id: GroupId) -> Option<&mut SM> {
        self.groups.get_mut(&id).map(|entry| &mut entry.sm)
    }

    pub fn len(&self) -> usize {
        self.groups.len()
    }

    pub fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }

    /// Iterate all groups (mut), exposing only the node. Useful for
    /// tick-level fan-out (heartbeats, commit-index advance, etc.).
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&GroupId, &mut RaftNode<S, T>)> {
        self.groups.iter_mut().map(|(id, entry)| (id, &mut entry.node))
    }

    /// Iterate all groups (mut), exposing both the node and its bound
    /// state machine. Needed by apply loops that drive committed entries
    /// into the state machine.
    pub(crate) fn iter_entries_mut(
        &mut self,
    ) -> impl Iterator<Item = (&GroupId, &mut RaftNode<S, T>, &mut SM)> {
        self.groups
            .iter_mut()
            .map(|(id, entry)| (id, &mut entry.node, &mut entry.sm))
    }

    /// Dispatch a decoded inbound frame to the group named by `inbound.group_id`.
    /// Unknown group => `Err(RaftError::Protocol(...))` (drop-with-error; caller decides).
    pub async fn dispatch<'a>(&mut self, inbound: InboundRaftMessage<'a>) -> Result<(), RaftError> {
        let gid = inbound.group_id;
        match self.groups.get_mut(&gid) {
            Some(entry) => entry.node.handle_inbound(inbound).await,
            None => Err(RaftError::Protocol(format!(
                "no raft group registered for group_id {}",
                gid.0
            ))),
        }
    }
}

impl<S, T, SM> Default for RaftGroupRegistry<S, T, SM>
where
    S: RaftStorage,
    T: RaftTransport,
    SM: StateMachine,
{
    fn default() -> Self {
        Self::new()
    }
}
