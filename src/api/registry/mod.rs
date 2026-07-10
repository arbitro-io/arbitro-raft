//! Multi-Raft group registry.
//!
//! Owns N `RaftNode` instances keyed by [`GroupId`], along with the
//! [`StateMachine`] bound to each group. Incoming frames arrive multiplexed
//! on a single transport, are decoded once, and dispatched to the right
//! group by `group_id`.

use std::collections::HashMap;

use crate::{
    GroupId, InboundRaftMessage, RaftError, RaftNode, RaftStorage, RaftTransport, StateMachine,
};

pub(crate) mod batch_scratch;
pub mod heartbeats;

pub(crate) use batch_scratch::{BatchScratch, FrameOut};

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
    groups: HashMap<GroupId, GroupEntry<S, T, SM>>,
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
            groups: HashMap::new(),
            scratch: BatchScratch::default(),
        }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            groups: HashMap::with_capacity(cap),
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
