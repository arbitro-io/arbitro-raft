//! Multi-Raft group registry.
//!
//! Owns N `RaftNode` instances keyed by [`GroupId`]. Incoming frames arrive
//! multiplexed on a single transport, are decoded once, and dispatched to
//! the right group by `group_id`.

use std::collections::HashMap;

use crate::{
    GroupId, InboundRaftMessage, RaftError, RaftNode, RaftStorage, RaftTransport,
};

pub struct RaftGroupRegistry<S, T>
where
    S: RaftStorage,
    T: RaftTransport,
{
    groups: HashMap<GroupId, RaftNode<S, T>>,
}

impl<S, T> RaftGroupRegistry<S, T>
where
    S: RaftStorage,
    T: RaftTransport,
{
    pub fn new() -> Self {
        Self { groups: HashMap::new() }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self { groups: HashMap::with_capacity(cap) }
    }

    /// Insert a new group. Panics-free: returns Err if the id is already used.
    pub fn insert(&mut self, id: GroupId, node: RaftNode<S, T>) -> Result<(), RaftError> {
        if self.groups.contains_key(&id) {
            return Err(RaftError::Protocol(format!("duplicate group id {}", id.0)));
        }
        self.groups.insert(id, node);
        Ok(())
    }

    pub fn remove(&mut self, id: GroupId) -> Option<RaftNode<S, T>> {
        self.groups.remove(&id)
    }

    pub fn get(&self, id: GroupId) -> Option<&RaftNode<S, T>> {
        self.groups.get(&id)
    }

    pub fn get_mut(&mut self, id: GroupId) -> Option<&mut RaftNode<S, T>> {
        self.groups.get_mut(&id)
    }

    pub fn len(&self) -> usize {
        self.groups.len()
    }

    pub fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }

    /// Iterate all groups (mut). Useful for tick-level fan-out (heartbeats,
    /// commit-index advance, etc.).
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&GroupId, &mut RaftNode<S, T>)> {
        self.groups.iter_mut()
    }

    /// Dispatch a decoded inbound frame to the group named by `inbound.group_id`.
    /// Unknown group => `Err(RaftError::Protocol(...))` (drop-with-error; caller decides).
    pub async fn dispatch<'a>(&mut self, inbound: InboundRaftMessage<'a>) -> Result<(), RaftError> {
        let gid = inbound.group_id;
        match self.groups.get_mut(&gid) {
            Some(node) => node.handle_inbound(inbound).await,
            None => Err(RaftError::Protocol(format!(
                "no raft group registered for group_id {}",
                gid.0
            ))),
        }
    }
}

impl<S, T> Default for RaftGroupRegistry<S, T>
where
    S: RaftStorage,
    T: RaftTransport,
{
    fn default() -> Self {
        Self::new()
    }
}
