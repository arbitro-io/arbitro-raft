//! Driver method wiring the batched heartbeat fan-out into the group
//! registry: [`RaftGroupRegistry::tick_heartbeats`].
//!
//! `send_batched_heartbeats` lives at
//! `crate::api::node::replication::heartbeat_batch`; `node::replication` is
//! `pub(crate)`, so the one compiled copy is imported directly (dup-J2 —
//! this file previously re-included the source via `#[path]`, compiling it
//! twice).
use crate::api::node::replication::heartbeat_batch as heartbeat_batch_inline;

use std::sync::Arc;

use crate::{RaftError, RaftStorage, RaftTransport, StateMachine};

use super::RaftGroupRegistry;

impl<S, T, SM> RaftGroupRegistry<S, T, SM>
where
    S: RaftStorage,
    T: RaftTransport,
    SM: StateMachine,
{
    /// Emit heartbeats for every group where this node is the leader,
    /// coalescing per-peer sends so the transport sees ONE vectored write
    /// per unique peer regardless of group count.
    ///
    /// Returns the total number of (group, peer) heartbeat frames enqueued.
    pub async fn tick_heartbeats(&mut self, transport: &Arc<T>) -> Result<usize, RaftError> {
        let expected_leader_groups = self.len();
        // Split the borrow so `groups` (for the node iterator) and
        // `scratch` (the reusable per-tick buffers) can be held mutably at
        // the same time — going through a method like `iter_mut()` here
        // would borrow all of `self` and conflict with `&mut self.scratch`.
        let RaftGroupRegistry { groups, scratch, .. } = self;
        heartbeat_batch_inline::send_batched_heartbeats(
            groups.iter_mut().map(|(gid, entry)| (*gid, &mut entry.node)),
            scratch,
            transport,
            expected_leader_groups,
        )
        .await
    }
}
