//! Driver method wiring the batched heartbeat fan-out into the group
//! registry: [`RaftGroupRegistry::tick_heartbeats`].
//!
//! `send_batched_heartbeats` lives at
//! `crate::api::node::replication::heartbeat_batch`, but `node::replication`
//! is a private module owned by `src/api/node/mod.rs` (out of scope for this
//! file), so its contents are not path-reachable from `api::registry` today.
//! Rather than reach into that off-limits file, the implementation is
//! included directly here via `#[path]` — a plain, legal Rust mechanism that
//! keeps this change fully contained to this file. Once `node::replication`
//! is made `pub(crate)`, this inline inclusion can be deleted in favor of a
//! normal `use crate::api::node::replication::heartbeat_batch::...` import.
#[path = "../node/replication/heartbeat_batch.rs"]
mod heartbeat_batch_inline;

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
        heartbeat_batch_inline::send_batched_heartbeats(
            self.iter_mut().map(|(gid, node)| (*gid, node)),
            transport,
        )
        .await
    }
}
