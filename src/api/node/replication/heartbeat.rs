use super::super::RaftNode;
use crate::{PeerId, RaftError, RaftMessage};
use tracing::info;

impl<S, T> RaftNode<S, T>
where
    S: crate::RaftStorage,
    T: crate::RaftTransport,
{
    /// Build the heartbeat `AppendEntries` wire for a specific peer, without
    /// sending. Returns `Ok(None)` if this node is not the leader for its
    /// group (defensive: batched callers already filter, but we double-check).
    ///
    /// Shared between the single-group [`send_heartbeat_once`] path and the
    /// multi-group batched fan-out in
    /// [`super::heartbeat_batch::send_batched_heartbeats`].
    pub(crate) fn build_heartbeat_wire(
        &mut self,
        peer: PeerId,
    ) -> Result<Option<crate::protocol::AppendEntries>, RaftError> {
        if !self.is_leader() {
            return Ok(None);
        }
        self.ensure_leader_progress_initialized()?;
        let (prev_idx, prev_term, _) = self.build_append_for_peer(peer)?;
        Ok(Some(crate::protocol::AppendEntries {
            term: self.hard_state.current_term.0.into(),
            leader_id: self.config.node_id.0.into(),
            prev_log_index: prev_idx.0.into(),
            prev_log_term: prev_term.0.into(),
            leader_commit: self.soft_state.commit_index.0.into(),
            entry_count: 0.into(),
            _pad: 0.into(),
        }))
    }

    /// Crate-internal accessor for this node's configured peer set.
    ///
    /// Exposed only inside the `node` module tree so the multi-Raft batched
    /// heartbeat path can iterate peers per group without going through
    /// `RaftNode::config` (which is `pub(crate)` on the struct itself).
    pub(crate) fn peers_view(&self) -> &[PeerId] {
        &self.config.peers
    }

    pub async fn send_heartbeat_once(&mut self) -> Result<(), RaftError> {
        if !self.is_leader() {
            return Err(RaftError::NotLeader {
                leader_hint: self
                    .soft_state
                    .leader_id
                    .map(|l| crate::LeaderHint { leader_id: l }),
            });
        }

        self.scratch_peers.clear();
        for peer in self
            .config
            .peers
            .iter()
            .copied()
            .filter(|p| *p != self.config.node_id)
        {
            self.scratch_peers.push(peer);
        }

        let mut sent = 0usize;
        for i in 0..self.scratch_peers.len() {
            let peer = self.scratch_peers[i];
            let req = match self.build_heartbeat_wire(peer)? {
                Some(req) => req,
                None => continue,
            };

            // Heartbeat uses the same vectored path but with empty entries
            let msg = RaftMessage::AppendEntriesVectored(&req, &[]);
            if self.send_message(peer, &msg).await {
                sent += 1;
            }
        }
        info!(
            node_id = self.config.node_id.0,
            term = self.hard_state.current_term.0,
            sent,
            "heartbeat sent"
        );
        Ok(())
    }
}
