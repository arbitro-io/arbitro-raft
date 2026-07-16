use super::super::RaftNode;
use crate::{PeerId, RaftError, RaftMessage};

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

    pub async fn send_heartbeat_once(&mut self) -> Result<(), RaftError> {
        if !self.is_leader() {
            return Err(RaftError::NotLeader {
                leader_hint: self
                    .soft_state
                    .leader_id
                    .map(|l| crate::LeaderHint { leader_id: l }),
            });
        }

        self.ensure_leader_progress_initialized()?;

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

        let last_log = self.cached_last_log.0;
        let mut sent = 0usize;
        for i in 0..self.scratch_peers.len() {
            let peer = self.scratch_peers[i];

            // If the peer is behind, replicate its backlog instead of a bare
            // heartbeat. Plain heartbeats carry no entries, so without this a
            // lagging follower — e.g. a freshly added voter, or one recovering
            // after a partition — would never catch up between client proposals
            // (PS11). `send_append_attempt` ships entries from the peer's
            // next_index; the ack advances its progress on the next tick.
            let behind = self
                .peer_progress
                .get(&peer)
                .is_some_and(|p| p.next_index <= last_log);
            if behind {
                if self.send_append_attempt(peer, 0).await?.is_some() {
                    sent += 1;
                }
                continue;
            }

            let req = match self.build_heartbeat_wire(peer)? {
                Some(req) => req,
                None => continue,
            };
            // Caught-up peer: bare heartbeat (empty entries) on the vectored path.
            let msg = RaftMessage::AppendEntriesVectored(&req, &[]);
            if self.send_message(peer, &msg).await {
                sent += 1;
            }
        }
        tracing::trace!(
            node_id = self.config.node_id.0,
            term = self.hard_state.current_term.0,
            sent,
            "heartbeat sent"
        );
        Ok(())
    }
}
