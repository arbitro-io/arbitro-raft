use super::super::RaftNode;
use crate::RaftError;
use tracing::info;

impl<S, T> RaftNode<S, T>
where
    S: crate::RaftStorage,
    T: crate::RaftTransport,
{
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

        let mut sent = 0usize;
        for i in 0..self.scratch_peers.len() {
            let peer = self.scratch_peers[i];
            let (frame, _) = self.build_append_for_peer(peer)?;
            if self.send_best_effort(peer, frame).await {
                sent += 1;
            }
        }
        info!(
            node_id = self.config.node_id.0,
            term    = self.hard_state.current_term.0,
            sent,
            "heartbeat sent"
        );
        Ok(())
    }
}
