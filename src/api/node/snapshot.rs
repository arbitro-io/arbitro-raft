use std::time::Duration;
use tracing::info;
use bytes::Bytes;

use crate::{
    InstallSnapshot, InstallSnapshotResp, InstallSnapshotRespView, InstallSnapshotView,
    PeerId, RaftError, RaftMessage, Role, InboundRaftMessageView,
};
use super::RaftNode;
use super::progress::PendingSnapshot;

impl<S, T> RaftNode<S, T>
where
    S: crate::RaftStorage,
    T: crate::RaftTransport,
{
    pub async fn install_snapshot_once(
        &mut self,
        peer: PeerId,
        meta: crate::SnapshotMeta,
        snapshot: Bytes,
    ) -> Result<(), RaftError> {
        if !self.is_leader() {
            return Err(RaftError::NotLeader {
                leader_hint: self.soft_state.leader_id.map(|leader_id| crate::LeaderHint { leader_id }),
            });
        }

        let chunk_size = self.config.limits.snapshot_chunk_bytes.max(1);
        let total_len = snapshot.len();
        let mut offset = 0usize;
        let timeout = Duration::from_millis(self.config.timing.heartbeat_ms as u64 * 2);

        loop {
            let end = (offset + chunk_size).min(total_len);
            let done = end == total_len;
            let chunk_bytes = if total_len == 0 { Bytes::new() } else { Bytes::copy_from_slice(&snapshot[offset..end]) };

            self.transport.send(peer, RaftMessage::InstallSnapshot(InstallSnapshot {
                term: self.hard_state.current_term,
                leader_id: self.config.node_id,
                meta: meta.clone(),
                chunk: crate::SnapshotChunk { offset: offset as u64, bytes: chunk_bytes, done },
            })).await?;

            loop {
                let Some(inbound) = self.transport.recv_timeout(timeout).await? else {
                    return Err(RaftError::NoQuorum);
                };
                let from = inbound.from;
                match inbound.message {
                    crate::RaftMessageView::InstallSnapshotResp(resp) if from == peer => {
                        if resp.term().0 > self.hard_state.current_term.0 {
                            self.step_down(resp.term())?;
                            return Err(RaftError::TermChanged { current: self.hard_state.current_term });
                        }
                        offset = resp.next_offset() as usize;
                        if resp.accepted() && (done || offset >= total_len) {
                            info!(node_id = self.config.node_id.0, peer = peer.0, bytes = total_len, "snapshot installed");
                            return Ok(());
                        }
                        break;
                    }
                    message => self.handle_inbound(InboundRaftMessageView { from, message }).await?,
                }
            }
        }
    }

    pub(crate) async fn handle_install_snapshot(&mut self, msg: InstallSnapshotView) -> Result<(), RaftError> {
        if msg.term().0 < self.hard_state.current_term.0 {
            self.transport.send(msg.from(), RaftMessage::InstallSnapshotResp(InstallSnapshotResp {
                term: self.hard_state.current_term, accepted: false, next_offset: 0,
            })).await?;
            return Ok(());
        }

        if msg.term().0 > self.hard_state.current_term.0 { self.step_down(msg.term())?; }
        self.soft_state.role = Role::Follower;
        self.soft_state.is_leader = false;
        self.soft_state.leader_id = Some(msg.leader_id());

        let meta = msg.meta();
        let pending = self.pending_snapshots.entry(msg.from()).or_insert_with(|| PendingSnapshot {
            meta: meta.clone(), bytes: Vec::new(),
        });

        if msg.offset() == 0 || pending.meta != meta {
            pending.meta = meta.clone();
            pending.bytes.clear();
        }

        if pending.bytes.len() as u64 != msg.offset() {
            let next_offset = pending.bytes.len() as u64;
            self.transport.send(msg.from(), RaftMessage::InstallSnapshotResp(InstallSnapshotResp {
                term: self.hard_state.current_term, accepted: false, next_offset,
            })).await?;
            return Ok(());
        }

        pending.bytes.extend_from_slice(msg.chunk_bytes().as_ref());
        let next_offset = pending.bytes.len() as u64;

        if msg.done() {
            let completed = self.pending_snapshots.remove(&msg.from()).ok_or_else(|| RaftError::Snapshot("missing pending snapshot".into()))?;
            self.storage.save_snapshot(&completed.meta, &completed.bytes)?;
            if self.hard_state.commit_index.0 < completed.meta.last_included_index.0 {
                self.hard_state.commit_index = completed.meta.last_included_index;
                self.storage.save_hard_state(&self.hard_state)?;
            }
        }

        self.transport.send(msg.from(), RaftMessage::InstallSnapshotResp(InstallSnapshotResp {
            term: self.hard_state.current_term, accepted: true, next_offset,
        })).await?;
        Ok(())
    }

    pub(crate) async fn handle_install_snapshot_response(&mut self, resp: InstallSnapshotRespView) -> Result<(), RaftError> {
        if resp.term().0 > self.hard_state.current_term.0 { self.step_down(resp.term())?; }
        Ok(())
    }
}
