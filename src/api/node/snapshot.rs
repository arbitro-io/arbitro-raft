use std::time::Duration;
use tracing::info;

use crate::protocol::{InstallSnapshot, InstallSnapshotResp};
use crate::{InboundRaftMessage, PeerId, RaftError, RaftMessage, Role};

use super::progress::PendingSnapshot;
use super::RaftNode;

impl<S, T> RaftNode<S, T>
where
    S: crate::RaftStorage,
    T: crate::RaftTransport,
{
    pub async fn install_snapshot_once(
        &mut self,
        peer: PeerId,
        meta: crate::SnapshotMeta,
        snapshot: &[u8],
    ) -> Result<(), RaftError> {
        if !self.is_leader() {
            return Err(RaftError::NotLeader {
                leader_hint: self
                    .soft_state
                    .leader_id
                    .map(|leader_id| crate::LeaderHint { leader_id }),
            });
        }

        let chunk_size = self.config.limits.snapshot_chunk_bytes.max(1);
        let total_len = snapshot.len();
        let mut offset = 0usize;
        let timeout = Duration::from_millis(self.config.timing.heartbeat_ms * 2);

        loop {
            let end = (offset + chunk_size).min(total_len);
            let done = end == total_len;
            let chunk_bytes = if total_len == 0 {
                &[]
            } else {
                &snapshot[offset..end]
            };

            let req = InstallSnapshot {
                term: self.hard_state.current_term.0.into(),
                leader_id: self.config.node_id.0.into(),
                last_included_index: meta.last_included_index.0.into(),
                last_included_term: meta.last_included_term.0.into(),
                offset: (offset as u64).into(),
                chunk_len: (chunk_bytes.len() as u32).into(),
                done: if done { 1 } else { 0 },
                _pad: [0; 3],
            };

            let msg = RaftMessage::InstallSnapshot(&req, chunk_bytes);
            self.send_message(peer, &msg).await;

            // loop scratch buffer is now managed by ArbitroRaft or injected
            let mut local_buf = [0u8; 4096];
            loop {
                let n = match self
                    .transport
                    .recv_frame_timeout(timeout, &mut local_buf)
                    .await?
                {
                    Some(n) => n,
                    None => return Err(RaftError::NoQuorum),
                };
                let inbound = crate::decode_message(&local_buf[..n])?;
                let from = inbound.from;

                match inbound.message {
                    RaftMessage::InstallSnapshotResp(resp) if from == peer => {
                        let resp_term = crate::Term(resp.term.get());
                        if resp_term.0 > self.hard_state.current_term.0 {
                            self.step_down(resp_term)?;
                            return Err(RaftError::TermChanged {
                                current: self.hard_state.current_term,
                            });
                        }
                        offset = resp.next_offset.get() as usize;
                        if resp.accepted != 0 && (done || offset >= total_len) {
                            info!(
                                node_id = self.config.node_id.0,
                                peer = peer.0,
                                bytes = total_len,
                                "snapshot installed"
                            );
                            return Ok(());
                        }
                        break;
                    }
                    message => {
                        self.handle_inbound(InboundRaftMessage { from, message })
                            .await?
                    }
                }
            }
        }
    }

    pub(crate) async fn handle_install_snapshot(
        &mut self,
        from: PeerId,
        msg: &InstallSnapshot,
        payload: &[u8],
    ) -> Result<(), RaftError> {
        let msg_term = crate::Term(msg.term.get());
        let leader_id = crate::PeerId(msg.leader_id.get());
        let msg_offset = msg.offset.get();
        let msg_done = msg.done != 0;
        let meta = crate::SnapshotMeta {
            last_included_index: crate::LogIndex(msg.last_included_index.get()),
            last_included_term: crate::Term(msg.last_included_term.get()),
        };

        // Evict any stalled snapshot transfers before processing new chunks.
        self.pending_snapshots.retain(|_, snap| !snap.is_expired());

        if msg_term.0 < self.hard_state.current_term.0 {
            let resp = InstallSnapshotResp {
                term: self.hard_state.current_term.0.into(),
                accepted: 0,
                next_offset: 0.into(),
                _pad: [0; 7],
            };
            self.send_message(from, &RaftMessage::InstallSnapshotResp(&resp))
                .await;
            return Ok(());
        }

        if msg_term.0 > self.hard_state.current_term.0 {
            self.step_down(msg_term)?;
        }
        self.soft_state.role = Role::Follower;
        self.soft_state.is_leader = false;
        self.soft_state.leader_id = Some(leader_id);

        let pending = self
            .pending_snapshots
            .entry(from)
            .or_insert_with(|| PendingSnapshot::new(meta.clone()));

        if msg_offset == 0 || pending.meta != meta {
            pending.reset(meta.clone());
        }

        if pending.bytes.len() as u64 != msg_offset {
            let next_offset = pending.bytes.len() as u64;
            let resp = InstallSnapshotResp {
                term: self.hard_state.current_term.0.into(),
                accepted: 0,
                next_offset: next_offset.into(),
                _pad: [0; 7],
            };
            self.send_message(from, &RaftMessage::InstallSnapshotResp(&resp))
                .await;
            return Ok(());
        }

        pending.bytes.extend_from_slice(payload);
        let next_offset = pending.bytes.len() as u64;

        if msg_done {
            let completed = self
                .pending_snapshots
                .remove(&from)
                .ok_or_else(|| RaftError::Snapshot("missing pending snapshot".into()))?;
            self.storage
                .save_snapshot(&completed.meta, &completed.bytes)?;
            if self.soft_state.commit_index.0 < completed.meta.last_included_index.0 {
                self.soft_state.commit_index = completed.meta.last_included_index;
            }
        }

        let resp = InstallSnapshotResp {
            term: self.hard_state.current_term.0.into(),
            accepted: 1,
            next_offset: next_offset.into(),
            _pad: [0; 7],
        };
        self.send_message(from, &RaftMessage::InstallSnapshotResp(&resp))
            .await;
        Ok(())
    }

    pub(crate) async fn handle_install_snapshot_response(
        &mut self,
        _from: PeerId,
        resp: &InstallSnapshotResp,
    ) -> Result<(), RaftError> {
        let resp_term = crate::Term(resp.term.get());
        if resp_term.0 > self.hard_state.current_term.0 {
            self.step_down(resp_term)?;
        }
        Ok(())
    }
}
