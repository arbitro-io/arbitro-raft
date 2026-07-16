use std::time::Duration;
use tracing::info;

use crate::protocol::{InstallSnapshot, InstallSnapshotResp};
use crate::{InboundRaftMessage, LogIndex, PeerId, RaftError, RaftMessage, Role};

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
        // Snapshot the term we started in. handle_inbound below can process a
        // higher-term RPC and step this node down; we must not keep sending
        // InstallSnapshot frames as "leader" after that happens.
        let start_term = self.hard_state.current_term;

        loop {
            offset = offset.min(total_len);
            let end = offset
                .checked_add(chunk_size)
                .unwrap_or(total_len)
                .min(total_len);
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
                let group_id = inbound.group_id;

                match inbound.message {
                    RaftMessage::InstallSnapshotResp(resp) if from == peer => {
                        let resp_term = crate::Term(resp.term.get());
                        if resp_term.0 > self.hard_state.current_term.0 {
                            self.step_down(resp_term)?;
                            return Err(RaftError::TermChanged {
                                current: self.hard_state.current_term,
                            });
                        }
                        let next_off = resp.next_offset.get() as usize;
                        if next_off > total_len {
                            return Err(RaftError::Protocol(
                                "install snapshot next_offset past total".into(),
                            ));
                        }
                        offset = next_off;
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
                        self.handle_inbound(InboundRaftMessage { from, group_id, message })
                            .await?;
                        // handle_inbound may have stepped us down on a higher term.
                        // Aborting here avoids stamping later chunks with the new
                        // term while still identifying ourselves as leader.
                        if !self.is_leader()
                            || self.hard_state.current_term != start_term
                        {
                            return Err(RaftError::NotLeader {
                                leader_hint: self
                                    .soft_state
                                    .leader_id
                                    .map(|leader_id| crate::LeaderHint { leader_id }),
                            });
                        }
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

        // Wire-length field must agree with the decoded payload slice. The
        // codec already checks this, but re-validating here defends against a
        // future codec path that hands us a mismatched pair.
        if payload.len() as u32 != msg.chunk_len.get() {
            return Err(RaftError::Protocol(
                "install snapshot chunk_len mismatch".into(),
            ));
        }

        // Reject snapshots from a non-member (PS6 / P1-5): a spoofed `from`
        // could otherwise force us to allocate an unbounded pending-snapshot
        // buffer (multi-GiB) for a peer we will never converse with. A real
        // snapshot always arrives from a current voter (the leader).
        if !self.config.peers.contains(&from) {
            tracing::warn!(
                node_id = self.config.node_id.0,
                from = from.0,
                "ignoring InstallSnapshot from a non-member peer"
            );
            self.metrics.inc_frames_dropped_nonfatal();
            return Ok(());
        }

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

        let max_snapshot_bytes = self.config.limits.max_snapshot_bytes;
        if pending.bytes.len().saturating_add(payload.len()) > max_snapshot_bytes {
            // Buffered bytes plus this chunk would exceed the configured cap.
            // Drop the transfer so a runaway or hostile leader cannot exhaust
            // follower memory, and NACK from offset 0 so a legitimate retry
            // starts a fresh session.
            self.pending_snapshots.remove(&from);
            let resp = InstallSnapshotResp {
                term: self.hard_state.current_term.0.into(),
                accepted: 0,
                next_offset: 0.into(),
                _pad: [0; 7],
            };
            self.send_message(from, &RaftMessage::InstallSnapshotResp(&resp))
                .await;
            return Err(RaftError::Snapshot(format!(
                "install snapshot exceeds max_snapshot_bytes ({})",
                max_snapshot_bytes
            )));
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
                self.set_commit_index(completed.meta.last_included_index);
            }

            // The Raft §7 rule: once a snapshot is installed, any local log
            // entry whose index/term disagrees with the snapshot boundary is
            // from a divergent branch and must be discarded wholesale;
            // otherwise the snapshot's prefix is safe to drop.
            let last_idx = completed.meta.last_included_index;
            let last_term = completed.meta.last_included_term;
            let mut dummy = [0u8; 8];
            let boundary_conflict = self
                .storage
                .entry_at(last_idx, &mut dummy)?
                .map(|e| e.term != last_term)
                .unwrap_or(false);
            if boundary_conflict {
                self.storage.truncate_suffix(LogIndex(1))?;
            } else {
                self.storage
                    .truncate_before(LogIndex(last_idx.0.saturating_add(1)))?;
            }
            // Re-seat the metadata arena at the new post-snapshot base so
            // subsequent AppendEntries append at last_included_index + 1.
            self.log_metadata
                .clear(LogIndex(last_idx.0.saturating_add(1)));
            self.cached_last_log = (last_idx, last_term);

            // Hand off to the outer apply loop. Source-of-truth is the storage
            // layer (`load_snapshot`) — this call is a discoverable hook point.
            super::snapshot_install::mark_snapshot_installed(self, &completed.meta);
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

    /// If `peer`'s `next_index` has fallen below the on-disk snapshot's
    /// `last_included_index`, stream the snapshot to it. Otherwise no-op.
    ///
    /// Public bridge into the `snapshot_install` module — that module is
    /// private to `node/` so `ArbitroRaft` reaches it through this method.
    #[inline]
    pub async fn maybe_install_snapshot_to_lagging_peer(
        &mut self,
        peer: PeerId,
    ) -> Result<bool, RaftError> {
        super::snapshot_install::maybe_install_snapshot_to_lagging_peer(self, peer).await
    }

    /// If the on-disk snapshot is more recent than `last_applied`, restore
    /// `sm` from it and jump `last_applied` to the snapshot boundary.
    /// Idempotent — returns `false` when nothing to do.
    ///
    /// Public bridge into the `snapshot_install` module — that module is
    /// private to `node/` so `ArbitroRaft` reaches it through this method.
    #[inline]
    pub fn restore_state_machine_from_snapshot<SM>(
        &mut self,
        sm: &mut SM,
    ) -> Result<bool, RaftError>
    where
        SM: crate::StateMachine,
    {
        super::snapshot_install::restore_state_machine_from_snapshot(self, sm)
    }
}
