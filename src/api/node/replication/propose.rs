use bytes::Bytes;
use std::time::Duration;

use super::super::progress::{AppendAdvance, AppendAttemptState};
use super::super::RaftNode;
use crate::{
    AppendEntriesRespView, EntryPayload, InboundRaftMessageView, LogEntry, LogIndex, PeerId,
    RaftError,
};

impl<S, T> RaftNode<S, T>
where
    S: crate::RaftStorage,
    T: crate::RaftTransport,
{
    /// Append `payloads` to local log, populating `scratch_entries` and `scratch_indexes`.
    /// Returns the last appended `LogIndex`.
    fn append_propose_entries(&mut self, payloads: Vec<Bytes>) -> Result<LogIndex, RaftError> {
        let last_log_index = self.storage.last_log_position()?.0;
        self.scratch_entries.clear();
        self.scratch_indexes.clear();
        let mut next_raw = last_log_index.0 + 1;
        for payload in payloads {
            let next_index = LogIndex(next_raw);
            self.scratch_entries.push(LogEntry {
                term:    self.hard_state.current_term,
                index:   next_index,
                payload: EntryPayload(payload),
            });
            self.scratch_indexes.push(next_index);
            next_raw += 1;
        }
        let last_index = *self.scratch_indexes.last().unwrap();
        self.storage.append_entries(&self.scratch_entries)?;
        Ok(last_index)
    }

    /// Collect remote peers into `scratch_peers`, send one `AppendEntries` attempt to each,
    /// and populate `scratch_pending` with peers that received the frame.
    async fn send_initial_appends(&mut self) -> Result<(), RaftError> {
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
        self.scratch_pending.clear();
        for i in 0..self.scratch_peers.len() {
            let peer = self.scratch_peers[i];
            if let Some(sent_last_index) = self.send_append_attempt(peer, 1).await? {
                self.scratch_pending.insert(
                    peer,
                    AppendAttemptState { attempts: 1, sent_last_index },
                );
            }
        }
        Ok(())
    }

    /// Route an `AppendEntriesResp` during quorum wait: advance replication state for pending
    /// peers or forward to the normal response handler for peers not in `scratch_pending`.
    async fn process_append_resp(
        &mut self,
        from: PeerId,
        resp: AppendEntriesRespView,
        last_index: LogIndex,
        accepted: &mut usize,
    ) -> Result<(), RaftError> {
        if let Some(state) = self.scratch_pending.get(&from).copied() {
            if let AppendAdvance::Completed =
                self.advance_append_replication(from, last_index, state, resp).await?
            {
                self.scratch_pending.remove(&from);
                *accepted += 1;
            }
        } else {
            self.handle_append_entries_response(resp).await?;
        }
        Ok(())
    }

    /// Block until a quorum has acknowledged `last_index` or `timeout` expires without progress.
    /// Returns the number of accepted acknowledgements (leader self-ack = 1 on entry).
    async fn gather_quorum_acks(
        &mut self,
        needed: usize,
        last_index: LogIndex,
        timeout: Duration,
    ) -> Result<usize, RaftError> {
        let mut accepted = 1usize;
        while accepted < needed && !self.scratch_pending.is_empty() {
            // Burst-drain: consume all immediately-available frames before yielding.
            while let Some(raw) = self.transport.recv_frame_timeout(Duration::ZERO).await? {
                let inbound = crate::decode_message_view(raw)?;
                let from    = inbound.from;
                match inbound.message {
                    crate::RaftMessageView::AppendEntriesResp(resp) => {
                        self.process_append_resp(from, resp, last_index, &mut accepted).await?;
                    }
                    message => {
                        self.handle_inbound(InboundRaftMessageView { from, message }).await?;
                    }
                }
                if accepted >= needed || self.scratch_pending.is_empty() {
                    break;
                }
            }
            if accepted >= needed || self.scratch_pending.is_empty() {
                break;
            }

            // Blocking wait: yield only when the queue is actually empty.
            let raw = match self.transport.recv_frame_timeout(timeout).await? {
                Some(r) => r,
                None    => break,
            };
            let inbound = crate::decode_message_view(raw)?;
            let from    = inbound.from;
            match inbound.message {
                crate::RaftMessageView::AppendEntriesResp(resp) => {
                    self.process_append_resp(from, resp, last_index, &mut accepted).await?;
                }
                message => {
                    self.handle_inbound(InboundRaftMessageView { from, message }).await?;
                }
            }
        }
        Ok(accepted)
    }

    pub async fn propose_once(&mut self, payload: Bytes) -> Result<LogIndex, RaftError> {
        let indexes = self.propose_batch_once(vec![payload]).await?;
        Ok(*indexes
            .first()
            .ok_or(RaftError::Protocol("propose failed to return index".into()))?)
    }

    pub async fn propose_batch_once(
        &mut self,
        payloads: Vec<Bytes>,
    ) -> Result<Vec<LogIndex>, RaftError> {
        if !self.is_leader() {
            return Err(RaftError::NotLeader {
                leader_hint: self
                    .soft_state
                    .leader_id
                    .map(|l| crate::LeaderHint { leader_id: l }),
            });
        }
        if payloads.is_empty() {
            return Ok(Vec::new());
        }
        // Progress must be initialized BEFORE append so that next_index covers
        // the entries we are about to write. Calling it after would set next_index
        // beyond the new entries, causing the first AppendEntries to be empty.
        self.ensure_leader_progress_initialized()?;
        let last_index = self.append_propose_entries(payloads)?;
        self.send_initial_appends().await?;

        let needed  = super::super::quorum(self.config.peers.len());
        let timeout = Duration::from_millis(self.config.timing.heartbeat_ms as u64 * 2);
        let accepted = self.gather_quorum_acks(needed, last_index, timeout).await?;

        if accepted < needed {
            return Err(RaftError::NoQuorum);
        }
        self.soft_state.commit_index = last_index;
        self.drain_inbound_ready().await?;
        Ok(self.scratch_indexes.clone())
    }

    pub async fn replicate_batch_async(
        &mut self,
        payloads: &[Bytes],
    ) -> Result<Vec<LogIndex>, RaftError> {
        if !self.is_leader() {
            return Err(RaftError::NotLeader {
                leader_hint: self
                    .soft_state
                    .leader_id
                    .map(|l| crate::LeaderHint { leader_id: l }),
            });
        }
        if payloads.is_empty() {
            return Ok(Vec::new());
        }
        // Progress must be initialized BEFORE append — same reason as propose_batch_once.
        self.ensure_leader_progress_initialized()?;
        let last_log_index = self.storage.last_log_position()?.0;
        self.scratch_entries.clear();
        self.scratch_indexes.clear();
        let mut next_raw = last_log_index.0 + 1;
        for payload in payloads {
            let next_index = LogIndex(next_raw);
            self.scratch_entries.push(LogEntry {
                term:    self.hard_state.current_term,
                index:   next_index,
                payload: EntryPayload(payload.clone()),
            });
            self.scratch_indexes.push(next_index);
            next_raw += 1;
        }
        self.storage.append_entries(&self.scratch_entries)?;

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
        for i in 0..self.scratch_peers.len() {
            let peer = self.scratch_peers[i];
            self.send_append_attempt(peer, 1).await?;
        }
        Ok(self.scratch_indexes.clone())
    }
}
