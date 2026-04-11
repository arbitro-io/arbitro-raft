use std::time::Duration;

use super::super::progress::{AppendAdvance, AppendAttemptState};
use super::super::RaftNode;
use crate::{
    AppendEntriesResp, EntryPayload, InboundRaftMessage, LogEntry, LogIndex, PeerId, RaftError,
    RaftMessage,
};

impl<S, T> RaftNode<S, T>
where
    S: crate::RaftStorage,
    T: crate::RaftTransport,
{
    /// Append `payloads` to local log, populating `scratch_entries` and `scratch_indexes`.
    /// Returns the last appended `LogIndex`.
    fn append_propose_entries(&mut self, payloads: &[&[u8]]) -> Result<LogIndex, RaftError> {
        let last_log_index = self.cached_last_log.0;
        self.scratch_entries.clear();
        self.scratch_indexes.clear();
        let mut next_raw = last_log_index.0 + 1;
        for payload in payloads {
            let next_index = LogIndex(next_raw);
            let entry = LogEntry {
                term: self.hard_state.current_term,
                index: next_index,
                payload: EntryPayload(payload),
            };
            // Safety: scratch_entries in the struct is Vec<LogEntry<'static>>.
            // Transmute to 'static for preallocated storage. Safe because we clear it after use.
            let entry_static =
                unsafe { std::mem::transmute::<LogEntry<'_>, LogEntry<'static>>(entry) };
            self.scratch_entries.push(entry_static);
            self.scratch_indexes.push(next_index);
            next_raw += 1;
        }
        let last_index = *self.scratch_indexes.last().unwrap();

        // Safety: ensure storage sees the entries with a valid ephemeral lifetime
        let entries_ref = unsafe {
            std::mem::transmute::<&[LogEntry<'static>], &[LogEntry<'_>]>(&self.scratch_entries)
        };
        self.storage.append_entries(entries_ref)?;

        self.cached_last_log = (last_index, self.hard_state.current_term);
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
                    AppendAttemptState {
                        attempts: 1,
                        sent_last_index,
                    },
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
        resp: &AppendEntriesResp,
        last_index: LogIndex,
        accepted: &mut usize,
    ) -> Result<(), RaftError> {
        if let Some(state) = self.scratch_pending.get(&from).copied() {
            if let AppendAdvance::Completed = self
                .advance_append_replication(from, last_index, state, resp)
                .await?
            {
                self.scratch_pending.remove(&from);
                *accepted += 1;
            }
        } else {
            self.handle_append_entries_response(from, resp).await?;
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
        let mut inbound_buf = vec![0; 64 * 1024]; // Temporary inbound scratch for quorum check loop, outside hot-path
        while accepted < needed && !self.scratch_pending.is_empty() {
            // Burst-drain: consume all immediately-available frames before yielding.
            while let Some(n) = self
                .transport
                .recv_frame_timeout(Duration::ZERO, &mut inbound_buf)
                .await?
            {
                let inbound = crate::decode_message(&inbound_buf[..n])?;
                let from = inbound.from;
                match inbound.message {
                    RaftMessage::AppendEntriesResp(resp) => {
                        self.process_append_resp(from, resp, last_index, &mut accepted)
                            .await?;
                    }
                    message => {
                        self.handle_inbound(InboundRaftMessage { from, message })
                            .await?;
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
            let n = match self
                .transport
                .recv_frame_timeout(timeout, &mut inbound_buf)
                .await?
            {
                Some(r) => r,
                None => break,
            };
            let inbound = crate::decode_message(&inbound_buf[..n])?;
            let from = inbound.from;
            match inbound.message {
                RaftMessage::AppendEntriesResp(resp) => {
                    self.process_append_resp(from, resp, last_index, &mut accepted)
                        .await?;
                }
                message => {
                    self.handle_inbound(InboundRaftMessage { from, message })
                        .await?;
                }
            }
        }
        Ok(accepted)
    }

    pub async fn propose_once(&mut self, payload: &[u8]) -> Result<LogIndex, RaftError> {
        let scratch = [payload];
        let indexes = self.propose_batch_once(&scratch).await?;
        Ok(*indexes
            .first()
            .ok_or(RaftError::Protocol("propose failed to return index".into()))?)
    }

    pub async fn propose_batch_once(
        &mut self,
        payloads: &[&[u8]],
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
        // the entries we are about to write.
        self.ensure_leader_progress_initialized()?;
        let last_index = self.append_propose_entries(payloads)?;
        self.send_initial_appends().await?;

        let needed = super::super::quorum(self.config.peers.len());
        let timeout = Duration::from_millis(self.config.timing.heartbeat_ms as u64 * 2);
        let accepted = self.gather_quorum_acks(needed, last_index, timeout).await?;

        if accepted < needed {
            return Err(RaftError::NoQuorum);
        }
        self.soft_state.commit_index = last_index;
        self.drain_inbound_ready().await?;
        Ok(self.scratch_indexes.clone())
    }

    /// Fire-and-forget batch replication.
    pub async fn replicate_batch_async(
        &mut self,
        payloads: &[&[u8]],
    ) -> Result<(LogIndex, usize), RaftError> {
        if !self.is_leader() {
            return Err(RaftError::NotLeader {
                leader_hint: self
                    .soft_state
                    .leader_id
                    .map(|l| crate::LeaderHint { leader_id: l }),
            });
        }
        if payloads.is_empty() {
            return Ok((LogIndex(0), 0));
        }
        self.ensure_leader_progress_initialized()?;

        let (prev_log_index, prev_log_term) = self.cached_last_log;
        let first_index = LogIndex(prev_log_index.0 + 1);

        self.scratch_entries.clear();
        let mut next_raw = first_index.0;
        for payload in payloads {
            let entry = LogEntry {
                term: self.hard_state.current_term,
                index: LogIndex(next_raw),
                payload: EntryPayload(payload),
            };
            // Safety: transmute to 'static for scratchpad storage.
            let entry_static =
                unsafe { std::mem::transmute::<LogEntry<'_>, LogEntry<'static>>(entry) };
            self.scratch_entries.push(entry_static);
            next_raw += 1;
        }

        // Safety: storage call
        let entries_ref = unsafe {
            std::mem::transmute::<&[LogEntry<'static>], &[LogEntry<'_>]>(&self.scratch_entries)
        };
        self.storage.append_entries(entries_ref)?;

        if let Some(last) = self.scratch_entries.last() {
            self.cached_last_log = (last.index, last.term);
        }

        // --- Vectored Encoding ---
        self.scratch_vectored.clear();
        let vectored_ref = unsafe {
            std::mem::transmute::<&mut Vec<(*const u8, usize)>, &mut Vec<&[u8]>>(
                &mut self.scratch_vectored,
            )
        };

        crate::protocol::encode_append_entries_vectored(
            self.config.node_id,
            self.hard_state.current_term,
            self.config.node_id,
            prev_log_index,
            prev_log_term,
            self.soft_state.commit_index,
            entries_ref,
            &mut self.scratch_outbound,
            vectored_ref,
        )?;

        // Broadcast to all peers
        for peer in self
            .config
            .peers
            .iter()
            .copied()
            .filter(|p| *p != self.config.node_id)
        {
            let _ = self.transport.send_vectored(peer, vectored_ref).await;
        }
        self.scratch_vectored.clear();
        self.scratch_entries.clear();

        Ok((first_index, payloads.len()))
    }
}
