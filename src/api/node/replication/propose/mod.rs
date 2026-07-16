use super::super::RaftNode;
use crate::{
    protocol::codec::{encode_message_to_bytes, encode_message_vectored},
    AppendEntries, EntryPayload, LogEntry, LogIndex, RaftError, RaftMessage,
};
use std::time::Duration;

mod initial;
mod quorum;

use initial::should_use_vectored;

impl<S, T> RaftNode<S, T>
where
    S: crate::RaftStorage,
    T: crate::RaftTransport,
{
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
    ) -> Result<&[LogIndex], RaftError> {
        if !self.is_leader() {
            return Err(RaftError::NotLeader {
                leader_hint: self
                    .soft_state
                    .leader_id
                    .map(|l| crate::LeaderHint { leader_id: l }),
            });
        }
        if payloads.is_empty() {
            return Ok(&[]);
        }
        // Progress must be initialized BEFORE append so that next_index covers
        // the entries we are about to write.
        self.ensure_leader_progress_initialized()?;
        let last_index = self.append_propose_entries(payloads)?;
        let res = self.send_initial_appends().await;
        self.scratch_entries.clear();
        res?;

        let needed = super::super::quorum(self.config.peers.len());
        let timeout = Duration::from_millis(self.config.timing.heartbeat_ms * 2);
        // Term at which this batch was appended. If the leader steps down
        // mid-gather (a higher term arrives on an ack) this guards the fast path
        // below from committing on a now-stale ack counter.
        let propose_term = self.hard_state.current_term;
        // Gathers acks until the entry meets the EFFECTIVE commit quorum (dual
        // during a joint transition, simple majority otherwise). `needed` is the
        // fast-path union count used only when not joint.
        let accepted = self.gather_quorum_acks(needed, last_index, timeout).await?;

        // Advance the commit index.
        //
        // Non-joint fast path: the gather already proved `needed` distinct members
        // replicated `last_index`, the entry was appended at `propose_term`, and
        // the leader has not stepped down (role + term re-check). Committing
        // `last_index` directly is the classic §5.4.2-safe majority rule; it skips
        // the quorum re-sort AND leaves `scratch_indexes` (the return slice)
        // untouched — see G1.
        //
        // Joint path: fall back to the full dual-quorum rule (majority-of-old AND
        // majority-of-new). Never commit `last_index` on a raw union count — that
        // is the config-change data-loss bug (C2). The joint arm of
        // `try_advance_commit_index` reads `subset_quorum_index` and likewise never
        // touches `scratch_indexes`.
        if self.joint_peers.is_none()
            && self.is_leader()
            && self.hard_state.current_term == propose_term
            && accepted >= needed
        {
            if last_index > self.soft_state.commit_index {
                self.set_commit_index(last_index);
            }
        } else {
            self.try_advance_commit_index()?;
        }
        if self.soft_state.commit_index < last_index {
            return Err(RaftError::NoQuorum);
        }
        self.drain_inbound_ready().await?;
        Ok(&self.scratch_indexes)
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
        for entry in entries_ref {
            self.log_metadata.append(entry.index, entry.term);
        }

        if let Some(last) = self.scratch_entries.last() {
            self.cached_last_log = (last.index, last.term);
        }

        // --- Parallel Fan-out Encoding ---
        let req = AppendEntries {
            term: self.hard_state.current_term.0.into(),
            leader_id: self.config.node_id.0.into(),
            prev_log_index: prev_log_index.0.into(),
            prev_log_term: prev_log_term.0.into(),
            leader_commit: self.soft_state.commit_index.0.into(),
            entry_count: (entries_ref.len() as u32).into(),
            _pad: 0.into(),
        };
        let msg = RaftMessage::AppendEntriesVectored(&req, entries_ref);

        // Collect peers once so the hot branches below don't repeat the filter.
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

        if should_use_vectored(entries_ref) {
            // --- Vectored fan-out (bulk replication path) ---
            self.scratch_vectored.clear();
            // SAFETY: see `send_initial_appends` — scratch_vectored is cleared
            // before returning, no borrow escapes self.
            let iovs: &mut Vec<&[u8]> = unsafe {
                std::mem::transmute::<&mut Vec<(*const u8, usize)>, &mut Vec<&[u8]>>(
                    &mut self.scratch_vectored,
                )
            };
            encode_message_vectored(self.config.node_id, &msg, &mut self.scratch_outbound, iovs)?;

            let slices: &[&[u8]] = iovs.as_slice();
            let transport = &self.transport;
            let mut sends = Vec::with_capacity(self.scratch_peers.len());
            for &peer in &self.scratch_peers {
                sends.push(async move { transport.send_vectored(peer, slices).await });
            }

            let results = futures::future::join_all(sends).await;
            for res in results {
                if let Err(e) = res {
                    tracing::error!(error = %e, "parallel fan-out send failed");
                }
            }

            self.scratch_vectored.clear();
        } else {
            // --- Contiguous fan-out (control-plane / small-entry path) ---
            let frame = encode_message_to_bytes(self.config.node_id, &msg)?;
            let transport = &self.transport;
            let mut sends = Vec::with_capacity(self.scratch_peers.len());
            for &peer in &self.scratch_peers {
                let f = frame.clone();
                sends.push(async move { transport.send_frame_owned(peer, f).await });
            }

            let results = futures::future::join_all(sends).await;
            for res in results {
                if let Err(e) = res {
                    tracing::error!(error = %e, "parallel fan-out send failed");
                }
            }
        }

        self.scratch_entries.clear();

        Ok((first_index, payloads.len()))
    }
}
