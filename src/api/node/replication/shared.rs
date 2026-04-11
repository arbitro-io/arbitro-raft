use std::time::Duration;

use super::super::RaftNode;
use crate::{LogIndex, PeerId, RaftError, RaftMessage, Term};

impl<S, T> RaftNode<S, T>
where
    S: crate::RaftStorage,
    T: crate::RaftTransport,
{
    /// Prepare entry metadata and read payloads from storage.
    pub(crate) fn build_append_for_peer(
        &mut self,
        peer: PeerId,
    ) -> Result<(LogIndex, Term, LogIndex), RaftError> {
        let progress = self
            .peer_progress
            .get(&peer)
            .copied()
            .ok_or(RaftError::PeerUnknown(peer))?;
        let prev_log_index = LogIndex(progress.next_index.0.saturating_sub(1));
        let prev_log_term = self.term_at(prev_log_index)?;
        self.scratch_entries.clear();

        // Pass a dummy buffer or reuse scratch_outbound if storage needs it for temporary read

        // Safety: scratch_entries in the struct is Vec<LogEntry<'static>>.
        // We transmute it to Vec<LogEntry<'_>> for the duration of the storage call.
        let scratch_ref = unsafe {
            std::mem::transmute::<&mut Vec<crate::LogEntry<'static>>, &mut Vec<crate::LogEntry<'_>>>(
                &mut self.scratch_entries,
            )
        };

        self.storage.read_entries(
            progress.next_index,
            LogIndex(u64::MAX),
            scratch_ref,
            &mut self.scratch_payload,
        )?;

        self.scratch_entries
            .truncate(self.config.limits.append_batch_entries.max(1));

        let last_idx = self
            .scratch_entries
            .last()
            .map(|e| e.index)
            .unwrap_or(prev_log_index);

        Ok((prev_log_index, prev_log_term, last_idx))
    }

    pub(crate) async fn send_append_attempt(
        &mut self,
        peer: PeerId,
        _attempt: u64,
    ) -> Result<Option<LogIndex>, RaftError> {
        let (prev_idx, prev_term, last_idx) = self.build_append_for_peer(peer)?;

        // Create AppendEntries metadata on stack
        let req = crate::protocol::AppendEntries {
            term: self.hard_state.current_term.0.into(),
            leader_id: self.config.node_id.0.into(),
            prev_log_index: prev_idx.0.into(),
            prev_log_term: prev_term.0.into(),
            leader_commit: self.soft_state.commit_index.0.into(),
            entry_count: (self.scratch_entries.len() as u32).into(),
            _pad: 0.into(),
        };

        // SAFETY: transmute from 'static storage to ephemeral for transport
        let entries_ref = unsafe {
            std::mem::transmute::<&[crate::LogEntry<'static>], &[crate::LogEntry<'_>]>(
                &self.scratch_entries,
            )
        };

        let msg = RaftMessage::AppendEntriesVectored(&req, entries_ref);
        if self.send_message(peer, &msg).await {
            Ok(Some(last_idx))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn initialize_leader_progress(&mut self) -> Result<(), RaftError> {
        self.peer_progress.clear();
        let last_index = self.cached_last_log.0;
        for peer in self
            .config
            .peers
            .iter()
            .copied()
            .filter(|p| *p != self.config.node_id)
        {
            self.peer_progress.insert(
                peer,
                super::super::progress::PeerProgress {
                    next_index: LogIndex(last_index.0 + 1),
                    match_index: LogIndex(0),
                },
            );
        }
        Ok(())
    }

    pub(crate) fn ensure_leader_progress_initialized(&mut self) -> Result<(), RaftError> {
        if self.peer_progress.is_empty() {
            self.initialize_leader_progress()?;
        }
        Ok(())
    }

    pub(crate) fn term_at(&mut self, index: LogIndex) -> Result<Term, RaftError> {
        if index.0 == 0 {
            return Ok(Term(0));
        }
        self.storage
            .entry_at(index, &mut self.scratch_payload)?
            .map(|e| e.term)
            .ok_or_else(|| RaftError::CorruptLog(format!("missing term at index {}", index.0)))
    }

    /// Check whether a quorum of peers has replicated the latest entries and, if so,
    /// advance `commit_index` to the highest index confirmed by a quorum.
    pub(crate) fn try_advance_commit_index(&mut self) -> Result<(), RaftError> {
        if self.peer_progress.is_empty() {
            return Ok(());
        }
        let last_index = self.cached_last_log.0;
        if last_index <= self.soft_state.commit_index {
            return Ok(());
        }
        // Gather: leader self (last_index) + all peer match_indexes.
        self.scratch_indexes.clear();
        self.scratch_indexes.push(last_index);
        for progress in self.peer_progress.values() {
            self.scratch_indexes.push(progress.match_index);
        }
        // Sort descending → quorum-th largest is the safe commit point.
        self.scratch_indexes.sort_unstable_by(|a, b| b.0.cmp(&a.0));
        let quorum = super::super::quorum(self.config.peers.len());
        let Some(&quorum_index) = self.scratch_indexes.get(quorum.saturating_sub(1)) else {
            return Ok(());
        };
        if quorum_index <= self.soft_state.commit_index {
            return Ok(());
        }
        // Safety rule: only commit if the quorum entry belongs to current_term.
        let quorum_term = if quorum_index == self.cached_last_log.0 {
            self.cached_last_log.1
        } else {
            self.storage
                .entry_at(quorum_index, &mut self.scratch_payload)?
                .map(|e| e.term)
                .unwrap_or(crate::Term(0))
        };
        if quorum_term == self.hard_state.current_term {
            self.soft_state.commit_index = quorum_index;
        }
        Ok(())
    }

    pub(crate) async fn drain_inbound_ready(&mut self) -> Result<(), RaftError> {
        let mut drain_buf = vec![0; 64 * 1024]; // Scratch for drained packets
        loop {
            match self
                .transport
                .recv_frame_timeout(Duration::ZERO, &mut drain_buf)
                .await?
            {
                Some(n) => {
                    let inbound = crate::decode_message(&drain_buf[..n])?;
                    self.handle_inbound(inbound).await?;
                }
                None => break,
            }
        }
        Ok(())
    }
}
