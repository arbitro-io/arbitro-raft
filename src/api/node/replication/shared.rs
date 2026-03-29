use std::time::Duration;

use super::super::RaftNode;
use crate::{AppendEntries, LogIndex, PeerId, RaftError, RaftMessage, Term};

impl<S, T> RaftNode<S, T>
where
    S: crate::RaftStorage,
    T: crate::RaftTransport,
{
    pub(crate) fn build_append_for_peer(
        &mut self,
        peer: PeerId,
    ) -> Result<AppendEntries, RaftError> {
        let progress = self
            .peer_progress
            .get(&peer)
            .copied()
            .ok_or(RaftError::PeerUnknown(peer))?;
        let prev_log_index = LogIndex(progress.next_index.0.saturating_sub(1));
        let prev_log_term  = self.term_at(prev_log_index)?;
        self.scratch_entries.clear();
        self.storage.read_entries(
            progress.next_index,
            LogIndex(u64::MAX),
            &mut self.scratch_entries,
        )?;
        self.scratch_entries
            .truncate(self.config.limits.append_batch_entries.max(1));

        AppendEntries::new_unchecked(
            self.hard_state.current_term,
            self.config.node_id,
            prev_log_index,
            prev_log_term,
            self.soft_state.commit_index,
            &self.scratch_entries,
        )
    }

    pub(crate) async fn send_append_attempt(
        &mut self,
        peer: PeerId,
        _attempt: u64,
    ) -> Result<Option<LogIndex>, RaftError> {
        let msg = self.build_append_for_peer(peer)?;
        let last_idx = self
            .scratch_entries
            .last()
            .map(|e| e.index)
            .unwrap_or(msg.prev_log_index());
        let frame = self.encode_msg(&RaftMessage::AppendEntries(msg))?;
        if self.send_best_effort(peer, frame).await {
            Ok(Some(last_idx))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn initialize_leader_progress(&mut self) -> Result<(), RaftError> {
        self.peer_progress.clear();
        let last_index = self.storage.last_log_position()?.0;
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
                    next_index:  LogIndex(last_index.0 + 1),
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

    pub(crate) fn term_at(&self, index: LogIndex) -> Result<Term, RaftError> {
        if index.0 == 0 {
            return Ok(Term(0));
        }
        self.storage
            .entry_at(index)?
            .map(|e| e.term)
            .ok_or_else(|| RaftError::CorruptLog(format!("missing term at index {}", index.0)))
    }

    /// Check whether a quorum of peers has replicated the latest entries and, if so,
    /// advance `commit_index` to the highest index confirmed by a quorum.
    ///
    /// Per Raft §5.4.2, only entries from `current_term` may be committed this way.
    /// `commit_index` is volatile — no `save_hard_state` needed.
    pub(crate) fn try_advance_commit_index(&mut self) -> Result<(), RaftError> {
        if self.peer_progress.is_empty() {
            return Ok(());
        }
        let last_index = self.storage.last_log_position()?.0;
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
        if let Some(entry) = self.storage.entry_at(quorum_index)? {
            if entry.term == self.hard_state.current_term {
                self.soft_state.commit_index = quorum_index;
            }
        }
        Ok(())
    }

    pub(crate) async fn drain_inbound_ready(&mut self) -> Result<(), RaftError> {
        loop {
            match self.transport.recv_frame_timeout(Duration::ZERO).await? {
                Some(raw) => {
                    let inbound = crate::decode_message_view(raw)?;
                    self.handle_inbound(inbound).await?;
                }
                None => break,
            }
        }
        Ok(())
    }
}
