use super::super::progress::{AppendAdvance, AppendAttemptState};
use super::super::RaftNode;
use crate::{AppendEntriesResp, AppendEntriesRespView, AppendEntriesView, LogIndex, PeerId, RaftError, RaftMessage, Role};

impl<S, T> RaftNode<S, T>
where
    S: crate::RaftStorage,
    T: crate::RaftTransport,
{
    /// Scan incoming entries against local log, truncate on conflict, append new ones.
    fn apply_append_entries(&mut self, msg: &AppendEntriesView) -> Result<(), RaftError> {
        let entry_count = msg.entry_count();
        let mut append_from = entry_count;
        for (idx, incoming) in msg.entries()?.enumerate() {
            match self.storage.entry_at(incoming.index())? {
                Some(local) if local.term == incoming.term() => {}
                Some(_) => {
                    self.storage_truncate(incoming.index())?;
                    append_from = idx;
                    break;
                }
                None => {
                    append_from = idx;
                    break;
                }
            }
        }
        if append_from < entry_count {
            self.scratch_entries.clear();
            for entry in msg.entries()?.skip(append_from) {
                self.scratch_entries.push(entry.to_owned());
            }
            self.storage.append_entries(&self.scratch_entries)?;
            if let Some(last) = self.scratch_entries.last() {
                self.cached_last_log = (last.index, last.term);
            }
        }
        Ok(())
    }

    pub(crate) async fn handle_append_entries(
        &mut self,
        msg: AppendEntriesView,
    ) -> Result<(), RaftError> {
        if msg.term().0 < self.hard_state.current_term.0 {
            let frame = self.encode_msg(&RaftMessage::AppendEntriesResp(AppendEntriesResp {
                term:        self.hard_state.current_term,
                success:     false,
                match_index: self.cached_last_log.0,
            }))?;
            self.transport.send_frame(msg.from(), frame).await?;
            return Ok(());
        }
        if msg.term().0 > self.hard_state.current_term.0 {
            self.step_down(msg.term())?;
        }
        self.soft_state.role      = Role::Follower;
        self.soft_state.is_leader = false;
        self.soft_state.leader_id = Some(msg.leader_id());

        let prev_ok = if msg.prev_log_index().0 == 0 {
            true
        } else {
            self.storage
                .entry_at(msg.prev_log_index())?
                .map(|e| e.term == msg.prev_log_term())
                .unwrap_or(false)
        };

        if !prev_ok {
            let frame = self.encode_msg(&RaftMessage::AppendEntriesResp(AppendEntriesResp {
                term:        self.hard_state.current_term,
                success:     false,
                match_index: self.cached_last_log.0,
            }))?;
            self.transport.send_frame(msg.from(), frame).await?;
            return Ok(());
        }

        self.apply_append_entries(&msg)?;

        let last_log_index = self.cached_last_log.0;
        if msg.leader_commit() > self.soft_state.commit_index {
            // commit_index is volatile — no save_hard_state needed here
            self.soft_state.commit_index =
                LogIndex(msg.leader_commit().0.min(last_log_index.0));
        }

        let frame = self.encode_msg(&RaftMessage::AppendEntriesResp(AppendEntriesResp {
            term:        self.hard_state.current_term,
            success:     true,
            match_index: last_log_index,
        }))?;
        self.transport.send_frame(msg.from(), frame).await?;
        Ok(())
    }

    pub(crate) async fn handle_append_entries_response(
        &mut self,
        resp: AppendEntriesRespView,
    ) -> Result<(), RaftError> {
        if resp.term().0 > self.hard_state.current_term.0 {
            self.step_down(resp.term())?;
            return Ok(());
        }
        if !self.is_leader() {
            return Ok(());
        }
        let Some(progress) = self.peer_progress.get_mut(&resp.from()) else {
            return Ok(());
        };
        if resp.success() {
            if resp.match_index() > progress.match_index {
                progress.match_index = resp.match_index();
            }
            let next_index = LogIndex(resp.match_index().0.saturating_add(1));
            if next_index > progress.next_index {
                progress.next_index = next_index;
            }
        } else if resp.match_index() >= progress.match_index {
            progress.next_index = LogIndex(
                progress
                    .next_index
                    .0
                    .saturating_sub(1)
                    .max(resp.match_index().0.saturating_add(1))
                    .max(1),
            );
        }
        Ok(())
    }

    pub(crate) async fn advance_append_replication(
        &mut self,
        peer: PeerId,
        target_index: LogIndex,
        state: AppendAttemptState,
        resp: AppendEntriesRespView,
    ) -> Result<AppendAdvance, RaftError> {
        if resp.term().0 > self.hard_state.current_term.0 {
            self.step_down(resp.term())?;
            return Err(RaftError::TermChanged { current: self.hard_state.current_term });
        }
        let progress = self
            .peer_progress
            .get_mut(&peer)
            .ok_or(RaftError::PeerUnknown(peer))?;
        if resp.success() {
            if resp.match_index() < state.sent_last_index {
                return Ok(AppendAdvance::Ignored);
            }
            progress.match_index = resp.match_index();
            progress.next_index  = LogIndex(resp.match_index().0 + 1);
            if progress.match_index >= target_index {
                return Ok(AppendAdvance::Completed);
            }
        } else {
            if resp.match_index() < progress.match_index {
                return Ok(AppendAdvance::Ignored);
            }
            progress.next_index = LogIndex(
                progress
                    .next_index
                    .0
                    .saturating_sub(1)
                    .max(resp.match_index().0.saturating_add(1))
                    .max(1),
            );
        }
        let next_attempt = state.attempts + 1;
        if let Some(sent_last_index) = self.send_append_attempt(peer, next_attempt).await? {
            Ok(AppendAdvance::Retry(AppendAttemptState { attempts: next_attempt, sent_last_index }))
        } else {
            Ok(AppendAdvance::Dropped)
        }
    }
}

