use super::super::progress::{AppendAdvance, AppendAttemptState};
use super::super::RaftNode;
use crate::protocol::codec::wire::EntryHeader;
use crate::protocol::{
    AppendEntries, AppendEntriesEntryIter, AppendEntriesRawIter, AppendEntriesResp, SeededPayloads,
};
use crate::{LogIndex, PeerId, RaftError, RaftMessage, Role};
use zerocopy::Ref;

impl<S, T> RaftNode<S, T>
where
    S: crate::RaftStorage,
    T: crate::RaftTransport,
{
    /// Scan incoming entries against local log, truncate on conflict, append new ones.
    fn apply_append_entries(
        &mut self,
        msg: &AppendEntries,
        payload: &[u8],
    ) -> Result<(), RaftError> {
        let entry_count = msg.entry_count.get() as usize;
        let mut append_from = entry_count;
        // A well-formed AppendEntries carries entries starting at
        // `prev_log_index + 1`, each index exactly one greater than the last.
        // Enforce that (P1-7 index sweep): a spoofed frame with a discontinuous
        // or near-`u64::MAX` index would otherwise be appended verbatim and
        // corrupt the log-index arena — up to a node-killing overflow in
        // `get_term`. Rejecting the batch is a non-fatal frame error that the
        // dispatch layer logs and drops, so one bad peer can't harm the node.
        let expected_start = msg.prev_log_index.get().saturating_add(1);
        let iter = AppendEntriesEntryIter::new(payload, entry_count);
        for (idx, incoming) in iter.enumerate() {
            if incoming.index.0 != expected_start.saturating_add(idx as u64) {
                return Err(RaftError::Protocol(
                    "AppendEntries entries not contiguous with prev_log_index".into(),
                ));
            }
            let local_term = match self.log_metadata.get_term(incoming.index) {
                Some(t) => Some(t),
                None => {
                    let mut dummy = [0u8; 8];
                    self.storage
                        .entry_at(incoming.index, &mut dummy)?
                        .map(|e| e.term)
                }
            };
            match local_term {
                Some(t) if t == incoming.term => {}
                Some(_) => {
                    self.storage_truncate(incoming.index)?;
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
            let iter = AppendEntriesEntryIter::new(payload, entry_count);
            for entry in iter.skip(append_from) {
                // Safety: we transmute the lifetime to 'static to store it in the preallocated scratchpad.
                // This is safe because scratch_entries is cleared immediately after the storage call.
                let entry_static = unsafe {
                    std::mem::transmute::<crate::LogEntry<'_>, crate::LogEntry<'static>>(entry)
                };
                self.scratch_entries.push(entry_static);
            }

            // SAFETY: storage traits expect LogEntry<'_>.
            let entries_ref = unsafe {
                std::mem::transmute::<&[crate::LogEntry<'static>], &[crate::LogEntry<'_>]>(
                    &self.scratch_entries,
                )
            };
            self.storage.append_entries(entries_ref)?;
            for entry in entries_ref {
                self.log_metadata.append(entry.index, entry.term);
            }

            if let Some(last) = self.scratch_entries.last() {
                self.cached_last_log = (last.index, last.term);
            }

            // Append-time config activation (Raft §4.1): if any newly-appended
            // entry is a config change, adopt it NOW — a server uses the latest
            // configuration in its log whether or not it has committed. This is
            // what lets a removed node recognize its removal (and stop
            // campaigning) the instant it receives the entry, instead of racing
            // to apply it before the leader drops it from replication.
            let latest_config = self.scratch_entries.iter().rev().find_map(|e| {
                crate::api::node::membership::ConfigChangeEntry::decode(e.payload.0)
            });
            self.scratch_entries.clear();
            if let Some(entry) = latest_config {
                self.apply_config_change(&entry)?;
            }
        }
        Ok(())
    }

    fn apply_append_entries_seeded(
        &mut self,
        msg: &AppendEntries,
        headers_bytes: &[u8],
        payloads: &[u8],
    ) -> Result<(), RaftError> {
        let entry_count = msg.entry_count.get() as usize;
        let mut append_from = entry_count;

        // Same contiguity guard as `apply_append_entries` (P1-7 index sweep):
        // reject a seeded batch whose header indices are not `prev_log_index+1`,
        // then strictly consecutive — a spoofed near-`u64::MAX` index would
        // otherwise corrupt the log-index arena.
        let expected_start = msg.prev_log_index.get().saturating_add(1);
        let iter = AppendEntriesRawIter::new(headers_bytes, SeededPayloads::Contiguous(payloads));
        for (idx, (incoming_header, _)) in iter.enumerate() {
            let incoming_index = LogIndex(incoming_header.index.get());
            if incoming_index.0 != expected_start.saturating_add(idx as u64) {
                return Err(RaftError::Protocol(
                    "seeded AppendEntries entries not contiguous with prev_log_index".into(),
                ));
            }
            let incoming_term = crate::Term(incoming_header.term.get());

            let local_term = match self.log_metadata.get_term(incoming_index) {
                Some(t) => Some(t),
                None => {
                    let mut dummy = [0u8; 8];
                    self.storage
                        .entry_at(incoming_index, &mut dummy)?
                        .map(|e| e.term)
                }
            };
            match local_term {
                Some(t) if t == incoming_term => {}
                Some(_) => {
                    self.storage_truncate(incoming_index)?;
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
            let headers_ref = Ref::<&[u8], [EntryHeader]>::from_bytes(headers_bytes)
                .map_err(|_| RaftError::Protocol("header alignment in seeded batch".into()))?;
            let headers = Ref::into_ref(headers_ref);

            let final_headers = &headers[append_from..];

            // Transform contiguous block to list of refs using scratchpad
            self.scratch_payload_refs.clear();
            let p_iter =
                AppendEntriesRawIter::new(headers_bytes, SeededPayloads::Contiguous(payloads));
            for (idx, (_, payload)) in p_iter.enumerate() {
                if idx >= append_from {
                    // SAFETY: ephemeral pointers cleared after storage call
                    self.scratch_payload_refs
                        .push(unsafe { std::mem::transmute::<&[u8], &'static [u8]>(payload) });
                }
            }

            self.storage
                .append_entries_seeded(final_headers, &self.scratch_payload_refs)?;
            self.scratch_payload_refs.clear();
            for h in final_headers {
                self.log_metadata
                    .append(LogIndex(h.index.get()), crate::Term(h.term.get()));
            }

            if let Some(last) = final_headers.last() {
                self.cached_last_log = (LogIndex(last.index.get()), crate::Term(last.term.get()));
            }

            // Append-time config activation (Raft §4.1) — same rule as the
            // contiguous `apply_append_entries` path: adopt the latest
            // config-change entry in the just-appended range immediately.
            let mut latest_config: Option<crate::api::node::membership::ConfigChangeEntry> = None;
            let p_iter =
                AppendEntriesRawIter::new(headers_bytes, SeededPayloads::Contiguous(payloads));
            for (idx, (_, payload)) in p_iter.enumerate() {
                if idx >= append_from {
                    if let Some(entry) =
                        crate::api::node::membership::ConfigChangeEntry::decode(payload)
                    {
                        latest_config = Some(entry);
                    }
                }
            }
            if let Some(entry) = latest_config {
                self.apply_config_change(&entry)?;
            }
        }
        Ok(())
    }

    pub(crate) async fn handle_append_entries(
        &mut self,
        from: PeerId,
        msg: &AppendEntries,
        payload: &[u8],
    ) -> Result<(), RaftError> {
        let term = crate::Term(msg.term.get());
        let leader_id = PeerId(msg.leader_id.get());
        let prev_log_idx = LogIndex(msg.prev_log_index.get());
        let prev_log_term = crate::Term(msg.prev_log_term.get());
        let leader_commit = LogIndex(msg.leader_commit.get());

        if term.0 < self.hard_state.current_term.0 {
            let resp = AppendEntriesResp {
                term: self.hard_state.current_term.0.into(),
                success: 0,
                match_index: self.cached_last_log.0 .0.into(),
                _pad: [0; 7],
            };
            self.send_message(from, &RaftMessage::AppendEntriesResp(&resp))
                .await;
            return Ok(());
        }
        if term.0 > self.hard_state.current_term.0 {
            self.step_down(term)?;
        }
        self.soft_state.role = Role::Follower;
        self.soft_state.is_leader = false;
        self.soft_state.leader_id = Some(leader_id);
        // Leader contact stamp for pre-vote leader-stickiness (§4.2.2).
        self.last_leader_contact = Some(std::time::Instant::now());

        let prev_ok = match self.log_metadata.get_term(prev_log_idx) {
            Some(t) => t == prev_log_term,
            None if prev_log_idx.0 == 0 => true,
            None => {
                let mut dummy = [0; 8];
                match self.storage.entry_at(prev_log_idx, &mut dummy)? {
                    Some(e) => e.term == prev_log_term,
                    // Entry may have been compacted by a snapshot. It matches iff
                    // it lines up with the snapshot boundary we last installed.
                    None => {
                        self.cached_last_log.0 == prev_log_idx
                            && self.cached_last_log.1 == prev_log_term
                    }
                }
            }
        };

        if !prev_ok {
            // Reject hint: point the leader at the conflict, never at our own
            // (possibly longer, divergent) tail. `min(last, prev-1)` keeps the
            // hint at or below the leader's prev_log_index, so its next_index
            // walks back correctly instead of jumping past its own log (which
            // would crash `term_at` with CorruptLog and kill the leader).
            let hint = self.cached_last_log.0 .0.min(prev_log_idx.0.saturating_sub(1));
            let resp = AppendEntriesResp {
                term: self.hard_state.current_term.0.into(),
                success: 0,
                match_index: hint.into(),
                _pad: [0; 7],
            };
            self.send_message(from, &RaftMessage::AppendEntriesResp(&resp))
                .await;
            return Ok(());
        }

        self.apply_append_entries(msg, payload)?;

        let last_log_index = self.cached_last_log.0;
        if leader_commit > self.soft_state.commit_index {
            self.set_commit_index(LogIndex(leader_commit.0.min(last_log_index.0)));
        }

        let resp = AppendEntriesResp {
            term: self.hard_state.current_term.0.into(),
            success: 1,
            match_index: last_log_index.0.into(),
            _pad: [0; 7],
        };
        self.send_message(from, &RaftMessage::AppendEntriesResp(&resp))
            .await;
        Ok(())
    }

    pub(crate) async fn handle_append_entries_seeded(
        &mut self,
        from: PeerId,
        msg: &AppendEntries,
        headers_bytes: &[u8],
        payloads: &[u8],
    ) -> Result<(), RaftError> {
        let term = crate::Term(msg.term.get());
        let leader_id = PeerId(msg.leader_id.get());
        let prev_log_idx = LogIndex(msg.prev_log_index.get());
        let prev_log_term = crate::Term(msg.prev_log_term.get());
        let leader_commit = LogIndex(msg.leader_commit.get());

        if term.0 < self.hard_state.current_term.0 {
            let resp = AppendEntriesResp {
                term: self.hard_state.current_term.0.into(),
                success: 0,
                match_index: self.cached_last_log.0 .0.into(),
                _pad: [0; 7],
            };
            self.send_message(from, &RaftMessage::AppendEntriesResp(&resp))
                .await;
            return Ok(());
        }
        if term.0 > self.hard_state.current_term.0 {
            self.step_down(term)?;
        }
        self.soft_state.role = Role::Follower;
        self.soft_state.is_leader = false;
        self.soft_state.leader_id = Some(leader_id);
        // Leader contact stamp for pre-vote leader-stickiness (§4.2.2).
        self.last_leader_contact = Some(std::time::Instant::now());

        let prev_ok = match self.log_metadata.get_term(prev_log_idx) {
            Some(t) => t == prev_log_term,
            None if prev_log_idx.0 == 0 => true,
            None => {
                let mut dummy = [0; 8];
                match self.storage.entry_at(prev_log_idx, &mut dummy)? {
                    Some(e) => e.term == prev_log_term,
                    // Entry may have been compacted by a snapshot. It matches iff
                    // it lines up with the snapshot boundary we last installed.
                    None => {
                        self.cached_last_log.0 == prev_log_idx
                            && self.cached_last_log.1 == prev_log_term
                    }
                }
            }
        };

        if !prev_ok {
            // Reject hint: point the leader at the conflict, never at our own
            // (possibly longer, divergent) tail. `min(last, prev-1)` keeps the
            // hint at or below the leader's prev_log_index, so its next_index
            // walks back correctly instead of jumping past its own log (which
            // would crash `term_at` with CorruptLog and kill the leader).
            let hint = self.cached_last_log.0 .0.min(prev_log_idx.0.saturating_sub(1));
            let resp = AppendEntriesResp {
                term: self.hard_state.current_term.0.into(),
                success: 0,
                match_index: hint.into(),
                _pad: [0; 7],
            };
            self.send_message(from, &RaftMessage::AppendEntriesResp(&resp))
                .await;
            return Ok(());
        }

        self.apply_append_entries_seeded(msg, headers_bytes, payloads)?;

        let last_log_index = self.cached_last_log.0;
        if leader_commit > self.soft_state.commit_index {
            self.set_commit_index(LogIndex(leader_commit.0.min(last_log_index.0)));
        }

        let resp = AppendEntriesResp {
            term: self.hard_state.current_term.0.into(),
            success: 1,
            match_index: last_log_index.0.into(),
            _pad: [0; 7],
        };
        self.send_message(from, &RaftMessage::AppendEntriesResp(&resp))
            .await;
        Ok(())
    }

    pub(crate) async fn handle_append_entries_response(
        &mut self,
        from: PeerId,
        resp: &AppendEntriesResp,
    ) -> Result<(), RaftError> {
        let resp_term = crate::Term(resp.term.get());
        let resp_match_index = LogIndex(resp.match_index.get());
        let resp_success = resp.success != 0;

        if resp_term.0 > self.hard_state.current_term.0 {
            self.step_down(resp_term)?;
            return Ok(());
        }
        if !self.is_leader() {
            return Ok(());
        }
        // The leader never advances a peer's next_index past its own last log
        // + 1. A divergent/longer follower reports a stale tail as its reject
        // hint; without this cap the walk-back could jump forward past our log
        // and crash `term_at` (CorruptLog), killing the leader.
        let next_index_cap = self.cached_last_log.0 .0.saturating_add(1);
        let Some(progress) = self.peer_progress.get_mut(&from) else {
            return Ok(());
        };
        if resp_success {
            if resp_match_index > progress.match_index {
                progress.match_index = resp_match_index;
            }
            let next_index = LogIndex(resp_match_index.0.saturating_add(1).min(next_index_cap));
            if next_index > progress.next_index {
                progress.next_index = next_index;
            }
        } else if resp_match_index >= progress.match_index {
            progress.next_index = LogIndex(
                progress
                    .next_index
                    .0
                    .saturating_sub(1)
                    .max(resp_match_index.0.saturating_add(1))
                    .max(1)
                    .min(next_index_cap),
            );
        }
        Ok(())
    }

    pub(crate) async fn advance_append_replication(
        &mut self,
        peer: PeerId,
        target_index: LogIndex,
        state: AppendAttemptState,
        resp: &AppendEntriesResp,
    ) -> Result<AppendAdvance, RaftError> {
        let resp_term = crate::Term(resp.term.get());
        let resp_match_index = LogIndex(resp.match_index.get());
        let resp_success = resp.success != 0;

        if resp_term.0 > self.hard_state.current_term.0 {
            self.step_down(resp_term)?;
            return Err(RaftError::TermChanged {
                current: self.hard_state.current_term,
            });
        }
        // Never advance next_index past the leader's own last log + 1 (see the
        // clamp rationale in `handle_append_entries_response`).
        let next_index_cap = self.cached_last_log.0 .0.saturating_add(1);
        let progress = self
            .peer_progress
            .get_mut(&peer)
            .ok_or(RaftError::PeerUnknown(peer))?;
        if resp_success {
            if resp_match_index < state.sent_last_index {
                return Ok(AppendAdvance::Ignored);
            }
            progress.match_index = resp_match_index;
            progress.next_index = LogIndex(resp_match_index.0.saturating_add(1).min(next_index_cap));
            if progress.match_index >= target_index {
                return Ok(AppendAdvance::Completed);
            }
        } else {
            if resp_match_index < progress.match_index {
                return Ok(AppendAdvance::Ignored);
            }
            progress.next_index = LogIndex(
                progress
                    .next_index
                    .0
                    .saturating_sub(1)
                    .max(resp_match_index.0.saturating_add(1))
                    .max(1)
                    .min(next_index_cap),
            );
        }
        let next_attempt = state.attempts + 1;
        if let Some(sent_last_index) = self.send_append_attempt(peer, next_attempt).await? {
            Ok(AppendAdvance::Retry(AppendAttemptState {
                attempts: next_attempt,
                sent_last_index,
            }))
        } else {
            Ok(AppendAdvance::Dropped)
        }
    }
}
