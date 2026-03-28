use bytes::Bytes;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

use super::progress::{AppendAdvance, AppendAttemptState};
use super::RaftNode;
use crate::{
    AppendEntries, AppendEntriesResp, AppendEntriesRespView, AppendEntriesView, EntryPayload,
    InboundRaftMessageView, LogEntry, LogIndex, PeerId, RaftError, RaftMessage, Role, Term,
};

impl<S, T> RaftNode<S, T>
where
    S: crate::RaftStorage,
    T: crate::RaftTransport,
{
    pub async fn send_heartbeat_once(&mut self) -> Result<(), RaftError> {
        if !self.is_leader() {
            return Err(RaftError::NotLeader {
                leader_hint: self
                    .soft_state
                    .leader_id
                    .map(|l| crate::LeaderHint { leader_id: l }),
            });
        }
        self.ensure_leader_progress_initialized()?;
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

        let mut sent = 0usize;
        for i in 0..self.scratch_peers.len() {
            let peer = self.scratch_peers[i];
            let msg = self.build_append_for_peer(peer)?;
            if self
                .send_best_effort(peer, RaftMessage::AppendEntries(msg), "heartbeat")
                .await
            {
                sent += 1;
            }
        }
        info!(
            node_id = self.config.node_id.0,
            term = self.hard_state.current_term.0,
            sent,
            "heartbeat sent"
        );
        Ok(())
    }

    pub async fn propose_once(&mut self, payload: Bytes) -> Result<LogIndex, RaftError> {
        let mut payloads = Vec::with_capacity(1);
        payloads.push(payload);
        let indexes = self.propose_batch_once(payloads).await?;
        Ok(*indexes
            .first()
            .ok_or(RaftError::Protocol("propose failed to return index".into()))?)
    }

    pub async fn propose_batch_once(
        &mut self,
        payloads: Vec<Bytes>,
    ) -> Result<Vec<LogIndex>, RaftError> {
        let started = Instant::now();
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

        let last_log_index = self.storage.last_log_position()?.0;
        self.scratch_entries.clear();
        self.scratch_indexes.clear();
        let mut next_raw = last_log_index.0 + 1;
        for payload in payloads {
            let next_index = LogIndex(next_raw);
            self.scratch_entries.push(LogEntry {
                term: self.hard_state.current_term,
                index: next_index,
                payload: EntryPayload(payload),
            });
            self.scratch_indexes.push(next_index);
            next_raw += 1;
        }
        let last_index = *self.scratch_indexes.last().unwrap();
        self.storage.append_entries(&self.scratch_entries)?;
        self.ensure_leader_progress_initialized()?;

        let needed = super::quorum(self.config.peers.len());
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

        let mut accepted = 1usize;
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

        let timeout = Duration::from_millis(self.config.timing.heartbeat_ms as u64 * 2);
        while accepted < needed && !self.scratch_pending.is_empty() {
            let Some(inbound) = self.transport.recv_timeout(timeout).await? else {
                break;
            };
            let from = inbound.from;
            match inbound.message {
                crate::RaftMessageView::AppendEntriesResp(resp) => {
                    let Some(state) = self.scratch_pending.get(&from).copied() else {
                        self.handle_append_entries_response(resp).await?;
                        continue;
                    };
                    match self
                        .advance_append_replication(from, last_index, state, resp)
                        .await?
                    {
                        AppendAdvance::Completed => {
                            self.scratch_pending.remove(&from);
                            accepted += 1;
                        }
                        AppendAdvance::Retry(next_state) => {
                            self.scratch_pending.insert(from, next_state);
                        }
                        _ => {
                            self.scratch_pending.remove(&from);
                        }
                    }
                }
                message => {
                    self.handle_inbound(InboundRaftMessageView { from, message })
                        .await?;
                }
            }
        }

        if accepted < needed {
            return Err(RaftError::NoQuorum);
        }
        self.hard_state.commit_index = last_index;
        self.storage.save_hard_state(&self.hard_state)?;
        super::trace_log(
            self.config.node_id,
            format!(
                "propose_batch_once committed last_index={} total_us={}",
                last_index.0,
                started.elapsed().as_micros()
            ),
        );
        self.drain_inbound_ready().await?;
        Ok(self.scratch_indexes.clone())
    }

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
        let prev_log_term = self.term_at(prev_log_index)?;
        self.scratch_entries.clear();
        self.storage.read_entries(
            progress.next_index,
            LogIndex(u64::MAX),
            &mut self.scratch_entries,
        )?;
        self.scratch_entries
            .truncate(self.config.limits.append_batch_entries.max(1));
        AppendEntries::new(
            self.hard_state.current_term,
            self.config.node_id,
            prev_log_index,
            prev_log_term,
            self.hard_state.commit_index,
            &self.scratch_entries,
        )
    }

    pub(crate) async fn send_append_attempt(
        &mut self,
        peer: PeerId,
        _attempt: u64,
    ) -> Result<Option<LogIndex>, RaftError> {
        let msg = self.build_append_for_peer(peer)?;
        let mut last_idx = msg.prev_log_index();
        if let Some(last) = msg.entries()?.last() {
            last_idx = last.index();
        }
        if self
            .send_best_effort(peer, RaftMessage::AppendEntries(msg), "append")
            .await
        {
            Ok(Some(last_idx))
        } else {
            Ok(None)
        }
    }

    pub(crate) async fn handle_append_entries(
        &mut self,
        msg: AppendEntriesView,
    ) -> Result<(), RaftError> {
        if msg.term().0 < self.hard_state.current_term.0 {
            self.transport
                .send(
                    msg.from(),
                    RaftMessage::AppendEntriesResp(AppendEntriesResp {
                        term: self.hard_state.current_term,
                        success: false,
                        match_index: self.storage.last_log_position()?.0,
                    }),
                )
                .await?;
            return Ok(());
        }

        if msg.term().0 > self.hard_state.current_term.0 {
            self.step_down(msg.term())?;
        }
        self.soft_state.role = Role::Follower;
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
            self.transport
                .send(
                    msg.from(),
                    RaftMessage::AppendEntriesResp(AppendEntriesResp {
                        term: self.hard_state.current_term,
                        success: false,
                        match_index: self.storage.last_log_position()?.0,
                    }),
                )
                .await?;
            return Ok(());
        }

        let entry_count = msg.entry_count();
        let mut append_from = entry_count;
        for (idx, incoming) in msg.entries()?.enumerate() {
            match self.storage.entry_at(incoming.index())? {
                Some(local) if local.term == incoming.term() => {}
                Some(_) => {
                    self.storage.truncate_suffix(incoming.index())?;
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
        }

        let last_log_index = self.storage.last_log_position()?.0;
        if msg.leader_commit() > self.hard_state.commit_index {
            self.hard_state.commit_index = LogIndex(msg.leader_commit().0.min(last_log_index.0));
            self.storage.save_hard_state(&self.hard_state)?;
        }

        self.transport
            .send(
                msg.from(),
                RaftMessage::AppendEntriesResp(AppendEntriesResp {
                    term: self.hard_state.current_term,
                    success: true,
                    match_index: last_log_index,
                }),
            )
            .await?;
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
            return Err(RaftError::TermChanged {
                current: self.hard_state.current_term,
            });
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
            progress.next_index = LogIndex(resp.match_index().0 + 1);
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
            Ok(AppendAdvance::Retry(AppendAttemptState {
                attempts: next_attempt,
                sent_last_index,
            }))
        } else {
            Ok(AppendAdvance::Dropped)
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
                super::progress::PeerProgress {
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

    pub(crate) async fn send_best_effort(
        &self,
        peer: PeerId,
        msg: RaftMessage,
        _phase: &str,
    ) -> bool {
        self.transport.send(peer, msg).await.is_ok()
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

    pub(crate) async fn drain_inbound_ready(&mut self) -> Result<(), RaftError> {
        while let Some(inbound) = self.transport.recv_timeout(Duration::ZERO).await? {
            let from = inbound.from;
            self.handle_inbound(InboundRaftMessageView {
                from,
                message: inbound.message,
            })
            .await?;
        }
        Ok(())
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
        let last_log_index = self.storage.last_log_position()?.0;
        self.scratch_entries.clear();
        self.scratch_indexes.clear();
        let mut next_raw = last_log_index.0 + 1;
        for payload in payloads {
            let next_index = LogIndex(next_raw);
            self.scratch_entries.push(LogEntry {
                term: self.hard_state.current_term,
                index: next_index,
                payload: EntryPayload(payload.clone()),
            });
            self.scratch_indexes.push(next_index);
            next_raw += 1;
        }
        self.storage.append_entries(&self.scratch_entries)?;
        self.ensure_leader_progress_initialized()?;

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
            let _ = self.send_append_attempt(peer, 1).await;
        }
        Ok(self.scratch_indexes.clone())
    }
}
