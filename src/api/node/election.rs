use super::RaftNode;
use crate::protocol::{RequestVote, RequestVoteResp};
use crate::{InboundRaftMessage, RaftError, RaftMessage, Role, Term};
use std::time::{Duration, Instant};
use tracing::{debug, info};

impl<S, T> RaftNode<S, T>
where
    S: crate::RaftStorage,
    T: crate::RaftTransport,
{
    pub async fn campaign_once(&mut self, inbound_buf: &mut [u8]) -> Result<bool, RaftError> {
        // Self-membership guard (see `campaign_pre_vote`): a non-voter must not
        // seek leadership even if the pre-vote path is bypassed.
        if !self.config.peers.contains(&self.config.node_id) {
            return Ok(false);
        }
        let started = super::trace_enabled().then(Instant::now);
        self.soft_state.role = Role::Candidate;
        self.soft_state.is_leader = false;
        self.soft_state.leader_id = None;
        // Saturating so an adversarially-large adopted term (a peer can send
        // `u64::MAX`) can never wrap to 0 on the next election — a term-0
        // candidate would re-enter an already-decided term (P1-7). At u64::MAX
        // the node simply stops advancing, which is safe (it can still follow).
        self.hard_state.current_term = Term(self.hard_state.current_term.0.saturating_add(1));
        self.hard_state.voted_for = Some(self.config.node_id);
        self.storage.save_hard_state(&self.hard_state)?;
        let term = self.hard_state.current_term;
        let votes_needed = super::quorum(self.config.peers.len());
        info!(
            node_id = self.config.node_id.0,
            term = term.0,
            "starting election"
        );
        if super::trace_enabled() {
            super::trace_log(
                self.config.node_id,
                format!(
                    "campaign_once start term={} votes_needed={}",
                    term.0, votes_needed
                ),
            );
        }

        let (last_log_idx, last_log_term) = self.storage.last_log_position()?;
        let req = RequestVote {
            term: term.0.into(),
            candidate_id: self.config.node_id.0.into(),
            last_log_index: last_log_idx.0.into(),
            last_log_term: last_log_term.0.into(),
        };
        let possible_votes = self.broadcast_request_vote(&req).await?;
        if possible_votes < votes_needed {
            return Err(RaftError::NoQuorum);
        }

        if !self
            .collect_votes(term, votes_needed, possible_votes, inbound_buf)
            .await?
        {
            return Ok(false);
        }
        // Defense in depth (Election Safety): only assume leadership if we are
        // still the candidate for the exact term we campaigned in. collect_votes
        // already guarantees this, but the crown must never be claimed on a
        // stale term even if that guard were ever weakened.
        if self.soft_state.role != Role::Candidate || self.hard_state.current_term != term {
            return Ok(false);
        }
        self.soft_state.role = Role::Leader;
        self.soft_state.is_leader = true;
        self.soft_state.leader_id = Some(self.config.node_id);
        self.initialize_leader_progress()?;
        info!(
            node_id = self.config.node_id.0,
            term = term.0,
            "leader elected"
        );
        if super::trace_enabled() {
            let total_us = started.map(|s| s.elapsed().as_micros()).unwrap_or(0);
            super::trace_log(
                self.config.node_id,
                format!(
                    "campaign_once elected term={} total_us={}",
                    term.0, total_us
                ),
            );
        }
        Ok(true)
    }
    async fn broadcast_request_vote(&mut self, req: &RequestVote) -> Result<usize, RaftError> {
        self.scratch_peers.clear();
        for peer in self.config.peers.iter().copied() {
            if peer != self.config.node_id {
                self.scratch_peers.push(peer);
            }
        }
        let msg = RaftMessage::RequestVote(req);
        let frame = crate::protocol::encode_message_to_bytes(self.config.node_id, &msg)?;
        let mut sends = Vec::with_capacity(self.scratch_peers.len());
        for i in 0..self.scratch_peers.len() {
            let peer = self.scratch_peers[i];
            sends.push(self.transport.send_frame_owned(peer, frame.clone()));
        }
        let mut possible_votes = 1usize;
        for res in futures::future::join_all(sends).await {
            if res.is_ok() {
                possible_votes += 1;
            }
        }
        Ok(possible_votes)
    }
    async fn collect_votes(
        &mut self,
        term: Term,
        votes_needed: usize,
        possible_votes: usize,
        inbound_buf: &mut [u8],
    ) -> Result<bool, RaftError> {
        let mut votes = 1usize;
        self.scratch_responders.clear();
        let deadline = Instant::now() + self.election_timeout();

        while votes < votes_needed && self.scratch_responders.len() < possible_votes {
            let mut msg_slots = [None; 16];
            let messages_count = self
                .drain_inbound_frames(deadline, inbound_buf, &mut msg_slots)
                .await?;

            if messages_count == 0 {
                return Err(RaftError::NoQuorum);
            }

            // 1. Process responses first
            for slot in msg_slots.iter().take(messages_count) {
                let inbound = slot.unwrap();
                let from = inbound.from;
                if let RaftMessage::RequestVoteResp(resp) = inbound.message {
                    if self.scratch_responders.contains(&from) {
                        continue;
                    }
                    self.scratch_responders.push(from);
                    let resp_term = Term(resp.term.get());
                    if resp_term.0 > self.hard_state.current_term.0 {
                        self.step_down(resp_term)?;
                        return Ok(false);
                    }
                    if resp_term == term && resp.vote_granted != 0 {
                        votes += 1;
                        debug!(
                            node_id = self.config.node_id.0,
                            voter = from.0,
                            votes,
                            needed = votes_needed,
                            "vote granted"
                        );
                    }
                }
            }

            if votes >= votes_needed {
                return Ok(true);
            }

            // 2. Process requests second
            for slot in msg_slots.iter().take(messages_count) {
                let inbound = slot.unwrap();
                if !matches!(inbound.message, RaftMessage::RequestVoteResp(_)) {
                    if let Err(e) = self.handle_inbound(inbound).await {
                        if e.is_fatal() {
                            return Err(e);
                        }
                        tracing::warn!(
                            node_id = self.config.node_id.0,
                            error = %e,
                            "dropping frame after non-fatal handler error during campaign"
                        );
                    }
                }
            }

            // Handling a request (an AppendEntries or RequestVote at a higher
            // term) may have stepped us down and adopted a new term. If we are
            // no longer a candidate for THIS term, abandon the campaign — the
            // votes accumulated so far were cast for the old term, and counting
            // them to `votes_needed` would crown us leader of a term we never
            // won (an Election-Safety violation).
            if self.soft_state.role != Role::Candidate
                || self.hard_state.current_term != term
            {
                return Ok(false);
            }
        }
        // Loop exit condition already guarantees role/term were unchanged since
        // the last check above (the `while` body ran to completion), so the
        // accumulated `votes` are all for `term`.
        Ok(votes >= votes_needed)
    }

    pub(crate) async fn handle_request_vote(
        &mut self,
        from: crate::PeerId,
        msg: &RequestVote,
    ) -> Result<(), RaftError> {
        let msg_term = Term(msg.term.get());
        let candidate_id = crate::PeerId(msg.candidate_id.get());
        let msg_log_term = Term(msg.last_log_term.get());
        let msg_log_idx = crate::LogIndex(msg.last_log_index.get());

        if msg_term.0 > self.hard_state.current_term.0 {
            self.step_down(msg_term)?;
        }

        let (last_log_index, last_log_term) = self.storage.last_log_position()?;
        let candidate_up_to_date = msg_log_term.0 > last_log_term.0
            || (msg_log_term == last_log_term && msg_log_idx >= last_log_index);

        let can_vote = msg_term == self.hard_state.current_term
            && candidate_up_to_date
            && self
                .hard_state
                .voted_for
                .map(|voted| voted == candidate_id)
                .unwrap_or(true);

        if can_vote {
            self.hard_state.voted_for = Some(candidate_id);
            self.storage.save_hard_state(&self.hard_state)?;
        }

        let resp_msg = RequestVoteResp {
            term: self.hard_state.current_term.0.into(),
            vote_granted: if can_vote { 1 } else { 0 },
            _pad: [0; 7],
        };
        self.send_message(from, &RaftMessage::RequestVoteResp(&resp_msg))
            .await;
        Ok(())
    }

    pub(crate) async fn handle_request_vote_response(
        &mut self,
        _from: crate::PeerId,
        resp: &RequestVoteResp,
    ) -> Result<(), RaftError> {
        let term = Term(resp.term.get());
        if term.0 > self.hard_state.current_term.0 {
            self.step_down(term)?;
        }
        Ok(())
    }
}

impl<S, T> RaftNode<S, T>
where
    S: crate::RaftStorage,
    T: crate::RaftTransport,
{
    pub(crate) fn election_timeout(&self) -> Duration {
        Duration::from_millis(self.config.timing.election_max_ms.max(1))
    }

    pub async fn campaign_pre_vote(&mut self, inbound_buf: &mut [u8]) -> Result<bool, RaftError> {
        // A node that is not a voter in its own configuration — e.g. one just
        // removed by a committed config change — must never campaign. A removed
        // server starting elections can disrupt or even seize a cluster it no
        // longer belongs to (Raft §4.2.2 / self-membership).
        if !self.config.peers.contains(&self.config.node_id) {
            return Ok(false);
        }
        // Saturating (P1-7): the hypothetical next term must not wrap to 0.
        let pre_vote_term = Term(self.hard_state.current_term.0.saturating_add(1));
        let votes_needed = super::quorum(self.config.peers.len());

        info!(
            node_id = self.config.node_id.0,
            term = pre_vote_term.0,
            "starting pre-vote phase"
        );

        let (last_log_idx, last_log_term) = self.storage.last_log_position()?;
        let req = RequestVote {
            term: pre_vote_term.0.into(),
            candidate_id: self.config.node_id.0.into(),
            last_log_index: last_log_idx.0.into(),
            last_log_term: last_log_term.0.into(),
        };

        self.scratch_peers.clear();
        for peer in self.config.peers.iter().copied() {
            if peer != self.config.node_id {
                self.scratch_peers.push(peer);
            }
        }

        let msg = RaftMessage::PreVote(&req);
        let frame = crate::protocol::encode_message_to_bytes(self.config.node_id, &msg)?;
        let mut sends = Vec::with_capacity(self.scratch_peers.len());
        for i in 0..self.scratch_peers.len() {
            let peer = self.scratch_peers[i];
            sends.push(self.transport.send_frame_owned(peer, frame.clone()));
        }

        let mut possible_votes = 1usize;
        for res in futures::future::join_all(sends).await {
            if res.is_ok() {
                possible_votes += 1;
            }
        }

        if possible_votes < votes_needed {
            return Ok(false);
        }

        let mut votes = 1usize;
        self.scratch_responders.clear();
        // Absolute deadline: processing non-response messages (concurrent
        // PreVote requests) must not extend the collection window, otherwise
        // split-vote persists indefinitely with 3+ simultaneous campaigns.
        let deadline = Instant::now() + self.election_timeout();

        while votes < votes_needed && self.scratch_responders.len() < possible_votes {
            let mut msg_slots = [None; 16];
            let messages_count = self
                .drain_inbound_frames(deadline, inbound_buf, &mut msg_slots)
                .await?;

            if messages_count == 0 {
                return Ok(false); // Timeout or empty
            }

            // 1. Process responses first
            for slot in msg_slots.iter().take(messages_count) {
                let inbound = slot.unwrap();
                let from = inbound.from;
                if let RaftMessage::PreVoteResp(resp) = inbound.message {
                    if self.scratch_responders.contains(&from) {
                        continue;
                    }
                    self.scratch_responders.push(from);
                    let resp_term = Term(resp.term.get());
                    if resp_term.0 > self.hard_state.current_term.0 {
                        self.step_down(resp_term)?;
                        return Ok(false);
                    }
                    if resp_term == self.hard_state.current_term && resp.vote_granted != 0 {
                        votes += 1;
                        debug!(
                            node_id = self.config.node_id.0,
                            voter = from.0,
                            votes,
                            needed = votes_needed,
                            "pre-vote granted"
                        );
                    }
                }
            }

            if votes >= votes_needed {
                return Ok(true);
            }

            // 2. Process requests second
            for slot in msg_slots.iter().take(messages_count) {
                let inbound = slot.unwrap();
                if !matches!(inbound.message, RaftMessage::PreVoteResp(_)) {
                    self.handle_inbound(inbound).await?;
                }
            }

            // A concurrent higher-term message handled above may have advanced
            // our term via step-down. If our term has reached the pre-vote term
            // (start term + 1), the cluster has moved on — abandon the pre-vote
            // rather than proceed to a disruptive real election.
            if self.hard_state.current_term.0 >= pre_vote_term.0 {
                return Ok(false);
            }
        }

        Ok(votes >= votes_needed)
    }

    pub(crate) async fn handle_pre_vote(
        &mut self,
        from: crate::PeerId,
        msg: &RequestVote,
    ) -> Result<(), RaftError> {
        let msg_term = Term(msg.term.get());
        let msg_log_term = Term(msg.last_log_term.get());
        let msg_log_idx = crate::LogIndex(msg.last_log_index.get());

        let (last_log_index, last_log_term) = self.storage.last_log_position()?;
        let candidate_up_to_date = msg_log_term.0 > last_log_term.0
            || (msg_log_term == last_log_term && msg_log_idx >= last_log_index);

        // Leader-stickiness (§4.2.2): deny the pre-vote while a leader is active.
        // A leader is active if EITHER we are the leader ourselves — a leader
        // never stamps `last_leader_contact` for itself, so this arm is what
        // stops a leader from granting a challenger's pre-vote and handing away
        // its own term (G2) — OR we accepted an AppendEntries from a current
        // leader within the minimum election timeout. This prevents a
        // partitioned node, or one just removed by a config change and still
        // campaigning, from disrupting a healthy cluster and seizing leadership
        // under a stale configuration.
        let leader_active = self.is_leader()
            || self.last_leader_contact.is_some_and(|t| {
                t.elapsed()
                    < std::time::Duration::from_millis(self.config.timing.election_min_ms.max(1))
            });

        let can_grant =
            !leader_active && msg_term.0 >= self.hard_state.current_term.0 && candidate_up_to_date;

        let resp_msg = RequestVoteResp {
            term: self.hard_state.current_term.0.into(),
            vote_granted: if can_grant { 1 } else { 0 },
            _pad: [0; 7],
        };
        self.send_message(from, &RaftMessage::PreVoteResp(&resp_msg))
            .await;
        Ok(())
    }

    pub(crate) async fn handle_pre_vote_response(
        &mut self,
        _from: crate::PeerId,
        resp: &RequestVoteResp,
    ) -> Result<(), RaftError> {
        let term = Term(resp.term.get());
        if term.0 > self.hard_state.current_term.0 {
            self.step_down(term)?;
        }
        Ok(())
    }

    async fn drain_inbound_frames<'a>(
        &self,
        deadline: Instant,
        inbound_buf: &'a mut [u8],
        msg_slots: &mut [Option<InboundRaftMessage<'a>>; 16],
    ) -> Result<usize, RaftError> {
        let mut messages_count = 0;
        let mut rest_buf = inbound_buf;

        // Block for the first frame until `deadline`, then ZERO-poll the rest.
        // A frame we cannot decode is skipped (not fatal), and a non-fatal recv
        // error ends the drain for this tick — a single bad or version-skewed
        // peer must never terminate a campaign (P0-2). Only a Fatal-class error
        // (local storage / corrupt log) propagates.
        while messages_count < 16 && !rest_buf.is_empty() {
            let timeout = if messages_count == 0 {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    break;
                }
                remaining
            } else {
                Duration::ZERO
            };

            let recv = match self.transport.recv_frame_timeout(timeout, rest_buf).await {
                Ok(opt) => opt,
                Err(e) if e.is_fatal() => return Err(e),
                Err(e) => {
                    tracing::warn!(
                        node_id = self.config.node_id.0,
                        error = %e,
                        "tolerating non-fatal recv while draining campaign frames"
                    );
                    break;
                }
            };
            let Some(n) = recv else { break };

            let (frame_buf, next_buf) = rest_buf.split_at_mut(n);
            rest_buf = next_buf;
            match crate::decode_message(frame_buf) {
                Ok(inbound) => {
                    msg_slots[messages_count] = Some(inbound);
                    messages_count += 1;
                }
                Err(e) => {
                    tracing::warn!(
                        node_id = self.config.node_id.0,
                        error = %e,
                        "dropping undecodable frame while draining campaign frames"
                    );
                }
            }
        }

        Ok(messages_count)
    }
}
