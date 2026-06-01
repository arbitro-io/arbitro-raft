use std::time::{Duration, Instant};
use tracing::{debug, info};

use super::RaftNode;
use crate::protocol::{RequestVote, RequestVoteResp};
use crate::{InboundRaftMessage, RaftError, RaftMessage, Role, Term};

impl<S, T> RaftNode<S, T>
where
    S: crate::RaftStorage,
    T: crate::RaftTransport,
{
    pub async fn campaign_once(&mut self, inbound_buf: &mut [u8]) -> Result<bool, RaftError> {
        let started = super::trace_enabled().then(Instant::now);
        self.soft_state.role = Role::Candidate;
        self.soft_state.is_leader = false;
        self.soft_state.leader_id = None;
        self.hard_state.current_term = Term(self.hard_state.current_term.0 + 1);
        self.hard_state.voted_for = Some(self.config.node_id);
        self.storage.save_hard_state(&self.hard_state)?;

        let term = self.hard_state.current_term;
        let votes_needed = super::quorum(self.config.peers.len());
        info!(node_id = self.config.node_id.0, term = term.0, "starting election");
        if super::trace_enabled() {
            super::trace_log(
                self.config.node_id,
                format!("campaign_once start term={} votes_needed={}", term.0, votes_needed),
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

        if !self.collect_votes(term, votes_needed, possible_votes, inbound_buf).await? {
            return Ok(false);
        }

        self.soft_state.role = Role::Leader;
        self.soft_state.is_leader = true;
        self.soft_state.leader_id = Some(self.config.node_id);
        self.initialize_leader_progress()?;

        info!(node_id = self.config.node_id.0, term = term.0, "leader elected");
        if super::trace_enabled() {
            let total_us = started.map(|s| s.elapsed().as_micros()).unwrap_or(0);
            super::trace_log(
                self.config.node_id,
                format!("campaign_once elected term={} total_us={}", term.0, total_us),
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
        let timeout = self.election_timeout();

        while votes < votes_needed && self.scratch_responders.len() < possible_votes {
            let n = match self
                .transport
                .recv_frame_timeout(timeout, inbound_buf)
                .await?
            {
                Some(n) => n,
                None => return Err(RaftError::NoQuorum),
            };
            let inbound = crate::decode_message(&inbound_buf[..n])?;
            let from = inbound.from;

            match inbound.message {
                RaftMessage::RequestVoteResp(resp) => {
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
                message => {
                    self.handle_inbound(InboundRaftMessage { from, message })
                        .await?;
                }
            }
        }
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

    pub(crate) fn election_timeout(&self) -> Duration {
        Duration::from_millis(self.config.timing.election_max_ms.max(1))
    }

    pub async fn campaign_pre_vote(&mut self, inbound_buf: &mut [u8]) -> Result<bool, RaftError> {
        let pre_vote_term = Term(self.hard_state.current_term.0 + 1);
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
        let timeout = self.election_timeout();

        while votes < votes_needed && self.scratch_responders.len() < possible_votes {
            let n = match self
                .transport
                .recv_frame_timeout(timeout, inbound_buf)
                .await?
            {
                Some(n) => n,
                None => return Ok(false),
            };
            let inbound = crate::decode_message(&inbound_buf[..n])?;
            let from = inbound.from;

            match inbound.message {
                RaftMessage::PreVoteResp(resp) => {
                    if self.scratch_responders.contains(&from) {
                        continue;
                    }
                    self.scratch_responders.push(from);
                    let resp_term = Term(resp.term.get());
                    if resp_term.0 > self.hard_state.current_term.0 {
                        self.step_down(resp_term)?;
                        return Ok(false);
                    }
                    if resp_term == pre_vote_term && resp.vote_granted != 0 {
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
                message => {
                    self.handle_inbound(InboundRaftMessage { from, message })
                        .await?;
                }
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

        let can_grant = msg_term.0 >= self.hard_state.current_term.0
            && candidate_up_to_date;

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
}
