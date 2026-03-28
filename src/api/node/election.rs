use std::collections::HashSet;
use std::time::{Duration, Instant};
use tracing::{debug, info};

use crate::{
    RaftError, RaftMessage, RequestVote, RequestVoteResp,
    RequestVoteRespView, RequestVoteView, Role, Term, InboundRaftMessageView,
};
use super::RaftNode;

impl<S, T> RaftNode<S, T>
where
    S: crate::RaftStorage,
    T: crate::RaftTransport,
{
    pub async fn campaign_once(&mut self) -> Result<bool, RaftError> {
        let started = Instant::now();
        self.soft_state.role = Role::Candidate;
        self.soft_state.is_leader = false;
        self.soft_state.leader_id = None;
        self.hard_state.current_term = Term(self.hard_state.current_term.0 + 1);
        self.hard_state.voted_for = Some(self.config.node_id);
        // Persist before any send — guide §Orden de persistencia
        self.storage.save_hard_state(&self.hard_state)?;

        let term = self.hard_state.current_term;
        let votes_needed = super::quorum(self.config.peers.len());
        let mut votes = 1usize;
        let mut possible_votes = 1usize;
        let mut responders = HashSet::new();

        info!(node_id = self.config.node_id.0, term = term.0, "starting election");
        super::trace_log(self.config.node_id, format!("campaign_once start term={} votes_needed={}", term.0, votes_needed));

        let (last_log_index, last_log_term) = self.storage.last_log_position()?;

        let req = RequestVote {
            term,
            candidate_id: self.config.node_id,
            last_log_index,
            last_log_term,
        };

        for peer in self.config.peers.iter().copied().filter(|peer| *peer != self.config.node_id) {
            if self.encode_and_send_best_effort(peer, &RaftMessage::RequestVote(req.clone())).await {
                possible_votes += 1;
            }
        }

        if possible_votes < votes_needed {
            return Err(RaftError::NoQuorum);
        }

        let timeout = self.election_timeout();
        // Collect votes until we have quorum or all possible respondents have answered.
        // Must wait for ALL possible_votes — not possible_votes-1 — to avoid discarding
        // the decisive vote in tight clusters.
        while votes < votes_needed && responders.len() < possible_votes {
            let raw = match self.transport.recv_frame_timeout(timeout).await? {
                Some(r) => r,
                None => return Err(RaftError::NoQuorum),
            };
            let inbound = crate::decode_message_view(raw)?;
            let from = inbound.from;

            match inbound.message {
                crate::RaftMessageView::RequestVoteResp(resp) => {
                    if !responders.insert(from) { continue; }
                    if resp.term().0 > self.hard_state.current_term.0 {
                        self.step_down(resp.term())?;
                        return Ok(false);
                    }
                    if resp.term() == term && resp.vote_granted() {
                        votes += 1;
                        debug!(node_id = self.config.node_id.0, voter = from.0, votes, needed = votes_needed, "vote granted");
                    }
                }
                message => {
                    self.handle_inbound(InboundRaftMessageView { from, message }).await?;
                }
            }
        }

        if votes < votes_needed {
            return Err(RaftError::NoQuorum);
        }

        self.soft_state.role = Role::Leader;
        self.soft_state.is_leader = true;
        self.soft_state.leader_id = Some(self.config.node_id);
        self.initialize_leader_progress()?;

        info!(node_id = self.config.node_id.0, term = term.0, "leader elected");
        super::trace_log(self.config.node_id, format!("campaign_once elected term={} total_us={}", term.0, started.elapsed().as_micros()));
        Ok(true)
    }

    pub(crate) async fn handle_request_vote(&mut self, msg: RequestVoteView) -> Result<(), RaftError> {
        if msg.term().0 > self.hard_state.current_term.0 {
            self.step_down(msg.term())?;
        }

        let (last_log_index, last_log_term) = self.storage.last_log_position()?;
        let candidate_up_to_date = msg.last_log_term().0 > last_log_term.0
            || (msg.last_log_term() == last_log_term && msg.last_log_index() >= last_log_index);

        let can_vote = msg.term() == self.hard_state.current_term
            && candidate_up_to_date
            && self.hard_state.voted_for.map(|voted| voted == msg.candidate_id()).unwrap_or(true);

        if can_vote {
            self.hard_state.voted_for = Some(msg.candidate_id());
            self.storage.save_hard_state(&self.hard_state)?;
        }

        let frame = self.encode_msg(&RaftMessage::RequestVoteResp(RequestVoteResp {
            term: self.hard_state.current_term,
            vote_granted: can_vote,
        }))?;
        self.transport.send_frame(msg.from(), frame).await?;
        Ok(())
    }

    pub(crate) async fn handle_request_vote_response(&mut self, resp: RequestVoteRespView) -> Result<(), RaftError> {
        if resp.term().0 > self.hard_state.current_term.0 {
            self.step_down(resp.term())?;
        }
        Ok(())
    }

    pub(crate) fn election_timeout(&self) -> Duration {
        Duration::from_millis(self.config.timing.election_max_ms.max(1))
    }
}
