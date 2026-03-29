use crate::{LogIndex, PeerId, RaftError, RequestVote, RequestVoteResp, Term};
use crate::protocol::codec::wire::RaftFrameView;

#[derive(Debug, Clone)]
pub struct RequestVoteView {
    pub(crate) from:  PeerId,
    pub(crate) frame: RaftFrameView,
}

impl RequestVoteView {
    pub(crate) fn new(from: PeerId, frame: RaftFrameView) -> Self {
        Self { from, frame }
    }

    #[inline] pub fn from(&self) -> PeerId    { self.from }
    #[inline] pub fn term(&self) -> Term      { Term(self.frame.body_u64(0)) }
    #[inline] pub fn candidate_id(&self) -> PeerId { PeerId(self.frame.body_u64(8)) }
    #[inline] pub fn last_log_index(&self) -> LogIndex { LogIndex(self.frame.body_u64(16)) }
    #[inline] pub fn last_log_term(&self) -> Term  { Term(self.frame.body_u64(24)) }

    pub fn to_owned(&self) -> RequestVote {
        RequestVote {
            term:           self.term(),
            candidate_id:   self.candidate_id(),
            last_log_index: self.last_log_index(),
            last_log_term:  self.last_log_term(),
        }
    }
}

// ── RequestVoteRespView ───────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct RequestVoteRespView {
    pub(crate) from:  PeerId,
    pub(crate) frame: RaftFrameView,
}

impl RequestVoteRespView {
    pub(crate) fn new(from: PeerId, frame: RaftFrameView) -> Self {
        Self { from, frame }
    }

    #[inline] pub fn from(&self) -> PeerId  { self.from }
    #[inline] pub fn term(&self) -> Term    { Term(self.frame.body_u64(0)) }
    #[inline] pub fn vote_granted(&self) -> bool { self.frame.body_byte(8) != 0 }

    pub fn to_owned(&self) -> RequestVoteResp {
        RequestVoteResp { term: self.term(), vote_granted: self.vote_granted() }
    }
}

// Satisfy unused import
#[allow(unused_imports)]
use RaftError as _;
