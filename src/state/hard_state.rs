use serde::{Deserialize, Serialize};

use crate::{LogIndex, PeerId, Term};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HardState {
    pub current_term: Term,
    pub voted_for: Option<PeerId>,
    pub commit_index: LogIndex,
}

impl Default for HardState {
    fn default() -> Self {
        Self {
            current_term: Term(0),
            voted_for: None,
            commit_index: LogIndex(0),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SoftState {
    pub leader_id: Option<PeerId>,
    pub is_leader: bool,
    pub role: Role,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotMeta {
    pub last_included_index: LogIndex,
    pub last_included_term: Term,
}
