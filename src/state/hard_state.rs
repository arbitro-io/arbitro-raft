

use crate::{LogIndex, PeerId, Term};

/// Durable state that MUST be persisted before any message is sent.
///
/// Only `current_term` and `voted_for` are true hard state per Raft §5.
/// `commit_index` is volatile — do NOT add it here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HardState {
    pub current_term: Term,
    pub voted_for: Option<PeerId>,
}

impl Default for HardState {
    fn default() -> Self {
        Self {
            current_term: Term(0),
            voted_for: None,
        }
    }
}

/// Volatile state reconstructed in memory on every restart.
///
/// `commit_index` starts at 0 on every restart and is advanced by the leader
/// via AppendEntries. Persisting it would violate linearizability when the log
/// is truncated across crashes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SoftState {
    pub leader_id: Option<PeerId>,
    pub is_leader: bool,
    pub role: Role,
    /// Highest log index known to be committed. Volatile — always 0 on restart.
    pub commit_index: LogIndex,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotMeta {
    pub last_included_index: LogIndex,
    pub last_included_term: Term,
}
