use crate::dispatch::DispatchOptions;
use crate::{PeerId, RaftError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DispatchFailure {
    Timeout,
    /// The dispatching node lost leadership (stepped down) while the
    /// transaction was still in flight. In-flight custom dispatches are
    /// leader-epoch-scoped: on step-down every pending waiter is resolved
    /// with this deterministic failure instead of parking forever
    /// (A5 / C7 / ERR-7). Surfaces to `wait()` as [`RaftError::NotLeader`].
    LostLeadership,
    Impossible(String),
    Failed(String),
}

impl From<DispatchFailure> for RaftError {
    fn from(value: DispatchFailure) -> Self {
        match value {
            DispatchFailure::Timeout => RaftError::Dispatch("dispatch timed out".into()),
            DispatchFailure::LostLeadership => RaftError::NotLeader { leader_hint: None },
            DispatchFailure::Impossible(msg) | DispatchFailure::Failed(msg) => {
                RaftError::Dispatch(msg)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum DispatchPeerState<R> {
    Pending,
    Progress(Vec<u8>),
    Accepted(R),
    Rejected(Vec<u8>),
    Failed(Vec<u8>),
    Disconnected,
}

#[derive(Debug, Clone)]
pub struct DispatchPeerResult<R> {
    pub peer: PeerId,
    pub state: DispatchPeerState<R>,
}

#[derive(Debug, Clone)]
pub struct DispatchResult<R> {
    pub tx_id: u64,
    pub command: u8,
    pub options: DispatchOptions,
    pub peers: Vec<DispatchPeerResult<R>>,
}

impl<R> DispatchResult<R> {
    pub fn accepted_count(&self) -> usize {
        self.peers
            .iter()
            .filter(|p| matches!(p.state, DispatchPeerState::Accepted(_)))
            .count()
    }

    pub fn failed_count(&self) -> usize {
        self.peers
            .iter()
            .filter(|p| {
                matches!(
                    p.state,
                    DispatchPeerState::Rejected(_) | DispatchPeerState::Failed(_)
                )
            })
            .count()
    }

    pub fn disconnected_count(&self) -> usize {
        self.peers
            .iter()
            .filter(|p| matches!(p.state, DispatchPeerState::Disconnected))
            .count()
    }
}
