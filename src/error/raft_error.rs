use std::fmt::{Display, Formatter};

use crate::{LeaderHint, PeerId, Term};

#[derive(Debug)]
pub enum RaftError {
    NotLeader { leader_hint: Option<LeaderHint> },
    NoQuorum,
    TermChanged { current: Term },
    PeerUnknown(PeerId),
    InvalidConfig(&'static str),
    InvalidPayload(&'static str),
    Protocol(String),
    Dispatch(String),
    Storage(String),
    Transport(String),
    CorruptLog(String),
    Snapshot(String),
    Io(std::io::Error),
}

impl Display for RaftError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotLeader { leader_hint } => write!(f, "not leader: {leader_hint:?}"),
            Self::NoQuorum => write!(f, "no quorum"),
            Self::TermChanged { current } => write!(f, "term changed: {}", current.0),
            Self::PeerUnknown(peer) => write!(f, "unknown peer {}", peer.0),
            Self::InvalidConfig(msg) => write!(f, "invalid config: {msg}"),
            Self::InvalidPayload(msg) => write!(f, "invalid payload: {msg}"),
            Self::Protocol(msg) => write!(f, "protocol error: {msg}"),
            Self::Dispatch(msg) => write!(f, "dispatch error: {msg}"),
            Self::Storage(msg) => write!(f, "storage error: {msg}"),
            Self::Transport(msg) => write!(f, "transport error: {msg}"),
            Self::CorruptLog(msg) => write!(f, "corrupt log: {msg}"),
            Self::Snapshot(msg) => write!(f, "snapshot error: {msg}"),
            Self::Io(err) => Display::fmt(err, f),
        }
    }
}

/// Severity class of a [`RaftError`] — lets the run loop and callers react
/// correctly instead of treating every error identically (which is how a
/// single bad peer frame used to terminate the whole consensus loop).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorClass {
    /// A transient peer/network condition — safe to drop the frame or retry.
    Transient,
    /// This node is not the leader; the caller should redirect to the hint.
    NotLeaderRedirect,
    /// A malformed or unexpected inbound frame — drop it, keep serving.
    BadFrame,
    /// This node's own durable state is broken (storage/log corruption). The
    /// run loop MUST halt rather than risk a safety violation.
    Fatal,
}

impl RaftError {
    /// Classify this error for run-loop and caller decision-making.
    pub fn class(&self) -> ErrorClass {
        match self {
            // Local durable state is broken — halting is the safe response.
            Self::Storage(_) | Self::CorruptLog(_) | Self::Io(_) => ErrorClass::Fatal,
            // Redirect the client to the current leader.
            Self::NotLeader { .. } => ErrorClass::NotLeaderRedirect,
            // The inbound frame / dispatch command / snapshot is bad or from a
            // version-skewed peer — drop it, never die on one peer's input.
            Self::Protocol(_)
            | Self::Dispatch(_)
            | Self::Snapshot(_)
            | Self::InvalidPayload(_)
            | Self::InvalidConfig(_)
            | Self::PeerUnknown(_) => ErrorClass::BadFrame,
            // Peer/network hiccup or a benign control-flow signal.
            Self::NoQuorum | Self::TermChanged { .. } | Self::Transport(_) => {
                ErrorClass::Transient
            }
        }
    }

    /// Whether this error must terminate the consensus run loop. Only
    /// [`ErrorClass::Fatal`] errors qualify; all others are logged and
    /// tolerated so one bad frame cannot kill the node.
    #[inline]
    pub fn is_fatal(&self) -> bool {
        matches!(self.class(), ErrorClass::Fatal)
    }
}

impl std::error::Error for RaftError {}

impl From<std::io::Error> for RaftError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}
