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

impl std::error::Error for RaftError {}

impl From<std::io::Error> for RaftError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}
