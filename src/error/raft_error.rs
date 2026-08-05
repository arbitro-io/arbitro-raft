use std::fmt::{Display, Formatter};

use crate::{LeaderHint, PeerId, Term};

#[derive(Debug)]
pub enum RaftError {
    NotLeader {
        leader_hint: Option<LeaderHint>,
    },
    NoQuorum,
    /// Leadership transfer (§4.2.3) aborted: the target could not catch up to
    /// the leader's last log index within one election timeout. The leader has
    /// RESUMED normal operation — no step-down happened, proposals are
    /// accepted again — so the caller may simply retry the transfer later.
    TransferTimeout(PeerId),
    TermChanged {
        current: Term,
    },
    /// The node is overloaded: the bounded client-proposal mailbox (or the
    /// commit-notification slot pool) is full — the run loop is not draining
    /// proposals as fast as clients submit them (H5). Nothing was enqueued
    /// and nothing was acknowledged; this is a pure backpressure signal.
    /// Retryable: classifies [`ErrorClass::Transient`] — back off briefly and
    /// resubmit.
    Overloaded,
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
            Self::TransferTimeout(target) => write!(
                f,
                "leadership transfer to peer {} timed out; leadership retained",
                target.0
            ),
            Self::TermChanged { current } => write!(f, "term changed: {}", current.0),
            Self::Overloaded => write!(
                f,
                "overloaded: client proposal mailbox full; back off and retry"
            ),
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
    /// Local resources are exhausted (disk full / quota / out-of-memory) —
    /// the C8 degradation class, the minimal slice of the E5 taxonomy.
    /// Durable state is INTACT: the failed write simply did not happen and
    /// was never acknowledged, so nothing diverged. The run loop degrades to
    /// read-only survival instead of halting: a leader steps down (soft, no
    /// hard-state write required) and rejects new proposals; a follower stops
    /// acking; both resume normal operation once a subsequent storage
    /// operation succeeds (space recovered). Contrast with [`Fatal`]: a
    /// cluster-wide disk-full must not become cluster-wide node death while
    /// every byte of data is intact.
    ///
    /// A storage implementation signals this class by returning
    /// [`RaftError::Io`] with an [`std::io::ErrorKind`] of `StorageFull`,
    /// `QuotaExceeded`, or `OutOfMemory` (ENOSPC/EDQUOT map to these
    /// automatically via `std::io::Error::from_raw_os_error`). A
    /// `RaftError::Storage(String)` NEVER classifies as `Resource` — an
    /// implementation that stringifies its IO errors opts out of graceful
    /// degradation and keeps today's fail-fast behavior.
    ///
    /// [`Fatal`]: ErrorClass::Fatal
    Resource,
    /// This node's own durable state is broken (storage/log corruption). The
    /// run loop MUST halt rather than risk a safety violation.
    Fatal,
}

/// Whether an [`std::io::ErrorKind`] denotes resource exhaustion (C8) —
/// out of disk space, quota, or memory — as opposed to data loss/corruption.
/// Deliberately conservative: only kinds that unambiguously mean "the write
/// was refused for lack of resources, nothing durable was damaged" qualify;
/// everything else stays [`ErrorClass::Fatal`] (never mask corruption).
#[inline]
fn io_kind_is_resource_exhaustion(kind: std::io::ErrorKind) -> bool {
    matches!(
        kind,
        std::io::ErrorKind::StorageFull
            | std::io::ErrorKind::QuotaExceeded
            | std::io::ErrorKind::OutOfMemory
    )
}

impl RaftError {
    /// Classify this error for run-loop and caller decision-making.
    pub fn class(&self) -> ErrorClass {
        match self {
            // C8: resource exhaustion (ENOSPC-class) — durable state intact,
            // write refused, nothing acknowledged. Degrade, don't die.
            Self::Io(err) if io_kind_is_resource_exhaustion(err.kind()) => ErrorClass::Resource,
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
            // Peer/network hiccup or a benign control-flow signal. A transfer
            // timeout is transient by design: the leader resumed normal
            // operation and the transfer may simply be retried. Overload (H5)
            // is transient by construction: the mailbox drains every tick, so
            // backing off briefly and retrying is the correct client response.
            Self::NoQuorum
            | Self::TermChanged { .. }
            | Self::Transport(_)
            | Self::Overloaded
            | Self::TransferTimeout(_) => ErrorClass::Transient,
        }
    }

    /// Whether this error must terminate the consensus run loop. Only
    /// [`ErrorClass::Fatal`] errors qualify; all others are logged and
    /// tolerated so one bad frame cannot kill the node. Note that
    /// [`ErrorClass::Resource`] (disk-full class, C8) is intentionally NOT
    /// fatal: the run loop degrades to read-only survival instead.
    #[inline]
    pub fn is_fatal(&self) -> bool {
        matches!(self.class(), ErrorClass::Fatal)
    }

    /// Whether this error is resource exhaustion (disk full / quota / OOM) —
    /// [`ErrorClass::Resource`], the C8 graceful-degradation class.
    #[inline]
    pub fn is_resource_exhaustion(&self) -> bool {
        matches!(self.class(), ErrorClass::Resource)
    }
}

impl std::error::Error for RaftError {}

impl From<std::io::Error> for RaftError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}
