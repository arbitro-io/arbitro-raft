use std::net::SocketAddr;

use crate::{ClusterId, LimitsConfig, PeerId, TimingConfig};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BootstrapPeer {
    pub id: PeerId,
    pub addr: SocketAddr,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NodeConfig {
    pub cluster_id: ClusterId,
    pub node_id: PeerId,
    /// Current voter set known at startup. This is the authority for quorum math in v1.
    pub peers: Vec<PeerId>,
    /// Learner (non-voting member) set known at startup (A13).
    ///
    /// Learners receive log replication (AppendEntries, snapshot catch-up)
    /// exactly like followers but are excluded from EVERY quorum: commit-index
    /// advancement, election vote counts, check-quorum contact, and ReadIndex
    /// confirmation. A learner never campaigns. Must be disjoint from `peers`;
    /// a node whose own id appears here (and not in `peers`) boots as a
    /// learner. Runtime learner changes (`add_learner` / `remove_learner` /
    /// `promote_learner`) mutate this set via replicated control entries, the
    /// same way `peers` diverges from its startup value via config changes.
    pub learners: Vec<PeerId>,
    /// Static seed list used only for initial discovery/bootstrap.
    /// Runtime membership is allowed to diverge later.
    pub bootstrap_peers: Vec<BootstrapPeer>,
    pub timing: TimingConfig,
    pub limits: LimitsConfig,
}
