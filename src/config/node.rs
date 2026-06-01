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
    /// Static seed list used only for initial discovery/bootstrap.
    /// Runtime membership is allowed to diverge later.
    pub bootstrap_peers: Vec<BootstrapPeer>,
    pub timing: TimingConfig,
    pub limits: LimitsConfig,
}
