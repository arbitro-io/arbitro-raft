use std::net::SocketAddr;

use crate::{ClusterId, LimitsConfig, PeerId, TimingConfig};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BootstrapPeer {
    pub id: PeerId,
    pub addr: SocketAddr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
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
impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            cluster_id: ClusterId::default(),
            node_id: PeerId::default(),
            peers: Vec::new(),
            bootstrap_peers: Vec::new(),
            timing: TimingConfig::default(),
            limits: LimitsConfig::default(),
        }
    }
}
