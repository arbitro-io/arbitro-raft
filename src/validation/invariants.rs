use crate::{NodeConfig, RaftError};

pub fn validate_node_config(cfg: &NodeConfig) -> Result<(), RaftError> {
    if cfg.peers.is_empty() {
        return Err(RaftError::InvalidConfig("peers must not be empty"));
    }
    if !cfg.peers.contains(&cfg.node_id) {
        return Err(RaftError::InvalidConfig("peers must include node_id"));
    }
    if cfg.bootstrap_peers.is_empty() {
        return Err(RaftError::InvalidConfig(
            "bootstrap_peers must not be empty",
        ));
    }
    if !cfg
        .bootstrap_peers
        .iter()
        .any(|peer| peer.id == cfg.node_id)
    {
        return Err(RaftError::InvalidConfig(
            "bootstrap_peers must include node_id for v1 bootstrap",
        ));
    }
    if cfg.timing.election_min_ms >= cfg.timing.election_max_ms {
        return Err(RaftError::InvalidConfig(
            "election_min_ms must be < election_max_ms",
        ));
    }
    Ok(())
}
