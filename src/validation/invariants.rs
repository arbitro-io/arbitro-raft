use crate::{NodeConfig, RaftError};
use std::collections::HashSet;

pub fn validate_node_config(cfg: &NodeConfig) -> Result<(), RaftError> {
    if cfg.peers.is_empty() {
        return Err(RaftError::InvalidConfig("peers must not be empty"));
    }
    if !cfg.peers.contains(&cfg.node_id) {
        return Err(RaftError::InvalidConfig("peers must include node_id"));
    }
    if cfg.bootstrap_peers.is_empty() {
        return Err(RaftError::InvalidConfig("bootstrap_peers must not be empty"));
    }
    if !cfg.bootstrap_peers.iter().any(|peer| peer.id == cfg.node_id) {
        return Err(RaftError::InvalidConfig("bootstrap_peers must include node_id for v1 bootstrap"));
    }

    // Duplicate peers break quorum math silently
    let mut seen = HashSet::with_capacity(cfg.peers.len());
    for peer in &cfg.peers {
        if !seen.insert(peer) {
            return Err(RaftError::InvalidConfig("peers must not contain duplicates"));
        }
    }

    if cfg.timing.heartbeat_ms == 0 {
        return Err(RaftError::InvalidConfig("heartbeat_ms must be > 0"));
    }
    if cfg.timing.election_min_ms >= cfg.timing.election_max_ms {
        return Err(RaftError::InvalidConfig("election_min_ms must be < election_max_ms"));
    }
    // Heartbeat must fire well before any election timeout to prevent unnecessary elections
    if cfg.timing.heartbeat_ms >= cfg.timing.election_min_ms {
        return Err(RaftError::InvalidConfig(
            "heartbeat_ms must be < election_min_ms (recommended: heartbeat_ms * 3 <= election_min_ms)"
        ));
    }

    if cfg.limits.snapshot_chunk_bytes == 0 {
        return Err(RaftError::InvalidConfig("snapshot_chunk_bytes must be > 0"));
    }

    Ok(())
}
