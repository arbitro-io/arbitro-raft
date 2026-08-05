use crate::{NodeConfig, RaftError};
use std::collections::HashSet;

pub fn validate_node_config(cfg: &NodeConfig) -> Result<(), RaftError> {
    if cfg.peers.is_empty() {
        return Err(RaftError::InvalidConfig("peers must not be empty"));
    }
    // A13: a node boots either as a VOTER (its id is in `peers`) or as a
    // LEARNER (its id is in `learners`). A node in neither set is a
    // misconfiguration — it could never participate in the cluster.
    if !cfg.peers.contains(&cfg.node_id) && !cfg.learners.contains(&cfg.node_id) {
        return Err(RaftError::InvalidConfig(
            "node_id must appear in peers (voter) or learners (non-voting member)",
        ));
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

    // Duplicate peers break quorum math silently
    let mut seen = HashSet::with_capacity(cfg.peers.len());
    for peer in &cfg.peers {
        if !seen.insert(peer) {
            return Err(RaftError::InvalidConfig(
                "peers must not contain duplicates",
            ));
        }
    }

    // A13: learners must be duplicate-free and disjoint from the voter set —
    // a peer present in both would be fanned out to twice and, worse, could
    // blur the voter/learner boundary the quorum math depends on.
    let mut seen_learners = HashSet::with_capacity(cfg.learners.len());
    for learner in &cfg.learners {
        if !seen_learners.insert(learner) {
            return Err(RaftError::InvalidConfig(
                "learners must not contain duplicates",
            ));
        }
        if cfg.peers.contains(learner) {
            return Err(RaftError::InvalidConfig(
                "learners must be disjoint from peers",
            ));
        }
    }

    if cfg.timing.heartbeat_ms == 0 {
        return Err(RaftError::InvalidConfig("heartbeat_ms must be > 0"));
    }
    if cfg.timing.election_min_ms >= cfg.timing.election_max_ms {
        return Err(RaftError::InvalidConfig(
            "election_min_ms must be < election_max_ms",
        ));
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
