// Joint-consensus membership changes (Raft §4.3).
//
// Encoded as a versioned control envelope carried inside a normal log entry
// payload (`EntryPayload(&[u8])`). Storage-trait and wire-protocol formats
// are unchanged; membership machinery is entirely a state machine on top of
// the log.

use crate::{LogIndex, PeerId, RaftError, RaftNode, RaftStorage, RaftTransport};

use super::progress::PeerProgress;

/// First byte of every config-change entry payload.
///
/// The value `0xC0` is reserved: application entries MUST NOT begin with
/// this byte. See [`ConfigChangeEntry`] for the full byte layout.
pub const CONFIG_CHANGE_MAGIC: u8 = 0xC0;

/// Current wire version for [`ConfigChangeEntry`].
pub const CONFIG_CHANGE_VERSION: u8 = 1;

/// Header size in bytes: magic + version + phase + pad + old_len + new_len.
const HEADER_LEN: usize = 12;

/// Phase of a joint-consensus transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigChangePhase {
    /// Joint configuration `C_old_new` — both old and new voter sets are
    /// active simultaneously.
    Joint,
    /// Final configuration `C_new` — only the new voter set is active.
    Final,
}

/// A membership-change control entry.
///
/// # Wire layout
///
/// | Bytes           | Field       | Notes                                  |
/// |-----------------|-------------|----------------------------------------|
/// | `[0]`           | `magic`     | Always [`CONFIG_CHANGE_MAGIC`] (0xC0). |
/// | `[1]`           | `version`   | Always [`CONFIG_CHANGE_VERSION`] (1).  |
/// | `[2]`           | `phase`     | `1` = Joint, `2` = Final.              |
/// | `[3]`           | `_pad`      | Reserved, must be `0`.                 |
/// | `[4..8]`        | `old_len`   | `u32` little-endian.                   |
/// | `[8..12]`       | `new_len`   | `u32` little-endian.                   |
/// | `[12..]`        | `old_peers` | `old_len` × `u64` little-endian.       |
/// | (after)         | `new_peers` | `new_len` × `u64` little-endian.       |
///
/// The reserved leading byte `0xC0` guarantees these entries are
/// unambiguously distinguishable from any application payload.
///
/// # Quorum rule
///
/// While a `Joint` entry is the effective configuration, commit requires
/// a majority of `old_peers` AND a majority of `new_peers` independently
/// (Raft §4.3, dual-quorum). A single-set majority of the union is NOT
/// sufficient — that admits commits agreed to entirely inside `new_peers`
/// with zero acks from `old_peers`, which breaks the safety hand-off
/// between the two configurations.
///
/// `config.peers` still holds the union of the two sets for the purpose
/// of replication targets and broadcast fan-out; the dual-quorum rule
/// gates commit progression on top of that broadcast set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigChangeEntry {
    pub phase: ConfigChangePhase,
    pub old_peers: Vec<PeerId>,
    pub new_peers: Vec<PeerId>,
}

impl ConfigChangeEntry {
    /// Encode this entry to the wire layout documented on the type.
    pub fn encode(&self) -> Vec<u8> {
        let old_len = self.old_peers.len();
        let new_len = self.new_peers.len();
        let mut out = Vec::with_capacity(HEADER_LEN + (old_len + new_len) * 8);

        out.push(CONFIG_CHANGE_MAGIC);
        out.push(CONFIG_CHANGE_VERSION);
        out.push(match self.phase {
            ConfigChangePhase::Joint => 1,
            ConfigChangePhase::Final => 2,
        });
        out.push(0u8); // _pad
        out.extend_from_slice(&(old_len as u32).to_le_bytes());
        out.extend_from_slice(&(new_len as u32).to_le_bytes());
        for p in &self.old_peers {
            out.extend_from_slice(&p.0.to_le_bytes());
        }
        for p in &self.new_peers {
            out.extend_from_slice(&p.0.to_le_bytes());
        }
        out
    }

    /// Decode a payload. Returns `None` if the bytes do not look like a
    /// config-change entry (wrong magic, wrong version, or malformed
    /// length) — callers should treat that as an application payload.
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < HEADER_LEN {
            return None;
        }
        if bytes[0] != CONFIG_CHANGE_MAGIC {
            return None;
        }
        if bytes[1] != CONFIG_CHANGE_VERSION {
            return None;
        }
        let phase = match bytes[2] {
            1 => ConfigChangePhase::Joint,
            2 => ConfigChangePhase::Final,
            _ => return None,
        };
        // bytes[3] is reserved padding — ignore its value on read.

        let old_len = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]) as usize;
        let new_len = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]) as usize;

        let old_bytes = old_len.checked_mul(8)?;
        let new_bytes = new_len.checked_mul(8)?;
        let total = HEADER_LEN.checked_add(old_bytes)?.checked_add(new_bytes)?;
        if bytes.len() < total {
            return None;
        }

        let mut old_peers = Vec::with_capacity(old_len);
        let mut cursor = HEADER_LEN;
        for _ in 0..old_len {
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&bytes[cursor..cursor + 8]);
            old_peers.push(PeerId(u64::from_le_bytes(buf)));
            cursor += 8;
        }

        let mut new_peers = Vec::with_capacity(new_len);
        for _ in 0..new_len {
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&bytes[cursor..cursor + 8]);
            new_peers.push(PeerId(u64::from_le_bytes(buf)));
            cursor += 8;
        }

        Some(Self {
            phase,
            old_peers,
            new_peers,
        })
    }
}

impl<S, T> RaftNode<S, T>
where
    S: RaftStorage,
    T: RaftTransport,
{
    /// Apply a committed config-change entry to this node's effective
    /// voter set.
    ///
    /// Joint phase — `config.peers` becomes the union of `old_peers` and
    /// `new_peers` (used for replication fan-out) and `joint_peers` is
    /// set to `Some((old, new))` so [`try_advance_commit_index`] enforces
    /// the dual-quorum rule (majority-of-old AND majority-of-new).
    ///
    /// Final phase — `joint_peers` is cleared and `config.peers` becomes
    /// exactly `new_peers`. If this node was the leader and is no longer
    /// in the voter set, it steps down immediately.
    ///
    /// If this node is the leader, `peer_progress` is reconciled with the
    /// new voter set: newly added voters get a fresh `PeerProgress`
    /// anchored at `last_log_index + 1`, and peers that are no longer
    /// voters are dropped. Existing progress for retained peers is left
    /// intact so in-flight replication does not restart.
    ///
    /// [`try_advance_commit_index`]: super::RaftNode::try_advance_commit_index
    pub(crate) fn apply_config_change(
        &mut self,
        entry: &ConfigChangeEntry,
    ) -> Result<(), RaftError> {
        self.metrics.inc_config_changes_applied();
        match entry.phase {
            ConfigChangePhase::Joint => {
                let mut merged: Vec<PeerId> = Vec::with_capacity(
                    entry.old_peers.len() + entry.new_peers.len(),
                );
                merged.extend_from_slice(&entry.old_peers);
                for p in &entry.new_peers {
                    if !merged.contains(p) {
                        merged.push(*p);
                    }
                }
                self.config.peers = merged;
                self.joint_peers = Some((entry.old_peers.clone(), entry.new_peers.clone()));
            }
            ConfigChangePhase::Final => {
                if entry.new_peers.is_empty() {
                    return Err(RaftError::InvalidConfig(
                        "config-change Final phase with empty new_peers",
                    ));
                }
                self.config.peers = entry.new_peers.clone();
                self.joint_peers = None;
            }
        }

        if self.is_leader() {
            self.reconcile_leader_progress_with_config();
        }

        if self.is_leader() && !self.config.peers.contains(&self.config.node_id) {
            let term = self.hard_state.current_term;
            self.step_down(term)?;
        }
        Ok(())
    }

    /// Bring `peer_progress` into agreement with `config.peers`: insert a
    /// fresh entry for every voter that has none, and drop entries for
    /// peers no longer in the voter set. Retained peers keep their
    /// in-flight progress so replication does not restart from scratch.
    fn reconcile_leader_progress_with_config(&mut self) {
        let last_index = self.cached_last_log.0;
        let self_id = self.config.node_id;

        // Drop entries for peers no longer in config.peers.
        let stale: Vec<PeerId> = self
            .peer_progress
            .iter()
            .map(|(peer, _)| peer)
            .filter(|peer| !self.config.peers.contains(peer))
            .collect();
        for peer in stale {
            self.peer_progress.remove(&peer);
            self.pending_snapshots.remove(&peer);
            self.scratch_started.remove(&peer);
        }

        // Insert fresh progress for any voter missing one (leader excluded).
        for &peer in &self.config.peers {
            if peer == self_id {
                continue;
            }
            if !self.peer_progress.contains_key(&peer) {
                self.peer_progress.insert(
                    peer,
                    PeerProgress {
                        next_index: LogIndex(last_index.0 + 1),
                        match_index: LogIndex(0),
                    },
                );
            }
        }
    }
}

/// Apply the entry to `node` if — and only if — `payload` is a
/// well-formed config-change entry. Returns `Ok(true)` when the payload
/// was a config-change entry (and has been applied), `Ok(false)` when
/// it was an ordinary application payload.
///
/// The `ArbitroRaft` apply loop calls this unconditionally for every
/// committed entry immediately BEFORE forwarding the payload to the
/// user state machine — application payloads short-circuit through the
/// `false` branch, and control payloads never reach user code.
pub fn apply_if_config_change<S, T>(
    node: &mut RaftNode<S, T>,
    payload: &[u8],
) -> Result<bool, RaftError>
where
    S: RaftStorage,
    T: RaftTransport,
{
    if let Some(entry) = ConfigChangeEntry::decode(payload) {
        node.apply_config_change(&entry)?;
        Ok(true)
    } else {
        Ok(false)
    }
}
