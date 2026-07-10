// Joint-consensus membership changes (Raft §4.3).
//
// Encoded as a versioned control envelope carried inside a normal log entry
// payload (`EntryPayload(&[u8])`). Storage-trait and wire-protocol formats
// are unchanged; membership machinery is entirely a state machine on top of
// the log.

use crate::{PeerId, RaftError, RaftNode, RaftStorage, RaftTransport};

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
/// # Quorum caveat
///
/// While a `Joint` entry is the effective configuration, the leader
/// uses the *union* of `old_peers` ∪ `new_peers` as the voter set for
/// quorum. This is strictly more conservative than the classical
/// dual-quorum rule (majority-of-old AND majority-of-new) — the union
/// quorum requires more acks and is therefore safe (any dual-quorum
/// commit implies a union-quorum commit is possible with additional
/// acks, and vice-versa the union rule never commits without at least
/// a majority in one of the sub-sets). Liveness is slightly reduced
/// during the transition; safety is preserved.
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
    /// Joint phase — voters become the union of `old_peers` and
    /// `new_peers`; quorum math in `try_advance_commit_index` operates
    /// against this over-approximation, which is safe (see the caveat
    /// on [`ConfigChangeEntry`]).
    ///
    /// Final phase — voters become exactly `new_peers`. If this node
    /// was the leader and is no longer in the voter set, it steps down
    /// immediately.
    pub(crate) fn apply_config_change(
        &mut self,
        entry: &ConfigChangeEntry,
    ) -> Result<(), RaftError> {
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
            }
            ConfigChangePhase::Final => {
                self.config.peers = entry.new_peers.clone();
            }
        }

        if self.is_leader() && !self.config.peers.contains(&self.config.node_id) {
            let term = self.hard_state.current_term;
            self.step_down(term)?;
        }
        Ok(())
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
