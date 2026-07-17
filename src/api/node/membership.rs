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

/// Discriminator byte for the leader no-op control entry (A11 / ReadIndex).
///
/// Lives in byte `[2]` of the reserved `0xC0` control envelope, in the same
/// slot as the config-change phase but far from its values (`1` = Joint,
/// `2` = Final) so the two control families can never be confused.
/// [`ConfigChangeEntry::decode`] returns `None` for it (unknown phase), and
/// [`apply_if_config_change`] consumes it explicitly — a no-op entry never
/// reaches the user state machine. Safe to introduce because user payloads
/// beginning with `0xC0` have always been rejected at propose time, so no
/// existing log can contain these bytes as application data.
pub(crate) const NOOP_DISCRIMINANT: u8 = 0x7F;

/// The full wire payload of a leader no-op entry: 4 bytes,
/// `[0xC0, version, 0x7F, 0]`.
#[inline]
pub(crate) fn noop_entry() -> [u8; 4] {
    [CONFIG_CHANGE_MAGIC, CONFIG_CHANGE_VERSION, NOOP_DISCRIMINANT, 0]
}

/// Whether `payload` is exactly a leader no-op control entry.
#[inline]
pub(crate) fn is_noop_entry(payload: &[u8]) -> bool {
    payload == noop_entry()
}

/// Header size in bytes: magic + version + phase + pad + old_len + new_len.
const HEADER_LEN: usize = 12;

/// Phase of a joint-consensus transition, or a learner-set change (A13).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigChangePhase {
    /// Joint configuration `C_old_new` — both old and new voter sets are
    /// active simultaneously.
    Joint,
    /// Final configuration `C_new` — only the new voter set is active.
    Final,
    /// A13: add the peers in `new_peers` as LEARNERS (non-voting members).
    /// Does NOT touch the voter set and therefore changes no quorum — see
    /// the single-entry rationale on [`ConfigChangeEntry`].
    AddLearner,
    /// A13: remove the peers in `new_peers` from the learner set. Does NOT
    /// touch the voter set and therefore changes no quorum.
    RemoveLearner,
}

/// A membership-change control entry.
///
/// # Wire layout
///
/// | Bytes           | Field       | Notes                                  |
/// |-----------------|-------------|----------------------------------------|
/// | `[0]`           | `magic`     | Always [`CONFIG_CHANGE_MAGIC`] (0xC0). |
/// | `[1]`           | `version`   | Always [`CONFIG_CHANGE_VERSION`] (1).  |
/// | `[2]`           | `phase`     | `1` = Joint, `2` = Final, `3` = AddLearner, `4` = RemoveLearner. |
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
///
/// # Learner entries (A13)
///
/// `AddLearner` / `RemoveLearner` reuse this envelope with the affected
/// peer ids carried in `new_peers` (`old_peers` is empty and ignored on
/// read). Unlike voter transitions they are SINGLE control entries, not a
/// joint pair: a learner is excluded from every quorum, so adding or
/// removing one changes no majority on either side of §4.3's safety
/// argument — there is no "two disjoint majorities" hazard to bridge, and
/// the joint machinery would add cost without adding safety. Promotion of
/// a learner to voter IS a quorum change and goes through the normal
/// Joint→Final voter transition (`promote_learner`).
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
            ConfigChangePhase::AddLearner => 3,
            ConfigChangePhase::RemoveLearner => 4,
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
            3 => ConfigChangePhase::AddLearner,
            4 => ConfigChangePhase::RemoveLearner,
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
                // A13: a peer entering the voter set stops being a learner
                // (promotion) — the sets stay disjoint at all times.
                let peers = &self.config.peers;
                self.config.learners.retain(|p| !peers.contains(p));
            }
            ConfigChangePhase::Final => {
                if entry.new_peers.is_empty() {
                    return Err(RaftError::InvalidConfig(
                        "config-change Final phase with empty new_peers",
                    ));
                }
                self.config.peers = entry.new_peers.clone();
                self.joint_peers = None;
                // A13: keep voter/learner disjointness after the final set
                // lands (a promoted learner is now a plain voter).
                let peers = &self.config.peers;
                self.config.learners.retain(|p| !peers.contains(p));
            }
            // A13: learner-set changes. Single-entry (no joint pair) because
            // no quorum changes — see the type-level rationale. Idempotent on
            // re-apply: the apply loop re-applies committed entries after a
            // restart (`last_applied` is volatile), and followers also adopt
            // these at append time (§4.1 append-time rule, same as voter
            // config entries).
            ConfigChangePhase::AddLearner => {
                for p in &entry.new_peers {
                    if !self.config.peers.contains(p) && !self.config.learners.contains(p) {
                        self.config.learners.push(*p);
                    }
                }
            }
            ConfigChangePhase::RemoveLearner => {
                let removed = &entry.new_peers;
                self.config.learners.retain(|p| !removed.contains(p));
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

    /// Undo a leader-side append-time joint activation whose Joint entry
    /// never reached the log (the propose failed BEFORE appending — e.g.
    /// a leadership-transfer freeze or a step-down raced the call). The
    /// effective voter set reverts to `old_peers` and the dual-quorum
    /// rule is disarmed. Callers must NOT invoke this when the Joint
    /// entry IS in the log: an appended-but-uncommitted config entry
    /// stays active (Raft §4.1 append-time rule), exactly as on a
    /// follower.
    pub(crate) fn revert_joint_activation(&mut self, old_peers: Vec<PeerId>) {
        self.config.peers = old_peers;
        self.joint_peers = None;
        if self.is_leader() {
            self.reconcile_leader_progress_with_config();
        }
    }

    /// A13: undo a leader-side append-time learner-set activation whose
    /// control entry never reached the log (the propose failed BEFORE
    /// appending). Mirrors [`revert_joint_activation`]: an appended-but-
    /// uncommitted learner entry stays active (§4.1 append-time rule).
    ///
    /// [`revert_joint_activation`]: RaftNode::revert_joint_activation
    pub(crate) fn revert_learner_activation(&mut self, prev_learners: Vec<PeerId>) {
        self.config.learners = prev_learners;
        if self.is_leader() {
            self.reconcile_leader_progress_with_config();
        }
    }

    /// Bring `peer_progress` into agreement with the effective membership
    /// (`config.peers` voters + `config.learners` non-voting members, A13):
    /// insert a fresh entry for every member that has none, and drop entries
    /// for peers no longer in either set. Retained peers keep their in-flight
    /// progress so replication does not restart from scratch. Learners get a
    /// progress entry because the leader replicates to them exactly like
    /// followers — the quorum math simply never reads their `match_index`.
    fn reconcile_leader_progress_with_config(&mut self) {
        let last_index = self.cached_last_log.0;
        let self_id = self.config.node_id;

        // Drop entries for peers in neither the voter nor the learner set.
        let stale: Vec<PeerId> = self
            .peer_progress
            .iter()
            .map(|(peer, _)| peer)
            .filter(|peer| {
                !self.config.peers.contains(peer) && !self.config.learners.contains(peer)
            })
            .collect();
        for peer in stale {
            self.peer_progress.remove(&peer);
            self.pending_snapshots.remove(&peer);
            self.last_voter_contact.remove(&peer);
        }

        // Insert fresh progress for any member missing one (leader excluded).
        for &peer in self
            .config
            .peers
            .iter()
            .chain(self.config.learners.iter())
        {
            if peer == self_id {
                continue;
            }
            if !self.peer_progress.contains_key(&peer) {
                self.peer_progress.insert(
                    peer,
                    PeerProgress {
                        // B8 arithmetic policy: saturating at the boundary.
                        next_index: LogIndex(last_index.0.saturating_add(1)),
                        match_index: LogIndex(0),
                    },
                );
            }
        }
    }
}

/// Apply the entry to `node` if — and only if — `payload` is a
/// well-formed control entry (config change or leader no-op). Returns
/// `Ok(true)` when the payload was a control entry (and has been
/// consumed), `Ok(false)` when it was an ordinary application payload.
///
/// The `ArbitroRaft` apply loop calls this unconditionally for every
/// committed entry immediately BEFORE forwarding the payload to the
/// user state machine — application payloads short-circuit through the
/// `false` branch, and control payloads never reach user code. The A11
/// leader no-op (see [`noop_entry`]) is consumed with no state change:
/// its sole purpose is to commit an entry of the leader's current term
/// so ReadIndex's §6.4 guard holds.
pub fn apply_if_config_change<S, T>(
    node: &mut RaftNode<S, T>,
    payload: &[u8],
) -> Result<bool, RaftError>
where
    S: RaftStorage,
    T: RaftTransport,
{
    if is_noop_entry(payload) {
        return Ok(true);
    }
    if let Some(entry) = ConfigChangeEntry::decode(payload) {
        node.apply_config_change(&entry)?;
        Ok(true)
    } else {
        Ok(false)
    }
}
