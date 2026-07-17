use std::time::{Duration, Instant};

use crate::{LogIndex, PeerId, SnapshotMeta};

// ── PeerMap ───────────────────────────────────────────────────────────────────

/// Compact linear-scan map for small peer sets (≤ 16 nodes).
///
/// For a 3-node cluster the inner `Vec` holds 2 entries.  A linear scan
/// through 2 packed `(PeerId, V)` pairs beats SipHash + bucket lookup on
/// every metric: no hash computation, one cache line, no indirection.
pub(crate) struct PeerMap<V> {
    entries: Vec<(PeerId, V)>,
}

impl<V> PeerMap<V> {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: Vec::with_capacity(capacity),
        }
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    #[inline]
    pub(crate) fn clear(&mut self) {
        self.entries.clear();
    }

    pub(crate) fn insert(&mut self, key: PeerId, value: V) {
        if let Some(slot) = self.entries.iter_mut().find(|(k, _)| *k == key) {
            slot.1 = value;
        } else {
            self.entries.push((key, value));
        }
    }

    #[inline]
    pub(crate) fn get(&self, key: &PeerId) -> Option<&V> {
        self.entries.iter().find(|(k, _)| k == key).map(|(_, v)| v)
    }

    #[inline]
    pub(crate) fn get_mut(&mut self, key: &PeerId) -> Option<&mut V> {
        self.entries
            .iter_mut()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v)
    }

    pub(crate) fn remove(&mut self, key: &PeerId) {
        if let Some(pos) = self.entries.iter().position(|(k, _)| k == key) {
            self.entries.swap_remove(pos);
        }
    }

    #[inline]
    pub(crate) fn iter(&self) -> impl Iterator<Item = (PeerId, &V)> {
        self.entries.iter().map(|(k, v)| (*k, v))
    }

    #[inline]
    pub(crate) fn contains_key(&self, key: &PeerId) -> bool {
        self.entries.iter().any(|(k, _)| k == key)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PeerProgress {
    pub(crate) next_index: LogIndex,
    pub(crate) match_index: LogIndex,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AppendAttemptState {
    pub(crate) attempts: u64,
    pub(crate) sent_last_index: LogIndex,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AppendAdvance {
    Completed,
    Retry(AppendAttemptState),
    Dropped,
    Ignored,
}

#[derive(Debug, Clone)]
pub(crate) struct PendingSnapshot {
    pub(crate) meta: SnapshotMeta,
    pub(crate) bytes: Vec<u8>,
    /// Wall-clock deadline after which this transfer is evicted as stalled.
    /// Refreshed on every accepted chunk (progress), so only a transfer that
    /// makes NO progress for the configured window is evicted (C4 / P1-5).
    pub(crate) deadline: Instant,
}

impl PendingSnapshot {
    pub(crate) fn new(meta: SnapshotMeta, stall_timeout: Duration) -> Self {
        Self {
            meta,
            bytes: Vec::new(),
            deadline: Instant::now() + stall_timeout, // F3
        }
    }

    /// Reset the transfer (new snapshot or re-start) and refresh the deadline.
    pub(crate) fn reset(&mut self, meta: SnapshotMeta, stall_timeout: Duration) {
        self.meta = meta;
        self.bytes.clear();
        self.touch(stall_timeout);
    }

    /// Record progress: push the stall deadline forward.
    pub(crate) fn touch(&mut self, stall_timeout: Duration) {
        self.deadline = Instant::now() + stall_timeout; // F3
    }

    pub(crate) fn is_expired(&self) -> bool {
        Instant::now() > self.deadline // F3
    }
}

/// Leader-side per-peer snapshot-install attempt tracking (PS7 / C4).
///
/// A follower that keeps rejecting or re-requesting a snapshot must not be
/// able to drive an unbounded re-stream loop: after
/// `limits.snapshot_max_attempts_per_peer` consecutive failed attempts the
/// leader refuses further installs to that peer until
/// `limits.snapshot_attempt_cooldown_ms` elapses, then resets and retries.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct SnapshotAttempts {
    /// Consecutive failed install attempts since the last success/reset.
    pub(crate) attempts: u32,
    /// While `Some(t)` and `now < t`, installs to this peer are refused.
    pub(crate) cooldown_until: Option<Instant>,
}
