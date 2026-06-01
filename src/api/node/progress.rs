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
    #[allow(dead_code)]
    pub(crate) fn new() -> Self { Self { entries: Vec::new() } }

    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: Vec::with_capacity(capacity),
        }
    }

    #[inline] pub(crate) fn is_empty(&self) -> bool { self.entries.is_empty() }
    #[inline] pub(crate) fn clear(&mut self)          { self.entries.clear(); }

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
        self.entries.iter_mut().find(|(k, _)| k == key).map(|(_, v)| v)
    }

    pub(crate) fn remove(&mut self, key: &PeerId) {
        if let Some(pos) = self.entries.iter().position(|(k, _)| k == key) {
            self.entries.swap_remove(pos);
        }
    }

    #[inline]
    pub(crate) fn values(&self) -> impl Iterator<Item = &V> {
        self.entries.iter().map(|(_, v)| v)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PeerProgress {
    pub(crate) next_index:  LogIndex,
    pub(crate) match_index: LogIndex,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AppendAttemptState {
    pub(crate) attempts:        u64,
    pub(crate) sent_last_index: LogIndex,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AppendAdvance {
    Completed,
    Retry(AppendAttemptState),
    Dropped,
    Ignored,
}

/// Stalled snapshot transfers are evicted after this duration.
pub(crate) const SNAPSHOT_DEADLINE: Duration = Duration::from_secs(60);

#[derive(Debug, Clone)]
pub(crate) struct PendingSnapshot {
    pub(crate) meta:     SnapshotMeta,
    pub(crate) bytes:    Vec<u8>,
    /// Wall-clock deadline after which this transfer is evicted as stalled.
    pub(crate) deadline: Instant,
}

impl PendingSnapshot {
    pub(crate) fn new(meta: SnapshotMeta) -> Self {
        Self {
            meta,
            bytes:    Vec::new(),
            deadline: Instant::now() + SNAPSHOT_DEADLINE,
        }
    }

    /// Reset the transfer (new snapshot or re-start) and refresh the deadline.
    pub(crate) fn reset(&mut self, meta: SnapshotMeta) {
        self.meta = meta;
        self.bytes.clear();
        self.deadline = Instant::now() + SNAPSHOT_DEADLINE;
    }

    pub(crate) fn is_expired(&self) -> bool {
        Instant::now() > self.deadline
    }
}
