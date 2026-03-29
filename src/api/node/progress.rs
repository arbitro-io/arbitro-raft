use std::time::{Duration, Instant};

use crate::{LogIndex, SnapshotMeta};

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
