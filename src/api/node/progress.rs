use crate::{LogIndex, SnapshotMeta};

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
}
