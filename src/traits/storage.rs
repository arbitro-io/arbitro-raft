use crate::protocol::codec::wire::EntryHeader;
use crate::{HardState, LogEntry, LogIndex, RaftError, SnapshotMeta, Term};

/// Persistent state backing a Raft node.
///
/// # Durability contract (P1-1)
///
/// Raft's safety proof assumes the *stable storage* below is truly durable: a
/// value is on disk **before** the write call returns. Two writes are on the
/// critical safety path:
///
/// * [`save_hard_state`](RaftStorage::save_hard_state) — persists
///   `current_term` and `voted_for`. It MUST be durable (fsync'd, or an
///   equivalent barrier) before returning `Ok`. The node calls it before
///   granting a vote or advancing its term; if it returned `Ok` while the value
///   was still only in the page cache, a crash + restart could resurrect a node
///   that "forgot" it already voted in a term — a **double vote → two leaders
///   in one term → committed-entry divergence**.
/// * [`append_entries`](RaftStorage::append_entries) — a leader that counts a
///   follower's ack toward a commit quorum assumes the follower has the entry
///   durably. An implementation that acks before the entry is durable narrows
///   the crash window in which a committed entry can be lost.
///
/// A purely in-memory implementation trivially satisfies "durable before
/// return" *within a process* but loses everything on restart; use it only for
/// tests or caches, never where crash-recovery matters. Implementations are
/// free to batch/group-commit fsyncs across calls as long as no call returns
/// `Ok` before its own value is durable.
pub trait RaftStorage: Send + Sync + 'static {
    /// Load the durable `HardState` (term + vote). Called once in
    /// `RaftNode::new`; returning stale state here is the restart-side of the
    /// double-vote hazard described in the trait docs.
    fn load_hard_state(&self) -> Result<HardState, RaftError>;
    /// Persist `HardState` durably before returning `Ok` — see the trait-level
    /// durability contract.
    fn save_hard_state(&self, state: &HardState) -> Result<(), RaftError>;
    /// Append entries to the log durably before returning `Ok` — see the
    /// trait-level durability contract.
    fn append_entries(&self, entries: &[LogEntry<'_>]) -> Result<(), RaftError>;

    /// OPTIMIZED PATH: Appends entries from a pre-serialized header block and payload refs.
    /// Default implementation returns an error as it's optional for high-speed storages.
    fn append_entries_seeded(
        &self,
        _headers: &[EntryHeader],
        _payloads: &[&[u8]],
    ) -> Result<(), RaftError> {
        Err(RaftError::Storage("Seeded append not implemented".into()))
    }

    fn read_entries<'a>(
        &self,
        from: LogIndex,
        to: LogIndex,
        out: &mut Vec<LogEntry<'a>>,
        payload_buf: &'a mut [u8],
    ) -> Result<usize, RaftError>;

    /// MAGIC ZEROCOPY PATH: Returns a contiguous slice of entry headers as bytes in O(1).
    /// Used by the leader to perform zero-allocation replication.
    fn read_entry_headers(
        &self,
        _from: LogIndex,
        _to: LogIndex,
    ) -> Result<Option<&[EntryHeader]>, RaftError> {
        Ok(None)
    }

    /// OPTIMIZED PATH: Iterates over payloads to avoid LogEntry construction.
    fn for_each_payload(
        &self,
        _from: LogIndex,
        _to: LogIndex,
        _f: &mut dyn FnMut(&[u8]),
    ) -> Result<(), RaftError> {
        Err(RaftError::Storage(
            "for_each_payload not implemented".into(),
        ))
    }

    /// Retrieves a specific entry from the log by its index.
    /// Uses `payload_buf` to store the payload data.
    fn entry_at<'a>(
        &self,
        index: LogIndex,
        payload_buf: &'a mut [u8],
    ) -> Result<Option<LogEntry<'a>>, RaftError>;

    /// Returns the highest index in the log, and its corresponding term.
    fn last_log_position(&self) -> Result<(LogIndex, Term), RaftError>;

    fn truncate_suffix(&self, from: LogIndex) -> Result<(), RaftError>;

    /// Delete every log entry with index STRICTLY LESS THAN `up_to`.
    ///
    /// Called after a snapshot has been saved via `save_snapshot`. The default
    /// implementation is a no-op — storages that want log compaction must
    /// override it. It is NEVER an error to leave entries in place; the log
    /// simply grows unbounded (existing behavior).
    fn truncate_before(&self, _up_to: LogIndex) -> Result<(), RaftError> {
        Ok(())
    }

    fn save_snapshot(&self, meta: &SnapshotMeta, snapshot: &[u8]) -> Result<(), RaftError>;
    fn load_snapshot(&self) -> Result<Option<(SnapshotMeta, Vec<u8>)>, RaftError>;
}
