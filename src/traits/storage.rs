use crate::protocol::codec::wire::EntryHeader;
use crate::{HardState, LogEntry, LogIndex, RaftError, SnapshotMeta, Term};

pub trait RaftStorage: Send + Sync + 'static {
    fn load_hard_state(&self) -> Result<HardState, RaftError>;
    fn save_hard_state(&self, state: &HardState) -> Result<(), RaftError>;
    fn append_entries(&self, entries: &[LogEntry<'_>]) -> Result<(), RaftError>;

    /// OPTIMIZED PATH: Appends entries from a pre-serialized header block and payload refs.
    /// Default implementation returns an error as it's optional for high-speed storages.
    fn append_entries_seeded(&self, _headers: &[EntryHeader], _payloads: &[&[u8]]) -> Result<(), RaftError> {
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
    fn read_entry_headers(&self, _from: LogIndex, _to: LogIndex) -> Result<Option<&[EntryHeader]>, RaftError> {
        Ok(None)
    }

    /// OPTIMIZED PATH: Iterates over payloads to avoid LogEntry construction.
    fn for_each_payload(&self, _from: LogIndex, _to: LogIndex, _f: &mut dyn FnMut(&[u8])) -> Result<(), RaftError> {
        Err(RaftError::Storage("for_each_payload not implemented".into()))
    }

    /// Retrieves a specific entry from the log by its index.
    /// Uses `payload_buf` to store the payload data.
    fn entry_at<'a>(&self, index: LogIndex, payload_buf: &'a mut [u8]) -> Result<Option<LogEntry<'a>>, RaftError>;
    
    /// Returns the highest index in the log, and its corresponding term.
    fn last_log_position(&self) -> Result<(LogIndex, Term), RaftError>;
    
    fn truncate_suffix(&self, from: LogIndex) -> Result<(), RaftError>;
    fn save_snapshot(&self, meta: &SnapshotMeta, snapshot: &[u8]) -> Result<(), RaftError>;
    fn load_snapshot(&self) -> Result<Option<(SnapshotMeta, Vec<u8>)>, RaftError>;
}
