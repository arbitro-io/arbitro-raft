use crate::{HardState, LogEntry, LogIndex, RaftError, SnapshotMeta, Term};

pub trait RaftStorage: Send + Sync + 'static {
    fn load_hard_state(&self) -> Result<HardState, RaftError>;
    fn save_hard_state(&self, state: &HardState) -> Result<(), RaftError>;
    fn append_entries(&self, entries: &[LogEntry<'_>]) -> Result<(), RaftError>;
    fn read_entries<'a>(&self, from: LogIndex, to: LogIndex, out: &mut Vec<LogEntry<'a>>, payload_buf: &'a mut [u8]) -> Result<usize, RaftError>;
    /// Retrieves a specific entry from the log by its index.
    /// Uses `payload_buf` to store the payload data.
    fn entry_at<'a>(&self, index: LogIndex, payload_buf: &'a mut [u8]) -> Result<Option<LogEntry<'a>>, RaftError>;
    
    /// Returns the highest index in the log, and its corresponding term.
    fn last_log_position(&self) -> Result<(LogIndex, Term), RaftError>;
    
    fn truncate_suffix(&self, from: LogIndex) -> Result<(), RaftError>;
    fn save_snapshot(&self, meta: &SnapshotMeta, snapshot: &[u8]) -> Result<(), RaftError>;
    fn load_snapshot(&self) -> Result<Option<(SnapshotMeta, Vec<u8>)>, RaftError>;
}
