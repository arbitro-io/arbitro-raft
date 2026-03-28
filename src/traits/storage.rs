use crate::{HardState, LogEntry, LogIndex, RaftError, SnapshotMeta, Term};

pub trait RaftStorage: Send + Sync + 'static {
    fn load_hard_state(&self) -> Result<HardState, RaftError>;
    fn save_hard_state(&self, state: &HardState) -> Result<(), RaftError>;
    fn append_entries(&self, entries: &[LogEntry]) -> Result<(), RaftError>;
    fn read_entries(&self, from: LogIndex, to: LogIndex, out: &mut Vec<LogEntry>) -> Result<(), RaftError>;
    /// Retrieves a specific entry from the log by its index.
    ///
    /// # Performance Warning
    ///
    /// The default implementation is `O(N)`. Production systems MUST override this method to be
    /// `O(1)` or `O(log N)` via an index lookup or direct seek.
    fn entry_at(&self, index: LogIndex) -> Result<Option<LogEntry>, RaftError> {
        let mut entries = Vec::new();
        self.read_entries(index, LogIndex(index.0.saturating_add(1)), &mut entries)?;
        Ok(entries.pop())
    }
    /// Returns the highest index in the log, and its corresponding term.
    ///
    /// # Performance Warning
    ///
    /// The default implementation is `O(N)` since it iterates from the beginning of the storage.
    /// Production systems MUST override this method to be `O(1)` by keeping track of the last index in memory.
    fn last_log_position(&self) -> Result<(LogIndex, Term), RaftError> {
        let mut entries = Vec::new(); // Un único Vec para leer todo. En uso real la base de datos almacena índices sueltos de última posición o se usa otro query, pero para el fallback sirve.
        self.read_entries(LogIndex(1), LogIndex(u64::MAX), &mut entries)?;
        if let Some(last) = entries.last() {
            Ok((last.index, last.term))
        } else {
            Ok((LogIndex(0), Term(0)))
        }
    }
    fn truncate_suffix(&self, from: LogIndex) -> Result<(), RaftError>;
    fn save_snapshot(&self, meta: &SnapshotMeta, snapshot: &[u8]) -> Result<(), RaftError>;
    fn load_snapshot(&self) -> Result<Option<(SnapshotMeta, Vec<u8>)>, RaftError>;
}
