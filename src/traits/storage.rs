use crate::{HardState, LogEntry, LogIndex, RaftError, SnapshotMeta, Term};

pub trait RaftStorage: Send + Sync + 'static {
    fn load_hard_state(&self) -> Result<HardState, RaftError>;
    fn save_hard_state(&self, state: &HardState) -> Result<(), RaftError>;
    fn append_entries(&self, entries: &[LogEntry]) -> Result<(), RaftError>;
    fn read_entries(&self, from: LogIndex, to: LogIndex) -> Result<Vec<LogEntry>, RaftError>;
    fn entry_at(&self, index: LogIndex) -> Result<Option<LogEntry>, RaftError> {
        let mut entries = self.read_entries(index, LogIndex(index.0.saturating_add(1)))?;
        Ok(entries.pop())
    }
    fn last_log_position(&self) -> Result<(LogIndex, Term), RaftError> {
        let entries = self.read_entries(LogIndex(1), LogIndex(u64::MAX))?;
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
