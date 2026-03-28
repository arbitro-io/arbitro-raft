use crate::{LogIndex, Term};
use bytes::Bytes;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntryPayload(pub Bytes);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    pub term: Term,
    pub index: LogIndex,
    pub payload: EntryPayload,
}
