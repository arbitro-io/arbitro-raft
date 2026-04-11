use crate::{LogIndex, Term};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EntryPayload<'a>(pub &'a [u8]);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogEntry<'a> {
    pub term: Term,
    pub index: LogIndex,
    pub payload: EntryPayload<'a>,
}
