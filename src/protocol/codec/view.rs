use crate::{LogIndex, PeerId, Term};
use crate::protocol::message::AppendEntriesEntryIter;
use super::wire::AppendEntries;

/// A zero-copy view over a received AppendEntries message and its contiguous entry payload.
#[derive(Debug, Clone, Copy)]
pub struct AppendEntriesView<'a> {
    wire: &'a AppendEntries,
    payload: &'a [u8],
}

impl<'a> AppendEntriesView<'a> {
    pub fn new(wire: &'a AppendEntries, payload: &'a [u8]) -> Self {
        Self { wire, payload }
    }

    pub fn term(&self) -> Term {
        Term(self.wire.term.get())
    }

    pub fn leader_id(&self) -> PeerId {
        PeerId(self.wire.leader_id.get())
    }

    pub fn prev_log_index(&self) -> LogIndex {
        LogIndex(self.wire.prev_log_index.get())
    }

    pub fn prev_log_term(&self) -> Term {
        Term(self.wire.prev_log_term.get())
    }

    pub fn leader_commit(&self) -> LogIndex {
        LogIndex(self.wire.leader_commit.get())
    }

    pub fn entry_count(&self) -> u32 {
        self.wire.entry_count.get()
    }

    /// Returns an iterator over the entries in this message.
    /// Each entry is parsed lazily from the payload.
    pub fn entries(&self) -> Option<AppendEntriesEntryIter<'a>> {
        let count = self.entry_count() as usize;
        Some(AppendEntriesEntryIter::new(self.payload, count))
    }

    pub fn payload(&self) -> &'a [u8] {
        self.payload
    }

    pub fn wire(&self) -> &'a AppendEntries {
        self.wire
    }
}
