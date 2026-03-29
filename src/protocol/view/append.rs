use bytes::Bytes;

use crate::{
    AppendEntries, AppendEntriesResp, EntryPayload, LogEntry, LogIndex, PeerId, RaftError, Term,
};
use crate::protocol::codec::wire::{EntryHeaderView, RaftFrameView};

// ── AppendEntriesView ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct AppendEntriesView {
    pub(crate) from:  PeerId,
    pub(crate) frame: RaftFrameView,
}

impl AppendEntriesView {
    pub(crate) fn new(from: PeerId, frame: RaftFrameView) -> Self { Self { from, frame } }

    #[inline] pub fn from(&self) -> PeerId       { self.from }
    #[inline] pub fn term(&self) -> Term         { Term(self.frame.body_u64(0)) }
    #[inline] pub fn leader_id(&self) -> PeerId  { PeerId(self.frame.body_u64(8)) }
    #[inline] pub fn prev_log_index(&self) -> LogIndex { LogIndex(self.frame.body_u64(16)) }
    #[inline] pub fn prev_log_term(&self) -> Term { Term(self.frame.body_u64(24)) }
    #[inline] pub fn leader_commit(&self) -> LogIndex { LogIndex(self.frame.body_u64(32)) }
    #[inline] pub fn entry_count(&self) -> usize { self.frame.body_u32(40) as usize }

    pub fn entries(&self) -> Result<EntryIter<'_>, RaftError> {
        EntryIter::new(self)
    }

    pub fn to_owned(&self) -> AppendEntries {
        let body_start = self.frame.body_offset();
        let body_end   = body_start + self.frame.body().len();
        AppendEntries::from_validated_bytes(self.frame.frame().slice(body_start..body_end))
    }
}

// ── EntryIter ─────────────────────────────────────────────────────────────────

pub struct EntryIter<'a> {
    view:      &'a AppendEntriesView,
    remaining: usize,
    offset:    usize,
}

impl<'a> EntryIter<'a> {
    fn new(view: &'a AppendEntriesView) -> Result<Self, RaftError> {
        Ok(Self { view, remaining: view.entry_count(), offset: 48 })
    }
}

impl<'a> Iterator for EntryIter<'a> {
    type Item = EntryView;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let entry = EntryHeaderView::parse(self.view.frame.body(), self.offset).ok()?;
        self.offset    = entry.payload_end();
        self.remaining -= 1;
        Some(EntryView {
            frame:         self.view.frame.frame().clone(),
            body_offset:   self.view.frame.body_offset(),
            header_offset: entry.header_offset(),
        })
    }
}

// ── EntryView ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct EntryView {
    pub(crate) frame:         Bytes,
    pub(crate) body_offset:   usize,
    pub(crate) header_offset: usize,
}

impl EntryView {
    fn header_view(&self) -> EntryHeaderView<'_> {
        EntryHeaderView::parse(&self.frame[self.body_offset..], self.header_offset).unwrap()
    }

    #[inline]
    pub fn term(&self) -> Term { Term(self.header_view().term()) }

    #[inline]
    pub fn index(&self) -> LogIndex { LogIndex(self.header_view().index()) }

    #[inline]
    pub fn payload(&self) -> Bytes {
        let view = self.header_view();
        self.frame.slice(
            (self.body_offset + view.payload_start())..(self.body_offset + view.payload_end()),
        )
    }

    #[inline]
    pub fn to_owned(&self) -> LogEntry {
        LogEntry { term: self.term(), index: self.index(), payload: EntryPayload(self.payload()) }
    }
}

// ── AppendEntriesRespView ─────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct AppendEntriesRespView {
    pub(crate) from:  PeerId,
    pub(crate) frame: RaftFrameView,
}

impl AppendEntriesRespView {
    pub(crate) fn new(from: PeerId, frame: RaftFrameView) -> Self { Self { from, frame } }

    #[inline] pub fn from(&self) -> PeerId        { self.from }
    #[inline] pub fn term(&self) -> Term          { Term(self.frame.body_u64(0)) }
    #[inline] pub fn match_index(&self) -> LogIndex { LogIndex(self.frame.body_u64(8)) }
    #[inline] pub fn success(&self) -> bool       { self.frame.body_byte(16) != 0 }

    pub fn to_owned(&self) -> AppendEntriesResp {
        AppendEntriesResp {
            term:        self.term(),
            success:     self.success(),
            match_index: self.match_index(),
        }
    }
}
