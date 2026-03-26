use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use zerocopy::IntoBytes;

use crate::{EntryPayload, LogEntry, LogIndex, PeerId, RaftError, SnapshotMeta, Term};

use super::codec::{validate_append_entries_body, AppendEntriesBody, EntryHeader, EntryHeaderView};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestVote {
    pub term: Term,
    pub candidate_id: PeerId,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestVoteResp {
    pub term: Term,
    pub vote_granted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendEntries {
    bytes: Bytes,
}

impl AppendEntries {
    pub fn new(
        term: Term,
        leader_id: PeerId,
        prev_log_index: LogIndex,
        prev_log_term: Term,
        leader_commit: LogIndex,
        entries: &[LogEntry],
    ) -> Result<Self, RaftError> {
        let mut body_len = std::mem::size_of::<AppendEntriesBody>();
        for entry in entries {
            body_len = body_len
                .checked_add(std::mem::size_of::<EntryHeader>())
                .and_then(|value| value.checked_add(entry.payload.0.len()))
                .ok_or_else(|| RaftError::Protocol("append entries body overflow".into()))?;
        }

        let mut bytes = BytesMut::with_capacity(body_len);
        let body = AppendEntriesBody {
            term: term.0.into(),
            leader_id: leader_id.0.into(),
            prev_log_index: prev_log_index.0.into(),
            prev_log_term: prev_log_term.0.into(),
            leader_commit: leader_commit.0.into(),
            entry_count: (entries.len() as u32).into(),
            _pad: 0.into(),
        };
        bytes.extend_from_slice(body.as_bytes());

        for entry in entries {
            let header = EntryHeader {
                term: entry.term.0.into(),
                index: entry.index.0.into(),
                payload_len: (entry.payload.0.len() as u32).into(),
                _pad: 0.into(),
            };
            bytes.extend_from_slice(header.as_bytes());
            bytes.extend_from_slice(entry.payload.0.as_ref());
        }

        let bytes = bytes.freeze();
        validate_append_entries_body(bytes.as_ref())?;
        Ok(Self { bytes })
    }

    pub(crate) fn from_validated_bytes(bytes: Bytes) -> Self {
        Self { bytes }
    }

    #[inline]
    pub fn bytes(&self) -> &Bytes {
        &self.bytes
    }

    #[inline]
    pub fn term(&self) -> Term {
        Term(self.body_u64(0))
    }

    #[inline]
    pub fn leader_id(&self) -> PeerId {
        PeerId(self.body_u64(8))
    }

    #[inline]
    pub fn prev_log_index(&self) -> LogIndex {
        LogIndex(self.body_u64(16))
    }

    #[inline]
    pub fn prev_log_term(&self) -> Term {
        Term(self.body_u64(24))
    }

    #[inline]
    pub fn leader_commit(&self) -> LogIndex {
        LogIndex(self.body_u64(32))
    }

    #[inline]
    pub fn entry_count(&self) -> usize {
        self.body_u32(40) as usize
    }

    pub fn entries(&self) -> Result<AppendEntriesEntryIter<'_>, RaftError> {
        AppendEntriesEntryIter::new(&self.bytes)
    }

    #[inline]
    fn body_u64(&self, offset: usize) -> u64 {
        let end = offset + 8;
        u64::from_le_bytes(self.bytes[offset..end].try_into().unwrap())
    }

    #[inline]
    fn body_u32(&self, offset: usize) -> u32 {
        let end = offset + 4;
        u32::from_le_bytes(self.bytes[offset..end].try_into().unwrap())
    }
}

pub struct AppendEntriesEntryIter<'a> {
    body: &'a Bytes,
    remaining: usize,
    offset: usize,
}

impl<'a> AppendEntriesEntryIter<'a> {
    fn new(body: &'a Bytes) -> Result<Self, RaftError> {
        validate_append_entries_body(body.as_ref())?;
        Ok(Self {
            body,
            remaining: u32::from_le_bytes(body[40..44].try_into().unwrap()) as usize,
            offset: std::mem::size_of::<AppendEntriesBody>(),
        })
    }
}

impl<'a> Iterator for AppendEntriesEntryIter<'a> {
    type Item = AppendEntriesEntryView<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }

        let view = EntryHeaderView::parse(self.body.as_ref(), self.offset).ok()?;
        self.offset = view.payload_end();
        self.remaining -= 1;
        Some(AppendEntriesEntryView {
            body: self.body,
            header_offset: view.header_offset(),
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct AppendEntriesEntryView<'a> {
    body: &'a Bytes,
    header_offset: usize,
}

impl<'a> AppendEntriesEntryView<'a> {
    #[inline]
    fn header_view(&self) -> EntryHeaderView<'_> {
        EntryHeaderView::parse(self.body.as_ref(), self.header_offset).unwrap()
    }

    #[inline]
    pub fn term(&self) -> Term {
        Term(self.header_view().term())
    }

    #[inline]
    pub fn index(&self) -> LogIndex {
        LogIndex(self.header_view().index())
    }

    #[inline]
    pub fn payload(&self) -> Bytes {
        let header = self.header_view();
        self.body.slice(header.payload_start()..header.payload_end())
    }

    #[inline]
    pub fn to_owned(&self) -> LogEntry {
        LogEntry {
            term: self.term(),
            index: self.index(),
            payload: EntryPayload(self.payload()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendEntriesResp {
    pub term: Term,
    pub success: bool,
    pub match_index: LogIndex,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotChunk {
    pub offset: u64,
    pub bytes: Bytes,
    pub done: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstallSnapshot {
    pub term: Term,
    pub leader_id: PeerId,
    pub meta: SnapshotMeta,
    pub chunk: SnapshotChunk,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstallSnapshotResp {
    pub term: Term,
    pub accepted: bool,
    pub next_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RaftCustomMessage {
    pub bytes: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RaftCustomResponse {
    pub bytes: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RaftMessage {
    RequestVote(RequestVote),
    RequestVoteResp(RequestVoteResp),
    AppendEntries(AppendEntries),
    AppendEntriesResp(AppendEntriesResp),
    InstallSnapshot(InstallSnapshot),
    InstallSnapshotResp(InstallSnapshotResp),
    Custom(RaftCustomMessage),
    CustomResponse(RaftCustomResponse),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundRaftMessage {
    pub from: PeerId,
    pub message: RaftMessage,
}
