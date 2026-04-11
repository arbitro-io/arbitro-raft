use super::codec::wire::{
    AppendEntries, AppendEntriesResp, EntryHeader, InstallSnapshot, InstallSnapshotResp,
    RequestVote, RequestVoteResp,
};
use crate::{EntryPayload, LogEntry, LogIndex, PeerId, RaftError, SnapshotMeta, Term};
use zerocopy::{FromBytes, Ref};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaftMessage<'a> {
    RequestVote(&'a RequestVote),
    RequestVoteResp(&'a RequestVoteResp),
    AppendEntries(&'a AppendEntries, &'a [u8]), // Body + Raw payload (for inbound)
    AppendEntriesVectored(&'a AppendEntries, &'a [LogEntry<'a>]), // Body + Parsed entries (for outbound)
    AppendEntriesResp(&'a AppendEntriesResp),
    InstallSnapshot(&'a InstallSnapshot, &'a [u8]),
    InstallSnapshotResp(&'a InstallSnapshotResp),
    Custom(&'a [u8]),
    CustomResponse(&'a [u8]),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InboundRaftMessage<'a> {
    pub from: PeerId,
    pub message: RaftMessage<'a>,
}

impl<'a> InboundRaftMessage<'a> {
    pub fn as_append_entries(&self) -> Option<super::codec::AppendEntriesView<'a>> {
        if let RaftMessage::AppendEntries(msg, payload) = self.message {
            Some(super::codec::AppendEntriesView::new(msg, payload))
        } else {
            None
        }
    }

    pub fn as_request_vote(&self) -> Option<&'a RequestVote> {
        if let RaftMessage::RequestVote(msg) = self.message {
            Some(msg)
        } else {
            None
        }
    }

    pub fn as_request_vote_resp(&self) -> Option<&'a RequestVoteResp> {
        if let RaftMessage::RequestVoteResp(msg) = self.message {
            Some(msg)
        } else {
            None
        }
    }

    pub fn as_append_entries_resp(&self) -> Option<&'a AppendEntriesResp> {
        if let RaftMessage::AppendEntriesResp(msg) = self.message {
            Some(msg)
        } else {
            None
        }
    }

    pub fn as_install_snapshot(&self) -> Option<(&'a InstallSnapshot, &'a [u8])> {
        if let RaftMessage::InstallSnapshot(msg, payload) = self.message {
            Some((msg, payload))
        } else {
            None
        }
    }

    pub fn as_install_snapshot_resp(&self) -> Option<&'a InstallSnapshotResp> {
        if let RaftMessage::InstallSnapshotResp(msg) = self.message {
            Some(msg)
        } else {
            None
        }
    }

    pub fn as_custom(&self) -> Option<&'a [u8]> {
        if let RaftMessage::Custom(payload) = self.message {
            Some(payload)
        } else {
            None
        }
    }

    pub fn as_custom_response(&self) -> Option<&'a [u8]> {
        if let RaftMessage::CustomResponse(payload) = self.message {
            Some(payload)
        } else {
            None
        }
    }
}

pub struct AppendEntriesEntryIter<'a> {
    payload: &'a [u8],
    remaining: usize,
}

impl<'a> AppendEntriesEntryIter<'a> {
    pub fn new(payload: &'a [u8], count: usize) -> Self {
        Self {
            payload,
            remaining: count,
        }
    }
}

impl<'a> Iterator for AppendEntriesEntryIter<'a> {
    type Item = LogEntry<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }

        let (header_ref, rest) = Ref::<&'a [u8], EntryHeader>::from_prefix(self.payload).ok()?;
        let header = Ref::into_ref(header_ref);
        let len = header.payload_len.get() as usize;

        if rest.len() < len {
            return None;
        }

        let (data, next_payload) = rest.split_at(len);
        self.payload = next_payload;
        self.remaining -= 1;

        Some(LogEntry {
            term: Term(header.term.get()),
            index: LogIndex(header.index.get()),
            payload: EntryPayload(data),
        })
    }
}
