use super::wire::{
    AppendEntries, EntryHeader, KIND_APPEND_ENTRIES, KIND_APPEND_ENTRIES_RESP,
    KIND_APPEND_ENTRIES_SEEDED, KIND_CUSTOM, KIND_CUSTOM_RESPONSE, KIND_INSTALL_SNAPSHOT,
    KIND_INSTALL_SNAPSHOT_RESP, KIND_PRE_VOTE, KIND_PRE_VOTE_RESP, KIND_REQUEST_VOTE,
    KIND_REQUEST_VOTE_RESP, KIND_TIMEOUT_NOW,
};
use crate::{RaftError, RaftMessage};
use zerocopy::IntoBytes;

mod contiguous;
mod vectored;

/// Checked `usize → u32` conversion for wire length/count fields (B6).
///
/// A value that does not fit in `u32` (e.g. a > 4 GiB body) must fail the
/// encode instead of being silently truncated by `as u32` — truncation would
/// stamp a wrong `body_len`/`payload_len` on the wire and corrupt the stream.
/// Belt-and-suspenders under the `MAX_FRAME_SIZE` caps enforced upstream.
#[inline]
pub(super) fn wire_len_u32(len: usize, what: &'static str) -> Result<u32, RaftError> {
    u32::try_from(len)
        .map_err(|_| RaftError::Protocol(format!("{what} {len} exceeds u32 wire field")))
}

pub use contiguous::encode_message_to_bytes;
pub use vectored::{
    encode_append_entries_vectored, encode_message_vectored, encode_message_vectored_with_group,
};

pub(super) fn kind_of(msg: &RaftMessage) -> u8 {
    match msg {
        RaftMessage::RequestVote(_) => KIND_REQUEST_VOTE,
        RaftMessage::RequestVoteResp(_) => KIND_REQUEST_VOTE_RESP,
        RaftMessage::PreVote(_) => KIND_PRE_VOTE,
        RaftMessage::PreVoteResp(_) => KIND_PRE_VOTE_RESP,
        RaftMessage::AppendEntries(_, _) => KIND_APPEND_ENTRIES,
        RaftMessage::AppendEntriesVectored(_, _) => KIND_APPEND_ENTRIES,
        RaftMessage::AppendEntriesResp(_) => KIND_APPEND_ENTRIES_RESP,
        RaftMessage::AppendEntriesSeeded { .. }
        | RaftMessage::AppendEntriesSeededVectored { .. } => KIND_APPEND_ENTRIES_SEEDED,
        RaftMessage::InstallSnapshot(_, _) => KIND_INSTALL_SNAPSHOT,
        RaftMessage::InstallSnapshotResp(_) => KIND_INSTALL_SNAPSHOT_RESP,
        RaftMessage::TimeoutNow(_) => KIND_TIMEOUT_NOW,
        RaftMessage::Custom(_) => KIND_CUSTOM,
        RaftMessage::CustomResponse(_) => KIND_CUSTOM_RESPONSE,
    }
}

pub(super) fn body_total_len(msg: &RaftMessage) -> usize {
    match msg {
        RaftMessage::RequestVote(m) => m.as_bytes().len(),
        RaftMessage::RequestVoteResp(m) => m.as_bytes().len(),
        RaftMessage::PreVote(m) => m.as_bytes().len(),
        RaftMessage::PreVoteResp(m) => m.as_bytes().len(),
        RaftMessage::AppendEntries(m, p) => m.as_bytes().len() + p.len(),
        RaftMessage::AppendEntriesVectored(_m, entries) => {
            let mut len = std::mem::size_of::<AppendEntries>();
            for e in *entries {
                len += std::mem::size_of::<EntryHeader>() + e.payload.0.len();
            }
            len
        }
        RaftMessage::AppendEntriesResp(m) => m.as_bytes().len(),
        RaftMessage::AppendEntriesSeeded {
            ae,
            headers,
            payloads,
        } => ae.as_bytes().len() + headers.len() + payloads.len(),
        RaftMessage::AppendEntriesSeededVectored {
            ae,
            headers,
            payloads,
        } => {
            let mut len = ae.as_bytes().len() + headers.len();
            for p in *payloads {
                len += p.len();
            }
            len
        }
        RaftMessage::InstallSnapshot(m, p) => m.as_bytes().len() + p.len(),
        RaftMessage::InstallSnapshotResp(m) => m.as_bytes().len(),
        RaftMessage::TimeoutNow(m) => m.as_bytes().len(),
        RaftMessage::Custom(p) => p.len(),
        RaftMessage::CustomResponse(p) => p.len(),
    }
}
