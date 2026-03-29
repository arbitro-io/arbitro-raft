mod append;
mod custom;
mod snapshot;
mod vote;

pub use append::{AppendEntriesRespView, AppendEntriesView, EntryView};
pub use custom::{RaftCustomMessageView, RaftCustomResponseView};
pub use snapshot::{InstallSnapshotRespView, InstallSnapshotView};
pub use vote::{RequestVoteRespView, RequestVoteView};

use bytes::Bytes;

use crate::{InboundRaftMessage, PeerId, RaftError, RaftMessage};

use super::codec::{
    parse_append_entries_resp_view, parse_append_entries_view, parse_custom_message_view,
    parse_custom_response_view, parse_install_snapshot_resp_view, parse_install_snapshot_view,
    parse_raft_frame_view, parse_request_vote_resp_view, parse_request_vote_view,
    KIND_APPEND_ENTRIES, KIND_APPEND_ENTRIES_RESP, KIND_CUSTOM, KIND_CUSTOM_RESPONSE,
    KIND_INSTALL_SNAPSHOT, KIND_INSTALL_SNAPSHOT_RESP, KIND_REQUEST_VOTE, KIND_REQUEST_VOTE_RESP,
};

// ── RaftMessageView ───────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum RaftMessageView {
    RequestVote(RequestVoteView),
    RequestVoteResp(RequestVoteRespView),
    AppendEntries(AppendEntriesView),
    AppendEntriesResp(AppendEntriesRespView),
    InstallSnapshot(InstallSnapshotView),
    InstallSnapshotResp(InstallSnapshotRespView),
    Custom(RaftCustomMessageView),
    CustomResponse(RaftCustomResponseView),
}

impl RaftMessageView {
    pub fn parse(frame: Bytes) -> Result<InboundRaftMessageView, RaftError> {
        let frame = parse_raft_frame_view(frame)?;
        let from  = frame.from();
        let message = match frame.kind() {
            KIND_REQUEST_VOTE         => Self::RequestVote(parse_request_vote_view(frame)?),
            KIND_REQUEST_VOTE_RESP    => Self::RequestVoteResp(parse_request_vote_resp_view(frame)?),
            KIND_APPEND_ENTRIES       => Self::AppendEntries(parse_append_entries_view(frame)?),
            KIND_APPEND_ENTRIES_RESP  => Self::AppendEntriesResp(parse_append_entries_resp_view(frame)?),
            KIND_INSTALL_SNAPSHOT     => Self::InstallSnapshot(parse_install_snapshot_view(frame)?),
            KIND_INSTALL_SNAPSHOT_RESP => Self::InstallSnapshotResp(parse_install_snapshot_resp_view(frame)?),
            KIND_CUSTOM               => Self::Custom(parse_custom_message_view(frame)?),
            KIND_CUSTOM_RESPONSE      => Self::CustomResponse(parse_custom_response_view(frame)?),
            other => {
                return Err(RaftError::Protocol(format!("unknown raft message kind {}", other)))
            }
        };
        Ok(InboundRaftMessageView { from, message })
    }

    fn into_owned_message(self) -> RaftMessage {
        match self {
            Self::RequestVote(v)         => RaftMessage::RequestVote(v.to_owned()),
            Self::RequestVoteResp(v)     => RaftMessage::RequestVoteResp(v.to_owned()),
            Self::AppendEntries(v)       => RaftMessage::AppendEntries(v.to_owned()),
            Self::AppendEntriesResp(v)   => RaftMessage::AppendEntriesResp(v.to_owned()),
            Self::InstallSnapshot(v)     => RaftMessage::InstallSnapshot(v.to_owned()),
            Self::InstallSnapshotResp(v) => RaftMessage::InstallSnapshotResp(v.to_owned()),
            Self::Custom(v)              => RaftMessage::Custom(v.to_owned()),
            Self::CustomResponse(v)      => RaftMessage::CustomResponse(v.to_owned()),
        }
    }
}

// ── InboundRaftMessageView ────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct InboundRaftMessageView {
    pub from:    PeerId,
    pub message: RaftMessageView,
}

impl InboundRaftMessageView {
    pub fn into_owned(self) -> InboundRaftMessage {
        InboundRaftMessage { from: self.from, message: self.message.into_owned_message() }
    }
}
