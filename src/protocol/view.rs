use bytes::Bytes;

use crate::{
    AppendEntries, AppendEntriesResp, DispatchResponseView, DispatchView, EntryPayload,
    InboundRaftMessage, InstallSnapshot, InstallSnapshotResp, LogEntry, LogIndex, PeerId,
    RaftCustomMessage, RaftCustomResponse, RaftError, RaftMessage, RequestVote, RequestVoteResp,
    SnapshotChunk, SnapshotMeta, Term,
};

use super::codec::{
    parse_append_entries_resp_view, parse_append_entries_view, parse_custom_message_view,
    parse_custom_response_view, parse_install_snapshot_resp_view, parse_install_snapshot_view,
    parse_raft_frame_view,
    parse_request_vote_resp_view,
    parse_request_vote_view, EntryHeaderView, RaftFrameView, KIND_APPEND_ENTRIES,
    KIND_APPEND_ENTRIES_RESP, KIND_CUSTOM, KIND_CUSTOM_RESPONSE, KIND_INSTALL_SNAPSHOT,
    KIND_INSTALL_SNAPSHOT_RESP, KIND_REQUEST_VOTE, KIND_REQUEST_VOTE_RESP,
};

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
        let from = frame.from();
        let message = match frame.kind() {
            KIND_REQUEST_VOTE => Self::RequestVote(parse_request_vote_view(frame)?),
            KIND_REQUEST_VOTE_RESP => Self::RequestVoteResp(parse_request_vote_resp_view(frame)?),
            KIND_APPEND_ENTRIES => Self::AppendEntries(parse_append_entries_view(frame)?),
            KIND_APPEND_ENTRIES_RESP => {
                Self::AppendEntriesResp(parse_append_entries_resp_view(frame)?)
            }
            KIND_INSTALL_SNAPSHOT => Self::InstallSnapshot(parse_install_snapshot_view(frame)?),
            KIND_INSTALL_SNAPSHOT_RESP => {
                Self::InstallSnapshotResp(parse_install_snapshot_resp_view(frame)?)
            }
            KIND_CUSTOM => Self::Custom(parse_custom_message_view(frame)?),
            KIND_CUSTOM_RESPONSE => Self::CustomResponse(parse_custom_response_view(frame)?),
            other => {
                return Err(RaftError::Protocol(format!(
                    "unknown raft message kind {}",
                    other
                )))
            }
        };
        Ok(InboundRaftMessageView { from, message })
    }

    fn into_owned_message(self) -> RaftMessage {
        match self {
            Self::RequestVote(view) => RaftMessage::RequestVote(view.to_owned()),
            Self::RequestVoteResp(view) => RaftMessage::RequestVoteResp(view.to_owned()),
            Self::AppendEntries(view) => RaftMessage::AppendEntries(view.to_owned()),
            Self::AppendEntriesResp(view) => RaftMessage::AppendEntriesResp(view.to_owned()),
            Self::InstallSnapshot(view) => RaftMessage::InstallSnapshot(view.to_owned()),
            Self::InstallSnapshotResp(view) => RaftMessage::InstallSnapshotResp(view.to_owned()),
            Self::Custom(view) => RaftMessage::Custom(view.to_owned()),
            Self::CustomResponse(view) => RaftMessage::CustomResponse(view.to_owned()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct InboundRaftMessageView {
    pub from: PeerId,
    pub message: RaftMessageView,
}

impl InboundRaftMessageView {
    pub fn into_owned(self) -> InboundRaftMessage {
        InboundRaftMessage {
            from: self.from,
            message: self.message.into_owned_message(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RequestVoteView {
    from: PeerId,
    frame: RaftFrameView,
}

impl RequestVoteView {
    pub(crate) fn new(from: PeerId, frame: RaftFrameView) -> Self {
        Self { from, frame }
    }

    #[inline]
    pub fn from(&self) -> PeerId {
        self.from
    }

    #[inline]
    pub fn term(&self) -> Term {
        Term(self.frame.body_u64(0))
    }

    #[inline]
    pub fn candidate_id(&self) -> PeerId {
        PeerId(self.frame.body_u64(8))
    }

    #[inline]
    pub fn last_log_index(&self) -> LogIndex {
        LogIndex(self.frame.body_u64(16))
    }

    #[inline]
    pub fn last_log_term(&self) -> Term {
        Term(self.frame.body_u64(24))
    }

    pub fn to_owned(&self) -> RequestVote {
        RequestVote {
            term: self.term(),
            candidate_id: self.candidate_id(),
            last_log_index: self.last_log_index(),
            last_log_term: self.last_log_term(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RequestVoteRespView {
    from: PeerId,
    frame: RaftFrameView,
}

impl RequestVoteRespView {
    pub(crate) fn new(from: PeerId, frame: RaftFrameView) -> Self {
        Self { from, frame }
    }

    #[inline]
    pub fn from(&self) -> PeerId {
        self.from
    }

    #[inline]
    pub fn term(&self) -> Term {
        Term(self.frame.body_u64(0))
    }

    #[inline]
    pub fn vote_granted(&self) -> bool {
        self.frame.body_byte(8) != 0
    }

    pub fn to_owned(&self) -> RequestVoteResp {
        RequestVoteResp {
            term: self.term(),
            vote_granted: self.vote_granted(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AppendEntriesView {
    from: PeerId,
    frame: RaftFrameView,
}

impl AppendEntriesView {
    pub(crate) fn new(from: PeerId, frame: RaftFrameView) -> Self {
        Self { from, frame }
    }

    #[inline]
    pub fn from(&self) -> PeerId {
        self.from
    }

    #[inline]
    pub fn term(&self) -> Term {
        Term(self.frame.body_u64(0))
    }

    #[inline]
    pub fn leader_id(&self) -> PeerId {
        PeerId(self.frame.body_u64(8))
    }

    #[inline]
    pub fn prev_log_index(&self) -> LogIndex {
        LogIndex(self.frame.body_u64(16))
    }

    #[inline]
    pub fn prev_log_term(&self) -> Term {
        Term(self.frame.body_u64(24))
    }

    #[inline]
    pub fn leader_commit(&self) -> LogIndex {
        LogIndex(self.frame.body_u64(32))
    }

    #[inline]
    pub fn entry_count(&self) -> usize {
        self.frame.body_u32(40) as usize
    }

    pub fn entries(&self) -> Result<EntryIter<'_>, RaftError> {
        EntryIter::new(self)
    }

    pub fn to_owned(&self) -> AppendEntries {
        let body_start = self.frame.body_offset();
        let body_end = body_start + self.frame.body().len();
        AppendEntries::from_validated_bytes(self.frame.frame().slice(body_start..body_end))
    }
}

pub struct EntryIter<'a> {
    view: &'a AppendEntriesView,
    remaining: usize,
    offset: usize,
}

impl<'a> EntryIter<'a> {
    fn new(view: &'a AppendEntriesView) -> Result<Self, RaftError> {
        let offset = 48;
        Ok(Self {
            view,
            remaining: view.entry_count(),
            offset,
        })
    }
}

impl<'a> Iterator for EntryIter<'a> {
    type Item = EntryView;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let entry = EntryHeaderView::parse(self.view.frame.body(), self.offset).ok()?;
        self.offset = entry.payload_end();
        self.remaining -= 1;
        Some(EntryView {
            frame: self.view.frame.frame().clone(),
            body_offset: self.view.frame.body_offset(),
            header_offset: entry.header_offset(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct EntryView {
    frame: Bytes,
    body_offset: usize,
    header_offset: usize,
}

impl EntryView {
    fn header_view(&self) -> EntryHeaderView<'_> {
        EntryHeaderView::parse(&self.frame[self.body_offset..], self.header_offset).unwrap()
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
        let view = self.header_view();
        self.frame.slice(
            (self.body_offset + view.payload_start())..(self.body_offset + view.payload_end()),
        )
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

#[derive(Debug, Clone)]
pub struct AppendEntriesRespView {
    from: PeerId,
    frame: RaftFrameView,
}

impl AppendEntriesRespView {
    pub(crate) fn new(from: PeerId, frame: RaftFrameView) -> Self {
        Self { from, frame }
    }

    #[inline]
    pub fn from(&self) -> PeerId {
        self.from
    }

    #[inline]
    pub fn term(&self) -> Term {
        Term(self.frame.body_u64(0))
    }

    #[inline]
    pub fn match_index(&self) -> LogIndex {
        LogIndex(self.frame.body_u64(8))
    }

    #[inline]
    pub fn success(&self) -> bool {
        self.frame.body_byte(16) != 0
    }

    pub fn to_owned(&self) -> AppendEntriesResp {
        AppendEntriesResp {
            term: self.term(),
            success: self.success(),
            match_index: self.match_index(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct InstallSnapshotView {
    from: PeerId,
    frame: RaftFrameView,
}

impl InstallSnapshotView {
    pub(crate) fn new(from: PeerId, frame: RaftFrameView) -> Self {
        Self { from, frame }
    }

    #[inline]
    pub fn from(&self) -> PeerId {
        self.from
    }

    #[inline]
    pub fn term(&self) -> Term {
        Term(self.frame.body_u64(0))
    }

    #[inline]
    pub fn leader_id(&self) -> PeerId {
        PeerId(self.frame.body_u64(8))
    }

    #[inline]
    pub fn meta(&self) -> SnapshotMeta {
        SnapshotMeta {
            last_included_index: LogIndex(self.frame.body_u64(16)),
            last_included_term: Term(self.frame.body_u64(24)),
        }
    }

    #[inline]
    pub fn offset(&self) -> u64 {
        self.frame.body_u64(32)
    }

    #[inline]
    pub fn chunk_len(&self) -> usize {
        self.frame.body_u32(40) as usize
    }

    #[inline]
    pub fn done(&self) -> bool {
        self.frame.body_byte(44) != 0
    }

    #[inline]
    pub fn chunk_bytes(&self) -> Bytes {
        self.frame.frame().slice(
            (self.frame.body_offset() + 48)..(self.frame.body_offset() + 48 + self.chunk_len()),
        )
    }

    pub fn to_owned(&self) -> InstallSnapshot {
        InstallSnapshot {
            term: self.term(),
            leader_id: self.leader_id(),
            meta: self.meta(),
            chunk: SnapshotChunk {
                offset: self.offset(),
                bytes: self.chunk_bytes(),
                done: self.done(),
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct InstallSnapshotRespView {
    from: PeerId,
    frame: RaftFrameView,
}

impl InstallSnapshotRespView {
    pub(crate) fn new(from: PeerId, frame: RaftFrameView) -> Self {
        Self { from, frame }
    }

    #[inline]
    pub fn from(&self) -> PeerId {
        self.from
    }

    #[inline]
    pub fn term(&self) -> Term {
        Term(self.frame.body_u64(0))
    }

    #[inline]
    pub fn next_offset(&self) -> u64 {
        self.frame.body_u64(8)
    }

    #[inline]
    pub fn accepted(&self) -> bool {
        self.frame.body_byte(16) != 0
    }

    pub fn to_owned(&self) -> InstallSnapshotResp {
        InstallSnapshotResp {
            term: self.term(),
            accepted: self.accepted(),
            next_offset: self.next_offset(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RaftCustomMessageView {
    from: PeerId,
    dispatch: DispatchView,
}

impl RaftCustomMessageView {
    pub(crate) fn new(from: PeerId, dispatch: DispatchView) -> Self {
        Self { from, dispatch }
    }

    #[inline]
    pub fn from(&self) -> PeerId {
        self.from
    }

    #[inline]
    pub fn command(&self) -> u8 {
        self.dispatch.command()
    }

    #[inline]
    pub fn tx_id(&self) -> u64 {
        self.dispatch.tx_id()
    }

    #[inline]
    pub fn body(&self) -> &[u8] {
        self.dispatch.body()
    }

    #[inline]
    pub fn dispatch(&self) -> &DispatchView {
        &self.dispatch
    }

    pub fn to_owned(&self) -> RaftCustomMessage {
        RaftCustomMessage {
            bytes: self.dispatch.frame_bytes().clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RaftCustomResponseView {
    from: PeerId,
    response: DispatchResponseView,
}

impl RaftCustomResponseView {
    pub(crate) fn new(from: PeerId, response: DispatchResponseView) -> Self {
        Self { from, response }
    }

    #[inline]
    pub fn from(&self) -> PeerId {
        self.from
    }

    #[inline]
    pub fn tx_id(&self) -> u64 {
        self.response.tx_id()
    }

    #[inline]
    pub fn command(&self) -> u8 {
        self.response.command()
    }

    #[inline]
    pub fn response(&self) -> &DispatchResponseView {
        &self.response
    }

    pub fn to_owned(&self) -> RaftCustomResponse {
        RaftCustomResponse {
            bytes: self.response.frame_bytes().clone(),
        }
    }
}
