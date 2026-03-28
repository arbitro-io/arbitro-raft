use std::sync::OnceLock;
use std::time::Instant;

use bytes::{Bytes, BytesMut};
use zerocopy::byteorder::little_endian::{U16, U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Ref};

use crate::{DispatchResponseView, DispatchView, InboundRaftMessage, PeerId, RaftError, RaftMessage};

pub const RAFT_MAGIC: u32 = 0x5241_4654;
pub const RAFT_VERSION: u8 = 0x01;
pub const RAFT_FRAME_HEADER_SIZE: usize = std::mem::size_of::<RaftFrameHeader>();

pub const KIND_REQUEST_VOTE: u8 = 1;
pub const KIND_REQUEST_VOTE_RESP: u8 = 2;
pub const KIND_APPEND_ENTRIES: u8 = 3;
pub const KIND_APPEND_ENTRIES_RESP: u8 = 4;
pub const KIND_INSTALL_SNAPSHOT: u8 = 5;
pub const KIND_INSTALL_SNAPSHOT_RESP: u8 = 6;
pub const KIND_CUSTOM: u8 = 7;
pub const KIND_CUSTOM_RESPONSE: u8 = 8;

fn trace_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var_os("ARBITRO_RAFT_TRACE").is_some())
}

fn trace_log(from: PeerId, msg: impl AsRef<str>) {
    if trace_enabled() {
        eprintln!("[raft-codec from={}] {}", from.0, msg.as_ref());
    }
}

fn view_kind(view: &super::view::RaftMessageView) -> u8 {
    match view {
        super::view::RaftMessageView::RequestVote(_) => KIND_REQUEST_VOTE,
        super::view::RaftMessageView::RequestVoteResp(_) => KIND_REQUEST_VOTE_RESP,
        super::view::RaftMessageView::AppendEntries(_) => KIND_APPEND_ENTRIES,
        super::view::RaftMessageView::AppendEntriesResp(_) => KIND_APPEND_ENTRIES_RESP,
        super::view::RaftMessageView::InstallSnapshot(_) => KIND_INSTALL_SNAPSHOT,
        super::view::RaftMessageView::InstallSnapshotResp(_) => KIND_INSTALL_SNAPSHOT_RESP,
        super::view::RaftMessageView::Custom(_) => KIND_CUSTOM,
        super::view::RaftMessageView::CustomResponse(_) => KIND_CUSTOM_RESPONSE,
    }
}

#[derive(IntoBytes, FromBytes, KnownLayout, Immutable, Clone, Copy, Debug)]
#[repr(C)]
pub(crate) struct RaftFrameHeader {
    magic: U32,
    version: u8,
    kind: u8,
    flags: U16,
    from: U64,
    body_len: U32,
    reserved: U32,
}

#[derive(IntoBytes, FromBytes, KnownLayout, Immutable, Clone, Copy, Debug)]
#[repr(C)]
pub(crate) struct RequestVoteBody {
    term: U64,
    candidate_id: U64,
    last_log_index: U64,
    last_log_term: U64,
}

#[derive(IntoBytes, FromBytes, KnownLayout, Immutable, Clone, Copy, Debug)]
#[repr(C)]
pub(crate) struct RequestVoteRespBody {
    term: U64,
    vote_granted: u8,
    _pad: [u8; 7],
}

#[derive(IntoBytes, FromBytes, KnownLayout, Immutable, Clone, Copy, Debug)]
#[repr(C)]
pub(crate) struct AppendEntriesBody {
    pub(crate) term: U64,
    pub(crate) leader_id: U64,
    pub(crate) prev_log_index: U64,
    pub(crate) prev_log_term: U64,
    pub(crate) leader_commit: U64,
    pub(crate) entry_count: U32,
    pub(crate) _pad: U32,
}

#[derive(IntoBytes, FromBytes, KnownLayout, Immutable, Clone, Copy, Debug)]
#[repr(C)]
pub(crate) struct EntryHeader {
    pub(crate) term: U64,
    pub(crate) index: U64,
    pub(crate) payload_len: U32,
    pub(crate) _pad: U32,
}

#[derive(IntoBytes, FromBytes, KnownLayout, Immutable, Clone, Copy, Debug)]
#[repr(C)]
pub(crate) struct AppendEntriesRespBody {
    term: U64,
    match_index: U64,
    success: u8,
    _pad: [u8; 7],
}

#[derive(IntoBytes, FromBytes, KnownLayout, Immutable, Clone, Copy, Debug)]
#[repr(C)]
pub(crate) struct InstallSnapshotBody {
    term: U64,
    leader_id: U64,
    last_included_index: U64,
    last_included_term: U64,
    offset: U64,
    chunk_len: U32,
    done: u8,
    _pad: [u8; 3],
}

#[derive(IntoBytes, FromBytes, KnownLayout, Immutable, Clone, Copy, Debug)]
#[repr(C)]
pub(crate) struct InstallSnapshotRespBody {
    term: U64,
    next_offset: U64,
    accepted: u8,
    _pad: [u8; 7],
}

impl RaftFrameHeader {
    #[inline]
    fn ref_from_prefix(buf: &[u8]) -> Option<(&Self, &[u8])> {
        Ref::<_, Self>::from_prefix(buf)
            .ok()
            .map(|(r, rest)| (Ref::into_ref(r), rest))
    }
}

macro_rules! wire_prefix {
    ($ty:ty, $buf:expr, $name:expr) => {{
        Ref::<_, $ty>::from_prefix($buf)
            .ok()
            .map(|(r, rest)| (Ref::into_ref(r), rest))
            .ok_or_else(|| RaftError::Protocol(format!("short {}", $name)))?
    }};
}

#[derive(Debug, Clone)]
pub struct RaftFrameView {
    frame: Bytes,
}

impl RaftFrameView {
    #[inline]
    pub fn from(&self) -> PeerId {
        let (header, _) = RaftFrameHeader::ref_from_prefix(&self.frame).unwrap();
        PeerId(header.from.get())
    }

    #[inline]
    pub fn kind(&self) -> u8 {
        let (header, _) = RaftFrameHeader::ref_from_prefix(&self.frame).unwrap();
        header.kind
    }

    #[inline]
    pub fn body(&self) -> &[u8] {
        &self.frame[RAFT_FRAME_HEADER_SIZE..]
    }

    #[inline]
    pub fn frame(&self) -> &Bytes {
        &self.frame
    }

    #[inline]
    pub fn body_offset(&self) -> usize {
        RAFT_FRAME_HEADER_SIZE
    }

    #[inline]
    pub fn body_u64(&self, offset: usize) -> u64 {
        let start = offset;
        let end = start + 8;
        u64::from_le_bytes(self.body()[start..end].try_into().unwrap())
    }

    #[inline]
    pub fn body_u32(&self, offset: usize) -> u32 {
        let start = offset;
        let end = start + 4;
        u32::from_le_bytes(self.body()[start..end].try_into().unwrap())
    }

    #[inline]
    pub fn body_byte(&self, offset: usize) -> u8 {
        self.body()[offset]
    }
}

#[derive(Debug, Clone, Copy)]
pub struct EntryHeaderView<'a> {
    body: &'a [u8],
    header_offset: usize,
    payload_start: usize,
    payload_end: usize,
}

impl<'a> EntryHeaderView<'a> {
    pub fn parse(body: &'a [u8], header_offset: usize) -> Result<Self, RaftError> {
        let (header, _) = wire_prefix!(EntryHeader, &body[header_offset..], "append entry header");
        let payload_start = header_offset + std::mem::size_of::<EntryHeader>();
        let payload_end = payload_start
            .checked_add(header.payload_len.get() as usize)
            .ok_or_else(|| RaftError::Protocol("entry payload overflow".into()))?;
        if payload_end > body.len() {
            return Err(RaftError::Protocol("short append entry payload".into()));
        }
        Ok(Self {
            body,
            header_offset,
            payload_start,
            payload_end,
        })
    }

    #[inline]
    fn header(&self) -> &'a EntryHeader {
        let (header, _) = Ref::<_, EntryHeader>::from_prefix(&self.body[self.header_offset..])
            .ok()
            .map(|(r, rest)| (Ref::into_ref(r), rest))
            .unwrap();
        header
    }

    #[inline]
    pub fn term(&self) -> u64 {
        self.header().term.get()
    }

    #[inline]
    pub fn index(&self) -> u64 {
        self.header().index.get()
    }

    #[inline]
    pub fn payload_start(&self) -> usize {
        self.payload_start
    }

    #[inline]
    pub fn payload_end(&self) -> usize {
        self.payload_end
    }

    #[inline]
    pub fn header_offset(&self) -> usize {
        self.header_offset
    }
}

pub fn parse_raft_frame_view(frame: Bytes) -> Result<RaftFrameView, RaftError> {
    let (header, rest) = RaftFrameHeader::ref_from_prefix(&frame)
        .ok_or_else(|| RaftError::Protocol("short raft frame header".into()))?;
    if header.magic.get() != RAFT_MAGIC {
        return Err(RaftError::Protocol("invalid raft frame magic".into()));
    }
    if header.version != RAFT_VERSION {
        return Err(RaftError::Protocol(format!(
            "unsupported raft frame version {}",
            header.version
        )));
    }
    let body_len = header.body_len.get() as usize;
    if rest.len() != body_len {
        return Err(RaftError::Protocol(format!(
            "raft frame length mismatch: header={} actual={}",
            body_len,
            rest.len()
        )));
    }
    Ok(RaftFrameView { frame })
}

pub fn parse_request_vote_view(
    frame: RaftFrameView,
) -> Result<super::view::RequestVoteView, RaftError> {
    validate_body_exact::<RequestVoteBody>(&frame, "request vote body")?;
    let from = frame.from();
    Ok(super::view::RequestVoteView::new(from, frame))
}

pub fn parse_request_vote_resp_view(
    frame: RaftFrameView,
) -> Result<super::view::RequestVoteRespView, RaftError> {
    validate_body_exact::<RequestVoteRespBody>(&frame, "request vote resp body")?;
    let from = frame.from();
    Ok(super::view::RequestVoteRespView::new(from, frame))
}

pub fn parse_append_entries_view(
    frame: RaftFrameView,
) -> Result<super::view::AppendEntriesView, RaftError> {
    validate_append_entries_body(frame.body())?;
    let from = frame.from();
    Ok(super::view::AppendEntriesView::new(from, frame))
}

pub(crate) fn validate_append_entries_body(body: &[u8]) -> Result<(), RaftError> {
    let (wire, _) = wire_prefix!(AppendEntriesBody, body, "append entries body");
    let mut offset = std::mem::size_of::<AppendEntriesBody>();
    for _ in 0..wire.entry_count.get() {
        let entry = EntryHeaderView::parse(body, offset)?;
        offset = entry.payload_end();
    }
    if offset != body.len() {
        return Err(RaftError::Protocol("append entries trailing bytes".into()));
    }
    Ok(())
}

pub fn parse_append_entries_resp_view(
    frame: RaftFrameView,
) -> Result<super::view::AppendEntriesRespView, RaftError> {
    validate_body_exact::<AppendEntriesRespBody>(&frame, "append entries resp body")?;
    let from = frame.from();
    Ok(super::view::AppendEntriesRespView::new(from, frame))
}

pub fn parse_install_snapshot_view(
    frame: RaftFrameView,
) -> Result<super::view::InstallSnapshotView, RaftError> {
    let body = frame.body();
    let (wire, _) = wire_prefix!(InstallSnapshotBody, body, "install snapshot body");
    let payload_start = std::mem::size_of::<InstallSnapshotBody>();
    let payload_end = payload_start
        .checked_add(wire.chunk_len.get() as usize)
        .ok_or_else(|| RaftError::Protocol("snapshot chunk overflow".into()))?;
    if payload_end != body.len() {
        return Err(RaftError::Protocol(
            "install snapshot length mismatch".into(),
        ));
    }
    let from = frame.from();
    Ok(super::view::InstallSnapshotView::new(from, frame))
}

pub fn parse_install_snapshot_resp_view(
    frame: RaftFrameView,
) -> Result<super::view::InstallSnapshotRespView, RaftError> {
    validate_body_exact::<InstallSnapshotRespBody>(&frame, "install snapshot resp body")?;
    let from = frame.from();
    Ok(super::view::InstallSnapshotRespView::new(from, frame))
}

pub fn parse_custom_message_view(
    frame: RaftFrameView,
) -> Result<super::view::RaftCustomMessageView, RaftError> {
    let body = frame
        .frame()
        .slice(frame.body_offset()..(frame.body_offset() + frame.body().len()));
    let custom = DispatchView::parse(body)?;
    let from = frame.from();
    Ok(super::view::RaftCustomMessageView::new(from, custom))
}

pub fn parse_custom_response_view(
    frame: RaftFrameView,
) -> Result<super::view::RaftCustomResponseView, RaftError> {
    let body = frame
        .frame()
        .slice(frame.body_offset()..(frame.body_offset() + frame.body().len()));
    let response = DispatchResponseView::parse(body)?;
    let from = frame.from();
    Ok(super::view::RaftCustomResponseView::new(from, response))
}

fn validate_body_exact<T>(frame: &RaftFrameView, name: &str) -> Result<(), RaftError>
where
    T: FromBytes + KnownLayout + Immutable,
{
    let (_, rest) = Ref::<_, T>::from_prefix(frame.body())
        .ok()
        .map(|(r, rest)| (Ref::into_ref(r), rest))
        .ok_or_else(|| RaftError::Protocol(format!("short {}", name)))?;
    if !rest.is_empty() {
        return Err(RaftError::Protocol(format!("{} trailing bytes", name)));
    }
    Ok(())
}

pub fn encode_message(from: PeerId, msg: &RaftMessage) -> Result<Bytes, RaftError> {
    let body_len = body_len(msg)?;
    let mut frame = BytesMut::with_capacity(RAFT_FRAME_HEADER_SIZE + body_len);
    encode_message_into(from, msg, &mut frame)?;
    Ok(frame.freeze())
}

pub fn encode_message_into(from: PeerId, msg: &RaftMessage, buf: &mut BytesMut) -> Result<(), RaftError> {
    let started = Instant::now();
    let body_len = body_len(msg)?;
    buf.reserve(RAFT_FRAME_HEADER_SIZE + body_len);
    let header = RaftFrameHeader {
        magic: U32::new(RAFT_MAGIC),
        version: RAFT_VERSION,
        kind: kind_of(msg),
        flags: U16::new(0),
        from: U64::new(from.0),
        body_len: U32::new(body_len as u32),
        reserved: U32::new(0),
    };
    buf.extend_from_slice(header.as_bytes());

    match msg {
        RaftMessage::RequestVote(msg) => {
            let body = RequestVoteBody {
                term: U64::new(msg.term.0),
                candidate_id: U64::new(msg.candidate_id.0),
                last_log_index: U64::new(msg.last_log_index.0),
                last_log_term: U64::new(msg.last_log_term.0),
            };
            buf.extend_from_slice(body.as_bytes());
        }
        RaftMessage::RequestVoteResp(msg) => {
            let body = RequestVoteRespBody {
                term: U64::new(msg.term.0),
                vote_granted: u8::from(msg.vote_granted),
                _pad: [0; 7],
            };
            buf.extend_from_slice(body.as_bytes());
        }
        RaftMessage::AppendEntries(msg) => {
            buf.extend_from_slice(msg.bytes().as_ref());
        }
        RaftMessage::AppendEntriesResp(msg) => {
            let body = AppendEntriesRespBody {
                term: U64::new(msg.term.0),
                match_index: U64::new(msg.match_index.0),
                success: u8::from(msg.success),
                _pad: [0; 7],
            };
            buf.extend_from_slice(body.as_bytes());
        }
        RaftMessage::InstallSnapshot(msg) => {
            let body = InstallSnapshotBody {
                term: U64::new(msg.term.0),
                leader_id: U64::new(msg.leader_id.0),
                last_included_index: U64::new(msg.meta.last_included_index.0),
                last_included_term: U64::new(msg.meta.last_included_term.0),
                offset: U64::new(msg.chunk.offset),
                chunk_len: U32::new(msg.chunk.bytes.len() as u32),
                done: u8::from(msg.chunk.done),
                _pad: [0; 3],
            };
            buf.extend_from_slice(body.as_bytes());
            buf.extend_from_slice(msg.chunk.bytes.as_ref());
        }
        RaftMessage::InstallSnapshotResp(msg) => {
            let body = InstallSnapshotRespBody {
                term: U64::new(msg.term.0),
                next_offset: U64::new(msg.next_offset),
                accepted: u8::from(msg.accepted),
                _pad: [0; 7],
            };
            buf.extend_from_slice(body.as_bytes());
        }
        RaftMessage::Custom(msg) => {
            buf.extend_from_slice(msg.bytes.as_ref());
        }
        RaftMessage::CustomResponse(msg) => {
            buf.extend_from_slice(msg.bytes.as_ref());
        }
    }

    trace_log(
        from,
        format!(
            "encode kind={} body_len={} encode_us={}",
            kind_of(msg),
            body_len,
            started.elapsed().as_micros()
        ),
    );
    Ok(())
}

pub fn decode_message_view(frame: Bytes) -> Result<super::view::InboundRaftMessageView, RaftError> {
    let started = Instant::now();
    let inbound = super::view::RaftMessageView::parse(frame)?;
    trace_log(
        inbound.from,
        format!(
            "decode kind={} decode_us={}",
            view_kind(&inbound.message),
            started.elapsed().as_micros()
        ),
    );
    Ok(inbound)
}

pub fn decode_message(frame: Bytes) -> Result<InboundRaftMessage, RaftError> {
    Ok(decode_message_view(frame)?.into_owned())
}

fn kind_of(msg: &RaftMessage) -> u8 {
    match msg {
        RaftMessage::RequestVote(_) => KIND_REQUEST_VOTE,
        RaftMessage::RequestVoteResp(_) => KIND_REQUEST_VOTE_RESP,
        RaftMessage::AppendEntries(_) => KIND_APPEND_ENTRIES,
        RaftMessage::AppendEntriesResp(_) => KIND_APPEND_ENTRIES_RESP,
        RaftMessage::InstallSnapshot(_) => KIND_INSTALL_SNAPSHOT,
        RaftMessage::InstallSnapshotResp(_) => KIND_INSTALL_SNAPSHOT_RESP,
        RaftMessage::Custom(_) => KIND_CUSTOM,
        RaftMessage::CustomResponse(_) => KIND_CUSTOM_RESPONSE,
    }
}

fn body_len(msg: &RaftMessage) -> Result<usize, RaftError> {
    let len = match msg {
        RaftMessage::RequestVote(_) => std::mem::size_of::<RequestVoteBody>(),
        RaftMessage::RequestVoteResp(_) => std::mem::size_of::<RequestVoteRespBody>(),
        RaftMessage::AppendEntries(msg) => msg.bytes().len(),
        RaftMessage::AppendEntriesResp(_) => std::mem::size_of::<AppendEntriesRespBody>(),
        RaftMessage::InstallSnapshot(msg) => std::mem::size_of::<InstallSnapshotBody>()
            .checked_add(msg.chunk.bytes.len())
            .ok_or_else(|| RaftError::Protocol("install snapshot body overflow".into()))?,
        RaftMessage::InstallSnapshotResp(_) => std::mem::size_of::<InstallSnapshotRespBody>(),
        RaftMessage::Custom(msg) => msg.bytes.len(),
        RaftMessage::CustomResponse(msg) => msg.bytes.len(),
    };
    Ok(len)
}
