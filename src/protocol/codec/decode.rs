use bytes::Bytes;
use zerocopy::{FromBytes, Immutable, KnownLayout, Ref};

use crate::{RaftError};

use super::wire::{
    parse_prefix, AppendEntriesBody, AppendEntriesRespBody, EntryHeaderView, InstallSnapshotBody,
    InstallSnapshotRespBody, RaftFrameView, RequestVoteBody, RequestVoteRespBody,
    KIND_APPEND_ENTRIES, KIND_APPEND_ENTRIES_RESP, KIND_CUSTOM, KIND_CUSTOM_RESPONSE,
    KIND_INSTALL_SNAPSHOT, KIND_INSTALL_SNAPSHOT_RESP, KIND_REQUEST_VOTE, KIND_REQUEST_VOTE_RESP,
    RAFT_MAGIC, RAFT_VERSION,
};

// ── Frame parse ───────────────────────────────────────────────────────────────

pub fn parse_raft_frame_view(frame: Bytes) -> Result<RaftFrameView, RaftError> {
    let (header, rest) = super::wire::RaftFrameHeader::ref_from_prefix(&frame)
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

// ── Per-kind view constructors ────────────────────────────────────────────────

pub fn parse_request_vote_view(
    frame: RaftFrameView,
) -> Result<super::super::view::RequestVoteView, RaftError> {
    validate_body_exact::<RequestVoteBody>(&frame, "request vote body")?;
    let from = frame.from();
    Ok(super::super::view::RequestVoteView::new(from, frame))
}

pub fn parse_request_vote_resp_view(
    frame: RaftFrameView,
) -> Result<super::super::view::RequestVoteRespView, RaftError> {
    validate_body_exact::<RequestVoteRespBody>(&frame, "request vote resp body")?;
    let from = frame.from();
    Ok(super::super::view::RequestVoteRespView::new(from, frame))
}

pub fn parse_append_entries_view(
    frame: RaftFrameView,
) -> Result<super::super::view::AppendEntriesView, RaftError> {
    validate_append_entries_body(frame.body())?;
    let from = frame.from();
    Ok(super::super::view::AppendEntriesView::new(from, frame))
}

pub fn parse_append_entries_resp_view(
    frame: RaftFrameView,
) -> Result<super::super::view::AppendEntriesRespView, RaftError> {
    validate_body_exact::<AppendEntriesRespBody>(&frame, "append entries resp body")?;
    let from = frame.from();
    Ok(super::super::view::AppendEntriesRespView::new(from, frame))
}

pub fn parse_install_snapshot_view(
    frame: RaftFrameView,
) -> Result<super::super::view::InstallSnapshotView, RaftError> {
    let body = frame.body();
    let (wire, _) = parse_prefix::<InstallSnapshotBody>(body, "install snapshot body")?;
    let payload_start = std::mem::size_of::<InstallSnapshotBody>();
    let payload_end   = payload_start
        .checked_add(wire.chunk_len.get() as usize)
        .ok_or_else(|| RaftError::Protocol("snapshot chunk overflow".into()))?;
    if payload_end != body.len() {
        return Err(RaftError::Protocol("install snapshot length mismatch".into()));
    }
    let from = frame.from();
    Ok(super::super::view::InstallSnapshotView::new(from, frame))
}

pub fn parse_install_snapshot_resp_view(
    frame: RaftFrameView,
) -> Result<super::super::view::InstallSnapshotRespView, RaftError> {
    validate_body_exact::<InstallSnapshotRespBody>(&frame, "install snapshot resp body")?;
    let from = frame.from();
    Ok(super::super::view::InstallSnapshotRespView::new(from, frame))
}

pub fn parse_custom_message_view(
    frame: RaftFrameView,
) -> Result<super::super::view::RaftCustomMessageView, RaftError> {
    let body   = frame.frame().slice(frame.body_offset()..(frame.body_offset() + frame.body().len()));
    let custom = crate::DispatchView::parse(body)?;
    let from   = frame.from();
    Ok(super::super::view::RaftCustomMessageView::new(from, custom))
}

pub fn parse_custom_response_view(
    frame: RaftFrameView,
) -> Result<super::super::view::RaftCustomResponseView, RaftError> {
    let body     = frame.frame().slice(frame.body_offset()..(frame.body_offset() + frame.body().len()));
    let response = crate::DispatchResponseView::parse(body)?;
    let from     = frame.from();
    Ok(super::super::view::RaftCustomResponseView::new(from, response))
}

// ── AppendEntries body validation ─────────────────────────────────────────────

pub(crate) fn validate_append_entries_body(body: &[u8]) -> Result<(), RaftError> {
    let (wire, _) = parse_prefix::<AppendEntriesBody>(body, "append entries body")?;
    let mut offset = std::mem::size_of::<AppendEntriesBody>();
    for _ in 0..wire.entry_count.get() {
        let entry = EntryHeaderView::parse(body, offset)?;
        offset    = entry.payload_end();
    }
    if offset != body.len() {
        return Err(RaftError::Protocol("append entries trailing bytes".into()));
    }
    Ok(())
}

// ── Top-level decode ──────────────────────────────────────────────────────────

pub fn decode_message_view(
    frame: Bytes,
) -> Result<super::super::view::InboundRaftMessageView, RaftError> {
    let inbound = super::super::view::RaftMessageView::parse(frame)?;
    if super::trace_enabled() {
        let kind = match &inbound.message {
            super::super::view::RaftMessageView::RequestVote(_)         => KIND_REQUEST_VOTE,
            super::super::view::RaftMessageView::RequestVoteResp(_)     => KIND_REQUEST_VOTE_RESP,
            super::super::view::RaftMessageView::AppendEntries(_)       => KIND_APPEND_ENTRIES,
            super::super::view::RaftMessageView::AppendEntriesResp(_)   => KIND_APPEND_ENTRIES_RESP,
            super::super::view::RaftMessageView::InstallSnapshot(_)     => KIND_INSTALL_SNAPSHOT,
            super::super::view::RaftMessageView::InstallSnapshotResp(_) => KIND_INSTALL_SNAPSHOT_RESP,
            super::super::view::RaftMessageView::Custom(_)              => KIND_CUSTOM,
            super::super::view::RaftMessageView::CustomResponse(_)      => KIND_CUSTOM_RESPONSE,
        };
        super::trace_log(inbound.from, format!("decode kind={}", kind));
    }
    Ok(inbound)
}

pub fn decode_message(frame: Bytes) -> Result<crate::InboundRaftMessage, RaftError> {
    Ok(decode_message_view(frame)?.into_owned())
}

// ── Body validation helper ────────────────────────────────────────────────────

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
