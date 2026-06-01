use zerocopy::{FromBytes, Immutable, KnownLayout, Ref};

use crate::{InboundRaftMessage, PeerId, RaftError, RaftMessage};

use super::wire::{
    AppendEntries, AppendEntriesResp, EntryHeader, InstallSnapshot, InstallSnapshotResp,
    RaftFrameHeader, RequestVote, RequestVoteResp, KIND_APPEND_ENTRIES, KIND_APPEND_ENTRIES_RESP,
    KIND_APPEND_ENTRIES_SEEDED, KIND_CUSTOM, KIND_CUSTOM_RESPONSE, KIND_INSTALL_SNAPSHOT,
    KIND_INSTALL_SNAPSHOT_RESP, KIND_REQUEST_VOTE, KIND_REQUEST_VOTE_RESP, RAFT_MAGIC, RAFT_VERSION,
};

// ── Shared parse helper ───────────────────────────────────────────────────────

#[inline]
pub(crate) fn parse_prefix<'a, T>(buf: &'a [u8], name: &str) -> Result<(&'a T, &'a [u8]), RaftError>
where
    T: FromBytes + KnownLayout + Immutable,
{
    Ref::<&'a [u8], T>::from_prefix(buf)
        .ok()
        .map(|(r, rest)| (Ref::into_ref(r), rest))
        .ok_or_else(|| RaftError::Protocol(format!("short {}", name)))
}

// ── AppendEntries body validation ─────────────────────────────────────────────

pub(crate) fn validate_append_entries_body(
    entry_count: u32,
    mut body: &[u8],
) -> Result<(), RaftError> {
    for _ in 0..entry_count {
        let (header, rest) = parse_prefix::<EntryHeader>(body, "append entry header")?;
        let len = header.payload_len.get() as usize;
        if rest.len() < len {
            return Err(RaftError::Protocol("short append entry payload".into()));
        }
        body = &rest[len..];
    }
    if !body.is_empty() {
        return Err(RaftError::Protocol("append entries trailing bytes".into()));
    }
    Ok(())
}

fn validate_seeded_payloads(
    headers: &[u8],
    payloads: &[u8],
    count: usize,
) -> Result<(), RaftError> {
    let header_size = std::mem::size_of::<EntryHeader>();
    let mut expected_len = 0usize;
    for i in 0..count {
        let offset = i * header_size;
        let header_bytes = &headers[offset..offset + header_size];
        let header_ref = Ref::<&[u8], EntryHeader>::from_bytes(header_bytes)
            .map_err(|_| RaftError::Protocol("unaligned seeded entry header".into()))?;
        let header = Ref::into_ref(header_ref);
        expected_len += header.payload_len.get() as usize;
    }
    if expected_len != payloads.len() {
        return Err(RaftError::Protocol(format!(
            "seeded payloads size mismatch: headers specify={} actual={}",
            expected_len,
            payloads.len()
        )));
    }
    Ok(())
}

// ── Top-level decode ──────────────────────────────────────────────────────────

pub fn decode_message<'a>(frame: &'a [u8]) -> Result<InboundRaftMessage<'a>, RaftError> {
    let (header, rest) = parse_prefix::<RaftFrameHeader>(frame, "raft frame header")?;

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

    let from = PeerId(header.from.get());
    let body = rest;

    let message = match header.kind {
        KIND_REQUEST_VOTE => {
            let (wire, body_rest) = parse_prefix::<RequestVote>(body, "request vote body")?;
            if !body_rest.is_empty() {
                return Err(RaftError::Protocol("request vote trailing bytes".into()));
            }
            RaftMessage::RequestVote(wire)
        }
        KIND_REQUEST_VOTE_RESP => {
            let (wire, body_rest) =
                parse_prefix::<RequestVoteResp>(body, "request vote resp body")?;
            if !body_rest.is_empty() {
                return Err(RaftError::Protocol(
                    "request vote resp trailing bytes".into(),
                ));
            }
            RaftMessage::RequestVoteResp(wire)
        }
        KIND_APPEND_ENTRIES => {
            let (wire, body_rest) = parse_prefix::<AppendEntries>(body, "append entries body")?;
            validate_append_entries_body(wire.entry_count.get(), body_rest)?;
            RaftMessage::AppendEntries(wire, body_rest)
        }
        KIND_APPEND_ENTRIES_RESP => {
            let (wire, body_rest) =
                parse_prefix::<AppendEntriesResp>(body, "append entries resp body")?;
            if !body_rest.is_empty() {
                return Err(RaftError::Protocol(
                    "append entries resp trailing bytes".into(),
                ));
            }
            RaftMessage::AppendEntriesResp(wire)
        }
        KIND_APPEND_ENTRIES_SEEDED => {
            let (ae, body_rest) =
                parse_prefix::<AppendEntries>(body, "append entries seeded body")?;
            let count = ae.entry_count.get() as usize;
            let headers_len = count * std::mem::size_of::<EntryHeader>();

            if body_rest.len() < headers_len {
                return Err(RaftError::Protocol("short seeded headers block".into()));
            }

            let (headers, payloads) = body_rest.split_at(headers_len);
            validate_seeded_payloads(headers, payloads, count)?;
            RaftMessage::AppendEntriesSeeded {
                ae,
                headers,
                payloads,
            }
        }
        KIND_INSTALL_SNAPSHOT => {
            let (wire, body_rest) = parse_prefix::<InstallSnapshot>(body, "install snapshot body")?;
            let chunk_len = wire.chunk_len.get() as usize;
            if body_rest.len() != chunk_len {
                return Err(RaftError::Protocol(
                    "install snapshot chunk length mismatch".into(),
                ));
            }
            RaftMessage::InstallSnapshot(wire, body_rest)
        }
        KIND_INSTALL_SNAPSHOT_RESP => {
            let (wire, body_rest) =
                parse_prefix::<InstallSnapshotResp>(body, "install snapshot resp body")?;
            if !body_rest.is_empty() {
                return Err(RaftError::Protocol(
                    "install snapshot resp trailing bytes".into(),
                ));
            }
            RaftMessage::InstallSnapshotResp(wire)
        }
        KIND_CUSTOM => RaftMessage::Custom(body),
        KIND_CUSTOM_RESPONSE => RaftMessage::CustomResponse(body),
        other => {
            return Err(RaftError::Protocol(format!(
                "unknown raft message kind {}",
                other
            )))
        }
    };

    Ok(InboundRaftMessage { from, message })
}
