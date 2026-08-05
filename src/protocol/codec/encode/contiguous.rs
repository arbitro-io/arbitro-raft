use super::{body_total_len, kind_of, wire_len_u32};
use crate::protocol::codec::wire::{
    AppendEntries, EntryHeader, RaftFrameHeader, RAFT_FRAME_HEADER_SIZE, RAFT_MAGIC, RAFT_VERSION,
};
use crate::{PeerId, RaftError, RaftMessage};
use zerocopy::{IntoBytes, Ref};

/// Encodes a message into a single contiguous `Bytes` object.
/// Useful for parallel fan-out (sharing the same frame across multiple sends).
pub fn encode_message_to_bytes(from: PeerId, msg: &RaftMessage) -> Result<bytes::Bytes, RaftError> {
    let body_len = body_total_len(msg);
    // B6: checked conversion BEFORE the allocation — a > 4 GiB body must be
    // rejected here, not truncated into a wrong `body_len` on the wire.
    let body_len_u32 = wire_len_u32(body_len, "frame body length")?;
    let total_len = RAFT_FRAME_HEADER_SIZE + body_len;

    let mut buf = bytes::BytesMut::with_capacity(total_len);
    // Safety: `set_len` exposes `total_len` uninitialized bytes; every one of
    // them is written before `freeze()`. The header block below writes ALL
    // eight `RaftFrameHeader` fields (including `flags` and `reserved`, which
    // are set to 0), and each message arm writes exactly `body_len` body bytes
    // (`body_total_len` is the sum of those writes). No byte is left
    // uninitialized, so nothing uninitialized is ever read or sent on the wire.
    unsafe { buf.set_len(total_len) };

    // 1. Frame Header — write every field; leaving `flags`/`reserved` unset
    //    would ship uninitialized heap bytes on the wire (UB + info leak).
    {
        let (h_bytes, _) = buf.split_at_mut(RAFT_FRAME_HEADER_SIZE);
        let (h_ref, _) = Ref::<_, RaftFrameHeader>::from_prefix(h_bytes)
            .map_err(|_| RaftError::Protocol("alignment".into()))?;
        let header = Ref::into_mut(h_ref);
        header.magic.set(RAFT_MAGIC);
        header.version = RAFT_VERSION;
        header.kind = kind_of(msg);
        header.flags.set(0);
        header.from.set(from.0);
        header.body_len.set(body_len_u32);
        header.reserved.set(0);
        header.group_id.set(0);
    }

    match msg {
        RaftMessage::AppendEntriesVectored(m, entries) => {
            let mut offset = RAFT_FRAME_HEADER_SIZE;

            // Body Header
            let ae_len = std::mem::size_of::<AppendEntries>();
            buf[offset..offset + ae_len].copy_from_slice(m.as_bytes());
            offset += ae_len;

            for e in *entries {
                let eh_len = std::mem::size_of::<EntryHeader>();
                // Entry Header
                let eh = EntryHeader {
                    term: e.term.0.into(),
                    index: e.index.0.into(),
                    // B6: checked — never truncate a payload length.
                    payload_len: wire_len_u32(e.payload.0.len(), "entry payload length")?.into(),
                    _pad: 0.into(),
                };
                buf[offset..offset + eh_len].copy_from_slice(eh.as_bytes());
                offset += eh_len;

                // Payload
                let p_len = e.payload.0.len();
                if p_len > 0 {
                    buf[offset..offset + p_len].copy_from_slice(e.payload.0);
                    offset += p_len;
                }
            }
        }
        RaftMessage::AppendEntries(m, p) => {
            let mut offset = RAFT_FRAME_HEADER_SIZE;
            buf[offset..offset + m.as_bytes().len()].copy_from_slice(m.as_bytes());
            offset += m.as_bytes().len();
            if !p.is_empty() {
                buf[offset..offset + p.len()].copy_from_slice(p);
            }
        }
        RaftMessage::RequestVote(m) => {
            buf[RAFT_FRAME_HEADER_SIZE..].copy_from_slice(m.as_bytes());
        }
        RaftMessage::RequestVoteResp(m) => {
            buf[RAFT_FRAME_HEADER_SIZE..].copy_from_slice(m.as_bytes());
        }
        RaftMessage::PreVote(m) => {
            buf[RAFT_FRAME_HEADER_SIZE..].copy_from_slice(m.as_bytes());
        }
        RaftMessage::PreVoteResp(m) => {
            buf[RAFT_FRAME_HEADER_SIZE..].copy_from_slice(m.as_bytes());
        }
        RaftMessage::AppendEntriesResp(m) => {
            buf[RAFT_FRAME_HEADER_SIZE..].copy_from_slice(m.as_bytes());
        }
        RaftMessage::InstallSnapshot(m, p) => {
            let mut offset = RAFT_FRAME_HEADER_SIZE;
            buf[offset..offset + m.as_bytes().len()].copy_from_slice(m.as_bytes());
            offset += m.as_bytes().len();
            if !p.is_empty() {
                buf[offset..offset + p.len()].copy_from_slice(p);
            }
        }
        RaftMessage::InstallSnapshotResp(m) => {
            buf[RAFT_FRAME_HEADER_SIZE..].copy_from_slice(m.as_bytes());
        }
        RaftMessage::TimeoutNow(m) => {
            buf[RAFT_FRAME_HEADER_SIZE..].copy_from_slice(m.as_bytes());
        }
        RaftMessage::AppendEntriesSeeded {
            ae,
            headers,
            payloads,
        } => {
            let mut offset = RAFT_FRAME_HEADER_SIZE;
            buf[offset..offset + ae.as_bytes().len()].copy_from_slice(ae.as_bytes());
            offset += ae.as_bytes().len();
            if !headers.is_empty() {
                buf[offset..offset + headers.len()].copy_from_slice(headers);
                offset += headers.len();
            }
            if !payloads.is_empty() {
                buf[offset..offset + payloads.len()].copy_from_slice(payloads);
            }
        }
        RaftMessage::AppendEntriesSeededVectored {
            ae,
            headers,
            payloads,
        } => {
            let mut offset = RAFT_FRAME_HEADER_SIZE;
            buf[offset..offset + ae.as_bytes().len()].copy_from_slice(ae.as_bytes());
            offset += ae.as_bytes().len();
            if !headers.is_empty() {
                buf[offset..offset + headers.len()].copy_from_slice(headers);
                offset += headers.len();
            }
            for p in *payloads {
                if !p.is_empty() {
                    buf[offset..offset + p.len()].copy_from_slice(p);
                    offset += p.len();
                }
            }
        }
        RaftMessage::Custom(p) | RaftMessage::CustomResponse(p) => {
            if !p.is_empty() {
                buf[RAFT_FRAME_HEADER_SIZE..].copy_from_slice(p);
            }
        }
    }

    Ok(buf.freeze())
}
