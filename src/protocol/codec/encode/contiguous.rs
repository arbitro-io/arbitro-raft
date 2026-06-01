use crate::{PeerId, RaftError, RaftMessage};
use crate::protocol::codec::wire::{
    AppendEntries, EntryHeader, RaftFrameHeader, RAFT_FRAME_HEADER_SIZE,
    RAFT_MAGIC, RAFT_VERSION,
};
use super::{kind_of, body_total_len};
use zerocopy::{IntoBytes, Ref};

/// Encodes a message into a single contiguous `Bytes` object.
/// Useful for parallel fan-out (sharing the same frame across multiple sends).
pub fn encode_message_to_bytes(from: PeerId, msg: &RaftMessage) -> Result<bytes::Bytes, RaftError> {
    let body_len = body_total_len(msg);
    let total_len = RAFT_FRAME_HEADER_SIZE + body_len;

    let mut buf = bytes::BytesMut::with_capacity(total_len);
    // Safety: we are about to fill the exact total_len bytes.
    unsafe { buf.set_len(total_len) };

    // 1. Frame Header
    {
        let (h_bytes, _) = buf.split_at_mut(RAFT_FRAME_HEADER_SIZE);
        let (h_ref, _) = Ref::<_, RaftFrameHeader>::from_prefix(h_bytes)
            .map_err(|_| RaftError::Protocol("alignment".into()))?;
        let header = Ref::into_mut(h_ref);
        header.magic.set(RAFT_MAGIC);
        header.version = RAFT_VERSION;
        header.kind = kind_of(msg);
        header.from.set(from.0);
        header.body_len.set(body_len as u32);
        header._pad.set(0);
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
                    payload_len: (e.payload.0.len() as u32).into(),
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
