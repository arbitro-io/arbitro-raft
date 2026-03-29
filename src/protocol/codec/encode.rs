use bytes::{Bytes, BytesMut};
use zerocopy::byteorder::little_endian::{U16, U32, U64};
use zerocopy::IntoBytes;

use crate::{PeerId, RaftError, RaftMessage};

use super::wire::{
    AppendEntriesRespBody, InstallSnapshotBody, InstallSnapshotRespBody, RaftFrameHeader,
    RequestVoteBody, RequestVoteRespBody, RAFT_FRAME_HEADER_SIZE, RAFT_MAGIC, RAFT_VERSION,
    KIND_APPEND_ENTRIES, KIND_APPEND_ENTRIES_RESP, KIND_CUSTOM, KIND_CUSTOM_RESPONSE,
    KIND_INSTALL_SNAPSHOT, KIND_INSTALL_SNAPSHOT_RESP, KIND_REQUEST_VOTE, KIND_REQUEST_VOTE_RESP,
};

pub fn encode_message(from: PeerId, msg: &RaftMessage) -> Result<Bytes, RaftError> {
    let body_len = body_len(msg)?;
    let mut frame = BytesMut::with_capacity(RAFT_FRAME_HEADER_SIZE + body_len);
    encode_message_into(from, msg, &mut frame)?;
    Ok(frame.freeze())
}

pub fn encode_message_into(
    from: PeerId,
    msg:  &RaftMessage,
    buf:  &mut BytesMut,
) -> Result<(), RaftError> {
    let body_len = body_len(msg)?;
    buf.reserve(RAFT_FRAME_HEADER_SIZE + body_len);
    let header = RaftFrameHeader {
        magic:    U32::new(RAFT_MAGIC),
        version:  RAFT_VERSION,
        kind:     kind_of(msg),
        flags:    U16::new(0),
        from:     U64::new(from.0),
        body_len: U32::new(body_len as u32),
        reserved: U32::new(0),
    };
    buf.extend_from_slice(header.as_bytes());

    match msg {
        RaftMessage::RequestVote(msg) => {
            let body = RequestVoteBody {
                term:           U64::new(msg.term.0),
                candidate_id:   U64::new(msg.candidate_id.0),
                last_log_index: U64::new(msg.last_log_index.0),
                last_log_term:  U64::new(msg.last_log_term.0),
            };
            buf.extend_from_slice(body.as_bytes());
        }
        RaftMessage::RequestVoteResp(msg) => {
            let body = RequestVoteRespBody {
                term:        U64::new(msg.term.0),
                vote_granted: u8::from(msg.vote_granted),
                _pad:        [0; 7],
            };
            buf.extend_from_slice(body.as_bytes());
        }
        RaftMessage::AppendEntries(msg) => {
            buf.extend_from_slice(msg.bytes().as_ref());
        }
        RaftMessage::AppendEntriesResp(msg) => {
            let body = AppendEntriesRespBody {
                term:        U64::new(msg.term.0),
                match_index: U64::new(msg.match_index.0),
                success:     u8::from(msg.success),
                _pad:        [0; 7],
            };
            buf.extend_from_slice(body.as_bytes());
        }
        RaftMessage::InstallSnapshot(msg) => {
            let body = InstallSnapshotBody {
                term:                U64::new(msg.term.0),
                leader_id:           U64::new(msg.leader_id.0),
                last_included_index: U64::new(msg.meta.last_included_index.0),
                last_included_term:  U64::new(msg.meta.last_included_term.0),
                offset:              U64::new(msg.chunk.offset),
                chunk_len:           U32::new(msg.chunk.bytes.len() as u32),
                done:                u8::from(msg.chunk.done),
                _pad:                [0; 3],
            };
            buf.extend_from_slice(body.as_bytes());
            buf.extend_from_slice(msg.chunk.bytes.as_ref());
        }
        RaftMessage::InstallSnapshotResp(msg) => {
            let body = InstallSnapshotRespBody {
                term:        U64::new(msg.term.0),
                next_offset: U64::new(msg.next_offset),
                accepted:    u8::from(msg.accepted),
                _pad:        [0; 7],
            };
            buf.extend_from_slice(body.as_bytes());
        }
        RaftMessage::Custom(msg)         => { buf.extend_from_slice(msg.bytes.as_ref()); }
        RaftMessage::CustomResponse(msg) => { buf.extend_from_slice(msg.bytes.as_ref()); }
    }

    if super::trace_enabled() {
        super::trace_log(from, format!("encode kind={} body_len={}", kind_of(msg), body_len));
    }
    Ok(())
}

fn kind_of(msg: &RaftMessage) -> u8 {
    match msg {
        RaftMessage::RequestVote(_)         => KIND_REQUEST_VOTE,
        RaftMessage::RequestVoteResp(_)     => KIND_REQUEST_VOTE_RESP,
        RaftMessage::AppendEntries(_)       => KIND_APPEND_ENTRIES,
        RaftMessage::AppendEntriesResp(_)   => KIND_APPEND_ENTRIES_RESP,
        RaftMessage::InstallSnapshot(_)     => KIND_INSTALL_SNAPSHOT,
        RaftMessage::InstallSnapshotResp(_) => KIND_INSTALL_SNAPSHOT_RESP,
        RaftMessage::Custom(_)              => KIND_CUSTOM,
        RaftMessage::CustomResponse(_)      => KIND_CUSTOM_RESPONSE,
    }
}

fn body_len(msg: &RaftMessage) -> Result<usize, RaftError> {
    let len = match msg {
        RaftMessage::RequestVote(_)         => std::mem::size_of::<RequestVoteBody>(),
        RaftMessage::RequestVoteResp(_)     => std::mem::size_of::<RequestVoteRespBody>(),
        RaftMessage::AppendEntries(msg)     => msg.bytes().len(),
        RaftMessage::AppendEntriesResp(_)   => std::mem::size_of::<AppendEntriesRespBody>(),
        RaftMessage::InstallSnapshot(msg)   => std::mem::size_of::<InstallSnapshotBody>()
            .checked_add(msg.chunk.bytes.len())
            .ok_or_else(|| RaftError::Protocol("install snapshot body overflow".into()))?,
        RaftMessage::InstallSnapshotResp(_) => std::mem::size_of::<InstallSnapshotRespBody>(),
        RaftMessage::Custom(msg)            => msg.bytes.len(),
        RaftMessage::CustomResponse(msg)    => msg.bytes.len(),
    };
    Ok(len)
}
