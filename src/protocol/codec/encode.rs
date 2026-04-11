use super::wire::{
    AppendEntries, AppendEntriesResp, EntryHeader, InstallSnapshot, InstallSnapshotResp,
    RaftFrameHeader, RequestVote, RequestVoteResp, KIND_APPEND_ENTRIES, KIND_APPEND_ENTRIES_RESP,
    KIND_CUSTOM, KIND_CUSTOM_RESPONSE, KIND_INSTALL_SNAPSHOT, KIND_INSTALL_SNAPSHOT_RESP,
    KIND_REQUEST_VOTE, KIND_REQUEST_VOTE_RESP, RAFT_FRAME_HEADER_SIZE, RAFT_MAGIC, RAFT_VERSION,
};
use crate::{LogEntry, LogIndex, PeerId, RaftError, RaftMessage, Term};
use zerocopy::{IntoBytes, Ref};

/// Populates `out_vectored` with references to headers written in `header_buf`
/// and original payloads from `msg`. ELIMINATES ALL PAYLOAD COPIES.
pub fn encode_message_vectored<'a>(
    from: PeerId,
    msg: &RaftMessage<'a>,
    header_buf: &'a mut [u8],
    out_vectored: &mut Vec<&'a [u8]>,
) -> Result<(), RaftError> {
    out_vectored.clear();

    let body_len = body_total_len(msg);
    let st_len = body_struct_len(msg);
    let total_header_len = RAFT_FRAME_HEADER_SIZE + st_len;

    if header_buf.len() < total_header_len {
        return Err(RaftError::Protocol("header buffer too small".into()));
    }

    // 1. Write headers first (scoping the mutable borrow)
    {
        let (h_bytes, body_bytes) = header_buf.split_at_mut(RAFT_FRAME_HEADER_SIZE);

        // Frame Header
        let (h_ref, _) = Ref::<_, RaftFrameHeader>::from_prefix(h_bytes)
            .map_err(|_| RaftError::Protocol("alignment".into()))?;
        let header = Ref::into_mut(h_ref);
        header.magic.set(RAFT_MAGIC);
        header.version = RAFT_VERSION;
        header.kind = kind_of(msg);
        header.flags.set(0);
        header.from.set(from.0);
        header.body_len.set(body_len as u32);
        header.reserved.set(0);
        header._pad.set(0);

        // Body Struct (if any)
        if st_len > 0 {
            match msg {
                RaftMessage::RequestVote(m) => body_bytes[..st_len].copy_from_slice(m.as_bytes()),
                RaftMessage::RequestVoteResp(m) => {
                    body_bytes[..st_len].copy_from_slice(m.as_bytes())
                }
                RaftMessage::AppendEntries(m, _) => {
                    body_bytes[..st_len].copy_from_slice(m.as_bytes())
                }
                RaftMessage::AppendEntriesResp(m) => {
                    body_bytes[..st_len].copy_from_slice(m.as_bytes())
                }
                RaftMessage::InstallSnapshot(m, _) => {
                    body_bytes[..st_len].copy_from_slice(m.as_bytes())
                }
                RaftMessage::InstallSnapshotResp(m) => {
                    body_bytes[..st_len].copy_from_slice(m.as_bytes())
                }
                _ => {}
            }
        }
    }

    // 2. Collection (Immutable phase)
    match msg {
        RaftMessage::AppendEntriesVectored(m, entries) => {
            return encode_append_entries_vectored(
                from,
                Term(m.term.get()),
                PeerId(m.leader_id.get()),
                LogIndex(m.prev_log_index.get()),
                Term(m.prev_log_term.get()),
                LogIndex(m.leader_commit.get()),
                entries,
                header_buf,
                out_vectored,
            );
        }
        RaftMessage::Custom(payload) | RaftMessage::CustomResponse(payload) => {
            out_vectored.push(&header_buf[..RAFT_FRAME_HEADER_SIZE]);
            if !payload.is_empty() {
                out_vectored.push(payload);
            }
        }
        RaftMessage::AppendEntries(_, payload) | RaftMessage::InstallSnapshot(_, payload) => {
            out_vectored.push(&header_buf[..total_header_len]);
            if !payload.is_empty() {
                out_vectored.push(payload);
            }
        }
        _ => {
            out_vectored.push(&header_buf[..total_header_len]);
        }
    }

    Ok(())
}

/// Specialized vectored encoder for batches. Interleaves Entry headers (written to header_buf)
/// and their respective payloads (from entries).
pub fn encode_append_entries_vectored<'a>(
    from: PeerId,
    term: Term,
    leader_id: PeerId,
    prev_log_index: LogIndex,
    prev_log_term: Term,
    leader_commit: LogIndex,
    entries: &'a [LogEntry<'a>],
    header_buf: &'a mut [u8],
    out_vectored: &mut Vec<&'a [u8]>,
) -> Result<(), RaftError> {
    out_vectored.clear();

    let mut body_payload_len = 0;
    for e in entries {
        body_payload_len += std::mem::size_of::<EntryHeader>() + e.payload.0.len();
    }
    let body_wire_len = std::mem::size_of::<AppendEntries>() + body_payload_len;

    let total_headers_needed = RAFT_FRAME_HEADER_SIZE
        + std::mem::size_of::<AppendEntries>()
        + (entries.len() * std::mem::size_of::<EntryHeader>());

    if header_buf.len() < total_headers_needed {
        return Err(RaftError::Protocol(
            "header buffer too small for batch".into(),
        ));
    }

    let header_buf_ptr = header_buf.as_mut_ptr();

    // 1. Frame Header & AppendEntries metadata (Write phase)
    unsafe {
        // Frame Header
        let h_bytes = std::slice::from_raw_parts_mut(header_buf_ptr, RAFT_FRAME_HEADER_SIZE);
        let (h_ref, _) = Ref::<_, RaftFrameHeader>::from_prefix(h_bytes).unwrap();
        let header = Ref::into_mut(h_ref);
        header.magic.set(RAFT_MAGIC);
        header.version = RAFT_VERSION;
        header.kind = KIND_APPEND_ENTRIES;
        header.from.set(from.0);
        header.body_len.set(body_wire_len as u32);
        header._pad.set(0);

        // AppendEntries Body
        let ae_ptr = header_buf_ptr.add(RAFT_FRAME_HEADER_SIZE);
        let ae_bytes = std::slice::from_raw_parts_mut(ae_ptr, std::mem::size_of::<AppendEntries>());
        let (ae_ref, _) = Ref::<_, AppendEntries>::from_prefix(ae_bytes).unwrap();
        let body = Ref::into_mut(ae_ref);
        body.term.set(term.0);
        body.leader_id.set(leader_id.0);
        body.prev_log_index.set(prev_log_index.0);
        body.prev_log_term.set(prev_log_term.0);
        body.leader_commit.set(leader_commit.0);
        body.entry_count.set(entries.len() as u32);
        body._pad.set(0);
    }

    // 2. Collection phase
    let first_chunk_len = RAFT_FRAME_HEADER_SIZE + std::mem::size_of::<AppendEntries>();
    // SAFETY: Life is 'a because data is in header_buf. No mutation happens to this part anymore.
    out_vectored.push(unsafe { std::slice::from_raw_parts(header_buf_ptr, first_chunk_len) });

    let mut header_offset = first_chunk_len;

    // 3. Interleave Entries
    for e in entries {
        let eh_len = std::mem::size_of::<EntryHeader>();

        // Write EntryHeader
        unsafe {
            let eh_ptr = header_buf_ptr.add(header_offset);
            let eh_bytes = std::slice::from_raw_parts_mut(eh_ptr, eh_len);
            let (eh_ref, _) = Ref::<_, EntryHeader>::from_prefix(eh_bytes).unwrap();
            let eh = Ref::into_mut(eh_ref);
            eh.term.set(e.term.0);
            eh.index.set(e.index.0);
            eh.payload_len.set(e.payload.0.len() as u32);
            eh._pad.set(0);

            // Push Header Slice
            out_vectored.push(std::slice::from_raw_parts(eh_ptr, eh_len));
        }
        header_offset += eh_len;

        // Push Original Payload Slice
        if !e.payload.0.is_empty() {
            out_vectored.push(e.payload.0);
        }
    }

    Ok(())
}

fn kind_of(msg: &RaftMessage) -> u8 {
    match msg {
        RaftMessage::RequestVote(_) => KIND_REQUEST_VOTE,
        RaftMessage::RequestVoteResp(_) => KIND_REQUEST_VOTE_RESP,
        RaftMessage::AppendEntries(_, _) => KIND_APPEND_ENTRIES,
        RaftMessage::AppendEntriesVectored(_, _) => KIND_APPEND_ENTRIES,
        RaftMessage::AppendEntriesResp(_) => KIND_APPEND_ENTRIES_RESP,
        RaftMessage::InstallSnapshot(_, _) => KIND_INSTALL_SNAPSHOT,
        RaftMessage::InstallSnapshotResp(_) => KIND_INSTALL_SNAPSHOT_RESP,
        RaftMessage::Custom(_) => KIND_CUSTOM,
        RaftMessage::CustomResponse(_) => KIND_CUSTOM_RESPONSE,
    }
}

fn body_struct_len(msg: &RaftMessage) -> usize {
    match msg {
        RaftMessage::RequestVote(_) => std::mem::size_of::<RequestVote>(),
        RaftMessage::RequestVoteResp(_) => std::mem::size_of::<RequestVoteResp>(),
        RaftMessage::AppendEntries(_, _) => std::mem::size_of::<AppendEntries>(),
        RaftMessage::AppendEntriesVectored(_, _) => std::mem::size_of::<AppendEntries>(),
        RaftMessage::AppendEntriesResp(_) => std::mem::size_of::<AppendEntriesResp>(),
        RaftMessage::InstallSnapshot(_, _) => std::mem::size_of::<InstallSnapshot>(),
        RaftMessage::InstallSnapshotResp(_) => std::mem::size_of::<InstallSnapshotResp>(),
        RaftMessage::Custom(_) => 0,
        RaftMessage::CustomResponse(_) => 0,
    }
}

fn body_total_len(msg: &RaftMessage) -> usize {
    match msg {
        RaftMessage::RequestVote(m) => m.as_bytes().len(),
        RaftMessage::RequestVoteResp(m) => m.as_bytes().len(),
        RaftMessage::AppendEntries(m, p) => m.as_bytes().len() + p.len(),
        RaftMessage::AppendEntriesVectored(m, entries) => {
            let mut len = std::mem::size_of::<AppendEntries>();
            for e in *entries {
                len += std::mem::size_of::<EntryHeader>() + e.payload.0.len();
            }
            len
        }
        RaftMessage::AppendEntriesResp(m) => m.as_bytes().len(),
        RaftMessage::InstallSnapshot(m, p) => m.as_bytes().len() + p.len(),
        RaftMessage::InstallSnapshotResp(m) => m.as_bytes().len(),
        RaftMessage::Custom(p) => p.len(),
        RaftMessage::CustomResponse(p) => p.len(),
    }
}
