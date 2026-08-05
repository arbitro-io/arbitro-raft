use super::{body_total_len, kind_of, wire_len_u32};
use crate::protocol::codec::wire::{
    AppendEntries, EntryHeader, RaftFrameHeader, KIND_APPEND_ENTRIES, RAFT_FRAME_HEADER_SIZE,
    RAFT_MAGIC, RAFT_VERSION,
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
    encode_message_vectored_impl(from, 0, msg, header_buf, out_vectored)
}

/// Same as [`encode_message_vectored`] but stamps `group_id` into the frame
/// header, allowing a single transport to multiplex frames for many Raft
/// groups. Use `GroupId(0)` for the default single-group behavior.
pub fn encode_message_vectored_with_group<'a>(
    from: PeerId,
    group_id: crate::GroupId,
    msg: &RaftMessage<'a>,
    header_buf: &'a mut [u8],
    out_vectored: &mut Vec<&'a [u8]>,
) -> Result<(), RaftError> {
    encode_message_vectored_impl(from, group_id.0, msg, header_buf, out_vectored)
}

fn encode_message_vectored_impl<'a>(
    from: PeerId,
    group_id: u64,
    msg: &RaftMessage<'a>,
    header_buf: &'a mut [u8],
    out_vectored: &mut Vec<&'a [u8]>,
) -> Result<(), RaftError> {
    if let RaftMessage::AppendEntriesVectored(m, entries) = msg {
        return encode_append_entries_vectored_with_pad(
            from,
            group_id,
            Term(m.term.get()),
            PeerId(m.leader_id.get()),
            LogIndex(m.prev_log_index.get()),
            Term(m.prev_log_term.get()),
            LogIndex(m.leader_commit.get()),
            // A11: `_pad` carries the ReadIndex probe token — it must survive
            // the scalar re-build, or heartbeat acks could never be
            // correlated to a confirmation round.
            m._pad.get(),
            entries,
            header_buf,
            out_vectored,
        );
    }

    out_vectored.clear();

    let body_len = body_total_len(msg);
    // B6: checked — a > 4 GiB body must fail the encode, never truncate.
    let body_len_u32 = wire_len_u32(body_len, "frame body length")?;
    let total_header_len = RAFT_FRAME_HEADER_SIZE;

    if header_buf.len() < total_header_len {
        return Err(RaftError::Protocol("header buffer too small".into()));
    }

    // 1. Write Frame Header ONLY (Cero copias de cuerpos de structs)
    {
        let (h_bytes, _) = header_buf.split_at_mut(RAFT_FRAME_HEADER_SIZE);

        // Frame Header
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
        header.group_id.set(group_id);
    }

    match msg {
        RaftMessage::AppendEntriesVectored(..) => unreachable!("handled above"),
        RaftMessage::RequestVote(m) => {
            out_vectored.push(&header_buf[..RAFT_FRAME_HEADER_SIZE]);
            out_vectored.push(m.as_bytes());
        }
        RaftMessage::RequestVoteResp(m) => {
            out_vectored.push(&header_buf[..RAFT_FRAME_HEADER_SIZE]);
            out_vectored.push(m.as_bytes());
        }
        RaftMessage::PreVote(m) => {
            out_vectored.push(&header_buf[..RAFT_FRAME_HEADER_SIZE]);
            out_vectored.push(m.as_bytes());
        }
        RaftMessage::PreVoteResp(m) => {
            out_vectored.push(&header_buf[..RAFT_FRAME_HEADER_SIZE]);
            out_vectored.push(m.as_bytes());
        }
        RaftMessage::AppendEntries(m, p) => {
            out_vectored.push(&header_buf[..RAFT_FRAME_HEADER_SIZE]);
            out_vectored.push(m.as_bytes());
            if !p.is_empty() {
                out_vectored.push(p);
            }
        }
        RaftMessage::AppendEntriesResp(m) => {
            out_vectored.push(&header_buf[..RAFT_FRAME_HEADER_SIZE]);
            out_vectored.push(m.as_bytes());
        }
        RaftMessage::AppendEntriesSeeded {
            ae,
            headers,
            payloads,
        } => {
            out_vectored.push(&header_buf[..RAFT_FRAME_HEADER_SIZE]);
            out_vectored.push(ae.as_bytes());
            if !headers.is_empty() {
                out_vectored.push(headers);
            }
            if !payloads.is_empty() {
                out_vectored.push(payloads);
            }
        }
        RaftMessage::AppendEntriesSeededVectored {
            ae,
            headers,
            payloads,
        } => {
            out_vectored.reserve(payloads.len() + 3);
            out_vectored.push(&header_buf[..RAFT_FRAME_HEADER_SIZE]);
            out_vectored.push(ae.as_bytes());
            if !headers.is_empty() {
                out_vectored.push(headers);
            }
            for p in *payloads {
                if !p.is_empty() {
                    out_vectored.push(p);
                }
            }
        }
        RaftMessage::InstallSnapshot(m, p) => {
            out_vectored.push(&header_buf[..RAFT_FRAME_HEADER_SIZE]);
            out_vectored.push(m.as_bytes());
            if !p.is_empty() {
                out_vectored.push(p);
            }
        }
        RaftMessage::InstallSnapshotResp(m) => {
            out_vectored.push(&header_buf[..RAFT_FRAME_HEADER_SIZE]);
            out_vectored.push(m.as_bytes());
        }
        RaftMessage::TimeoutNow(m) => {
            out_vectored.push(&header_buf[..RAFT_FRAME_HEADER_SIZE]);
            out_vectored.push(m.as_bytes());
        }
        RaftMessage::Custom(p) | RaftMessage::CustomResponse(p) => {
            out_vectored.push(&header_buf[..RAFT_FRAME_HEADER_SIZE]);
            if !p.is_empty() {
                out_vectored.push(p);
            }
        }
    }

    Ok(())
}

/// Specialized vectored encoder for batches. Interleaves Entry headers (written to header_buf)
/// and their respective payloads (from entries).
///
/// The `AppendEntries._pad` reserved word is encoded as `0`; callers that need
/// to carry the A11 ReadIndex probe token go through
/// [`encode_message_vectored`] with a populated `AppendEntriesVectored` body
/// (or [`encode_append_entries_vectored_with_pad`] internally).
#[allow(clippy::too_many_arguments)]
pub fn encode_append_entries_vectored<'a>(
    from: PeerId,
    group_id: u64,
    term: Term,
    leader_id: PeerId,
    prev_log_index: LogIndex,
    prev_log_term: Term,
    leader_commit: LogIndex,
    entries: &'a [LogEntry<'a>],
    header_buf: &'a mut [u8],
    out_vectored: &mut Vec<&'a [u8]>,
) -> Result<(), RaftError> {
    encode_append_entries_vectored_with_pad(
        from,
        group_id,
        term,
        leader_id,
        prev_log_index,
        prev_log_term,
        leader_commit,
        0,
        entries,
        header_buf,
        out_vectored,
    )
}

/// [`encode_append_entries_vectored`] with an explicit value for the
/// `AppendEntries._pad` reserved word (A11: the ReadIndex probe token that
/// followers echo back in `AppendEntriesResp._pad[0..4]`).
// B13: the three `Ref::from_prefix(..).unwrap()`s below operate on slices
// whose lengths were computed from the very struct sizes being taken
// (`total_headers_needed` is checked before the writes), so they cannot fail.
#[allow(clippy::too_many_arguments, clippy::unwrap_used)]
pub(crate) fn encode_append_entries_vectored_with_pad<'a>(
    from: PeerId,
    group_id: u64,
    term: Term,
    leader_id: PeerId,
    prev_log_index: LogIndex,
    prev_log_term: Term,
    leader_commit: LogIndex,
    pad: u32,
    entries: &'a [LogEntry<'a>],
    header_buf: &'a mut [u8],
    out_vectored: &mut Vec<&'a [u8]>,
) -> Result<(), RaftError> {
    out_vectored.clear();
    out_vectored.reserve(2 * entries.len() + 1);

    let mut body_payload_len = 0;
    for e in entries {
        body_payload_len += std::mem::size_of::<EntryHeader>() + e.payload.0.len();
    }
    let body_wire_len = std::mem::size_of::<AppendEntries>() + body_payload_len;
    // B6: checked — a > 4 GiB batch body must fail the encode, never truncate.
    let body_wire_len_u32 = wire_len_u32(body_wire_len, "frame body length")?;
    let entry_count_u32 = wire_len_u32(entries.len(), "entry count")?;

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
        header.flags.set(0);
        header.from.set(from.0);
        header.body_len.set(body_wire_len_u32);
        header.reserved.set(0);
        header.group_id.set(group_id);

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
        body.entry_count.set(entry_count_u32);
        body._pad.set(pad);
    }

    // 2. Collection phase
    let first_chunk_len = RAFT_FRAME_HEADER_SIZE + std::mem::size_of::<AppendEntries>();
    // SAFETY: Life is 'a because data is in header_buf. No mutation happens to this part anymore.
    out_vectored.push(unsafe { std::slice::from_raw_parts(header_buf_ptr, first_chunk_len) });

    let mut header_offset = first_chunk_len;

    // 3. Interleave Entries
    for e in entries {
        let eh_len = std::mem::size_of::<EntryHeader>();
        // B6: checked — never truncate a payload length.
        let payload_len_u32 = wire_len_u32(e.payload.0.len(), "entry payload length")?;

        // Write EntryHeader
        unsafe {
            let eh_ptr = header_buf_ptr.add(header_offset);
            let eh_bytes = std::slice::from_raw_parts_mut(eh_ptr, eh_len);
            let (eh_ref, _) = Ref::<_, EntryHeader>::from_prefix(eh_bytes).unwrap();
            let eh = Ref::into_mut(eh_ref);
            eh.term.set(e.term.0);
            eh.index.set(e.index.0);
            eh.payload_len.set(payload_len_u32);
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
