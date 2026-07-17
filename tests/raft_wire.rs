/// Zero-copy wire protocol validation and roundtrip tests.
use arbitro_raft::{
    decode_message, encode_message_to_bytes, encode_message_vectored, AppendEntries, EntryPayload,
    LogEntry, LogIndex, PeerId, RaftMessage, Term, TimeoutNow,
};

#[test]
fn invariant_wire_encode_decode_roundtrip_is_lossless() {
    let p1 = b"a";
    let p2 = b"bb";

    let entries = vec![
        LogEntry {
            term: Term(3),
            index: LogIndex(1),
            payload: EntryPayload(p1),
        },
        LogEntry {
            term: Term(3),
            index: LogIndex(2),
            payload: EntryPayload(p2),
        },
    ];

    let ae = AppendEntries {
        term: Term(3).0.into(),
        leader_id: PeerId(1).0.into(),
        prev_log_index: 0.into(),
        prev_log_term: 0.into(),
        leader_commit: 0.into(),
        entry_count: (entries.len() as u32).into(),
        _pad: 0.into(),
    };

    let msg = RaftMessage::AppendEntriesVectored(&ae, &entries);

    let mut header_buf = [0u8; 128];
    let mut vectors = Vec::new();
    encode_message_vectored(PeerId(1), &msg, &mut header_buf, &mut vectors).unwrap();

    let mut frame = Vec::new();
    for v in vectors {
        frame.extend_from_slice(v);
    }

    let inbound = decode_message(&frame).unwrap();

    assert_eq!(inbound.from, PeerId(1));
    let ae_view = inbound.as_append_entries().unwrap();
    assert_eq!(ae_view.term(), Term(3));
    assert_eq!(ae_view.leader_id(), PeerId(1));
    assert_eq!(ae_view.entry_count(), 2);

    let decoded: Vec<_> = ae_view.entries().unwrap().collect();
    assert_eq!(decoded[0].index, LogIndex(1));
    assert_eq!(decoded[1].payload.0, b"bb");
}

#[test]
fn invariant_wire_encode_to_bytes_roundtrip_is_lossless() {
    let p1 = b"a";
    let p2 = b"bb";

    let entries = vec![
        LogEntry {
            term: Term(3),
            index: LogIndex(1),
            payload: EntryPayload(p1),
        },
        LogEntry {
            term: Term(3),
            index: LogIndex(2),
            payload: EntryPayload(p2),
        },
    ];

    let ae = AppendEntries {
        term: Term(3).0.into(),
        leader_id: PeerId(1).0.into(),
        prev_log_index: 0.into(),
        prev_log_term: 0.into(),
        leader_commit: 0.into(),
        entry_count: (entries.len() as u32).into(),
        _pad: 0.into(),
    };

    let msg = RaftMessage::AppendEntriesVectored(&ae, &entries);

    // Test the Bytes-based encoder
    let frame = encode_message_to_bytes(PeerId(1), &msg).unwrap();
    let inbound = decode_message(&frame).unwrap();

    assert_eq!(inbound.from, PeerId(1));
    let ae_view = inbound.as_append_entries().unwrap();
    assert_eq!(ae_view.term(), Term(3));
    assert_eq!(ae_view.leader_id(), PeerId(1));
    assert_eq!(ae_view.entry_count(), 2);

    let decoded: Vec<_> = ae_view.entries().unwrap().collect();
    assert_eq!(decoded[0].term, Term(3));
    assert_eq!(decoded[0].index, LogIndex(1));
    assert_eq!(decoded[0].payload.0, b"a");
    assert_eq!(decoded[1].index, LogIndex(2));
    assert_eq!(decoded[1].payload.0, b"bb");
}

#[test]
fn invariant_timeout_now_roundtrip_contiguous_is_lossless() {
    let msg_wire = TimeoutNow {
        term: 42u64.into(),
        leader_id: 7u64.into(),
    };
    let msg = RaftMessage::TimeoutNow(&msg_wire);

    let frame = encode_message_to_bytes(PeerId(7), &msg).unwrap();
    let inbound = decode_message(&frame).unwrap();

    assert_eq!(inbound.from, PeerId(7));
    let decoded = inbound
        .as_timeout_now()
        .expect("decoded frame must be TimeoutNow");
    assert_eq!(decoded.term.get(), 42);
    assert_eq!(decoded.leader_id.get(), 7);
}

#[test]
fn invariant_timeout_now_roundtrip_vectored_is_lossless() {
    let msg_wire = TimeoutNow {
        term: u64::MAX.into(),
        leader_id: 3u64.into(),
    };
    let msg = RaftMessage::TimeoutNow(&msg_wire);

    let mut header_buf = [0u8; 128];
    let mut vectors = Vec::new();
    encode_message_vectored(PeerId(3), &msg, &mut header_buf, &mut vectors).unwrap();

    let mut frame = Vec::new();
    for v in vectors {
        frame.extend_from_slice(v);
    }

    let inbound = decode_message(&frame).unwrap();
    assert_eq!(inbound.from, PeerId(3));
    let decoded = inbound
        .as_timeout_now()
        .expect("decoded frame must be TimeoutNow");
    assert_eq!(decoded.term.get(), u64::MAX);
    assert_eq!(decoded.leader_id.get(), 3);
}

#[test]
fn invariant_timeout_now_trailing_bytes_rejected() {
    let msg_wire = TimeoutNow {
        term: 1u64.into(),
        leader_id: 1u64.into(),
    };
    let frame = encode_message_to_bytes(PeerId(1), &RaftMessage::TimeoutNow(&msg_wire)).unwrap();
    let mut oversized = frame.to_vec();
    oversized.push(0xFF);
    // body_len no longer matches → frame-length mismatch must reject.
    assert!(decode_message(&oversized).is_err());
}

#[test]
fn invariant_corrupt_magic_is_rejected_not_dropped() {
    let mut garbage = vec![0u8; 32];
    // Write wrong magic
    garbage[0] = 0xDE;
    garbage[1] = 0xAD;
    garbage[2] = 0xBE;
    garbage[3] = 0xEF;
    let result = decode_message(&garbage);
    assert!(
        result.is_err(),
        "corrupt frame must return an error, never Ok"
    );
}

// ---------------------------------------------------------------------------
// B6 (P15): a body whose length exceeds u32::MAX must FAIL the encode instead
// of silently truncating `body_len` on the wire (frame corruption).
//
// 4097 entries all aliasing the same 1 MiB payload buffer add up to a logical
// body of ~4.001 GiB (> u32::MAX) while only 1 MiB of real memory is used.
// ---------------------------------------------------------------------------

#[test]
fn oversized_body_rejected_not_truncated() {
    use arbitro_raft::RaftError;

    let payload = vec![0u8; 1024 * 1024];
    let entries: Vec<LogEntry> = (0..4097u64)
        .map(|i| LogEntry {
            term: Term(1),
            index: LogIndex(i + 1),
            payload: EntryPayload(&payload),
        })
        .collect();

    let ae = AppendEntries {
        term: Term(1).0.into(),
        leader_id: PeerId(1).0.into(),
        prev_log_index: 0.into(),
        prev_log_term: 0.into(),
        leader_commit: 0.into(),
        entry_count: (entries.len() as u32).into(),
        _pad: 0.into(),
    };
    let msg = RaftMessage::AppendEntriesVectored(&ae, &entries);

    // Contiguous path: the checked conversion must reject BEFORE attempting
    // a > 4 GiB allocation for the frame.
    assert!(
        matches!(
            encode_message_to_bytes(PeerId(1), &msg),
            Err(RaftError::Protocol(_))
        ),
        "contiguous encode must reject a >4 GiB body with a Protocol error"
    );

    // Vectored path: the header buffer is large enough for all entry headers,
    // so the only legitimate failure is the checked body-length conversion.
    let mut header_buf = vec![0u8; 256 * 1024];
    let mut vectors = Vec::new();
    match encode_message_vectored(PeerId(1), &msg, &mut header_buf, &mut vectors) {
        Err(RaftError::Protocol(_)) => {}
        other => panic!("expected Protocol error for >4 GiB body, got {other:?}"),
    }
}
