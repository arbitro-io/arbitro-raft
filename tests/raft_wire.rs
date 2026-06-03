/// Zero-copy wire protocol validation and roundtrip tests.
use arbitro_raft::{
    decode_message, encode_message_to_bytes, encode_message_vectored, AppendEntries, EntryPayload,
    LogEntry, LogIndex, PeerId, RaftMessage, Term,
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
