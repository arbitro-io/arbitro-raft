use arbitro_raft::{
    decode_message, encode_message, validate_node_config, AppendEntries, BootstrapPeer, ClusterId,
    DispatchScope, DispatchSpec, EntryPayload, LimitsConfig, LogEntry, LogIndex, NodeConfig,
    PeerId, RaftCustomMessage, RaftMessage, RaftMessageView, Term, TimingConfig,
};
use std::net::{Ipv4Addr, SocketAddr};
use bytes::{BufMut, Bytes, BytesMut};

#[test]
fn node_config_validation_enforces_required_invariants() {
    let cfg = NodeConfig {
        cluster_id: ClusterId(7),
        node_id: PeerId(2),
        peers: vec![PeerId(1), PeerId(2), PeerId(3)],
        bootstrap_peers: vec![
            BootstrapPeer {
                id: PeerId(1),
                addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 9101)),
            },
            BootstrapPeer {
                id: PeerId(2),
                addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 9102)),
            },
            BootstrapPeer {
                id: PeerId(3),
                addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 9103)),
            },
        ],
        timing: TimingConfig::default(),
        limits: LimitsConfig::default(),
    };
    validate_node_config(&cfg).unwrap();

    let bad = NodeConfig {
        peers: vec![PeerId(1), PeerId(3)],
        ..cfg
    };
    assert!(validate_node_config(&bad).is_err());

    let bad_bootstrap = NodeConfig {
        bootstrap_peers: vec![BootstrapPeer {
            id: PeerId(1),
            addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 9101)),
        }],
        ..cfg
    };
    assert!(validate_node_config(&bad_bootstrap).is_err());
}

#[test]
fn protocol_message_roundtrip_is_stable() {
    let append = AppendEntries::new(
        Term(11),
        PeerId(1),
        LogIndex(9),
        Term(10),
        LogIndex(10),
        &[LogEntry {
            term: Term(11),
            index: LogIndex(10),
            payload: EntryPayload(bytes::Bytes::from_static(b"replicate-this")),
        }],
    )
    .unwrap();
    let msg = RaftMessage::AppendEntries(append);

    let encoded = encode_message(PeerId(9), &msg).unwrap();
    let decoded = decode_message(encoded).unwrap();
    assert_eq!(decoded.from, PeerId(9));
    assert_eq!(decoded.message, msg);
}

#[test]
fn append_entries_view_reads_fields_lazily_from_bytes() {
    let payload_a = bytes::Bytes::from_static(b"alpha");
    let payload_b = bytes::Bytes::from_static(b"beta");
    let append = AppendEntries::new(
        Term(11),
        PeerId(7),
        LogIndex(9),
        Term(10),
        LogIndex(10),
        &[
            LogEntry {
                term: Term(11),
                index: LogIndex(10),
                payload: EntryPayload(payload_a.clone()),
            },
            LogEntry {
                term: Term(11),
                index: LogIndex(11),
                payload: EntryPayload(payload_b.clone()),
            },
        ],
    )
    .unwrap();
    let msg = RaftMessage::AppendEntries(append);

    let encoded = encode_message(PeerId(3), &msg).unwrap();
    let inbound = RaftMessageView::parse(encoded).unwrap();
    assert_eq!(inbound.from, PeerId(3));

    match inbound.message {
        RaftMessageView::AppendEntries(view) => {
            assert_eq!(view.term(), Term(11));
            assert_eq!(view.leader_id(), PeerId(7));
            assert_eq!(view.prev_log_index(), LogIndex(9));
            assert_eq!(view.prev_log_term(), Term(10));
            assert_eq!(view.leader_commit(), LogIndex(10));
            assert_eq!(view.entry_count(), 2);

            let entries: Vec<_> = view.entries().unwrap().collect();
            assert_eq!(entries.len(), 2);
            assert_eq!(entries[0].term(), Term(11));
            assert_eq!(entries[0].index(), LogIndex(10));
            assert_eq!(entries[0].payload().as_ref(), payload_a.as_ref());
            assert_eq!(entries[1].term(), Term(11));
            assert_eq!(entries[1].index(), LogIndex(11));
            assert_eq!(entries[1].payload().as_ref(), payload_b.as_ref());
        }
        other => panic!("unexpected view variant: {other:?}"),
    }
}

#[test]
fn custom_message_roundtrip_keeps_lazy_command_view() {
    let spec = DispatchSpec::new(0x33, encode_empty, decode_empty, encode_empty, decode_empty)
        .with_scope(DispatchScope::Followers);
    let custom = spec.dispatch(()).build().unwrap().into_bytes();

    let msg = RaftMessage::Custom(RaftCustomMessage { bytes: custom.clone() });

    let encoded = encode_message(PeerId(7), &msg).unwrap();
    let decoded = decode_message(encoded.clone()).unwrap();
    assert_eq!(decoded.from, PeerId(7));
    assert_eq!(decoded.message, msg);

    let inbound = RaftMessageView::parse(encoded).unwrap();
    match inbound.message {
        RaftMessageView::Custom(view) => {
            assert_eq!(view.from(), PeerId(7));
            assert_eq!(view.command(), 0x33);
            assert_eq!(view.body(), &[] as &[u8]);
            assert_eq!(view.dispatch().scope(), DispatchScope::Followers);
        }
        other => panic!("unexpected view variant: {other:?}"),
    }
}

fn encode_empty(_: &()) -> Result<Bytes, arbitro_raft::RaftError> {
    let mut out = BytesMut::with_capacity(0);
    out.put_slice(&[]);
    Ok(out.freeze())
}

fn decode_empty(bytes: &[u8]) -> Result<(), arbitro_raft::RaftError> {
    if bytes.is_empty() {
        Ok(())
    } else {
        Err(arbitro_raft::RaftError::Dispatch(
            "expected empty payload".into(),
        ))
    }
}
