/// Raft correctness invariant tests.
///
/// These tests verify that the implementation upholds the safety and liveness
/// invariants stated in the Raft paper (§5) and the arbitro-raft guide.
/// No network is involved — all state transitions are exercised in-process.
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::StreamExt;

use arbitro_raft::{
    decode_message, validate_node_config, BootstrapPeer, ClusterId, EntryPayload, HardState,
    LimitsConfig, LogEntry, LogIndex, NodeConfig, PeerId, RaftError, RaftMessage, RaftStorage,
    RaftTransport, SnapshotMeta, Term, TimingConfig,
};

// ---------------------------------------------------------------------------
// Minimal in-process transport for correctness tests
// ---------------------------------------------------------------------------

struct TestTransport {
    tx: futures::channel::mpsc::UnboundedSender<Vec<u8>>,
    rx: Arc<tokio::sync::Mutex<futures::channel::mpsc::UnboundedReceiver<Vec<u8>>>>,
}

impl TestTransport {
    fn new() -> (Self, futures::channel::mpsc::UnboundedReceiver<Vec<u8>>) {
        let (_tx, rx) = futures::channel::mpsc::unbounded();
        let (out_tx, out_rx) = futures::channel::mpsc::unbounded();
        (
            Self {
                tx: out_tx,
                rx: Arc::new(tokio::sync::Mutex::new(rx)),
            },
            out_rx,
        )
    }
}

impl RaftTransport for TestTransport {
    fn send_vectored(
        &self,
        _peer: PeerId,
        slices: &[&[u8]],
    ) -> impl std::future::Future<Output = Result<(), RaftError>> + Send {
        let mut frame = Vec::new();
        for s in slices {
            frame.extend_from_slice(s);
        }
        let tx = self.tx.clone();
        async move {
            let _ = tx.unbounded_send(frame);
            Ok(())
        }
    }

    fn send_frame_owned(
        &self,
        _peer: PeerId,
        frame: bytes::Bytes,
    ) -> impl std::future::Future<Output = Result<(), RaftError>> + Send {
        let tx = self.tx.clone();
        async move {
            let _ = tx.unbounded_send(frame.to_vec());
            Ok(())
        }
    }

    fn recv_frame(
        &self,
        out: &mut [u8],
    ) -> impl std::future::Future<Output = Result<usize, RaftError>> + Send {
        let rx = self.rx.clone();
        async move {
            let mut rx = rx.lock().await;
            let frame = rx
                .next()
                .await
                .ok_or(RaftError::Transport("closed".into()))?;
            let len = frame.len();
            if out.len() < len {
                return Err(RaftError::Transport("buffer too small".into()));
            }
            out[..len].copy_from_slice(&frame);
            Ok(len)
        }
    }

    fn recv_frame_timeout(
        &self,
        timeout: Duration,
        out: &mut [u8],
    ) -> impl std::future::Future<Output = Result<Option<usize>, RaftError>> + Send {
        let rx = self.rx.clone();
        async move {
            let mut rx = rx.lock().await;
            match tokio::time::timeout(timeout, rx.next()).await {
                Ok(Some(frame)) => {
                    let len = frame.len();
                    if out.len() < len {
                        return Err(RaftError::Transport("buffer too small".into()));
                    }
                    out[..len].copy_from_slice(&frame);
                    Ok(Some(len))
                }
                _ => Ok(None),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Minimal in-process storage
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct StoredEntry {
    term: Term,
    index: LogIndex,
    payload: Vec<u8>,
}

#[derive(Clone, Default)]
struct TestStorage {
    hard_state: Arc<Mutex<Option<HardState>>>,
    entries: Arc<Mutex<Vec<StoredEntry>>>,
}

impl RaftStorage for TestStorage {
    fn load_hard_state(&self) -> Result<HardState, RaftError> {
        Ok(self.hard_state.lock().unwrap().clone().unwrap_or_default())
    }
    fn save_hard_state(&self, state: &HardState) -> Result<(), RaftError> {
        *self.hard_state.lock().unwrap() = Some(state.clone());
        Ok(())
    }
    fn append_entries(&self, new_entries: &[LogEntry<'_>]) -> Result<(), RaftError> {
        let mut entries = self.entries.lock().unwrap();
        for e in new_entries {
            entries.push(StoredEntry {
                term: e.term,
                index: e.index,
                payload: e.payload.0.to_vec(),
            });
        }
        Ok(())
    }
    fn read_entries<'a>(
        &self,
        from: LogIndex,
        to: LogIndex,
        out: &mut Vec<LogEntry<'a>>,
        payload_buf: &'a mut [u8],
    ) -> Result<usize, RaftError> {
        let entries = self.entries.lock().unwrap();
        let mut offset = 0;
        for e in entries.iter() {
            if e.index >= from && e.index < to {
                let len = e.payload.len();
                if offset + len > payload_buf.len() {
                    return Err(RaftError::Storage("payload_buf too small".into()));
                }
                payload_buf[offset..offset + len].copy_from_slice(&e.payload);

                // SAFETY: We ensure payload_buf lives as long as 'a
                let static_payload = unsafe {
                    std::mem::transmute::<&[u8], &'a [u8]>(&payload_buf[offset..offset + len])
                };

                out.push(LogEntry {
                    term: e.term,
                    index: e.index,
                    payload: EntryPayload(static_payload),
                });
                offset += len;
            }
        }
        Ok(offset)
    }
    fn truncate_suffix(&self, from: LogIndex) -> Result<(), RaftError> {
        self.entries.lock().unwrap().retain(|e| e.index < from);
        Ok(())
    }
    fn save_snapshot(&self, _: &SnapshotMeta, _: &[u8]) -> Result<(), RaftError> {
        Ok(())
    }
    fn load_snapshot(&self) -> Result<Option<(SnapshotMeta, Vec<u8>)>, RaftError> {
        Ok(None)
    }
    fn last_log_position(&self) -> Result<(LogIndex, Term), RaftError> {
        Ok(self
            .entries
            .lock()
            .unwrap()
            .last()
            .map(|e| (e.index, e.term))
            .unwrap_or_default())
    }
    fn entry_at<'a>(
        &self,
        index: LogIndex,
        payload_buf: &'a mut [u8],
    ) -> Result<Option<LogEntry<'a>>, RaftError> {
        let entries = self.entries.lock().unwrap();
        if let Some(e) = entries.iter().rev().find(|e| e.index == index) {
            if payload_buf.len() < e.payload.len() {
                return Err(RaftError::Storage("payload_buf too small".into()));
            }
            payload_buf[..e.payload.len()].copy_from_slice(&e.payload);

            // SAFETY: We ensure payload_buf lives as long as 'a
            let static_payload =
                unsafe { std::mem::transmute::<&[u8], &'a [u8]>(&payload_buf[..e.payload.len()]) };

            Ok(Some(LogEntry {
                term: e.term,
                index: e.index,
                payload: EntryPayload(static_payload),
            }))
        } else {
            Ok(None)
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn config_3node(node_id: u64) -> NodeConfig {
    let peers_id = [1u64, 2, 3];
    NodeConfig {
        node_id: PeerId(node_id),
        cluster_id: ClusterId(1),
        peers: peers_id.iter().copied().map(PeerId).collect(),
        bootstrap_peers: peers_id
            .iter()
            .map(|&id| BootstrapPeer {
                id: PeerId(id),
                addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 9000 + id as u16)),
            })
            .collect(),
        timing: TimingConfig {
            heartbeat_ms: 50,
            election_min_ms: 150,
            election_max_ms: 300,
        },
        limits: LimitsConfig::default(),
    }
}

fn quorum(nodes: usize) -> usize {
    (nodes / 2) + 1
}

// ---------------------------------------------------------------------------
// Test 1: validate_node_config rejects invalid configurations
// ---------------------------------------------------------------------------

#[test]
fn invariant_config_validation_rejects_invalid_inputs() {
    // Duplicate peers must be rejected — they break quorum math silently
    let mut cfg = config_3node(1);
    cfg.peers = vec![PeerId(1), PeerId(1), PeerId(2)];
    assert!(
        validate_node_config(&cfg).is_err(),
        "duplicate peers must be rejected"
    );

    // heartbeat_ms must be strictly less than election_min_ms
    let mut cfg = config_3node(1);
    cfg.timing.heartbeat_ms = 150;
    cfg.timing.election_min_ms = 150;
    assert!(
        validate_node_config(&cfg).is_err(),
        "heartbeat >= election_min must be rejected"
    );

    // node_id must be in peers
    let mut cfg = config_3node(1);
    cfg.peers = vec![PeerId(2), PeerId(3)];
    assert!(
        validate_node_config(&cfg).is_err(),
        "node_id absent from peers must be rejected"
    );

    // Valid config must pass
    assert!(validate_node_config(&config_3node(1)).is_ok());
}

// ---------------------------------------------------------------------------
// Test 2: quorum calculation is correct for all cluster sizes
// ---------------------------------------------------------------------------

#[test]
fn invariant_quorum_requires_strict_majority() {
    // Quorum of N nodes = floor(N/2) + 1
    assert_eq!(quorum(1), 1);
    assert_eq!(quorum(2), 2);
    assert_eq!(quorum(3), 2);
    assert_eq!(quorum(5), 3);
    assert_eq!(quorum(7), 4);
}

// ---------------------------------------------------------------------------
// Test 3: HardState does NOT contain commit_index
// ---------------------------------------------------------------------------

#[test]
fn invariant_hard_state_is_commit_index_free() {
    // commit_index is volatile per Raft §5.3 — must NOT be persisted in HardState.
    // This test ensures the struct definition hasn't regressed.
    let hs = HardState {
        current_term: Term(5),
        voted_for: Some(PeerId(2)),
    };
    assert_eq!(hs.current_term, Term(5));
    assert_eq!(hs.voted_for, Some(PeerId(2)));
}

// ---------------------------------------------------------------------------
// Test 4: encode → decode roundtrip preserves all field values
// ---------------------------------------------------------------------------

#[test]
fn invariant_wire_encode_decode_roundtrip_is_lossless() {
    use arbitro_raft::{encode_message_vectored, AppendEntries, Term};

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
    use arbitro_raft::{encode_message_to_bytes, AppendEntries, Term};

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

    // Test the new Bytes-based encoder
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

// ---------------------------------------------------------------------------
// Test 5: Frame with wrong magic is rejected — not silently dropped
// ---------------------------------------------------------------------------

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
// Test 6: node starts as Follower, not Leader
// ---------------------------------------------------------------------------

#[test]
fn invariant_node_starts_as_follower() {
    use arbitro_raft::Role;
    let (transport, _) = TestTransport::new();
    let node =
        arbitro_raft::RaftNode::new(config_3node(1), TestStorage::default(), transport).unwrap();
    assert_eq!(
        node.role(),
        Role::Follower,
        "a freshly initialized node must be a Follower"
    );
    assert!(!node.is_leader());
}

// ---------------------------------------------------------------------------
// Test 7: become_leader_for_benchmark sets leader state and increments term
// ---------------------------------------------------------------------------

#[test]
fn invariant_benchmark_leader_promotion_is_consistent() {
    use arbitro_raft::Role;
    let (transport, _) = TestTransport::new();
    let mut node =
        arbitro_raft::RaftNode::new(config_3node(1), TestStorage::default(), transport).unwrap();
    node.become_leader_for_benchmark(Term(4));
    assert_eq!(node.role(), Role::Leader);
    assert!(node.is_leader());
    assert_eq!(node.current_term(), Term(4));
}
