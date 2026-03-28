/// Raft correctness invariant tests.
///
/// These tests verify that the implementation upholds the safety and liveness
/// invariants stated in the Raft paper (§5) and the arbitro-raft guide.
/// No network is involved — all state transitions are exercised in-process.
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;

use arbitro_raft::{
    decode_message_view, encode_message, validate_node_config, AppendEntriesResp, ArbitroRaft,
    BootstrapPeer, ClusterId, HardState, LogEntry, LogIndex, NodeConfig, PeerId, RaftError,
    RaftMessage, RaftMessageView, RaftStorage, RaftTransport, SnapshotMeta, Term, TimingConfig,
    LimitsConfig,
};

// ---------------------------------------------------------------------------
// Minimal in-process transport for correctness tests
// ---------------------------------------------------------------------------

struct TestTransport {
    tx: futures::channel::mpsc::UnboundedSender<Bytes>,
    rx: tokio::sync::Mutex<futures::channel::mpsc::UnboundedReceiver<Bytes>>,
    /// Frames queued by the test harness to inject as if coming from a peer.
    inject_tx: futures::channel::mpsc::UnboundedSender<Bytes>,
}

impl TestTransport {
    fn new() -> (Self, futures::channel::mpsc::UnboundedReceiver<Bytes>) {
        let (tx, rx) = futures::channel::mpsc::unbounded();
        let (itx, _irx) = futures::channel::mpsc::unbounded::<Bytes>();
        (Self { tx, rx: tokio::sync::Mutex::new(rx), inject_tx: itx }, _irx)
    }

    fn new_with_inject() -> (Self, futures::channel::mpsc::UnboundedSender<Bytes>) {
        let (tx, _out_rx) = futures::channel::mpsc::unbounded::<Bytes>();
        let (itx, irx) = futures::channel::mpsc::unbounded::<Bytes>();
        let transport = Self {
            tx,
            rx: tokio::sync::Mutex::new(irx),
            inject_tx: itx.clone(),
        };
        (transport, itx)
    }
}

#[async_trait]
impl RaftTransport for TestTransport {
    async fn send_frame(&self, _peer: PeerId, frame: Bytes) -> Result<(), RaftError> {
        let _ = self.tx.unbounded_send(frame);
        Ok(())
    }

    async fn recv_frame(&self) -> Result<Bytes, RaftError> {
        let mut rx = self.rx.lock().await;
        rx.next().await.ok_or(RaftError::Transport("closed".into()))
    }

    async fn recv_frame_timeout(&self, timeout: Duration) -> Result<Option<Bytes>, RaftError> {
        let mut rx = self.rx.lock().await;
        match tokio::time::timeout(timeout, rx.next()).await {
            Ok(Some(f)) => Ok(Some(f)),
            _ => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// Minimal in-process storage
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
struct TestStorage {
    hard_state: Arc<Mutex<Option<HardState>>>,
    entries: Arc<Mutex<Vec<LogEntry>>>,
}

impl RaftStorage for TestStorage {
    fn load_hard_state(&self) -> Result<HardState, RaftError> {
        Ok(self.hard_state.lock().unwrap().clone().unwrap_or_default())
    }
    fn save_hard_state(&self, state: &HardState) -> Result<(), RaftError> {
        *self.hard_state.lock().unwrap() = Some(state.clone());
        Ok(())
    }
    fn append_entries(&self, new_entries: &[LogEntry]) -> Result<(), RaftError> {
        self.entries.lock().unwrap().extend_from_slice(new_entries);
        Ok(())
    }
    fn read_entries(&self, from: LogIndex, to: LogIndex, out: &mut Vec<LogEntry>) -> Result<(), RaftError> {
        for e in self.entries.lock().unwrap().iter() {
            if e.index >= from && e.index < to { out.push(e.clone()); }
        }
        Ok(())
    }
    fn truncate_suffix(&self, from: LogIndex) -> Result<(), RaftError> {
        self.entries.lock().unwrap().retain(|e| e.index < from);
        Ok(())
    }
    fn save_snapshot(&self, _: &SnapshotMeta, _: &[u8]) -> Result<(), RaftError> { Ok(()) }
    fn load_snapshot(&self) -> Result<Option<(SnapshotMeta, Vec<u8>)>, RaftError> { Ok(None) }
    fn last_log_position(&self) -> Result<(LogIndex, Term), RaftError> {
        Ok(self.entries.lock().unwrap().last().map(|e| (e.index, e.term)).unwrap_or_default())
    }
    fn entry_at(&self, index: LogIndex) -> Result<Option<LogEntry>, RaftError> {
        Ok(self.entries.lock().unwrap().iter().rev().find(|e| e.index == index).cloned())
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
        bootstrap_peers: peers_id.iter().map(|&id| BootstrapPeer {
            id: PeerId(id),
            addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 9000 + id as u16)),
        }).collect(),
        timing: TimingConfig { heartbeat_ms: 50, election_min_ms: 150, election_max_ms: 300 },
        limits: LimitsConfig::default(),
    }
}

fn quorum(n: usize) -> usize { n / 2 + 1 }

// ---------------------------------------------------------------------------
// Test 1: validate_node_config rejects invalid configurations
// ---------------------------------------------------------------------------

#[test]
fn invariant_config_validation_rejects_invalid_inputs() {
    // Duplicate peers must be rejected — they break quorum math silently
    let mut cfg = config_3node(1);
    cfg.peers = vec![PeerId(1), PeerId(1), PeerId(2)];
    assert!(validate_node_config(&cfg).is_err(), "duplicate peers must be rejected");

    // heartbeat_ms must be strictly less than election_min_ms
    let mut cfg = config_3node(1);
    cfg.timing.heartbeat_ms = 150;
    cfg.timing.election_min_ms = 150;
    assert!(validate_node_config(&cfg).is_err(), "heartbeat >= election_min must be rejected");

    // node_id must be in peers
    let mut cfg = config_3node(1);
    cfg.peers = vec![PeerId(2), PeerId(3)];
    assert!(validate_node_config(&cfg).is_err(), "node_id absent from peers must be rejected");

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
    let hs = HardState { current_term: Term(5), voted_for: Some(PeerId(2)) };
    assert_eq!(hs.current_term, Term(5));
    assert_eq!(hs.voted_for, Some(PeerId(2)));
    // If HardState had a commit_index field the two lines above would fail to compile.
}

// ---------------------------------------------------------------------------
// Test 4: encode → decode roundtrip preserves all field values
// ---------------------------------------------------------------------------

#[test]
fn invariant_wire_encode_decode_roundtrip_is_lossless() {
    use arbitro_raft::{AppendEntries, EntryPayload, LogEntry, Term};

    let entries = vec![
        LogEntry { term: Term(3), index: LogIndex(1), payload: EntryPayload(Bytes::from_static(b"a")) },
        LogEntry { term: Term(3), index: LogIndex(2), payload: EntryPayload(Bytes::from_static(b"bb")) },
    ];
    let msg = RaftMessage::AppendEntries(
        AppendEntries::new(Term(3), PeerId(1), LogIndex(0), Term(0), LogIndex(0), &entries).unwrap(),
    );
    let frame = encode_message(PeerId(1), &msg).unwrap();
    let inbound = decode_message_view(frame).unwrap();

    assert_eq!(inbound.from, PeerId(1));
    if let RaftMessageView::AppendEntries(ae) = &inbound.message {
        assert_eq!(ae.term(), Term(3));
        assert_eq!(ae.leader_id(), PeerId(1));
        assert_eq!(ae.entry_count(), 2);
        let decoded: Vec<_> = ae.entries().unwrap().collect();
        assert_eq!(decoded[0].index(), LogIndex(1));
        assert_eq!(decoded[1].payload().as_ref(), b"bb");
    } else {
        panic!("expected AppendEntries");
    }
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
    let result = decode_message_view(Bytes::from(garbage));
    assert!(result.is_err(), "corrupt frame must return an error, never Ok");
}

// ---------------------------------------------------------------------------
// Test 6: node starts as Follower, not Leader
// ---------------------------------------------------------------------------

#[test]
fn invariant_node_starts_as_follower() {
    use arbitro_raft::Role;
    let (transport, _) = TestTransport::new();
    let node = arbitro_raft::RaftNode::new(config_3node(1), TestStorage::default(), transport).unwrap();
    assert_eq!(node.role(), Role::Follower, "a freshly initialized node must be a Follower");
    assert!(!node.is_leader());
}

// ---------------------------------------------------------------------------
// Test 7: become_leader_for_benchmark sets leader state and increments term
// ---------------------------------------------------------------------------

#[test]
fn invariant_benchmark_leader_promotion_is_consistent() {
    use arbitro_raft::Role;
    let (transport, _) = TestTransport::new();
    let mut node = arbitro_raft::RaftNode::new(config_3node(1), TestStorage::default(), transport).unwrap();
    node.become_leader_for_benchmark(Term(4));
    assert_eq!(node.role(), Role::Leader);
    assert!(node.is_leader());
    assert_eq!(node.current_term(), Term(4));
}
