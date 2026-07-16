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
    validate_node_config, BootstrapPeer, ClusterId, EntryPayload, HardState, LimitsConfig,
    LogEntry, LogIndex, NodeConfig, PeerId, RaftError, RaftStorage, RaftTransport, SnapshotMeta,
    Term, TimingConfig,
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
            if timeout.is_zero() {
                if let Ok(frame) = rx.try_recv() {
                    let len = frame.len();
                    if out.len() < len {
                        return Err(RaftError::Transport("buffer too small".into()));
                    }
                    out[..len].copy_from_slice(&frame);
                    return Ok(Some(len));
                }
                return Ok(None);
            }
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
// Test 4: node starts as Follower, not Leader
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
// Test 5: become_leader_for_benchmark sets leader state and increments term
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

// ---------------------------------------------------------------------------
// Test 6 (BUG-1 regression): CommitIndexObserver observes commit_index writes.
// ---------------------------------------------------------------------------
//
// The apply loop in arbitro-server polls this observer to bound the entries
// it applies to the state machine at the current commit boundary — never past
// the Raft-committed frontier. If a code path writes `soft_state.commit_index`
// without going through `set_commit_index`, the observer would not see the new
// value and the apply loop would either apply too little (safe, only stalls)
// or, worse, would fall back to reading `last_log_position` and apply
// uncommitted entries (unsafe, violates State Machine Safety).
//
// This test locks in the invariant: every `commit_index` write updates the
// atomic mirror.

#[test]
fn commit_index_observer_reflects_writes() {
    let (transport, _) = TestTransport::new();
    let mut node =
        arbitro_raft::RaftNode::new(config_3node(1), TestStorage::default(), transport).unwrap();

    let observer = node.commit_index_observer();
    assert_eq!(
        observer.get(),
        LogIndex(0),
        "freshly-created node must publish commit_index = 0"
    );

    // Drive the single write path: set_commit_index() must update both
    // soft_state (visible via commit_index()) and the atomic mirror.
    node.set_commit_index(LogIndex(7));
    assert_eq!(node.commit_index(), LogIndex(7));
    assert_eq!(
        observer.get(),
        LogIndex(7),
        "observer must see the new commit_index"
    );

    // Monotonic advance — Raft never regresses commit_index in normal
    // operation, but this test also exercises the storage on a second write.
    node.set_commit_index(LogIndex(42));
    assert_eq!(observer.get(), LogIndex(42));

    // A cloned observer sees the same value — the atomic is shared.
    let observer2 = observer.clone();
    node.set_commit_index(LogIndex(100));
    assert_eq!(observer.get(), LogIndex(100));
    assert_eq!(observer2.get(), LogIndex(100));
}

// ---------------------------------------------------------------------------
// Test 7 (P1-2): status() + leader_id() report a consistent initial snapshot.
// ---------------------------------------------------------------------------

#[test]
fn status_and_leader_id_report_initial_state() {
    use arbitro_raft::Role;
    let (transport, _) = TestTransport::new();
    let node =
        arbitro_raft::RaftNode::new(config_3node(1), TestStorage::default(), transport).unwrap();

    // A fresh node knows of no leader — this is the getter an operator calls.
    assert_eq!(node.leader_id(), None);

    let s = node.status();
    assert_eq!(s.node_id, PeerId(1));
    assert_eq!(s.term, Term(0));
    assert_eq!(s.role, Role::Follower);
    assert_eq!(s.leader_id, None);
    assert!(!s.is_leader);
    assert_eq!(s.commit_index, LogIndex(0));
    assert_eq!(s.last_applied, LogIndex(0));
    assert_eq!(s.last_log_index, LogIndex(0));
    assert_eq!(s.voter_count, 3);
    assert!(!s.config_change_in_progress);

    // Promotion flips role/is_leader; status() reflects it consistently.
    let mut node = node;
    node.become_leader_for_benchmark(Term(5));
    let s2 = node.status();
    assert!(s2.is_leader);
    assert_eq!(s2.role, Role::Leader);
    assert_eq!(s2.term, Term(5));
}

// ---------------------------------------------------------------------------
// Test 8 (P1-7): starting an election at u64::MAX term saturates, never wraps.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn campaign_term_saturates_instead_of_wrapping() {
    // Simulate having adopted an adversarially-large term from a peer.
    let storage = TestStorage::default();
    storage
        .save_hard_state(&HardState {
            current_term: Term(u64::MAX),
            voted_for: None,
        })
        .unwrap();

    let (transport, _out) = TestTransport::new();
    let node = arbitro_raft::RaftNode::new(config_3node(1), storage, transport).unwrap();
    let mut raft = arbitro_raft::ArbitroRaft::new(node, arbitro_raft::NoopStateMachine);

    assert_eq!(raft.status().term, Term(u64::MAX), "seeded term must load");

    // No peer will grant a vote, so this campaign fails — but the term bump
    // happens first, and it must NOT wrap to 0.
    let _ = raft.campaign_once().await;

    assert_eq!(
        raft.status().term,
        Term(u64::MAX),
        "term must saturate at u64::MAX, never wrap to 0 (P1-7)"
    );
}

// ---------------------------------------------------------------------------
// Test 9 (P1-1 durability): hard_state survives a restart, so a crashed node
// cannot double-vote in a term it already voted in.
// ---------------------------------------------------------------------------

#[test]
fn hard_state_survives_restart_no_double_vote() {
    // A node voted for peer 2 in term 5, then crashed. On restart the durable
    // hard_state must return so the node knows it already voted in term 5 —
    // this is what prevents a double vote (two leaders in one term) after a
    // crash. save_hard_state's durability is the contract; here we prove the
    // load side recovers term AND voted_for.
    let storage = TestStorage::default();
    storage
        .save_hard_state(&HardState {
            current_term: Term(5),
            voted_for: Some(PeerId(2)),
        })
        .unwrap();

    // "Restart": a brand-new node instance over the same durable storage.
    let (transport, _) = TestTransport::new();
    let node =
        arbitro_raft::RaftNode::new(config_3node(1), storage, transport).unwrap();

    assert_eq!(node.current_term(), Term(5), "term must survive restart");
    assert_eq!(
        node.hard_state().voted_for,
        Some(PeerId(2)),
        "the recorded vote must survive restart so the node cannot double-vote in term 5"
    );
}

// ---------------------------------------------------------------------------
// Test 10 (P1-4 frame-size): an over-large payload is rejected at propose time,
// not committed locally and then silently un-replicable.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn oversized_payload_rejected_at_propose() {
    let (transport, _out) = TestTransport::new();
    let node =
        arbitro_raft::RaftNode::new(config_3node(1), TestStorage::default(), transport).unwrap();
    let mut raft = arbitro_raft::ArbitroRaft::new(node, arbitro_raft::NoopStateMachine);

    // Well past MAX_ENTRY_PAYLOAD (64 KiB - 512). The size guard runs before the
    // leader check, so we get the size rejection, not NotLeader.
    let huge = vec![0u8; 128 * 1024];
    match raft.propose_once(&huge).await {
        Err(arbitro_raft::RaftError::InvalidPayload(_)) => {}
        other => panic!("expected InvalidPayload for oversized payload, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Test 11 (P1-2b metrics): counters observe election activity, and a clone
// taken before the node moves into a task still sees updates.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn metrics_count_election_activity() {
    let (transport, _out) = TestTransport::new();
    let node =
        arbitro_raft::RaftNode::new(config_3node(1), TestStorage::default(), transport).unwrap();
    let mut raft = arbitro_raft::ArbitroRaft::new(node, arbitro_raft::NoopStateMachine);

    // Clone the metrics handle up front — the pattern an operator uses before
    // the node is moved into its run task.
    let metrics = raft.metrics();
    assert_eq!(metrics.snapshot().elections_started, 0);

    // Starts a real election (no peers grant a vote, so it fails — but the
    // start is counted).
    let _ = raft.campaign_once().await;

    assert_eq!(
        metrics.snapshot().elections_started,
        1,
        "the earlier-cloned metrics handle must observe the election start"
    );
}
