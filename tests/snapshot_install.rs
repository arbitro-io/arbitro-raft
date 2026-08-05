//! End-to-end tests: slow follower catches up via `InstallSnapshot`.
//!
//! A 3-node cluster where node 3 is partitioned before proposals land, the
//! leader compacts its log past node 3's `next_index`, and the leader's
//! `install_snapshot_to_lagging_peers` streams the snapshot to node 3,
//! which restores its state machine and re-joins the log.

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arbitro_raft::{
    ArbitroRaft, BootstrapPeer, ClusterId, CommitIndexObserver, EntryPayload, HardState,
    LimitsConfig, LogEntry, LogIndex, NodeConfig, PeerId, RaftError, RaftNode, RaftStorage,
    RaftTransport, SnapshotMeta, StateMachine, Term, TimingConfig,
};

// ---------------------------------------------------------------------------
// CountingSM — observable state machine.
// ---------------------------------------------------------------------------

#[derive(Default, Clone)]
struct CountingSM {
    applied: Arc<AtomicUsize>,
    restored: Arc<AtomicUsize>,
    state_bytes: Arc<Mutex<Vec<u8>>>,
}

impl StateMachine for CountingSM {
    fn apply(&mut self, entry: &[u8]) -> Result<(), RaftError> {
        self.applied.fetch_add(1, Ordering::SeqCst);
        self.state_bytes.lock().unwrap().extend_from_slice(entry);
        Ok(())
    }
    fn snapshot(&self) -> Result<Vec<u8>, RaftError> {
        Ok(self.state_bytes.lock().unwrap().clone())
    }
    fn restore(&mut self, snapshot: &[u8]) -> Result<(), RaftError> {
        self.restored.fetch_add(1, Ordering::SeqCst);
        *self.state_bytes.lock().unwrap() = snapshot.to_vec();
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// AbortOnDrop — cancel background tasks on scope exit.
// ---------------------------------------------------------------------------

struct AbortOnDrop {
    handles: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        for h in &self.handles {
            h.abort();
        }
    }
}

// ---------------------------------------------------------------------------
// TestStorage — in-memory storage that honors truncate_before / save_snapshot.
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
    snapshot: Arc<Mutex<Option<(SnapshotMeta, Vec<u8>)>>>,
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
        let mut buf = payload_buf;
        let mut written = 0;
        for e in entries.iter() {
            if e.index >= from && e.index < to {
                let len = e.payload.len();
                if len > buf.len() {
                    return Err(RaftError::Storage("payload_buf too small".into()));
                }
                // Split the front chunk off the remaining buffer so the
                // shared ref pushed into `out` is never invalidated by a
                // later write through `buf` — no transmute, and Stacked
                // Borrows (Miri) clean.
                let (chunk, rest) = std::mem::take(&mut buf).split_at_mut(len);
                chunk.copy_from_slice(&e.payload);
                buf = rest;
                out.push(LogEntry {
                    term: e.term,
                    index: e.index,
                    payload: EntryPayload(chunk),
                });
                written += len;
            }
        }
        Ok(written)
    }

    fn truncate_suffix(&self, from: LogIndex) -> Result<(), RaftError> {
        self.entries.lock().unwrap().retain(|e| e.index < from);
        Ok(())
    }
    fn truncate_before(&self, up_to: LogIndex) -> Result<(), RaftError> {
        self.entries.lock().unwrap().retain(|e| e.index >= up_to);
        Ok(())
    }
    fn save_snapshot(&self, meta: &SnapshotMeta, bytes: &[u8]) -> Result<(), RaftError> {
        *self.snapshot.lock().unwrap() = Some((meta.clone(), bytes.to_vec()));
        Ok(())
    }
    fn load_snapshot(&self) -> Result<Option<(SnapshotMeta, Vec<u8>)>, RaftError> {
        Ok(self.snapshot.lock().unwrap().clone())
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

            // Shared reborrow-for-return of the 'a buffer (borrow-checked).
            let static_payload: &'a [u8] = &payload_buf[..e.payload.len()];

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
// NetworkHub + RoutingTransport (partition-aware, in-process).
// ---------------------------------------------------------------------------

struct NetworkHub {
    senders: Mutex<
        std::collections::HashMap<PeerId, tokio::sync::mpsc::UnboundedSender<(PeerId, Vec<u8>)>>,
    >,
    partitions: Arc<Mutex<std::collections::HashSet<(PeerId, PeerId)>>>,
}

impl NetworkHub {
    fn new() -> Self {
        Self {
            senders: Mutex::new(std::collections::HashMap::new()),
            partitions: Arc::new(Mutex::new(std::collections::HashSet::new())),
        }
    }
    fn register(
        &self,
        peer: PeerId,
        sender: tokio::sync::mpsc::UnboundedSender<(PeerId, Vec<u8>)>,
    ) {
        self.senders.lock().unwrap().insert(peer, sender);
    }
    fn send(&self, from: PeerId, to: PeerId, data: Vec<u8>) {
        {
            let parts = self.partitions.lock().unwrap();
            if parts.contains(&(from, to)) || parts.contains(&(to, from)) {
                return;
            }
        }
        if let Some(sender) = self.senders.lock().unwrap().get(&to) {
            let _ = sender.send((from, data));
        }
    }
}

struct RoutingTransport {
    from: PeerId,
    hub: Arc<NetworkHub>,
    rx: Arc<tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<(PeerId, Vec<u8>)>>>,
}

impl RaftTransport for RoutingTransport {
    fn send_vectored(
        &self,
        peer: PeerId,
        slices: &[&[u8]],
    ) -> impl std::future::Future<Output = Result<(), RaftError>> + Send {
        let mut data = Vec::new();
        for s in slices {
            data.extend_from_slice(s);
        }
        self.hub.send(self.from, peer, data);
        async move { Ok(()) }
    }
    fn send_frame_owned(
        &self,
        peer: PeerId,
        frame: bytes::Bytes,
    ) -> impl std::future::Future<Output = Result<(), RaftError>> + Send {
        self.hub.send(self.from, peer, frame.to_vec());
        async move { Ok(()) }
    }
    fn recv_frame(
        &self,
        out: &mut [u8],
    ) -> impl std::future::Future<Output = Result<usize, RaftError>> + Send {
        let rx = self.rx.clone();
        async move {
            let mut rx = rx.lock().await;
            if let Some((_from, data)) = rx.recv().await {
                if out.len() < data.len() {
                    return Err(RaftError::Transport("buffer too small".into()));
                }
                out[..data.len()].copy_from_slice(&data);
                Ok(data.len())
            } else {
                Err(RaftError::Transport("channel closed".into()))
            }
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
                if let Ok((_from, data)) = rx.try_recv() {
                    if out.len() < data.len() {
                        return Err(RaftError::Transport("buffer too small".into()));
                    }
                    out[..data.len()].copy_from_slice(&data);
                    return Ok(Some(data.len()));
                }
                return Ok(None);
            }
            match tokio::time::timeout(timeout, rx.recv()).await {
                Ok(Some((_from, data))) => {
                    if out.len() < data.len() {
                        return Err(RaftError::Transport("buffer too small".into()));
                    }
                    out[..data.len()].copy_from_slice(&data);
                    Ok(Some(data.len()))
                }
                _ => Ok(None),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers.
// ---------------------------------------------------------------------------

fn config_3node(node_id: u64) -> NodeConfig {
    let peers_id = [1u64, 2, 3];
    NodeConfig {
        node_id: PeerId(node_id),
        cluster_id: ClusterId(1),
        peers: peers_id.iter().copied().map(PeerId).collect(),
        learners: Vec::new(),
        bootstrap_peers: peers_id
            .iter()
            .map(|&id| BootstrapPeer {
                id: PeerId(id),
                addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 9100 + id as u16)),
            })
            .collect(),
        // Election timers wide enough that spawned followers never campaign
        // before receiving the leader's first heartbeat.
        timing: TimingConfig {
            heartbeat_ms: 50,
            election_min_ms: 800,
            election_max_ms: 1200,
        },
        limits: LimitsConfig::default(),
    }
}

/// Local mirror of B3's `compact_up_to_last_applied` — the crate helper in
/// `src/api/node/log_compaction.rs` lives in a `pub(crate)` module and cannot
/// be imported from an integration test. The behavior is identical:
/// snapshot the SM, persist it, then truncate the log strictly below
/// `last_applied`. Operates on the caller-owned `TestStorage` clone.
fn compact_leader(storage: &TestStorage, sm: &CountingSM, last_applied: LogIndex) -> SnapshotMeta {
    assert!(last_applied.0 > 0, "cannot compact: last_applied is 0");
    let term = storage
        .entries
        .lock()
        .unwrap()
        .iter()
        .rev()
        .find(|e| e.index == last_applied)
        .map(|e| e.term)
        .expect("last_applied entry must exist prior to compaction");
    let meta = SnapshotMeta {
        last_included_index: last_applied,
        last_included_term: term,
    };
    let bytes = sm.snapshot().unwrap();
    storage.save_snapshot(&meta, &bytes).unwrap();
    storage.truncate_before(last_applied).unwrap();
    meta
}

struct ClusterSetup {
    leader: ArbitroRaft<TestStorage, RoutingTransport, CountingSM>,
    leader_storage: TestStorage,
    leader_sm: CountingSM,
    #[allow(dead_code)]
    node2_sm: CountingSM,
    #[allow(dead_code)]
    node2_commit: CommitIndexObserver,
    node3_sm: CountingSM,
    node3_commit: CommitIndexObserver,
    hub: Arc<NetworkHub>,
    _guard: AbortOnDrop,
}

fn boot_cluster() -> ClusterSetup {
    let hub = Arc::new(NetworkHub::new());
    let mut handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    let register = |id: PeerId| -> RoutingTransport {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        hub.register(id, tx);
        RoutingTransport {
            from: id,
            hub: hub.clone(),
            rx: Arc::new(tokio::sync::Mutex::new(rx)),
        }
    };

    // Node 1 — leader, driven manually by the test task.
    let leader_storage = TestStorage::default();
    let n1_transport = register(PeerId(1));
    let mut n1_node = RaftNode::new(config_3node(1), leader_storage.clone(), n1_transport).unwrap();
    n1_node.become_leader_for_benchmark(Term(2));
    let leader_sm = CountingSM::default();
    let leader = ArbitroRaft::new(n1_node, leader_sm.clone());

    // Node 2 — follower, spawned.
    let n2_storage = TestStorage::default();
    let n2_transport = register(PeerId(2));
    let n2_node = RaftNode::new(config_3node(2), n2_storage, n2_transport).unwrap();
    let node2_sm = CountingSM::default();
    let n2_raft = ArbitroRaft::new(n2_node, node2_sm.clone());
    let node2_commit = n2_raft.commit_index_observer();
    handles.push(tokio::spawn(async move {
        let mut r = n2_raft;
        let _ = r.run().await;
    }));

    // Node 3 — follower, spawned.
    let n3_storage = TestStorage::default();
    let n3_transport = register(PeerId(3));
    let n3_node = RaftNode::new(config_3node(3), n3_storage, n3_transport).unwrap();
    let node3_sm = CountingSM::default();
    let n3_raft = ArbitroRaft::new(n3_node, node3_sm.clone());
    let node3_commit = n3_raft.commit_index_observer();
    handles.push(tokio::spawn(async move {
        let mut r = n3_raft;
        let _ = r.run().await;
    }));

    ClusterSetup {
        leader,
        leader_storage,
        leader_sm,
        node2_sm,
        node2_commit,
        node3_sm,
        node3_commit,
        hub,
        _guard: AbortOnDrop { handles },
    }
}

/// Drive the leader through `ticks` `run_once` steps with a `step_ms` gap
/// so followers spawned on the same runtime get scheduling time between.
async fn pump_leader<S, T, SM>(
    leader: &mut ArbitroRaft<S, T, SM>,
    ticks: usize,
    step_ms: u64,
) -> Result<(), RaftError>
where
    S: RaftStorage,
    T: RaftTransport,
    SM: StateMachine,
{
    for _ in 0..ticks {
        tokio::time::sleep(Duration::from_millis(step_ms)).await;
        leader.run_once().await?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Test 1 — slow follower catches up via InstallSnapshot.
// ---------------------------------------------------------------------------

// Un-ignored with C3: the install path now re-anchors the peer's progress at
// the snapshot boundary on success, so post-install reconciliation no longer
// depends on a lucky walk-back — the flow is deterministic.
#[tokio::test(flavor = "multi_thread")]
async fn test_slow_follower_catches_up_via_snapshot() {
    let ClusterSetup {
        mut leader,
        leader_storage,
        leader_sm,
        node3_sm,
        node3_commit,
        hub,
        _guard,
        ..
    } = boot_cluster();

    // Announce leadership so followers park under this leader.
    leader.send_heartbeat_once().await.unwrap();
    pump_leader(&mut leader, 4, 25).await.unwrap();

    // Partition node 3 both directions.
    {
        let mut p = hub.partitions.lock().unwrap();
        p.insert((PeerId(1), PeerId(3)));
        p.insert((PeerId(2), PeerId(3)));
    }

    // Propose 50 single-byte entries — quorum = leader + node 2.
    for i in 0..50u8 {
        leader.propose_once(&[i]).await.unwrap();
    }

    // Drive the leader apply loop until last_applied catches up.
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        leader.run_once().await.unwrap();
        if leader.node().last_applied().0 >= 50 {
            break;
        }
    }
    assert_eq!(leader.node().last_applied(), LogIndex(50));

    // Compact the leader — mirrors B3's compact_up_to_last_applied.
    let snap_meta = compact_leader(
        &leader_storage,
        leader.state_machine(),
        leader.node().last_applied(),
    );
    assert_eq!(snap_meta.last_included_index, LogIndex(50));

    // Un-partition node 3.
    hub.partitions.lock().unwrap().clear();

    // Trigger snapshot install to lagging peers.
    let sent = leader.install_snapshot_to_lagging_peers().await.unwrap();
    assert!(sent >= 1, "expected node 3 to be a lagging peer");

    // Wait for chunk delivery + follower apply loop invoking
    // `restore_state_machine_from_snapshot`.
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        let _ = leader.run_once().await; // keep leader active
        if node3_sm.restored.load(Ordering::SeqCst) >= 1 {
            break;
        }
    }

    // 9a — restore counter incremented.
    assert_eq!(
        node3_sm.restored.load(Ordering::SeqCst),
        1,
        "node 3 must have restored its SM from the snapshot"
    );
    // 9b — byte-for-byte parity with the leader's SM.
    let leader_bytes = leader_sm.state_bytes.lock().unwrap().clone();
    let n3_bytes = node3_sm.state_bytes.lock().unwrap().clone();
    assert_eq!(
        n3_bytes, leader_bytes,
        "node 3's state must match the leader after snapshot install"
    );
    // 9c — `last_applied >= 50` observed via SM state length. `RaftNode::
    // last_applied` is not externally observable while the node is owned
    // by a spawned task; the restore path atomically advances last_applied
    // to the snapshot boundary, so a full state_bytes length implies it.
    assert!(
        n3_bytes.len() >= 50,
        "state_bytes length ({}) implies last_applied < 50",
        n3_bytes.len(),
    );
    // 9d — commit_index observer advanced past the snapshot boundary.
    assert!(
        node3_commit.get().0 >= 50,
        "node 3 commit_index ({}) did not reach snapshot boundary",
        node3_commit.get().0,
    );
}

// ---------------------------------------------------------------------------
// Test 2 — post-snapshot correctness: new entries flow through and apply.
// ---------------------------------------------------------------------------

// Un-ignored with C3: the leader now advances the peer's next_index to
// boundary+1 after a successful install, so the post-snapshot entry is
// replicated with prev_log_index AT the boundary (which the follower accepts
// via its snapshot-boundary fallback) instead of a stale pre-install
// next_index that produced a non-contiguous batch the follower had to drop.
#[tokio::test(flavor = "multi_thread")]
async fn test_snapshot_receiver_restores_state_machine() {
    let ClusterSetup {
        mut leader,
        leader_storage,
        leader_sm,
        node3_sm,
        node3_commit,
        hub,
        _guard,
        ..
    } = boot_cluster();

    leader.send_heartbeat_once().await.unwrap();
    pump_leader(&mut leader, 4, 25).await.unwrap();

    {
        let mut p = hub.partitions.lock().unwrap();
        p.insert((PeerId(1), PeerId(3)));
        p.insert((PeerId(2), PeerId(3)));
    }

    for i in 0..50u8 {
        leader.propose_once(&[i]).await.unwrap();
    }

    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        leader.run_once().await.unwrap();
        if leader.node().last_applied().0 >= 50 {
            break;
        }
    }
    assert_eq!(leader.node().last_applied(), LogIndex(50));

    let _ = compact_leader(
        &leader_storage,
        leader.state_machine(),
        leader.node().last_applied(),
    );

    hub.partitions.lock().unwrap().clear();

    let sent = leader.install_snapshot_to_lagging_peers().await.unwrap();
    assert!(sent >= 1);

    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        let _ = leader.run_once().await;
        if node3_sm.restored.load(Ordering::SeqCst) >= 1 {
            break;
        }
    }

    let baseline_applied = node3_sm.applied.load(Ordering::SeqCst);

    // Post-snapshot correctness: 51st entry, distinctive byte.
    let extra: u8 = 0xAB;
    leader.propose_once(&[extra]).await.unwrap();

    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        leader.run_once().await.unwrap();
        if node3_sm.applied.load(Ordering::SeqCst) > baseline_applied && node3_commit.get().0 >= 51
        {
            break;
        }
    }

    assert!(
        node3_sm.applied.load(Ordering::SeqCst) > baseline_applied,
        "node 3 did not apply the post-snapshot entry"
    );
    let leader_bytes = leader_sm.state_bytes.lock().unwrap().clone();
    let n3_bytes = node3_sm.state_bytes.lock().unwrap().clone();
    assert_eq!(
        n3_bytes, leader_bytes,
        "node 3's state must match the leader after the post-snapshot entry"
    );
    assert_eq!(n3_bytes.last().copied(), Some(extra));
}
