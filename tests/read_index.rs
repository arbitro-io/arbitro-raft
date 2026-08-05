//! End-to-end tests for the linearizable ReadIndex primitive (Raft §6.4, A11).
//!
//! Scenarios:
//!   1. A follower's `read_index` request is refused with `NotLeader` (plus
//!      redirect hint when it knows the leader).
//!   2. A leader returns a `read_index >= ` the index of an entry that was
//!      known committed before the call, and after the (internal) apply wait
//!      the state machine reflects that entry. Also covers the §6.4
//!      current-term commit guard: the FIRST read on a fresh leader (no
//!      client entries of its term committed yet) commits an internal no-op —
//!      which must never reach the user state machine.
//!   3. Safety: a partitioned (about-to-be-deposed) old leader does NOT serve
//!      a successful ReadIndex — quorum confirmation fails within its
//!      deadline and the read errors. No stale read is ever served.
//!
//! Harness: same in-memory `NetworkHub` + `RoutingTransport` used by the
//! leadership-transfer tests, with a directional block list for partitions.

use std::collections::{HashMap, HashSet};
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arbitro_raft::{
    ArbitroRaft, BootstrapPeer, ClusterId, EntryPayload, HardState, LimitsConfig, LogEntry,
    LogIndex, NodeConfig, PeerId, RaftError, RaftStorage, RaftTransport, SnapshotMeta,
    StateMachine, Term, TimingConfig,
};

// ---------------------------------------------------------------------------
// RecordingSM — stores every applied payload so tests can assert exactly what
// reached the user state machine (and what did NOT — e.g. the A11 no-op).
// ---------------------------------------------------------------------------
#[derive(Clone, Default)]
struct RecordingSM {
    applied: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl RecordingSM {
    fn applied(&self) -> Vec<Vec<u8>> {
        self.applied.lock().unwrap().clone()
    }
}

impl StateMachine for RecordingSM {
    fn apply(&mut self, entry: &[u8]) -> Result<(), RaftError> {
        self.applied.lock().unwrap().push(entry.to_vec());
        Ok(())
    }
    fn snapshot(&self) -> Result<Vec<u8>, RaftError> {
        Ok(Vec::new())
    }
    fn restore(&mut self, _snapshot: &[u8]) -> Result<(), RaftError> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// AbortOnDrop — cancel every driver task on drop.
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
// TestStorage — in-memory RaftStorage with shared state via Arc<Mutex>.
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
                // later write through `buf`.
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
            let view: &'a [u8] = &payload_buf[..e.payload.len()];
            Ok(Some(LogEntry {
                term: e.term,
                index: e.index,
                payload: EntryPayload(view),
            }))
        } else {
            Ok(None)
        }
    }
}

// ---------------------------------------------------------------------------
// Config helper.
// ---------------------------------------------------------------------------
fn make_config(node_id: u64, peers: &[u64]) -> NodeConfig {
    NodeConfig {
        node_id: PeerId(node_id),
        cluster_id: ClusterId(1),
        peers: peers.iter().copied().map(PeerId).collect(),
        learners: Vec::new(),
        bootstrap_peers: peers
            .iter()
            .map(|&id| BootstrapPeer {
                id: PeerId(id),
                addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 9300 + id as u16)),
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

// ---------------------------------------------------------------------------
// NetworkHub & RoutingTransport — with a directional block list.
// ---------------------------------------------------------------------------
struct NetworkHub {
    senders: Mutex<HashMap<PeerId, tokio::sync::mpsc::UnboundedSender<(PeerId, Vec<u8>)>>>,
    /// Frames from `.0` to `.1` are silently dropped while present.
    blocked: Mutex<HashSet<(PeerId, PeerId)>>,
}

impl NetworkHub {
    fn new() -> Self {
        Self {
            senders: Mutex::new(HashMap::new()),
            blocked: Mutex::new(HashSet::new()),
        }
    }

    fn register(
        &self,
        peer: PeerId,
        sender: tokio::sync::mpsc::UnboundedSender<(PeerId, Vec<u8>)>,
    ) {
        self.senders.lock().unwrap().insert(peer, sender);
    }

    fn block(&self, from: PeerId, to: PeerId) {
        self.blocked.lock().unwrap().insert((from, to));
    }

    /// Fully isolate `node` from every other registered peer (both ways).
    fn isolate(&self, node: PeerId) {
        let peers: Vec<PeerId> = self.senders.lock().unwrap().keys().copied().collect();
        for p in peers {
            if p != node {
                self.block(node, p);
                self.block(p, node);
            }
        }
    }

    fn send(&self, from: PeerId, to: PeerId, data: Vec<u8>) {
        if self.blocked.lock().unwrap().contains(&(from, to)) {
            return;
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
                if let Ok((_sender, data)) = rx.try_recv() {
                    if out.len() < data.len() {
                        return Err(RaftError::Transport("buffer too small".into()));
                    }
                    out[..data.len()].copy_from_slice(&data);
                    return Ok(Some(data.len()));
                }
                return Ok(None);
            }
            match tokio::time::timeout(timeout, rx.recv()).await {
                Ok(Some((_sender, data))) => {
                    if out.len() < data.len() {
                        return Err(RaftError::Transport("buffer too small".into()));
                    }
                    out[..data.len()].copy_from_slice(&data);
                    Ok(Some(data.len()))
                }
                Ok(None) => Ok(None),
                Err(_) => Ok(None),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Shared driver plumbing.
// ---------------------------------------------------------------------------
type Raft = ArbitroRaft<TestStorage, RoutingTransport, RecordingSM>;
type SharedRaft = Arc<tokio::sync::Mutex<Raft>>;

fn spawn_driver(raft: SharedRaft) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let mut r = raft.lock().await;
            let keep_going = match r.run_once().await {
                Ok(cont) => cont,
                Err(_) => break,
            };
            drop(r);
            if !keep_going {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
}

fn boot_node(hub: Arc<NetworkHub>, id: u64, peers: &[u64]) -> (SharedRaft, RecordingSM) {
    let peer_id = PeerId(id);
    let config = make_config(id, peers);
    let storage = TestStorage::default();

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    hub.register(peer_id, tx);

    let transport = RoutingTransport {
        from: peer_id,
        hub,
        rx: Arc::new(tokio::sync::Mutex::new(rx)),
    };

    let node = arbitro_raft::RaftNode::new(config, storage, transport).unwrap();
    let sm = RecordingSM::default();
    let raft = ArbitroRaft::new(node, sm.clone());
    (Arc::new(tokio::sync::Mutex::new(raft)), sm)
}

async fn find_leader(rafts: &[SharedRaft]) -> Option<usize> {
    for (i, r) in rafts.iter().enumerate() {
        let g = r.lock().await;
        if g.node().is_leader() {
            return Some(i);
        }
    }
    None
}

async fn await_leader(rafts: &[SharedRaft]) -> usize {
    for _ in 0..40 {
        if let Some(i) = find_leader(rafts).await {
            return i;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("no leader elected within budget");
}

struct Cluster {
    hub: Arc<NetworkHub>,
    rafts: Vec<SharedRaft>,
    sms: Vec<RecordingSM>,
    _guard: AbortOnDrop,
}

async fn boot_cluster(ids: &[u64]) -> Cluster {
    let hub = Arc::new(NetworkHub::new());
    let mut rafts = Vec::new();
    let mut sms = Vec::new();
    let mut guard = AbortOnDrop { handles: vec![] };
    for &id in ids {
        let (r, sm) = boot_node(hub.clone(), id, ids);
        guard.handles.push(spawn_driver(r.clone()));
        rafts.push(r);
        sms.push(sm);
    }
    // Let the cluster elect an initial leader.
    tokio::time::sleep(Duration::from_millis(1200)).await;
    Cluster {
        hub,
        rafts,
        sms,
        _guard: guard,
    }
}

// ---------------------------------------------------------------------------
// 1. Follower refuses ReadIndex with NotLeader (+ redirect hint).
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn read_index_on_follower_returns_not_leader() {
    let cluster = boot_cluster(&[1, 2, 3]).await;
    let leader_i = await_leader(&cluster.rafts).await;
    let leader_id = {
        let g = cluster.rafts[leader_i].lock().await;
        g.node_id()
    };

    // Pick any follower and ask it for a read index.
    let follower_i = (0..cluster.rafts.len())
        .find(|i| *i != leader_i)
        .expect("3-node cluster has a follower");
    let mut follower = cluster.rafts[follower_i].lock().await;
    assert!(
        !follower.node().is_leader(),
        "picked node must be a follower"
    );

    match follower.read_index().await {
        Err(RaftError::NotLeader { leader_hint }) => {
            // A settled follower knows its leader — the hint must name it.
            if let Some(hint) = leader_hint {
                assert_eq!(
                    hint.leader_id, leader_id,
                    "redirect hint must point at the actual leader"
                );
            }
        }
        other => panic!("follower read_index must be NotLeader, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// 2. Leader ReadIndex covers a known-committed entry; the state machine
//    reflects it after the apply wait; the A11 no-op never reaches the SM.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn read_index_covers_committed_entry_and_sm_reflects_it() {
    let cluster = boot_cluster(&[1, 2, 3]).await;
    let leader_i = await_leader(&cluster.rafts).await;

    let mut leader = cluster.rafts[leader_i].lock().await;

    // First read on a fresh leader: no entry of the current term has been
    // proposed yet, so the §6.4 guard commits an internal no-op. The read
    // must still succeed (read_index >= the no-op's index >= 1)...
    let first_ri = leader
        .read_index()
        .await
        .expect("fresh leader must serve a read index by committing a no-op");
    assert!(
        first_ri >= LogIndex(1),
        "guard no-op must have committed at index >= 1, got {first_ri:?}"
    );
    // ...and the no-op is a CONTROL entry: it must never reach the user SM.
    assert!(
        cluster.sms[leader_i].applied().is_empty(),
        "the A11 guard no-op leaked into the user state machine"
    );

    // Commit a real entry, then take a read index.
    let payload = b"read-index-test-value".to_vec();
    let committed_idx = leader
        .propose_once(&payload)
        .await
        .expect("propose on the leader must commit");

    let ri = leader
        .read_index()
        .await
        .expect("leader with healthy quorum must serve a read index");
    assert!(
        ri >= committed_idx,
        "read_index {ri:?} must cover the entry committed at {committed_idx:?} before the call"
    );
    // Apply wait ran inside read_index: last_applied >= ri >= committed_idx,
    // so the state machine must already reflect the committed entry.
    assert!(
        leader.node().last_applied() >= ri,
        "read_index returned before the local apply wait completed"
    );
    assert!(
        cluster.sms[leader_i]
            .applied()
            .iter()
            .any(|p| p == &payload),
        "state machine must reflect the committed entry at the read point"
    );
}

// ---------------------------------------------------------------------------
// 3. SAFETY: a partitioned old leader must NOT serve a ReadIndex — quorum
//    confirmation fails and the read errors; no stale read is possible.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn read_index_refused_on_partitioned_deposed_leader() {
    let cluster = boot_cluster(&[1, 2, 3]).await;
    let leader_i = await_leader(&cluster.rafts).await;

    // Commit an entry while the cluster is healthy.
    {
        let mut leader = cluster.rafts[leader_i].lock().await;
        leader
            .propose_once(b"pre-partition-entry")
            .await
            .expect("healthy propose must commit");
    }

    // Lock the old leader FIRST so its own driver cannot run check-quorum
    // and abdicate before we exercise the dangerous window: a leader that
    // does not yet know it is partitioned.
    let mut old_leader = cluster.rafts[leader_i].lock().await;
    let old_leader_id = old_leader.node_id();
    cluster.hub.isolate(old_leader_id);
    assert!(
        old_leader.node().is_leader(),
        "old leader must still believe it leads at the start of the window"
    );

    // The majority side is free to elect a new leader and accept new writes
    // during this call — exactly the scenario a stale read would corrupt.
    let result = old_leader.read_index().await;
    match result {
        Err(RaftError::NoQuorum) | Err(RaftError::NotLeader { .. }) => {}
        Ok(idx) => panic!("partitioned old leader served ReadIndex {idx:?} — stale read hole!"),
        Err(other) => panic!("expected NoQuorum/NotLeader, got unexpected error {other:?}"),
    }
    drop(old_leader);

    // Sanity: the majority side remains available — a new leader emerges and
    // serves both writes and reads.
    let mut served = false;
    for _ in 0..40 {
        if let Some(new_i) = find_leader(&cluster.rafts).await {
            let new_id = {
                let g = cluster.rafts[new_i].lock().await;
                g.node_id()
            };
            if new_id != old_leader_id {
                let mut new_leader = cluster.rafts[new_i].lock().await;
                if new_leader
                    .propose_once(b"post-partition-entry")
                    .await
                    .is_ok()
                {
                    let ri = new_leader
                        .read_index()
                        .await
                        .expect("new majority leader must serve reads");
                    assert!(ri >= LogIndex(1));
                    served = true;
                    break;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        served,
        "majority side must elect a new leader that serves writes and reads"
    );
}
