//! End-to-end tests for joint-consensus membership changes.
//!
//! Scenario 1 — grow a 3-node cluster into a 4-node cluster.
//! Scenario 2 — shrink a 4-node cluster back into a 3-node cluster.
//!
//! Both tests are gated with `#[ignore]` because the joint-consensus apply
//! hook (`apply_if_config_change`) is not currently invoked by
//! `ArbitroRaft::apply_committed_entries` — the peer set is therefore never
//! mutated at runtime, so the leader never routes AppendEntries to the new
//! voter and the assertions below cannot pass in-tree. The tests are kept
//! green under `cargo check --all-targets` so the scaffolding is ready the
//! moment the apply hook is wired.

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arbitro_raft::{
    ArbitroRaft, BootstrapPeer, ClusterId, EntryPayload, HardState, LimitsConfig, LogEntry,
    LogIndex, NodeConfig, PeerId, RaftError, RaftStorage, RaftTransport, SnapshotMeta,
    StateMachine, Term, TimingConfig,
};

// ---------------------------------------------------------------------------
// CountingSM — records how many entries have been applied to the SM.
// ---------------------------------------------------------------------------
#[derive(Clone, Default)]
struct CountingSM {
    counter: Arc<AtomicU64>,
}

impl StateMachine for CountingSM {
    fn apply(&mut self, _entry: &[u8]) -> Result<(), RaftError> {
        self.counter.fetch_add(1, Ordering::SeqCst);
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
        let mut offset = 0;
        for e in entries.iter() {
            if e.index >= from && e.index < to {
                let len = e.payload.len();
                if offset + len > payload_buf.len() {
                    return Err(RaftError::Storage("payload_buf too small".into()));
                }
                payload_buf[offset..offset + len].copy_from_slice(&e.payload);

                // SAFETY: payload_buf outlives 'a for the duration of this call.
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

            // SAFETY: payload_buf outlives 'a for the duration of this call.
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
// Config helper.
// ---------------------------------------------------------------------------
fn make_config(node_id: u64, peers: &[u64]) -> NodeConfig {
    NodeConfig {
        node_id: PeerId(node_id),
        cluster_id: ClusterId(1),
        peers: peers.iter().copied().map(PeerId).collect(),
        bootstrap_peers: peers
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

// ---------------------------------------------------------------------------
// NetworkHub & RoutingTransport.
// ---------------------------------------------------------------------------
struct NetworkHub {
    senders: Mutex<
        std::collections::HashMap<PeerId, tokio::sync::mpsc::UnboundedSender<(PeerId, Vec<u8>)>>,
    >,
}

impl NetworkHub {
    fn new() -> Self {
        Self {
            senders: Mutex::new(std::collections::HashMap::new()),
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
type Raft = ArbitroRaft<TestStorage, RoutingTransport, CountingSM>;
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

fn boot_node(
    hub: Arc<NetworkHub>,
    id: u64,
    peers: &[u64],
) -> (SharedRaft, TestStorage, CountingSM) {
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

    let node = arbitro_raft::RaftNode::new(config, storage.clone(), transport).unwrap();
    let sm = CountingSM::default();
    let raft = ArbitroRaft::new(node, sm.clone());
    (Arc::new(tokio::sync::Mutex::new(raft)), storage, sm)
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

// ---------------------------------------------------------------------------
// Test 1 — add a fourth node via config change.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
#[ignore = "joint-quorum apply hook not wired: peers() never updates, node 4 catch-up impossible"]
async fn test_add_fourth_node_via_config_change() {
    let hub = Arc::new(NetworkHub::new());
    let initial = [1u64, 2, 3];

    let mut rafts: Vec<SharedRaft> = Vec::new();
    let mut storages: Vec<TestStorage> = Vec::new();
    let mut sms: Vec<CountingSM> = Vec::new();
    let mut guard = AbortOnDrop { handles: vec![] };

    for id in initial {
        let (r, s, sm) = boot_node(hub.clone(), id, &initial);
        guard.handles.push(spawn_driver(r.clone()));
        rafts.push(r);
        storages.push(s);
        sms.push(sm);
    }

    tokio::time::sleep(Duration::from_millis(1200)).await;
    let leader = await_leader(&rafts).await;

    // Establish committed log by proposing 5 application entries.
    for i in 0..5u8 {
        let payload = [b'x', i];
        let mut r = rafts[leader].lock().await;
        r.propose_once(&payload).await.expect("app propose failed");
    }

    // Boot node 4 with the target 4-peer configuration and spawn its driver
    // BEFORE proposing the config change so it can receive catch-up frames.
    let target = [1u64, 2, 3, 4];
    let (r4, s4, sm4) = boot_node(hub.clone(), 4, &target);
    guard.handles.push(spawn_driver(r4.clone()));
    rafts.push(r4);
    storages.push(s4.clone());
    sms.push(sm4);

    // Give node 4 a moment to enter its follower loop.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let final_idx = {
        let mut r = rafts[leader].lock().await;
        r.propose_config_change(vec![PeerId(1), PeerId(2), PeerId(3), PeerId(4)])
            .await
            .expect("config change failed")
    };
    assert!(
        final_idx.0 >= 7,
        "final_idx {} < 7 — expected 5 app + 2 config entries",
        final_idx.0
    );

    // Allow node 4 to catch up via AppendEntries.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 6a — node 4's state machine applied everything through final_idx.
    {
        let r = rafts[3].lock().await;
        assert!(
            r.node().last_applied() >= final_idx,
            "node 4 last_applied {} < final_idx {}",
            r.node().last_applied().0,
            final_idx.0,
        );
    }

    // 6b — node 4's storage last log position is at or beyond final_idx.
    let (last_log_idx, _) = s4.last_log_position().unwrap();
    assert!(
        last_log_idx >= final_idx,
        "node 4 storage last_log {} < final_idx {}",
        last_log_idx.0,
        final_idx.0,
    );

    // 6c — every original node reports the new voter set.
    let expected: Vec<PeerId> = target.iter().copied().map(PeerId).collect();
    for i in 0..3 {
        let r = rafts[i].lock().await;
        assert_eq!(
            r.node().peers(),
            expected.as_slice(),
            "node {} peers not updated",
            i + 1,
        );
    }

    // 6d — post-transition write commits under the new 4-node quorum.
    {
        let mut r = rafts[leader].lock().await;
        r.propose_once(b"post")
            .await
            .expect("post-transition propose failed");
    }
}

// ---------------------------------------------------------------------------
// Test 2 — remove the fourth node via config change.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
#[ignore = "joint-quorum apply hook not wired: peers() never shrinks, removed node never steps down"]
async fn test_remove_node_via_config_change() {
    let hub = Arc::new(NetworkHub::new());
    let initial = [1u64, 2, 3, 4];

    let mut rafts: Vec<SharedRaft> = Vec::new();
    let mut storages: Vec<TestStorage> = Vec::new();
    let mut guard = AbortOnDrop { handles: vec![] };

    for id in initial {
        let (r, s, _sm) = boot_node(hub.clone(), id, &initial);
        guard.handles.push(spawn_driver(r.clone()));
        rafts.push(r);
        storages.push(s);
    }

    tokio::time::sleep(Duration::from_millis(1500)).await;
    let leader = await_leader(&rafts).await;

    for i in 0..5u8 {
        let payload = [b'y', i];
        let mut r = rafts[leader].lock().await;
        r.propose_once(&payload).await.expect("app propose failed");
    }

    let final_idx = {
        let mut r = rafts[leader].lock().await;
        r.propose_config_change(vec![PeerId(1), PeerId(2), PeerId(3)])
            .await
            .expect("config change failed")
    };
    assert!(
        final_idx.0 >= 7,
        "final_idx {} < 7 — expected 5 app + 2 config entries",
        final_idx.0,
    );

    tokio::time::sleep(Duration::from_millis(500)).await;

    let expected: Vec<PeerId> = [1u64, 2, 3].iter().copied().map(PeerId).collect();
    for i in 0..3 {
        let r = rafts[i].lock().await;
        assert_eq!(
            r.node().peers(),
            expected.as_slice(),
            "node {} peers not shrunk to [1,2,3]",
            i + 1,
        );
    }

    // Node 4 must either have stepped down (not leader) or, at minimum, no
    // longer be counted in the new quorum. We express that as: node 4's
    // peer set no longer contains its own id, OR node 4 is not a leader.
    {
        let r = rafts[3].lock().await;
        let peers = r.node().peers().to_vec();
        let is_leader = r.node().is_leader();
        assert!(
            !is_leader && !peers.contains(&PeerId(4)),
            "node 4 was not removed from quorum: is_leader={is_leader} peers={peers:?}",
        );
    }
}
