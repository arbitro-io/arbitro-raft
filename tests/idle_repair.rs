//! A9 / PS11 — idle-cluster repair of lagging and diverged followers.
//!
//! AUDIT_REPORT PS11 claimed "repair only piggybacks on new traffic; a quiet
//! cluster leaves a diverged/lagging follower behind indefinitely". The fix
//! is the behind-peer branch on the periodic heartbeat path
//! (`send_heartbeat_once`): when a follower's `next_index <= last_log` the
//! heartbeat tick ships its backlog (bounded by `append_batch_entries`)
//! instead of a bare wire, and a diverged follower's conflict rejects walk
//! `next_index` back until that same branch takes over — with ZERO client
//! traffic. These tests pin that behavior:
//!
//!   1. Lagging follower: partitioned while the cluster commits, healed, then
//!      the cluster goes fully idle — the follower must converge to the
//!      leader's last index purely via heartbeat-driven repair.
//!   2. Diverged follower: boots with a conflicting uncommitted tail from an
//!      old term — the idle path must truncate the conflict and rebuild it
//!      byte-identical to the leader's log, again with zero proposals.
//!   3. No-op when caught up: once every follower is at `last_log_index`,
//!      idle heartbeats are BARE (entry_count == 0) — repair must not add
//!      steady-state entry traffic.
//!
//! Harness: same in-memory `NetworkHub` + `RoutingTransport` pattern as the
//! graceful-drain tests (directional block list for partition control), plus
//! a frame snoop that decodes hub traffic to classify heartbeat wires.

use std::collections::{HashMap, HashSet};
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arbitro_raft::{
    decode_message, ArbitroRaft, BootstrapPeer, ClusterId, EntryPayload, HardState, LimitsConfig,
    LogEntry, LogIndex, NodeConfig, PeerId, RaftError, RaftStorage, RaftTransport, SnapshotMeta,
    StateMachine, Term, TimingConfig,
};

// ---------------------------------------------------------------------------
// NoopSM — entries carry opaque payloads; the state machine is irrelevant here.
// ---------------------------------------------------------------------------
struct NoopSM;
impl StateMachine for NoopSM {
    fn apply(&mut self, _entry: &[u8]) -> Result<(), RaftError> {
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

impl TestStorage {
    /// Pre-seed the log with `(term, index, payload)` triples (test setup).
    fn seed(&self, entries: &[(u64, u64, &[u8])], term: u64) {
        let mut g = self.entries.lock().unwrap();
        for (t, i, p) in entries {
            g.push(StoredEntry {
                term: Term(*t),
                index: LogIndex(*i),
                payload: p.to_vec(),
            });
        }
        *self.hard_state.lock().unwrap() = Some(HardState {
            current_term: Term(term),
            voted_for: None,
        });
    }

    /// Resolved log view: for each index 1..=last, the effective entry
    /// (last-occurrence-wins, mirroring `entry_at`'s reverse scan).
    fn log_view(&self) -> Vec<(u64, u64, Vec<u8>)> {
        let g = self.entries.lock().unwrap();
        let last = g.last().map(|e| e.index.0).unwrap_or(0);
        let mut out = Vec::new();
        for idx in 1..=last {
            if let Some(e) = g.iter().rev().find(|e| e.index.0 == idx) {
                out.push((e.term.0, e.index.0, e.payload.clone()));
            }
        }
        out
    }

    fn last_index(&self) -> u64 {
        self.entries
            .lock()
            .unwrap()
            .last()
            .map(|e| e.index.0)
            .unwrap_or(0)
    }
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
// NetworkHub & RoutingTransport — directional block list + heartbeat snoop.
// ---------------------------------------------------------------------------
struct NetworkHub {
    senders: Mutex<HashMap<PeerId, tokio::sync::mpsc::UnboundedSender<(PeerId, Vec<u8>)>>>,
    /// Frames from `.0` to `.1` are silently dropped while present.
    blocked: Mutex<HashSet<(PeerId, PeerId)>>,
    /// While set, every AppendEntries frame crossing the hub is classified.
    snoop_on: AtomicBool,
    bare_heartbeats: AtomicU64,
    entry_frames: AtomicU64,
}

impl NetworkHub {
    fn new() -> Self {
        Self {
            senders: Mutex::new(HashMap::new()),
            blocked: Mutex::new(HashSet::new()),
            snoop_on: AtomicBool::new(false),
            bare_heartbeats: AtomicU64::new(0),
            entry_frames: AtomicU64::new(0),
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

    fn unblock_all(&self) {
        self.blocked.lock().unwrap().clear();
    }

    /// Fully isolate `peer` from every other node, both directions.
    fn isolate(&self, peer: PeerId, all: &[u64]) {
        for &other in all {
            let other = PeerId(other);
            if other != peer {
                self.block(peer, other);
                self.block(other, peer);
            }
        }
    }

    fn start_snoop(&self) {
        self.bare_heartbeats.store(0, Ordering::SeqCst);
        self.entry_frames.store(0, Ordering::SeqCst);
        self.snoop_on.store(true, Ordering::SeqCst);
    }

    fn stop_snoop(&self) -> (u64, u64) {
        self.snoop_on.store(false, Ordering::SeqCst);
        (
            self.bare_heartbeats.load(Ordering::SeqCst),
            self.entry_frames.load(Ordering::SeqCst),
        )
    }

    fn send(&self, from: PeerId, to: PeerId, data: Vec<u8>) {
        if self.blocked.lock().unwrap().contains(&(from, to)) {
            return;
        }
        if self.snoop_on.load(Ordering::SeqCst) {
            if let Ok(inbound) = decode_message(&data) {
                if let Some(ae) = inbound.as_append_entries() {
                    if ae.entry_count() > 0 {
                        self.entry_frames.fetch_add(1, Ordering::SeqCst);
                    } else {
                        self.bare_heartbeats.fetch_add(1, Ordering::SeqCst);
                    }
                }
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
// Cluster plumbing.
// ---------------------------------------------------------------------------
type Raft = ArbitroRaft<TestStorage, RoutingTransport, NoopSM>;
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

fn boot_node(hub: Arc<NetworkHub>, id: u64, peers: &[u64], storage: TestStorage) -> SharedRaft {
    let peer_id = PeerId(id);
    let config = make_config(id, peers);

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    hub.register(peer_id, tx);

    let transport = RoutingTransport {
        from: peer_id,
        hub,
        rx: Arc::new(tokio::sync::Mutex::new(rx)),
    };

    let node = arbitro_raft::RaftNode::new(config, storage, transport).unwrap();
    let raft = ArbitroRaft::new(node, NoopSM);
    Arc::new(tokio::sync::Mutex::new(raft))
}

struct Cluster {
    hub: Arc<NetworkHub>,
    rafts: Vec<SharedRaft>,
    storages: Vec<TestStorage>,
    _guard: AbortOnDrop,
}

async fn boot_cluster_with_storages(ids: &[u64], storages: Vec<TestStorage>) -> Cluster {
    let hub = Arc::new(NetworkHub::new());
    let mut rafts = Vec::new();
    let mut guard = AbortOnDrop { handles: vec![] };
    for (i, &id) in ids.iter().enumerate() {
        let r = boot_node(hub.clone(), id, ids, storages[i].clone());
        guard.handles.push(spawn_driver(r.clone()));
        rafts.push(r);
    }
    tokio::time::sleep(Duration::from_millis(1200)).await;
    Cluster {
        hub,
        rafts,
        storages,
        _guard: guard,
    }
}

async fn boot_cluster(ids: &[u64]) -> Cluster {
    let storages: Vec<TestStorage> = ids.iter().map(|_| TestStorage::default()).collect();
    boot_cluster_with_storages(ids, storages).await
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
    for _ in 0..60 {
        if let Some(i) = find_leader(rafts).await {
            return i;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("no leader elected within budget");
}

// ---------------------------------------------------------------------------
// Test 1 — idle-cluster repair of a lagging follower (PS11 headline case).
//
// Partition one follower, commit entries through the remaining majority, heal
// the partition, then STOP all client traffic. The lagging follower must
// converge to the leader's last index purely via heartbeat-driven repair
// within a bounded number of heartbeat ticks.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn idle_cluster_repairs_lagging_follower_after_partition_heals() {
    let ids = [1u64, 2, 3];
    let cluster = boot_cluster(&ids).await;
    let leader = await_leader(&cluster.rafts).await;
    let leader_id = PeerId(ids[leader]);

    // Fully isolate one follower.
    let lagging = (0..ids.len()).find(|&i| i != leader).unwrap();
    let lagging_id = PeerId(ids[lagging]);
    cluster.hub.isolate(lagging_id, &ids);

    // Commit entries through the remaining 2/3 majority.
    for i in 0..10u8 {
        let mut r = cluster.rafts[leader].lock().await;
        r.propose_once(&[b'v', i])
            .await
            .expect("propose with 2/3 majority must commit");
    }
    let leader_last = cluster.storages[leader].last_index();
    assert!(
        cluster.storages[lagging].last_index() < leader_last,
        "test setup: partitioned follower must actually be lagging"
    );

    // Heal the partition. From here on there is ZERO client traffic — the
    // only frames a correct leader emits are periodic heartbeats. PS11:
    // those heartbeats must carry the missing entries for the behind peer.
    cluster.hub.unblock_all();

    // Bound: repair needs at most a handful of 50ms heartbeat ticks (one
    // probe/backup round plus batched backlog). 4s is dozens of ticks —
    // generous but still catches "never repairs" (the PS11 failure mode).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(4);
    loop {
        if cluster.storages[lagging].last_index() >= leader_last {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "PS11 REGRESSION: idle cluster never repaired the lagging follower \
             (follower last={}, leader last={})",
            cluster.storages[lagging].last_index(),
            leader_last,
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Byte-identical prefix up to the leader's last index.
    let leader_view = cluster.storages[leader].log_view();
    let follower_view = cluster.storages[lagging].log_view();
    assert_eq!(
        leader_view,
        follower_view[..leader_view.len().min(follower_view.len())].to_vec(),
        "repaired follower log must be byte-identical to the leader's"
    );

    // The leader must still be the same node (repair, not re-election chaos).
    let g = cluster.rafts[leader].lock().await;
    assert!(
        g.node().is_leader(),
        "leader {:?} should have survived the idle repair window",
        leader_id
    );
}

// ---------------------------------------------------------------------------
// Test 2 — diverged follower repaired on the idle path, byte-identical.
//
// Node 3 boots with a conflicting uncommitted tail from an old term (idx 5..7
// at term 2); nodes 1/2 hold the authoritative log (idx 5..6 at term 3). With
// ZERO proposals ever issued, the elected leader must walk node 3's
// next_index back via conflict rejects and rebuild its log byte-identical —
// all driven purely by heartbeat ticks.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn diverged_follower_repaired_to_leader_log_on_idle_path() {
    let ids = [1u64, 2, 3];

    // Common committed prefix idx 1..=4 (term 1).
    let prefix: Vec<(u64, u64, &[u8])> =
        vec![(1, 1, b"e1"), (1, 2, b"e2"), (1, 3, b"e3"), (1, 4, b"e4")];

    let s1 = TestStorage::default();
    let s2 = TestStorage::default();
    let s3 = TestStorage::default();

    // Authoritative log on 1 and 2: prefix + idx 5,6 at term 3.
    let mut auth = prefix.clone();
    auth.push((3, 5, b"c5"));
    auth.push((3, 6, b"c6"));
    s1.seed(&auth, 3);
    s2.seed(&auth, 3);

    // Diverged log on 3: prefix + a LONGER conflicting tail from stale term 2.
    let mut diverged = prefix.clone();
    diverged.push((2, 5, b"x5"));
    diverged.push((2, 6, b"x6"));
    diverged.push((2, 7, b"x7"));
    s3.seed(&diverged, 2);

    let cluster = boot_cluster_with_storages(&ids, vec![s1.clone(), s2.clone(), s3.clone()]).await;

    // Election: node 3's last log term (2) is stale vs 1/2 (3), so §5.4.1
    // guarantees the leader is node 1 or node 2.
    let leader = await_leader(&cluster.rafts).await;
    assert!(
        leader != 2,
        "log-stale diverged node must not win the election"
    );

    // ZERO proposals — the ONLY traffic is heartbeat ticks. The diverged
    // follower needs: bare probe reject -> next_index walk-back (one or more
    // rounds) -> behind-branch entry ship -> conflict truncation -> append.
    // Each round costs one 50ms heartbeat tick; 5s bounds it generously.
    let leader_view = cluster.storages[leader].log_view();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if cluster.storages[2].log_view() == leader_view {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "PS11 REGRESSION: diverged follower never repaired on the idle path \
             (follower={:?}, leader={:?})",
            cluster.storages[2].log_view(),
            leader_view,
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // The conflicting tail (x5,x6,x7) must be gone — byte-identical equality
    // above already proves it, but spell out the sharpest invariant: the
    // stale idx-7 entry may not survive.
    assert_eq!(
        cluster.storages[2].last_index(),
        cluster.storages[leader].last_index(),
        "diverged follower must not keep a longer stale tail"
    );
}

// ---------------------------------------------------------------------------
// Test 3 — no steady-state entry traffic when everyone is caught up.
//
// Once all followers are at last_log_index, idle heartbeats must be BARE
// (entry_count == 0). The repair branch may only fire for peers actually
// behind — A9 must not increase steady-state traffic.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn caught_up_cluster_sends_bare_heartbeats_only() {
    let ids = [1u64, 2, 3];
    let cluster = boot_cluster(&ids).await;
    let leader = await_leader(&cluster.rafts).await;

    // Commit some entries with everyone reachable.
    for i in 0..5u8 {
        let mut r = cluster.rafts[leader].lock().await;
        r.propose_once(&[b'w', i]).await.expect("propose failed");
    }

    // Wait until every node holds the full log, then a settle margin so any
    // in-flight append/ack pair drains.
    let leader_last = cluster.storages[leader].last_index();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        if cluster
            .storages
            .iter()
            .all(|s| s.last_index() == leader_last)
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "cluster failed to fully catch up before the steady-state window"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Snoop ~10 heartbeat intervals of a fully idle, fully caught-up cluster.
    cluster.hub.start_snoop();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let (bare, with_entries) = cluster.hub.stop_snoop();

    assert!(
        bare > 0,
        "heartbeats must keep flowing in the idle window (got none)"
    );
    assert_eq!(
        with_entries, 0,
        "steady-state idle heartbeats must carry NO entries when all \
         followers are caught up (saw {with_entries} entry-carrying frames \
         vs {bare} bare heartbeats)"
    );
}
