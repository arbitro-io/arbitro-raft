//! End-to-end tests for the graceful shutdown / drain protocol (A12).
//!
//! Scenarios:
//!   1. A draining LEADER hands leadership to the most caught-up voter BEFORE
//!      the cluster would need a full election timeout: the transfer target
//!      (deterministic — the only fully caught-up follower) leads at exactly
//!      `old_term + 1`, and the draining node ends stopped and non-leader.
//!   2. `commit_waiters` on a draining leader never park forever: writes that
//!      committed resolve `Ok`, writes that cannot commit (acks partitioned
//!      away) resolve `Err` — every future completes within a bound.
//!   3. Draining a FOLLOWER is a clean no-op stop and `run()` returns
//!      `Ok(())` — a graceful shutdown is never surfaced as an error.
//!
//! Harness: same in-memory `NetworkHub` + `RoutingTransport` as the
//! leadership-transfer tests (directional block list for deterministic lag).

use std::collections::{HashMap, HashSet};
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
                addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 9200 + id as u16)),
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

    fn unblock(&self, from: PeerId, to: PeerId) {
        self.blocked.lock().unwrap().remove(&(from, to));
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
type Raft = ArbitroRaft<TestStorage, RoutingTransport, CountingSM>;
type SharedRaft = Arc<tokio::sync::Mutex<Raft>>;

/// The tick-driver pattern `drain()` documents as its concurrency model:
/// one `run_once()` per lock acquisition; a concurrent `drain().await` under
/// the same mutex makes the next tick observe `Ok(false)` and exit cleanly.
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

struct Cluster {
    hub: Arc<NetworkHub>,
    rafts: Vec<SharedRaft>,
    #[allow(dead_code)]
    storages: Vec<TestStorage>,
    #[allow(dead_code)]
    sms: Vec<CountingSM>,
    _guard: AbortOnDrop,
}

async fn boot_cluster(ids: &[u64]) -> Cluster {
    let hub = Arc::new(NetworkHub::new());
    let mut rafts = Vec::new();
    let mut storages = Vec::new();
    let mut sms = Vec::new();
    let mut guard = AbortOnDrop { handles: vec![] };
    for &id in ids {
        let (r, s, sm) = boot_node(hub.clone(), id, ids);
        guard.handles.push(spawn_driver(r.clone()));
        rafts.push(r);
        storages.push(s);
        sms.push(sm);
    }
    tokio::time::sleep(Duration::from_millis(1200)).await;
    Cluster {
        hub,
        rafts,
        storages,
        sms,
        _guard: guard,
    }
}

/// Poll (≤ `budget`) until node `idx` reports leadership, asserting Election
/// Safety (never two distinct leaders for the same term) on the way.
async fn await_specific_leader_with_safety_check(
    rafts: &[SharedRaft],
    idx: usize,
    budget: Duration,
) {
    let deadline = tokio::time::Instant::now() + budget;
    let mut term_leaders: HashMap<u64, u64> = HashMap::new();
    loop {
        for r in rafts.iter() {
            let g = r.lock().await;
            let st = g.status();
            if st.is_leader {
                let prev = term_leaders.insert(st.term.0, st.node_id.0);
                if let Some(prev_leader) = prev {
                    assert_eq!(
                        prev_leader, st.node_id.0,
                        "ELECTION SAFETY VIOLATION: two leaders ({} and {}) observed at term {}",
                        prev_leader, st.node_id.0, st.term.0,
                    );
                }
            }
        }
        {
            let g = rafts[idx].lock().await;
            if g.node().is_leader() {
                return;
            }
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("target node {} did not become leader within budget", idx);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

// ---------------------------------------------------------------------------
// Test 1 — a draining leader hands off to the most caught-up voter BEFORE
// the cluster would need a full election timeout.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn test_leader_drain_hands_off_to_most_caught_up_follower() {
    let ids = [1u64, 2, 3];
    let cluster = boot_cluster(&ids).await;
    let leader = await_leader(&cluster.rafts).await;
    let follower_a = (leader + 1) % ids.len();
    let follower_b = (leader + 2) % ids.len();
    let leader_id = PeerId(ids[leader]);
    let b_id = PeerId(ids[follower_b]);

    // Make follower B lag so follower A is DETERMINISTICALLY the most
    // caught-up voter — the drain must pick A as its transfer target.
    cluster.hub.block(leader_id, b_id);
    for i in 0..5u8 {
        let mut r = cluster.rafts[leader].lock().await;
        r.propose_once(&[b'd', i]).await.expect("propose failed");
    }

    let old_term = cluster.rafts[leader].lock().await.status().term;

    // Drain the leader. While a stopping leader that just vanishes forces the
    // cluster through a full election timeout, the drain hands off inline.
    let started = tokio::time::Instant::now();
    {
        let mut r = cluster.rafts[leader].lock().await;
        r.drain()
            .await
            .expect("drain on a healthy leader must succeed");
        // The draining node ends stopped and non-leader.
        assert!(
            !r.node().is_leader(),
            "draining leader must have handed leadership off before stopping"
        );
        assert!(
            !r.run_once()
                .await
                .expect("run_once after drain must not error"),
            "run_once after drain must report a clean stop (Ok(false))"
        );
    }

    // The transfer target (follower A) must lead — and quickly. A natural
    // (timeout-driven) election could be won by any node and only AFTER a
    // full election timeout of silence; the sanctioned handoff elects
    // exactly A at exactly old_term + 1.
    await_specific_leader_with_safety_check(&cluster.rafts, follower_a, Duration::from_secs(2))
        .await;
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(2),
        "handoff took {elapsed:?} — a graceful drain must move leadership promptly"
    );
    {
        let g = cluster.rafts[follower_a].lock().await;
        let st = g.status();
        assert_eq!(
            st.term.0,
            old_term.0 + 1,
            "a sanctioned handoff campaigns at exactly old_term + 1 \
             (a timeout-driven election would burn extra terms)"
        );
    }

    cluster.hub.unblock(leader_id, b_id);

    // The new leader accepts writes — the cluster survived the drain.
    {
        let mut r = cluster.rafts[follower_a].lock().await;
        r.propose_once(b"post-drain")
            .await
            .expect("new leader must accept proposals after the drain");
    }
}

// ---------------------------------------------------------------------------
// Test 2 — every commit_waiter on a draining leader resolves: committed
// writes succeed, uncommittable ones fail. None park forever.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn test_drain_resolves_or_fails_every_commit_waiter() {
    let ids = [1u64, 2, 3];
    let cluster = boot_cluster(&ids).await;
    let leader = await_leader(&cluster.rafts).await;
    let leader_id = PeerId(ids[leader]);
    let handle = cluster.rafts[leader].lock().await.client_handle();

    // Phase 1 — writes that COMMIT before the drain must resolve Ok.
    for i in 0..3u8 {
        handle
            .write(&[b'k', i])
            .await
            .expect("pre-drain write must commit");
    }

    // Phase 2 — cut every ack path to the leader: entries it replicates from
    // now on can never commit, so their waiters sit in commit_waiters.
    for &fid in &ids {
        if PeerId(fid) != leader_id {
            cluster.hub.block(PeerId(fid), leader_id);
        }
    }
    let mut inflight = Vec::new();
    for i in 0..5u8 {
        let h = handle.clone();
        inflight.push(tokio::spawn(async move { h.write(&[b'u', i]).await }));
    }
    // Let the driver pull them out of the client channel and replicate them
    // into commit_waiters (no acks can arrive, so they cannot commit).
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Drain. The transfer cannot confirm any target caught up (acks are
    // blocked), so it aborts and the drain falls back to a clean stop —
    // which must FAIL the parked waiters rather than strand them.
    {
        let mut r = cluster.rafts[leader].lock().await;
        r.drain()
            .await
            .expect("drain must degrade to a clean stop when no handoff target is reachable");
        assert!(!r.run_once().await.unwrap(), "node must be stopped");
    }

    // EVERY in-flight write resolves within a bound; uncommittable ones fail.
    for (i, fut) in inflight.into_iter().enumerate() {
        let resolved = tokio::time::timeout(Duration::from_secs(5), fut)
            .await
            .unwrap_or_else(|_| panic!("write {i} parked forever across a drain"))
            .expect("write task must not panic");
        assert!(
            resolved.is_err(),
            "write {i} could never reach quorum and must resolve with an error, got {resolved:?}"
        );
    }

    // Writes submitted AFTER the drain fail fast — the intake is closed.
    let late = handle.write(b"too-late").await;
    assert!(
        late.is_err(),
        "a write after drain must fail fast, got {late:?}"
    );

    for &fid in &ids {
        if PeerId(fid) != leader_id {
            cluster.hub.unblock(PeerId(fid), leader_id);
        }
    }
}

// ---------------------------------------------------------------------------
// Test 3 — draining a follower is a clean no-op stop; run() returns Ok(()).
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn test_drain_follower_is_clean_stop_and_run_returns_ok() {
    let ids = [1u64, 2, 3];
    let cluster = boot_cluster(&ids).await;
    let leader = await_leader(&cluster.rafts).await;
    let follower = (leader + 1) % ids.len();

    {
        let mut r = cluster.rafts[follower].lock().await;
        assert!(
            !r.node().is_leader(),
            "harness: picked node must be a follower"
        );
        r.drain()
            .await
            .expect("draining a follower must be a clean no-op stop");
        assert!(!r.node().is_leader());

        // A12 run()-return contract: after a graceful drain the run loop
        // exits with Ok(()) — never an error.
        let run_result = r.run().await;
        assert!(
            matches!(run_result, Ok(())),
            "run() after drain must return Ok(()), got {run_result:?}"
        );

        // Drain is idempotent.
        r.drain()
            .await
            .expect("second drain must be a no-op Ok(())");

        // Intake is closed.
        let handle = r.client_handle();
        drop(r);
        let res = handle.write(b"after-follower-drain").await;
        assert!(res.is_err(), "writes to a drained node must fail fast");
    }

    // The rest of the cluster is unaffected: the leader still leads and
    // commits with the remaining majority.
    {
        let mut r = cluster.rafts[leader].lock().await;
        assert!(
            r.node().is_leader(),
            "leader must be unaffected by a follower drain"
        );
        r.propose_once(b"still-alive")
            .await
            .expect("cluster must keep committing after a follower drains");
    }
}
