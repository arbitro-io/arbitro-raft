//! End-to-end tests for leadership transfer (Raft §4.2.3, `TimeoutNow`).
//!
//! Scenarios:
//!   1. Transfer to an already-caught-up follower — the target becomes leader,
//!      the old leader steps down, and at most one leader exists per term
//!      throughout the handoff.
//!   2. Transfer to a lagging follower — the transfer first replicates the
//!      backlog, then hands off.
//!   3. Transfer to a follower that CANNOT catch up (partitioned) — the
//!      transfer aborts with `TransferTimeout` and leadership is unchanged.
//!   4. Transfer initiated on a non-leader — `NotLeader`; unknown target —
//!      `PeerUnknown`.
//!   5. A proposal arriving mid-transfer is rejected with a redirect hint at
//!      the incoming leader and, when retried there, commits exactly once.
//!
//! Harness: same in-memory `NetworkHub` + `RoutingTransport` used by the
//! membership tests, extended with a directional block list so a follower can
//! be made to lag (or stay unreachable) deterministically.

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
                addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 9100 + id as u16)),
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
    storages: Vec<TestStorage>,
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

/// Poll (≤ `budget`) until node `idx` reports leadership, recording every
/// observed `(term → leader)` claim on the way and asserting Election Safety
/// (never two distinct leaders for the same term). Panics on timeout.
async fn await_specific_leader_with_safety_check(
    rafts: &[SharedRaft],
    idx: usize,
    budget: Duration,
) {
    let deadline = tokio::time::Instant::now() + budget;
    let mut term_leaders: HashMap<u64, u64> = HashMap::new();
    loop {
        let mut target_leads = false;
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
                target_leads = true;
            }
        }
        if target_leads {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("target node {} did not become leader within budget", idx);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

// ---------------------------------------------------------------------------
// Test 1 — transfer to an already-caught-up follower.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn test_transfer_to_caught_up_follower() {
    let ids = [1u64, 2, 3];
    let cluster = boot_cluster(&ids).await;
    let leader = await_leader(&cluster.rafts).await;

    // Establish a committed log.
    for i in 0..5u8 {
        let mut r = cluster.rafts[leader].lock().await;
        r.propose_once(&[b'x', i]).await.expect("propose failed");
    }

    let target = (leader + 1) % ids.len();
    let old_term = cluster.rafts[leader].lock().await.status().term;

    {
        let mut r = cluster.rafts[leader].lock().await;
        r.transfer_leadership(PeerId(ids[target]))
            .await
            .expect("transfer to caught-up follower must succeed");
    }

    await_specific_leader_with_safety_check(&cluster.rafts, target, Duration::from_secs(3)).await;

    // Old leader must have stepped down; the new leader's term is strictly higher.
    {
        let old = cluster.rafts[leader].lock().await;
        assert!(
            !old.node().is_leader(),
            "old leader must be a follower after the handoff"
        );
    }
    {
        let new = cluster.rafts[target].lock().await;
        assert!(
            new.status().term > old_term,
            "the target must lead at a strictly higher term ({} vs {})",
            new.status().term.0,
            old_term.0,
        );
    }

    // The new leader accepts proposals.
    {
        let mut r = cluster.rafts[target].lock().await;
        r.propose_once(b"post-transfer")
            .await
            .expect("propose on new leader failed");
    }
}

// ---------------------------------------------------------------------------
// Test 2 — transfer to a lagging follower: it is caught up first.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn test_transfer_to_lagging_follower_catches_up_first() {
    let ids = [1u64, 2, 3];
    let cluster = boot_cluster(&ids).await;
    let leader = await_leader(&cluster.rafts).await;
    let target = (leader + 1) % ids.len();
    let leader_id = PeerId(ids[leader]);
    let target_id = PeerId(ids[target]);

    // Make the target lag: drop everything the leader sends it, then commit
    // 5 entries via the remaining majority.
    cluster.hub.block(leader_id, target_id);
    let mut last_idx = LogIndex(0);
    for i in 0..5u8 {
        let mut r = cluster.rafts[leader].lock().await;
        last_idx = r.propose_once(&[b'y', i]).await.expect("propose failed");
    }
    // The target must genuinely be behind before the transfer.
    {
        let (target_last, _) = cluster.storages[target].last_log_position().unwrap();
        assert!(
            target_last < last_idx,
            "harness failure: target was expected to lag (has {}, leader committed {})",
            target_last.0,
            last_idx.0,
        );
    }

    // Heal the link; the transfer itself must replicate the backlog.
    cluster.hub.unblock(leader_id, target_id);
    {
        let mut r = cluster.rafts[leader].lock().await;
        r.transfer_leadership(target_id)
            .await
            .expect("transfer must catch the lagging follower up and succeed");
    }

    await_specific_leader_with_safety_check(&cluster.rafts, target, Duration::from_secs(3)).await;

    // The target holds the full log and applies every committed entry once.
    let (target_last, _) = cluster.storages[target].last_log_position().unwrap();
    assert!(
        target_last >= last_idx,
        "new leader's log ({}) must include every transferred entry (≥ {})",
        target_last.0,
        last_idx.0,
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        if cluster.sms[target].counter.load(Ordering::SeqCst) >= 5 {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "new leader applied {} entries, expected 5",
                cluster.sms[target].counter.load(Ordering::SeqCst)
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        cluster.sms[target].counter.load(Ordering::SeqCst),
        5,
        "entries must be applied exactly once on the new leader"
    );
}

// ---------------------------------------------------------------------------
// Test 3 — target cannot catch up: transfer aborts, leadership unchanged.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn test_transfer_aborts_when_target_cannot_catch_up() {
    let ids = [1u64, 2, 3];
    let cluster = boot_cluster(&ids).await;
    let leader = await_leader(&cluster.rafts).await;
    let target = (leader + 1) % ids.len();
    let leader_id = PeerId(ids[leader]);
    let target_id = PeerId(ids[target]);

    // Partition the target from the leader and commit entries it cannot get.
    cluster.hub.block(leader_id, target_id);
    for i in 0..3u8 {
        let mut r = cluster.rafts[leader].lock().await;
        r.propose_once(&[b'z', i]).await.expect("propose failed");
    }

    let term_before = cluster.rafts[leader].lock().await.status().term;
    let err = {
        let mut r = cluster.rafts[leader].lock().await;
        r.transfer_leadership(target_id)
            .await
            .expect_err("transfer to an unreachable target must abort")
    };
    assert!(
        matches!(err, RaftError::TransferTimeout(t) if t == target_id),
        "expected TransferTimeout({}), got {err:?}",
        target_id.0,
    );

    // Leadership is unchanged: same node, same term, and proposals flow again
    // immediately (no freeze is left behind by an aborted transfer).
    {
        let mut r = cluster.rafts[leader].lock().await;
        assert!(r.node().is_leader(), "leader must retain leadership on abort");
        assert_eq!(
            r.status().term,
            term_before,
            "an aborted transfer must not burn a term"
        );
        r.propose_once(b"after-abort")
            .await
            .expect("leader must accept proposals right after an aborted transfer");
    }

    cluster.hub.unblock(leader_id, target_id);
}

// ---------------------------------------------------------------------------
// Test 4 — invalid initiations: not leader / unknown target.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn test_transfer_rejected_when_not_leader_or_target_unknown() {
    let ids = [1u64, 2, 3];
    let cluster = boot_cluster(&ids).await;
    let leader = await_leader(&cluster.rafts).await;
    let follower = (leader + 1) % ids.len();
    let other = (leader + 2) % ids.len();

    // A follower must refuse to initiate.
    {
        let mut r = cluster.rafts[follower].lock().await;
        let err = r
            .transfer_leadership(PeerId(ids[other]))
            .await
            .expect_err("a follower must not initiate a transfer");
        assert!(
            matches!(err, RaftError::NotLeader { .. }),
            "expected NotLeader, got {err:?}"
        );
    }

    // The leader must refuse a target outside the voter set.
    {
        let mut r = cluster.rafts[leader].lock().await;
        let err = r
            .transfer_leadership(PeerId(99))
            .await
            .expect_err("a non-voter target must be rejected");
        assert!(
            matches!(err, RaftError::PeerUnknown(PeerId(99))),
            "expected PeerUnknown(99), got {err:?}"
        );
        // Both rejections are non-destructive: still the leader.
        assert!(r.node().is_leader());
    }
}

// ---------------------------------------------------------------------------
// Test 5 — a proposal arriving mid-transfer is neither lost nor duplicated.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn test_proposal_mid_transfer_not_lost_or_double_committed() {
    let ids = [1u64, 2, 3];
    let cluster = boot_cluster(&ids).await;
    let leader = await_leader(&cluster.rafts).await;
    let target = (leader + 1) % ids.len();
    let target_id = PeerId(ids[target]);

    // Committed baseline: 3 entries applied everywhere.
    for i in 0..3u8 {
        let mut r = cluster.rafts[leader].lock().await;
        r.propose_once(&[b'b', i]).await.expect("propose failed");
    }

    // Initiate the transfer and, while the handoff window is open (we still
    // hold the old leader's lock, so its state cannot change under us), submit
    // a proposal: it must be REJECTED with a redirect hint at the incoming
    // leader — not silently appended into a log about to be handed off.
    {
        let mut r = cluster.rafts[leader].lock().await;
        r.transfer_leadership(target_id)
            .await
            .expect("transfer must succeed");
        let err = r
            .propose_once(b"mid-transfer")
            .await
            .expect_err("proposals must be frozen during the handoff window");
        match err {
            RaftError::NotLeader { leader_hint } => {
                assert_eq!(
                    leader_hint.map(|h| h.leader_id),
                    Some(target_id),
                    "the freeze rejection must redirect clients at the transfer target"
                );
            }
            other => panic!("expected NotLeader with redirect hint, got {other:?}"),
        }
    }

    await_specific_leader_with_safety_check(&cluster.rafts, target, Duration::from_secs(3)).await;

    // Retry the rejected proposal against the new leader — exactly-once end state.
    {
        let mut r = cluster.rafts[target].lock().await;
        r.propose_once(b"mid-transfer")
            .await
            .expect("retried proposal must commit on the new leader");
    }

    // Every node converges to exactly 4 applied entries: 3 baseline + 1
    // retried. 5 would mean the frozen proposal leaked into the old leader's
    // log (double commit); 3 would mean the retry was lost.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let counts: Vec<u64> = cluster
            .sms
            .iter()
            .map(|sm| sm.counter.load(Ordering::SeqCst))
            .collect();
        if counts.iter().all(|&c| c == 4) {
            break;
        }
        assert!(
            counts.iter().all(|&c| c <= 4),
            "double commit detected: applied counts {counts:?} exceed 4"
        );
        if tokio::time::Instant::now() >= deadline {
            panic!("nodes did not converge to exactly 4 applied entries: {counts:?}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let _ = &cluster.storages; // keep storages alive for the whole test
}
