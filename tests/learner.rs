//! A13 — learner (non-voting member) role.
//!
//! Proves the four contract points of the learner phase:
//!
//! 1. A learner receives and stores replicated entries (catching up from
//!    behind) but is NOT counted in the commit quorum — in a 3-voter +
//!    1-learner cluster an entry acked by only the leader and the learner
//!    does not commit.
//! 2. A learner never wins or forces an election, and a candidate cannot
//!    reach majority through a learner's (forged or real) vote grant —
//!    the exclusion is enforced on the counting side.
//! 3. `promote_learner` turns a caught-up learner into a voter through the
//!    normal §4.3 joint transition, and THEN it counts toward quorum.
//! 4. Adding a learner to a 3-node cluster does not change the commit
//!    threshold — availability is preserved.

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arbitro_raft::{
    encode_message_to_bytes, ArbitroRaft, BootstrapPeer, ClusterId, EntryPayload, HardState,
    LimitsConfig, LogEntry, LogIndex, NodeConfig, PeerId, RaftError, RaftMessage, RaftStorage,
    RaftTransport, RequestVoteResp, SnapshotMeta, StateMachine, Term, TimingConfig,
};

// ---------------------------------------------------------------------------
// NoopSM
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
// Config builder — voters + learners.
// ---------------------------------------------------------------------------
fn make_config(node_id: u64, peers: &[u64], learners: &[u64]) -> NodeConfig {
    NodeConfig {
        node_id: PeerId(node_id),
        cluster_id: ClusterId(1),
        peers: peers.iter().copied().map(PeerId).collect(),
        learners: learners.iter().copied().map(PeerId).collect(),
        bootstrap_peers: peers
            .iter()
            .chain(learners.iter())
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
// NetworkHub & RoutingTransport — controllable in-memory delivery.
// ---------------------------------------------------------------------------
struct NetworkHub {
    senders: Mutex<
        std::collections::HashMap<PeerId, tokio::sync::mpsc::UnboundedSender<(PeerId, Vec<u8>)>>,
    >,
    /// Peers whose traffic (both directions) is silently dropped.
    blocked: Mutex<std::collections::HashSet<PeerId>>,
}

impl NetworkHub {
    fn new() -> Self {
        Self {
            senders: Mutex::new(std::collections::HashMap::new()),
            blocked: Mutex::new(std::collections::HashSet::new()),
        }
    }

    fn register(
        &self,
        peer: PeerId,
        sender: tokio::sync::mpsc::UnboundedSender<(PeerId, Vec<u8>)>,
    ) {
        self.senders.lock().unwrap().insert(peer, sender);
    }

    fn set_blocked(&self, peers: &[PeerId]) {
        let mut blocked = self.blocked.lock().unwrap();
        blocked.clear();
        blocked.extend(peers.iter().copied());
    }

    fn send(&self, from: PeerId, to: PeerId, data: Vec<u8>) {
        {
            let blocked = self.blocked.lock().unwrap();
            if blocked.contains(&from) || blocked.contains(&to) {
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

fn boot_node(
    hub: Arc<NetworkHub>,
    id: u64,
    peers: &[u64],
    learners: &[u64],
) -> (SharedRaft, TestStorage) {
    let peer_id = PeerId(id);
    let config = make_config(id, peers, learners);
    let storage = TestStorage::default();

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    hub.register(peer_id, tx);

    let transport = RoutingTransport {
        from: peer_id,
        hub,
        rx: Arc::new(tokio::sync::Mutex::new(rx)),
    };

    let node = arbitro_raft::RaftNode::new(config, storage.clone(), transport).unwrap();
    let raft = ArbitroRaft::new(node, NoopSM);
    (Arc::new(tokio::sync::Mutex::new(raft)), storage)
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
// Test 1 — a learner replicates (catching up from behind) but its ack never
// counts toward the commit quorum.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn test_learner_replicates_but_never_counts_in_commit_quorum() {
    let hub = Arc::new(NetworkHub::new());
    let voters = [1u64, 2, 3];

    let mut rafts: Vec<SharedRaft> = Vec::new();
    let mut storages: Vec<TestStorage> = Vec::new();
    let mut guard = AbortOnDrop { handles: vec![] };

    for id in voters {
        let (r, s) = boot_node(hub.clone(), id, &voters, &[]);
        guard.handles.push(spawn_driver(r.clone()));
        rafts.push(r);
        storages.push(s);
    }

    tokio::time::sleep(Duration::from_millis(1200)).await;
    let leader = await_leader(&rafts).await;

    // Build committed history BEFORE the learner exists, so the learner has
    // a real backlog to catch up on.
    for i in 0..5u8 {
        let payload = [b'x', i];
        let mut r = rafts[leader].lock().await;
        r.propose_once(&payload).await.expect("app propose failed");
    }

    // Boot node 4 as a LEARNER (its own config: voters [1,2,3], learner self).
    let (r4, s4) = boot_node(hub.clone(), 4, &voters, &[4]);
    guard.handles.push(spawn_driver(r4.clone()));
    rafts.push(r4.clone());

    let add_idx = {
        let mut r = rafts[leader].lock().await;
        let idx = r.add_learner(PeerId(4)).await.expect("add_learner failed");
        assert_eq!(r.learners(), &[PeerId(4)], "leader learner set not updated");
        assert_eq!(
            r.node().peers(),
            &[PeerId(1), PeerId(2), PeerId(3)],
            "add_learner must not touch the voter set"
        );
        idx
    };

    // (1a) The learner catches up from behind: its storage reaches the
    // add-learner entry (5 app entries + the control entry) via the
    // heartbeat-driven repair path.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let (last, _) = s4.last_log_position().unwrap();
        if last >= add_idx {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "learner never caught up: storage last_log {} < add_idx {}",
                last.0, add_idx.0
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // (1b) THE PIN — block both non-leader voters. Reachable acks for a new
    // entry are now {leader (self), learner}. That is 1 voter of 3: the
    // entry must NOT commit, even though the learner acks it. Retried
    // (heal + re-pin) if leadership shifts under the block, mirroring the
    // dual-quorum membership test.
    let mut pinned: Option<(usize, LogIndex, LogIndex)> = None;
    for _attempt in 0..10 {
        let leader = await_leader(&rafts[..3]).await;
        let leader_id = PeerId(voters[leader]);
        let blocked: Vec<PeerId> = voters
            .iter()
            .copied()
            .map(PeerId)
            .filter(|p| *p != leader_id)
            .collect();
        assert_eq!(blocked.len(), 2);

        let mut r = rafts[leader].lock().await;
        if !r.node().is_leader() {
            drop(r);
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        hub.set_blocked(&blocked);
        let commit_before = r.commit_index();
        let res = r.propose_once(b"learner-ack-must-not-commit").await;
        let entry_idx = r.status().last_log_index;
        let commit_after = r.commit_index();
        drop(r);

        match res {
            Ok(idx) => panic!(
                "propose committed at {} with only the leader and a learner \
                 reachable — learner counted in commit quorum",
                idx.0
            ),
            Err(_) if entry_idx > commit_before => {
                assert_eq!(
                    commit_after, commit_before,
                    "commit advanced from {} to {} on leader+learner acks alone",
                    commit_before.0, commit_after.0,
                );
                pinned = Some((leader, commit_before, entry_idx));
                break;
            }
            Err(_) => {
                // Deposed before the append (leadership raced the block) —
                // heal and retry the whole pin.
                hub.set_blocked(&[]);
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
    let (leader, commit_before, entry_idx) =
        pinned.expect("could not pin the learner-quorum scenario within retry budget");

    // The learner DID receive and store the uncommitted entry (replication
    // reaches it) — while its own commit index stays below the entry.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        let (last, _) = s4.last_log_position().unwrap();
        if last >= entry_idx {
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "learner never received the blocked-quorum entry: last_log {} < {}",
                last.0, entry_idx.0
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    {
        let g = r4.lock().await;
        assert!(
            g.commit_index() < entry_idx,
            "learner commit_index {} reached the uncommitted entry {}",
            g.commit_index().0,
            entry_idx.0,
        );
    }
    // And the leader still has not committed it.
    {
        let g = rafts[leader].lock().await;
        assert!(
            g.commit_index() <= commit_before,
            "leader commit advanced to {} while a voter majority was unreachable",
            g.commit_index().0,
        );
    }

    // Heal and confirm the cluster recovers (a voter majority commits again).
    hub.set_blocked(&[]);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if tokio::time::Instant::now() >= deadline {
            panic!("cluster did not recover a committing leader after heal");
        }
        let Some(l) = find_leader(&rafts[..3]).await else {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        };
        let mut r = rafts[l].lock().await;
        if r.propose_once(b"post-heal").await.is_ok() {
            break;
        }
        drop(r);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

// ---------------------------------------------------------------------------
// Test 2 — a learner never campaigns, and a candidate cannot reach majority
// through a learner's vote grant (counting-side enforcement).
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn test_learner_never_wins_or_supplies_election_majority() {
    let hub = Arc::new(NetworkHub::new());

    // Voters {1, 2}, learner 3. Node 2 is NEVER booted — the only way node 1
    // could reach the 2-vote majority is by counting the learner's grant.
    let (r1, s1) = boot_node(hub.clone(), 1, &[1, 2], &[3]);
    let (r3, s3) = boot_node(hub.clone(), 3, &[1, 2], &[3]);
    let mut guard = AbortOnDrop { handles: vec![] };
    // Only the learner gets a driver; node 1 is driven manually so the
    // campaign timing is deterministic.
    guard.handles.push(spawn_driver(r3.clone()));

    let learner_metrics = r3.lock().await.metrics();

    // (2a) The learner never campaigns: after many election timeouts its
    // term is untouched and it never left the follower role.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    {
        let g = r3.lock().await;
        let st = g.status();
        assert!(!st.is_leader, "learner became leader");
        assert_eq!(
            st.term.0, 0,
            "learner term inflated to {} — it campaigned",
            st.term.0
        );
        assert_eq!(
            learner_metrics.snapshot().elections_started,
            0,
            "learner started an election"
        );
    }
    assert_eq!(
        s3.load_hard_state().unwrap().current_term.0,
        0,
        "learner persisted a campaign term"
    );

    // (2b) THE PIN — forge a granted RequestVoteResp from the learner for the
    // term node 1 is about to campaign in (term 1), park it in node 1's
    // inbox, then campaign. votes_needed = 2; granted = {self} + the
    // learner's grant. Without the counting-side membership filter that is
    // "2 of 2" and node 1 crowns itself; with it the campaign must fail.
    let forged = RequestVoteResp {
        term: 1u64.into(),
        vote_granted: 1,
        _pad: [0; 7],
    };
    let frame = encode_message_to_bytes(PeerId(3), &RaftMessage::RequestVoteResp(&forged))
        .expect("encode forged vote");
    hub.send(PeerId(3), PeerId(1), frame.to_vec());

    {
        let mut r = r1.lock().await;
        let res = r.campaign_once().await;
        let won = matches!(res, Ok(true));
        assert!(
            !won,
            "candidate won an election counting a learner's vote grant"
        );
        assert!(
            !r.node().is_leader(),
            "candidate holds leadership after a learner-supplied 'majority'"
        );
    }
    assert_eq!(
        s1.load_hard_state().unwrap().current_term.0,
        1,
        "campaign should have bumped node 1 to term 1 and no further"
    );
}

// ---------------------------------------------------------------------------
// Test 3 — promote_learner turns the learner into a voter, and THEN its ack
// counts toward the (grown) quorum.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn test_promote_learner_then_counts_toward_quorum() {
    let hub = Arc::new(NetworkHub::new());
    let voters = [1u64, 2, 3];

    let mut rafts: Vec<SharedRaft> = Vec::new();
    let mut guard = AbortOnDrop { handles: vec![] };

    for id in voters {
        let (r, _s) = boot_node(hub.clone(), id, &voters, &[]);
        guard.handles.push(spawn_driver(r.clone()));
        rafts.push(r);
    }

    tokio::time::sleep(Duration::from_millis(1200)).await;
    let leader = await_leader(&rafts).await;

    for i in 0..3u8 {
        let payload = [b'y', i];
        let mut r = rafts[leader].lock().await;
        r.propose_once(&payload).await.expect("app propose failed");
    }

    let (r4, _s4) = boot_node(hub.clone(), 4, &voters, &[4]);
    guard.handles.push(spawn_driver(r4.clone()));
    rafts.push(r4);

    {
        let mut r = rafts[leader].lock().await;
        r.add_learner(PeerId(4)).await.expect("add_learner failed");
    }

    // Wait for the caught-up predicate — the operator contract gating
    // promotion.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        {
            let r = rafts[leader].lock().await;
            if matches!(r.learner_caught_up(PeerId(4)), Ok(true)) {
                break;
            }
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("learner never reported caught up");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Promote — runs the full joint transition to C_new = {1,2,3,4}.
    {
        let mut r = rafts[leader].lock().await;
        r.promote_learner(PeerId(4))
            .await
            .expect("promote_learner failed");
        assert!(
            r.learners().is_empty(),
            "promoted peer still in the leader's learner set"
        );
        assert!(
            r.node().peers().contains(&PeerId(4)),
            "promoted peer missing from the leader's voter set"
        );
        assert_eq!(r.status().voter_count, 4, "voter count did not grow to 4");
    }

    // Every node converges on the 4-voter configuration.
    let expected: Vec<PeerId> = [1u64, 2, 3, 4].iter().copied().map(PeerId).collect();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    'outer: loop {
        let mut ok = true;
        for r in rafts.iter() {
            let g = r.lock().await;
            if g.node().peers() != expected.as_slice() {
                ok = false;
            }
        }
        if ok {
            break 'outer;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("cluster did not converge on the promoted 4-voter set");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // THE PIN — quorum(4) = 3. Block one ORIGINAL non-leader voter: the
    // remaining acks are {leader, one original voter, promoted node}. The
    // propose can only commit if the promoted node's ack now COUNTS.
    let leader_id = PeerId(voters[leader]);
    let blocked = voters
        .iter()
        .copied()
        .map(PeerId)
        .find(|p| *p != leader_id)
        .expect("a non-leader original voter exists");
    hub.set_blocked(&[blocked]);

    {
        let mut r = rafts[leader].lock().await;
        let commit_before = r.commit_index();
        let idx = r
            .propose_once(b"promoted-voter-completes-quorum")
            .await
            .expect(
                "propose failed with 3 of 4 voters reachable — the promoted \
                 learner's ack is not being counted as a voter ack",
            );
        assert!(idx > commit_before);
        assert!(r.commit_index() >= idx, "entry reported but not committed");
    }
    hub.set_blocked(&[]);
}

// ---------------------------------------------------------------------------
// Test 4 — adding a learner does not change the commit threshold of the
// voter set (availability preserved).
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn test_add_learner_preserves_commit_threshold() {
    let hub = Arc::new(NetworkHub::new());
    let voters = [1u64, 2, 3];

    let mut rafts: Vec<SharedRaft> = Vec::new();
    let mut guard = AbortOnDrop { handles: vec![] };

    for id in voters {
        let (r, _s) = boot_node(hub.clone(), id, &voters, &[]);
        guard.handles.push(spawn_driver(r.clone()));
        rafts.push(r);
    }

    tokio::time::sleep(Duration::from_millis(1200)).await;
    let leader = await_leader(&rafts).await;

    let (r4, _s4) = boot_node(hub.clone(), 4, &voters, &[4]);
    guard.handles.push(spawn_driver(r4.clone()));

    {
        let mut r = rafts[leader].lock().await;
        r.add_learner(PeerId(4)).await.expect("add_learner failed");
        // The voter set — and with it the commit threshold — is untouched.
        assert_eq!(
            r.status().voter_count,
            3,
            "voter_count changed on add_learner"
        );
        assert_eq!(r.node().peers(), &[PeerId(1), PeerId(2), PeerId(3)]);
    }

    // THE PIN — with one voter down, a 3-voter cluster still commits on
    // 2 voter acks. Had the learner joined as a 4th VOTER (the pre-A13
    // behavior), quorum would be 3 and this propose could only succeed by
    // counting the (possibly far-behind) newcomer — the availability dip
    // learners exist to avoid.
    let leader_id = PeerId(voters[leader]);
    let blocked = voters
        .iter()
        .copied()
        .map(PeerId)
        .find(|p| *p != leader_id)
        .expect("a non-leader voter exists");
    hub.set_blocked(&[blocked]);

    {
        let mut r = rafts[leader].lock().await;
        let commit_before = r.commit_index();
        let idx = r
            .propose_once(b"threshold-unchanged")
            .await
            .expect("3-voter cluster with 1 voter down failed to commit after add_learner");
        assert!(idx > commit_before);
        assert!(r.commit_index() >= idx);
    }
    hub.set_blocked(&[]);
}
