//! H7/H2/H6/H9 — MultiRaftDriver: two Raft groups (each a 2-node cluster)
//! multiplexed over ONE shared transport per process, driven share-nothing by
//! one driver per process. Proves: per-frame routing by group_id, independent
//! election + commit per group, no cross-talk, unknown-group frames counted
//! and dropped, and the per-group idle memory diet (no MB-class buffers on an
//! idle group).

use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arbitro_raft::api::registry::MultiRaftDriver;
use arbitro_raft::{
    BootstrapPeer, ClusterId, EntryPayload, GroupId, HardState, LimitsConfig, LogEntry, LogIndex,
    NodeConfig, PeerId, RaftError, RaftStorage, RaftTransport, SnapshotMeta, StateMachine, Term,
    TimingConfig,
};
use tokio::sync::mpsc;

// ---------------------------------------------------------------------------
// AbortOnDrop — prevent task leakage / hung test threads.
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
// CountingSM — counts apply() calls per group.
// ---------------------------------------------------------------------------
#[derive(Default, Clone)]
struct CountingSM {
    count: Arc<AtomicUsize>,
}

impl StateMachine for CountingSM {
    fn apply(&mut self, _entry: &[u8]) -> Result<(), RaftError> {
        self.count.fetch_add(1, Ordering::SeqCst);
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
// In-process storage — same shape as tests/multi_raft_registry.rs.
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
            let payload: &'a [u8] = &payload_buf[..e.payload.len()];
            Ok(Some(LogEntry {
                term: e.term,
                index: e.index,
                payload: EntryPayload(payload),
            }))
        } else {
            Ok(None)
        }
    }
}

// ---------------------------------------------------------------------------
// NetTransport — one shared in-process transport per "process". All groups on
// a process send/recv through this single instance; frames carry group_id in
// the header, exactly like a real multiplexed socket.
// ---------------------------------------------------------------------------
struct NetTransport {
    peers: Mutex<HashMap<u64, mpsc::UnboundedSender<Vec<u8>>>>,
    rx: tokio::sync::Mutex<mpsc::UnboundedReceiver<Vec<u8>>>,
}

impl NetTransport {
    fn pair() -> (
        Arc<NetTransport>,
        Arc<NetTransport>,
        mpsc::UnboundedSender<Vec<u8>>,
    ) {
        let (tx_a, rx_a) = mpsc::unbounded_channel();
        let (tx_b, rx_b) = mpsc::unbounded_channel();
        let a = Arc::new(NetTransport {
            peers: Mutex::new(HashMap::from([(2u64, tx_b)])),
            rx: tokio::sync::Mutex::new(rx_a),
        });
        let b = Arc::new(NetTransport {
            peers: Mutex::new(HashMap::from([(1u64, tx_a.clone())])),
            rx: tokio::sync::Mutex::new(rx_b),
        });
        // tx_a: raw injection handle into process A's inbound queue.
        (a, b, tx_a)
    }

    fn send_to(&self, peer: PeerId, frame: Vec<u8>) -> Result<(), RaftError> {
        let peers = self.peers.lock().unwrap();
        match peers.get(&peer.0) {
            Some(tx) => {
                let _ = tx.send(frame); // peer gone == frame lost, fine
                Ok(())
            }
            None => Err(RaftError::Transport(format!("unknown peer {}", peer.0))),
        }
    }
}

impl RaftTransport for NetTransport {
    fn send_vectored(
        &self,
        peer: PeerId,
        slices: &[&[u8]],
    ) -> impl std::future::Future<Output = Result<(), RaftError>> + Send {
        let frame: Vec<u8> = slices.concat();
        let res = self.send_to(peer, frame);
        async move { res }
    }

    fn send_frame_owned(
        &self,
        peer: PeerId,
        frame: bytes::Bytes,
    ) -> impl std::future::Future<Output = Result<(), RaftError>> + Send {
        let res = self.send_to(peer, frame.to_vec());
        async move { res }
    }

    fn recv_frame(
        &self,
        out: &mut [u8],
    ) -> impl std::future::Future<Output = Result<usize, RaftError>> + Send {
        async move {
            let mut rx = self.rx.lock().await;
            match rx.recv().await {
                Some(frame) => {
                    if out.len() < frame.len() {
                        return Err(RaftError::Transport("recv buffer too small".into()));
                    }
                    out[..frame.len()].copy_from_slice(&frame);
                    Ok(frame.len())
                }
                None => Err(RaftError::Transport("transport closed".into())),
            }
        }
    }

    fn recv_frame_timeout(
        &self,
        timeout: Duration,
        out: &mut [u8],
    ) -> impl std::future::Future<Output = Result<Option<usize>, RaftError>> + Send {
        async move {
            let mut rx = self.rx.lock().await;
            match tokio::time::timeout(timeout, rx.recv()).await {
                Err(_) => Ok(None),
                Ok(None) => Err(RaftError::Transport("transport closed".into())),
                Ok(Some(frame)) => {
                    if out.len() < frame.len() {
                        return Err(RaftError::Transport("recv buffer too small".into()));
                    }
                    out[..frame.len()].copy_from_slice(&frame);
                    Ok(Some(frame.len()))
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
fn two_node_config(node_id: u64) -> NodeConfig {
    NodeConfig {
        cluster_id: ClusterId(7),
        node_id: PeerId(node_id),
        peers: vec![PeerId(1), PeerId(2)],
        learners: Vec::new(),
        bootstrap_peers: vec![
            BootstrapPeer {
                id: PeerId(1),
                addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 9001)),
            },
            BootstrapPeer {
                id: PeerId(2),
                addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 9002)),
            },
        ],
        timing: TimingConfig {
            heartbeat_ms: 20,
            // Elections are test-driven (driver.campaign); a huge randomized
            // window keeps the timer tick from ever campaigning on its own.
            election_min_ms: 30_000,
            election_max_ms: 30_500,
        },
        limits: LimitsConfig::default(),
    }
}

const G1: GroupId = GroupId(1);
const G2: GroupId = GroupId(2);

/// Two groups, each a 2-node cluster spanning "process" A (node 1) and
/// "process" B (node 2), one shared transport per process, one driver per
/// process. Group 1 commits 3 entries, group 2 commits 5 — independently,
/// with all frames multiplexed on the same two queues.
#[tokio::test(flavor = "multi_thread")]
async fn test_two_groups_one_driver_route_elect_commit_no_crosstalk() {
    let (net_a, net_b, raw_inject_a) = NetTransport::pair();

    let mut driver_a: MultiRaftDriver<TestStorage, NetTransport, CountingSM> =
        MultiRaftDriver::new(net_a);
    let mut driver_b: MultiRaftDriver<TestStorage, NetTransport, CountingSM> =
        MultiRaftDriver::new(net_b);

    let a_counts = [Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0))];
    let b_counts = [Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0))];

    for (i, gid) in [G1, G2].into_iter().enumerate() {
        driver_a
            .add_group(
                gid,
                two_node_config(1),
                TestStorage::default(),
                CountingSM {
                    count: a_counts[i].clone(),
                },
            )
            .unwrap();
        driver_b
            .add_group(
                gid,
                two_node_config(2),
                TestStorage::default(),
                CountingSM {
                    count: b_counts[i].clone(),
                },
            )
            .unwrap();
    }

    // H6: freshly-added groups are on the memory diet — zero MB-class bytes.
    assert_eq!(driver_a.group_idle_scratch_bytes(G1), Some(0));
    assert_eq!(driver_a.group_idle_scratch_bytes(G2), Some(0));

    // Process B runs autonomously: routes every inbound frame to its group,
    // steps it, applies commits.
    let b_task = tokio::spawn(async move {
        loop {
            if driver_b.run_once(Duration::from_millis(5)).await.is_err() {
                break;
            }
        }
    });
    let _guard = AbortOnDrop {
        handles: vec![b_task],
    };

    // Elect A's node as leader of BOTH groups — each election runs a real
    // RequestVote round-trip through the shared multiplexed transport.
    assert!(driver_a.campaign(G1).await.unwrap(), "G1 election failed");
    assert!(driver_a.campaign(G2).await.unwrap(), "G2 election failed");
    assert!(driver_a.group(G1).unwrap().is_leader());
    assert!(driver_a.group(G2).unwrap().is_leader());

    // Commit DIFFERENT entry counts per group: 3 in G1, 5 in G2. Each propose
    // is a full replicate→quorum-ack round trip over the shared queues.
    for i in 0..3u32 {
        let idx = driver_a.propose(G1, &i.to_le_bytes()).await.unwrap();
        assert_eq!(idx, LogIndex(u64::from(i) + 1));
    }
    for i in 0..5u32 {
        let idx = driver_a.propose(G2, &i.to_le_bytes()).await.unwrap();
        assert_eq!(idx, LogIndex(u64::from(i) + 1));
    }

    // Leader-side state: independent commit indexes, independent SM applies.
    assert_eq!(driver_a.group(G1).unwrap().commit_index(), LogIndex(3));
    assert_eq!(driver_a.group(G2).unwrap().commit_index(), LogIndex(5));
    assert_eq!(a_counts[0].load(Ordering::SeqCst), 3);
    assert_eq!(a_counts[1].load(Ordering::SeqCst), 5);

    // Pump A so its per-group heartbeats advertise leader_commit; B's driver
    // routes each heartbeat to the right group and applies. Poll until B's
    // state machines converge.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        driver_a.run_once(Duration::from_millis(5)).await.unwrap();
        let (b1, b2) = (
            b_counts[0].load(Ordering::SeqCst),
            b_counts[1].load(Ordering::SeqCst),
        );
        if b1 == 3 && b2 == 5 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "follower state machines never converged: G1={b1} (want 3), G2={b2} (want 5)"
        );
    }

    // No cross-talk: exact counts on both sides, distinct per group.
    assert_eq!(b_counts[0].load(Ordering::SeqCst), 3);
    assert_eq!(b_counts[1].load(Ordering::SeqCst), 5);

    // H9: a frame naming a group this driver does not host is counted and
    // dropped — no panic, no mis-route, no state change.
    let before = driver_a.unknown_group_frames();
    let mut rogue = vec![0u8; 40];
    rogue[24..32].copy_from_slice(&99u64.to_le_bytes());
    raw_inject_a.send(rogue).unwrap();
    for _ in 0..5 {
        driver_a.run_once(Duration::from_millis(2)).await.unwrap();
        if driver_a.unknown_group_frames() > before {
            break;
        }
    }
    assert_eq!(driver_a.unknown_group_frames(), before + 1);

    // A garbage frame TAGGED with a live group is dropped at decode without
    // touching the group's state.
    let mut junk = vec![0xAAu8; 48];
    junk[24..32].copy_from_slice(&G1.0.to_le_bytes());
    raw_inject_a.send(junk).unwrap();
    for _ in 0..5 {
        driver_a.run_once(Duration::from_millis(2)).await.unwrap();
    }
    assert_eq!(driver_a.group(G1).unwrap().commit_index(), LogIndex(3));
    assert_eq!(driver_a.group(G2).unwrap().commit_index(), LogIndex(5));
    assert!(driver_a.group(G1).unwrap().is_leader());

    // H6: after real traffic, idle groups STILL hold zero MB-class bytes —
    // the shared scratch always returned to the driver.
    assert_eq!(driver_a.group_idle_scratch_bytes(G1), Some(0));
    assert_eq!(driver_a.group_idle_scratch_bytes(G2), Some(0));

    // remove_group restores standalone buffers on the returned node.
    let (_node, _sm) = driver_a.remove_group(G2).unwrap();
    assert_eq!(driver_a.len(), 1);
    assert!(driver_a.group(G2).is_none());
}

/// H6 cancellation-safety: a `propose` future dropped at an `.await` INSIDE
/// the scratch lend window must return the driver's MB-class buffers through
/// the RAII guard — the group holds zero idle scratch bytes afterwards
/// (nothing stranded, no O(groups x 17 MiB) leak) and stays fully usable
/// (a subsequent propose still commits).
#[tokio::test]
async fn test_cancelled_propose_returns_scratch_and_group_survives() {
    use std::future::Future;

    let (net_a, net_b, _raw) = NetTransport::pair();
    let mut driver_a: MultiRaftDriver<TestStorage, NetTransport, CountingSM> =
        MultiRaftDriver::new(net_a);
    let mut driver_b: MultiRaftDriver<TestStorage, NetTransport, CountingSM> =
        MultiRaftDriver::new(net_b);

    let a_count = Arc::new(AtomicUsize::new(0));
    driver_a
        .add_group(
            G1,
            two_node_config(1),
            TestStorage::default(),
            CountingSM {
                count: a_count.clone(),
            },
        )
        .unwrap();
    driver_b
        .add_group(
            G1,
            two_node_config(2),
            TestStorage::default(),
            CountingSM::default(),
        )
        .unwrap();

    // Follower loop on "process" B.
    let b_task = tokio::spawn(async move {
        loop {
            if driver_b.run_once(Duration::from_millis(5)).await.is_err() {
                break;
            }
        }
    });
    let _guard = AbortOnDrop {
        handles: vec![b_task],
    };

    assert!(driver_a.campaign(G1).await.unwrap(), "election failed");
    assert!(driver_a.group(G1).unwrap().is_leader());
    assert_eq!(driver_a.group_idle_scratch_bytes(G1), Some(0));

    // Start a propose and cancel it at its first internal `.await`: on this
    // current-thread runtime one manual poll appends + replicates the entry,
    // then parks awaiting quorum acks (the follower task cannot run during a
    // synchronous poll), so dropping the future here is a cancellation
    // exactly inside the lend window.
    {
        let mut fut = std::pin::pin!(driver_a.propose(G1, b"cancelled-mid-flight"));
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(
            fut.as_mut().poll(&mut cx).is_pending(),
            "propose must park awaiting quorum acks"
        );
        // `fut` dropped here — cancellation mid-await.
    }

    // The RAII guard returned the MB-class scratch to the driver on drop:
    // the idle group holds ZERO scratch bytes, not ~17 MiB.
    assert_eq!(
        driver_a.group_idle_scratch_bytes(G1),
        Some(0),
        "cancelled propose stranded the driver scratch on the node"
    );

    // The group is still usable: a subsequent propose commits. The cancelled
    // propose had already appended entry 1, so this one lands at index 2 and
    // committing it applies both entries.
    let idx = driver_a.propose(G1, b"after-cancel").await.unwrap();
    assert_eq!(idx, LogIndex(2));
    assert!(driver_a.group(G1).unwrap().commit_index() >= idx);
    assert_eq!(a_count.load(Ordering::SeqCst), 2);
    assert_eq!(driver_a.group_idle_scratch_bytes(G1), Some(0));
}
