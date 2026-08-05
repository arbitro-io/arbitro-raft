//! # Per-core multi-Raft blueprint (`MultiRaftDriver`)
//!
//! This example is the deployment blueprint for running MANY Raft groups on
//! ONE core with share-nothing ownership. The pattern:
//!
//! - **One driver per core.** Each core (or dedicated task/thread) owns one
//!   `MultiRaftDriver` and one shared transport. There is no cross-core
//!   locking: a group lives on exactly one driver, and everything that group
//!   needs — node state, storage handle, state machine, timers — is owned by
//!   that driver. Scaling out is `N cores -> N drivers -> many groups each`.
//!
//! - **Groups come and go at runtime.** `add_group(gid, config, storage, sm)`
//!   registers a group on the driver and immediately puts it on the memory
//!   diet (below); `remove_group(gid)` hands back the node + state machine
//!   with standalone buffers restored, so a group can be migrated to another
//!   driver/core without restarting anything.
//!
//! - **The loop is `run_once`: demux -> step -> apply.** Every inbound frame
//!   carries its `group_id` in the fixed header. One `run_once(max_wait)`
//!   call drains parked + live frames from the shared transport, routes each
//!   by group id (O(1) map hit, no allocation, no lock), steps the owning
//!   group, applies newly committed entries to that group's state machine,
//!   and finally services per-group heartbeat/election timers. A frame naming
//!   a group this driver does not host is counted
//!   (`unknown_group_frames`) and dropped before decode — never mis-routed.
//!
//! - **Driver-owned scratch, lent per step.** The driver holds ONE set of
//!   MB-class buffers (~33 MiB) for the whole core. When a group is stepped,
//!   the shared buffers are `mem::swap`ed in; when the step ends they swap
//!   back out. An idle group therefore holds ~KB, not ~MB — this example
//!   prints `idle_scratch_bytes=0` per group to prove it. Big-buffer memory
//!   is O(cores), not O(groups): 1000 idle groups on one core still cost one
//!   ~33 MiB scratch set plus ~100 KiB each.
//!
//! Below, one process pair (A = node 1, B = node 2) hosts THREE independent
//! 2-node Raft groups multiplexed over a single in-process transport per
//! side. A is elected leader of every group, then commits a different number
//! of entries per group (2 / 4 / 6) to show independent progress and zero
//! cross-talk on the shared queues. See `docs/MULTI_RAFT.md` and
//! `tests/multi_driver.rs` for the full contract.
//!
//! Run with: `cargo run --release --example multiraft_driver`

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

// --- CountingSM: counts apply() calls per group -----------------------------
#[derive(Clone)]
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

// --- In-memory storage (compact copy of the tests/multi_driver.rs harness) --
#[derive(Clone)]
struct StoredEntry {
    term: Term,
    index: LogIndex,
    payload: Vec<u8>,
}

#[derive(Clone, Default)]
struct MemStorage {
    hard_state: Arc<Mutex<Option<HardState>>>,
    entries: Arc<Mutex<Vec<StoredEntry>>>,
}

impl RaftStorage for MemStorage {
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

// --- NetTransport: ONE shared in-process transport per "process" ------------
// All groups on a process share this single instance; frames carry group_id
// in the header, exactly like one multiplexed socket.
struct NetTransport {
    peers: Mutex<HashMap<u64, mpsc::UnboundedSender<Vec<u8>>>>,
    rx: tokio::sync::Mutex<mpsc::UnboundedReceiver<Vec<u8>>>,
}

impl NetTransport {
    fn pair() -> (Arc<NetTransport>, Arc<NetTransport>) {
        let (tx_a, rx_a) = mpsc::unbounded_channel();
        let (tx_b, rx_b) = mpsc::unbounded_channel();
        let a = Arc::new(NetTransport {
            peers: Mutex::new(HashMap::from([(2u64, tx_b)])),
            rx: tokio::sync::Mutex::new(rx_a),
        });
        let b = Arc::new(NetTransport {
            peers: Mutex::new(HashMap::from([(1u64, tx_a)])),
            rx: tokio::sync::Mutex::new(rx_b),
        });
        (a, b)
    }

    fn send_to(&self, peer: PeerId, frame: Vec<u8>) -> Result<(), RaftError> {
        match self.peers.lock().unwrap().get(&peer.0) {
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
        let res = self.send_to(peer, slices.concat());
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

// --- Config: 2-node cluster; elections are example-driven (campaign) --------
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
            // Huge window: the timer never self-campaigns; we call campaign().
            election_min_ms: 30_000,
            election_max_ms: 30_500,
        },
        limits: LimitsConfig::default(),
    }
}

/// (group id, entries to commit) — a DIFFERENT count per group.
const GROUPS: [(GroupId, usize); 3] = [(GroupId(1), 2), (GroupId(2), 4), (GroupId(3), 6)];

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    // One shared transport per "process"; one driver per "core".
    let (net_a, net_b) = NetTransport::pair();
    let mut driver_a: MultiRaftDriver<MemStorage, NetTransport, CountingSM> =
        MultiRaftDriver::new(net_a);
    let mut driver_b: MultiRaftDriver<MemStorage, NetTransport, CountingSM> =
        MultiRaftDriver::new(net_b);

    let a_counts: Vec<Arc<AtomicUsize>> = GROUPS
        .iter()
        .map(|_| Arc::new(AtomicUsize::new(0)))
        .collect();
    let b_counts: Vec<Arc<AtomicUsize>> = GROUPS
        .iter()
        .map(|_| Arc::new(AtomicUsize::new(0)))
        .collect();

    // Runtime group registration: 3 groups on each driver, one shared
    // transport per side. add_group also puts each group on the memory diet.
    for (i, (gid, _)) in GROUPS.into_iter().enumerate() {
        driver_a
            .add_group(
                gid,
                two_node_config(1),
                MemStorage::default(),
                CountingSM {
                    count: a_counts[i].clone(),
                },
            )
            .expect("add_group A");
        driver_b
            .add_group(
                gid,
                two_node_config(2),
                MemStorage::default(),
                CountingSM {
                    count: b_counts[i].clone(),
                },
            )
            .expect("add_group B");
    }

    // Process B is a pure run_once loop: demux -> step -> apply.
    let b_task = tokio::spawn(async move {
        loop {
            if driver_b.run_once(Duration::from_millis(5)).await.is_err() {
                break;
            }
        }
    });

    // Elect A leader of each group (a real RequestVote round-trip per group
    // over the shared queues), then commit 2 / 4 / 6 entries respectively.
    for (gid, entries) in GROUPS {
        assert!(
            driver_a.campaign(gid).await.expect("campaign"),
            "election failed for group {}",
            gid.0
        );
        for i in 0..entries as u64 {
            let idx = driver_a
                .propose(gid, &i.to_le_bytes())
                .await
                .expect("propose");
            assert_eq!(idx, LogIndex(i + 1));
        }
    }

    // Pump A so per-group heartbeats advertise leader_commit; wait (bounded)
    // until B's three state machines converge to 2 / 4 / 6.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        driver_a
            .run_once(Duration::from_millis(5))
            .await
            .expect("run_once");
        let done = GROUPS
            .iter()
            .enumerate()
            .all(|(i, (_, want))| b_counts[i].load(Ordering::SeqCst) == *want);
        if done {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "follower state machines never converged"
        );
    }
    b_task.abort();

    // Independent progress per group + the memory diet: idle scratch is 0.
    for (i, (gid, _)) in GROUPS.into_iter().enumerate() {
        let node = driver_a.group(gid).expect("group");
        assert!(node.is_leader());
        println!(
            "group {}: leader={} commit={} applied={} idle_scratch_bytes={}",
            gid.0,
            node.node_id().0,
            node.commit_index().0,
            a_counts[i].load(Ordering::SeqCst),
            driver_a.group_idle_scratch_bytes(gid).expect("scratch")
        );
    }
    println!(
        "followers converged: applied {:?} across {} groups, unknown_group_frames={}",
        b_counts
            .iter()
            .map(|c| c.load(Ordering::SeqCst))
            .collect::<Vec<_>>(),
        driver_a.len(),
        driver_a.unknown_group_frames()
    );
}
