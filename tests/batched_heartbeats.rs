//! Proves that `RaftGroupRegistry::tick_heartbeats` (F2) coalesces per-peer
//! heartbeat sends across N groups into a single `RaftTransport::send_vectored`
//! call per unique peer, regardless of how many groups this node leads.
//!
//! See `src/api/node/replication/heartbeat_batch.rs` (line ~131) for the
//! two-slices-per-frame (header + AppendEntries body) wire layout this test's
//! `slice_count` assertions are anchored to.

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arbitro_raft::api::RaftGroupRegistry;
use arbitro_raft::{
    BootstrapPeer, ClusterId, EntryPayload, GroupId, HardState, LimitsConfig, LogEntry, LogIndex,
    NodeConfig, NoopStateMachine, PeerId, RaftError, RaftNode, RaftStorage, RaftTransport,
    SnapshotMeta, Term, TimingConfig,
};

// ---------------------------------------------------------------------------
// CountingTransport — records one (peer, slice_count, total_bytes) entry per
// `send_vectored` call. Everything else is a no-op / never-ready stub, since
// this test never drives an inbound recv path.
// ---------------------------------------------------------------------------

#[derive(Default)]
struct CountingTransport {
    sends: Arc<Mutex<Vec<(PeerId, usize, usize)>>>,
}

impl RaftTransport for CountingTransport {
    fn send_vectored(
        &self,
        peer: PeerId,
        slices: &[&[u8]],
    ) -> impl std::future::Future<Output = Result<(), RaftError>> + Send {
        let total: usize = slices.iter().map(|s| s.len()).sum();
        self.sends.lock().unwrap().push((peer, slices.len(), total));
        async move { Ok(()) }
    }

    fn send_frame_owned(
        &self,
        _peer: PeerId,
        _frame: bytes::Bytes,
    ) -> impl std::future::Future<Output = Result<(), RaftError>> + Send {
        async move { Ok(()) }
    }

    fn recv_frame(
        &self,
        _out: &mut [u8],
    ) -> impl std::future::Future<Output = Result<usize, RaftError>> + Send {
        async move {
            Err(RaftError::Transport(
                "CountingTransport has no inbound path".into(),
            ))
        }
    }

    fn recv_frame_timeout(
        &self,
        _timeout: Duration,
        _out: &mut [u8],
    ) -> impl std::future::Future<Output = Result<Option<usize>, RaftError>> + Send {
        async move { Ok(None) }
    }
}

// `RaftNode` owns its transport by value, but the test needs every group's
// node to share the SAME `CountingTransport` instance so all sends land in
// one `sends` log. Integration tests compile as their own crate, so neither
// `RaftTransport` nor `std::sync::Arc` is local here — `impl RaftTransport
// for Arc<CountingTransport>` would violate the orphan rule. `SharedTransport`
// is a local newtype around `Arc<CountingTransport>` that sidesteps that:
// every clone is cheap (Arc) and forwards to the same inner instance.
#[derive(Clone, Default)]
struct SharedTransport(Arc<CountingTransport>);

impl RaftTransport for SharedTransport {
    fn send_vectored(
        &self,
        peer: PeerId,
        slices: &[&[u8]],
    ) -> impl std::future::Future<Output = Result<(), RaftError>> + Send {
        self.0.send_vectored(peer, slices)
    }

    fn send_frame_owned(
        &self,
        peer: PeerId,
        frame: bytes::Bytes,
    ) -> impl std::future::Future<Output = Result<(), RaftError>> + Send {
        self.0.send_frame_owned(peer, frame)
    }

    fn recv_frame(
        &self,
        out: &mut [u8],
    ) -> impl std::future::Future<Output = Result<usize, RaftError>> + Send {
        self.0.recv_frame(out)
    }

    fn recv_frame_timeout(
        &self,
        timeout: Duration,
        out: &mut [u8],
    ) -> impl std::future::Future<Output = Result<Option<usize>, RaftError>> + Send {
        self.0.recv_frame_timeout(timeout, out)
    }
}

// ---------------------------------------------------------------------------
// TestStorage — minimal in-memory `RaftStorage`, mirroring the pattern used
// throughout tests/raft_correctness.rs and tests/multi_raft_registry.rs.
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
// Helpers
// ---------------------------------------------------------------------------

/// Self is `PeerId(1)`, remote peers are `PeerId(2)` and `PeerId(3)`.
fn three_node_config(group: GroupId) -> NodeConfig {
    let peers_id = [1u64, 2, 3];
    NodeConfig {
        node_id: PeerId(1),
        cluster_id: ClusterId(group.0),
        peers: peers_id.iter().copied().map(PeerId).collect(),
        learners: Vec::new(),
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

/// Builds a 5-group registry, all sharing `transport`, and returns it
/// alongside the group ids in insertion order.
fn build_five_group_registry(
    transport: &SharedTransport,
) -> RaftGroupRegistry<TestStorage, SharedTransport, NoopStateMachine> {
    let mut registry = RaftGroupRegistry::new();
    for i in 1..=5u64 {
        let gid = GroupId(i);
        let node = RaftNode::new(
            three_node_config(gid),
            TestStorage::default(),
            transport.clone(),
        )
        .unwrap();
        registry.insert(gid, node, NoopStateMachine).unwrap();
    }
    registry
}

const HEADER_BYTES: usize = 32; // RAFT_FRAME_HEADER_SIZE
const BODY_BYTES: usize = 48; // size_of::<AppendEntries>()
const SLICES_PER_FRAME: usize = 2; // header + body, see heartbeat_batch.rs:131

// ---------------------------------------------------------------------------
// Test 1: all 5 groups lead -> one send_vectored per unique remote peer,
// each carrying 5 groups worth of coalesced frames.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn test_batched_heartbeats_one_send_per_peer() {
    let shared = SharedTransport::default();
    let sends_log = Arc::clone(&shared.0.sends);
    let transport_param: Arc<SharedTransport> = Arc::new(shared.clone());

    let mut registry = build_five_group_registry(&shared);

    for i in 1..=5u64 {
        registry
            .get_mut(GroupId(i))
            .unwrap()
            .become_leader_for_benchmark(Term(1));
    }

    let total_frames = registry.tick_heartbeats(&transport_param).await.unwrap();

    assert_eq!(
        total_frames, 10,
        "5 groups * 2 remote peers = 10 heartbeat frames"
    );

    let sends = sends_log.lock().unwrap();
    assert_eq!(
        sends.len(),
        2,
        "exactly one send_vectored call per unique remote peer, self excluded"
    );

    let mut peers: Vec<PeerId> = sends.iter().map(|(p, _, _)| *p).collect();
    peers.sort_by_key(|p| p.0);
    assert_eq!(peers, vec![PeerId(2), PeerId(3)]);

    for &(peer, slice_count, total_bytes) in sends.iter() {
        assert_eq!(
            slice_count,
            5 * SLICES_PER_FRAME,
            "peer {:?}: 5 groups x 2 slices/group coalesced into one call",
            peer
        );
        assert_eq!(
            total_bytes,
            5 * (HEADER_BYTES + BODY_BYTES),
            "peer {:?}: 5 x (32-byte header + 48-byte body)",
            peer
        );
    }
}

// ---------------------------------------------------------------------------
// Test 2: only 3 of 5 groups lead -> followers emit no heartbeat frames, but
// sends are still coalesced to one call per unique remote peer.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn test_batched_heartbeats_skip_follower_groups() {
    let shared = SharedTransport::default();
    let sends_log = Arc::clone(&shared.0.sends);
    let transport_param: Arc<SharedTransport> = Arc::new(shared.clone());

    let mut registry = build_five_group_registry(&shared);

    // Only groups 1..=3 become leader; groups 4 and 5 stay followers.
    for i in 1..=3u64 {
        registry
            .get_mut(GroupId(i))
            .unwrap()
            .become_leader_for_benchmark(Term(1));
    }

    let total_frames = registry.tick_heartbeats(&transport_param).await.unwrap();

    assert_eq!(
        total_frames, 6,
        "3 leading groups * 2 remote peers = 6 heartbeat frames"
    );

    let sends = sends_log.lock().unwrap();
    assert_eq!(
        sends.len(),
        2,
        "exactly one send_vectored call per unique remote peer, self excluded"
    );

    let mut peers: Vec<PeerId> = sends.iter().map(|(p, _, _)| *p).collect();
    peers.sort_by_key(|p| p.0);
    assert_eq!(peers, vec![PeerId(2), PeerId(3)]);

    for &(peer, slice_count, total_bytes) in sends.iter() {
        assert_eq!(
            slice_count,
            3 * SLICES_PER_FRAME,
            "peer {:?}: only the 3 leading groups contribute frames",
            peer
        );
        assert_eq!(
            total_bytes,
            3 * (HEADER_BYTES + BODY_BYTES),
            "peer {:?}: 3 x (32-byte header + 48-byte body)",
            peer
        );
    }
}
