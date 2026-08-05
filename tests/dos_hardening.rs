//! D3 DoS-hardening tests — per-peer decode-error jail in the run loop.
//!
//! A hostile (or badly version-skewed) peer that floods the node with
//! undecodable garbage must not get a full decode + warn-log per frame
//! forever: after `limits.inbound_decode_error_jail_threshold` decode errors
//! inside one error window the sender is jailed for
//! `limits.inbound_jail_cooldown_ms`, and its frames are shed pre-decode
//! (`frames_shed_jailed` metric). Senders that are not current members all
//! share one "unknown" accounting bucket, so spoofing random `from` ids
//! cannot grow the tracking map, and jailing the unknown bucket never
//! affects a real member.
//!
//! All tests drive `ArbitroRaft::run_once` through a deterministic inbox
//! transport — no spawned cluster, no scheduler dependence.

use std::collections::VecDeque;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arbitro_raft::{
    ArbitroRaft, BootstrapPeer, ClusterId, EntryPayload, HardState, LimitsConfig, LogEntry,
    LogIndex, NodeConfig, NoopStateMachine, PeerId, RaftError, RaftNode, RaftStorage,
    RaftTransport, SnapshotMeta, Term, TimingConfig, RAFT_FRAME_HEADER_SIZE,
};

// ---------------------------------------------------------------------------
// TestStorage — in-memory storage (same shape as tests/snapshot_hardening.rs).
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
            let shared: &'a [u8] = &payload_buf[..e.payload.len()];
            Ok(Some(LogEntry {
                term: e.term,
                index: e.index,
                payload: EntryPayload(shared),
            }))
        } else {
            Ok(None)
        }
    }
}

// ---------------------------------------------------------------------------
// InboxTransport — frames are popped from a seeded inbox; sends are dropped.
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
struct InboxTransport {
    inbox: Arc<Mutex<VecDeque<Vec<u8>>>>,
}

impl InboxTransport {
    fn push_inbound(&self, frame: Vec<u8>) {
        self.inbox.lock().unwrap().push_back(frame);
    }
}

impl RaftTransport for InboxTransport {
    fn send_vectored(
        &self,
        _peer: PeerId,
        _slices: &[&[u8]],
    ) -> impl std::future::Future<Output = Result<(), RaftError>> + Send {
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
        out: &mut [u8],
    ) -> impl std::future::Future<Output = Result<usize, RaftError>> + Send {
        let inbox = self.inbox.clone();
        async move {
            match inbox.lock().unwrap().pop_front() {
                Some(data) => {
                    out[..data.len()].copy_from_slice(&data);
                    Ok(data.len())
                }
                None => Err(RaftError::Transport("inbox empty".into())),
            }
        }
    }
    fn recv_frame_timeout(
        &self,
        _timeout: Duration,
        out: &mut [u8],
    ) -> impl std::future::Future<Output = Result<Option<usize>, RaftError>> + Send {
        let inbox = self.inbox.clone();
        async move {
            match inbox.lock().unwrap().pop_front() {
                Some(data) => {
                    out[..data.len()].copy_from_slice(&data);
                    Ok(Some(data.len()))
                }
                None => Ok(None),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers.
// ---------------------------------------------------------------------------

fn config_3node(node_id: u64, limits: LimitsConfig) -> NodeConfig {
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
                addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 9300 + id as u16)),
            })
            .collect(),
        // Long election timeout so no test tick ever campaigns.
        timing: TimingConfig {
            heartbeat_ms: 10,
            election_min_ms: 60_000,
            election_max_ms: 90_000,
        },
        limits,
    }
}

fn jail_limits(threshold: u32, window_ms: u64, cooldown_ms: u64) -> LimitsConfig {
    LimitsConfig {
        inbound_decode_error_jail_threshold: threshold,
        inbound_decode_error_window_ms: window_ms,
        inbound_jail_cooldown_ms: cooldown_ms,
        ..LimitsConfig::default()
    }
}

/// A 32-byte frame with an INVALID magic and the given claimed sender id —
/// decodes to `Err(Protocol("invalid raft frame magic"))` while still
/// carrying an attributable header (`from` at offset 8).
fn garbage_frame(from: u64) -> Vec<u8> {
    let mut f = vec![0u8; RAFT_FRAME_HEADER_SIZE];
    // magic[0..4] left as zeros => invalid.
    f[4] = 0x01; // version
    f[5] = 1; // kind = RequestVote
    f[8..16].copy_from_slice(&from.to_le_bytes());
    // body_len = 0, reserved = 0, group_id = 0.
    f
}

fn follower_raft(
    limits: LimitsConfig,
) -> (
    ArbitroRaft<TestStorage, InboxTransport, NoopStateMachine>,
    InboxTransport,
) {
    let storage = TestStorage::default();
    let transport = InboxTransport::default();
    let node = RaftNode::new(config_3node(1, limits), storage, transport.clone()).unwrap();
    (ArbitroRaft::new(node, NoopStateMachine), transport)
}

/// Drive `run_once` until the inbox is drained (each follower tick bursts up
/// to 128 frames).
async fn drain_inbox(
    raft: &mut ArbitroRaft<TestStorage, InboxTransport, NoopStateMachine>,
    transport: &InboxTransport,
) {
    while !transport.inbox.lock().unwrap().is_empty() {
        raft.run_once().await.expect("run_once must not error");
    }
}

// ---------------------------------------------------------------------------
// Tests.
// ---------------------------------------------------------------------------

/// (b) A peer flooding decode errors is jailed at the threshold and every
/// subsequent frame is shed pre-decode — the loop does not pay a decode +
/// warn-log per garbage frame forever.
#[tokio::test]
async fn garbage_flood_from_one_peer_is_jailed_and_shed() {
    let (mut raft, transport) = follower_raft(jail_limits(8, 10_000, 60_000));

    for _ in 0..100 {
        transport.push_inbound(garbage_frame(2));
    }
    drain_inbox(&mut raft, &transport).await;

    let m = raft.metrics().snapshot();
    assert_eq!(
        m.frames_dropped_nonfatal, 8,
        "exactly threshold frames get the full decode-error path"
    );
    assert_eq!(m.peers_jailed, 1, "one jail event for the flooding peer");
    assert_eq!(
        m.frames_shed_jailed, 92,
        "the remaining flood is shed pre-decode"
    );
}

/// The jail is temporary: after the cooldown the peer's frames are decoded
/// (and counted) again instead of being shed.
#[tokio::test]
async fn jail_releases_after_cooldown() {
    let (mut raft, transport) = follower_raft(jail_limits(4, 10_000, 100));

    for _ in 0..10 {
        transport.push_inbound(garbage_frame(2));
    }
    drain_inbox(&mut raft, &transport).await;
    let m = raft.metrics().snapshot();
    assert_eq!(m.frames_dropped_nonfatal, 4);
    assert_eq!(m.peers_jailed, 1);
    assert_eq!(m.frames_shed_jailed, 6);

    tokio::time::sleep(Duration::from_millis(200)).await;

    transport.push_inbound(garbage_frame(2));
    drain_inbox(&mut raft, &transport).await;
    let m = raft.metrics().snapshot();
    assert_eq!(
        m.frames_dropped_nonfatal, 5,
        "post-cooldown frame must be decoded (and counted), not shed"
    );
    assert_eq!(
        m.frames_shed_jailed, 6,
        "no additional shedding after release"
    );
}

/// Frames claiming a NON-member id share one "unknown" bucket: they can jail
/// the bucket, but never a real member — and the tracking map cannot be
/// grown by spoofing random ids.
#[tokio::test]
async fn unknown_sender_bucket_never_jails_members() {
    let (mut raft, transport) = follower_raft(jail_limits(4, 10_000, 60_000));

    // Flood with random non-member ids — all fold into the unknown bucket.
    for i in 0..10u64 {
        transport.push_inbound(garbage_frame(1_000_000 + i));
    }
    drain_inbox(&mut raft, &transport).await;
    let m = raft.metrics().snapshot();
    assert_eq!(m.frames_dropped_nonfatal, 4);
    assert_eq!(m.peers_jailed, 1, "the unknown bucket is jailed once");
    assert_eq!(m.frames_shed_jailed, 6);

    // A member's frame still reaches decode while the unknown bucket sits
    // in jail.
    transport.push_inbound(garbage_frame(2));
    drain_inbox(&mut raft, &transport).await;
    let m = raft.metrics().snapshot();
    assert_eq!(
        m.frames_dropped_nonfatal, 5,
        "member frames are unaffected by the unknown-bucket jail"
    );
    assert_eq!(m.frames_shed_jailed, 6);
}

/// `inbound_decode_error_jail_threshold = 0` disables the cutoff entirely —
/// the historical drop-and-continue behavior, byte for byte.
#[tokio::test]
async fn threshold_zero_disables_jailing() {
    let (mut raft, transport) = follower_raft(jail_limits(0, 10_000, 60_000));

    for _ in 0..50 {
        transport.push_inbound(garbage_frame(2));
    }
    drain_inbox(&mut raft, &transport).await;

    let m = raft.metrics().snapshot();
    assert_eq!(m.frames_dropped_nonfatal, 50);
    assert_eq!(m.peers_jailed, 0);
    assert_eq!(m.frames_shed_jailed, 0);
}
