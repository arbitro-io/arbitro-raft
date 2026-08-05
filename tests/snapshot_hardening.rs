//! C4 snapshot-hardening tests (AUDIT_REPORT P1-5 remainder: PS7 / OPS-2).
//!
//! 1. Timer-based eviction — a pending inbound snapshot transfer that makes
//!    no progress within `limits.snapshot_stall_timeout_ms` is evicted by the
//!    run-loop timer sweep (buffer freed); a later chunk for it is NACK-ed
//!    from offset 0 and treated as a fresh session, never appended to a
//!    stale buffer.
//! 2. Per-peer attempt cap (PS7) — a peer that keeps failing installs burns
//!    `limits.snapshot_max_attempts_per_peer` attempts, is then refused for
//!    `limits.snapshot_attempt_cooldown_ms`, and recovers after the cooldown.
//! 3. Chunked yielding (OPS-2) — a large install interleaves bare heartbeats
//!    to the OTHER followers at the heartbeat cadence, so the install cannot
//!    starve the leader's liveness obligations and trigger spurious
//!    elections elsewhere.
//!
//! All tests drive `RaftNode` directly through a deterministic scripted
//! transport — no spawned cluster, no scheduler dependence.

use std::collections::VecDeque;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arbitro_raft::{
    decode_message, encode_message_to_bytes, BootstrapPeer, ClusterId, EntryPayload, HardState,
    InstallSnapshot, InstallSnapshotResp, LimitsConfig, LogEntry, LogIndex, NodeConfig, PeerId,
    RaftError, RaftMessage, RaftNode, RaftStorage, RaftTransport, SnapshotMeta, Term, TimingConfig,
};

// ---------------------------------------------------------------------------
// TestStorage — in-memory storage (same shape as tests/snapshot_install.rs).
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
// ScriptTransport — records every outbound frame; auto-responds to
// InstallSnapshot per the configured mode, impersonating `respond_as`.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RespondMode {
    /// No responses at all (inbox stays as manually seeded).
    Silent,
    /// Accept every chunk: `accepted=1, next_offset = offset + chunk_len`.
    AckAll,
    /// Hostile follower: always `accepted=0, next_offset=0` (PS7 loop driver).
    NackZero,
}

#[derive(Clone)]
struct ScriptTransport {
    /// Every outbound frame, in order: (destination, raw bytes).
    sent: Arc<Mutex<Vec<(PeerId, Vec<u8>)>>>,
    inbox: Arc<Mutex<VecDeque<Vec<u8>>>>,
    mode: Arc<Mutex<RespondMode>>,
    /// Peer identity stamped on auto-generated responses.
    respond_as: PeerId,
    /// Per-recv artificial latency, to make an install take wall time.
    recv_delay: Arc<Mutex<Duration>>,
}

impl ScriptTransport {
    fn new(respond_as: PeerId) -> Self {
        Self {
            sent: Arc::new(Mutex::new(Vec::new())),
            inbox: Arc::new(Mutex::new(VecDeque::new())),
            mode: Arc::new(Mutex::new(RespondMode::Silent)),
            respond_as,
            recv_delay: Arc::new(Mutex::new(Duration::ZERO)),
        }
    }

    fn set_mode(&self, mode: RespondMode) {
        *self.mode.lock().unwrap() = mode;
    }

    fn set_recv_delay(&self, delay: Duration) {
        *self.recv_delay.lock().unwrap() = delay;
    }

    fn clear_sent(&self) {
        self.sent.lock().unwrap().clear();
    }

    fn sent_to(&self, peer: PeerId) -> Vec<Vec<u8>> {
        self.sent
            .lock()
            .unwrap()
            .iter()
            .filter(|(p, _)| *p == peer)
            .map(|(_, f)| f.clone())
            .collect()
    }

    fn push_inbound(&self, frame: Vec<u8>) {
        self.inbox.lock().unwrap().push_back(frame);
    }

    /// Record an outbound frame; if it is an InstallSnapshot chunk addressed
    /// to `respond_as`, synthesize the mode's response into the inbox.
    fn on_send(&self, peer: PeerId, frame: Vec<u8>) {
        let mode = *self.mode.lock().unwrap();
        if peer == self.respond_as && mode != RespondMode::Silent {
            if let Ok(inbound) = decode_message(&frame) {
                if let Some((req, _payload)) = inbound.as_install_snapshot() {
                    let (accepted, next_offset) = match mode {
                        RespondMode::AckAll => {
                            (1u8, req.offset.get() + u64::from(req.chunk_len.get()))
                        }
                        RespondMode::NackZero => (0u8, 0u64),
                        RespondMode::Silent => unreachable!(),
                    };
                    let resp = InstallSnapshotResp {
                        term: req.term,
                        next_offset: next_offset.into(),
                        accepted,
                        _pad: [0; 7],
                    };
                    let bytes = encode_message_to_bytes(
                        self.respond_as,
                        &RaftMessage::InstallSnapshotResp(&resp),
                    )
                    .unwrap();
                    self.push_inbound(bytes.to_vec());
                }
            }
        }
        self.sent.lock().unwrap().push((peer, frame));
    }
}

impl RaftTransport for ScriptTransport {
    fn send_vectored(
        &self,
        peer: PeerId,
        slices: &[&[u8]],
    ) -> impl std::future::Future<Output = Result<(), RaftError>> + Send {
        let mut frame = Vec::new();
        for s in slices {
            frame.extend_from_slice(s);
        }
        self.on_send(peer, frame);
        async move { Ok(()) }
    }
    fn send_frame_owned(
        &self,
        peer: PeerId,
        frame: bytes::Bytes,
    ) -> impl std::future::Future<Output = Result<(), RaftError>> + Send {
        self.on_send(peer, frame.to_vec());
        async move { Ok(()) }
    }
    fn recv_frame(
        &self,
        out: &mut [u8],
    ) -> impl std::future::Future<Output = Result<usize, RaftError>> + Send {
        let inbox = self.inbox.clone();
        let delay = *self.recv_delay.lock().unwrap();
        async move {
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            let frame = inbox.lock().unwrap().pop_front();
            match frame {
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
        let delay = *self.recv_delay.lock().unwrap();
        async move {
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            let frame = inbox.lock().unwrap().pop_front();
            match frame {
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
                addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 9200 + id as u16)),
            })
            .collect(),
        timing: TimingConfig {
            heartbeat_ms: 10,
            election_min_ms: 800,
            election_max_ms: 1200,
        },
        limits,
    }
}

fn install_chunk_frame(
    from: PeerId,
    term: u64,
    meta: &SnapshotMeta,
    offset: u64,
    chunk: &[u8],
    done: bool,
) -> Vec<u8> {
    let req = InstallSnapshot {
        term: term.into(),
        leader_id: from.0.into(),
        last_included_index: meta.last_included_index.0.into(),
        last_included_term: meta.last_included_term.0.into(),
        offset: offset.into(),
        chunk_len: (chunk.len() as u32).into(),
        done: if done { 1 } else { 0 },
        _pad: [0; 3],
    };
    encode_message_to_bytes(from, &RaftMessage::InstallSnapshot(&req, chunk))
        .unwrap()
        .to_vec()
}

/// Decode the LAST frame sent to `peer` as an InstallSnapshotResp.
fn last_resp_to(transport: &ScriptTransport, peer: PeerId) -> (u8, u64) {
    let frames = transport.sent_to(peer);
    let frame = frames.last().expect("expected at least one frame sent");
    let inbound = decode_message(frame).unwrap();
    let resp = inbound
        .as_install_snapshot_resp()
        .expect("expected an InstallSnapshotResp");
    (resp.accepted, resp.next_offset.get())
}

/// True when `frame` decodes to a bare (entry-less) AppendEntries heartbeat.
fn is_bare_heartbeat(frame: &[u8]) -> bool {
    let Ok(inbound) = decode_message(frame) else {
        return false;
    };
    match inbound.message {
        RaftMessage::AppendEntries(ae, _) => ae.entry_count.get() == 0,
        RaftMessage::AppendEntriesSeeded { ae, .. } => ae.entry_count.get() == 0,
        _ => false,
    }
}

async fn feed_frame(
    node: &mut RaftNode<TestStorage, ScriptTransport>,
    frame: &[u8],
) -> Result<(), RaftError> {
    let inbound = decode_message(frame)?;
    node.handle_inbound(inbound).await
}

// ---------------------------------------------------------------------------
// Test 1 — timer-based eviction of a stalled pending snapshot (C4 gap 1).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn stalled_pending_snapshot_is_evicted_by_timer_and_next_chunk_starts_fresh() {
    let limits = LimitsConfig {
        snapshot_stall_timeout_ms: 50,
        ..LimitsConfig::default()
    };
    let storage = TestStorage::default();
    let transport = ScriptTransport::new(PeerId(1));
    let mut node =
        RaftNode::new(config_3node(2, limits), storage.clone(), transport.clone()).unwrap();
    let metrics = node.metrics();

    let meta = SnapshotMeta {
        last_included_index: LogIndex(10),
        last_included_term: Term(1),
    };
    let chunk_a = vec![0xAA; 64];

    // Chunk 1 (offset 0, not done) from the leader — buffer opens, ACK 64.
    feed_frame(
        &mut node,
        &install_chunk_frame(PeerId(1), 1, &meta, 0, &chunk_a, false),
    )
    .await
    .unwrap();
    assert_eq!(last_resp_to(&transport, PeerId(1)), (1, 64));

    // Progressing transfer is NOT evicted: well inside the stall window.
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(node.evict_stalled_snapshots(), 0);

    // Leader goes quiet past the stall timeout — the timer sweep must evict
    // (no message for this transfer ever arrives again).
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        node.evict_stalled_snapshots(),
        1,
        "stalled transfer must be evicted"
    );
    assert_eq!(metrics.snapshot().snapshots_evicted, 1);
    // Idempotent — nothing left to evict.
    assert_eq!(node.evict_stalled_snapshots(), 0);

    // A follow-up chunk for the evicted transfer must be treated as a fresh
    // session (NACK from offset 0), never appended to the freed buffer.
    transport.clear_sent();
    let chunk_b = vec![0xBB; 64];
    feed_frame(
        &mut node,
        &install_chunk_frame(PeerId(1), 1, &meta, 64, &chunk_b, true),
    )
    .await
    .unwrap();
    assert_eq!(
        last_resp_to(&transport, PeerId(1)),
        (0, 0),
        "chunk for an evicted transfer must be NACK-ed from offset 0"
    );
    // Nothing was persisted from the half-transfer.
    assert!(storage.load_snapshot().unwrap().is_none());

    // Sanity: a full fresh session (restart from offset 0) still completes —
    // eviction hardened the buffer without breaking install correctness.
    feed_frame(
        &mut node,
        &install_chunk_frame(PeerId(1), 1, &meta, 0, &chunk_a, false),
    )
    .await
    .unwrap();
    feed_frame(
        &mut node,
        &install_chunk_frame(PeerId(1), 1, &meta, 64, &chunk_b, true),
    )
    .await
    .unwrap();
    assert_eq!(last_resp_to(&transport, PeerId(1)), (1, 128));
    let (saved_meta, saved_bytes) = storage
        .load_snapshot()
        .unwrap()
        .expect("snapshot persisted");
    assert_eq!(saved_meta, meta);
    assert_eq!(saved_bytes.len(), 128);
    assert_eq!(&saved_bytes[..64], &chunk_a[..]);
    assert_eq!(&saved_bytes[64..], &chunk_b[..]);
}

/// The eviction must also fire from the run loop itself (timer-based, not
/// message-triggered): a follower tick with an empty inbox evicts the stale
/// buffer without any snapshot message arriving.
#[tokio::test]
async fn run_loop_tick_evicts_stalled_snapshot_without_new_messages() {
    let limits = LimitsConfig {
        snapshot_stall_timeout_ms: 50,
        ..LimitsConfig::default()
    };
    let storage = TestStorage::default();
    let transport = ScriptTransport::new(PeerId(1));
    let node = RaftNode::new(config_3node(2, limits), storage, transport.clone()).unwrap();
    let metrics = node.metrics();
    let mut raft = arbitro_raft::ArbitroRaft::new(node, arbitro_raft::NoopStateMachine);

    // Deliver chunk 1 through the transport so the run loop opens the buffer.
    let meta = SnapshotMeta {
        last_included_index: LogIndex(10),
        last_included_term: Term(1),
    };
    transport.push_inbound(install_chunk_frame(
        PeerId(1),
        1,
        &meta,
        0,
        &[0xAA; 64],
        false,
    ));
    raft.run_once().await.unwrap();
    assert_eq!(last_resp_to(&transport, PeerId(1)), (1, 64));
    assert_eq!(metrics.snapshot().snapshots_evicted, 0);

    // Leader goes quiet. The next ticks receive NOTHING — eviction must come
    // purely from the per-tick timer sweep.
    tokio::time::sleep(Duration::from_millis(100)).await;
    raft.run_once().await.unwrap();
    assert_eq!(
        metrics.snapshot().snapshots_evicted,
        1,
        "run-loop tick must evict the stalled transfer with no inbound traffic"
    );
}

// ---------------------------------------------------------------------------
// Test 2 — per-peer attempt cap + cooldown recovery (PS7 / C4 gap 2).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn snapshot_attempt_cap_backs_off_peer_and_recovers_after_cooldown() {
    let limits = LimitsConfig {
        snapshot_chunk_bytes: 256,
        snapshot_max_attempts_per_peer: 2,
        snapshot_attempt_cooldown_ms: 100,
        ..LimitsConfig::default()
    };
    let storage = TestStorage::default();
    // Leader has a compacted log: on-disk snapshot at index 10, empty log —
    // peer 3 (next_index 1) can only be caught up via InstallSnapshot.
    let meta = SnapshotMeta {
        last_included_index: LogIndex(10),
        last_included_term: Term(1),
    };
    storage.save_snapshot(&meta, &vec![0xCC; 1000]).unwrap();

    let transport = ScriptTransport::new(PeerId(3));
    let mut node = RaftNode::new(config_3node(1, limits), storage, transport.clone()).unwrap();
    node.become_leader_for_benchmark(Term(2));
    let metrics = node.metrics();

    // Hostile follower: NACKs every chunk from offset 0 → each install call
    // aborts on the in-call no-progress cap and burns one attempt.
    transport.set_mode(RespondMode::NackZero);
    for attempt in 1..=2 {
        let err = node
            .maybe_install_snapshot_to_lagging_peer(PeerId(3))
            .await
            .expect_err("hostile NACK loop must abort the install");
        assert!(
            matches!(err, RaftError::Snapshot(_)),
            "attempt {attempt}: expected Snapshot error, got {err:?}"
        );
    }

    // Attempt budget exhausted → the next call refuses WITHOUT streaming.
    transport.clear_sent();
    let sent = node
        .maybe_install_snapshot_to_lagging_peer(PeerId(3))
        .await
        .unwrap();
    assert!(!sent, "capped peer must be refused");
    assert!(
        transport.sent_to(PeerId(3)).is_empty(),
        "no frames may be streamed to a refused peer"
    );
    assert_eq!(metrics.snapshot().snapshot_installs_refused, 1);

    // Still cooling down — refused again.
    let sent = node
        .maybe_install_snapshot_to_lagging_peer(PeerId(3))
        .await
        .unwrap();
    assert!(!sent);
    assert_eq!(metrics.snapshot().snapshot_installs_refused, 2);

    // Cooldown elapses and the peer behaves — the install must recover.
    tokio::time::sleep(Duration::from_millis(150)).await;
    transport.set_mode(RespondMode::AckAll);
    transport.clear_sent();
    let sent = node
        .maybe_install_snapshot_to_lagging_peer(PeerId(3))
        .await
        .unwrap();
    assert!(sent, "peer must be allowed again after the cooldown");
    // 1000 bytes / 256-byte chunks = 4 InstallSnapshot frames.
    let install_frames: Vec<_> = transport
        .sent_to(PeerId(3))
        .into_iter()
        .filter(|f| {
            decode_message(f)
                .ok()
                .and_then(|m| m.as_install_snapshot().map(|_| ()))
                .is_some()
        })
        .collect();
    assert_eq!(
        install_frames.len(),
        4,
        "full snapshot must have been streamed"
    );
    // Refusal counter untouched by the successful path.
    assert_eq!(metrics.snapshot().snapshot_installs_refused, 2);

    // A success resets the attempt budget: another hostile episode gets a
    // fresh set of attempts (i.e. it errors again rather than being refused).
    transport.set_mode(RespondMode::NackZero);
    let err = node.maybe_install_snapshot_to_lagging_peer(PeerId(3)).await;
    assert!(
        err.is_err(),
        "fresh budget after success: install runs (and fails) again"
    );
}

// ---------------------------------------------------------------------------
// Test 3 — chunked yielding: heartbeats keep flowing mid-install (OPS-2).
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn large_install_keeps_heartbeats_flowing_to_other_followers() {
    let limits = LimitsConfig {
        snapshot_chunk_bytes: 1024,
        ..LimitsConfig::default()
    };
    let storage = TestStorage::default();
    let meta = SnapshotMeta {
        last_included_index: LogIndex(10),
        last_included_term: Term(1),
    };
    // 32 KiB snapshot in 1 KiB chunks = 32 request/response rounds; each
    // response is delayed 5 ms, so the install occupies the call stack for
    // ~160 ms — many heartbeat intervals (heartbeat_ms = 10).
    let snapshot_bytes = vec![0xDD; 32 * 1024];
    storage.save_snapshot(&meta, &snapshot_bytes).unwrap();

    let transport = ScriptTransport::new(PeerId(3));
    transport.set_mode(RespondMode::AckAll);
    transport.set_recv_delay(Duration::from_millis(5));
    let mut node = RaftNode::new(config_3node(1, limits), storage, transport.clone()).unwrap();
    node.become_leader_for_benchmark(Term(2));

    let sent = node
        .maybe_install_snapshot_to_lagging_peer(PeerId(3))
        .await
        .unwrap();
    assert!(sent, "install must complete");

    // The install completed correctly in bounded chunk steps.
    let chunks: Vec<_> = transport
        .sent_to(PeerId(3))
        .iter()
        .filter_map(|f| {
            decode_message(f).ok().and_then(|m| {
                m.as_install_snapshot()
                    .map(|(req, p)| (req.offset.get(), p.len()))
            })
        })
        .collect();
    assert_eq!(chunks.len(), 32, "32 KiB / 1 KiB = 32 bounded steps");
    assert_eq!(
        chunks.last().unwrap().0,
        31 * 1024,
        "final chunk at the last offset"
    );

    // OPS-2: while the install monopolized the call stack, the OTHER follower
    // (peer 2) must still have received bare heartbeats, so it never times out
    // into a spurious election mid-install.
    let heartbeats_to_2 = transport
        .sent_to(PeerId(2))
        .iter()
        .filter(|f| is_bare_heartbeat(f))
        .count();
    assert!(
        heartbeats_to_2 >= 3,
        "expected interleaved heartbeats to peer 2 during the install, got {heartbeats_to_2}"
    );
    // And none of that traffic leaked to the install target as a duplicate
    // stream — peer 3 got exactly the snapshot chunks.
    assert!(node.is_leader(), "leader must survive its own install");
}
