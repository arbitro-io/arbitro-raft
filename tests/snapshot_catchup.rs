//! C3 — automatic snapshot catch-up for peers below the compaction horizon.
//!
//! `install_snapshot_to_lagging_peers` existed (B2/C4) but nothing in the run
//! loop ever called it: a follower whose `next_index` fell below the leader's
//! log start — its backlog compacted away by C2 / a snapshot's
//! `truncate_before` — was stranded forever, and (per C2's conservative
//! clamp) blocked the compaction horizon from ever advancing again. C3 wires
//! the escalation into the A9 heartbeat repair path in `send_heartbeat_once`:
//!
//!   1. End-to-end hand-off chain: a leader whose log was genuinely compacted
//!      by C2 (follower-side policy compaction in a previous role — the
//!      organic path to a below-horizon peer, since a live leader's own C2
//!      horizon is clamped by every voter's match_index) must catch up an
//!      empty follower via an AUTOMATIC InstallSnapshot on the idle heartbeat
//!      path with ZERO client traffic, then hand the tail to A9's plain
//!      AppendEntries, after which C2 compaction advances again (the repaired
//!      follower no longer clamps the horizon).
//!   2. No spurious installs: a follower that is merely lagging but still
//!      ABOVE the log start is repaired by A9's AppendEntries — no
//!      InstallSnapshot frame may cross the wire, even though a snapshot
//!      exists on the leader's disk whose boundary covers the peer.
//!   3. Attempt-cap interaction (C4/PS7): a below-horizon follower that never
//!      answers installs burns exactly `snapshot_max_attempts_per_peer`
//!      attempts and is then refused under cooldown — the heartbeat tick must
//!      not spin re-streams, and the leader loop survives every failure.

use std::collections::{HashMap, HashSet, VecDeque};
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arbitro_raft::{
    decode_message, encode_message_to_bytes, AppendEntries, AppendEntriesResp, ArbitroRaft,
    BootstrapPeer, ClusterId, EntryPayload, HardState, LimitsConfig, LogEntry, LogIndex,
    NodeConfig, PeerId, RaftError, RaftMessage, RaftNode, RaftStorage, RaftTransport, SnapshotMeta,
    StateMachine, Term, TimingConfig,
};

// ---------------------------------------------------------------------------
// CountingSM — observable state machine (payload bytes concatenated in apply
// order, plus apply/restore counters).
// ---------------------------------------------------------------------------

#[derive(Default, Clone)]
struct CountingSM {
    applied: Arc<AtomicUsize>,
    restored: Arc<AtomicUsize>,
    state_bytes: Arc<Mutex<Vec<u8>>>,
}

impl CountingSM {
    fn bytes(&self) -> Vec<u8> {
        self.state_bytes.lock().unwrap().clone()
    }
}

impl StateMachine for CountingSM {
    fn apply(&mut self, entry: &[u8]) -> Result<(), RaftError> {
        self.applied.fetch_add(1, Ordering::SeqCst);
        self.state_bytes.lock().unwrap().extend_from_slice(entry);
        Ok(())
    }
    fn snapshot(&self) -> Result<Vec<u8>, RaftError> {
        Ok(self.state_bytes.lock().unwrap().clone())
    }
    fn restore(&mut self, snapshot: &[u8]) -> Result<(), RaftError> {
        self.restored.fetch_add(1, Ordering::SeqCst);
        *self.state_bytes.lock().unwrap() = snapshot.to_vec();
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// AbortOnDrop — cancel background tasks on scope exit.
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
// TestStorage — in-memory storage honoring truncate_before / save_snapshot
// (same shape as tests/log_compaction.rs).
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

impl TestStorage {
    fn first_index(&self) -> Option<u64> {
        self.entries.lock().unwrap().first().map(|e| e.index.0)
    }
    fn last_index(&self) -> u64 {
        self.entries
            .lock()
            .unwrap()
            .last()
            .map(|e| e.index.0)
            .unwrap_or(0)
    }
    fn snapshot_meta(&self) -> Option<SnapshotMeta> {
        self.snapshot
            .lock()
            .unwrap()
            .as_ref()
            .map(|(m, _)| m.clone())
    }
    /// Pre-seed the log with `(term, index, payload)` triples plus hard state.
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
// AckTransport — deterministic queue transport (same shape as
// tests/log_compaction.rs): records every outbound frame; peers listed in
// `ack_peers` auto-respond to AppendEntries with a success ack. Everything
// else stays silent. `recv_frame_timeout` never waits.
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct AckTransport {
    sent: Arc<Mutex<Vec<(PeerId, Vec<u8>)>>>,
    inbox: Arc<Mutex<VecDeque<Vec<u8>>>>,
    ack_peers: Arc<Mutex<HashSet<PeerId>>>,
}

impl AckTransport {
    fn new(ack_peers: &[u64]) -> Self {
        Self {
            sent: Arc::new(Mutex::new(Vec::new())),
            inbox: Arc::new(Mutex::new(VecDeque::new())),
            ack_peers: Arc::new(Mutex::new(ack_peers.iter().copied().map(PeerId).collect())),
        }
    }

    fn push_inbound(&self, frame: Vec<u8>) {
        self.inbox.lock().unwrap().push_back(frame);
    }

    /// InstallSnapshot frames recorded as sent to `peer`.
    fn install_frames_to(&self, peer: PeerId) -> usize {
        self.sent
            .lock()
            .unwrap()
            .iter()
            .filter(|(to, frame)| {
                *to == peer
                    && decode_message(frame)
                        .map(|m| matches!(m.message, RaftMessage::InstallSnapshot(_, _)))
                        .unwrap_or(false)
            })
            .count()
    }

    fn on_send(&self, peer: PeerId, frame: Vec<u8>) {
        if self.ack_peers.lock().unwrap().contains(&peer) {
            if let Ok(inbound) = decode_message(&frame) {
                let ae_opt = match inbound.message {
                    RaftMessage::AppendEntries(ae, _) => Some(*ae),
                    RaftMessage::AppendEntriesSeeded { ae, .. } => Some(*ae),
                    _ => None,
                };
                if let Some(ae) = ae_opt {
                    let match_index = ae.prev_log_index.get() + u64::from(ae.entry_count.get());
                    let resp = AppendEntriesResp {
                        term: ae.term,
                        match_index: match_index.into(),
                        success: 1,
                        _pad: [0; 7],
                    };
                    let bytes =
                        encode_message_to_bytes(peer, &RaftMessage::AppendEntriesResp(&resp))
                            .unwrap();
                    self.push_inbound(bytes.to_vec());
                }
            }
        }
        self.sent.lock().unwrap().push((peer, frame));
    }
}

impl RaftTransport for AckTransport {
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
        async move {
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
        async move {
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
// NetworkHub + RoutingTransport — partition-aware in-process wiring with an
// always-on InstallSnapshot frame counter (same shape as
// tests/snapshot_install.rs plus the counter).
// ---------------------------------------------------------------------------

struct NetworkHub {
    senders: Mutex<HashMap<PeerId, tokio::sync::mpsc::UnboundedSender<(PeerId, Vec<u8>)>>>,
    /// Frames from `.0` to `.1` are silently dropped while present.
    blocked: Mutex<HashSet<(PeerId, PeerId)>>,
    /// Every InstallSnapshot frame that actually crossed the hub.
    install_frames: AtomicU64,
}

impl NetworkHub {
    fn new() -> Self {
        Self {
            senders: Mutex::new(HashMap::new()),
            blocked: Mutex::new(HashSet::new()),
            install_frames: AtomicU64::new(0),
        }
    }
    fn register(
        &self,
        peer: PeerId,
        sender: tokio::sync::mpsc::UnboundedSender<(PeerId, Vec<u8>)>,
    ) {
        self.senders.lock().unwrap().insert(peer, sender);
    }
    fn isolate(&self, peer: PeerId, all: &[u64]) {
        let mut blocked = self.blocked.lock().unwrap();
        for &other in all {
            let other = PeerId(other);
            if other != peer {
                blocked.insert((peer, other));
                blocked.insert((other, peer));
            }
        }
    }
    fn unblock_all(&self) {
        self.blocked.lock().unwrap().clear();
    }
    fn install_frame_count(&self) -> u64 {
        self.install_frames.load(Ordering::SeqCst)
    }
    fn send(&self, from: PeerId, to: PeerId, data: Vec<u8>) {
        if self.blocked.lock().unwrap().contains(&(from, to)) {
            return;
        }
        if let Ok(inbound) = decode_message(&data) {
            if matches!(inbound.message, RaftMessage::InstallSnapshot(_, _)) {
                self.install_frames.fetch_add(1, Ordering::SeqCst);
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
                if let Ok((_from, data)) = rx.try_recv() {
                    if out.len() < data.len() {
                        return Err(RaftError::Transport("buffer too small".into()));
                    }
                    out[..data.len()].copy_from_slice(&data);
                    return Ok(Some(data.len()));
                }
                return Ok(None);
            }
            match tokio::time::timeout(timeout, rx.recv()).await {
                Ok(Some((_from, data))) => {
                    if out.len() < data.len() {
                        return Err(RaftError::Transport("buffer too small".into()));
                    }
                    out[..data.len()].copy_from_slice(&data);
                    Ok(Some(data.len()))
                }
                _ => Ok(None),
            }
        }
    }
}

fn register_transport(hub: &Arc<NetworkHub>, id: PeerId) -> RoutingTransport {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    hub.register(id, tx);
    RoutingTransport {
        from: id,
        hub: hub.clone(),
        rx: Arc::new(tokio::sync::Mutex::new(rx)),
    }
}

// ---------------------------------------------------------------------------
// Config + frame helpers.
// ---------------------------------------------------------------------------

fn config_3node(node_id: u64, limits: LimitsConfig, timing: TimingConfig) -> NodeConfig {
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
                addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 9400 + id as u16)),
            })
            .collect(),
        timing,
        limits,
    }
}

/// Followers spawned under a manually-driven leader must never campaign
/// before hearing from it.
fn slow_election_timing() -> TimingConfig {
    TimingConfig {
        heartbeat_ms: 50,
        election_min_ms: 800,
        election_max_ms: 1200,
    }
}

fn compaction_limits(threshold_entries: u64, min_retain: u64) -> LimitsConfig {
    LimitsConfig {
        compaction_threshold_entries: threshold_entries,
        // Isolate the entries trigger in these tests.
        compaction_threshold_bytes: 0,
        compaction_min_retain: min_retain,
        ..LimitsConfig::default()
    }
}

/// Serialize entries as the AppendEntries wire body: per entry a 24-byte
/// little-endian EntryHeader (term u64, index u64, payload_len u32, pad u32)
/// followed by the payload bytes.
fn entries_blob(entries: &[(u64, u64, Vec<u8>)]) -> Vec<u8> {
    let mut blob = Vec::new();
    for (term, index, payload) in entries {
        blob.extend_from_slice(&term.to_le_bytes());
        blob.extend_from_slice(&index.to_le_bytes());
        blob.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        blob.extend_from_slice(&0u32.to_le_bytes());
        blob.extend_from_slice(payload);
    }
    blob
}

fn append_entries_frame(
    from: PeerId,
    term: u64,
    prev_idx: u64,
    prev_term: u64,
    leader_commit: u64,
    entries: &[(u64, u64, Vec<u8>)],
) -> Vec<u8> {
    let blob = entries_blob(entries);
    let ae = AppendEntries {
        term: term.into(),
        leader_id: from.0.into(),
        prev_log_index: prev_idx.into(),
        prev_log_term: prev_term.into(),
        leader_commit: leader_commit.into(),
        entry_count: (entries.len() as u32).into(),
        _pad: 0u32.into(),
    };
    encode_message_to_bytes(from, &RaftMessage::AppendEntries(&ae, &blob))
        .unwrap()
        .to_vec()
}

/// Tick the manually-driven node until `cond` holds (or the budget runs out).
async fn pump_until<S, T, SM, F>(
    raft: &mut ArbitroRaft<S, T, SM>,
    mut cond: F,
    ticks: usize,
) -> bool
where
    S: RaftStorage,
    T: RaftTransport,
    SM: StateMachine,
    F: FnMut() -> bool,
{
    for _ in 0..ticks {
        if cond() {
            return true;
        }
        raft.run_once().await.unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    cond()
}

// ---------------------------------------------------------------------------
// Test 1 — the full C3 hand-off chain, zero client traffic during catch-up:
// C2-compacted leader log → automatic InstallSnapshot on the idle heartbeat
// path → A9 AppendEntries takes over the tail → C2 horizon advances again.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn below_horizon_follower_auto_caught_up_via_snapshot_then_append_entries() {
    let limits = || compaction_limits(16, 0);

    // ── Phase 0: produce a GENUINELY C2-compacted storage for node 1. ──────
    //
    // A live leader's own C2 horizon is clamped by every voter's match_index,
    // so the organic path to a below-horizon peer is: this node applied and
    // policy-compacted its log as a FOLLOWER (follower horizon = its own
    // applied state, no clamp), then won leadership while another voter was
    // still down. We reproduce exactly that: feed 60 committed entries
    // through the real run loop and let `log_compaction::maybe_compact` fire.
    let leader_storage = TestStorage::default();
    {
        let transport = AckTransport::new(&[]);
        let node = RaftNode::new(
            config_3node(1, limits(), slow_election_timing()),
            leader_storage.clone(),
            transport.clone(),
        )
        .unwrap();
        let mut temp = ArbitroRaft::new(node, CountingSM::default());
        let entries: Vec<(u64, u64, Vec<u8>)> =
            (1..=60u64).map(|i| (2u64, i, vec![i as u8])).collect();
        transport.push_inbound(append_entries_frame(PeerId(2), 2, 0, 0, 60, &entries));
        temp.run_once().await.unwrap();
        assert_eq!(temp.node().last_applied(), LogIndex(60));
    }
    // C2 fired: snapshot at 60, prefix truncated (retain 0 keeps only idx 60).
    let meta = leader_storage
        .snapshot_meta()
        .expect("phase 0: C2 policy compaction must have persisted a snapshot");
    assert_eq!(meta.last_included_index, LogIndex(60));
    assert_eq!(
        leader_storage.first_index(),
        Some(60),
        "phase 0: C2 must have truncated the log prefix"
    );

    // ── Phase 1: 3-node cluster; node 1 leads with the compacted log. ──────
    // Node 2 holds the full log (caught up); node 3 is EMPTY — it was down
    // the whole time and its backlog no longer exists anywhere in the log.
    let hub = Arc::new(NetworkHub::new());
    let mut handles = Vec::new();

    let n1_transport = register_transport(&hub, PeerId(1));
    let mut n1_node = RaftNode::new(
        config_3node(1, limits(), slow_election_timing()),
        leader_storage.clone(),
        n1_transport,
    )
    .unwrap();
    n1_node.become_leader_for_benchmark(Term(3));
    let leader_sm = CountingSM::default();
    let leader_metrics = n1_node.metrics();
    let mut leader = ArbitroRaft::new(n1_node, leader_sm.clone());

    let n2_storage = TestStorage::default();
    {
        let full: Vec<(u64, u64, Vec<u8>)> =
            (1..=60u64).map(|i| (2u64, i, vec![i as u8])).collect();
        let as_refs: Vec<(u64, u64, &[u8])> = full
            .iter()
            .map(|(t, i, p)| (*t, *i, p.as_slice()))
            .collect();
        n2_storage.seed(&as_refs, 2);
    }
    let n2_transport = register_transport(&hub, PeerId(2));
    let n2_node = RaftNode::new(
        config_3node(2, limits(), slow_election_timing()),
        n2_storage.clone(),
        n2_transport,
    )
    .unwrap();
    let n2_raft = ArbitroRaft::new(n2_node, CountingSM::default());
    handles.push(tokio::spawn(async move {
        let mut r = n2_raft;
        let _ = r.run().await;
    }));

    let n3_storage = TestStorage::default();
    let n3_transport = register_transport(&hub, PeerId(3));
    let n3_node = RaftNode::new(
        config_3node(3, limits(), slow_election_timing()),
        n3_storage.clone(),
        n3_transport,
    )
    .unwrap();
    let n3_sm = CountingSM::default();
    let n3_raft = ArbitroRaft::new(n3_node, n3_sm.clone());
    let n3_commit = n3_raft.commit_index_observer();
    handles.push(tokio::spawn(async move {
        let mut r = n3_raft;
        let _ = r.run().await;
    }));
    let _guard = AbortOnDrop { handles };

    // Announce leadership; from here on there is ZERO client traffic — the
    // only leader-driven frames are heartbeat ticks. Node 3's reject walks
    // its next_index back to 60; the entry at 60 no longer exists in the
    // leader's log (post-restore the boundary entry itself is gone), so the
    // C3 trigger must escalate to an automatic InstallSnapshot.
    leader.send_heartbeat_once().await.unwrap();
    let caught_up = pump_until(
        &mut leader,
        || n3_sm.restored.load(Ordering::SeqCst) >= 1,
        400,
    )
    .await;
    assert!(
        caught_up,
        "C3 REGRESSION: below-horizon follower was never caught up via an \
         automatic InstallSnapshot on the idle heartbeat path"
    );

    assert!(
        hub.install_frame_count() >= 1,
        "an InstallSnapshot frame must actually have crossed the wire"
    );
    assert_eq!(
        n3_sm.restored.load(Ordering::SeqCst),
        1,
        "node 3 must have restored exactly once"
    );
    assert_eq!(
        n3_sm.applied.load(Ordering::SeqCst),
        0,
        "the idle-path catch-up must be a pure snapshot restore — \
         no AppendEntries entry may have been applied (zero client traffic)"
    );
    assert_eq!(
        n3_sm.bytes(),
        (1..=60u8).collect::<Vec<u8>>(),
        "node 3's restored state must be byte-identical to the snapshot"
    );
    assert_eq!(
        n3_sm.bytes(),
        leader_sm.bytes(),
        "node 3's state must match the leader's after the install"
    );
    assert!(
        n3_commit.get().0 >= 60,
        "node 3's commit index must have reached the snapshot boundary"
    );
    let installs_after_catchup = hub.install_frame_count();

    // ── Phase 2: A9 hand-off — the tail flows via plain AppendEntries. ─────
    for i in 61..=80u64 {
        leader.propose_once(&[i as u8]).await.unwrap();
    }
    let tail_applied = pump_until(
        &mut leader,
        || n3_sm.applied.load(Ordering::SeqCst) >= 20,
        400,
    )
    .await;
    assert!(
        tail_applied,
        "post-snapshot entries must reach node 3 via AppendEntries \
         (applied={})",
        n3_sm.applied.load(Ordering::SeqCst)
    );
    assert_eq!(
        hub.install_frame_count(),
        installs_after_catchup,
        "the tail must flow via AppendEntries — no repeated installs \
         (progress was re-anchored past the boundary)"
    );

    // ── Phase 3: C2 compaction advances — node 3 no longer clamps. ─────────
    for i in 81..=100u64 {
        leader.propose_once(&[i as u8]).await.unwrap();
    }
    let horizon_advanced = pump_until(
        &mut leader,
        || leader_storage.first_index().is_some_and(|f| f > 61),
        400,
    )
    .await;
    assert!(
        horizon_advanced,
        "C2 compaction must advance past the old boundary once the repaired \
         follower's match_index un-clamps the horizon (first_index={:?})",
        leader_storage.first_index()
    );
    assert!(
        leader_metrics.snapshot().log_compactions >= 1,
        "the horizon advance must come from a policy-triggered compaction"
    );

    // Final convergence: byte-identical state machines.
    let converged = pump_until(&mut leader, || n3_sm.bytes() == leader_sm.bytes(), 400).await;
    assert!(
        converged,
        "node 3 must converge byte-identical to the leader"
    );
    assert_eq!(
        leader_sm.bytes(),
        (1..=100u64).map(|i| i as u8).collect::<Vec<u8>>()
    );
    assert_eq!(
        hub.install_frame_count(),
        installs_after_catchup,
        "no further installs across the whole hand-off chain"
    );
}

// ---------------------------------------------------------------------------
// Test 2 — no spurious installs: a follower that is lagging but still ABOVE
// the log start is repaired by A9's AppendEntries, never a snapshot — even
// though the leader HAS an on-disk snapshot whose boundary covers the peer
// (C2's clamped compaction persists the snapshot but keeps the log intact).
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn lagging_follower_above_horizon_repaired_by_append_entries_not_snapshot() {
    let limits = || compaction_limits(8, 4);
    let hub = Arc::new(NetworkHub::new());
    let mut handles = Vec::new();

    // Node 1 — manually-driven leader (deterministic: leadership can never
    // move, so the clamped-compaction setup below cannot be perturbed by a
    // follower that policy-compacted its own log and then won an election).
    let leader_storage = TestStorage::default();
    let n1_transport = register_transport(&hub, PeerId(1));
    let mut n1_node = RaftNode::new(
        config_3node(1, limits(), slow_election_timing()),
        leader_storage.clone(),
        n1_transport,
    )
    .unwrap();
    n1_node.become_leader_for_benchmark(Term(2));
    let leader_sm = CountingSM::default();
    let mut leader = ArbitroRaft::new(n1_node, leader_sm.clone());

    // Node 2 — healthy follower (acks everything, forms the quorum).
    let n2_transport = register_transport(&hub, PeerId(2));
    let n2_node = RaftNode::new(
        config_3node(2, limits(), slow_election_timing()),
        TestStorage::default(),
        n2_transport,
    )
    .unwrap();
    let n2_raft = ArbitroRaft::new(n2_node, CountingSM::default());
    handles.push(tokio::spawn(async move {
        let mut r = n2_raft;
        let _ = r.run().await;
    }));

    // Node 3 — the lagging follower, isolated BEFORE any entry lands: its
    // match_index (0) clamps the leader's C2 horizon, so the leader's log
    // stays fully intact even though the compaction trigger fires and
    // persists a snapshot whose boundary covers everything node 3 needs.
    let n3_storage = TestStorage::default();
    let n3_transport = register_transport(&hub, PeerId(3));
    let n3_node = RaftNode::new(
        config_3node(3, limits(), slow_election_timing()),
        n3_storage.clone(),
        n3_transport,
    )
    .unwrap();
    let n3_sm = CountingSM::default();
    let n3_raft = ArbitroRaft::new(n3_node, n3_sm.clone());
    handles.push(tokio::spawn(async move {
        let mut r = n3_raft;
        let _ = r.run().await;
    }));
    let _guard = AbortOnDrop { handles };

    hub.isolate(PeerId(3), &[1, 2, 3]);
    leader.send_heartbeat_once().await.unwrap();

    // Commit 20 entries through the leader + node 2 majority.
    for i in 0..20u8 {
        leader
            .propose_once(&[b'v', i])
            .await
            .expect("propose with 2/3 majority must commit");
    }

    // The leader's C2 trigger fired (threshold 8 < 20): snapshot persisted,
    // truncation clamped to a no-op by the lagging voter (match_index 0).
    let compacted = pump_until(
        &mut leader,
        || leader_storage.snapshot_meta().is_some(),
        400,
    )
    .await;
    assert!(compacted, "leader never persisted the policy snapshot");
    assert_eq!(
        leader_storage.snapshot_meta().unwrap().last_included_index,
        LogIndex(20)
    );
    assert_eq!(
        leader_storage.first_index(),
        Some(1),
        "the lagging voter must clamp truncation — log stays intact"
    );

    // Heal. ZERO client traffic from here: the idle heartbeat path must
    // repair the follower with plain AppendEntries. A sloppy C3 trigger that
    // gated only on the snapshot boundary (next_index <= boundary) would
    // stream a spurious snapshot here — the peer is below the boundary (its
    // next_index is 1, the boundary is 20) but ABOVE the log start.
    hub.unblock_all();
    let repaired = pump_until(
        &mut leader,
        || n3_storage.last_index() >= 20 && n3_sm.bytes() == leader_sm.bytes(),
        400,
    )
    .await;
    assert!(
        repaired,
        "idle path never repaired the lagging follower \
         (follower last={}, leader last={})",
        n3_storage.last_index(),
        leader_storage.last_index(),
    );

    assert_eq!(
        hub.install_frame_count(),
        0,
        "a follower above the log start must be repaired by AppendEntries — \
         NO InstallSnapshot may cross the wire"
    );
    assert_eq!(
        n3_sm.restored.load(Ordering::SeqCst),
        0,
        "the follower must never have restored from a snapshot"
    );
    assert!(
        leader.node().is_leader(),
        "the leader should have survived the idle repair window"
    );
}

// ---------------------------------------------------------------------------
// Test 3 — attempt-cap interaction (C4/PS7): a below-horizon follower that
// never answers installs is backed off, not spun. Exactly
// `snapshot_max_attempts_per_peer` streams are attempted, then the cooldown
// refuses further installs, and the leader loop survives every failure.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn failing_snapshot_escalation_is_backed_off_by_attempt_cap() {
    let limits = LimitsConfig {
        snapshot_max_attempts_per_peer: 2,
        // Effectively "forever" at test scale: proves the refusal is durable.
        snapshot_attempt_cooldown_ms: 60_000,
        ..LimitsConfig::default()
    };
    let timing = TimingConfig {
        heartbeat_ms: 10,
        election_min_ms: 800,
        election_max_ms: 1200,
    };

    // Leader storage: compacted log (only the boundary entry survives) plus
    // the on-disk snapshot at index 60 — the state C2 leaves behind.
    let storage = TestStorage::default();
    storage.seed(&[(2, 60, b"x")], 2);
    storage
        .save_snapshot(
            &SnapshotMeta {
                last_included_index: LogIndex(60),
                last_included_term: Term(2),
            },
            &[7u8; 512],
        )
        .unwrap();

    // Peer 2 acks heartbeats (keeps check-quorum alive); peer 3 is silent —
    // every install attempt to it times out.
    let transport = AckTransport::new(&[2]);
    let mut node = RaftNode::new(
        config_3node(1, limits, timing),
        storage.clone(),
        transport.clone(),
    )
    .unwrap();
    node.become_leader_for_benchmark(Term(3));
    let metrics = node.metrics();
    let mut leader = ArbitroRaft::new(node, CountingSM::default());

    // One reject from peer 3 walks its next_index back below the log start
    // (the restore path discards the boundary entry), arming the C3 trigger.
    let reject = AppendEntriesResp {
        term: 3u64.into(),
        match_index: 0u64.into(),
        success: 0,
        _pad: [0; 7],
    };
    transport.push_inbound(
        encode_message_to_bytes(PeerId(3), &RaftMessage::AppendEntriesResp(&reject))
            .unwrap()
            .to_vec(),
    );

    // Pump well past `snapshot_max_attempts_per_peer` heartbeat ticks.
    let capped = pump_until(
        &mut leader,
        || metrics.snapshot().snapshot_installs_refused >= 2,
        200,
    )
    .await;
    assert!(
        capped,
        "the C4 attempt cap must engage after repeated install failures \
         (refused={})",
        metrics.snapshot().snapshot_installs_refused
    );
    assert_eq!(
        transport.install_frames_to(PeerId(3)),
        2,
        "exactly snapshot_max_attempts_per_peer install streams may be \
         attempted before the cooldown engages"
    );

    // Keep ticking: the refusal must hold (no re-streams while cooling down).
    let refused_before = metrics.snapshot().snapshot_installs_refused;
    pump_until(&mut leader, || false, 30).await;
    assert_eq!(
        transport.install_frames_to(PeerId(3)),
        2,
        "no further install frames may be streamed during the cooldown — \
         the heartbeat path must not spin re-streams"
    );
    assert!(
        metrics.snapshot().snapshot_installs_refused > refused_before,
        "the trigger keeps re-checking (and being refused) each tick"
    );
    assert!(
        leader.node().is_leader(),
        "every install failure must be contained — the leader loop survives"
    );
}
