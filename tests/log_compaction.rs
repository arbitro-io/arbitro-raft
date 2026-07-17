//! C2 — log compaction wired into the run loop.
//!
//! 1. Policy trigger: after more than `compaction_threshold_entries` applied
//!    entries, a run-loop tick snapshots the SM, persists it, and truncates
//!    the log prefix — the log is bounded, `last_log_position` is intact, and
//!    reads below the horizon are served by the snapshot, not the log.
//! 2. Conservative horizon: a lagging follower's `match_index` clamps the
//!    truncation point, so a live voter is never stranded (C3 — snapshot
//!    catch-up — is what will let it advance later).
//! 3. In-flight install guard: no compaction while a pending inbound
//!    snapshot transfer exists (C4 tracking); once the transfer is evicted,
//!    the next tick compacts normally.
//! 4. Post-compaction the cluster still commits new entries, and a restart
//!    replays snapshot + log tail into an identical state machine.
//!
//! All tests drive nodes through a deterministic auto-acking transport — no
//! spawned cluster, no scheduler dependence (same style as
//! `tests/snapshot_hardening.rs`).

use std::collections::{HashSet, VecDeque};
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arbitro_raft::{
    decode_message, encode_message_to_bytes, AppendEntries, AppendEntriesResp, ArbitroRaft,
    BootstrapPeer, ClusterId, EntryPayload, HardState, LimitsConfig, LogEntry, LogIndex,
    NodeConfig, PeerId, RaftError, RaftMessage, RaftNode, RaftStorage, RaftTransport,
    SnapshotMeta, StateMachine, Term, TimingConfig,
};

// ---------------------------------------------------------------------------
// CountingSM — observable state machine (applied payload bytes concatenated).
// ---------------------------------------------------------------------------

#[derive(Default, Clone)]
struct CountingSM {
    state_bytes: Arc<Mutex<Vec<u8>>>,
    restored: Arc<Mutex<usize>>,
}

impl StateMachine for CountingSM {
    fn apply(&mut self, entry: &[u8]) -> Result<(), RaftError> {
        self.state_bytes.lock().unwrap().extend_from_slice(entry);
        Ok(())
    }
    fn snapshot(&self) -> Result<Vec<u8>, RaftError> {
        Ok(self.state_bytes.lock().unwrap().clone())
    }
    fn restore(&mut self, snapshot: &[u8]) -> Result<(), RaftError> {
        *self.restored.lock().unwrap() += 1;
        *self.state_bytes.lock().unwrap() = snapshot.to_vec();
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// TestStorage — in-memory storage honoring truncate_before / save_snapshot
// (same shape as tests/snapshot_hardening.rs).
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
    fn len(&self) -> usize {
        self.entries.lock().unwrap().len()
    }
    fn snapshot_meta(&self) -> Option<SnapshotMeta> {
        self.snapshot.lock().unwrap().as_ref().map(|(m, _)| m.clone())
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
// AckTransport — records every outbound frame; for peers in `ack_peers`,
// auto-responds to AppendEntries with a successful AppendEntriesResp whose
// match_index = prev_log_index + entry_count. Everything else stays silent.
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

    fn on_send(&self, peer: PeerId, frame: Vec<u8>) {
        if self.ack_peers.lock().unwrap().contains(&peer) {
            if let Ok(inbound) = decode_message(&frame) {
                let ae_opt = match inbound.message {
                    RaftMessage::AppendEntries(ae, _) => Some(*ae),
                    RaftMessage::AppendEntriesSeeded { ae, .. } => Some(*ae),
                    _ => None,
                };
                if let Some(ae) = ae_opt {
                    let match_index =
                        ae.prev_log_index.get() + u64::from(ae.entry_count.get());
                    let resp = AppendEntriesResp {
                        term: ae.term,
                        match_index: match_index.into(),
                        success: 1,
                        _pad: [0; 7],
                    };
                    let bytes = encode_message_to_bytes(
                        peer,
                        &RaftMessage::AppendEntriesResp(&resp),
                    )
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
        timing: TimingConfig {
            heartbeat_ms: 10,
            election_min_ms: 800,
            election_max_ms: 1200,
        },
        limits,
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

fn install_chunk_frame(
    from: PeerId,
    term: u64,
    meta: &SnapshotMeta,
    offset: u64,
    chunk: &[u8],
    done: bool,
) -> Vec<u8> {
    let req = arbitro_raft::InstallSnapshot {
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

/// Boot a manually-driven leader (node 1) over `storage`/`transport`.
fn boot_leader(
    storage: TestStorage,
    transport: AckTransport,
    limits: LimitsConfig,
) -> (ArbitroRaft<TestStorage, AckTransport, CountingSM>, CountingSM) {
    let mut node = RaftNode::new(config_3node(1, limits), storage, transport).unwrap();
    node.become_leader_for_benchmark(Term(2));
    let sm = CountingSM::default();
    (ArbitroRaft::new(node, sm.clone()), sm)
}

/// Tick the node until `cond` holds (or the tick budget runs out).
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
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    cond()
}

// ---------------------------------------------------------------------------
// Test 1 — the policy trigger fires and bounds the log.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn compaction_fires_after_threshold_and_bounds_the_log() {
    let storage = TestStorage::default();
    let transport = AckTransport::new(&[2, 3]); // both followers ack
    let (mut leader, leader_sm) = boot_leader(
        storage.clone(),
        transport.clone(),
        compaction_limits(8, 4),
    );
    let metrics = leader.metrics();

    // Propose 20 single-byte entries — the auto-acker commits each one.
    for i in 0..20u8 {
        leader.propose_once(&[i]).await.unwrap();
    }

    // Tick until the compaction ran with the full horizon: snapshot at 20,
    // log truncated up to 20 - min_retain = 16.
    let done = pump_until(
        &mut leader,
        || storage.snapshot_meta().map(|m| m.last_included_index.0) == Some(20)
            && storage.first_index() == Some(16),
        200,
    )
    .await;
    assert!(
        done,
        "compaction never reached the expected horizon: snapshot={:?} first={:?}",
        storage.snapshot_meta(),
        storage.first_index()
    );

    // Snapshot boundary and content.
    let (meta, snap_bytes) = storage.load_snapshot().unwrap().expect("snapshot persisted");
    assert_eq!(meta.last_included_index, LogIndex(20));
    assert_eq!(meta.last_included_term, Term(2));
    assert_eq!(
        snap_bytes,
        leader_sm.state_bytes.lock().unwrap().clone(),
        "snapshot must capture the SM state at last_applied"
    );
    assert_eq!(snap_bytes, (0..20u8).collect::<Vec<u8>>());

    // The log is bounded: retention tail only ([16, 20]).
    assert_eq!(storage.first_index(), Some(16));
    assert_eq!(storage.len(), 5);

    // last_log_position intact across truncation.
    assert_eq!(storage.last_log_position().unwrap(), (LogIndex(20), Term(2)));

    // Reads below the horizon come from the snapshot, not the log.
    let mut buf = [0u8; 64];
    assert!(storage.entry_at(LogIndex(15), &mut buf).unwrap().is_none());
    assert!(storage.entry_at(LogIndex(16), &mut buf).unwrap().is_some());
    assert!(meta.last_included_index.0 >= 15, "snapshot covers the truncated prefix");
    assert_eq!(snap_bytes[14], 14u8, "payload of a truncated entry is in the snapshot");

    assert!(metrics.snapshot().log_compactions >= 1);
}

// ---------------------------------------------------------------------------
// Test 2 — a lagging follower clamps the horizon (never stranded).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn lagging_follower_clamps_horizon_and_is_not_stranded() {
    let storage = TestStorage::default();
    let transport = AckTransport::new(&[2]); // peer 3 never responds
    let (mut leader, _sm) = boot_leader(
        storage.clone(),
        transport.clone(),
        compaction_limits(8, 4),
    );

    for i in 0..20u8 {
        leader.propose_once(&[i]).await.unwrap();
    }

    // The trigger fires (debt >= 8), a snapshot is persisted — but peer 3's
    // match_index is 0, so the conservative horizon clamps truncation to a
    // no-op: NOTHING may be discarded that the lagging voter still needs.
    let snapshotted = pump_until(
        &mut leader,
        || storage.snapshot_meta().is_some(),
        200,
    )
    .await;
    assert!(snapshotted, "snapshot must be persisted even when truncation is clamped");
    assert_eq!(
        storage.snapshot_meta().unwrap().last_included_index,
        LogIndex(20)
    );
    assert_eq!(
        storage.first_index(),
        Some(1),
        "compaction must NOT truncate past the lagging follower's match_index (0)"
    );
    assert_eq!(storage.len(), 20);

    // Peer 3 finally acks up to 20 (as if repaired) — the next threshold
    // crossing may then truncate, but never past peer 3's match_index.
    let ack = AppendEntriesResp {
        term: 2u64.into(),
        match_index: 20u64.into(),
        success: 1,
        _pad: [0; 7],
    };
    transport.push_inbound(
        encode_message_to_bytes(PeerId(3), &RaftMessage::AppendEntriesResp(&ack))
            .unwrap()
            .to_vec(),
    );
    // One tick so the run loop's burst drain records peer 3's match_index
    // BEFORE the next batch (a propose-time gather would treat the stale ack
    // as `Ignored` for the in-flight attempt).
    leader.run_once().await.unwrap();

    for i in 20..28u8 {
        leader.propose_once(&[i]).await.unwrap();
    }
    let done = pump_until(&mut leader, || storage.first_index() == Some(16), 200).await;
    assert!(
        done,
        "after the laggard advanced, compaction must proceed (first={:?})",
        storage.first_index()
    );
    // Horizon = min(last_applied=28, match2=28, match3=20) - retain(4) = 16.
    assert_eq!(storage.first_index(), Some(16));
    assert!(
        storage.first_index().unwrap() <= 21,
        "truncation must never pass the slowest voter's match_index + 1"
    );
}

// ---------------------------------------------------------------------------
// Test 3 — no compaction while a matching inbound install is in flight.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pending_snapshot_install_blocks_compaction_until_evicted() {
    let limits = LimitsConfig {
        compaction_threshold_entries: 8,
        compaction_threshold_bytes: 0,
        compaction_min_retain: 0,
        snapshot_stall_timeout_ms: 50,
        ..LimitsConfig::default()
    };
    let storage = TestStorage::default();
    let transport = AckTransport::new(&[]); // follower sends, nobody acks
    let node = RaftNode::new(config_3node(2, limits), storage.clone(), transport.clone()).unwrap();
    let metrics = node.metrics();
    let sm = CountingSM::default();
    let mut follower = ArbitroRaft::new(node, sm.clone());

    // An inbound snapshot transfer opens (offset 0, NOT done) — its boundary
    // (100) covers everything this follower could compact.
    let inflight_meta = SnapshotMeta {
        last_included_index: LogIndex(100),
        last_included_term: Term(1),
    };
    transport.push_inbound(install_chunk_frame(
        PeerId(1),
        1,
        &inflight_meta,
        0,
        &[0xAA; 64],
        false,
    ));

    // The leader also streams 10 committed entries — applying them crosses
    // the compaction threshold (8).
    let entries: Vec<(u64, u64, Vec<u8>)> =
        (1..=10u64).map(|i| (1u64, i, vec![i as u8])).collect();
    transport.push_inbound(append_entries_frame(PeerId(1), 1, 0, 0, 10, &entries));

    follower.run_once().await.unwrap();
    assert_eq!(
        follower.node().last_applied(),
        LogIndex(10),
        "entries must be applied"
    );
    // Threshold crossed, but the in-flight transfer must block compaction.
    assert!(
        storage.snapshot_meta().is_none(),
        "no compaction while a pending snapshot transfer is in flight"
    );
    assert_eq!(storage.first_index(), Some(1));
    assert_eq!(metrics.snapshot().log_compactions, 0);

    // The transfer stalls past the C4 timeout and is evicted; the very next
    // apply tick compacts under the same policy (follower horizon = its own
    // applied state, never past commit_index).
    tokio::time::sleep(Duration::from_millis(100)).await;
    transport.push_inbound(append_entries_frame(PeerId(1), 1, 10, 1, 10, &[]));
    follower.run_once().await.unwrap();

    assert_eq!(metrics.snapshot().snapshots_evicted, 1, "stalled transfer evicted");
    let meta = storage.snapshot_meta().expect("compaction fired after eviction");
    assert_eq!(meta.last_included_index, LogIndex(10));
    assert_eq!(meta.last_included_term, Term(1));
    // Follower horizon = last_applied (10) - retain (0); the boundary entry
    // itself is retained (truncate_before is strictly-below).
    assert_eq!(storage.first_index(), Some(10));
    assert_eq!(storage.len(), 1);
    assert_eq!(
        storage.load_snapshot().unwrap().unwrap().1,
        (1..=10u64).map(|i| i as u8).collect::<Vec<u8>>()
    );
    assert_eq!(metrics.snapshot().log_compactions, 1);
}

// ---------------------------------------------------------------------------
// Test 4 — post-compaction commits still flow; restart replays snapshot+tail.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn post_compaction_commits_flow_and_restart_replays_snapshot_plus_tail() {
    let storage = TestStorage::default();
    let transport = AckTransport::new(&[2, 3]);
    let (mut leader, leader_sm) = boot_leader(
        storage.clone(),
        transport.clone(),
        compaction_limits(8, 4),
    );

    for i in 0..20u8 {
        leader.propose_once(&[i]).await.unwrap();
    }
    let compacted = pump_until(
        &mut leader,
        || storage.snapshot_meta().map(|m| m.last_included_index.0) == Some(20)
            && storage.first_index() == Some(16),
        200,
    )
    .await;
    assert!(compacted, "compaction must land before the post-compaction phase");

    // The cluster still commits and applies new entries after compaction.
    for i in 0..5u8 {
        let idx = leader.propose_once(&[100 + i]).await.unwrap();
        assert_eq!(idx, LogIndex(21 + i as u64));
    }
    let applied = pump_until(
        &mut leader,
        || leader_sm.state_bytes.lock().unwrap().len() == 25,
        200,
    )
    .await;
    assert!(applied, "post-compaction entries must be applied");
    assert_eq!(storage.last_log_position().unwrap(), (LogIndex(25), Term(2)));

    let leader_state = leader_sm.state_bytes.lock().unwrap().clone();
    drop(leader);

    // "Restart": a fresh node over the SAME storage, fresh state machine.
    // Recovery = restore from the snapshot, then replay the log tail.
    let transport2 = AckTransport::new(&[]);
    let mut restarted = RaftNode::new(
        config_3node(1, compaction_limits(8, 4)),
        storage.clone(),
        transport2,
    )
    .unwrap();
    assert_eq!(
        restarted.status().last_log_index,
        LogIndex(25),
        "log tail survives the restart"
    );

    let mut recovered_sm = CountingSM::default();
    let restored = restarted
        .restore_state_machine_from_snapshot(&mut recovered_sm)
        .unwrap();
    assert!(restored, "restart must restore the SM from the snapshot");
    assert_eq!(*recovered_sm.restored.lock().unwrap(), 1);
    assert_eq!(restarted.last_applied(), LogIndex(20));

    // Replay the tail (21..=25) exactly as the apply loop would.
    let mut buf = [0u8; 64];
    for idx in 21..=25u64 {
        let entry = storage
            .entry_at(LogIndex(idx), &mut buf)
            .unwrap()
            .expect("tail entry present after restart");
        let payload = entry.payload.0.to_vec();
        recovered_sm.apply(&payload).unwrap();
    }
    assert_eq!(
        recovered_sm.state_bytes.lock().unwrap().clone(),
        leader_state,
        "snapshot + tail replay must reproduce the pre-restart state machine"
    );
}
