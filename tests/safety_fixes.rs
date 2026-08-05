//! Pinning tests for the A8 / A10 / B8 / B12 safety-fix batch.
//!
//! - A8 (PS8): check-quorum must count ONLY current-term frames from voters
//!   as quorum-lease contact; stale-term or non-member frames must not keep
//!   a partitioned leader alive.
//! - A10 (OPS-3): `propose_config_change` rejects a second change while a
//!   joint transition is active, and accepts one after completion.
//! - B8 (P3/P4): index arithmetic saturates at boundaries and is checked in
//!   protocol math — boundary values degrade safely instead of wrapping.
//! - B12 (PS12): the seeded append path propagates storage errors exactly
//!   like its non-seeded twin (durability-contract parity).

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arbitro_raft::{
    encode_message_to_bytes, AppendEntries, AppendEntriesResp, ArbitroRaft, BootstrapPeer,
    ClusterId, EntryPayload, GroupId, HardState, InboundRaftMessage, LimitsConfig, LogEntry,
    LogIndex, NodeConfig, NoopStateMachine, PeerId, RaftError, RaftMessage, RaftNode, RaftStorage,
    RaftTransport, SnapshotMeta, Term, TimingConfig,
};

// ---------------------------------------------------------------------------
// In-memory storage with injectable append faults and an optional lying
// `last_log_position` (for the B8 underflow test).
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
    /// When set, BOTH append arms fail with `RaftError::Storage` (B12).
    fail_appends: Arc<AtomicBool>,
    /// When set, `last_log_position` always reports (0, 0) — a broken
    /// storage used to reach the B8/P4 `prev_log_index` underflow site.
    lie_last_log: Arc<AtomicBool>,
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
        if self.fail_appends.load(Ordering::SeqCst) {
            return Err(RaftError::Storage("injected append failure".into()));
        }
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
    fn append_entries_seeded(
        &self,
        headers: &[arbitro_raft::EntryHeader],
        payloads: &[&[u8]],
    ) -> Result<(), RaftError> {
        if self.fail_appends.load(Ordering::SeqCst) {
            return Err(RaftError::Storage("injected append failure".into()));
        }
        let mut entries = self.entries.lock().unwrap();
        for (h, p) in headers.iter().zip(payloads.iter()) {
            entries.push(StoredEntry {
                term: Term(h.term.get()),
                index: LogIndex(h.index.get()),
                payload: p.to_vec(),
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
        if self.lie_last_log.load(Ordering::SeqCst) {
            return Ok((LogIndex(0), Term(0)));
        }
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
// Transport with an injectable inbound queue; outbound frames are discarded.
// ---------------------------------------------------------------------------

struct InjectTransport {
    rx: Arc<tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>>>,
}

impl InjectTransport {
    fn new() -> (Self, tokio::sync::mpsc::UnboundedSender<Vec<u8>>) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        (
            Self {
                rx: Arc::new(tokio::sync::Mutex::new(rx)),
            },
            tx,
        )
    }
}

impl RaftTransport for InjectTransport {
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
        let rx = self.rx.clone();
        async move {
            let mut rx = rx.lock().await;
            let frame = rx
                .recv()
                .await
                .ok_or(RaftError::Transport("closed".into()))?;
            if out.len() < frame.len() {
                return Err(RaftError::Transport("buffer too small".into()));
            }
            out[..frame.len()].copy_from_slice(&frame);
            Ok(frame.len())
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
                if let Ok(frame) = rx.try_recv() {
                    if out.len() < frame.len() {
                        return Err(RaftError::Transport("buffer too small".into()));
                    }
                    out[..frame.len()].copy_from_slice(&frame);
                    return Ok(Some(frame.len()));
                }
                return Ok(None);
            }
            match tokio::time::timeout(timeout, rx.recv()).await {
                Ok(Some(frame)) => {
                    if out.len() < frame.len() {
                        return Err(RaftError::Transport("buffer too small".into()));
                    }
                    out[..frame.len()].copy_from_slice(&frame);
                    Ok(Some(frame.len()))
                }
                _ => Ok(None),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
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

fn resp_msg(term: u64, match_index: u64) -> AppendEntriesResp {
    AppendEntriesResp {
        term: term.into(),
        match_index: match_index.into(),
        success: 1,
        _pad: [0; 7],
    }
}

fn encoded_resp_frame(from: u64, term: u64) -> Vec<u8> {
    let resp = resp_msg(term, 0);
    encode_message_to_bytes(PeerId(from), &RaftMessage::AppendEntriesResp(&resp))
        .expect("encode failed")
        .to_vec()
}

/// One 24-byte wire `EntryHeader` (term, index, payload_len, pad) as raw bytes.
fn entry_header_bytes(term: u64, index: u64, payload_len: u32) -> Vec<u8> {
    let mut h = Vec::with_capacity(24);
    h.extend_from_slice(&term.to_le_bytes());
    h.extend_from_slice(&index.to_le_bytes());
    h.extend_from_slice(&payload_len.to_le_bytes());
    h.extend_from_slice(&0u32.to_le_bytes());
    h
}

fn append_entries_body(term: u64, entry_count: u32) -> AppendEntries {
    AppendEntries {
        term: term.into(),
        leader_id: 2u64.into(),
        prev_log_index: 0u64.into(),
        prev_log_term: 0u64.into(),
        leader_commit: 0u64.into(),
        entry_count: entry_count.into(),
        _pad: 0u32.into(),
    }
}

// ---------------------------------------------------------------------------
// A8 — quorum-lease contact filter (unit-level).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a8_quorum_lease_counts_only_current_term_voter_contact() {
    let (transport, _tx) = InjectTransport::new();
    let mut node = RaftNode::new(
        make_config(1, &[1, 2, 3]),
        TestStorage::default(),
        transport,
    )
    .unwrap();
    node.become_leader_for_benchmark(Term(5));

    // Fresh leader: progress init stamps contact for every peer — lease alive.
    assert!(
        node.check_quorum_active(),
        "fresh leader must start with an active quorum lease"
    );

    // Let the initial contact stamps age past the election timeout (300ms max).
    tokio::time::sleep(Duration::from_millis(350)).await;
    assert!(
        !node.check_quorum_active(),
        "aged-out contact must drop the quorum lease"
    );

    // A STALE-term frame from a voter must NOT refresh the lease (A8/PS8).
    let stale = resp_msg(3, 0);
    node.handle_inbound(InboundRaftMessage {
        from: PeerId(2),
        group_id: GroupId(0),
        message: RaftMessage::AppendEntriesResp(&stale),
    })
    .await
    .unwrap();
    assert!(
        !node.check_quorum_active(),
        "a stale-term frame must not count as quorum contact (A8/PS8)"
    );

    // A current-term frame from a NON-member must NOT refresh the lease.
    let current = resp_msg(5, 0);
    node.handle_inbound(InboundRaftMessage {
        from: PeerId(99),
        group_id: GroupId(0),
        message: RaftMessage::AppendEntriesResp(&current),
    })
    .await
    .unwrap();
    assert!(
        !node.check_quorum_active(),
        "a non-member frame must not count as quorum contact"
    );

    // A current-term frame from a voter DOES refresh: self + peer 2 = 2 >= quorum(3).
    node.handle_inbound(InboundRaftMessage {
        from: PeerId(2),
        group_id: GroupId(0),
        message: RaftMessage::AppendEntriesResp(&current),
    })
    .await
    .unwrap();
    assert!(
        node.check_quorum_active(),
        "current-term voter contact must refresh the quorum lease"
    );
}

// ---------------------------------------------------------------------------
// A8 — end-to-end: a leader fed ONLY stale-term frames abdicates on the
// check-quorum deadline; one fed current-term voter contact retains.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a8_leader_abdicates_when_only_stale_term_contact_arrives() {
    let (transport, tx) = InjectTransport::new();
    let node = RaftNode::new(
        make_config(1, &[1, 2, 3]),
        TestStorage::default(),
        transport,
    )
    .unwrap();
    let mut raft = ArbitroRaft::new(node, NoopStateMachine);
    raft.node_mut().become_leader_for_benchmark(Term(5));

    // Keep feeding stale-term frames from both voters. Before the A8 fix these
    // refreshed the lease and the leader never abdicated; with the fix the
    // initial contact stamps age out (~300ms) and the next idle heartbeat tick
    // steps the leader down.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut abdicated = false;
    while std::time::Instant::now() < deadline {
        for from in [2u64, 3] {
            tx.send(encoded_resp_frame(from, 3)).unwrap();
        }
        // First tick drains the frames; second tick reaches the idle
        // heartbeat/check-quorum path.
        raft.run_once().await.unwrap();
        raft.run_once().await.unwrap();
        if !raft.node().is_leader() {
            abdicated = true;
            break;
        }
    }
    assert!(
        abdicated,
        "leader receiving only stale-term frames must abdicate on the \
         check-quorum deadline (A8/PS8)"
    );
}

#[tokio::test]
async fn a8_leader_retains_leadership_under_current_term_contact() {
    let (transport, tx) = InjectTransport::new();
    let node = RaftNode::new(
        make_config(1, &[1, 2, 3]),
        TestStorage::default(),
        transport,
    )
    .unwrap();
    let mut raft = ArbitroRaft::new(node, NoopStateMachine);
    raft.node_mut().become_leader_for_benchmark(Term(5));

    // 800ms > election_max (300ms): the initial stamps expire mid-run, so
    // survival proves the current-term frames are refreshing the lease.
    let until = std::time::Instant::now() + Duration::from_millis(800);
    while std::time::Instant::now() < until {
        for from in [2u64, 3] {
            tx.send(encoded_resp_frame(from, 5)).unwrap();
        }
        raft.run_once().await.unwrap();
        raft.run_once().await.unwrap();
        assert!(
            raft.node().is_leader(),
            "leader with current-term voter contact must retain leadership"
        );
    }
}

// ---------------------------------------------------------------------------
// A10 — config-change lifecycle: completion, re-acceptance, and rejection of
// a concurrent change while the joint config is active.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a10_concurrent_config_change_rejected_until_previous_completes() {
    let (transport, tx) = InjectTransport::new();
    let node = RaftNode::new(make_config(1, &[1]), TestStorage::default(), transport).unwrap();
    let mut raft = ArbitroRaft::new(node, NoopStateMachine);
    raft.node_mut().become_leader_for_benchmark(Term(1));

    // Pump: inject one dummy frame so the leader tick applies committed
    // entries (config changes apply through the run loop's apply path).
    async fn pump_apply(
        raft: &mut ArbitroRaft<TestStorage, InjectTransport, NoopStateMachine>,
        tx: &tokio::sync::mpsc::UnboundedSender<Vec<u8>>,
    ) {
        tx.send(encoded_resp_frame(7, 0)).unwrap();
        raft.run_once().await.unwrap();
    }

    // 1. Identity change on a single-node cluster commits standalone.
    raft.propose_config_change(vec![PeerId(1)])
        .await
        .expect("single-node identity config change must commit");
    assert!(
        raft.status().config_change_in_progress,
        "joint config stays active until the Final entry is APPLIED locally"
    );
    pump_apply(&mut raft, &tx).await;
    assert!(
        !raft.status().config_change_in_progress,
        "applying the Final entry must clear the joint config"
    );

    // 2. A NEW change is accepted once the previous one completed (A10).
    raft.propose_config_change(vec![PeerId(1)])
        .await
        .expect("config change after completion must be accepted");
    pump_apply(&mut raft, &tx).await;
    assert!(!raft.status().config_change_in_progress);

    // 3. A grow toward an unreachable peer stalls in its joint phase: the
    //    joint entry is appended (activating the joint config) but cannot
    //    commit under the dual quorum, so the propose times out.
    let res = raft.propose_config_change(vec![PeerId(1), PeerId(2)]).await;
    assert!(
        matches!(res, Err(RaftError::NoQuorum)),
        "grow to an unreachable peer must time out with NoQuorum, got {res:?}"
    );
    assert!(
        raft.status().config_change_in_progress,
        "appended-but-uncommitted joint entry must stay active (Raft §4.1)"
    );

    // 4. THE PIN (A10/OPS-3): a second change while the joint transition is
    //    active must be rejected — overlapping changes are outside §4.3.
    let res2 = raft.propose_config_change(vec![PeerId(1), PeerId(2)]).await;
    assert!(
        matches!(
            res2,
            Err(RaftError::InvalidConfig(
                "config change already in progress"
            ))
        ),
        "concurrent config change must be rejected with InvalidConfig, got {res2:?}"
    );
}

// ---------------------------------------------------------------------------
// B8 — boundary arithmetic: near-u64::MAX values degrade safely.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn b8_append_resp_match_index_near_max_saturates_and_is_capped() {
    let (transport, _tx) = InjectTransport::new();
    let mut node = RaftNode::new(
        make_config(1, &[1, 2, 3]),
        TestStorage::default(),
        transport,
    )
    .unwrap();
    node.become_leader_for_benchmark(Term(5));

    // A (spoofed/broken) follower acks match_index = u64::MAX. The unchecked
    // sibling of this site wrapped `match_index + 1` to 0 in release and
    // panicked in debug; the saturating+capped policy must clamp next_index
    // at the leader's own last log + 1.
    let resp = resp_msg(5, u64::MAX);
    node.handle_inbound(InboundRaftMessage {
        from: PeerId(2),
        group_id: GroupId(0),
        message: RaftMessage::AppendEntriesResp(&resp),
    })
    .await
    .unwrap();

    let (next_index, match_index) = node
        .peer_progress(PeerId(2))
        .expect("leader must track progress for voter 2");
    assert_eq!(match_index, LogIndex(u64::MAX));
    assert_eq!(
        next_index,
        LogIndex(1),
        "next_index must be capped at leader last_log + 1, not wrap past u64::MAX"
    );
}

#[tokio::test]
async fn b8_prev_log_index_underflow_surfaces_as_corrupt_log_not_wrap() {
    // A broken storage that reports a last-log tip SMALLER than the batch the
    // node just appended. The old `last_log - len` (P4) wrapped to ~u64::MAX
    // (debug: panic); the checked policy must surface CorruptLog instead.
    let storage = TestStorage::default();
    storage.lie_last_log.store(true, Ordering::SeqCst);
    let (transport, _tx) = InjectTransport::new();
    let mut node = RaftNode::new(make_config(1, &[1, 2, 3]), storage, transport).unwrap();
    node.become_leader_for_benchmark(Term(1));

    let res = node.propose_once(b"x").await;
    assert!(
        matches!(res, Err(RaftError::CorruptLog(_))),
        "prev_log_index underflow must surface as CorruptLog, got {res:?}"
    );
}

// ---------------------------------------------------------------------------
// B12 — seeded append path propagates storage errors like its twin.
// ---------------------------------------------------------------------------

async fn drive_seeded_append(storage: TestStorage) -> Result<(), RaftError> {
    let (transport, _tx) = InjectTransport::new();
    let mut node = RaftNode::new(make_config(1, &[1, 2, 3]), storage, transport).unwrap();
    let ae = append_entries_body(1, 1);
    let headers = entry_header_bytes(1, 1, 3);
    node.handle_inbound(InboundRaftMessage {
        from: PeerId(2),
        group_id: GroupId(0),
        message: RaftMessage::AppendEntriesSeeded {
            ae: &ae,
            headers: &headers,
            payloads: b"abc",
        },
    })
    .await
}

async fn drive_contiguous_append(storage: TestStorage) -> Result<(), RaftError> {
    let (transport, _tx) = InjectTransport::new();
    let mut node = RaftNode::new(make_config(1, &[1, 2, 3]), storage, transport).unwrap();
    let ae = append_entries_body(1, 1);
    // Contiguous wire form: EntryHeader immediately followed by the payload.
    let mut block = entry_header_bytes(1, 1, 3);
    block.extend_from_slice(b"abc");
    node.handle_inbound(InboundRaftMessage {
        from: PeerId(2),
        group_id: GroupId(0),
        message: RaftMessage::AppendEntries(&ae, &block),
    })
    .await
}

#[tokio::test]
async fn b12_seeded_append_storage_error_propagates_like_contiguous_twin() {
    // Fault ON: BOTH arms must surface the storage error (never swallow it) —
    // and it must classify Fatal so the run loop halts instead of acking.
    let seeded_storage = TestStorage::default();
    seeded_storage.fail_appends.store(true, Ordering::SeqCst);
    let seeded_err = drive_seeded_append(seeded_storage)
        .await
        .expect_err("seeded append with failing storage must surface the error (B12/PS12)");
    assert!(
        matches!(seeded_err, RaftError::Storage(_)) && seeded_err.is_fatal(),
        "seeded arm must propagate the storage error as Fatal, got {seeded_err:?}"
    );

    let contiguous_storage = TestStorage::default();
    contiguous_storage
        .fail_appends
        .store(true, Ordering::SeqCst);
    let contiguous_err = drive_contiguous_append(contiguous_storage)
        .await
        .expect_err("contiguous append with failing storage must surface the error");
    assert!(
        matches!(contiguous_err, RaftError::Storage(_)) && contiguous_err.is_fatal(),
        "contiguous arm must propagate the storage error as Fatal, got {contiguous_err:?}"
    );

    // Fault OFF: positive control — the seeded arm actually appends.
    let ok_storage = TestStorage::default();
    drive_seeded_append(ok_storage.clone())
        .await
        .expect("seeded append with healthy storage must succeed");
    let mut buf = [0u8; 16];
    let entry = ok_storage
        .entry_at(LogIndex(1), &mut buf)
        .unwrap()
        .expect("seeded entry must be durable at index 1");
    assert_eq!(entry.term, Term(1));
    assert_eq!(entry.payload.0, b"abc");
}

// ---------------------------------------------------------------------------
// B1/US3 — the seeded (zerocopy) and contiguous (fallback) leader append
// paths produce logically identical wire frames.
//
// The B1 restructure removed every `'static`-laundering transmute from the
// leader send path: payload views now live in dock-recycled LOCAL vecs whose
// lifetimes the borrow checker verifies (tied to `&self.storage` via the
// `for_each_payload`/`read_entry_headers` contracts). This test pins that the
// restructure did not change the bytes a follower decodes: the same backlog
// shipped through BOTH paths must decode to the same AppendEntries metadata
// and the same (term, index, payload) entry sequence. It also gives the
// seeded SEND path (previously exercised only by benches) test + Miri
// coverage.
// ---------------------------------------------------------------------------

/// Immutable pre-seeded storage servable through BOTH replication paths.
/// `seeded == true` advertises the zerocopy pair (`read_entry_headers` +
/// `for_each_payload`, views tied to `&self`); `seeded == false` keeps the
/// trait defaults so the engine takes the `read_entries` fallback.
struct DualPathStorage {
    seeded: bool,
    headers: Vec<arbitro_raft::EntryHeader>,
    payloads: Vec<Vec<u8>>,
}

impl DualPathStorage {
    fn new(seeded: bool, term: u64, payloads: Vec<Vec<u8>>) -> Self {
        let headers = payloads
            .iter()
            .enumerate()
            .map(|(i, p)| arbitro_raft::EntryHeader {
                term: term.into(),
                index: (i as u64 + 1).into(),
                payload_len: (p.len() as u32).into(),
                _pad: 0.into(),
            })
            .collect();
        Self {
            seeded,
            headers,
            payloads,
        }
    }
}

impl RaftStorage for DualPathStorage {
    fn load_hard_state(&self) -> Result<HardState, RaftError> {
        Ok(HardState::default())
    }
    fn save_hard_state(&self, _state: &HardState) -> Result<(), RaftError> {
        Ok(())
    }
    fn append_entries(&self, _entries: &[LogEntry<'_>]) -> Result<(), RaftError> {
        Ok(())
    }
    fn read_entries<'a>(
        &self,
        from: LogIndex,
        to: LogIndex,
        out: &mut Vec<LogEntry<'a>>,
        payload_buf: &'a mut [u8],
    ) -> Result<usize, RaftError> {
        let mut buf = payload_buf;
        let mut written = 0;
        for (h, p) in self.headers.iter().zip(self.payloads.iter()) {
            let index = LogIndex(h.index.get());
            if index >= from && index < to {
                let len = p.len();
                if len > buf.len() {
                    return Err(RaftError::Storage("payload_buf too small".into()));
                }
                let (chunk, rest) = std::mem::take(&mut buf).split_at_mut(len);
                chunk.copy_from_slice(p);
                buf = rest;
                out.push(LogEntry {
                    term: Term(h.term.get()),
                    index,
                    payload: EntryPayload(chunk),
                });
                written += len;
            }
        }
        Ok(written)
    }
    fn read_entry_headers(
        &self,
        from: LogIndex,
        max: LogIndex,
    ) -> Result<Option<&[arbitro_raft::EntryHeader]>, RaftError> {
        if !self.seeded || self.headers.is_empty() {
            return Ok(None);
        }
        let base = self.headers[0].index.get();
        let s = (from.0.saturating_sub(base)) as usize;
        if s >= self.headers.len() {
            return Ok(None);
        }
        let e = ((max.0.saturating_sub(base)) as usize)
            .saturating_add(1)
            .min(self.headers.len());
        // Views tied to `&self` — immutable storage, no unsafe needed.
        Ok(Some(&self.headers[s..e]))
    }
    fn for_each_payload<'a>(
        &'a self,
        from: LogIndex,
        to: LogIndex,
        f: &mut dyn FnMut(&'a [u8]),
    ) -> Result<(), RaftError> {
        let base = self.headers[0].index.get();
        for i in from.0..=to.0 {
            let slot = (i.saturating_sub(base)) as usize;
            f(&self.payloads[slot]);
        }
        Ok(())
    }
    fn entry_at<'a>(
        &self,
        index: LogIndex,
        payload_buf: &'a mut [u8],
    ) -> Result<Option<LogEntry<'a>>, RaftError> {
        for (h, p) in self.headers.iter().zip(self.payloads.iter()) {
            if h.index.get() == index.0 {
                if payload_buf.len() < p.len() {
                    return Err(RaftError::Storage("payload_buf too small".into()));
                }
                payload_buf[..p.len()].copy_from_slice(p);
                let payload: &'a [u8] = &payload_buf[..p.len()];
                return Ok(Some(LogEntry {
                    term: Term(h.term.get()),
                    index,
                    payload: EntryPayload(payload),
                }));
            }
        }
        Ok(None)
    }
    fn last_log_position(&self) -> Result<(LogIndex, Term), RaftError> {
        Ok(self
            .headers
            .last()
            .map(|h| (LogIndex(h.index.get()), Term(h.term.get())))
            .unwrap_or_default())
    }
    fn truncate_suffix(&self, _from: LogIndex) -> Result<(), RaftError> {
        Ok(())
    }
    fn save_snapshot(&self, _: &SnapshotMeta, _: &[u8]) -> Result<(), RaftError> {
        Ok(())
    }
    fn load_snapshot(&self) -> Result<Option<(SnapshotMeta, Vec<u8>)>, RaftError> {
        Ok(None)
    }
}

/// Transport that records every outbound frame (vectored writes are
/// concatenated exactly as a stream socket would see them).
struct CaptureTransport {
    sent: Arc<Mutex<Vec<(u64, Vec<u8>)>>>,
}

impl CaptureTransport {
    fn new() -> (Self, Arc<Mutex<Vec<(u64, Vec<u8>)>>>) {
        let sent = Arc::new(Mutex::new(Vec::new()));
        (Self { sent: sent.clone() }, sent)
    }
}

impl RaftTransport for CaptureTransport {
    fn send_vectored(
        &self,
        peer: PeerId,
        slices: &[&[u8]],
    ) -> impl std::future::Future<Output = Result<(), RaftError>> + Send {
        let mut frame = Vec::new();
        for s in slices {
            frame.extend_from_slice(s);
        }
        self.sent.lock().unwrap().push((peer.0, frame));
        async move { Ok(()) }
    }
    fn send_frame_owned(
        &self,
        peer: PeerId,
        frame: bytes::Bytes,
    ) -> impl std::future::Future<Output = Result<(), RaftError>> + Send {
        self.sent.lock().unwrap().push((peer.0, frame.to_vec()));
        async move { Ok(()) }
    }
    fn recv_frame(
        &self,
        _out: &mut [u8],
    ) -> impl std::future::Future<Output = Result<usize, RaftError>> + Send {
        async move { Err(RaftError::Transport("recv not used in this test".into())) }
    }
    fn recv_frame_timeout(
        &self,
        _timeout: Duration,
        _out: &mut [u8],
    ) -> impl std::future::Future<Output = Result<Option<usize>, RaftError>> + Send {
        async move { Ok(None) }
    }
}

/// Drive one heartbeat-tick repair append to peer 2 (whose `next_index` was
/// walked back to 3) and return the raw frame the leader put on the wire.
async fn capture_repair_frame(seeded: bool) -> Vec<u8> {
    let payloads: Vec<Vec<u8>> = (0..5u8).map(|i| vec![b'a' + i; 8 + i as usize]).collect();
    let storage = DualPathStorage::new(seeded, 1, payloads);
    let (transport, sent) = CaptureTransport::new();
    let mut node =
        RaftNode::new(make_config(1, &[1, 2, 3]), storage, transport).expect("node boot");
    node.become_leader_for_benchmark(Term(1));

    // Three failed probes walk peer 2's next_index 6 -> 5 -> 4 -> 3.
    let resp = AppendEntriesResp {
        term: 1u64.into(),
        match_index: 2u64.into(),
        success: 0,
        _pad: [0; 7],
    };
    for _ in 0..3 {
        node.handle_inbound(InboundRaftMessage {
            from: PeerId(2),
            group_id: GroupId(0),
            message: RaftMessage::AppendEntriesResp(&resp),
        })
        .await
        .expect("resp handled");
    }
    assert_eq!(
        node.peer_progress(PeerId(2)).map(|(n, _)| n),
        Some(LogIndex(3)),
        "walk-back must leave peer 2 at next_index 3"
    );

    // A9 repair branch ships entries [3..=5] through send_append_attempt.
    node.send_heartbeat_once().await.expect("heartbeat tick");

    let sent = sent.lock().unwrap();
    sent.iter()
        .find(|(p, _)| *p == 2)
        .map(|(_, f)| f.clone())
        .expect("no frame captured for peer 2")
}

#[tokio::test]
async fn b1_seeded_and_contiguous_append_paths_are_wire_equivalent() {
    use arbitro_raft::protocol::{AppendEntriesEntryIter, AppendEntriesRawIter, SeededPayloads};

    let seeded_frame = capture_repair_frame(true).await;
    let contiguous_frame = capture_repair_frame(false).await;

    let seeded = arbitro_raft::decode_message(&seeded_frame).expect("seeded frame decodes");
    let contiguous =
        arbitro_raft::decode_message(&contiguous_frame).expect("contiguous frame decodes");

    let (s_ae, s_headers, s_payloads) = seeded
        .as_append_entries_seeded()
        .expect("seeded path must emit an AppendEntriesSeeded frame");
    let RaftMessage::AppendEntries(c_ae, c_payload) = contiguous.message else {
        panic!("contiguous path must emit an AppendEntries frame");
    };

    // Identical replication metadata on both paths.
    for (name, s, c) in [
        ("term", s_ae.term.get(), c_ae.term.get()),
        ("leader_id", s_ae.leader_id.get(), c_ae.leader_id.get()),
        (
            "prev_log_index",
            s_ae.prev_log_index.get(),
            c_ae.prev_log_index.get(),
        ),
        (
            "prev_log_term",
            s_ae.prev_log_term.get(),
            c_ae.prev_log_term.get(),
        ),
        (
            "leader_commit",
            s_ae.leader_commit.get(),
            c_ae.leader_commit.get(),
        ),
        (
            "entry_count",
            s_ae.entry_count.get() as u64,
            c_ae.entry_count.get() as u64,
        ),
    ] {
        assert_eq!(s, c, "AppendEntries.{name} differs between paths");
    }
    assert_eq!(s_ae.entry_count.get(), 3, "repair must ship entries 3..=5");

    // Identical logical entries, and exactly the expected backlog.
    let s_entries: Vec<(u64, u64, Vec<u8>)> =
        AppendEntriesRawIter::new(s_headers, SeededPayloads::Contiguous(s_payloads))
            .map(|(h, p)| (h.term.get(), h.index.get(), p.to_vec()))
            .collect();
    let c_entries: Vec<(u64, u64, Vec<u8>)> =
        AppendEntriesEntryIter::new(c_payload, c_ae.entry_count.get() as usize)
            .map(|e| (e.term.0, e.index.0, e.payload.0.to_vec()))
            .collect();
    assert_eq!(s_entries, c_entries, "entry streams differ between paths");
    let expected: Vec<(u64, u64, Vec<u8>)> = (3..=5u64)
        .map(|i| (1, i, vec![b'a' + (i as u8 - 1); 8 + (i as usize - 1)]))
        .collect();
    assert_eq!(s_entries, expected, "unexpected backlog contents");
}
