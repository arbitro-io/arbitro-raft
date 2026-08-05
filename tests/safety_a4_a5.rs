//! Pinning tests for the A4 / A5 safety-fix batch.
//!
//! - A4 (audit Plausible P1): elections during a joint transition require
//!   BOTH majorities (Raft §4.3 applies to elections, not just commits).
//!   A candidate whose grants form a majority of the UNION but not of one
//!   side (old or new) must NOT win while joint; it must win once it holds
//!   both majorities. Both tests fail on the pre-fix union-only tally.
//! - A5 (C7 / ERR-7): a `step_down` must resolve every in-flight custom
//!   dispatch with a deterministic `LostLeadership` failure (surfaced as
//!   `RaftError::NotLeader`) BEFORE clearing `pending_custom`, so no
//!   `handle.wait()` caller parks forever. The test fails on the pre-fix
//!   silent `clear()` (the waiter hangs until the outer watchdog).

#![allow(clippy::ptr_arg)] // test helper takes &Vec deliberately

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arbitro_raft::{
    encode_message_to_bytes, AppendEntries, AppendEntriesResp, BootstrapPeer, ClusterId,
    ConfigChangeEntry, ConfigChangePhase, DispatchAckPolicy, DispatchFailPolicy, DispatchScope,
    DispatchSpec, EntryPayload, GroupId, HardState, InboundRaftMessage, LimitsConfig, LogEntry,
    LogIndex, NodeConfig, PeerId, RaftError, RaftMessage, RaftNode, RaftStorage, RaftTransport,
    RequestVoteResp, SnapshotMeta, Term, TimingConfig,
};

// ---------------------------------------------------------------------------
// In-memory storage (same shape as the other safety-test harnesses).
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
// Transport with an injectable inbound queue; outbound frames are discarded
// (every send succeeds, so all peers count as reachable voters).
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
                addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 9200 + id as u16)),
            })
            .collect(),
        timing: TimingConfig {
            heartbeat_ms: 50,
            // Short election window: a losing campaign resolves in ~200ms.
            election_min_ms: 100,
            election_max_ms: 200,
        },
        limits: LimitsConfig::default(),
    }
}

fn vote_resp_frame(from: u64, term: u64, granted: bool) -> Vec<u8> {
    let resp = RequestVoteResp {
        term: term.into(),
        vote_granted: if granted { 1 } else { 0 },
        _pad: [0; 7],
    };
    encode_message_to_bytes(PeerId(from), &RaftMessage::RequestVoteResp(&resp))
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

/// Put `node` into an ACTIVE joint configuration through the public
/// follower path: append-time activation (Raft §4.1) of a Joint entry
/// replicated by "leader 2" at term 1.
async fn activate_joint(
    node: &mut RaftNode<TestStorage, InjectTransport>,
    old_peers: &[u64],
    new_peers: &[u64],
) {
    let joint = ConfigChangeEntry {
        phase: ConfigChangePhase::Joint,
        old_peers: old_peers.iter().copied().map(PeerId).collect(),
        new_peers: new_peers.iter().copied().map(PeerId).collect(),
    };
    let payload = joint.encode();
    let ae = AppendEntries {
        term: 1u64.into(),
        leader_id: 2u64.into(),
        prev_log_index: 0u64.into(),
        prev_log_term: 0u64.into(),
        leader_commit: 0u64.into(),
        entry_count: 1u32.into(),
        _pad: 0u32.into(),
    };
    let mut block = entry_header_bytes(1, 1, payload.len() as u32);
    block.extend_from_slice(&payload);
    node.handle_inbound(InboundRaftMessage {
        from: PeerId(2),
        group_id: GroupId(0),
        message: RaftMessage::AppendEntries(&ae, &block),
    })
    .await
    .expect("joint entry append must succeed");
    assert!(
        node.status().config_change_in_progress,
        "joint config must be active after append-time activation"
    );
}

// ---------------------------------------------------------------------------
// A4 — a union majority WITHOUT a majority of C_old must not win while joint.
//
// C_old = {1,2,3}, C_new = {1,2,3,4,5} (grow), union = {1,2,3,4,5}.
// Candidate 1 + grants from 4 and 5 = 3 votes: a majority of the union
// (and of C_new), but only ONE vote from C_old. Pre-fix (union-only tally,
// quorum(5)=3) this campaign WINS — the assertion below is the pin.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a4_joint_election_union_majority_without_old_majority_must_not_win() {
    let (transport, tx) = InjectTransport::new();
    let mut node = RaftNode::new(
        make_config(1, &[1, 2, 3]),
        TestStorage::default(),
        transport,
    )
    .unwrap();
    activate_joint(&mut node, &[1, 2, 3], &[1, 2, 3, 4, 5]).await;
    assert_eq!(node.peers().len(), 5, "union must be the broadcast set");

    // Campaign #1: new-side grants only (4, 5); old-side voters deny.
    let campaign_term = node.current_term().0 + 1;
    tx.send(vote_resp_frame(4, campaign_term, true)).unwrap();
    tx.send(vote_resp_frame(5, campaign_term, true)).unwrap();
    tx.send(vote_resp_frame(2, campaign_term, false)).unwrap();
    tx.send(vote_resp_frame(3, campaign_term, false)).unwrap();

    let mut buf = vec![0u8; 64 * 1024];
    let won = node.campaign_once(&mut buf).await;
    assert!(
        !matches!(won, Ok(true)),
        "THE PIN (A4): a union majority without a majority of C_old must NOT \
         win an election while the joint config is active, got {won:?}"
    );
    assert!(
        !node.is_leader(),
        "candidate must not assume leadership on a union-only majority"
    );

    // Campaign #2: additionally granted by old-side voter 2 → majority of
    // C_old ({1,2} of 3) AND majority of C_new ({1,2,4,5} of 5) → must win.
    let campaign_term = node.current_term().0 + 1;
    tx.send(vote_resp_frame(2, campaign_term, true)).unwrap();
    tx.send(vote_resp_frame(4, campaign_term, true)).unwrap();
    tx.send(vote_resp_frame(5, campaign_term, true)).unwrap();

    let won = node
        .campaign_once(&mut buf)
        .await
        .expect("dual-majority campaign must not error");
    assert!(
        won && node.is_leader(),
        "a candidate holding BOTH majorities must win the joint election"
    );
}

// ---------------------------------------------------------------------------
// A4 (mirror) — a union majority WITHOUT a majority of C_new must not win.
//
// C_old = {1,2,3}, C_new = {4,5} (full replacement), union = {1,2,3,4,5}.
// Candidate 1 + grants from 2 and 3 = 3 votes: a majority of the union
// (and ALL of C_old), but ZERO votes from C_new. Pre-fix this wins too.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a4_joint_election_union_majority_without_new_majority_must_not_win() {
    let (transport, tx) = InjectTransport::new();
    let mut node = RaftNode::new(
        make_config(1, &[1, 2, 3]),
        TestStorage::default(),
        transport,
    )
    .unwrap();
    activate_joint(&mut node, &[1, 2, 3], &[4, 5]).await;
    assert_eq!(node.peers().len(), 5, "union must be the broadcast set");

    // Campaign #1: old-side grants only (2, 3); new-side voters deny.
    let campaign_term = node.current_term().0 + 1;
    tx.send(vote_resp_frame(2, campaign_term, true)).unwrap();
    tx.send(vote_resp_frame(3, campaign_term, true)).unwrap();
    tx.send(vote_resp_frame(4, campaign_term, false)).unwrap();
    tx.send(vote_resp_frame(5, campaign_term, false)).unwrap();

    let mut buf = vec![0u8; 64 * 1024];
    let won = node.campaign_once(&mut buf).await;
    assert!(
        !matches!(won, Ok(true)),
        "THE PIN (A4 mirror): a union majority without a majority of C_new \
         must NOT win an election while the joint config is active, got {won:?}"
    );
    assert!(!node.is_leader());

    // Campaign #2: grants from 2, 3 (old majority incl. self) AND 4, 5
    // (the whole of C_new) → both majorities → must win.
    let campaign_term = node.current_term().0 + 1;
    for peer in [2u64, 3, 4, 5] {
        tx.send(vote_resp_frame(peer, campaign_term, true)).unwrap();
    }
    let won = node
        .campaign_once(&mut buf)
        .await
        .expect("dual-majority campaign must not error");
    assert!(
        won && node.is_leader(),
        "a candidate holding BOTH majorities must win the joint election"
    );
}

// ---------------------------------------------------------------------------
// A5 — a step-down must resolve every pending custom dispatch with a
// deterministic LostLeadership failure (surfaced as NotLeader), not park
// the waiter (ERR-7). Pre-fix, `step_down` silently cleared the pending
// map and `handle.wait()` hung until the 60s dispatch timeout — the 2s
// watchdog below is the pin.
// ---------------------------------------------------------------------------

fn identity_codec(bytes: &[u8]) -> Result<Vec<u8>, RaftError> {
    Ok(bytes.to_vec())
}

fn identity_encode(value: &Vec<u8>) -> Result<Vec<u8>, RaftError> {
    Ok(value.clone())
}

fn a5_spec() -> DispatchSpec<Vec<u8>, Vec<u8>> {
    DispatchSpec::new(
        0x31,
        identity_encode,
        identity_codec,
        identity_encode,
        identity_codec,
    )
    .with_scope(DispatchScope::Followers)
    .with_ack_policy(DispatchAckPolicy::Quorum)
    .with_fail_policy(DispatchFailPolicy::AllowFailures)
    // Deliberately far beyond the watchdog: only the LostLeadership
    // notification can resolve the waiter inside the test budget.
    .with_timeout(Duration::from_secs(60))
}

#[tokio::test]
async fn a5_pending_dispatch_resolves_with_not_leader_on_step_down() {
    let (transport, _tx) = InjectTransport::new();
    let mut node = RaftNode::new(
        make_config(1, &[1, 2, 3]),
        TestStorage::default(),
        transport,
    )
    .unwrap();
    node.become_leader_for_benchmark(Term(1));

    // Fan out to followers 2 and 3; neither ever responds, so the
    // transaction stays pending on the leader.
    let handle = node
        .dispatch(a5_spec(), b"payload".to_vec())
        .await
        .expect("dispatch fan-out must succeed");
    assert!(
        !handle.is_ready(),
        "dispatch must be pending while no follower responded"
    );

    // Leadership loss: a higher-term frame forces step_down.
    let resp = AppendEntriesResp {
        term: 5u64.into(),
        match_index: 0u64.into(),
        success: 0,
        _pad: [0; 7],
    };
    node.handle_inbound(InboundRaftMessage {
        from: PeerId(2),
        group_id: GroupId(0),
        message: RaftMessage::AppendEntriesResp(&resp),
    })
    .await
    .unwrap();
    assert!(
        !node.is_leader(),
        "higher-term frame must depose the leader"
    );

    // THE PIN (A5 / ERR-7): the waiter must resolve promptly and
    // deterministically — not hang until the dispatch timeout.
    let res = tokio::time::timeout(Duration::from_secs(2), handle.wait())
        .await
        .expect(
            "dispatch waiter parked after step-down (ERR-7) — pending_custom \
             was cleared without a LostLeadership notification",
        );
    assert!(
        matches!(res, Err(RaftError::NotLeader { .. })),
        "lost-leadership dispatch must resolve with NotLeader, got {res:?}"
    );
    assert!(
        handle.is_ready(),
        "completion must be observable afterwards"
    );

    // A late follower response for the aborted tx must be a harmless no-op
    // (the registration was dropped) — the completion must not change.
    let res2 = handle.try_result().expect("completion must persist");
    assert!(matches!(res2, Err(RaftError::NotLeader { .. })));
}
