//! C1 — storage fault-injection tests: the C-D1 durability contract
//! ("durable before Ok") proven against a failing/tearing/short-reading
//! store, using the reusable `FaultStorage` harness (`tests/support/`).
//!
//! What must hold when storage faults:
//! - a vote whose `save_hard_state` fails is NEVER granted (double-vote
//!   prevention survives a persist failure) and the error is Fatal;
//! - a leader whose `append_entries` fails NEVER counts the entry toward a
//!   commit quorum, never fans it out, and surfaces the Fatal error;
//! - a follower whose `append_entries` fails NEVER acks (a leader must never
//!   count an un-persisted follower);
//! - a torn write (prefix persisted) either surfaces the error (no ack) or —
//!   if the storage lies with `Ok` — a restart converges to the durable
//!   prefix and normal replication repairs the tail (no silent divergence);
//! - a read fault on the critical inbound path is Fatal (never a wrong-data
//!   ack); a read fault on the leader's per-peer repair path is contained to
//!   that peer per the error taxonomy; a short read ships only what was
//!   actually read (metadata never fabricated).

#[path = "support/fault_storage.rs"]
mod support;

use support::{
    contiguous_entry_block, make_config, CaptureTransport, FaultOp, FaultStorage, TornReport,
};

use arbitro_raft::protocol::AppendEntriesEntryIter;
use arbitro_raft::{
    decode_message, AppendEntries, AppendEntriesResp, GroupId, HardState, InboundRaftMessage,
    LogIndex, PeerId, RaftError, RaftMessage, RaftNode, RaftStorage, RaftTransport, RequestVote,
    Term,
};

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Transport whose INBOUND side pops pre-loaded frames — used to hand a
/// follower's REAL wire ack to a leader mid-quorum-gather. Outbound frames
/// are discarded (this test asserts commit math, not fan-out).
#[derive(Clone, Default)]
struct QueueTransport {
    inbound: Arc<Mutex<VecDeque<Vec<u8>>>>,
}

impl QueueTransport {
    fn push_inbound(&self, frame: Vec<u8>) {
        self.inbound.lock().unwrap().push_back(frame);
    }

    fn pop_into(&self, out: &mut [u8]) -> Option<usize> {
        let frame = self.inbound.lock().unwrap().pop_front()?;
        out[..frame.len()].copy_from_slice(&frame);
        Some(frame.len())
    }
}

impl RaftTransport for QueueTransport {
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
        let popped = self.pop_into(out);
        async move { popped.ok_or_else(|| RaftError::Transport("queue empty".into())) }
    }

    fn recv_frame_timeout(
        &self,
        timeout: Duration,
        out: &mut [u8],
    ) -> impl std::future::Future<Output = Result<Option<usize>, RaftError>> + Send {
        let popped = self.pop_into(out);
        async move {
            if popped.is_none() && !timeout.is_zero() {
                tokio::time::sleep(timeout).await;
            }
            Ok(popped)
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn new_node(
    storage: &FaultStorage,
    transport: &CaptureTransport,
    id: u64,
    peers: &[u64],
) -> RaftNode<FaultStorage, CaptureTransport> {
    RaftNode::new(make_config(id, peers), storage.clone(), transport.clone())
        .expect("node must boot over the fault storage")
}

fn ae_msg(term: u64, leader: u64, prev_idx: u64, prev_term: u64, count: u32) -> AppendEntries {
    AppendEntries {
        term: term.into(),
        leader_id: leader.into(),
        prev_log_index: prev_idx.into(),
        prev_log_term: prev_term.into(),
        leader_commit: 0u64.into(),
        entry_count: count.into(),
        _pad: 0u32.into(),
    }
}

async fn handle_append(
    node: &mut RaftNode<FaultStorage, CaptureTransport>,
    from: u64,
    ae: &AppendEntries,
    block: &[u8],
) -> Result<(), RaftError> {
    node.handle_inbound(InboundRaftMessage {
        from: PeerId(from),
        group_id: GroupId(0),
        message: RaftMessage::AppendEntries(ae, block),
    })
    .await
}

/// Decode the single frame sent to `peer` as an AppendEntriesResp.
fn decoded_ack(transport: &CaptureTransport, peer: u64) -> (u8, u64) {
    let frames = transport.sent_to(peer);
    assert_eq!(frames.len(), 1, "expected exactly one frame to peer {peer}");
    let inbound = decode_message(&frames[0]).expect("frame must decode");
    let RaftMessage::AppendEntriesResp(resp) = inbound.message else {
        panic!("expected AppendEntriesResp, got another frame kind");
    };
    (resp.success, resp.match_index.get())
}

// ---------------------------------------------------------------------------
// 1. save_hard_state failing on a vote → the vote is NEVER granted.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn vote_persist_failure_must_not_grant_the_vote() {
    let storage = FaultStorage::new();
    // Same term as the incoming request so the grant-path save is the FIRST
    // save (no step-down save in front of it).
    storage.set_hard_state(HardState {
        current_term: Term(5),
        voted_for: None,
    });
    let transport = CaptureTransport::new();
    let mut node = new_node(&storage, &transport, 1, &[1, 2, 3]);
    storage.fail_nth(FaultOp::SaveHardState, 1);

    let req = RequestVote {
        term: 5u64.into(),
        candidate_id: 2u64.into(),
        last_log_index: 0u64.into(),
        last_log_term: 0u64.into(),
    };
    let err = node
        .handle_inbound(InboundRaftMessage {
            from: PeerId(2),
            group_id: GroupId(0),
            message: RaftMessage::RequestVote(&req),
        })
        .await
        .expect_err("a vote whose persist failed must surface the storage error");

    assert!(
        matches!(err, RaftError::Storage(_)) && err.is_fatal(),
        "vote-persist failure must classify Fatal (halt the node), got {err:?}"
    );
    assert_eq!(
        storage.calls(FaultOp::SaveHardState),
        1,
        "the grant save was attempted"
    );
    assert!(
        transport.sent().is_empty(),
        "the node must NOT send a vote response after a failed persist — \
         an un-persisted grant is the double-vote hazard (C-D1)"
    );
    assert_eq!(
        storage.durable_hard_state().unwrap().voted_for,
        None,
        "durable state must not record the vote the persist rejected"
    );
}

#[tokio::test]
async fn vote_persist_success_grants_and_is_durable_before_the_response() {
    let storage = FaultStorage::new();
    storage.set_hard_state(HardState {
        current_term: Term(5),
        voted_for: None,
    });
    let transport = CaptureTransport::new();
    let mut node = new_node(&storage, &transport, 1, &[1, 2, 3]);

    let req = RequestVote {
        term: 5u64.into(),
        candidate_id: 2u64.into(),
        last_log_index: 0u64.into(),
        last_log_term: 0u64.into(),
    };
    node.handle_inbound(InboundRaftMessage {
        from: PeerId(2),
        group_id: GroupId(0),
        message: RaftMessage::RequestVote(&req),
    })
    .await
    .expect("healthy vote path must succeed");

    // Positive control: the grant IS durable and the response IS sent.
    assert_eq!(
        storage.durable_hard_state().unwrap().voted_for,
        Some(PeerId(2)),
        "granted vote must be durable"
    );
    let frames = transport.sent_to(2);
    assert_eq!(frames.len(), 1);
    let inbound = decode_message(&frames[0]).expect("decodes");
    let RaftMessage::RequestVoteResp(resp) = inbound.message else {
        panic!("expected RequestVoteResp");
    };
    assert_eq!(resp.vote_granted, 1, "healthy path must grant");
}

// ---------------------------------------------------------------------------
// 2. save_hard_state failing at campaign start → no votes are solicited for
//    a term the node never durably entered.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn campaign_term_persist_failure_solicits_no_votes() {
    let storage = FaultStorage::new();
    let transport = CaptureTransport::new();
    let mut node = new_node(&storage, &transport, 1, &[1, 2, 3]);
    storage.fail_nth(FaultOp::SaveHardState, 1);

    let mut buf = vec![0u8; 64 * 1024];
    let err = node
        .campaign_once(&mut buf)
        .await
        .expect_err("campaign with failing term persist must surface the error");
    assert!(
        matches!(err, RaftError::Storage(_)) && err.is_fatal(),
        "campaign persist failure must classify Fatal, got {err:?}"
    );
    assert!(
        transport.sent().is_empty(),
        "no RequestVote may go on the wire for a term+self-vote that was \
         never durably persisted (double-vote prevention, C-D1)"
    );
}

// ---------------------------------------------------------------------------
// 3. append_entries failing on the LEADER → the entry is never counted
//    toward a commit quorum and never fanned out.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn leader_append_failure_never_counts_toward_commit() {
    let storage = FaultStorage::new();
    let transport = CaptureTransport::new();
    let mut node = new_node(&storage, &transport, 1, &[1, 2, 3]);
    node.become_leader_for_benchmark(Term(1));
    storage.fail_nth(FaultOp::AppendEntries, 1);

    let err = node
        .propose_once(b"payload")
        .await
        .expect_err("propose with failing local append must surface the error");
    assert!(
        matches!(err, RaftError::Storage(_)) && err.is_fatal(),
        "leader append failure must classify Fatal, got {err:?}"
    );
    assert_eq!(
        node.commit_index(),
        LogIndex(0),
        "an entry the leader could not persist must NEVER reach commit \
         (no false commit / no self-ack without durability)"
    );
    assert_eq!(
        node.status().last_log_index,
        LogIndex(0),
        "in-memory log metadata must not advance past the durable log"
    );
    assert!(
        transport.sent().is_empty(),
        "an entry the leader could not persist must not be replicated to peers"
    );
    assert!(
        storage.durable_entries().is_empty(),
        "nothing may be durable after the injected append failure"
    );
}

#[tokio::test]
async fn leader_append_success_commits_on_single_node_control() {
    // Positive control for the fault path above: with healthy storage the
    // same propose commits (single-node cluster = self quorum).
    let storage = FaultStorage::new();
    let transport = CaptureTransport::new();
    let mut node = new_node(&storage, &transport, 1, &[1]);
    node.become_leader_for_benchmark(Term(1));

    let idx = node
        .propose_once(b"payload")
        .await
        .expect("healthy single-node propose must commit");
    assert_eq!(idx, LogIndex(1));
    assert_eq!(node.commit_index(), LogIndex(1));
    let durable = storage.durable_entries();
    assert_eq!(durable.len(), 1);
    assert_eq!(durable[0], (1, 1, b"payload".to_vec()));
}

// ---------------------------------------------------------------------------
// 4. append_entries failing on a FOLLOWER → the follower must NOT ack.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn follower_append_failure_sends_no_ack() {
    let storage = FaultStorage::new();
    let transport = CaptureTransport::new();
    let mut node = new_node(&storage, &transport, 1, &[1, 2, 3]);
    storage.fail_nth(FaultOp::AppendEntries, 1);

    let ae = ae_msg(1, 2, 0, 0, 1);
    let block = contiguous_entry_block(&[(1, 1, b"abc")]);
    let err = handle_append(&mut node, 2, &ae, &block)
        .await
        .expect_err("follower append with failing storage must surface the error");

    assert!(
        matches!(err, RaftError::Storage(_)) && err.is_fatal(),
        "follower append failure must classify Fatal, got {err:?}"
    );
    assert!(
        transport.sent_to(2).is_empty(),
        "the follower must NOT ack an append it could not persist — the \
         leader would count it toward a commit quorum (C-D1)"
    );
    assert!(storage.durable_entries().is_empty());

    // Positive control: healthy storage acks with the persisted match index.
    let storage2 = FaultStorage::new();
    let transport2 = CaptureTransport::new();
    let mut node2 = new_node(&storage2, &transport2, 1, &[1, 2, 3]);
    handle_append(&mut node2, 2, &ae, &block)
        .await
        .expect("healthy follower append must succeed");
    let (success, match_index) = decoded_ack(&transport2, 2);
    assert_eq!((success, match_index), (1, 1), "healthy path acks match=1");
    assert_eq!(storage2.durable_entries().len(), 1);
}

// ---------------------------------------------------------------------------
// 5. Torn write, honest storage (prefix persisted + Err reported):
//    no ack, and a restart exposes exactly the durable prefix.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn torn_write_with_error_no_ack_and_prefix_survives_restart() {
    let storage = FaultStorage::new();
    let transport = CaptureTransport::new();
    let mut node = new_node(&storage, &transport, 1, &[1, 2, 3]);
    storage.torn_append(1, TornReport::Err);

    let ae = ae_msg(1, 2, 0, 0, 3);
    let block = contiguous_entry_block(&[(1, 1, b"e1"), (1, 2, b"e2"), (1, 3, b"e3")]);
    let err = handle_append(&mut node, 2, &ae, &block)
        .await
        .expect_err("torn append reporting Err must surface it");
    assert!(
        matches!(err, RaftError::Storage(_)) && err.is_fatal(),
        "torn write with error must classify Fatal, got {err:?}"
    );
    assert!(
        transport.sent_to(2).is_empty(),
        "the follower must not ack a batch it only partially persisted"
    );
    // The durable state is a clean contiguous PREFIX — never a gap.
    let durable = storage.durable_entries();
    assert_eq!(durable.len(), 1, "only the torn prefix may be durable");
    assert_eq!(durable[0], (1, 1, b"e1".to_vec()));

    // "Restart": a fresh node over the surviving bytes must serve the durable
    // truth. A leader probe at prev=3 must be REJECTED with a hint at the
    // durable tip (1) — the node must not hallucinate the torn-off tail.
    let transport2 = CaptureTransport::new();
    let mut restarted = new_node(&storage, &transport2, 1, &[1, 2, 3]);
    let probe = ae_msg(1, 2, 3, 1, 0);
    handle_append(&mut restarted, 2, &probe, &[])
        .await
        .expect("bare probe must be handled");
    let (success, hint) = decoded_ack(&transport2, 2);
    assert_eq!(
        success, 0,
        "restarted node must reject prev=3 (not durable)"
    );
    assert_eq!(hint, 1, "reject hint must point at the durable tip");
}

// ---------------------------------------------------------------------------
// 6. Torn write, LYING storage (prefix persisted + Ok reported): the node
//    cannot detect the lie in-process (the contract sits on the storage),
//    but a restart converges to the durable prefix and ordinary replication
//    repairs the tail — no silent divergence survives.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn torn_write_reported_ok_restart_recovers_durable_prefix_and_repairs() {
    let storage = FaultStorage::new();
    let transport = CaptureTransport::new();
    let mut node = new_node(&storage, &transport, 1, &[1, 2, 3]);
    storage.torn_append(1, TornReport::Ok);

    let ae = ae_msg(1, 2, 0, 0, 3);
    let block = contiguous_entry_block(&[(1, 1, b"e1"), (1, 2, b"e2"), (1, 3, b"e3")]);
    handle_append(&mut node, 2, &ae, &block)
        .await
        .expect("a lying Ok is indistinguishable from success in-process");

    // Documented trust boundary: the node acks what the storage claimed. The
    // divergence is the storage's contract violation, not a node decision —
    // this pins WHERE the durability responsibility sits (C-D1 trait docs).
    let (success, match_index) = decoded_ack(&transport, 2);
    assert_eq!((success, match_index), (1, 3), "node trusts the storage Ok");
    assert_eq!(
        storage.durable_entries().len(),
        1,
        "but only the prefix is actually durable (the lie)"
    );

    // Crash + restart: the node must come back serving ONLY the durable
    // prefix (reject a probe past it)…
    let transport2 = CaptureTransport::new();
    let mut restarted = new_node(&storage, &transport2, 1, &[1, 2, 3]);
    let probe = ae_msg(1, 2, 3, 1, 0);
    handle_append(&mut restarted, 2, &probe, &[])
        .await
        .expect("probe handled");
    let (success, hint) = decoded_ack(&transport2, 2);
    assert_eq!(
        (success, hint),
        (0, 1),
        "restart must expose the durable prefix, never the phantom tail"
    );

    // …and normal Raft repair (leader walks back, re-ships 2..=3) converges.
    transport2.clear();
    let repair = ae_msg(1, 2, 1, 1, 2);
    let repair_block = contiguous_entry_block(&[(1, 2, b"e2"), (1, 3, b"e3")]);
    handle_append(&mut restarted, 2, &repair, &repair_block)
        .await
        .expect("repair append must succeed");
    let (success, match_index) = decoded_ack(&transport2, 2);
    assert_eq!(
        (success, match_index),
        (1, 3),
        "repair re-acks the full log"
    );
    assert_eq!(
        storage.durable_entries().len(),
        3,
        "after repair the durable log is whole again — no divergence"
    );
}

// ---------------------------------------------------------------------------
// 7. Read fault on the follower's prev-entry consistency check → Fatal,
//    and NEVER an ack computed from unreadable data.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn follower_prev_check_read_fault_is_fatal_and_unacked() {
    let storage = FaultStorage::new();
    let transport = CaptureTransport::new();
    let mut node = new_node(&storage, &transport, 1, &[1, 2, 3]);
    storage.fail_nth(FaultOp::EntryAt, 1);

    // prev=5 is unknown to the (empty) metadata arena, forcing the storage
    // `entry_at` consult — which faults.
    let probe = ae_msg(1, 2, 5, 1, 0);
    let err = handle_append(&mut node, 2, &probe, &[])
        .await
        .expect_err("prev-check read fault must surface");
    assert!(
        matches!(err, RaftError::Storage(_)) && err.is_fatal(),
        "an unreadable log on the consistency-check path must be Fatal \
         (never guess), got {err:?}"
    );
    assert!(
        transport.sent_to(2).is_empty(),
        "no response may be derived from data the storage could not read"
    );

    // Positive control: with a readable (empty) log the same probe gets an
    // HONEST reject, not silence.
    let storage2 = FaultStorage::new();
    let transport2 = CaptureTransport::new();
    let mut node2 = new_node(&storage2, &transport2, 1, &[1, 2, 3]);
    handle_append(&mut node2, 2, &probe, &[])
        .await
        .expect("healthy probe handled");
    let (success, _) = decoded_ack(&transport2, 2);
    assert_eq!(success, 0, "healthy path answers with an honest reject");
}

// ---------------------------------------------------------------------------
// 8. Read fault on the LEADER's per-peer repair path → contained to that
//    peer per the error taxonomy (heartbeats to healthy peers continue,
//    the leader does not die), and nothing fabricated is sent.
// ---------------------------------------------------------------------------

/// Walk peer 2's next_index back from 6 to 3 with three failed-probe acks.
async fn walk_back_peer2(node: &mut RaftNode<FaultStorage, CaptureTransport>) {
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
        .expect("walk-back ack handled");
    }
    assert_eq!(
        node.peer_progress(PeerId(2)).map(|(n, _)| n),
        Some(LogIndex(3)),
        "setup: peer 2 must sit at next_index 3"
    );
}

fn seeded_leader_setup() -> (FaultStorage, CaptureTransport) {
    let storage = FaultStorage::new();
    storage.seed_entries(&[
        (1, 1, b"e1"),
        (1, 2, b"e2"),
        (1, 3, b"e3"),
        (1, 4, b"e4"),
        (1, 5, b"e5"),
    ]);
    (storage, CaptureTransport::new())
}

#[tokio::test]
async fn leader_repair_read_fault_is_contained_to_the_faulty_peer() {
    let (storage, transport) = seeded_leader_setup();
    let mut node = new_node(&storage, &transport, 1, &[1, 2, 3]);
    node.become_leader_for_benchmark(Term(1));
    walk_back_peer2(&mut node).await;

    // The FIRST read_entries of the tick is peer 2's backlog repair read.
    storage.fail_nth(FaultOp::ReadEntries, 1);
    transport.clear();

    node.send_heartbeat_once()
        .await
        .expect("a per-peer repair read fault must not abort the heartbeat tick");
    assert!(
        node.is_leader(),
        "the leader must survive a per-peer read fault"
    );
    assert!(
        transport.sent_to(2).is_empty(),
        "no frame may be sent to the peer whose backlog was unreadable — \
         never fabricate replication data"
    );
    assert_eq!(
        transport.sent_to(3).len(),
        1,
        "the healthy peer must still receive its heartbeat (containment)"
    );
}

// ---------------------------------------------------------------------------
// 9. Short read: the leader ships EXACTLY what the storage returned —
//    entry_count and entries always agree, nothing is fabricated for the
//    range the storage silently withheld.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn leader_short_read_ships_only_entries_actually_read() {
    let (storage, transport) = seeded_leader_setup();
    let mut node = new_node(&storage, &transport, 1, &[1, 2, 3]);
    node.become_leader_for_benchmark(Term(1));
    walk_back_peer2(&mut node).await;

    // Backlog for peer 2 is 3..=5, but the storage will return only ONE entry.
    storage.short_read(1);
    transport.clear();

    node.send_heartbeat_once().await.expect("heartbeat tick");
    let frames = transport.sent_to(2);
    assert_eq!(frames.len(), 1, "one repair frame to the lagging peer");
    let inbound = decode_message(&frames[0]).expect("frame decodes");
    let RaftMessage::AppendEntries(ae, payload) = inbound.message else {
        panic!("expected an AppendEntries repair frame");
    };
    assert_eq!(
        ae.entry_count.get(),
        1,
        "entry_count must reflect what was READ (1), not what was expected (3)"
    );
    let entries: Vec<(u64, u64, Vec<u8>)> =
        AppendEntriesEntryIter::new(payload, ae.entry_count.get() as usize)
            .map(|e| (e.term.0, e.index.0, e.payload.0.to_vec()))
            .collect();
    assert_eq!(
        entries,
        vec![(1, 3, b"e3".to_vec())],
        "the frame carries exactly the short-read prefix — no fabricated tail"
    );
    assert_eq!(ae.prev_log_index.get(), 2, "prev metadata stays consistent");
}

// ---------------------------------------------------------------------------
// 10. C9 — ack-after-persist, POSITIVE direction: a follower's ack implies
//     the acked entries survive a crash-restart over its durable bytes, so a
//     leader commit counted on that ack was safe. C1 pinned the NEGATIVE
//     ("no ack without persist", tests 4-5 above); this pins "ack ⇒ durable
//     across restart" — the promise the leader's commit math silently
//     assumes (`src/traits/storage.rs:19-22`).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn follower_ack_implies_entries_survive_crash_restart() {
    // Follower 1 persists entries 1..=2 from leader 2 and ACKs match=2.
    let follower_storage = FaultStorage::new();
    let follower_net = CaptureTransport::new();
    let mut follower = new_node(&follower_storage, &follower_net, 1, &[1, 2, 3]);

    let ae = ae_msg(1, 2, 0, 0, 2);
    let block = contiguous_entry_block(&[(1, 1, b"cmd1"), (1, 2, b"cmd2")]);
    handle_append(&mut follower, 2, &ae, &block)
        .await
        .expect("healthy follower append must succeed");
    let (success, match_index) = decoded_ack(&follower_net, 2);
    assert_eq!(
        (success, match_index),
        (1, 2),
        "follower acks the full batch"
    );

    // The ack promise: by the time the ack exists, the entries are durable.
    assert_eq!(follower_storage.durable_entries().len(), 2);

    // Bonus — the LEADER's commit decision consumes exactly this ack. Drive
    // the REAL propose→quorum path: a leader proposes the same two entries
    // and its transport hands back the follower's ACTUAL wire ack frame.
    // Quorum of 3 = leader self + this follower → commit_index = 2.
    let leader_storage = FaultStorage::new();
    let leader_net = QueueTransport::default();
    let ack_frames = follower_net.sent_to(2);
    leader_net.push_inbound(ack_frames[0].clone());
    let mut leader = RaftNode::new(
        make_config(2, &[1, 2, 3]),
        leader_storage.clone(),
        leader_net.clone(),
    )
    .expect("leader boots");
    leader.become_leader_for_benchmark(Term(1));
    assert_eq!(leader.commit_index(), LogIndex(0), "nothing committed yet");
    leader
        .propose_batch_once(&[b"cmd1", b"cmd2"])
        .await
        .expect("propose must reach quorum on the follower's ack");
    assert_eq!(
        leader.commit_index(),
        LogIndex(2),
        "the leader commits ON the follower's ack — the decision C9 verifies"
    );

    // CRASH the follower (drop the node) and restart a fresh node over the
    // SAME surviving durable bytes (C1's FaultStorage clone-restart pattern).
    drop(follower);
    let restart_net = CaptureTransport::new();
    let mut restarted = new_node(&follower_storage, &restart_net, 1, &[1, 2, 3]);

    // Raw storage truth after restart: the acked entries are still there.
    let (last_idx, last_term) = follower_storage
        .last_log_position()
        .expect("last_log_position readable after restart");
    assert_eq!(
        (last_idx, last_term),
        (LogIndex(2), Term(1)),
        "the acked tip must survive the crash"
    );
    let mut buf = vec![0u8; 64];
    let entry = follower_storage
        .entry_at(LogIndex(2), &mut buf)
        .expect("entry_at readable after restart")
        .expect("the acked entry 2 must exist after restart");
    assert_eq!(
        (entry.term, entry.payload.0),
        (Term(1), &b"cmd2"[..]),
        "the acked entry must be intact (term + payload)"
    );

    // And the restarted NODE serves the acked log: a leader probe at prev=2
    // (the tip the leader committed at) is ACCEPTED — so the pre-crash
    // commit decision was justified by durable state, not by luck.
    let probe = ae_msg(1, 2, 2, 1, 0);
    handle_append(&mut restarted, 2, &probe, &[])
        .await
        .expect("post-restart probe handled");
    let (success, match_index) = decoded_ack(&restart_net, 2);
    assert_eq!(
        (success, match_index),
        (1, 2),
        "restarted follower confirms the acked entries — ack ⇒ durable"
    );
}
