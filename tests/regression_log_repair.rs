//! A7 regression pins — C5: divergent-follower repair
//! (audit P0-7, AUDIT_REPORT Axis-6 scenario C5).
//!
//! The hole these tests pin closed: a follower with a LONGER divergent tail
//! (uncommitted entries from a deposed leader's term) rejected the current
//! leader's probe with `match_index = its own last log` — an index PAST the
//! leader's log. The leader's walk-back then jumped `next_index` FORWARD past
//! its own tail, `term_at` blew up with `CorruptLog`, and the leader died:
//! the divergent follower was never repaired. Two coordinated fixes are
//! pinned here:
//!   * follower side (`handler.rs`): the reject hint is
//!     `min(own_last, prev_log_index - 1)` — it points at the conflict,
//!     never at the follower's own longer stale tail;
//!   * leader side (`handler.rs`): `next_index` is clamped to
//!     `leader_last + 1` on BOTH the success and reject paths, so even a
//!     hostile/buggy hint cannot push replication past the leader's log.
//!
//! Scenario (per the audit spec): common prefix 1..=3 at term 1; the leader
//! (term 3) extends it with 4..=5 at term 3 and has committed through 5; the
//! follower instead carries 4..=10 at term 2 — a LONGER uncommitted tail from
//! the deposed term-2 leader. After repair the follower's log must be
//! byte-identical to the leader's committed prefix, in a bounded number of
//! AppendEntries rounds.
//!
//! // A7 degraded: F5 would run this under SimNet (drop/reorder/dup) and
//! // crash-restart. Today the exchange is ferried manually — every frame the
//! // leader emits is delivered to the follower and vice versa, one heartbeat
//! // tick per round — which makes the round count exact and deterministic.

use arbitro_raft::{
    AppendEntries, AppendEntriesResp, GroupId, InboundRaftMessage, LogIndex, PeerId, RaftMessage,
    RaftNode, RaftStorage, Term,
};

#[path = "support/a7_harness.rs"]
mod a7_harness;
use a7_harness::*;

/// Build the divergence fixture from the audit spec. Returns
/// `(leader, leader_storage, leader_sent, follower, follower_storage,
/// follower_sent)`. Leader = node 1, follower = node 2, cluster {1, 2}.
#[allow(clippy::type_complexity)]
fn build_divergent_pair() -> (
    RaftNode<TestStorage, CaptureTransport>,
    TestStorage,
    std::sync::Arc<std::sync::Mutex<Vec<(u64, Vec<u8>)>>>,
    RaftNode<TestStorage, CaptureTransport>,
    TestStorage,
    std::sync::Arc<std::sync::Mutex<Vec<(u64, Vec<u8>)>>>,
) {
    // Leader log: 1..=3 @ term 1 (common prefix), 4..=5 @ term 3. Payloads
    // are distinct per (index, term) so "byte-identical" is meaningful.
    let leader_storage = TestStorage::default();
    for i in 1..=3u64 {
        leader_storage.seed_entry(i, 1, &[b'p', i as u8, 1]);
    }
    for i in 4..=5u64 {
        leader_storage.seed_entry(i, 3, &[b'l', i as u8, 3]);
    }
    leader_storage.seed_hard_state(3, Some(1));

    // Follower log: same common prefix, then a LONGER divergent tail
    // 4..=10 @ term 2 from the deposed term-2 leader.
    let follower_storage = TestStorage::default();
    for i in 1..=3u64 {
        follower_storage.seed_entry(i, 1, &[b'p', i as u8, 1]);
    }
    for i in 4..=10u64 {
        follower_storage.seed_entry(i, 2, &[b'f', i as u8, 2]);
    }
    follower_storage.seed_hard_state(2, None);

    let (leader_tx, leader_sent) = CaptureTransport::new();
    let mut leader =
        RaftNode::new(make_config(1, &[1, 2]), leader_storage.clone(), leader_tx).unwrap();
    leader.become_leader_for_benchmark(Term(3));
    // The leader has committed its whole log (entries 4..=5 are of its own
    // term; in the live cluster the quorum ack already happened before the
    // follower diverged back in).
    leader.set_commit_index(LogIndex(5));

    let (follower_tx, follower_sent) = CaptureTransport::new();
    let follower =
        RaftNode::new(make_config(2, &[1, 2]), follower_storage.clone(), follower_tx).unwrap();

    (
        leader,
        leader_storage,
        leader_sent,
        follower,
        follower_storage,
        follower_sent,
    )
}

// ---------------------------------------------------------------------------
// Pin 1 — the divergent follower is repaired to the leader's committed
// prefix, byte-identical, in a bounded number of AppendEntries rounds.
//
// Expected exchange (each round = one leader heartbeat tick + full ferry):
//   round 1: probe prev=(5,t3)   -> reject hint 4  (next 6 -> 5)
//   round 2: ship [5], prev=(4,t3)-> reject hint 3  (next 5 -> 4)
//   round 3: ship [4,5], prev=(3,t1) -> prefix matches: truncate 4..=10,
//            append 4..=5 @ t3, adopt leader_commit=5, ack match=5.
// ROUND_BOUND is deliberately loose (8) but finite.
//
// Discriminating reversions:
//   * follower hint reverted to `match_index = own last (10)`: with the
//     leader clamp still present, next_index pins at leader_last+1 = 6 and
//     every round repeats the same rejected probe — the follower NEVER
//     converges and the bound trips;
//   * leader clamp ALSO reverted: next_index walks to 11, the repair read
//     for prev=10 fails (`term_at` past the leader's log -> CorruptLog);
//     `send_heartbeat_once`'s per-peer containment skips the peer forever —
//     again no convergence, and with the pre-P0-7 propagation the call
//     errors outright. Either way this test fails.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn c5_longer_divergent_follower_repaired_byte_identical_in_bounded_rounds() {
    const ROUND_BOUND: usize = 8;
    let (mut leader, leader_storage, leader_sent, mut follower, follower_storage, follower_sent) =
        build_divergent_pair();

    // Scenario integrity: the follower's tail really is longer and divergent.
    assert_eq!(
        follower_storage.last_log_position().unwrap(),
        (LogIndex(10), Term(2)),
        "fixture: follower must start with the longer term-2 tail"
    );

    let mut rounds_used = None;
    for round in 1..=ROUND_BOUND {
        // One leader heartbeat tick. Under the P0-7 reversion this is where
        // the CorruptLog death occurred — it must never error.
        leader
            .send_heartbeat_once()
            .await
            .expect("leader heartbeat/repair tick must never error (P0-7)");

        // Ferry leader -> follower.
        for frame in take_frames_for(&leader_sent, 2) {
            let inbound = arbitro_raft::decode_message(&frame).expect("decode leader frame");
            follower
                .handle_inbound(inbound)
                .await
                .expect("follower handles repair frame");
        }
        // Ferry follower -> leader (acks / reject hints).
        for frame in take_frames_for(&follower_sent, 1) {
            let inbound = arbitro_raft::decode_message(&frame).expect("decode follower frame");
            leader
                .handle_inbound(inbound)
                .await
                .expect("leader handles follower response");
        }

        // Leader-side clamp invariant, every round: next_index for the
        // follower must never exceed leader_last + 1 (= 6).
        let (next, _) = leader.peer_progress(PeerId(2)).expect("progress for peer 2");
        assert!(
            next <= LogIndex(6),
            "round {round}: leader next_index {} walked past its own log \
             (leader_last + 1 = 6) — the P0-7 forward jump",
            next.0,
        );

        if follower_storage.last_log_position().unwrap() == (LogIndex(5), Term(3)) {
            rounds_used = Some(round);
            break;
        }
    }

    let rounds = rounds_used.unwrap_or_else(|| {
        panic!(
            "follower not repaired within {ROUND_BOUND} rounds — walk-back \
             never converged (follower last_log = {:?})",
            follower_storage.last_log_position().unwrap(),
        )
    });
    assert!(
        rounds <= ROUND_BOUND,
        "repair must converge in a bounded number of rounds"
    );

    // THE PIN: byte-identical committed prefix. The follower's entire log
    // must now equal the leader's committed prefix (= its whole log, 1..=5):
    // same indexes, same terms, same payload bytes; the term-2 tail is gone.
    assert_eq!(
        follower_storage.log_triples(),
        leader_storage.log_triples(),
        "follower log must be byte-identical to the leader's committed prefix"
    );

    // The follower also adopted the leader's commit index for the repaired
    // prefix, and the leader learned the follower's match.
    assert_eq!(follower.commit_index(), LogIndex(5));
    let (next, matched) = leader.peer_progress(PeerId(2)).expect("progress for peer 2");
    assert_eq!(matched, LogIndex(5), "leader must see the follower caught up");
    assert_eq!(next, LogIndex(6));
    assert!(leader.is_leader(), "repair must not cost leadership");
    assert_eq!(leader.current_term(), Term(3));

    // Determinism bonus (documents the expected exchange): the repair takes
    // exactly 3 rounds today. If a legitimate protocol improvement (e.g.
    // smarter conflict hints) changes this, update the constant — the safety
    // bound above is ROUND_BOUND, not this exact count.
    assert_eq!(rounds, 3, "expected the 3-round walk-back documented above");
}

// ---------------------------------------------------------------------------
// Pin 2 — leader-side clamp in isolation: a forged/buggy reject hint
// pointing PAST the leader's log (the exact P0-7 crash input: the follower's
// longer stale tail, 10 > leader_last 5) must clamp `next_index` at
// leader_last + 1 and must not kill the leader's heartbeat path.
//
// Discriminating reversion: remove the `min(next_index_cap)` clamp in
// `handle_append_entries_response` — next_index becomes 11, the assertion
// fails, and the follow-up heartbeat exercises the forward-jump read.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn c5_forged_reject_hint_past_leader_log_is_clamped() {
    let (mut leader, _ls, leader_sent, _f, _fs, _fsent) = build_divergent_pair();

    // Forged reject: term matches, success = 0, match_index = 10 (the
    // follower's longer stale tail — past the leader's last log of 5).
    let forged = AppendEntriesResp {
        term: 3u64.into(),
        match_index: 10u64.into(),
        success: 0,
        _pad: [0; 7],
    };
    for _ in 0..3 {
        leader
            .handle_inbound(InboundRaftMessage {
                from: PeerId(2),
                group_id: GroupId(0),
                message: RaftMessage::AppendEntriesResp(&forged),
            })
            .await
            .expect("forged reject must be survivable");
        let (next, _) = leader.peer_progress(PeerId(2)).expect("progress for peer 2");
        assert!(
            next <= LogIndex(6),
            "THE PIN (P0-7): next_index {} advanced past leader_last + 1 on a \
             forged tail hint",
            next.0,
        );
        // The heartbeat path must stay alive (pre-fix: CorruptLog death here).
        leader
            .send_heartbeat_once()
            .await
            .expect("heartbeat after forged hint must not kill the leader");
        take_frames_for(&leader_sent, 2); // discard; this pin is state-only
    }
    assert!(leader.is_leader());
}

// ---------------------------------------------------------------------------
// Pin 3 — follower-side hint rule in isolation: a follower with a LONGER
// divergent tail must answer a conflicting probe with
// `min(own_last, prev - 1)` — the conflict point — never with its own tail.
//
// Discriminating reversion: put the reject hint back to
// `match_index = cached_last_log` (the pre-fix code). The response would
// carry 10 instead of 4 and the assertion fails — and against a clampless
// leader that 10 is exactly the forward jump that killed it.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn c5_divergent_follower_reject_hint_points_at_conflict_not_own_tail() {
    let (_l, _ls, _lsent, mut follower, _fs, follower_sent) = build_divergent_pair();

    // The leader's real probe: prev = (5, term 3), no entries.
    let probe = AppendEntries {
        term: 3u64.into(),
        leader_id: 1u64.into(),
        prev_log_index: 5u64.into(),
        prev_log_term: 3u64.into(),
        leader_commit: 5u64.into(),
        entry_count: 0u32.into(),
        _pad: 0u32.into(),
    };
    follower
        .handle_inbound(InboundRaftMessage {
            from: PeerId(1),
            group_id: GroupId(0),
            message: RaftMessage::AppendEntries(&probe, &[]),
        })
        .await
        .expect("probe handled");

    let frames = take_frames_for(&follower_sent, 1);
    let mut saw_reject = false;
    for frame in &frames {
        let inbound = arbitro_raft::decode_message(frame).expect("decode follower response");
        if let Some(resp) = inbound.as_append_entries_resp() {
            saw_reject = true;
            assert_eq!(resp.success, 0, "conflicting probe must be rejected");
            assert_eq!(
                resp.match_index.get(),
                4,
                "THE PIN (P0-7): reject hint must be min(own_last=10, prev-1=4) \
                 — the conflict point, not the follower's own longer tail"
            );
        }
    }
    assert!(saw_reject, "follower must have answered the probe");
}
