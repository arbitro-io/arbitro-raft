//! A7 regression pins — C1: Election Safety under overlapping campaigns
//! (audit P0-3, AUDIT_REPORT Axis-6 scenario C1).
//!
//! The hole these tests pin closed: a candidate that stepped down to a higher
//! term MID vote-collection kept counting stale old-term grants and still set
//! `role = Leader` (`election.rs`, the role/term bail in `collect_votes` plus
//! the pre-crown defense-in-depth check in `campaign_once`), and a voter's
//! grant had to be DURABLE before the response frame left the node
//! (`handle_request_vote` persists `voted_for` via `save_hard_state` before
//! `send_message`) so that a crash-restart between granting and the winner's
//! first heartbeat can never yield a second same-term vote — two leaders in
//! one term.
//!
//! // A7 degraded: the full-fidelity C1 scenario needs F5 (crash-restart at
//! // an exact instruction boundary inside the grant window + partition
//! // control at 10k seeds). Today's in-memory harness approximates it with:
//! //   (1) a scripted campaign that forces the exact "step-down between two
//! //       grants" interleaving deterministically,
//! //   (2) a storage-fault pin proving the grant is durable BEFORE the wire
//! //       says yes (the crash-window equivalence),
//! //   (3) a restart pin proving a persisted vote blocks a second candidate
//! //       in the same term, and
//! //   (4) a live 5-node cluster with node restarts sampled continuously
//! //       for the ≤1-leader-per-term invariant.

use std::sync::atomic::Ordering;
use std::time::Duration;

use arbitro_raft::{
    AppendEntries, GroupId, InboundRaftMessage, PeerId, RaftMessage, RaftNode, RequestVote,
    RequestVoteResp, Role, Term,
};

#[path = "support/a7_harness.rs"]
mod a7_harness;
use a7_harness::*;

// ---------------------------------------------------------------------------
// Wire helpers.
// ---------------------------------------------------------------------------

fn vote_grant_frame(from: u64, term: u64) -> Vec<u8> {
    let resp = RequestVoteResp {
        term: term.into(),
        vote_granted: 1,
        _pad: [0; 7],
    };
    arbitro_raft::encode_message_to_bytes(PeerId(from), &RaftMessage::RequestVoteResp(&resp))
        .expect("encode grant")
        .to_vec()
}

fn heartbeat_frame(from: u64, term: u64) -> Vec<u8> {
    let ae = AppendEntries {
        term: term.into(),
        leader_id: from.into(),
        prev_log_index: 0u64.into(),
        prev_log_term: 0u64.into(),
        leader_commit: 0u64.into(),
        entry_count: 0u32.into(),
        _pad: 0u32.into(),
    };
    arbitro_raft::encode_message_to_bytes(PeerId(from), &RaftMessage::AppendEntries(&ae, &[]))
        .expect("encode heartbeat")
        .to_vec()
}

fn request_vote(term: u64, candidate: u64) -> RequestVote {
    RequestVote {
        term: term.into(),
        candidate_id: candidate.into(),
        last_log_index: 0u64.into(),
        last_log_term: 0u64.into(),
    }
}

/// Decode every captured outbound frame from `sent` and return the
/// `(vote_granted, term)` of each RequestVoteResp found.
fn decoded_vote_responses(frames: &[Vec<u8>]) -> Vec<(bool, u64)> {
    frames
        .iter()
        .filter_map(|f| {
            let inbound = arbitro_raft::decode_message(f).ok()?;
            let resp = inbound.as_request_vote_resp()?;
            Some((resp.vote_granted != 0, resp.term.get()))
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Pin 1 — a candidate that steps down MID vote-collection must abandon the
// campaign; stale grants for the abandoned term must never crown it.
//
// Discriminating reversion: remove the role/term bail after the
// requests-processing phase in `collect_votes` AND the defense-in-depth
// check before `role = Leader` in `campaign_once` (P0-3, election.rs).
// Then the sequence below — one insufficient grant, a higher-term
// AppendEntries (overlapping campaign already won elsewhere), one more
// stale grant — reaches quorum(5) = 3 with {self, p2, p4} and sets
// `role = Leader` while `current_term` is 5: a leader of a term it never
// won one vote for. The assertions on `is_leader`/role and on the stale
// grant remaining UNCONSUMED both fail under that reversion.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn c1_stale_grants_after_mid_campaign_step_down_never_crown_leader() {
    // The ScriptedTransport serves exactly ONE frame per drain batch, forcing
    // this deterministic interleaving inside `collect_votes`:
    //   batch 1: grant from p2 at term 1  -> granted = {1,2}, below quorum(5)=3
    //   batch 2: AppendEntries from p3 at term 5 -> step-down mid-collection
    //   batch 3: stale grant from p4 at term 1 -> must never be read
    let script = vec![
        vote_grant_frame(2, 1),
        heartbeat_frame(3, 5),
        vote_grant_frame(4, 1),
    ];
    let (transport, queue) = ScriptedTransport::new(script);
    let storage = TestStorage::default();
    let mut node =
        RaftNode::new(make_config(1, &[1, 2, 3, 4, 5]), storage.clone(), transport).unwrap();

    let mut inbound_buf = vec![0u8; 64 * 1024];
    let elected = node
        .campaign_once(&mut inbound_buf)
        .await
        .expect("campaign must not error");

    assert!(
        !elected && !node.is_leader(),
        "THE PIN (P0-3): a campaign that stepped down mid-collection must \
         never claim leadership"
    );
    assert_eq!(node.role(), Role::Follower, "step-down must leave Follower");
    assert_eq!(
        node.current_term(),
        Term(5),
        "the higher term from the overlapping campaign must be adopted"
    );
    // The bail must happen AT the step-down — before the stale grant is even
    // read. If the campaign kept collecting, the queue would be empty.
    assert_eq!(
        queue.lock().unwrap().len(),
        1,
        "campaign must abandon at the step-down, leaving the stale grant \
         unconsumed"
    );
    // The persisted state a restart would reload agrees.
    let hs = storage.persisted_hard_state();
    assert_eq!(hs.current_term, Term(5));
    assert_eq!(hs.voted_for, None, "step-down must clear the persisted vote");

    // Re-deliver the stale term-1 grant explicitly (a delayed frame arriving
    // after the campaign) — it must be inert.
    let stale = RequestVoteResp {
        term: 1u64.into(),
        vote_granted: 1,
        _pad: [0; 7],
    };
    node.handle_inbound(InboundRaftMessage {
        from: PeerId(4),
        group_id: GroupId(0),
        message: RaftMessage::RequestVoteResp(&stale),
    })
    .await
    .expect("stale grant must be handled without error");
    assert!(
        !node.is_leader() && node.current_term() == Term(5),
        "a re-delivered stale grant outside a campaign must change nothing"
    );
}

// ---------------------------------------------------------------------------
// Pin 2 — the vote grant must be DURABLE before the response frame is sent.
//
// This is the in-memory equivalence of "voter crashes between granting and
// the winner's first heartbeat": if persisting `voted_for` fails, the grant
// frame must NOT leave the node — otherwise the candidate counts a vote the
// voter will not remember after restart, and a second candidate can collect
// the same voter in the same term (two leaders / Election Safety violation).
//
// Discriminating reversion: reorder `handle_request_vote` to send the
// response before `save_hard_state` (or drop the `?` on the save). The
// injected save failure would then let a GRANTED RequestVoteResp reach the
// wire while the restarted voter reloads `voted_for = None` — the
// "no granted frame emitted" assertion below fails.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn c1_vote_grant_is_durable_before_the_response_is_sent() {
    let storage = TestStorage::default();
    // Reach term 3 with `voted_for = None` first (persist working) via a
    // heartbeat from p3 claiming leadership of term 3.
    storage.seed_hard_state(3, None);

    let (transport, sent) = CaptureTransport::new();
    let mut node =
        RaftNode::new(make_config(2, &[1, 2, 3, 4, 5]), storage.clone(), transport).unwrap();

    // Now the durable medium "fails at the grant instant" (the crash window).
    storage.fail_save_hard_state.store(true, Ordering::SeqCst);

    let req = request_vote(3, 1);
    let res = node
        .handle_inbound(InboundRaftMessage {
            from: PeerId(1),
            group_id: GroupId(0),
            message: RaftMessage::RequestVote(&req),
        })
        .await;
    assert!(
        res.is_err(),
        "a grant whose persist failed must surface the storage error, got {res:?}"
    );

    // THE PIN: no granted RequestVoteResp may have reached the wire.
    let frames = take_frames_for(&sent, 1);
    let grants: Vec<_> = decoded_vote_responses(&frames)
        .into_iter()
        .filter(|(granted, _)| *granted)
        .collect();
    assert!(
        grants.is_empty(),
        "vote grant reached the wire before it was durable: {grants:?}"
    );

    // A restart reloads the persisted truth: no vote was cast, so the term-3
    // vote is still free — safe ONLY because no grant was ever sent.
    storage.fail_save_hard_state.store(false, Ordering::SeqCst);
    let hs = storage.persisted_hard_state();
    assert_eq!(hs.current_term, Term(3));
    assert_eq!(hs.voted_for, None, "unpersisted grant must not survive restart");
}

// ---------------------------------------------------------------------------
// Pin 3 — a persisted grant SURVIVES a crash-restart and blocks a second
// candidate in the same term (the "restart between grant and first
// heartbeat" window, degraded to a state-reload restart).
//
// Discriminating reversion: stop persisting `voted_for` (or stop reloading
// it in `RaftNode::new`). The restarted voter would grant BOTH candidates
// at term 3 — with the quorum-overlap argument broken, two leaders in one
// term become possible. The `granted == false` assertion for candidate 3
// fails under that reversion.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn c1_granted_vote_survives_restart_and_blocks_second_candidate() {
    let storage = TestStorage::default();

    // Phase 1 — voter 2 grants candidate 1 at term 3.
    {
        let (transport, sent) = CaptureTransport::new();
        let mut node =
            RaftNode::new(make_config(2, &[1, 2, 3, 4, 5]), storage.clone(), transport).unwrap();
        let req = request_vote(3, 1);
        node.handle_inbound(InboundRaftMessage {
            from: PeerId(1),
            group_id: GroupId(0),
            message: RaftMessage::RequestVote(&req),
        })
        .await
        .expect("vote request handled");
        let responses = decoded_vote_responses(&take_frames_for(&sent, 1));
        assert_eq!(
            responses,
            vec![(true, 3)],
            "voter must grant the first candidate of term 3"
        );
        // The grant is already durable — BEFORE any restart.
        let hs = storage.persisted_hard_state();
        assert_eq!(hs.current_term, Term(3));
        assert_eq!(hs.voted_for, Some(PeerId(1)));
    } // node dropped here == crash inside the grant window

    // Phase 2 — restart from the same durable state; candidate 3 asks for
    // the SAME term 3.
    let (transport, sent) = CaptureTransport::new();
    let mut node =
        RaftNode::new(make_config(2, &[1, 2, 3, 4, 5]), storage.clone(), transport).unwrap();
    assert_eq!(node.current_term(), Term(3), "restart must reload the term");

    let req_b = request_vote(3, 3);
    node.handle_inbound(InboundRaftMessage {
        from: PeerId(3),
        group_id: GroupId(0),
        message: RaftMessage::RequestVote(&req_b),
    })
    .await
    .expect("second vote request handled");
    let responses = decoded_vote_responses(&take_frames_for(&sent, 3));
    assert_eq!(
        responses,
        vec![(false, 3)],
        "THE PIN: the restarted voter must refuse a second candidate in the \
         same term — its persisted vote already belongs to candidate 1"
    );

    // Re-delivered request from the ORIGINAL candidate stays granted
    // (idempotent — same voter, same candidate, no double-vote hazard).
    let req_a = request_vote(3, 1);
    node.handle_inbound(InboundRaftMessage {
        from: PeerId(1),
        group_id: GroupId(0),
        message: RaftMessage::RequestVote(&req_a),
    })
    .await
    .expect("re-delivered vote request handled");
    let responses = decoded_vote_responses(&take_frames_for(&sent, 1));
    assert_eq!(
        responses,
        vec![(true, 3)],
        "a re-delivered request from the already-voted-for candidate is granted"
    );
    assert_eq!(storage.persisted_hard_state().voted_for, Some(PeerId(1)));
}

// ---------------------------------------------------------------------------
// Pin 4 — live 5-node cluster: overlapping campaigns + voter/leader restarts,
// with the Election Safety invariant (≤ 1 leader per term) asserted at every
// observed point.
//
// // A7 degraded: without F5 the restart instant cannot be pinned to the
// // exact grant-to-first-heartbeat window; restarts here land at arbitrary
// // points around elections (including that window across repeated runs).
// // The invariant asserted is the full Election Safety property, which must
// // hold at EVERY observation regardless of timing — so the test is
// // timing-robust while still exercising restart-during-election paths.
//
// Discriminating reversion: any Election Safety break that manifests under
// concurrent campaigns with restarts (e.g. reverting vote persistence or the
// P0-3 bail) allows two nodes to report `is_leader` for the same term across
// samples — the per-term leader-set assertion fails.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn c1_live_overlapping_campaigns_with_restarts_one_leader_per_term() {
    let hub = std::sync::Arc::new(NetworkHub::new());
    let ids = [1u64, 2, 3, 4, 5];

    let mut rafts: Vec<SharedRaft> = Vec::new();
    let mut storages: Vec<TestStorage> = Vec::new();
    let mut guard = AbortOnDrop { handles: vec![] };

    for &id in &ids {
        let storage = TestStorage::default();
        let raft = boot_node_with_storage(hub.clone(), id, &ids, storage.clone());
        guard.handles.push(spawn_driver(raft.clone()));
        rafts.push(raft);
        storages.push(storage);
    }

    // term -> set of node ids ever observed leading that term.
    let mut leaders_by_term: std::collections::HashMap<u64, std::collections::HashSet<u64>> =
        std::collections::HashMap::new();
    let mut observed_any_leader = false;

    async fn sample(
        rafts: &[SharedRaft],
        map: &mut std::collections::HashMap<u64, std::collections::HashSet<u64>>,
        any: &mut bool,
    ) {
        for r in rafts {
            let g = r.lock().await;
            let st = g.status();
            if st.is_leader {
                *any = true;
                map.entry(st.term.0).or_default().insert(st.node_id.0);
            }
        }
    }

    // Warm-up: let the simultaneous-startup election storm settle a little
    // while already sampling (overlapping campaigns happen right here).
    for _ in 0..10 {
        sample(&rafts, &mut leaders_by_term, &mut observed_any_leader).await;
        tokio::time::sleep(Duration::from_millis(60)).await;
    }

    // Two restart storms: first a non-leader voter (its persisted vote must
    // survive), then whichever node currently leads (forcing a re-election
    // while the deposed leader's storage still holds its old term/vote).
    for round in 0..2 {
        // Pick the restart victim under the invariant sample.
        let mut leader_idx: Option<usize> = None;
        for (i, r) in rafts.iter().enumerate() {
            if r.lock().await.status().is_leader {
                leader_idx = Some(i);
            }
        }
        let victim = match (round, leader_idx) {
            // Round 0: restart a follower (a voter that may just have granted).
            (0, Some(l)) => (l + 1) % rafts.len(),
            (0, None) => 0,
            // Round 1: restart the leader itself.
            (_, Some(l)) => l,
            (_, None) => 1,
        };

        // Crash-restart: abort the driver, reboot over the same storage.
        guard.handles[victim].abort();
        let id = ids[victim];
        let raft = boot_node_with_storage(hub.clone(), id, &ids, storages[victim].clone());
        guard.handles[victim] = spawn_driver(raft.clone());
        rafts[victim] = raft;

        for _ in 0..10 {
            sample(&rafts, &mut leaders_by_term, &mut observed_any_leader).await;
            tokio::time::sleep(Duration::from_millis(60)).await;
        }
    }

    // Liveness (sanity that the scenario was exercised): a leader must
    // re-emerge after the restart storms within a generous budget.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        sample(&rafts, &mut leaders_by_term, &mut observed_any_leader).await;
        let mut current_leader = false;
        for r in &rafts {
            if r.lock().await.status().is_leader {
                current_leader = true;
            }
        }
        if current_leader {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no leader re-emerged after restarts within budget"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(observed_any_leader, "scenario never observed any leader");

    // THE PIN — Election Safety at every observed point: no term may ever
    // have been led by two different nodes.
    for (term, leaders) in &leaders_by_term {
        assert!(
            leaders.len() <= 1,
            "ELECTION SAFETY VIOLATION: term {} had multiple leaders {:?} \
             (full map: {:?})",
            term,
            leaders,
            leaders_by_term,
        );
    }
}
