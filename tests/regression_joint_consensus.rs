//! A7 regression pins — C2/C3: joint-consensus dual-quorum safety
//! (audit P0-4, AUDIT_REPORT Axis-6 scenario C2/C3).
//!
//! The hole these tests pin closed: during a 3→5 grow the commit rule was a
//! single majority of the UNION, so an entry acked entirely inside the new
//! side (`{leader, 4, 5}` = majority of C_new AND of the union, but NOT of
//! C_old) could commit with zero old-side acks — breaking the §4.3 safety
//! hand-off (a future leader elected inside C_old need not have the entry).
//! The fix routes every commit decision through
//! `index_meets_commit_quorum`/`try_advance_commit_index`'s dual rule
//! (majority-of-old AND majority-of-new, `shared.rs`), with append-time
//! activation on the leader (`propose_config_change`, `mod.rs`) so the Joint
//! entry's OWN commit is already decided under the dual rule. The companion
//! hazard (C3): a leader crash/step-down mid-joint must not roll back the
//! active joint config (splitting the cluster's view of membership) nor lose
//! entries already committed under the dual rule.
//!
//! The acked-by-new-side-only pattern is constructed EXACTLY, with no timing
//! dependence, by injecting `AppendEntriesResp` frames from chosen peers and
//! pumping `run_once` (the same ack-control the A8/A10 pins use). The
//! complementary old-majority-only pattern is already pinned by
//! `membership_change::test_joint_entry_commit_requires_dual_quorum`.
//!
//! // A7 degraded: F5 would crash-KILL the leader process mid-joint and
//! // verify the surviving majority's recovery election; without process
//! // isolation the harness uses a higher-term step-down as the in-memory
//! // crash equivalent and asserts the survivor-visible state (log, commit
//! // index, active joint config) — the state a real recovery would read.

use arbitro_raft::{
    AppendEntriesResp, ArbitroRaft, ConfigChangeEntry, ConfigChangePhase, GroupId,
    InboundRaftMessage, LogIndex, NoopStateMachine, PeerId, RaftError, RaftMessage, RaftNode,
    Term,
};

#[path = "support/a7_harness.rs"]
mod a7_harness;
use a7_harness::*;

type Raft = ArbitroRaft<TestStorage, InjectTransport, NoopStateMachine>;

const OLD: [u64; 3] = [1, 2, 3];
const NEW: [u64; 5] = [1, 2, 3, 4, 5];

fn ack_frame(from: u64, term: u64, match_index: u64) -> Vec<u8> {
    let resp = AppendEntriesResp {
        term: term.into(),
        match_index: match_index.into(),
        success: 1,
        _pad: [0; 7],
    };
    arbitro_raft::encode_message_to_bytes(PeerId(from), &RaftMessage::AppendEntriesResp(&resp))
        .expect("encode ack")
        .to_vec()
}

fn higher_term_frame(from: u64, term: u64) -> Vec<u8> {
    let resp = AppendEntriesResp {
        term: term.into(),
        match_index: 0u64.into(),
        success: 0,
        _pad: [0; 7],
    };
    arbitro_raft::encode_message_to_bytes(PeerId(from), &RaftMessage::AppendEntriesResp(&resp))
        .expect("encode higher-term frame")
        .to_vec()
}

/// Boot a leader of the OLD 3-node config at term 5 and drive it into the
/// joint phase of a 3→5 grow. The propose times out with `NoQuorum` (no peer
/// acks yet), leaving the Joint entry APPENDED and the joint config ACTIVE
/// (§4.1 append-time rule) — the mid-joint state every pin below starts from.
/// Returns `(raft, inject_tx, joint_idx)`.
async fn enter_joint_phase() -> (Raft, tokio::sync::mpsc::UnboundedSender<Vec<u8>>, LogIndex) {
    let (transport, tx) = InjectTransport::new();
    let storage = TestStorage::default();
    let node = RaftNode::new(make_config(1, &OLD), storage, transport).unwrap();
    let mut raft = ArbitroRaft::new(node, NoopStateMachine);
    raft.node_mut().become_leader_for_benchmark(Term(5));

    let res = raft
        .propose_config_change(NEW.iter().copied().map(PeerId).collect())
        .await;
    assert!(
        matches!(res, Err(RaftError::NoQuorum)),
        "grow with no reachable peers must time out with NoQuorum, got {res:?}"
    );

    let st = raft.status();
    assert!(
        st.config_change_in_progress,
        "the appended-but-uncommitted Joint entry must keep the joint config \
         active (§4.1 append-time rule)"
    );
    let joint_idx = st.last_log_index;
    assert!(
        joint_idx > st.commit_index,
        "scenario integrity: the Joint entry ({}) must be appended and \
         uncommitted (commit {})",
        joint_idx.0,
        st.commit_index.0,
    );
    let expected_union: Vec<PeerId> = NEW.iter().copied().map(PeerId).collect();
    assert_eq!(
        raft.node().peers(),
        expected_union.as_slice(),
        "append-time activation must make the union the replication set"
    );
    (raft, tx, joint_idx)
}

/// Pump the run loop until the injected frames have been consumed; each
/// `run_once` burst-drains everything available and then runs the leader
/// tick (commit advance + apply).
async fn pump(raft: &mut Raft) {
    for _ in 0..3 {
        raft.run_once().await.expect("run_once");
    }
}

// ---------------------------------------------------------------------------
// Pin 1 (C2) — an entry acked by a majority of C_new (and of the union) but
// NOT of C_old must NOT commit during the joint phase.
//
// Ack pattern constructed: {leader(1), 4, 5} at the Joint entry's index —
//   * majority of C_new  {1,2,3,4,5}: 3 of 5  ✓
//   * majority of union  (5 voters):  3 of 5  ✓
//   * majority of C_old  {1,2,3}:     1 of 3  ✗   → must NOT commit.
//
// Discriminating reversion: put the commit decision back on a single
// majority of `config.peers` (the union) — the exact P0-4 propose-path bug
// (`propose/mod.rs` used union quorum) — or on C_new alone. Either way
// {1,4,5} = 3 acks satisfies it, `try_advance_commit_index` commits the
// Joint entry, and the "commit must not advance" assertion fails.
// The positive control (one old-side ack completes the dual rule and the
// SAME entry commits) proves the test measures the quorum rule, not a
// stalled pipeline.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn c2_new_majority_without_old_majority_must_not_commit() {
    let (mut raft, tx, joint_idx) = enter_joint_phase().await;
    let commit_before = raft.commit_index();

    // Acks from the ENTIRE new side (4 and 5) — with the leader's own entry
    // that is a majority of C_new and of the union, but only 1 of 3 in C_old.
    tx.send(ack_frame(4, 5, joint_idx.0)).unwrap();
    tx.send(ack_frame(5, 5, joint_idx.0)).unwrap();
    pump(&mut raft).await;

    assert_eq!(
        raft.commit_index(),
        commit_before,
        "THE PIN (C2/P0-4): the Joint entry at {} committed on a \
         new-side/union majority without a majority of C_old",
        joint_idx.0,
    );
    assert!(
        raft.node().is_leader(),
        "scenario integrity: the leader must still be leading (nothing \
         should have deposed it)"
    );

    // Positive control — ONE old-side ack (peer 2) completes the dual rule:
    // old {1,2} = 2 of 3 ✓, new {1,2,4,5} = 4 of 5 ✓ → the same entry commits.
    tx.send(ack_frame(2, 5, joint_idx.0)).unwrap();
    pump(&mut raft).await;
    assert!(
        raft.commit_index() >= joint_idx,
        "positive control: once BOTH majorities ack, the Joint entry must \
         commit (commit {} < joint {})",
        raft.commit_index().0,
        joint_idx.0,
    );
}

// ---------------------------------------------------------------------------
// Pin 2 (C3) — leader deposed mid-joint BEFORE the Joint entry commits: the
// appended Joint entry and the active joint config must survive the
// step-down (§4.1: an appended config entry stays effective until committed
// or superseded by a new leader's truncation).
//
// Discriminating reversion: roll back `joint_peers`/`config.peers` on
// step-down (e.g. "cleaning up" the transition in
// `demote_to_follower_keep_term`), or truncate/drop the uncommitted Joint
// entry. Either reversion makes this node campaign or vote under C_old
// while other nodes that already appended the Joint entry operate under the
// union — disjoint quorums, the exact split P0-4 warned about. The
// config/log assertions below fail under that reversion.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn c3_step_down_before_joint_commit_keeps_joint_config_and_log() {
    let (mut raft, tx, joint_idx) = enter_joint_phase().await;

    // A higher-term frame deposes the leader mid-joint (in-memory crash
    // equivalent — see the module-level A7 degraded note). One tick is
    // enough: the still-leader run path burst-drains the frame and steps down.
    tx.send(higher_term_frame(2, 99)).unwrap();
    raft.run_once().await.expect("run_once");

    assert!(!raft.node().is_leader(), "the higher term must depose the leader");
    assert_eq!(raft.status().term, Term(99), "the higher term must be adopted");

    // THE PIN: the joint transition survives the step-down intact.
    assert!(
        raft.status().config_change_in_progress,
        "step-down must NOT deactivate the appended joint config (§4.1)"
    );
    let expected_union: Vec<PeerId> = NEW.iter().copied().map(PeerId).collect();
    assert_eq!(
        raft.node().peers(),
        expected_union.as_slice(),
        "step-down must not split the config back to C_old"
    );
    assert!(
        raft.status().last_log_index >= joint_idx,
        "step-down must not drop the appended Joint entry"
    );
}

// ---------------------------------------------------------------------------
// Pin 3 (C3) — leader deposed mid-joint AFTER the Joint entry committed
// under the dual rule: the committed entry, the commit index, and the active
// joint config must all survive; the durable log still carries the Joint
// entry a recovering cluster would replay.
//
// Discriminating reversion: any step-down path that loses committed state —
// rolling back `commit_index` below a dual-quorum-committed entry, clearing
// the joint config, or truncating the log tail on demotion. Committed-entry
// loss is exactly what the dual rule exists to prevent (an entry committed
// without one side's majority can be lost to a leader elected inside that
// side); this pin asserts the committed Joint entry is still durable and
// visible after the demotion.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn c3_step_down_after_joint_commit_preserves_committed_entry() {
    let (mut raft, tx, joint_idx) = enter_joint_phase().await;

    // Commit the Joint entry under the REAL dual rule: old {1,2} of 3 ✓,
    // new {1,2,4,5} of 5 ✓.
    tx.send(ack_frame(4, 5, joint_idx.0)).unwrap();
    tx.send(ack_frame(5, 5, joint_idx.0)).unwrap();
    tx.send(ack_frame(2, 5, joint_idx.0)).unwrap();
    pump(&mut raft).await;
    assert!(
        raft.commit_index() >= joint_idx,
        "setup: the Joint entry must commit under the dual quorum"
    );
    let commit_at_depose = raft.commit_index();

    // Depose mid-transition (the Final entry has not been proposed — the
    // config change is still in flight).
    assert!(raft.status().config_change_in_progress);
    tx.send(higher_term_frame(3, 99)).unwrap();
    raft.run_once().await.expect("run_once");
    assert!(!raft.node().is_leader());

    // THE PIN: nothing committed was lost, and the config did not split.
    assert!(
        raft.commit_index() >= commit_at_depose,
        "step-down must never lose a committed entry (commit {} -> {})",
        commit_at_depose.0,
        raft.commit_index().0,
    );
    assert!(
        raft.status().last_log_index >= joint_idx,
        "the committed Joint entry must still be in the log"
    );
    assert!(
        raft.status().config_change_in_progress,
        "the joint config must remain active until the Final entry lands"
    );
    let expected_union: Vec<PeerId> = NEW.iter().copied().map(PeerId).collect();
    assert_eq!(raft.node().peers(), expected_union.as_slice());

    // Durability check a recovering node would rely on: the entry at
    // `joint_idx` is the Joint config-change entry, byte-decodable, with the
    // exact old/new voter sets.
    let mut buf = vec![0u8; 4096];
    let entry = raft
        .node()
        .read_entry_payload_into(joint_idx, &mut buf)
        .expect("read joint entry")
        .expect("joint entry must exist in the durable log");
    let decoded = ConfigChangeEntry::decode(entry.payload.0)
        .expect("the entry at joint_idx must decode as a config change");
    assert_eq!(decoded.phase, ConfigChangePhase::Joint);
    assert_eq!(
        decoded.old_peers,
        OLD.iter().copied().map(PeerId).collect::<Vec<_>>()
    );
    assert_eq!(
        decoded.new_peers,
        NEW.iter().copied().map(PeerId).collect::<Vec<_>>()
    );

}

// ---------------------------------------------------------------------------
// Pin 4 (dup-F1) — the check-quorum lease obeys the SAME joint dual-majority
// rule as commit/election/read-index: during an active joint transition,
// contact from a union majority that is NOT a dual majority must not keep
// the leader's quorum lease alive.
//
// Contact pattern constructed: {leader(1), 4, 5} —
//   * majority of the union (5 voters): 3 of 5  ✓
//   * majority of C_new {1,2,3,4,5}:    3 of 5  ✓
//   * majority of C_old {1,2,3}:        1 of 3  ✗  → lease must drop.
//
// Discriminating reversion: put `check_quorum_active` back on the plain
// union-majority contact counter (the pre-dup-F1 code) — {1,4,5} = 3 of 5
// then satisfies it and the first PIN assertion fails.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dup_f1_check_quorum_lease_requires_dual_majority_during_joint() {
    let (mut raft, _tx, _joint_idx) = enter_joint_phase().await;

    // Age out every contact stamped at leader-progress init (election
    // timeout = 300ms in `make_config`), so the only live contacts are the
    // ones this test injects.
    tokio::time::sleep(std::time::Duration::from_millis(350)).await;
    assert!(
        !raft.node_mut().check_quorum_active(),
        "scenario integrity: aged-out contacts must drop the lease"
    );

    // Current-term contact from the ENTIRE new side (4 and 5).
    let contact = AppendEntriesResp {
        term: 5u64.into(),
        match_index: 0u64.into(),
        success: 1,
        _pad: [0; 7],
    };
    for from in [4u64, 5] {
        raft.node_mut()
            .handle_inbound(InboundRaftMessage {
                from: PeerId(from),
                group_id: GroupId(0),
                message: RaftMessage::AppendEntriesResp(&contact),
            })
            .await
            .expect("inject contact frame");
    }
    assert!(
        !raft.node_mut().check_quorum_active(),
        "THE PIN (dup-F1): during a joint transition, a union-majority \
         contact set {{1,4,5}} without a majority of C_old must NOT keep \
         the check-quorum lease alive"
    );

    // Positive control — one old-side contact (peer 2) completes the dual
    // rule: old {1,2} = 2 of 3 ✓, new {1,2,4,5} = 4 of 5 ✓ → lease alive.
    raft.node_mut()
        .handle_inbound(InboundRaftMessage {
            from: PeerId(2),
            group_id: GroupId(0),
            message: RaftMessage::AppendEntriesResp(&contact),
        })
        .await
        .expect("inject old-side contact frame");
    assert!(
        raft.node_mut().check_quorum_active(),
        "positive control: a dual-majority contact set must keep the lease"
    );
}

// ---------------------------------------------------------------------------
// Pin 5 (dup-F1) — pre-vote obeys the SAME joint dual-majority rule as the
// real election: during an active joint transition, grants from a union
// majority that is NOT a dual majority must not clear the pre-vote gate
// (the subsequent real election would reject the same voter set, so passing
// pre-vote on it would only produce a doomed, disruptive term bump).
//
// Discriminating reversion: put `campaign_pre_vote` back on the plain
// `votes >= votes_needed` union counter (the pre-dup-F1 code) — grants from
// {4,5} plus self = 3 of 5 then satisfy it and the first PIN assertion
// fails.
// ---------------------------------------------------------------------------

fn pre_vote_grant_frame(from: u64, term: u64) -> Vec<u8> {
    let resp = arbitro_raft::RequestVoteResp {
        term: term.into(),
        vote_granted: 1,
        _pad: [0; 7],
    };
    arbitro_raft::encode_message_to_bytes(PeerId(from), &RaftMessage::PreVoteResp(&resp))
        .expect("encode pre-vote grant")
        .to_vec()
}

#[tokio::test]
async fn dup_f1_pre_vote_requires_dual_majority_during_joint() {
    let (mut raft, tx, _joint_idx) = enter_joint_phase().await;

    // Depose mid-joint: the joint config stays active (pinned by C3 above);
    // the node is now a follower of term 99 with the union as `peers`.
    tx.send(higher_term_frame(2, 99)).unwrap();
    raft.run_once().await.expect("run_once");
    assert!(!raft.node().is_leader(), "the higher term must depose the leader");
    assert!(
        raft.status().config_change_in_progress,
        "scenario integrity: the joint config must still be active"
    );

    let mut buf = vec![0u8; 64 * 1024];

    // Grants from the ENTIRE new side (4 and 5): with self that is {1,4,5}
    // — a union majority (3 of 5) but only 1 of 3 in C_old.
    tx.send(pre_vote_grant_frame(4, 99)).unwrap();
    tx.send(pre_vote_grant_frame(5, 99)).unwrap();
    let won = raft
        .node_mut()
        .campaign_pre_vote(&mut buf)
        .await
        .expect("campaign_pre_vote");
    assert!(
        !won,
        "THE PIN (dup-F1): during a joint transition, pre-vote grants from a \
         union majority without a majority of C_old must NOT clear the gate"
    );

    // Positive control — grants from {2,4,5}: old {1,2} = 2 of 3 ✓,
    // new {1,2,4,5} = 4 of 5 ✓ → the gate clears.
    tx.send(pre_vote_grant_frame(2, 99)).unwrap();
    tx.send(pre_vote_grant_frame(4, 99)).unwrap();
    tx.send(pre_vote_grant_frame(5, 99)).unwrap();
    let won = raft
        .node_mut()
        .campaign_pre_vote(&mut buf)
        .await
        .expect("campaign_pre_vote (positive control)");
    assert!(
        won,
        "positive control: a dual-majority grant set must clear pre-vote"
    );
}
