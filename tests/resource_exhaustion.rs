//! C8 — disk-full (ENOSPC-class) degradation policy.
//!
//! What must hold when local storage runs out of space (data intact, write
//! refused):
//! - the error classifies `ErrorClass::Resource`, NOT `Fatal`;
//! - a LEADER whose append hits ENOSPC steps down to read-only survival
//!   (rejects new proposals with `NotLeader`), increments the
//!   `resource_exhausted` metric, and the run loop KEEPS TICKING;
//! - the node recovers on its own once storage succeeds again (space freed):
//!   it can win an election and commit new writes — no restart;
//! - a FOLLOWER under ENOSPC never acks the un-persisted entry and its error
//!   is non-fatal (the run loop drops the frame instead of dying), then acks
//!   normally once space is freed;
//! - corruption (`CorruptLog`) still classifies Fatal and still kills the
//!   run loop — degradation must never mask corruption.

#[path = "support/fault_storage.rs"]
mod support;

use arbitro_raft::{
    AppendEntries, ArbitroRaft, ErrorClass, GroupId, InboundRaftMessage, NoopStateMachine, PeerId,
    RaftError, RaftMessage, RaftNode,
};
use support::{
    contiguous_entry_block, make_config, CaptureTransport, FaultStorage, StickyAppendFault,
};

fn single_node(
    storage: &FaultStorage,
    transport: &CaptureTransport,
) -> ArbitroRaft<FaultStorage, CaptureTransport, NoopStateMachine> {
    let node = RaftNode::new(make_config(1, &[1]), storage.clone(), transport.clone())
        .expect("single node must boot");
    ArbitroRaft::new(node, NoopStateMachine)
}

fn ae_msg(term: u64, leader: u64, count: u32) -> AppendEntries {
    AppendEntries {
        term: term.into(),
        leader_id: leader.into(),
        prev_log_index: 0u64.into(),
        prev_log_term: 0u64.into(),
        leader_commit: 0u64.into(),
        entry_count: count.into(),
        _pad: 0u32.into(),
    }
}

// ---------------------------------------------------------------------------
// 1. Classification: ENOSPC-class IO errors are Resource; everything
//    ambiguous or corrupt stays Fatal.
// ---------------------------------------------------------------------------

#[test]
fn enospc_classifies_resource_and_corruption_stays_fatal() {
    for kind in [
        std::io::ErrorKind::StorageFull,
        std::io::ErrorKind::QuotaExceeded,
        std::io::ErrorKind::OutOfMemory,
    ] {
        let err = RaftError::Io(std::io::Error::new(kind, "injected"));
        assert_eq!(err.class(), ErrorClass::Resource, "kind {kind:?}");
        assert!(
            err.is_resource_exhaustion() && !err.is_fatal(),
            "kind {kind:?}"
        );
    }
    // Conservative boundary: anything that is not unambiguously resource
    // exhaustion keeps the fail-fast Fatal classification.
    let generic_io = RaftError::Io(std::io::Error::other("unknown io failure"));
    assert_eq!(generic_io.class(), ErrorClass::Fatal);
    assert!(RaftError::Storage("stringified io".into()).is_fatal());
    assert!(RaftError::CorruptLog("bit rot".into()).is_fatal());
}

// ---------------------------------------------------------------------------
// 2. Leader: ENOSPC on append → step down, read-only survival, metric,
//    automatic recovery when space is freed. The node is NEVER killed.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn disk_full_leader_steps_down_read_only_and_recovers() {
    let storage = FaultStorage::new();
    let transport = CaptureTransport::new();
    let mut raft = single_node(&storage, &transport);
    let metrics = raft.metrics();

    assert!(
        raft.campaign_once().await.expect("campaign"),
        "single node must elect itself"
    );
    raft.propose_once(b"healthy")
        .await
        .expect("healthy write must commit");

    // Disk fills up. Queue a client write and drive the run loop: the tick
    // that tries to persist it hits ENOSPC and must DEGRADE, not die.
    storage.set_sticky_append_fault(Some(StickyAppendFault::DiskFull));
    let handle = raft.client_handle();
    let doomed = tokio::spawn(async move { handle.write(b"doomed").await });

    let mut stepped_down = false;
    for _ in 0..100 {
        let alive = raft
            .run_once()
            .await
            .expect("an ENOSPC-class error must never kill the run loop");
        assert!(
            alive,
            "the node must keep ticking through resource exhaustion"
        );
        if !raft.node().is_leader() {
            stepped_down = true;
            break;
        }
    }
    assert!(
        stepped_down,
        "the leader must step down to read-only survival on ENOSPC"
    );
    assert!(
        metrics.snapshot().resource_exhausted >= 1,
        "the resource_exhausted metric must be incremented"
    );
    let doomed = doomed.await.expect("join");
    assert!(
        doomed.is_err(),
        "the un-persistable write must resolve with an error, not hang"
    );

    // Read-only survival: new proposals are rejected with NotLeader.
    let err = raft
        .propose_once(b"rejected")
        .await
        .expect_err("a stepped-down node must reject proposals");
    assert!(
        matches!(err, RaftError::NotLeader { .. }),
        "rejection must be NotLeader (redirectable), got {err:?}"
    );
    let committed_before = raft.commit_index();

    // Space is freed → the node recovers WITHOUT a restart: it wins an
    // election again (the storage op succeeds now) and commits new writes.
    storage.set_sticky_append_fault(None);
    assert!(
        raft.campaign_once().await.expect("recovered campaign"),
        "the node must be electable again once storage recovered"
    );
    let idx = raft
        .propose_once(b"recovered")
        .await
        .expect("a write must commit again after space was freed");
    assert!(
        idx > committed_before,
        "the recovered write advances the log"
    );
    assert_eq!(raft.commit_index(), idx, "the recovered write committed");
}

// ---------------------------------------------------------------------------
// 3. Corruption is untouched by C8: a CorruptLog append error still
//    terminates the run loop (Fatal) — degradation never masks corruption.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn corrupt_log_on_append_still_kills_the_run_loop() {
    let storage = FaultStorage::new();
    let transport = CaptureTransport::new();
    let mut raft = single_node(&storage, &transport);
    assert!(raft.campaign_once().await.expect("campaign"));

    storage.set_sticky_append_fault(Some(StickyAppendFault::Corrupt));
    let handle = raft.client_handle();
    let doomed = tokio::spawn(async move { handle.write(b"doomed").await });

    let mut fatal = None;
    for _ in 0..100 {
        match raft.run_once().await {
            Ok(true) => continue,
            Ok(false) => panic!("node stopped cleanly; expected a fatal corruption error"),
            Err(e) => {
                fatal = Some(e);
                break;
            }
        }
    }
    let err = fatal.expect("corruption must terminate the run loop");
    assert!(
        matches!(err, RaftError::CorruptLog(_)) && err.is_fatal(),
        "corrupt-log must stay Fatal, got {err:?}"
    );
    let doomed = doomed.await.expect("join");
    assert!(
        doomed.is_err(),
        "the doomed write must resolve with an error"
    );
}

// ---------------------------------------------------------------------------
// 4. Follower: ENOSPC on append → no ack, non-fatal (the run loop's
//    non-fatal handler tolerance keeps the node alive), acks after recovery.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn follower_enospc_never_acks_is_nonfatal_and_recovers() {
    let storage = FaultStorage::new();
    let transport = CaptureTransport::new();
    let mut node = RaftNode::new(
        make_config(1, &[1, 2, 3]),
        storage.clone(),
        transport.clone(),
    )
    .expect("follower must boot");
    storage.set_sticky_append_fault(Some(StickyAppendFault::DiskFull));

    let ae = ae_msg(1, 2, 1);
    let block = contiguous_entry_block(&[(1, 1, b"cmd")]);
    let err = node
        .handle_inbound(InboundRaftMessage {
            from: PeerId(2),
            group_id: GroupId(0),
            message: RaftMessage::AppendEntries(&ae, &block),
        })
        .await
        .expect_err("append under ENOSPC must surface the error");
    assert!(
        err.is_resource_exhaustion() && !err.is_fatal(),
        "follower ENOSPC must classify Resource — the run loop then drops \
         the frame instead of dying, got {err:?}"
    );
    assert!(
        transport.sent_to(2).is_empty(),
        "no ack may be sent for an entry that was not persisted"
    );
    assert!(storage.durable_entries().is_empty());

    // Space freed → the SAME node persists and acks the leader's retry.
    storage.set_sticky_append_fault(None);
    node.handle_inbound(InboundRaftMessage {
        from: PeerId(2),
        group_id: GroupId(0),
        message: RaftMessage::AppendEntries(&ae, &block),
    })
    .await
    .expect("the retried append must succeed after recovery");
    assert_eq!(
        storage.durable_entries().len(),
        1,
        "entry durable after recovery"
    );
    assert_eq!(
        transport.sent_to(2).len(),
        1,
        "the recovered follower acks the retried append"
    );
}
