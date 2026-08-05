//! H5 — bounded, zero-copy client mailbox.
//!
//! What must hold:
//! - the client→node proposal mailbox is BOUNDED by
//!   `limits.client_mailbox_capacity`: a client flood cannot buffer without
//!   limit (the old unbounded channel's OOM path);
//! - a write against a FULL mailbox resolves SYNCHRONOUSLY (no hang, no
//!   panic, no silent drop) with the retryable `RaftError::Overloaded`
//!   (`ErrorClass::Transient`);
//! - a write against a STOPPED node keeps today's channel-closed mapping
//!   (a `Transport` error, distinct from overload);
//! - the happy path still commits, for both the copying `write(&[u8])` and
//!   the zero-copy `write_bytes(Bytes)` entry points.

#[path = "support/a7_harness.rs"]
mod a7;
#[path = "support/fault_storage.rs"]
mod support;

use std::sync::Arc;
use std::time::Duration;

use arbitro_raft::{ArbitroRaft, ErrorClass, NoopStateMachine, RaftError, RaftNode, Role};
use bytes::Bytes;
use futures::FutureExt;
use support::{make_config, CaptureTransport, FaultStorage};

fn raft_with_mailbox_cap(
    cap: usize,
) -> ArbitroRaft<FaultStorage, CaptureTransport, NoopStateMachine> {
    let mut config = make_config(1, &[1]);
    config.limits.client_mailbox_capacity = cap;
    let node = RaftNode::new(config, FaultStorage::new(), CaptureTransport::new())
        .expect("single node must boot");
    ArbitroRaft::new(node, NoopStateMachine)
}

// ---------------------------------------------------------------------------
// 1. Overload: a full mailbox rejects fast with the retryable error.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn full_mailbox_rejects_fast_with_retryable_overloaded() {
    // Tiny capacity so two queued proposals fill the mailbox. The run loop is
    // deliberately NOT driven — nothing drains.
    let raft = raft_with_mailbox_cap(2);
    let handle = raft.client_handle();

    // Each write's `try_send` runs on the first poll (before any await), so
    // `now_or_never` returning `None` proves the proposal was ENQUEUED and the
    // future parked awaiting commit. (Dropping the parked future releases its
    // notification slot; the proposal itself stays queued in the mailbox.)
    assert!(
        handle.write(b"one").now_or_never().is_none(),
        "first write must enqueue and park awaiting commit"
    );
    assert!(
        handle.write(b"two").now_or_never().is_none(),
        "second write must enqueue and park awaiting commit"
    );

    // Mailbox full → the third write must resolve on the FIRST poll (no hang)
    // with the retryable overload error (no panic, no silent drop).
    let err = handle
        .write(b"three")
        .now_or_never()
        .expect("an overloaded write must fail fast, not park")
        .expect_err("a full mailbox must reject the proposal");
    assert!(
        matches!(err, RaftError::Overloaded),
        "overload must surface as RaftError::Overloaded, got {err:?}"
    );
    assert_eq!(
        err.class(),
        ErrorClass::Transient,
        "overload must classify Transient (retryable)"
    );
    assert!(!err.is_fatal(), "overload must never be fatal");

    // The zero-copy entry point observes the same backpressure.
    let err = handle
        .write_bytes(Bytes::from_static(b"four"))
        .now_or_never()
        .expect("an overloaded write_bytes must fail fast, not park")
        .expect_err("a full mailbox must reject the proposal");
    assert!(matches!(err, RaftError::Overloaded), "got {err:?}");

    // Stopped node stays the channel-closed mapping — NOT overload — so a
    // retry loop keyed on `Overloaded` cannot spin against a dead node.
    drop(raft);
    let err = handle
        .write(b"late")
        .now_or_never()
        .expect("a write against a stopped node must fail fast")
        .expect_err("a stopped node must reject the proposal");
    assert!(
        matches!(err, RaftError::Transport(_)),
        "stopped-node rejection must stay a Transport error, got {err:?}"
    );
}

// ---------------------------------------------------------------------------
// 2. Happy path: both entry points still commit through the bounded mailbox.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn happy_path_write_and_zero_copy_write_bytes_still_commit() {
    // Live 3-node cluster over the routed in-memory hub — the same shape the
    // membership/drain suites use — so quorum acks flow and commits resolve.
    let hub = Arc::new(a7::NetworkHub::new());
    let peers = [1u64, 2, 3];
    let nodes: Vec<a7::SharedRaft> = peers
        .iter()
        .map(|&id| a7::boot_node_with_storage(hub.clone(), id, &peers, a7::TestStorage::default()))
        .collect();
    let _drivers = a7::AbortOnDrop {
        handles: nodes.iter().cloned().map(a7::spawn_driver).collect(),
    };

    let mut leader = None;
    for _ in 0..200 {
        for node in &nodes {
            if node.lock().await.role() == Role::Leader {
                leader = Some(node.clone());
            }
        }
        if leader.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let leader = leader.expect("a leader must emerge");
    let handle = leader.lock().await.client_handle();

    let copied = tokio::time::timeout(Duration::from_secs(5), handle.write(b"copied slice"))
        .await
        .expect("write must not hang")
        .expect("write must commit on a healthy cluster");
    let zero_copy = tokio::time::timeout(
        Duration::from_secs(5),
        handle.write_bytes(Bytes::from_static(b"zero-copy bytes")),
    )
    .await
    .expect("write_bytes must not hang")
    .expect("write_bytes must commit on a healthy cluster");

    assert!(
        zero_copy > copied,
        "the second write must land at a later log index"
    );
    assert!(
        leader.lock().await.commit_index() >= zero_copy,
        "commit index must reach the last committed write"
    );
}
