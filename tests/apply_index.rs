//! A6 — `StateMachine::apply_at` carries the committed `LogIndex`.
//!
//! Proves the two halves of the A6 contract:
//! - the ENGINE routes every committed entry through `apply_at`, passing the
//!   entry's real committed `LogIndex` in strict log order (never calling
//!   `apply` directly);
//! - the DEFAULT `apply_at` implementation forwards to `apply`, so an
//!   existing state machine that overrides only `apply` keeps receiving
//!   every committed entry, untouched.

#[path = "support/fault_storage.rs"]
mod support;

use std::sync::{Arc, Mutex};

use arbitro_raft::{ArbitroRaft, LogIndex, RaftError, RaftNode, StateMachine};
use support::{make_config, CaptureTransport, FaultStorage};

/// Overrides `apply_at` and records every `(index, payload)` pair; counts any
/// (forbidden) direct `apply` call.
#[derive(Clone, Default)]
struct IndexRecordingSm {
    seen: Arc<Mutex<Vec<(u64, Vec<u8>)>>>,
    direct_apply_calls: Arc<Mutex<u64>>,
}

impl StateMachine for IndexRecordingSm {
    fn apply(&mut self, _entry: &[u8]) -> Result<(), RaftError> {
        *self.direct_apply_calls.lock().unwrap() += 1;
        Ok(())
    }
    fn apply_at(&mut self, index: LogIndex, entry: &[u8]) -> Result<(), RaftError> {
        self.seen.lock().unwrap().push((index.0, entry.to_vec()));
        Ok(())
    }
    fn snapshot(&self) -> Result<Vec<u8>, RaftError> {
        Ok(Vec::new())
    }
    fn restore(&mut self, _snapshot: &[u8]) -> Result<(), RaftError> {
        Ok(())
    }
}

/// Pre-A6 style implementation: overrides ONLY `apply` (no `apply_at`) —
/// exactly what every existing `StateMachine` impl looks like.
#[derive(Clone, Default)]
struct DefaultOnlySm {
    seen: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl StateMachine for DefaultOnlySm {
    fn apply(&mut self, entry: &[u8]) -> Result<(), RaftError> {
        self.seen.lock().unwrap().push(entry.to_vec());
        Ok(())
    }
    fn snapshot(&self) -> Result<Vec<u8>, RaftError> {
        Ok(Vec::new())
    }
    fn restore(&mut self, _snapshot: &[u8]) -> Result<(), RaftError> {
        Ok(())
    }
}

fn single_node<SM: StateMachine>(sm: SM) -> ArbitroRaft<FaultStorage, CaptureTransport, SM> {
    let storage = FaultStorage::new();
    let transport = CaptureTransport::new();
    let node =
        RaftNode::new(make_config(1, &[1]), storage, transport).expect("single node must boot");
    ArbitroRaft::new(node, sm)
}

#[tokio::test]
async fn engine_passes_committed_log_index_to_apply_at() {
    let sm = IndexRecordingSm::default();
    let mut raft = single_node(sm.clone());
    assert!(
        raft.campaign_once().await.expect("campaign"),
        "single node must elect itself"
    );

    // Propose three entries; a single-node cluster commits each immediately.
    let mut expected = Vec::new();
    for payload in [&b"alpha"[..], b"bravo", b"charlie"] {
        let idx = raft
            .propose_once(payload)
            .await
            .expect("single-node propose must commit");
        expected.push((idx.0, payload.to_vec()));
    }

    // `drain` runs the apply loop as part of graceful shutdown — a
    // deterministic way to flush committed-but-unapplied entries.
    raft.drain().await.expect("drain must succeed");

    assert_eq!(
        *sm.seen.lock().unwrap(),
        expected,
        "apply_at must receive each entry's committed LogIndex, in log order"
    );
    assert_eq!(
        *sm.direct_apply_calls.lock().unwrap(),
        0,
        "the engine must route every apply through apply_at, never apply directly"
    );
}

#[tokio::test]
async fn default_apply_at_forwards_to_apply() {
    let sm = DefaultOnlySm::default();
    let mut raft = single_node(sm.clone());
    assert!(raft.campaign_once().await.expect("campaign"));

    let mut expected = Vec::new();
    for payload in [&b"one"[..], b"two"] {
        raft.propose_once(payload)
            .await
            .expect("single-node propose must commit");
        expected.push(payload.to_vec());
    }
    raft.drain().await.expect("drain must succeed");

    assert_eq!(
        *sm.seen.lock().unwrap(),
        expected,
        "an impl overriding only `apply` must still receive every committed \
         entry via the default apply_at forward (A6 is non-breaking)"
    );
}
