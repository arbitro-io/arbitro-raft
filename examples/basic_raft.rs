//! Basic ArbitroRaft usage — single-node cluster, sequential and batch writes.
//!
//! Demonstrates the two-layer API:
//!   - `RaftNode`    — the low-level engine (storage + transport + consensus)
//!   - `ArbitroRaft` — the async wrapper that adds timers and the client channel
//!
//! In a single-node cluster quorum = 1 (the leader itself), so every write
//! commits immediately without waiting for any transport round-trip.
//!
//! For concurrent writes across many tasks, see the `ClientHandle` pattern:
//!   let handle = raft.client_handle();
//!   tokio::spawn(async move { handle.write(payload).await });
//!   raft.run().await;  // event loop drives consensus
//!
//! Run with:  cargo run --example basic_raft

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use arbitro_raft::{
    ArbitroRaft, BootstrapPeer, ClusterId, HardState, LogEntry, LogIndex,
    NodeConfig, PeerId, RaftError, RaftNode, RaftStorage, RaftTransport,
    SnapshotMeta, Term,
};

// ── Minimal in-memory storage ─────────────────────────────────────────────────

#[derive(Clone, Default)]
struct MemStorage {
    entries:    Arc<Mutex<Vec<LogEntry>>>,
    hard_state: Arc<Mutex<HardState>>,
}

impl RaftStorage for MemStorage {
    fn load_hard_state(&self) -> Result<HardState, RaftError> {
        Ok(self.hard_state.lock().unwrap().clone())
    }
    fn save_hard_state(&self, hs: &HardState) -> Result<(), RaftError> {
        *self.hard_state.lock().unwrap() = hs.clone();
        Ok(())
    }
    fn append_entries(&self, new: &[LogEntry]) -> Result<(), RaftError> {
        self.entries.lock().unwrap().extend_from_slice(new);
        Ok(())
    }
    fn read_entries(&self, from: LogIndex, to: LogIndex, out: &mut Vec<LogEntry>) -> Result<(), RaftError> {
        out.extend(
            self.entries.lock().unwrap().iter()
                .filter(|e| e.index >= from && e.index < to)
                .cloned(),
        );
        Ok(())
    }
    fn truncate_suffix(&self, from: LogIndex) -> Result<(), RaftError> {
        self.entries.lock().unwrap().retain(|e| e.index < from);
        Ok(())
    }
    fn save_snapshot(&self, _: &SnapshotMeta, _: &[u8]) -> Result<(), RaftError> { Ok(()) }
    fn load_snapshot(&self) -> Result<Option<(SnapshotMeta, Vec<u8>)>, RaftError> { Ok(None) }
    fn last_log_position(&self) -> Result<(LogIndex, Term), RaftError> {
        Ok(self.entries.lock().unwrap().last()
            .map(|e| (e.index, e.term))
            .unwrap_or((LogIndex(0), Term(0))))
    }
    fn entry_at(&self, index: LogIndex) -> Result<Option<LogEntry>, RaftError> {
        Ok(self.entries.lock().unwrap().iter().find(|e| e.index == index).cloned())
    }
}

// ── No-op transport — single-node cluster never addresses remote peers ────────

struct NoopTransport;

#[async_trait]
impl RaftTransport for NoopTransport {
    async fn send_frame(&self, _: PeerId, _: Bytes) -> Result<(), RaftError> { Ok(()) }
    async fn recv_frame(&self) -> Result<Bytes, RaftError> {
        futures::future::pending().await // unreachable in 1-node cluster
    }
    async fn recv_frame_timeout(&self, _: Duration) -> Result<Option<Bytes>, RaftError> {
        Ok(None) // no peers → no inbound frames
    }
}

// ── Main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    // 1. Build a single-node cluster (quorum = 1 → commits immediately).
    let config = NodeConfig {
        node_id:    PeerId(1),
        cluster_id: ClusterId(1),
        peers:      vec![PeerId(1)],
        bootstrap_peers: vec![
            BootstrapPeer { id: PeerId(1), addr: "127.0.0.1:0".parse().unwrap() },
        ],
        ..Default::default()
    };

    let mut node = RaftNode::new(config, MemStorage::default(), NoopTransport).unwrap();

    // Elect the node as leader (skips the campaign round-trip for this demo).
    #[allow(deprecated)]
    node.become_leader_for_benchmark(Term(1));

    let mut raft = ArbitroRaft::new(node);

    // 2. propose_once — synchronous single-entry commit.
    //    In a 1-node cluster quorum is already satisfied by the leader itself,
    //    so this returns immediately without any network I/O.
    let idx_a = raft.propose_once(Bytes::from("entry-a")).await.unwrap();
    let idx_b = raft.propose_once(Bytes::from("entry-b")).await.unwrap();
    println!("propose_once → entry-a: {idx_a:?}  entry-b: {idx_b:?}");

    // 3. propose_batch_once — commit N entries in a single round-trip.
    //    All entries share the same AppendEntries frame; the response is one ACK.
    let batch = vec![
        Bytes::from("batch-1"),
        Bytes::from("batch-2"),
        Bytes::from("batch-3"),
    ];
    let indexes = raft.propose_batch_once(batch).await.unwrap();
    println!("propose_batch_once → {indexes:?}");
}
