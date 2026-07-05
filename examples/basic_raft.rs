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

use arbitro_raft::{
    ArbitroRaft, BootstrapPeer, ClusterId, EntryPayload, HardState, LogEntry, LogIndex, NodeConfig,
    PeerId, RaftError, RaftNode, RaftStorage, RaftTransport, SnapshotMeta, Term,
};

// ── Minimal in-memory storage ─────────────────────────────────────────────────

#[derive(Clone, Default)]
struct StoredEntry {
    term: Term,
    index: LogIndex,
    payload: Vec<u8>,
}

#[derive(Clone, Default)]
struct MemStorage {
    entries: Arc<Mutex<Vec<StoredEntry>>>,
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
    fn append_entries(&self, new: &[LogEntry<'_>]) -> Result<(), RaftError> {
        let mut entries = self.entries.lock().unwrap();
        for e in new {
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
        let mut offset = 0;
        let mut rest = payload_buf;

        for e in entries.iter().filter(|e| e.index >= from && e.index < to) {
            let len = e.payload.len();
            if len > rest.len() {
                break;
            }

            let (target, next_rest) = rest.split_at_mut(len);
            target.copy_from_slice(&e.payload);

            out.push(LogEntry {
                term: e.term,
                index: e.index,
                payload: EntryPayload(target),
            });

            offset += len;
            rest = next_rest;
        }
        Ok(offset)
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
            .unwrap_or((LogIndex(0), Term(0))))
    }
    fn entry_at<'a>(
        &self,
        index: LogIndex,
        payload_buf: &'a mut [u8],
    ) -> Result<Option<LogEntry<'a>>, RaftError> {
        let entries = self.entries.lock().unwrap();
        if let Some(e) = entries.iter().find(|e| e.index == index) {
            let len = e.payload.len();
            if len > payload_buf.len() {
                return Err(RaftError::Transport("buffer too small".into()));
            }
            payload_buf[..len].copy_from_slice(&e.payload);
            Ok(Some(LogEntry {
                term: e.term,
                index: e.index,
                payload: EntryPayload(&payload_buf[..len]),
            }))
        } else {
            Ok(None)
        }
    }
}

// ── No-op transport — single-node cluster never addresses remote peers ────────

struct NoopTransport;

impl RaftTransport for NoopTransport {
    fn send_vectored(
        &self,
        _: PeerId,
        _: &[&[u8]],
    ) -> impl std::future::Future<Output = Result<(), RaftError>> + Send {
        async move { Ok(()) }
    }
    fn send_frame_owned(
        &self,
        _: PeerId,
        _: bytes::Bytes,
    ) -> impl std::future::Future<Output = Result<(), RaftError>> + Send {
        async move { Ok(()) }
    }
    fn recv_frame(
        &self,
        _: &mut [u8],
    ) -> impl std::future::Future<Output = Result<usize, RaftError>> + Send {
        async move { futures::future::pending().await }
    }
    fn recv_frame_timeout(
        &self,
        _: Duration,
        _: &mut [u8],
    ) -> impl std::future::Future<Output = Result<Option<usize>, RaftError>> + Send {
        async move { Ok(None) }
    }
}

// ── Main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let config = NodeConfig {
        node_id: PeerId(1),
        cluster_id: ClusterId(1),
        peers: vec![PeerId(1)],
        bootstrap_peers: vec![BootstrapPeer {
            id: PeerId(1),
            addr: "127.0.0.1:0".parse().unwrap(),
        }],
        ..Default::default()
    };

    let mut node = RaftNode::new(config, MemStorage::default(), NoopTransport).unwrap();

    #[allow(deprecated)]
    node.become_leader_for_benchmark(Term(1));

    let mut raft = ArbitroRaft::new(node);

    let idx_a = raft.propose_once(b"entry-a").await.unwrap();
    let idx_b = raft.propose_once(b"entry-b").await.unwrap();
    println!("propose_once → entry-a: {idx_a:?}  entry-b: {idx_b:?}");

    let batch: &[&[u8]] = &[b"batch-1", b"batch-2", b"batch-3"];
    let indexes = raft.propose_batch_once(batch).await.unwrap();
    println!("propose_batch_once → {indexes:?}");
}
