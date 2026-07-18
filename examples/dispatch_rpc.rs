//! Custom RPC via the Dispatch layer.
//!
//! `DispatchSpec` turns any (params, response) pair into a typed, policy-aware
//! RPC that the consensus layer routes to one or more peers.  This example
//! uses `DispatchScope::LocalOnly` so the request is handled in-process,
//! but the exact same code works across a real TCP cluster by changing the
//! scope to `DispatchScope::All` or `DispatchScope::Followers`.
//!
//! Full round-trip:
//!   1. Define a spec  →  command ID + encode/decode fns + default options
//!   2. Register a handler on every node that should respond
//!   3. `node.dispatch(spec, params)` → `DispatchHandle<R>`
//!   4. `handle.wait()` → `DispatchResult<R>` with one entry per responding peer
//!
//! Run with:  cargo run --example dispatch_rpc

use std::sync::{Arc, Mutex};
use std::time::Duration;

use arbitro_raft::{
    BootstrapPeer, ClusterId, DispatchAckPolicy, DispatchPeerState, DispatchScope, DispatchSpec,
    EntryPayload, HardState, LogEntry, LogIndex, NodeConfig, PeerId, RaftError, RaftNode,
    RaftStorage, RaftTransport, SnapshotMeta, Term,
};

// ── Minimal storage / transport ── (Simplistic for example)

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
    fn append_entries(&self, new: &[LogEntry]) -> Result<(), RaftError> {
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

fn ping() -> DispatchSpec<String, String> {
    DispatchSpec::new(
        0x01,
        |s| Ok(s.as_bytes().to_vec()),
        |b| Ok(String::from_utf8_lossy(b).into_owned()),
        |s| Ok(s.as_bytes().to_vec()),
        |b| Ok(String::from_utf8_lossy(b).into_owned()),
    )
}

#[tokio::main]
async fn main() {
    let config = NodeConfig {
        node_id: PeerId(1),
        cluster_id: ClusterId(1),
        peers: vec![PeerId(1)],
        learners: Vec::new(),
        bootstrap_peers: vec![BootstrapPeer {
            id: PeerId(1),
            addr: "127.0.0.1:0".parse().unwrap(),
        }],
        ..Default::default()
    };

    let mut node = RaftNode::new(config, MemStorage::default(), NoopTransport).unwrap();

    #[allow(deprecated)]
    node.become_leader_for_benchmark(Term(1));

    let spec = ping()
        .with_scope(DispatchScope::LocalOnly)
        .with_ack_policy(DispatchAckPolicy::All);

    node.on_with(spec.clone(), |question: String, ctx| {
        Box::pin(async move {
            let answer = format!("pong: {question}");
            ctx.accept_bytes(answer.as_bytes().to_vec()).await
        })
    })
    .unwrap();

    let handle = node
        .dispatch(spec, "hello from leader".to_string())
        .await
        .unwrap();

    let result = handle.wait().await.unwrap();

    for peer_result in &result.peers {
        match &peer_result.state {
            DispatchPeerState::Accepted(reply) => {
                println!("peer {:?} → {reply}", peer_result.peer);
            }
            DispatchPeerState::Rejected(reason) => {
                eprintln!(
                    "peer {:?} rejected: {}",
                    peer_result.peer,
                    String::from_utf8_lossy(reason)
                );
            }
            other => eprintln!("peer {:?} state: {other:?}", peer_result.peer),
        }
    }
}
