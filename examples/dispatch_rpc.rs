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
    HardState, LogEntry, LogIndex, NodeConfig, PeerId, RaftError, RaftNode, RaftStorage,
    RaftTransport, SnapshotMeta, Term,
};
use async_trait::async_trait;
use bytes::Bytes;

// ── Minimal storage / transport (identical to basic_raft.rs) ─────────────────

#[derive(Clone, Default)]
struct MemStorage {
    entries: Arc<Mutex<Vec<LogEntry>>>,
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
    fn read_entries(
        &self,
        from: LogIndex,
        to: LogIndex,
        out: &mut Vec<LogEntry>,
    ) -> Result<(), RaftError> {
        out.extend(
            self.entries
                .lock()
                .unwrap()
                .iter()
                .filter(|e| e.index >= from && e.index < to)
                .cloned(),
        );
        Ok(())
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
    fn entry_at(&self, index: LogIndex) -> Result<Option<LogEntry>, RaftError> {
        Ok(self
            .entries
            .lock()
            .unwrap()
            .iter()
            .find(|e| e.index == index)
            .cloned())
    }
}

struct NoopTransport;

#[async_trait]
impl RaftTransport for NoopTransport {
    async fn send_frame(&self, _: PeerId, _: Bytes) -> Result<(), RaftError> {
        Ok(())
    }
    async fn recv_frame(&self) -> Result<Bytes, RaftError> {
        futures::future::pending().await
    }
    async fn recv_frame_timeout(&self, _: Duration) -> Result<Option<Bytes>, RaftError> {
        Ok(None)
    }
}

// ── Dispatch spec — "Ping" RPC: String → String ───────────────────────────────
//
// `DispatchSpec` is `Copy`, so you can freely pass or store it by value.
// The five arguments are:
//   1. command ID — a u8 that uniquely identifies this RPC in the cluster
//   2. encode_params   — fn(&Params) → Bytes
//   3. decode_params   — fn(&[u8])   → Params
//   4. encode_response — fn(&Resp)   → Bytes
//   5. decode_response — fn(&[u8])   → Resp

fn ping() -> DispatchSpec<String, String> {
    DispatchSpec::new(
        0x01,
        |s| Ok(Bytes::copy_from_slice(s.as_bytes())),
        |b| Ok(String::from_utf8_lossy(b).into_owned()),
        |s| Ok(Bytes::copy_from_slice(s.as_bytes())),
        |b| Ok(String::from_utf8_lossy(b).into_owned()),
    )
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

    // Build the spec once — `DispatchSpec` is `Copy` so reuse is free.
    let spec = ping()
        .with_scope(DispatchScope::LocalOnly)
        .with_ack_policy(DispatchAckPolicy::All);

    // 1. Register the handler — runs on any node that should respond to PING.
    //    The closure receives (decoded_params, DispatchContextView).
    //    Call ctx.accept_bytes / ctx.reject / ctx.fail to send back a response.
    node.on_with(spec, |question: String, ctx| {
        Box::pin(async move {
            let answer = format!("pong: {question}");
            ctx.accept_bytes(Bytes::copy_from_slice(answer.as_bytes()))
                .await
        })
    })
    .unwrap();

    // 2. Dispatch — fires the RPC, returns a handle immediately.
    //    LocalOnly: handler is invoked in-process with no transport involved.
    let handle = node
        .dispatch(spec, "hello from leader".to_string())
        .await
        .unwrap();

    // 3. Wait for the required acknowledgements (DispatchAckPolicy::All = every
    //    targeted peer must respond).
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
