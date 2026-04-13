// ARBITRO RAFT — DISPATCH RPC BENCHMARK
//
// Measures the orchestration overhead of the parallel Dispatch API.
// Reuses the setup from tests/dispatch_real.rs for minimal overhead.

use std::time::{Duration, Instant};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use arbitro_raft::{
    DispatchAckPolicy, DispatchHandle, DispatchSpec, PeerId, RaftError,
    RaftNode, NodeConfig, Term, RaftStorage, RaftTransport, HardState,
    LogIndex, LogEntry, SnapshotMeta, EntryPayload, Role,
};
use criterion::{criterion_group, criterion_main, Criterion, Throughput};

// ---------------------------------------------------------------------------
// Mock Logic from dispatch_real.rs
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SyncParams { start: u64, end: u64 }
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SyncAck { saved_until: u64 }

fn encode_sync_params(value: &SyncParams) -> Result<Vec<u8>, RaftError> {
    let mut out = Vec::with_capacity(16);
    out.extend_from_slice(&value.start.to_le_bytes());
    out.extend_from_slice(&value.end.to_le_bytes());
    Ok(out)
}
fn decode_sync_params(bytes: &[u8]) -> Result<SyncParams, RaftError> {
    Ok(SyncParams {
        start: u64::from_le_bytes(bytes[0..8].try_into().unwrap()),
        end: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
    })
}
fn encode_sync_ack(value: &SyncAck) -> Result<Vec<u8>, RaftError> {
    let mut out = Vec::with_capacity(8);
    out.extend_from_slice(&value.saved_until.to_le_bytes());
    Ok(out)
}
fn decode_sync_ack(bytes: &[u8]) -> Result<SyncAck, RaftError> {
    Ok(SyncAck {
        saved_until: u64::from_le_bytes(bytes[0..8].try_into().unwrap()),
    })
}

fn sync_spec() -> DispatchSpec<SyncParams, SyncAck> {
    DispatchSpec::new(0x21, encode_sync_params, decode_sync_params, encode_sync_ack, decode_sync_ack)
}

// ---------------------------------------------------------------------------
// Minimal MemStorage
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct NullStorage;
impl RaftStorage for NullStorage {
    fn load_hard_state(&self) -> Result<HardState, RaftError> { Ok(HardState::default()) }
    fn save_hard_state(&self, _s: &HardState) -> Result<(), RaftError> { Ok(()) }
    fn append_entries(&self, _e: &[LogEntry<'_>]) -> Result<(), RaftError> { Ok(()) }
    fn read_entries<'a>(&self, _f: LogIndex, _t: LogIndex, _o: &mut Vec<LogEntry<'a>>, _p: &'a mut [u8]) -> Result<usize, RaftError> { Ok(0) }
    fn truncate_suffix(&self, _f: LogIndex) -> Result<(), RaftError> { Ok(()) }
    fn save_snapshot(&self, _m: &SnapshotMeta, _s: &[u8]) -> Result<(), RaftError> { Ok(()) }
    fn load_snapshot(&self) -> Result<Option<(SnapshotMeta, Vec<u8>)>, RaftError> { Ok(None) }
    fn last_log_position(&self) -> Result<(LogIndex, Term), RaftError> { Ok((LogIndex(0), Term(0))) }
    fn entry_at<'a>(&self, _i: LogIndex, _p: &'a mut [u8]) -> Result<Option<LogEntry<'a>>, RaftError> { Ok(None) }
}

// ---------------------------------------------------------------------------
// Minimal Transport to measure Fan-out
// ---------------------------------------------------------------------------

struct NullTransport;
impl RaftTransport for NullTransport {
    fn send_vectored(&self, _p: PeerId, _s: &[&[u8]]) -> impl std::future::Future<Output = Result<(), RaftError>> + Send {
        async move { Ok(()) }
    }
    fn send_frame_owned(&self, _p: PeerId, _f: bytes::Bytes) -> impl std::future::Future<Output = Result<(), RaftError>> + Send {
        async move { Ok(()) }
    }
    fn recv_frame(&self, _o: &mut [u8]) -> impl std::future::Future<Output = Result<usize, RaftError>> + Send {
        async move { loop { tokio::task::yield_now().await; } }
    }
    fn recv_frame_timeout(&self, _t: Duration, _o: &mut [u8]) -> impl std::future::Future<Output = Result<Option<usize>, RaftError>> + Send {
        async move { Ok(None) }
    }
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

fn make_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
}

fn bench_dispatch_orchestration(c: &mut Criterion) {
    let mut group = c.benchmark_group("dispatch_orchestration");
    group.sample_size(100);
    let rt = make_runtime();
    let spec = sync_spec();

    // Measure orchestration cost for 3 peers, 10 peers, and 100 peers.
    // This highlights the O(1) nature of the parallel implementation vs sequential.
    for &num_peers in &[3, 10, 100] {
        group.throughput(Throughput::Elements(1));
        group.bench_function(format!("peers_{num_peers}"), |b| {
            b.to_async(&rt).iter_custom(|iters| {
                let spec = spec.clone();
                async move {
                    let mut config = NodeConfig::default();
                    config.node_id = PeerId(1);
                    config.peers = (1..=num_peers).map(PeerId).collect();
                    config.bootstrap_peers = vec![arbitro_raft::BootstrapPeer {
                        id: PeerId(1),
                        addr: "127.0.0.1:8001".parse().unwrap(),
                    }];
                    
                    let mut node = RaftNode::new(config, NullStorage, NullTransport).unwrap();
                    node.become_leader_for_benchmark(Term(1));
                    
                    // Register local handler because we are in the target list (PeerId 1)
                    node.on_with(spec.clone(), |_params, _ctx| {
                        Box::pin(async move { Ok(()) })
                    }).unwrap();

                    let params = SyncParams { start: 10, end: 20 };
                    let start = Instant::now();
                    for _ in 0..iters {
                        // We ONLY measure the call to dispatch(), which performs the parallel fan-out.
                        // We don't await the result because in this benchmark we don't have responders.
                        let _ = node.dispatch(spec.clone(), params).await.unwrap();
                    }
                    start.elapsed()
                }
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_dispatch_orchestration);
criterion_main!(benches);
