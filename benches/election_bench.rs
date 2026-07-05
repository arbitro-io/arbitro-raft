// ARBITRO RAFT — TCP E2E ELECTION BENCHMARK
//
// Measures election convergence time and re-election times under real TCP loopback.

use std::collections::HashMap;
use std::io::IoSlice;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arbitro_raft::{
    ArbitroRaft, BootstrapPeer, ClientHandle, ClusterId, EntryPayload, HardState, LimitsConfig,
    LogEntry, LogIndex, NodeConfig, PeerId, RaftError, RaftStorage, RaftTransport, Role,
    SnapshotMeta, Term, TimingConfig,
};
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

#[derive(Clone, Default)]
struct MemStorage {
    hard_state: Arc<Mutex<HardState>>,
    log: Arc<Mutex<Vec<(LogIndex, Term)>>>,
}

impl RaftStorage for MemStorage {
    fn load_hard_state(&self) -> Result<HardState, RaftError> {
        Ok(self.hard_state.lock().unwrap().clone())
    }
    fn save_hard_state(&self, state: &HardState) -> Result<(), RaftError> {
        *self.hard_state.lock().unwrap() = state.clone();
        Ok(())
    }
    fn append_entries(&self, new_entries: &[LogEntry<'_>]) -> Result<(), RaftError> {
        let mut log = self.log.lock().unwrap();
        for e in new_entries {
            log.push((e.index, e.term));
        }
        Ok(())
    }
    fn read_entries<'a>(
        &self,
        from: LogIndex,
        to: LogIndex,
        out: &mut Vec<LogEntry<'a>>,
        _p: &'a mut [u8],
    ) -> Result<usize, RaftError> {
        let log = self.log.lock().unwrap();
        for &(idx, term) in log.iter() {
            if idx >= from && idx < to {
                out.push(LogEntry {
                    term,
                    index: idx,
                    payload: EntryPayload(&[]),
                });
            }
        }
        Ok(0)
    }
    fn truncate_suffix(&self, from: LogIndex) -> Result<(), RaftError> {
        self.log.lock().unwrap().retain(|&(idx, _)| idx < from);
        Ok(())
    }
    fn save_snapshot(&self, _m: &SnapshotMeta, _s: &[u8]) -> Result<(), RaftError> {
        Ok(())
    }
    fn load_snapshot(&self) -> Result<Option<(SnapshotMeta, Vec<u8>)>, RaftError> {
        Ok(None)
    }
    fn last_log_position(&self) -> Result<(LogIndex, Term), RaftError> {
        Ok(self
            .log
            .lock()
            .unwrap()
            .last()
            .copied()
            .unwrap_or((LogIndex(0), Term(0))))
    }
    fn entry_at<'a>(
        &self,
        index: LogIndex,
        _p: &'a mut [u8],
    ) -> Result<Option<LogEntry<'a>>, RaftError> {
        let log = self.log.lock().unwrap();
        if let Some(&(_, term)) = log.iter().find(|&&(idx, _)| idx == index) {
            Ok(Some(LogEntry {
                term,
                index,
                payload: EntryPayload(&[]),
            }))
        } else {
            Ok(None)
        }
    }
}

struct TcpTransport {
    inbound_rx: Arc<tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>>>,
    peer_addrs: Arc<HashMap<PeerId, SocketAddr>>,
    connections: Arc<tokio::sync::Mutex<HashMap<PeerId, Arc<tokio::sync::Mutex<TcpStream>>>>>,
}

impl RaftTransport for TcpTransport {
    fn send_vectored(
        &self,
        peer: PeerId,
        slices: &[&[u8]],
    ) -> impl std::future::Future<Output = Result<(), RaftError>> + Send {
        let peer_addrs = self.peer_addrs.clone();
        let connections = self.connections.clone();
        let slices_static =
            unsafe { std::mem::transmute::<&[&[u8]], &'static [&'static [u8]]>(slices) };

        async move {
            let addr = *peer_addrs
                .get(&peer)
                .ok_or_else(|| RaftError::Transport(format!("unknown peer {:?}", peer)))?;
            let stream = {
                let mut conns = connections.lock().await;
                if let Some(s) = conns.get(&peer) {
                    s.clone()
                } else {
                    let s = TcpStream::connect(addr)
                        .await
                        .map_err(|e| RaftError::Transport(e.to_string()))?;
                    let s = Arc::new(tokio::sync::Mutex::new(s));
                    conns.insert(peer, s.clone());
                    s
                }
            };
            let mut s = stream.lock().await;
            let mut io_bufs: Vec<IoSlice<'_>> =
                slices_static.iter().map(|s| IoSlice::new(s)).collect();
            let write_fut = async {
                let mut bufs: &mut [IoSlice<'_>] = &mut io_bufs;
                while !bufs.is_empty() {
                    let n = s.write_vectored(bufs).await?;
                    if n == 0 {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::WriteZero,
                            "write_vectored returned 0",
                        ));
                    }
                    IoSlice::advance_slices(&mut bufs, n);
                }
                Ok::<(), std::io::Error>(())
            };
            match tokio::time::timeout(Duration::from_millis(50), write_fut).await {
                Ok(Ok(())) => Ok(()),
                _ => {
                    connections.lock().await.remove(&peer);
                    Err(RaftError::Transport("write timeout or error".into()))
                }
            }
        }
    }

    fn send_frame_owned(
        &self,
        peer: PeerId,
        frame: bytes::Bytes,
    ) -> impl std::future::Future<Output = Result<(), RaftError>> + Send {
        let peer_addrs = self.peer_addrs.clone();
        let connections = self.connections.clone();
        async move {
            let addr = *peer_addrs
                .get(&peer)
                .ok_or_else(|| RaftError::Transport(format!("unknown peer {:?}", peer)))?;
            let stream = {
                let mut conns = connections.lock().await;
                if let Some(s) = conns.get(&peer) {
                    s.clone()
                } else {
                    let s = TcpStream::connect(addr)
                        .await
                        .map_err(|e| RaftError::Transport(e.to_string()))?;
                    let s = Arc::new(tokio::sync::Mutex::new(s));
                    conns.insert(peer, s.clone());
                    s
                }
            };
            let mut s = stream.lock().await;
            let write_fut = s.write_all(&frame);
            match tokio::time::timeout(Duration::from_millis(50), write_fut).await {
                Ok(Ok(())) => Ok(()),
                _ => {
                    connections.lock().await.remove(&peer);
                    Err(RaftError::Transport("write timeout or error".into()))
                }
            }
        }
    }

    fn recv_frame(
        &self,
        out: &mut [u8],
    ) -> impl std::future::Future<Output = Result<usize, RaftError>> + Send {
        let rx = self.inbound_rx.clone();
        async move {
            let mut rx = rx.lock().await;
            if let Some(frame) = rx.recv().await {
                let len = frame.len();
                if out.len() < len {
                    return Err(RaftError::Transport("buffer too small".into()));
                }
                out[..len].copy_from_slice(&frame);
                Ok(len)
            } else {
                Err(RaftError::Transport("closed".into()))
            }
        }
    }

    fn recv_frame_timeout(
        &self,
        timeout: Duration,
        out: &mut [u8],
    ) -> impl std::future::Future<Output = Result<Option<usize>, RaftError>> + Send {
        let rx = self.inbound_rx.clone();
        async move {
            let mut rx = rx.lock().await;
            if timeout.is_zero() {
                if let Ok(frame) = rx.try_recv() {
                    let len = frame.len();
                    if out.len() < len {
                        return Err(RaftError::Transport("buffer too small".into()));
                    }
                    out[..len].copy_from_slice(&frame);
                    return Ok(Some(len));
                }
                return Ok(None);
            }
            match tokio::time::timeout(timeout, rx.recv()).await {
                Ok(Some(frame)) => {
                    let len = frame.len();
                    if out.len() < len {
                        return Err(RaftError::Transport("buffer too small".into()));
                    }
                    out[..len].copy_from_slice(&frame);
                    Ok(Some(len))
                }
                _ => Ok(None),
            }
        }
    }
}

const HEADER_SIZE: usize = 32;
const BODY_LEN_OFFSET: usize = 16;

async fn read_frame_into(stream: &mut TcpStream, buf: &mut Vec<u8>) -> Option<Vec<u8>> {
    loop {
        if buf.len() >= HEADER_SIZE {
            let body_len = u32::from_le_bytes(
                buf[BODY_LEN_OFFSET..BODY_LEN_OFFSET + 4]
                    .try_into()
                    .unwrap(),
            ) as usize;
            let total = HEADER_SIZE + body_len;
            if buf.len() >= total {
                let frame = buf[..total].to_vec();
                buf.drain(..total);
                return Some(frame);
            }
        }
        let mut tmp = [0u8; 4096];
        let n = stream.read(&mut tmp).await.unwrap_or(0);
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&tmp[..n]);
    }
}

async fn run_accept_loop(
    listener: TcpListener,
    inbound_tx: tokio::sync::mpsc::UnboundedSender<Vec<u8>>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            _ = shutdown.changed() => break,
            conn_res = listener.accept() => {
                if let Ok((mut stream, _)) = conn_res {
                    let tx = inbound_tx.clone();
                    let mut shutdown_conn = shutdown.clone();
                    tokio::spawn(async move {
                        let mut buf = Vec::with_capacity(65536);
                        loop {
                            tokio::select! {
                                _ = shutdown_conn.changed() => break,
                                frame_opt = read_frame_into(&mut stream, &mut buf) => {
                                    if let Some(frame) = frame_opt {
                                        if tx.send(frame).is_err() { break; }
                                    } else {
                                        break;
                                    }
                                }
                            }
                        }
                    });
                }
            }
        }
    }
}

struct NodeInstance {
    role: Arc<Mutex<Role>>,
    raft_handle: ClientHandle,
    run_task: JoinHandle<()>,
    accept_task: JoinHandle<()>,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    connections: Arc<tokio::sync::Mutex<HashMap<PeerId, Arc<tokio::sync::Mutex<TcpStream>>>>>,
}

impl NodeInstance {
    fn stop(self) {
        let _ = self.shutdown_tx.send(true);
        self.run_task.abort();
        self.accept_task.abort();
        let conns = self.connections.clone();
        tokio::spawn(async move {
            let mut lock = conns.lock().await;
            lock.clear();
        });
    }
}

async fn spawn_node(
    my_id: PeerId,
    all_peers: Vec<PeerId>,
    addrs: HashMap<PeerId, SocketAddr>,
    listener: TcpListener,
    election_min_ms: u64,
    election_max_ms: u64,
) -> NodeInstance {
    let (inbound_tx, inbound_rx) = tokio::sync::mpsc::unbounded_channel();
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let accept_task = tokio::spawn(run_accept_loop(listener, inbound_tx, shutdown_rx));
    let connections = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
    let transport = TcpTransport {
        inbound_rx: Arc::new(tokio::sync::Mutex::new(inbound_rx)),
        peer_addrs: Arc::new(addrs.clone()),
        connections: connections.clone(),
    };
    let config = NodeConfig {
        node_id: my_id,
        cluster_id: ClusterId(1),
        peers: all_peers.clone(),
        bootstrap_peers: addrs
            .iter()
            .map(|(&id, &addr)| BootstrapPeer { id, addr })
            .collect(),
        timing: TimingConfig {
            heartbeat_ms: 10,
            election_min_ms,
            election_max_ms,
        },
        limits: LimitsConfig::default(),
    };
    let storage = MemStorage::default();
    let node = arbitro_raft::RaftNode::new(config, storage, transport).unwrap();
    let mut raft = ArbitroRaft::new(node);
    let raft_handle = raft.client_handle();
    let role = Arc::new(Mutex::new(Role::Follower));
    let role_clone = role.clone();
    let run_task = tokio::spawn(async move {
        loop {
            match raft.run_once().await {
                Ok(true) => {
                    let mut lock = role_clone.lock().unwrap();
                    *lock = raft.role();
                }
                _ => break,
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    });
    NodeInstance {
        role,
        raft_handle,
        run_task,
        accept_task,
        shutdown_tx,
        connections,
    }
}

fn make_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap()
}

fn bench_tcp_election(c: &mut Criterion) {
    let mut group = c.benchmark_group("raft_tcp_election");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(5));
    let rt = make_runtime();

    for &num_nodes in &[3, 5] {
        group.throughput(Throughput::Elements(1));
        group.bench_function(format!("{num_nodes}node_cold_start"), |b| {
            b.to_async(&rt).iter_custom(|iters| async move {
                let mut total_duration = Duration::ZERO;
                let peers: Vec<PeerId> = (1..=num_nodes).map(|i| PeerId(i as u64)).collect();
                for _ in 0..iters {
                    let mut listeners = HashMap::new();
                    let mut addrs = HashMap::new();
                    for &id in &peers {
                        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
                        addrs.insert(id, l.local_addr().unwrap());
                        listeners.insert(id, l);
                    }
                    let start = Instant::now();
                    let mut nodes = Vec::new();
                    for &id in &peers {
                        let l = listeners.remove(&id).unwrap();
                        nodes.push(spawn_node(id, peers.clone(), addrs.clone(), l, 50, 100).await);
                    }
                    loop {
                        let mut has_leader = false;
                        for node in &nodes {
                            if *node.role.lock().unwrap() == Role::Leader {
                                has_leader = true;
                                break;
                            }
                        }
                        if has_leader {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                    total_duration += start.elapsed();
                    for n in nodes {
                        n.stop();
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                total_duration
            });
        });
    }
    group.finish();
}

fn bench_tcp_election_reelection(c: &mut Criterion) {
    let mut group = c.benchmark_group("raft_tcp_reelection");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(5));
    let rt = make_runtime();

    group.throughput(Throughput::Elements(1));
    group.bench_function("leader_failover_3node", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let mut total_duration = Duration::ZERO;
            let peers = vec![PeerId(1), PeerId(2), PeerId(3)];
            for _ in 0..iters {
                let mut listeners = HashMap::new();
                let mut addrs = HashMap::new();
                for &id in &peers {
                    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    addrs.insert(id, l.local_addr().unwrap());
                    listeners.insert(id, l);
                }
                let mut nodes = Vec::new();
                for &id in &peers {
                    let l = listeners.remove(&id).unwrap();
                    nodes.push(spawn_node(id, peers.clone(), addrs.clone(), l, 50, 100).await);
                }
                let mut leader_idx = 0;
                loop {
                    let mut found = false;
                    for (idx, node) in nodes.iter().enumerate() {
                        if *node.role.lock().unwrap() == Role::Leader {
                            leader_idx = idx;
                            found = true;
                            break;
                        }
                    }
                    if found {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                let start = Instant::now();
                let leader_node = nodes.remove(leader_idx);
                leader_node.stop();
                loop {
                    let mut has_leader = false;
                    for node in &nodes {
                        if *node.role.lock().unwrap() == Role::Leader {
                            has_leader = true;
                            break;
                        }
                    }
                    if has_leader {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                total_duration += start.elapsed();
                for n in nodes {
                    n.stop();
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            total_duration
        });
    });
    group.finish();
}

fn bench_tcp_election_under_load(c: &mut Criterion) {
    let mut group = c.benchmark_group("raft_tcp_reelection_under_load");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(5));
    let rt = make_runtime();

    group.throughput(Throughput::Elements(1));
    group.bench_function("reelect_under_load_3node", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let mut total_duration = Duration::ZERO;
            let peers = vec![PeerId(1), PeerId(2), PeerId(3)];
            for _ in 0..iters {
                let mut listeners = HashMap::new();
                let mut addrs = HashMap::new();
                for &id in &peers {
                    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    addrs.insert(id, l.local_addr().unwrap());
                    listeners.insert(id, l);
                }
                let mut nodes = Vec::new();
                for &id in &peers {
                    let l = listeners.remove(&id).unwrap();
                    nodes.push(spawn_node(id, peers.clone(), addrs.clone(), l, 50, 100).await);
                }
                let mut leader_idx = 0;
                loop {
                    let mut found = false;
                    for (idx, node) in nodes.iter().enumerate() {
                        if *node.role.lock().unwrap() == Role::Leader {
                            leader_idx = idx;
                            found = true;
                            break;
                        }
                    }
                    if found {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                let handle = nodes[leader_idx].raft_handle.clone();
                let load_handle = tokio::spawn(async move {
                    let payload = vec![0xBB; 256];
                    loop {
                        let _ = handle.write(&payload).await;
                        tokio::time::sleep(Duration::from_millis(2)).await;
                    }
                });
                tokio::time::sleep(Duration::from_millis(50)).await;
                let start = Instant::now();
                let leader_node = nodes.remove(leader_idx);
                leader_node.stop();
                load_handle.abort();
                loop {
                    let mut has_leader = false;
                    for node in &nodes {
                        if *node.role.lock().unwrap() == Role::Leader {
                            has_leader = true;
                            break;
                        }
                    }
                    if has_leader {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                total_duration += start.elapsed();
                for n in nodes {
                    n.stop();
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            total_duration
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_tcp_election,
    bench_tcp_election_reelection,
    bench_tcp_election_under_load
);
criterion_main!(benches);
