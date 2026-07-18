// ARBITRO RAFT — UNIFIED TRANSPORT-PARITY BENCHMARK
//
// One benchmark core, two transports. The ONLY variable between the `inmem`
// and `tcp` results is the byte-moving primitive:
//
//   inmem : tokio unbounded MPSC channels (frames as owned Vec<u8>)
//   tcp   : real TCP loopback sockets (127.0.0.1, TCP_NODELAY)
//
// Everything else is IDENTICAL across both variants:
//
//   * Storage        — one `BenchStorage` implementation (Vec-backed, no
//                      unsafe, no benchmark-only lifetime tricks) used by the
//                      leader in both variants.
//   * Follower model — followers are REAL tokio tasks in both variants. Each
//                      one blocks on its receive primitive, decodes the frame,
//                      runs the same `follower_handle_frame` (prev_log check,
//                      conflict truncation, entry append), encodes the ack and
//                      pushes it back through its send primitive. There is NO
//                      inline-synchronous follower: every propose round-trip
//                      pays real tokio task wakeups and cross-task scheduling
//                      in BOTH variants.
//   * Leader recv    — one shared `Inbound` implementation (async channel
//                      recv + tokio::time::timeout). No try_recv spin loops.
//   * Propose path   — `propose_once` / `propose_batch_once`, which block
//                      until the entry actually commits under quorum. Nothing
//                      is measured that is not a full consensus round.
//   * Copy budget    — each send materializes the wire frame exactly once:
//                      channel = one Vec<u8> materialization; TCP = one
//                      user->kernel copy (writev / write_all). Neither side
//                      gets a zero-copy shortcut the other lacks.
//   * Cluster shape  — 3 nodes (leader + 2 followers), quorum = 2.
//   * Leadership     — `become_leader_for_benchmark(Term(1))` in both (skips
//                      election; numbers exclude election/heartbeat traffic).
//   * Criterion cfg  — same sample size / warmup / measurement for both.
//
// Known, documented asymmetry: the TCP leader needs a socket accept/demux
// task that reframes inbound bytes before they reach the inbound channel.
// That task is inherent to socket-based transports (something must own the
// connection and split the byte stream into frames) and is part of what
// "using TCP" costs; the channel variant hands whole frames over directly.
//
// Run (full matrix, both transports):
//   cargo bench --bench raft_transport_bench
// Only one transport (env var or criterion filter, both work):
//   ARBITRO_BENCH_TRANSPORT=inmem cargo bench --bench raft_transport_bench
//   ARBITRO_BENCH_TRANSPORT=tcp   cargo bench --bench raft_transport_bench
//   cargo bench --bench raft_transport_bench -- 'inmem/'
//   cargo bench --bench raft_transport_bench -- 'tcp/'
// Criterion knobs (defaults keep the FULL non-durable run at a few minutes):
//   ARBITRO_BENCH_SAMPLE_SIZE (default 10)
//   ARBITRO_BENCH_WARMUP_MS   (default 1000; auto 3000 in durable mode)
//   ARBITRO_BENCH_MEASURE_MS  (default 3000; auto 20000 in durable mode)
//
// Durable (fsync) mode — K3, see docs/BENCH_METHODOLOGY_K3.md. Off by default;
// when off every path and Criterion id above is byte-identical to before:
//   ARBITRO_BENCH_FSYNC        none|nosync|data|full   (default none)
//   ARBITRO_BENCH_FSYNC_SCOPE  quorum|leader           (default quorum)
//   ARBITRO_BENCH_WAL_DIR      native-ext4 path        (default $HOME/.cache/…)
//   ARBITRO_BENCH_WAL_SEGMENT_MIB                       (default 16)
// The leader (always) and, under scope=quorum, each follower write real entry
// bytes to a preallocated per-node WAL and fdatasync/fsync before the ack. The
// batch axis then reads as: x1 = one fsync per commit, x1024 = group-commit.
// Example (durable, tcp only, headline data+quorum):
//   ARBITRO_BENCH_TRANSPORT=tcp ARBITRO_BENCH_FSYNC=data \
//     cargo bench --bench raft_transport_bench

use std::collections::{HashMap, HashSet};
use std::io::{IoSlice, Seek, SeekFrom, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use arbitro_raft::protocol::{
    AppendEntries, AppendEntriesEntryIter, AppendEntriesRawIter, AppendEntriesResp, RaftMessage,
    SeededPayloads,
};
use arbitro_raft::{
    decode_message, encode_message_vectored, ArbitroRaft, BootstrapPeer, ClusterId, EntryPayload,
    HardState, LogEntry, LogIndex, NodeConfig, PeerId, RaftError, RaftNode, RaftStorage,
    RaftTransport, SnapshotMeta, StateMachine, Term, TimingConfig,
};
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle;

// ---------------------------------------------------------------------------
// Durable WAL (K3) — real fsync cost for the fsync-mode bench.
//
// Off by default: with `ARBITRO_BENCH_FSYNC=none` (or unset) every code path
// and Criterion id below is byte-identical to the non-durable bench. When a
// durable mode is selected, the leader and (scope=quorum) each follower write
// REAL record bytes of every measured entry to a per-node preallocated WAL
// segment and fsync BEFORE the ack Raft's safety depends on. A fsync of an
// unwritten file is structurally impossible: a ~25-byte record precedes every
// barrier. Reads stay in RAM (WAL + RAM-index, the standard architecture); the
// WAL is never read back — this prices durability, it does not verify crash
// recovery (that is `tests/storage_faults.rs`). See docs/BENCH_METHODOLOGY_K3.md.
// ---------------------------------------------------------------------------

const ENTRY_KIND: u8 = 0x01;
const HARDSTATE_KIND: u8 = 0x02;

#[derive(Clone, Copy, PartialEq, Eq)]
enum FsyncPolicy {
    None,
    NoSync,
    Data,
    Full,
}

impl FsyncPolicy {
    fn from_env() -> Self {
        match std::env::var("ARBITRO_BENCH_FSYNC").ok().as_deref() {
            Some("nosync") => FsyncPolicy::NoSync,
            Some("data") => FsyncPolicy::Data,
            Some("full") => FsyncPolicy::Full,
            _ => FsyncPolicy::None,
        }
    }
    fn tag(self) -> &'static str {
        match self {
            FsyncPolicy::None => "none",
            FsyncPolicy::NoSync => "nosync",
            FsyncPolicy::Data => "data",
            FsyncPolicy::Full => "full",
        }
    }
    fn sync_call(self) -> &'static str {
        match self {
            FsyncPolicy::Data => "fdatasync",
            FsyncPolicy::Full => "fsync",
            _ => "none",
        }
    }
    /// A WAL is written for `NoSync`/`Data`/`Full`; only `None` bypasses it.
    fn writes_wal(self) -> bool {
        !matches!(self, FsyncPolicy::None)
    }
    /// Durable = a real barrier is issued (`Data`/`Full`). `NoSync` writes but
    /// does not sync (diagnostic only) and `None` does neither.
    fn is_durable(self) -> bool {
        matches!(self, FsyncPolicy::Data | FsyncPolicy::Full)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum FsyncScope {
    Quorum,
    Leader,
}

impl FsyncScope {
    fn from_env() -> Self {
        match std::env::var("ARBITRO_BENCH_FSYNC_SCOPE").ok().as_deref() {
            Some("leader") => FsyncScope::Leader,
            _ => FsyncScope::Quorum,
        }
    }
    fn tag(self) -> &'static str {
        match self {
            FsyncScope::Quorum => "quorum",
            FsyncScope::Leader => "leader",
        }
    }
}

#[derive(Clone)]
struct WalConfig {
    policy: FsyncPolicy,
    scope: FsyncScope,
    dir: PathBuf,
    segment_bytes: u64,
}

impl WalConfig {
    fn from_env() -> Self {
        let dir = std::env::var("ARBITRO_BENCH_WAL_DIR")
            .ok()
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
                PathBuf::from(home).join(".cache/arbitro-raft-bench/wal")
            });
        let segment_bytes = env_u64("ARBITRO_BENCH_WAL_SEGMENT_MIB", 16) * 1024 * 1024;
        WalConfig {
            policy: FsyncPolicy::from_env(),
            scope: FsyncScope::from_env(),
            dir,
            segment_bytes,
        }
    }

    /// Followers write+sync only under `scope=quorum`; `scope=leader` measures
    /// a leader-only-durable decomposition cell (comparable to no target).
    fn follower_policy(&self) -> FsyncPolicy {
        match self.scope {
            FsyncScope::Quorum => self.policy,
            FsyncScope::Leader => FsyncPolicy::None,
        }
    }
}

/// Process-wide config, parsed once (env is constant within a run).
fn wal_config() -> &'static WalConfig {
    static CFG: OnceLock<WalConfig> = OnceLock::new();
    CFG.get_or_init(WalConfig::from_env)
}

/// Per-process registry of already-preallocated segments so the steady-state
/// fdatasync never re-pays first-write extent/journal mapping across samples.
fn prealloc_registry() -> &'static Mutex<HashSet<PathBuf>> {
    static REG: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();
    REG.get_or_init(|| Mutex::new(HashSet::new()))
}

fn ensure_prealloc(path: &Path, segment_bytes: u64) {
    let mut reg = prealloc_registry().lock().unwrap();
    if reg.contains(path) {
        return;
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("create wal dir");
    }
    let mut f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .expect("create wal segment");
    // Zero-fill to force real extents: set_len alone leaves a sparse file and
    // reintroduces allocation metadata on first touch (overcharges fdatasync).
    let zeros = vec![0u8; 1024 * 1024];
    let mut written = 0u64;
    while written < segment_bytes {
        let n = ((segment_bytes - written) as usize).min(zeros.len());
        f.write_all(&zeros[..n]).expect("zero-fill wal");
        written += n as u64;
    }
    f.sync_all().expect("prealloc sync");
    reg.insert(path.to_path_buf());
}

/// One append-only WAL segment for a single node. Owned single-threaded by a
/// follower's `PeerState`; shared behind a `Mutex` by the leader's storage.
struct WalWriter {
    file: std::fs::File,
    offset: u64,
    segment_bytes: u64,
    policy: FsyncPolicy,
    scratch: Vec<u8>,
}

impl WalWriter {
    fn open(dir: &Path, segment_bytes: u64, policy: FsyncPolicy, node_id: u64) -> Self {
        let path = dir.join(format!("node{node_id}.wal"));
        ensure_prealloc(&path, segment_bytes);
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("open preallocated wal segment");
        WalWriter {
            file,
            offset: 0,
            segment_bytes,
            policy,
            scratch: Vec::with_capacity(64 * 1024),
        }
    }

    /// Record layout: len:u32le | crc32:u32le | kind:u8 | a:u64le | b:u64le | payload.
    /// CRC covers kind|a|b|payload. Overwrites in place; wraps at segment end.
    fn append_record(&mut self, kind: u8, a: u64, b: u64, payload: &[u8]) {
        self.scratch.clear();
        let rec_len = (1 + 8 + 8 + payload.len()) as u32; // kind + a + b + payload
        self.scratch.extend_from_slice(&rec_len.to_le_bytes());
        let crc_pos = self.scratch.len();
        self.scratch.extend_from_slice(&[0u8; 4]);
        let crc_start = self.scratch.len();
        self.scratch.push(kind);
        self.scratch.extend_from_slice(&a.to_le_bytes());
        self.scratch.extend_from_slice(&b.to_le_bytes());
        self.scratch.extend_from_slice(payload);
        let crc = crc32fast::hash(&self.scratch[crc_start..]);
        self.scratch[crc_pos..crc_pos + 4].copy_from_slice(&crc.to_le_bytes());

        let total = self.scratch.len() as u64;
        if self.offset + total > self.segment_bytes {
            self.offset = 0;
        }
        self.file
            .seek(SeekFrom::Start(self.offset))
            .expect("wal seek");
        self.file.write_all(&self.scratch).expect("wal write");
        self.offset += total;
    }

    fn sync(&self) {
        match self.policy {
            FsyncPolicy::Data => self.file.sync_data().expect("fdatasync"),
            FsyncPolicy::Full => self.file.sync_all().expect("fsync"),
            _ => {}
        }
    }
}

/// The mount filesystem backing `dir` (longest matching `/proc/mounts` prefix).
fn detect_fs(dir: &Path) -> String {
    let canon = std::fs::canonicalize(dir).unwrap_or_else(|_| dir.to_path_buf());
    let mounts = std::fs::read_to_string("/proc/mounts").unwrap_or_default();
    let mut best_mp = String::new();
    let mut best_fs = String::from("unknown");
    for line in mounts.lines() {
        let mut it = line.split_whitespace();
        let _dev = it.next();
        let Some(mp) = it.next() else { continue };
        let Some(fstype) = it.next() else { continue };
        if canon.starts_with(mp) && mp.len() >= best_mp.len() {
            best_mp = mp.to_string();
            best_fs = fstype.to_string();
        }
    }
    best_fs
}

/// 100× {overwrite 4 KiB at offset 0 + sync}. Returns (median, p95, max) µs.
/// Doubles as the anti-fake-barrier gate and the per-run disk normalizer.
fn calibration_probe(cfg: &WalConfig) -> (f64, f64, f64) {
    let path = cfg.dir.join("calib.probe");
    {
        // Small dedicated preallocated probe file.
        let mut f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .expect("create probe");
        f.write_all(&[0u8; 4096]).expect("probe prealloc");
        f.sync_all().expect("probe sync");
    }
    let mut f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .expect("open probe");
    let mut buf = [0x5Au8; 4096];
    let mut samples = Vec::with_capacity(100);
    for i in 0..100u64 {
        buf[0] = i as u8;
        let t = Instant::now();
        f.seek(SeekFrom::Start(0)).unwrap();
        f.write_all(&buf).unwrap();
        match cfg.policy {
            FsyncPolicy::Data => f.sync_data().unwrap(),
            FsyncPolicy::Full => f.sync_all().unwrap(),
            _ => {}
        }
        samples.push(t.elapsed().as_nanos() as f64 / 1000.0);
    }
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let med = samples[samples.len() / 2];
    let p95 = samples[(samples.len() as f64 * 0.95) as usize];
    let max = *samples.last().unwrap();
    (med, p95, max)
}

/// Run-once: refuse fake filesystems, gate on a real barrier, print the
/// mandatory disclosure line. No-op unless a durable mode is selected.
fn durable_startup() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        let cfg = wal_config();
        if !cfg.policy.writes_wal() {
            return;
        }
        std::fs::create_dir_all(&cfg.dir).expect("create wal dir");
        let fs = detect_fs(&cfg.dir);
        if matches!(fs.as_str(), "tmpfs" | "ramfs" | "9p" | "overlay") {
            panic!(
                "ARBITRO_BENCH_WAL_DIR ({}) is on '{fs}' — not a real disk. \
                 A durable bench there measures nothing. Point ARBITRO_BENCH_WAL_DIR at native ext4.",
                cfg.dir.display()
            );
        }
        let probe = calibration_probe(cfg);
        if cfg.policy.is_durable() && probe.0 < 2.0 {
            panic!(
                "calibration probe median {:.2}µs < 2µs on '{fs}' — no real barrier is completing; \
                 refusing to publish a fake durable number.",
                probe.0
            );
        }
        println!(
            "BENCH_CONTEXT {{\"bench\":\"raft_transport_bench\",\"fsync\":\"{}\",\"scope\":\"{}\",\
\"sync_call\":\"{}\",\"wal_dir\":\"{}\",\"wal_fs\":\"{}\",\"segment_mib\":{},\
\"prealloc\":\"zero-filled+recycled\",\"probe_us\":{{\"median\":{:.2},\"p95\":{:.2},\"max\":{:.2}}},\
\"cluster\":\"3-node loopback, single host, single physical device\",\
\"completion\":\"commit(+same-tick-noop-apply)\",\
\"caveats\":\"WSL2 vhd (virtualized barrier, not power-loss proof); loopback; single-group; consensus-core-only\"}}",
            cfg.policy.tag(),
            cfg.scope.tag(),
            cfg.policy.sync_call(),
            cfg.dir.display(),
            fs,
            cfg.segment_bytes / 1024 / 1024,
            probe.0,
            probe.1,
            probe.2,
        );
    });
}

/// Criterion-id segment: empty for `none` (legacy ids preserved), else
/// `/fsync-<policy>` (or `/fsync-<policy>-leader` for scope=leader).
fn fsync_id_segment() -> String {
    let cfg = wal_config();
    if !cfg.policy.writes_wal() {
        return String::new();
    }
    match cfg.scope {
        FsyncScope::Quorum => format!("/fsync-{}", cfg.policy.tag()),
        FsyncScope::Leader => format!("/fsync-{}-leader", cfg.policy.tag()),
    }
}

// ---------------------------------------------------------------------------
// NoopSM — apply is a no-op. Identical in both variants; consensus numbers
// deliberately exclude state-machine apply cost.
// ---------------------------------------------------------------------------

struct NoopSM;
impl StateMachine for NoopSM {
    fn apply(&mut self, _entry: &[u8]) -> Result<(), RaftError> {
        Ok(())
    }
    fn snapshot(&self) -> Result<Vec<u8>, RaftError> {
        Ok(Vec::new())
    }
    fn restore(&mut self, _snapshot: &[u8]) -> Result<(), RaftError> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// BenchStorage — THE single storage implementation used by both variants.
// Vec-backed, O(1) index arithmetic, no unsafe, no 'static transmutes.
// `read_entry_headers` keeps the trait default (None), so the engine takes
// the SAME (ergonomic) replication path in both variants.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct StoredEntry {
    term: Term,
    index: LogIndex,
    payload: Vec<u8>,
}

#[derive(Clone)]
struct BenchStorage {
    base_index: Arc<AtomicU64>,
    entries: Arc<Mutex<Vec<StoredEntry>>>,
    hard_state: Arc<Mutex<HardState>>,
    // Leader WAL (node 1). `None` unless a durable/nosync mode is selected.
    wal: Option<Arc<Mutex<WalWriter>>>,
}

impl BenchStorage {
    fn new() -> Self {
        let cfg = wal_config();
        let wal = cfg.policy.writes_wal().then(|| {
            Arc::new(Mutex::new(WalWriter::open(
                &cfg.dir,
                cfg.segment_bytes,
                cfg.policy,
                1,
            )))
        });
        Self {
            base_index: Arc::new(AtomicU64::new(0)),
            entries: Arc::new(Mutex::new(Vec::new())),
            hard_state: Arc::new(Mutex::new(HardState {
                current_term: Term(1),
                voted_for: None,
            })),
            wal,
        }
    }
}

impl RaftStorage for BenchStorage {
    fn load_hard_state(&self) -> Result<HardState, RaftError> {
        Ok(self.hard_state.lock().unwrap().clone())
    }

    fn save_hard_state(&self, state: &HardState) -> Result<(), RaftError> {
        // Off the steady-state path (election/term transitions only); persisted
        // for contract completeness so the durable mode is honest end-to-end.
        if let Some(wal) = &self.wal {
            let voted = state.voted_for.map(|p| p.0).unwrap_or(0);
            let mut w = wal.lock().unwrap();
            w.append_record(HARDSTATE_KIND, state.current_term.0, voted, &[]);
            w.sync();
        }
        *self.hard_state.lock().unwrap() = state.clone();
        Ok(())
    }

    fn append_entries(&self, new_entries: &[LogEntry<'_>]) -> Result<(), RaftError> {
        if new_entries.is_empty() {
            return Ok(());
        }
        // Durability seam: write REAL entry bytes and fsync BEFORE the RAM push
        // and `Ok`. One sync per engine call = the group-commit granularity
        // C-D1 permits (x1 = one fsync/commit, x1024 = one fsync/1024 entries).
        if let Some(wal) = &self.wal {
            let mut w = wal.lock().unwrap();
            for e in new_entries {
                w.append_record(ENTRY_KIND, e.term.0, e.index.0, e.payload.0);
            }
            w.sync();
        }
        let mut entries = self.entries.lock().unwrap();
        if entries.is_empty() {
            self.base_index
                .store(new_entries[0].index.0, Ordering::Relaxed);
        }
        for e in new_entries {
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
        if entries.is_empty() {
            return Ok(0);
        }
        let base = self.base_index.load(Ordering::Relaxed);
        let start = (from.0.saturating_sub(base)) as usize;
        let end = ((to.0.saturating_sub(base)) as usize).min(entries.len());

        let mut written = 0usize;
        // Carve `payload_buf` into disjoint chunks with split_at_mut so each
        // pushed LogEntry borrows its own region — no lifetime transmutes.
        let mut buf: &'a mut [u8] = payload_buf;
        if start < end {
            for e in &entries[start..end] {
                let len = e.payload.len();
                if len > buf.len() {
                    return Err(RaftError::Storage("payload_buf too small".into()));
                }
                let (chunk, rest) = std::mem::take(&mut buf).split_at_mut(len);
                chunk.copy_from_slice(&e.payload);
                let chunk: &'a [u8] = chunk;
                out.push(LogEntry {
                    term: e.term,
                    index: e.index,
                    payload: EntryPayload(chunk),
                });
                buf = rest;
                written += len;
            }
        }
        Ok(written)
    }

    fn entry_at<'a>(
        &self,
        index: LogIndex,
        payload_buf: &'a mut [u8],
    ) -> Result<Option<LogEntry<'a>>, RaftError> {
        let entries = self.entries.lock().unwrap();
        if entries.is_empty() {
            return Ok(None);
        }
        let base = self.base_index.load(Ordering::Relaxed);
        if index.0 < base {
            return Ok(None);
        }
        let pos = (index.0 - base) as usize;
        let Some(e) = entries.get(pos) else {
            return Ok(None);
        };
        let len = e.payload.len();
        if payload_buf.len() < len {
            return Err(RaftError::Storage("payload_buf too small".into()));
        }
        payload_buf[..len].copy_from_slice(&e.payload);
        Ok(Some(LogEntry {
            term: e.term,
            index: e.index,
            payload: EntryPayload(&payload_buf[..len]),
        }))
    }

    fn last_log_position(&self) -> Result<(LogIndex, Term), RaftError> {
        let entries = self.entries.lock().unwrap();
        Ok(entries
            .last()
            .map(|e| (e.index, e.term))
            .unwrap_or((LogIndex(0), Term(0))))
    }

    fn truncate_suffix(&self, from: LogIndex) -> Result<(), RaftError> {
        let mut entries = self.entries.lock().unwrap();
        if entries.is_empty() {
            return Ok(());
        }
        let base = self.base_index.load(Ordering::Relaxed);
        let cut = (from.0.saturating_sub(base)) as usize;
        entries.truncate(cut);
        if entries.is_empty() {
            self.base_index.store(0, Ordering::Relaxed);
        }
        Ok(())
    }

    fn save_snapshot(&self, _meta: &SnapshotMeta, _snapshot: &[u8]) -> Result<(), RaftError> {
        Ok(())
    }

    fn load_snapshot(&self) -> Result<Option<(SnapshotMeta, Vec<u8>)>, RaftError> {
        Ok(None)
    }
}

// ---------------------------------------------------------------------------
// PeerState + follower_handle_frame — THE single follower model.
// decode -> prev_log check -> conflict truncation -> append -> encode ack.
// Both transports run exactly this function inside a real tokio task.
// ---------------------------------------------------------------------------

struct PeerState {
    base_index: u64,
    log: Vec<(LogIndex, Term)>,
    // Follower WAL. `None` unless scope=quorum AND a durable/nosync mode is on.
    // Owned single-threaded by the follower task — no lock needed.
    wal: Option<WalWriter>,
}

impl PeerState {
    fn new(node_id: u64) -> Self {
        let cfg = wal_config();
        let fp = cfg.follower_policy();
        let wal = fp
            .writes_wal()
            .then(|| WalWriter::open(&cfg.dir, cfg.segment_bytes, fp, node_id));
        Self {
            base_index: 0,
            log: Vec::new(),
            wal,
        }
    }

    fn wal_append(&mut self, term: Term, idx: LogIndex, payload: &[u8]) {
        if let Some(w) = &mut self.wal {
            w.append_record(ENTRY_KIND, term.0, idx.0, payload);
        }
    }

    fn wal_sync(&self) {
        if let Some(w) = &self.wal {
            w.sync();
        }
    }

    fn last_position(&self) -> (LogIndex, Term) {
        self.log.last().copied().unwrap_or((LogIndex(0), Term(0)))
    }

    fn term_at(&self, idx: LogIndex) -> Option<Term> {
        if self.log.is_empty() || idx.0 < self.base_index {
            return None;
        }
        let pos = (idx.0 - self.base_index) as usize;
        self.log
            .get(pos)
            .filter(|&&(i, _)| i == idx)
            .map(|&(_, t)| t)
    }

    fn process_append(&mut self, ae: &AppendEntries, body: &[u8]) -> (bool, LogIndex) {
        let prev_idx = LogIndex(ae.prev_log_index.get());
        let prev_term = Term(ae.prev_log_term.get());

        if prev_idx.0 > 0 {
            match self.term_at(prev_idx) {
                None => return (false, self.last_position().0),
                Some(t) if t != prev_term => return (false, self.last_position().0),
                _ => {}
            }
        }

        let iter = AppendEntriesEntryIter::new(body, ae.entry_count.get() as usize);
        let mut appended = false;
        for ev in iter {
            let idx = ev.index;
            let term = ev.term;
            let payload = ev.payload.0;
            if self.log.is_empty() {
                self.base_index = idx.0;
                self.wal_append(term, idx, payload);
                self.log.push((idx, term));
                appended = true;
                continue;
            }
            if idx.0 < self.base_index {
                continue;
            }
            let pos = (idx.0 - self.base_index) as usize;
            if pos < self.log.len() {
                let (_, et) = self.log[pos];
                if et == term {
                    continue;
                }
                self.log.truncate(pos);
                if self.log.is_empty() {
                    self.base_index = idx.0;
                }
            }
            self.wal_append(term, idx, payload);
            self.log.push((idx, term));
            appended = true;
        }

        // One barrier per AE frame — only when real bytes were written, so a
        // duplicate/heartbeat frame never issues a fsync-of-nothing.
        if appended {
            self.wal_sync();
        }
        (true, self.last_position().0)
    }
}

fn encode_resp_frame(my_id: PeerId, term: u64, success: bool, match_index: u64) -> Vec<u8> {
    let resp = AppendEntriesResp {
        term: term.into(),
        success: if success { 1 } else { 0 },
        match_index: match_index.into(),
        _pad: [0; 7],
    };
    let mut header_buf = [0u8; 128];
    let mut vectors = Vec::new();
    encode_message_vectored(
        my_id,
        &RaftMessage::AppendEntriesResp(&resp),
        &mut header_buf,
        &mut vectors,
    )
    .expect("encode AppendEntriesResp");
    let total: usize = vectors.iter().map(|v| v.len()).sum();
    let mut frame = Vec::with_capacity(total);
    for v in vectors {
        frame.extend_from_slice(v);
    }
    frame
}

/// One inbound frame -> optional ack frame. Shared verbatim by both follower
/// tasks so the processing model cannot diverge between transports.
fn follower_handle_frame(state: &mut PeerState, my_id: PeerId, frame: &[u8]) -> Option<Vec<u8>> {
    let inbound = decode_message(frame).ok()?;
    match inbound.message {
        RaftMessage::AppendEntries(ae, body) => {
            let (success, match_index) = state.process_append(ae, body);
            Some(encode_resp_frame(
                my_id,
                ae.term.get(),
                success,
                match_index.0,
            ))
        }
        RaftMessage::AppendEntriesSeeded {
            ae,
            headers,
            payloads,
        } => {
            let iter = AppendEntriesRawIter::new(headers, SeededPayloads::Contiguous(payloads));

            let mut success = true;
            let prev_idx = LogIndex(ae.prev_log_index.get());
            let prev_term = Term(ae.prev_log_term.get());
            if prev_idx.0 > 0 {
                match state.term_at(prev_idx) {
                    None => success = false,
                    Some(t) if t != prev_term => success = false,
                    _ => {}
                }
            }
            if success {
                let mut appended = false;
                for (h, payload) in iter {
                    let idx = LogIndex(h.index.get());
                    let term = Term(h.term.get());
                    state.wal_append(term, idx, payload);
                    state.log.push((idx, term));
                    appended = true;
                }
                if appended {
                    state.wal_sync();
                }
            }
            let match_index = state.last_position().0;
            Some(encode_resp_frame(
                my_id,
                ae.term.get(),
                success,
                match_index.0,
            ))
        }
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Follower tasks — same model, different byte-moving primitive.
// ---------------------------------------------------------------------------

/// In-memory follower: blocks on a tokio MPSC channel. Every leader->follower
/// frame is an owned Vec<u8> handed across tasks — the recv side ALWAYS goes
/// through the tokio scheduler (task wakeup), never an inline call.
async fn run_channel_follower(
    mut rx: UnboundedReceiver<Vec<u8>>,
    tx: UnboundedSender<Vec<u8>>,
    my_id: PeerId,
) {
    let mut state = PeerState::new(my_id.0);
    while let Some(frame) = rx.recv().await {
        if let Some(resp) = follower_handle_frame(&mut state, my_id, &frame) {
            if tx.send(resp).is_err() {
                return;
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

/// TCP follower: blocks on a real loopback socket. Same handler, same task
/// model — only the recv/send primitive differs.
async fn run_tcp_follower(listener: TcpListener, leader_addr: SocketAddr, my_id: PeerId) {
    let Ok(mut leader_conn) = TcpStream::connect(leader_addr).await else {
        return;
    };
    let _ = leader_conn.set_nodelay(true);
    let Ok((mut stream, _)) = listener.accept().await else {
        return;
    };
    let _ = stream.set_nodelay(true);

    let mut buf = Vec::with_capacity(64 * 1024);
    let mut state = PeerState::new(my_id.0);
    while let Some(frame) = read_frame_into(&mut stream, &mut buf).await {
        if let Some(resp) = follower_handle_frame(&mut state, my_id, &frame) {
            if leader_conn.write_all(&resp).await.is_err() {
                return;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Inbound — THE single leader-side receive implementation.
// Real async blocking recv (tokio scheduler wakeups), tokio::time::timeout for
// bounded waits, single non-blocking try_recv for zero-timeout burst drains.
// No spin loops in either variant.
// ---------------------------------------------------------------------------

struct Inbound {
    rx: tokio::sync::Mutex<UnboundedReceiver<Vec<u8>>>,
}

impl Inbound {
    fn new(rx: UnboundedReceiver<Vec<u8>>) -> Self {
        Self {
            rx: tokio::sync::Mutex::new(rx),
        }
    }

    fn copy_out(frame: &[u8], out: &mut [u8]) -> Result<usize, RaftError> {
        if out.len() < frame.len() {
            return Err(RaftError::Transport("recv buffer too small".into()));
        }
        out[..frame.len()].copy_from_slice(frame);
        Ok(frame.len())
    }

    async fn recv(&self, out: &mut [u8]) -> Result<usize, RaftError> {
        let mut rx = self.rx.lock().await;
        match rx.recv().await {
            Some(frame) => Self::copy_out(&frame, out),
            None => Err(RaftError::Transport("inbound channel closed".into())),
        }
    }

    async fn recv_timeout(
        &self,
        timeout: Duration,
        out: &mut [u8],
    ) -> Result<Option<usize>, RaftError> {
        let mut rx = self.rx.lock().await;
        if timeout.is_zero() {
            return match rx.try_recv() {
                Ok(frame) => Self::copy_out(&frame, out).map(Some),
                Err(TryRecvError::Empty) => Ok(None),
                Err(TryRecvError::Disconnected) => {
                    Err(RaftError::Transport("inbound channel closed".into()))
                }
            };
        }
        match tokio::time::timeout(timeout, rx.recv()).await {
            Ok(Some(frame)) => Self::copy_out(&frame, out).map(Some),
            Ok(None) => Err(RaftError::Transport("inbound channel closed".into())),
            Err(_) => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// ChannelTransport — in-memory byte-moving primitive.
// ---------------------------------------------------------------------------

struct ChannelTransport {
    inbound: Inbound,
    peers: HashMap<PeerId, UnboundedSender<Vec<u8>>>,
}

impl RaftTransport for ChannelTransport {
    fn send_vectored(
        &self,
        peer: PeerId,
        slices: &[&[u8]],
    ) -> impl std::future::Future<Output = Result<(), RaftError>> + Send {
        async move {
            let tx = self
                .peers
                .get(&peer)
                .ok_or_else(|| RaftError::Transport(format!("unknown peer {peer:?}")))?;
            // One full-frame materialization — the channel counterpart of the
            // single user->kernel copy the TCP writev performs.
            let total: usize = slices.iter().map(|s| s.len()).sum();
            let mut frame = Vec::with_capacity(total);
            for s in slices {
                frame.extend_from_slice(s);
            }
            tx.send(frame)
                .map_err(|_| RaftError::Transport("follower channel closed".into()))
        }
    }

    fn send_frame_owned(
        &self,
        peer: PeerId,
        frame: bytes::Bytes,
    ) -> impl std::future::Future<Output = Result<(), RaftError>> + Send {
        async move {
            let tx = self
                .peers
                .get(&peer)
                .ok_or_else(|| RaftError::Transport(format!("unknown peer {peer:?}")))?;
            // Deliberate copy: parity with the user->kernel copy of write_all.
            tx.send(frame.to_vec())
                .map_err(|_| RaftError::Transport("follower channel closed".into()))
        }
    }

    fn recv_frame(
        &self,
        out: &mut [u8],
    ) -> impl std::future::Future<Output = Result<usize, RaftError>> + Send {
        async move { self.inbound.recv(out).await }
    }

    fn recv_frame_timeout(
        &self,
        timeout: Duration,
        out: &mut [u8],
    ) -> impl std::future::Future<Output = Result<Option<usize>, RaftError>> + Send {
        async move { self.inbound.recv_timeout(timeout, out).await }
    }
}

// ---------------------------------------------------------------------------
// TcpBenchTransport — TCP loopback byte-moving primitive.
// Connections are pre-established at cluster setup, so measurements never
// include connect() handshakes. No 'static transmutes: the send future
// legitimately borrows `slices` (RPITIT captures argument lifetimes).
// ---------------------------------------------------------------------------

struct TcpBenchTransport {
    inbound: Inbound,
    conns: HashMap<PeerId, Arc<tokio::sync::Mutex<TcpStream>>>,
}

impl RaftTransport for TcpBenchTransport {
    fn send_vectored(
        &self,
        peer: PeerId,
        slices: &[&[u8]],
    ) -> impl std::future::Future<Output = Result<(), RaftError>> + Send {
        async move {
            let conn = self
                .conns
                .get(&peer)
                .cloned()
                .ok_or_else(|| RaftError::Transport(format!("unknown peer {peer:?}")))?;
            let mut s = conn.lock().await;
            let mut io_bufs: Vec<IoSlice<'_>> = slices.iter().map(|x| IoSlice::new(x)).collect();
            let mut bufs: &mut [IoSlice<'_>] = &mut io_bufs;
            while !bufs.is_empty() {
                let n = s
                    .write_vectored(bufs)
                    .await
                    .map_err(|e| RaftError::Transport(e.to_string()))?;
                if n == 0 {
                    return Err(RaftError::Transport(
                        "write_vectored returned 0 — peer closed".into(),
                    ));
                }
                IoSlice::advance_slices(&mut bufs, n);
            }
            Ok(())
        }
    }

    fn send_frame_owned(
        &self,
        peer: PeerId,
        frame: bytes::Bytes,
    ) -> impl std::future::Future<Output = Result<(), RaftError>> + Send {
        async move {
            let conn = self
                .conns
                .get(&peer)
                .cloned()
                .ok_or_else(|| RaftError::Transport(format!("unknown peer {peer:?}")))?;
            let mut s = conn.lock().await;
            s.write_all(&frame)
                .await
                .map_err(|e| RaftError::Transport(e.to_string()))
        }
    }

    fn recv_frame(
        &self,
        out: &mut [u8],
    ) -> impl std::future::Future<Output = Result<usize, RaftError>> + Send {
        async move { self.inbound.recv(out).await }
    }

    fn recv_frame_timeout(
        &self,
        timeout: Duration,
        out: &mut [u8],
    ) -> impl std::future::Future<Output = Result<Option<usize>, RaftError>> + Send {
        async move { self.inbound.recv_timeout(timeout, out).await }
    }
}

// ---------------------------------------------------------------------------
// Cluster construction — identical node/config/storage, different wiring.
// ---------------------------------------------------------------------------

struct BenchCluster<T: RaftTransport> {
    raft: ArbitroRaft<BenchStorage, T, NoopSM>,
    tasks: Vec<JoinHandle<()>>,
}

impl<T: RaftTransport> Drop for BenchCluster<T> {
    fn drop(&mut self) {
        // Abort background tasks so samples do not leak tasks/sockets into
        // subsequent samples (the old tcp bench leaked its accept loop).
        for t in &self.tasks {
            t.abort();
        }
    }
}

fn make_config(addrs: [SocketAddr; 3]) -> NodeConfig {
    let mut config = NodeConfig {
        node_id: PeerId(1),
        cluster_id: ClusterId(1),
        peers: vec![PeerId(1), PeerId(2), PeerId(3)],
        learners: Vec::new(),
        bootstrap_peers: vec![
            BootstrapPeer {
                id: PeerId(1),
                addr: addrs[0],
            },
            BootstrapPeer {
                id: PeerId(2),
                addr: addrs[1],
            },
            BootstrapPeer {
                id: PeerId(3),
                addr: addrs[2],
            },
        ],
        ..Default::default()
    };
    // Durable mode: widen the propose gather window (= heartbeat_ms * 2) so a
    // single WAL flush stall on the VHD cannot exceed the deadline and panic.
    // No election timer runs in this harness, so these only shape the window.
    if wal_config().policy.is_durable() {
        config.timing = TimingConfig {
            heartbeat_ms: 500,
            election_min_ms: 2000,
            election_max_ms: 4000,
        };
    }
    config
}

fn make_leader<T: RaftTransport>(
    config: NodeConfig,
    transport: T,
) -> ArbitroRaft<BenchStorage, T, NoopSM> {
    let storage = BenchStorage::new();
    let mut node = RaftNode::new(config, storage, transport).unwrap();
    node.become_leader_for_benchmark(Term(1));
    ArbitroRaft::new(node, NoopSM)
}

async fn make_inmem_cluster() -> BenchCluster<ChannelTransport> {
    let (inbound_tx, inbound_rx) = unbounded_channel::<Vec<u8>>();
    let mut peers = HashMap::new();
    let mut tasks = Vec::new();
    for id in [2u64, 3] {
        let (tx, rx) = unbounded_channel::<Vec<u8>>();
        peers.insert(PeerId(id), tx);
        tasks.push(tokio::spawn(run_channel_follower(
            rx,
            inbound_tx.clone(),
            PeerId(id),
        )));
    }
    drop(inbound_tx);

    let transport = ChannelTransport {
        inbound: Inbound::new(inbound_rx),
        peers,
    };
    let addrs: [SocketAddr; 3] = [
        "127.0.0.1:9101".parse().unwrap(),
        "127.0.0.1:9102".parse().unwrap(),
        "127.0.0.1:9103".parse().unwrap(),
    ];
    BenchCluster {
        raft: make_leader(make_config(addrs), transport),
        tasks,
    }
}

async fn make_tcp_cluster() -> BenchCluster<TcpBenchTransport> {
    let l1 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let l2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let l3 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr1 = l1.local_addr().unwrap();
    let addr2 = l2.local_addr().unwrap();
    let addr3 = l3.local_addr().unwrap();

    let (inbound_tx, inbound_rx) = unbounded_channel::<Vec<u8>>();
    let mut tasks = Vec::new();

    // Leader-side accept/demux: reads follower->leader byte streams, splits
    // them into frames and forwards them to the shared inbound channel.
    tasks.push(tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = l1.accept().await else {
                return;
            };
            let _ = stream.set_nodelay(true);
            let tx = inbound_tx.clone();
            tokio::spawn(async move {
                let mut buf = Vec::with_capacity(64 * 1024);
                while let Some(frame) = read_frame_into(&mut stream, &mut buf).await {
                    if tx.send(frame).is_err() {
                        return;
                    }
                }
            });
        }
    }));

    tasks.push(tokio::spawn(run_tcp_follower(l2, addr1, PeerId(2))));
    tasks.push(tokio::spawn(run_tcp_follower(l3, addr1, PeerId(3))));

    // Pre-establish leader->follower connections (listener backlog makes this
    // safe even before the follower task reaches accept()).
    let mut conns = HashMap::new();
    for (id, addr) in [(PeerId(2), addr2), (PeerId(3), addr3)] {
        let s = TcpStream::connect(addr).await.unwrap();
        s.set_nodelay(true).unwrap();
        conns.insert(id, Arc::new(tokio::sync::Mutex::new(s)));
    }

    let transport = TcpBenchTransport {
        inbound: Inbound::new(inbound_rx),
        conns,
    };
    BenchCluster {
        raft: make_leader(make_config([addr1, addr2, addr3]), transport),
        tasks,
    }
}

// ---------------------------------------------------------------------------
// Measurement cores — generic over the transport; the compiler monomorphizes
// the exact same code for both variants.
// ---------------------------------------------------------------------------

async fn measure_propose_once<T: RaftTransport>(
    raft: &mut ArbitroRaft<BenchStorage, T, NoopSM>,
    payload: &[u8],
    iters: u64,
) -> Duration {
    // Untimed warm-up rounds per sample so first-touch costs never skew a
    // sample. Durable mode needs more to warm the VHD write path steady-state.
    let warmup = if wal_config().policy.is_durable() { 16 } else { 1 };
    for _ in 0..warmup {
        raft.propose_once(payload)
            .await
            .expect("warm-up propose must reach quorum");
    }
    let start = Instant::now();
    for _ in 0..iters {
        raft.propose_once(payload)
            .await
            .expect("propose must reach quorum");
    }
    start.elapsed()
}

async fn measure_propose_batch<T: RaftTransport>(
    raft: &mut ArbitroRaft<BenchStorage, T, NoopSM>,
    payloads: &[&[u8]],
    iters: u64,
) -> Duration {
    let warmup = if wal_config().policy.is_durable() { 16 } else { 1 };
    for _ in 0..warmup {
        raft.propose_batch_once(payloads)
            .await
            .expect("warm-up batch must reach quorum");
    }
    let start = Instant::now();
    for _ in 0..iters {
        raft.propose_batch_once(payloads)
            .await
            .expect("batch must reach quorum");
    }
    start.elapsed()
}

// ---------------------------------------------------------------------------
// Criterion wiring
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    InMem,
    Tcp,
}

impl Kind {
    fn name(self) -> &'static str {
        match self {
            Kind::InMem => "inmem",
            Kind::Tcp => "tcp",
        }
    }
}

fn kinds_from_env() -> Vec<Kind> {
    match std::env::var("ARBITRO_BENCH_TRANSPORT").ok().as_deref() {
        Some("tcp") => vec![Kind::Tcp],
        Some("inmem") => vec![Kind::InMem],
        _ => vec![Kind::InMem, Kind::Tcp],
    }
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn configure_group(group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>) {
    // Runs once (internally guarded): fs guard + calibration probe + context.
    durable_startup();
    let durable = wal_config().policy.is_durable();
    // At ms-scale durable ops, 20 s of measurement yields enough commits for a
    // stable estimate; non-durable keeps the fast few-minutes full run.
    let (warm_default, meas_default) = if durable { (3000, 20000) } else { (1000, 3000) };
    group.sample_size(env_u64("ARBITRO_BENCH_SAMPLE_SIZE", 10) as usize);
    group.warm_up_time(Duration::from_millis(env_u64(
        "ARBITRO_BENCH_WARMUP_MS",
        warm_default,
    )));
    group.measurement_time(Duration::from_millis(env_u64(
        "ARBITRO_BENCH_MEASURE_MS",
        meas_default,
    )));
    if durable {
        // Every sample gets a full iteration count (no linear-slope sampling)
        // so a per-op ms barrier is not amortized into an unreadable estimate.
        group.sampling_mode(criterion::SamplingMode::Flat);
    }
}

fn make_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap()
}

fn bench_transport_latency(c: &mut Criterion) {
    let rt = make_runtime();
    let kinds = kinds_from_env();

    let mut group = c.benchmark_group("raft_transport_latency");
    configure_group(&mut group);
    group.throughput(Throughput::Elements(1));

    for &kind in &kinds {
        for (label, size) in [("empty", 0usize), ("1kb", 1024)] {
            group.bench_function(
                format!("{}{}/propose_once/{label}", kind.name(), fsync_id_segment()),
                |b| {
                b.to_async(&rt).iter_custom(move |iters| async move {
                    let payload = vec![0xAAu8; size];
                    match kind {
                        Kind::InMem => {
                            let mut cluster = make_inmem_cluster().await;
                            measure_propose_once(&mut cluster.raft, &payload, iters).await
                        }
                        Kind::Tcp => {
                            let mut cluster = make_tcp_cluster().await;
                            measure_propose_once(&mut cluster.raft, &payload, iters).await
                        }
                    }
                });
            });
        }
    }
    group.finish();
}

fn bench_transport_batch(c: &mut Criterion) {
    let rt = make_runtime();
    let kinds = kinds_from_env();

    let mut group = c.benchmark_group("raft_transport_batch");
    configure_group(&mut group);

    for &kind in &kinds {
        for &batch in &[1u64, 64, 1024] {
            group.throughput(Throughput::Elements(batch));
            group.bench_function(
                format!(
                    "{}{}/propose_batch/empty_x{batch}",
                    kind.name(),
                    fsync_id_segment()
                ),
                |b| {
                    b.to_async(&rt).iter_custom(move |iters| async move {
                        let empty = [];
                        let payloads: Vec<&[u8]> = vec![&empty; batch as usize];
                        match kind {
                            Kind::InMem => {
                                let mut cluster = make_inmem_cluster().await;
                                measure_propose_batch(&mut cluster.raft, &payloads, iters).await
                            }
                            Kind::Tcp => {
                                let mut cluster = make_tcp_cluster().await;
                                measure_propose_batch(&mut cluster.raft, &payloads, iters).await
                            }
                        }
                    });
                },
            );
        }
    }
    group.finish();
}

criterion_group!(benches, bench_transport_latency, bench_transport_batch);
criterion_main!(benches);
