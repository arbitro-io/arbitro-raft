use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use crate::{
    HardState, InboundRaftMessage, LogEntry, LogIndex, NodeConfig, PeerId, RaftCustomRegistry,
    RaftError, RaftMessage, Role, SoftState, Term, TimingConfig,
};

mod dispatch;
mod election;
mod generational;
mod leader_balance;
pub(crate) mod log_compaction;
pub(crate) mod membership;
mod progress;
mod read_index;
pub(crate) mod replication;
mod scratch;
mod snapshot;
mod snapshot_install;
mod transfer;

pub(crate) use dispatch::PendingCustomDispatch;
pub(crate) use progress::{
    AppendAttemptState, PeerMap, PeerProgress, PendingSnapshot, SnapshotAttempts,
};

/// RaftNode implements the core Raft state machine logic.
///
/// The node is long-lived (not bound by ephemeral lifetimes). Hot-path
/// buffers of *borrowed* data (payload views, entry batches, iovec lists)
/// are not stored in the node at all — only their reusable allocations are,
/// via the capacity docks in [`scratch`]. Callers `take()` an empty `Vec`
/// re-typed to a local lifetime, fill it under normal borrow checking, and
/// `put()` the cleared allocation back. No reference is ever laundered to
/// `'static` and nothing borrowed is parked in `self` across a `&mut self`
/// reborrow (audit items US3/US4/US5).
pub struct RaftNode<S, T> {
    pub(crate) config: NodeConfig,
    pub(crate) storage: S,
    pub(crate) transport: T,
    pub(crate) hard_state: HardState,
    pub(crate) soft_state: SoftState,
    pub(crate) custom_registry: RaftCustomRegistry,
    pub(crate) pending_custom: HashMap<u64, Box<dyn PendingCustomDispatch + Send + Sync>>,
    pub(crate) peer_progress: PeerMap<PeerProgress>,
    /// Joint-consensus voter sets active while a `C_old_new` entry is the
    /// effective configuration. `None` outside a transition. During Joint
    /// phase, commit requires majority-of-old AND majority-of-new (Raft §4.3);
    /// `config.peers` still holds the union for broadcast/replication targets.
    pub(crate) joint_peers: Option<(Vec<PeerId>, Vec<PeerId>)>,
    pub(crate) pending_snapshots: HashMap<PeerId, PendingSnapshot>,
    /// Leader-side PS7 guard: consecutive failed snapshot-install attempts and
    /// active cooldown per peer. Cleared on step-down and on install success.
    pub(crate) snapshot_attempts: HashMap<PeerId, SnapshotAttempts>,
    pub(crate) log_metadata: generational::LogMetadataArena,

    // Scratchpads — pre-allocated buffers reused across calls on the hot path.
    // Plain-data scratch vecs: always call .clear() before use; never assume
    // they are empty. Borrowed-data scratch lives in the capacity docks
    // below (`EntryScratch` / `SliceScratch`), which hand out EMPTY vecs and
    // therefore need no clear-before-use convention.
    /// Capacity dock for `Vec<LogEntry<'_>>` append batches (see [`scratch`]).
    pub(crate) scratch_entries: scratch::EntryScratch,
    pub(crate) scratch_indexes: Vec<LogIndex>,
    /// Dedicated working buffer for the commit-quorum gather in
    /// [`try_advance_commit_index`]. Kept separate from `scratch_indexes` so the
    /// quorum sort never clobbers the per-entry indexes that `propose_batch_once`
    /// returns to the caller (regression G1).
    pub(crate) scratch_commit_acks: Vec<LogIndex>,
    pub(crate) scratch_peers: Vec<PeerId>,
    pub(crate) scratch_quorum_buf: Vec<u8>,
    /// Capacity dock for the vectored-I/O `Vec<&[u8]>` iovec list. Elements
    /// are built with true lifetimes at the call site — no raw-pointer or
    /// layout-assuming transmute is involved anymore (US5).
    pub(crate) scratch_vectored: scratch::SliceScratch,
    pub(crate) scratch_pending: PeerMap<AppendAttemptState>,
    /// Check-quorum contact map (A8 / PS8): wall-clock instant of the last
    /// CURRENT-term frame received from each VOTER while this node leads.
    /// Stamped by `handle_inbound`, read by `check_quorum_active`, cleared on
    /// step-down and on leader-progress (re)initialization. Learners are
    /// never stamped — their liveness must not keep the quorum lease alive.
    pub(crate) last_voter_contact: HashMap<PeerId, std::time::Instant>,
    pub(crate) scratch_outbound: Vec<u8>,
    pub(crate) scratch_payload: Vec<u8>,
    /// Capacity dock for `Vec<&[u8]>` payload-view lists (seeded replication
    /// and the client-batch fan-in). See [`scratch`].
    pub(crate) scratch_payload_refs: scratch::SliceScratch,
    pub(crate) scratch_responders: Vec<PeerId>,

    /// Cached last-log position — kept in sync with every append/truncate so
    /// `try_advance_commit_index` and leader-progress init avoid a storage read.
    pub(crate) cached_last_log: (LogIndex, Term),

    /// Last snapshot boundary this node knows of: `(last_included_index,
    /// last_included_term)`, or `(0, 0)` when no snapshot exists. Raft §7
    /// requires a node to remember these two values after compaction — the
    /// entry at (and below) the boundary may no longer exist in the log, yet
    /// its term is still needed for `prev_log` checks and `term_at` reads
    /// right after the boundary (C3). Loaded from storage at construction and
    /// refreshed on every snapshot save/restore/install.
    pub(crate) snapshot_boundary: (LogIndex, Term),

    /// Atomic mirror of `soft_state.commit_index`, updated via
    /// [`RaftNode::set_commit_index`] so external observers (e.g. an
    /// apply loop that lives in another task) can safely read the
    /// committed boundary without holding the node lock.
    pub(crate) commit_index_pub: Arc<AtomicU64>,

    /// Lifecycle counters, cloned out via [`RaftNode::metrics`] before the node
    /// moves into its run task. Incremented only on cold paths.
    pub(crate) metrics: RaftMetrics,

    /// Highest log index applied to the state machine.
    ///
    /// Volatile — reset to `LogIndex(0)` on restart. The state
    /// machine is responsible for its own persistence and
    /// reconciliation via `snapshot`/`restore`. The Raft protocol
    /// guarantees `last_applied <= commit_index` at all times.
    pub(crate) last_applied: LogIndex,

    /// Log-compaction debt (C2): entries applied to the state machine since
    /// the last snapshot was saved. When it reaches
    /// `limits.compaction_threshold_entries` the run loop compacts the log.
    /// Reset on every snapshot save/restore.
    pub(crate) compaction_debt_entries: u64,
    /// Log-compaction debt (C2): payload bytes applied since the last
    /// snapshot. Compared against `limits.compaction_threshold_bytes`.
    pub(crate) compaction_debt_bytes: u64,

    /// Wall-clock instant of the last accepted `AppendEntries` from a current
    /// leader. Backs the pre-vote leader-stickiness rule (§4.2.2): a follower
    /// that has heard from its leader within the minimum election timeout
    /// rejects pre-votes, so a partitioned or removed node cannot disrupt a
    /// healthy cluster with a spurious election.
    pub(crate) last_leader_contact: Option<std::time::Instant>,

    /// Leader-side §4.2.3 transfer window: set once `TimeoutNow` is sent to
    /// the target; while active client proposals are rejected with a redirect
    /// hint at the target. Cleared on step-down (the transfer resolved) or
    /// lazily on expiry (the transfer aborted; leadership resumes).
    pub(crate) pending_transfer: Option<transfer::PendingTransfer>,

    /// Target-side §4.2.3 sanction: the term at which a `TimeoutNow` was
    /// accepted. The run loop consumes it via
    /// [`RaftNode::take_forced_campaign`] and campaigns immediately —
    /// bypassing the election timeout AND pre-vote. Dropped if the term moved
    /// on before consumption.
    pub(crate) forced_campaign_term: Option<Term>,

    /// A11 (ReadIndex) probe sequence. Stamped into the reserved word of
    /// every outbound `AppendEntries` this leader builds and echoed back by
    /// followers in `AppendEntriesResp._pad[0..4]`. A quorum-confirmation
    /// round bumps it first and then counts ONLY acks echoing the new value —
    /// an ack buffered from before the read began echoes an older value (or
    /// 0) and can never confirm the round. Wrapping u32; 0 is reserved for
    /// "no probe" so the first bump starts at 1.
    pub(crate) read_probe_seq: u32,
}

/// Read-only, cheaply-clonable view of a Raft node's committed log index.
///
/// The Raft protocol guarantees that once an entry is committed it is
/// safe to apply to the state machine. Consumers polling `get()` see a
/// monotonically non-decreasing value.
#[derive(Debug, Clone)]
pub struct CommitIndexObserver {
    inner: Arc<AtomicU64>,
}

impl CommitIndexObserver {
    /// Current committed index. Safe to call from any thread.
    #[inline]
    pub fn get(&self) -> LogIndex {
        LogIndex(self.inner.load(Ordering::Acquire))
    }
}

/// A cheap, point-in-time snapshot of a node's consensus state, for operators,
/// health checks, and tests. Produced by [`RaftNode::status`] /
/// [`crate::ArbitroRaft::status`] under the run-loop's `&mut self`, so every
/// field reflects the same consistent instant. All fields are `Copy`-cheap;
/// nothing here allocates or locks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RaftStatus {
    /// This node's id.
    pub node_id: PeerId,
    /// Current term.
    pub term: Term,
    /// Current role (Follower / Candidate / Leader).
    pub role: Role,
    /// Who this node currently believes leads, or `None` when it does not know
    /// (start-up, mid-election, or right after a step-down). An operator asks
    /// this to find the cluster leader.
    pub leader_id: Option<PeerId>,
    /// Convenience: `role == Leader`.
    pub is_leader: bool,
    /// Highest index known committed (volatile; 0 after restart).
    pub commit_index: LogIndex,
    /// Highest index applied to the state machine (volatile; ≤ `commit_index`).
    pub last_applied: LogIndex,
    /// Index of the last entry in this node's log.
    pub last_log_index: LogIndex,
    /// Number of voters in the effective configuration (the union set while a
    /// joint transition is active). Learners (A13) are NOT counted here —
    /// they participate in no quorum; see [`RaftNode::learners`].
    pub voter_count: usize,
    /// True while a joint-consensus membership change is in flight on this node.
    pub config_change_in_progress: bool,
}

/// Cheaply-clonable, thread-safe lifecycle counters for a node.
///
/// Clone one via [`RaftNode::metrics`] / [`crate::ArbitroRaft::metrics`] BEFORE
/// moving the node into its run task, so an operator or another task can poll
/// the counters while the run loop owns `&mut self` (same pattern as
/// [`CommitIndexObserver`]). All updates are `Relaxed` — these are counters, not
/// synchronization — and every incrementer sits on a cold path (elections,
/// step-downs, config changes, dropped frames), never the steady-state
/// append/commit hot path.
#[derive(Debug, Clone, Default)]
pub struct RaftMetrics {
    inner: Arc<RaftMetricsInner>,
}

#[derive(Debug, Default)]
struct RaftMetricsInner {
    elections_started: AtomicU64,
    elections_won: AtomicU64,
    step_downs: AtomicU64,
    config_changes_applied: AtomicU64,
    frames_dropped_nonfatal: AtomicU64,
    snapshots_evicted: AtomicU64,
    snapshot_installs_refused: AtomicU64,
    log_compactions: AtomicU64,
    peers_jailed: AtomicU64,
    frames_shed_jailed: AtomicU64,
    resource_exhausted: AtomicU64,
}

/// Point-in-time copy of [`RaftMetrics`], returned by [`RaftMetrics::snapshot`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RaftMetricsSnapshot {
    /// Real elections this node started (post pre-vote).
    pub elections_started: u64,
    /// Elections this node won (became leader).
    pub elections_won: u64,
    /// Times this node stepped down to a higher term or lost quorum.
    pub step_downs: u64,
    /// Config-change entries this node applied (joint, final, or a
    /// learner-set change — A13).
    pub config_changes_applied: u64,
    /// Inbound frames dropped after a non-fatal decode/handle/recv error — a
    /// rising count signals a misbehaving or version-skewed peer.
    pub frames_dropped_nonfatal: u64,
    /// Inbound pending-snapshot transfers evicted after making no progress for
    /// `limits.snapshot_stall_timeout_ms` (C4 / P1-5). A rising count signals
    /// a leader (or spoofing peer) that starts transfers and goes quiet.
    pub snapshots_evicted: u64,
    /// Leader-side snapshot installs refused because a peer exhausted
    /// `limits.snapshot_max_attempts_per_peer` and is in cooldown (PS7).
    pub snapshot_installs_refused: u64,
    /// Policy-triggered log compactions completed by the run loop (C2):
    /// a state-machine snapshot was persisted (and the log prefix truncated
    /// up to the conservative horizon, when the horizon allowed it).
    pub log_compactions: u64,
    /// Jail events (D3): times a peer crossed
    /// `limits.inbound_decode_error_jail_threshold` decode errors within one
    /// error window and was put in the inbound jail. A rising count names a
    /// hostile or badly version-skewed peer.
    pub peers_jailed: u64,
    /// Frames shed pre-decode because their claimed sender was jailed (D3).
    /// These frames cost one header peek each — no decode, no per-frame log
    /// line — which is what bounds the CPU a garbage flood can burn.
    pub frames_shed_jailed: u64,
    /// Storage resource-exhaustion events (C8): times a storage operation
    /// failed with an ENOSPC-class error ([`crate::ErrorClass::Resource`] —
    /// disk full / quota / OOM) and the node degraded to read-only survival
    /// (leader stepped down and rejected proposals; follower stopped acking)
    /// instead of halting. A rising count is an operator page: free disk
    /// space — the node resumes normal operation on its own once a storage
    /// operation succeeds again.
    pub resource_exhausted: u64,
}

impl RaftMetrics {
    /// Read all counters at once. Safe from any thread.
    #[inline]
    pub fn snapshot(&self) -> RaftMetricsSnapshot {
        use std::sync::atomic::Ordering::Relaxed;
        RaftMetricsSnapshot {
            elections_started: self.inner.elections_started.load(Relaxed),
            elections_won: self.inner.elections_won.load(Relaxed),
            step_downs: self.inner.step_downs.load(Relaxed),
            config_changes_applied: self.inner.config_changes_applied.load(Relaxed),
            frames_dropped_nonfatal: self.inner.frames_dropped_nonfatal.load(Relaxed),
            snapshots_evicted: self.inner.snapshots_evicted.load(Relaxed),
            snapshot_installs_refused: self.inner.snapshot_installs_refused.load(Relaxed),
            log_compactions: self.inner.log_compactions.load(Relaxed),
            peers_jailed: self.inner.peers_jailed.load(Relaxed),
            frames_shed_jailed: self.inner.frames_shed_jailed.load(Relaxed),
            resource_exhausted: self.inner.resource_exhausted.load(Relaxed),
        }
    }

    #[inline]
    pub(crate) fn inc_elections_started(&self) {
        self.inner
            .elections_started
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    #[inline]
    pub(crate) fn inc_elections_won(&self) {
        self.inner
            .elections_won
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    #[inline]
    pub(crate) fn inc_step_downs(&self) {
        self.inner
            .step_downs
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    #[inline]
    pub(crate) fn inc_config_changes_applied(&self) {
        self.inner
            .config_changes_applied
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    #[inline]
    pub(crate) fn inc_frames_dropped_nonfatal(&self) {
        self.inner
            .frames_dropped_nonfatal
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    #[inline]
    pub(crate) fn inc_snapshots_evicted(&self) {
        self.inner
            .snapshots_evicted
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    #[inline]
    pub(crate) fn inc_snapshot_installs_refused(&self) {
        self.inner
            .snapshot_installs_refused
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    #[inline]
    pub(crate) fn inc_log_compactions(&self) {
        self.inner
            .log_compactions
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    #[inline]
    pub(crate) fn inc_peers_jailed(&self) {
        self.inner
            .peers_jailed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    #[inline]
    pub(crate) fn inc_frames_shed_jailed(&self) {
        self.inner
            .frames_shed_jailed
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    #[inline]
    pub(crate) fn inc_resource_exhausted(&self) {
        self.inner
            .resource_exhausted
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

// NOTE (US4): `RaftNode` used to carry `unsafe impl Send/Sync` because
// `scratch_vectored` stored raw pointers and other scratch fields stored
// `'static`-laundered references. Both are gone — the scratch docks store
// only empty vec allocations (see [`scratch`]) — so the auto-derived
// `Send`/`Sync` impls apply and are proven by the compiler. The static
// assertion below pins that property so a future field cannot silently
// reintroduce a manual (unchecked) impl requirement.
const _: () = {
    fn assert_send_sync<N: Send + Sync>() {}
    fn raft_node_is_auto_send_sync<S: Send + Sync + 'static, T: Send + Sync + 'static>() {
        assert_send_sync::<RaftNode<S, T>>();
    }
    let _ = raft_node_is_auto_send_sync::<(), ()>;
};

impl<S, T> RaftNode<S, T>
where
    S: crate::RaftStorage,
    T: crate::RaftTransport,
{
    pub fn new(config: NodeConfig, storage: S, transport: T) -> Result<Self, RaftError> {
        crate::validate_node_config(&config)?;
        let hard_state = storage.load_hard_state()?;
        let cached_last_log = storage.last_log_position()?;
        // §7: recover the snapshot boundary so terms at/around it stay
        // answerable after a restart even though the log prefix is gone.
        // C5/P4: meta-only probe — startup only needs the boundary, not the
        // snapshot payload.
        let snapshot_boundary = storage
            .load_snapshot_meta()?
            .map(|meta| (meta.last_included_index, meta.last_included_term))
            .unwrap_or((LogIndex(0), Term(0)));
        let soft_state = SoftState {
            leader_id: None,
            is_leader: false,
            role: Role::Follower,
            // commit_index is volatile — always 0 on restart, advanced by AppendEntries.
            commit_index: LogIndex(0),
        };
        let peer_count = config.peers.len();
        Ok(Self {
            config,
            storage,
            transport,
            hard_state,
            soft_state,
            custom_registry: RaftCustomRegistry::new(),
            pending_custom: HashMap::with_capacity(1024),
            peer_progress: PeerMap::with_capacity(peer_count),
            joint_peers: None,
            pending_snapshots: HashMap::with_capacity(peer_count),
            snapshot_attempts: HashMap::with_capacity(peer_count),
            scratch_entries: scratch::EntryScratch::with_capacity(1024),
            scratch_indexes: Vec::with_capacity(1024),
            scratch_commit_acks: Vec::with_capacity(peer_count + 1),
            scratch_peers: Vec::with_capacity(peer_count),
            scratch_quorum_buf: vec![0u8; 64 * 1024],
            scratch_vectored: scratch::SliceScratch::with_capacity(2048),
            scratch_pending: PeerMap::with_capacity(peer_count),
            last_voter_contact: HashMap::with_capacity(peer_count),
            scratch_outbound: vec![0; 1024 * 1024], // 1MB pre-allocated scratch for outbound encoding
            scratch_payload: vec![0; 16 * 1024 * 1024], // 16MB pre-allocated scratch for storage reads
            scratch_payload_refs: scratch::SliceScratch::with_capacity(1024),
            scratch_responders: Vec::with_capacity(peer_count),
            cached_last_log,
            snapshot_boundary,
            log_metadata: generational::LogMetadataArena::new(8192),
            commit_index_pub: Arc::new(AtomicU64::new(0)),
            metrics: RaftMetrics::default(),
            // last_applied is volatile — always 0 on restart, mirrors
            // the invariant that commit_index also re-initializes to 0.
            last_applied: LogIndex(0),
            compaction_debt_entries: 0,
            compaction_debt_bytes: 0,
            last_leader_contact: None,
            pending_transfer: None,
            forced_campaign_term: None,
            read_probe_seq: 0,
        })
    }

    /// Update `soft_state.commit_index` and publish the new value to
    /// any [`CommitIndexObserver`] holding a clone of the atomic mirror.
    ///
    /// This is the single write path for the commit index — call this
    /// instead of assigning `soft_state.commit_index` directly so
    /// external observers (e.g. an apply loop in another task) stay
    /// consistent.
    ///
    /// Callers outside the crate should NOT invoke this directly in
    /// production; the Raft loop drives commit-index progression. It
    /// is exposed (hidden from docs) purely so correctness tests can
    /// simulate commit advances without spinning up a full cluster.
    #[doc(hidden)]
    #[inline]
    pub fn set_commit_index(&mut self, idx: LogIndex) {
        self.soft_state.commit_index = idx;
        self.commit_index_pub.store(idx.0, Ordering::Release);
    }

    /// Cheaply-clonable read-only observer over the committed log index.
    ///
    /// Consumers must clone the observer before the node is moved into
    /// its background task (as with [`ArbitroRaft::client_handle`]).
    #[inline]
    pub fn commit_index_observer(&self) -> CommitIndexObserver {
        CommitIndexObserver {
            inner: self.commit_index_pub.clone(),
        }
    }

    #[inline]
    pub fn custom_registry(&self) -> &RaftCustomRegistry {
        &self.custom_registry
    }

    #[inline]
    pub fn role(&self) -> Role {
        self.soft_state.role
    }

    #[inline]
    pub fn node_id(&self) -> PeerId {
        self.config.node_id
    }

    /// Voter set this node is currently configured with.
    ///
    /// This is the same slice that `RaftGroupRegistry` uses for quorum math.
    /// The membership-change task (Group 2) mutates this set in place when a
    /// joint-consensus transition applies; readers must not cache the slice
    /// across a `run_once` boundary.
    #[inline]
    pub fn peers(&self) -> &[PeerId] {
        &self.config.peers
    }

    /// Learner (non-voting member) set this node is currently configured
    /// with (A13). Learners receive replication like followers but are
    /// excluded from every quorum; always disjoint from [`peers`].
    ///
    /// [`peers`]: RaftNode::peers
    #[inline]
    pub fn learners(&self) -> &[PeerId] {
        &self.config.learners
    }

    /// A13: leader-side caught-up predicate for a learner — the operator
    /// contract behind [`promote_learner`]. `true` when every entry this
    /// leader knows committed has been replicated to the learner
    /// (`match_index >= commit_index`). The uncommitted tail may still be
    /// in flight; that is safe to promote through because the Joint phase
    /// of the promotion counts the new voter's acks like any other voter's.
    ///
    /// Errors: [`RaftError::NotLeader`] on a non-leader,
    /// [`RaftError::PeerUnknown`] when `peer` is not a learner.
    ///
    /// [`promote_learner`]: crate::ArbitroRaft::promote_learner
    pub fn learner_caught_up(&self, peer: PeerId) -> Result<bool, RaftError> {
        if !self.is_leader() {
            return Err(self.not_leader_error());
        }
        if !self.config.learners.contains(&peer) {
            return Err(RaftError::PeerUnknown(peer));
        }
        Ok(self
            .peer_progress
            .get(&peer)
            .map(|p| p.match_index >= self.soft_state.commit_index)
            .unwrap_or(false))
    }

    #[inline]
    pub fn timing(&self) -> TimingConfig {
        self.config.timing
    }

    #[inline]
    pub fn transport(&self) -> &T {
        &self.transport
    }

    #[inline]
    pub fn is_leader(&self) -> bool {
        self.soft_state.role == Role::Leader
    }

    /// Standard `NotLeader` error carrying the current redirect hint
    /// (`soft_state.leader_id` — who this node currently believes leads).
    /// dup-F4: the ONE construction site for the generic not-leader
    /// rejection; paths that redirect somewhere more specific (the §4.2.3
    /// transfer target) build their own hint deliberately.
    #[inline]
    pub(crate) fn not_leader_error(&self) -> RaftError {
        RaftError::NotLeader {
            leader_hint: self
                .soft_state
                .leader_id
                .map(|leader_id| crate::LeaderHint { leader_id }),
        }
    }

    #[inline]
    pub fn current_term(&self) -> Term {
        self.hard_state.current_term
    }

    #[inline]
    pub fn hard_state(&self) -> &HardState {
        &self.hard_state
    }

    pub fn commit_index(&self) -> LogIndex {
        self.soft_state.commit_index
    }

    /// Highest log index applied to the state machine so far.
    ///
    /// Volatile — resets to `LogIndex(0)` on process restart. Advanced
    /// only by the apply loop in [`ArbitroRaft`], never larger than
    /// [`RaftNode::commit_index`].
    #[inline]
    pub fn last_applied(&self) -> LogIndex {
        self.last_applied
    }

    /// Who this node currently believes leads the cluster, or `None` when it
    /// does not know (start-up, mid-election, just-stepped-down). This is the
    /// getter an operator or a client-redirect path needs to find the leader;
    /// combine with [`RaftNode::current_term`] to know which term it belongs to.
    #[inline]
    pub fn leader_id(&self) -> Option<PeerId> {
        self.soft_state.leader_id
    }

    /// Clone the node's lifecycle counters ([`RaftMetrics`]). Do this before the
    /// node moves into its run task so an operator can poll them concurrently.
    #[inline]
    pub fn metrics(&self) -> RaftMetrics {
        self.metrics.clone()
    }

    /// Consistent point-in-time snapshot of this node's consensus state — see
    /// [`RaftStatus`]. Cheap (no alloc, no lock); intended for health checks,
    /// operator dashboards, and tests.
    #[inline]
    pub fn status(&self) -> RaftStatus {
        RaftStatus {
            node_id: self.config.node_id,
            term: self.hard_state.current_term,
            role: self.soft_state.role,
            leader_id: self.soft_state.leader_id,
            is_leader: self.soft_state.role == Role::Leader,
            commit_index: self.soft_state.commit_index,
            last_applied: self.last_applied,
            last_log_index: self.cached_last_log.0,
            voter_count: self.config.peers.len(),
            config_change_in_progress: self.joint_peers.is_some(),
        }
    }

    /// Advance the applied cursor. Callers MUST guarantee
    /// `idx <= self.commit_index()`; the Raft protocol requires
    /// `last_applied <= commit_index` at all times.
    #[inline]
    pub(crate) fn set_last_applied(&mut self, idx: LogIndex) {
        debug_assert!(
            idx <= self.soft_state.commit_index,
            "last_applied ({}) must not exceed commit_index ({})",
            idx.0,
            self.soft_state.commit_index.0,
        );
        self.last_applied = idx;
    }

    /// Read a single log entry into the supplied scratch buffer.
    ///
    /// Thin wrapper around [`crate::RaftStorage::entry_at`]. Exposed so
    /// the apply loop in [`ArbitroRaft`] can read committed entries
    /// without holding a `&mut` borrow of storage while it also holds
    /// `&mut` on the state machine (split-borrow via disjoint fields).
    #[inline]
    pub fn read_entry_payload_into<'a>(
        &self,
        index: LogIndex,
        buf: &'a mut [u8],
    ) -> Result<Option<LogEntry<'a>>, RaftError> {
        self.storage.entry_at(index, buf)
    }

    /// Timer-based eviction of stalled inbound snapshot transfers (C4 / P1-5).
    ///
    /// Drops (and frees the buffer of) every pending transfer that has made no
    /// progress for `limits.snapshot_stall_timeout_ms`. The run loop calls this
    /// on every tick, so eviction no longer depends on another message arriving
    /// for the same transfer — a leader that goes quiet mid-install cannot park
    /// a multi-GiB buffer forever. A later chunk for an evicted transfer is
    /// NACK-ed from offset 0 and treated as a fresh session.
    ///
    /// Returns the number of transfers evicted; each eviction is logged and
    /// counted in [`RaftMetricsSnapshot::snapshots_evicted`].
    pub fn evict_stalled_snapshots(&mut self) -> usize {
        if self.pending_snapshots.is_empty() {
            return 0;
        }
        let node_id = self.config.node_id;
        let metrics = &self.metrics;
        let mut evicted = 0usize;
        self.pending_snapshots.retain(|peer, snap| {
            if snap.is_expired() {
                tracing::warn!(
                    node_id = node_id.0,
                    peer = peer.0,
                    buffered_bytes = snap.bytes.len(),
                    last_included_index = snap.meta.last_included_index.0,
                    "evicting stalled pending snapshot transfer"
                );
                metrics.inc_snapshots_evicted();
                evicted += 1;
                false
            } else {
                true
            }
        });
        evicted
    }

    #[inline]
    pub fn peer_progress(&self, peer: PeerId) -> Option<(LogIndex, LogIndex)> {
        self.peer_progress
            .get(&peer)
            .map(|p| (p.next_index, p.match_index))
    }

    /// Forces this node to become leader for the given term bypassing election.
    #[doc(hidden)]
    pub fn become_leader_for_benchmark(&mut self, term: Term) {
        self.soft_state.is_leader = true;
        self.soft_state.role = Role::Leader;
        self.hard_state.current_term = term;
        self.soft_state.leader_id = Some(self.config.node_id);
        let _ = self.initialize_leader_progress();
    }

    /// Receive one raw frame from the transport, decode it, and dispatch to the
    /// appropriate handler. `inbound_buf` is provided by the outer loop to avoid allocs.
    pub async fn handle_once(&mut self, inbound_buf: &mut [u8]) -> Result<(), RaftError> {
        let n = self.transport.recv_frame(inbound_buf).await?;
        let inbound = crate::decode_message(&inbound_buf[..n])?;
        self.handle_inbound(inbound).await
    }

    /// O(1) dispatch — switch compiles to a jump table in optimized builds.
    pub async fn handle_inbound(
        &mut self,
        inbound: InboundRaftMessage<'_>,
    ) -> Result<(), RaftError> {
        let from = inbound.from;
        // Only track contact timestamps for configured members — `from` is
        // caller-controlled and an unbounded peer would otherwise leak entries
        // into last_voter_contact forever.
        //
        // A8 / PS8: additionally, only frames carrying the leader's CURRENT
        // term count as quorum-lease contact. A stale-term frame (a deposed
        // or partitioned peer replaying old state) must not keep the
        // check-quorum lease alive, or a leader that lost its majority would
        // never abdicate. Frames at a HIGHER term are intentionally excluded
        // too: their handler steps this node down (which clears the contact
        // map), so recording them first would be both wasted and misleading.
        // Term-less frames (Custom / CustomResponse) never count.
        if self.is_leader()
            && self.config.peers.contains(&from)
            && message_term(&inbound.message) == Some(self.hard_state.current_term)
        {
            self.last_voter_contact
                .insert(from, std::time::Instant::now());
        }
        match inbound.message {
            RaftMessage::AppendEntries(msg, payload) => {
                self.handle_append_entries(from, msg, payload).await
            }
            RaftMessage::AppendEntriesSeeded {
                ae,
                headers,
                payloads,
            } => {
                self.handle_append_entries_seeded(from, ae, headers, payloads)
                    .await
            }
            RaftMessage::AppendEntriesResp(msg) => {
                self.handle_append_entries_response(from, msg).await
            }
            RaftMessage::Custom(payload) => self.handle_custom_message(from, payload).await,
            RaftMessage::CustomResponse(payload) => {
                self.handle_custom_response(from, payload).await
            }
            RaftMessage::RequestVote(msg) => self.handle_request_vote(from, msg).await,
            RaftMessage::RequestVoteResp(msg) => self.handle_request_vote_response(from, msg).await,
            RaftMessage::PreVote(msg) => self.handle_pre_vote(from, msg).await,
            RaftMessage::PreVoteResp(msg) => self.handle_pre_vote_response(from, msg).await,
            RaftMessage::InstallSnapshot(msg, payload) => {
                self.handle_install_snapshot(from, msg, payload).await
            }
            RaftMessage::InstallSnapshotResp(msg) => {
                self.handle_install_snapshot_response(from, msg).await
            }
            RaftMessage::TimeoutNow(msg) => self.handle_timeout_now(from, msg).await,
            RaftMessage::AppendEntriesVectored(_, _)
            | RaftMessage::AppendEntriesSeededVectored { .. } => {
                // Inbound vectored messages are not expected in v0.1.
                // Protocol only uses vectored for OUTBOUND.
                Err(RaftError::Protocol("unexpected vectored inbound".into()))
            }
        }
    }

    /// Dispatch one drained inbound frame, tolerating non-fatal handler
    /// errors (dup-F5): a Fatal-class error (local storage / corrupt log)
    /// propagates; anything else is logged and the frame dropped, so a
    /// single bad or version-skewed peer can never abort the surrounding
    /// gather loop (B13 / P0-2). `context` names the loop for the log line.
    #[inline]
    pub(crate) async fn handle_inbound_tolerant(
        &mut self,
        inbound: InboundRaftMessage<'_>,
        context: &'static str,
    ) -> Result<(), RaftError> {
        if let Err(e) = self.handle_inbound(inbound).await {
            if e.is_fatal() {
                return Err(e);
            }
            tracing::warn!(
                node_id = self.config.node_id.0,
                error = %e,
                context,
                "dropping frame after non-fatal handler error"
            );
        }
        Ok(())
    }

    pub(crate) fn step_down(&mut self, new_term: Term) -> Result<(), RaftError> {
        self.hard_state.current_term = new_term;
        self.hard_state.voted_for = None;
        // Persist before any outbound send
        self.storage.save_hard_state(&self.hard_state)?;
        self.demote_to_follower_keep_term();
        Ok(())
    }

    /// C8: demote this node to follower WITHOUT touching durable hard state —
    /// the term is kept and the vote is NOT cleared, so no storage write is
    /// needed and this can never fail. Used by the resource-exhaustion
    /// degradation path, where the disk that just rejected a write cannot be
    /// required to persist a step-down either (and where clearing `voted_for`
    /// in memory while the persist fails would risk a same-term double vote).
    ///
    /// Raft-safe by construction: a leader that stops exercising leadership
    /// is always safe — it merely becomes a follower of its own term; every
    /// election/vote invariant reads the untouched hard state.
    pub(crate) fn demote_to_follower_keep_term(&mut self) {
        // Count only genuine demotions (Leader/Candidate → Follower), not a
        // follower simply adopting a higher term.
        if self.soft_state.role != Role::Follower {
            self.metrics.inc_step_downs();
        }
        self.soft_state.role = Role::Follower;
        self.soft_state.is_leader = false;
        self.soft_state.leader_id = None;
        self.peer_progress.clear();
        self.pending_snapshots.clear();
        // Leader-scoped PS7 attempt tracking must not survive a demotion.
        self.snapshot_attempts.clear();
        // Dispatch handles are leader-epoch-scoped by design: any custom
        // dispatch in flight is aborted at a leader transition. Resolve every
        // pending waiter with a deterministic LostLeadership failure BEFORE
        // dropping the map (A5 / C7 / ERR-7) — a silent clear() would park
        // `handle.wait()` callers until their dispatch timeout (or forever
        // when none is set).
        // NOTE: DispatchHandle (src/dispatch/tx/handle.rs) has no Drop impl,
        // so a handle dropped by the caller without awaiting completion still
        // holds its tx_id registration until this step-down sweep runs.
        for pending in self.pending_custom.values() {
            pending.abort_lost_leadership();
        }
        self.pending_custom.clear();
        // Stale contact timestamps must not survive a leader transition.
        self.last_voter_contact.clear();
        // A step-down resolves any in-flight §4.2.3 transfer window — either
        // the target's higher term arrived (transfer succeeded) or leadership
        // was lost some other way; in both cases the freeze must not survive.
        self.pending_transfer = None;
    }

    /// Truncate the log and refresh the last-log cache.
    #[inline]
    pub(crate) fn storage_truncate(&mut self, from: LogIndex) -> Result<(), RaftError> {
        self.storage.truncate_suffix(from)?;
        self.log_metadata.truncate_suffix(from);
        self.cached_last_log = self.storage.last_log_position()?;
        Ok(())
    }

    /// Encodes and sends a message using Vectored I/O to avoid payload copies.
    ///
    /// The iovec list is a *local* `Vec<&[u8]>` (allocation recycled through
    /// the `scratch_vectored` dock), so every slice it holds carries its true
    /// borrow-checked lifetime — no raw-pointer vec, no layout-assuming
    /// transmute (US5), no `'static` laundering (US3).
    ///
    /// `msg` must not borrow from `self` here (the `&mut self` receiver
    /// enforces that). Paths whose message borrows node-owned storage (the
    /// seeded zerocopy append) call [`send_message_vectored`] directly with
    /// split field borrows instead.
    pub(crate) async fn send_message(&mut self, peer: PeerId, msg: &RaftMessage<'_>) -> bool {
        let mut iovs = self.scratch_vectored.take();
        let sent = send_message_vectored(
            &self.transport,
            self.config.node_id,
            &mut self.scratch_outbound,
            &mut iovs,
            peer,
            msg,
        )
        .await;
        self.scratch_vectored.put(iovs);
        sent
    }
}

/// Encode `msg` (headers into `header_buf`, payload slices by reference) and
/// send it as one vectored write. Free function over the *disjoint* pieces of
/// a node so callers whose message borrows other node fields (e.g. storage
/// views on the seeded zerocopy path) can invoke it under normal split-field
/// borrow checking instead of laundering lifetimes to call `&mut self`.
// header_buf is grown by encode_message_vectored; a &mut [u8] slice can't.
#[allow(clippy::ptr_arg)]
pub(crate) async fn send_message_vectored<'a, T>(
    transport: &T,
    from: PeerId,
    header_buf: &'a mut Vec<u8>,
    iovs: &mut Vec<&'a [u8]>,
    peer: PeerId,
    msg: &RaftMessage<'a>,
) -> bool
where
    T: crate::RaftTransport,
{
    iovs.clear();
    if crate::protocol::encode_message_vectored(from, msg, header_buf, iovs).is_err() {
        iovs.clear();
        return false;
    }
    let ok = transport.send_vectored(peer, iovs).await.is_ok();
    iovs.clear();
    ok
}

/// Protocol term carried by an inbound message, or `None` for term-less
/// frames (Custom / CustomResponse). Used by the A8 / PS8 check-quorum
/// contact filter: only current-term frames from voters refresh the
/// leader's quorum lease.
fn message_term(msg: &RaftMessage<'_>) -> Option<Term> {
    match msg {
        RaftMessage::RequestVote(m) | RaftMessage::PreVote(m) => Some(Term(m.term.get())),
        RaftMessage::RequestVoteResp(m) | RaftMessage::PreVoteResp(m) => Some(Term(m.term.get())),
        RaftMessage::AppendEntries(m, _)
        | RaftMessage::AppendEntriesVectored(m, _)
        | RaftMessage::AppendEntriesSeeded { ae: m, .. }
        | RaftMessage::AppendEntriesSeededVectored { ae: m, .. } => Some(Term(m.term.get())),
        RaftMessage::AppendEntriesResp(m) => Some(Term(m.term.get())),
        RaftMessage::InstallSnapshot(m, _) => Some(Term(m.term.get())),
        RaftMessage::InstallSnapshotResp(m) => Some(Term(m.term.get())),
        RaftMessage::TimeoutNow(m) => Some(Term(m.term.get())),
        RaftMessage::Custom(_) | RaftMessage::CustomResponse(_) => None,
    }
}

pub(crate) fn quorum(nodes: usize) -> usize {
    (nodes / 2) + 1
}

pub(crate) fn trace_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var_os("ARBITRO_RAFT_TRACE").is_some())
}

pub(crate) fn trace_log(node_id: PeerId, msg: impl AsRef<str>) {
    if trace_enabled() {
        tracing::trace!(node = node_id.0, "{}", msg.as_ref());
    }
}

impl<S, T> RaftNode<S, T>
where
    S: crate::RaftStorage,
    T: crate::RaftTransport,
{
    /// Whether this leader's quorum lease is still alive: a majority of the
    /// voter set (self included) has been heard from — via a CURRENT-term
    /// frame (A8 / PS8) — within the last election timeout. The run loop
    /// steps the leader down when this returns `false`. Learners (A13) are
    /// excluded on both sides: the loop below walks `config.peers` only,
    /// and `handle_inbound` never stamps a learner's contact.
    ///
    /// dup-F1: the majority decision is [`RaftNode::voter_majority`] — the
    /// SAME joint-aware predicate elections, ReadIndex confirmation, and
    /// pre-vote use. During a §4.3 joint transition the lease therefore
    /// requires a majority of C_old AND a majority of C_new independently;
    /// a union-only majority must not keep a leader alive that could not
    /// commit or be re-elected under the same contact set.
    ///
    /// Exposed (hidden from docs) purely so correctness tests can assert the
    /// contact-filter behavior without spinning up a full cluster; production
    /// callers must let the run loop drive it.
    #[doc(hidden)]
    pub fn check_quorum_active(&mut self) -> bool {
        let timeout = self.election_timeout();
        let now = std::time::Instant::now();

        // Gather recently-heard-from voter IDENTITIES (not a counter) into
        // the reusable scratch — the joint arm of `voter_majority` needs to
        // know WHICH side each contact belongs to. No allocation: the
        // scratch vec is capacity-docked on the node.
        self.scratch_peers.clear();
        self.scratch_peers.push(self.config.node_id); // self is always active
        for &peer in &self.config.peers {
            if peer != self.config.node_id {
                if let Some(&last_contact) = self.last_voter_contact.get(&peer) {
                    if now.saturating_duration_since(last_contact) < timeout {
                        self.scratch_peers.push(peer);
                    }
                }
            }
        }
        self.voter_majority(&self.scratch_peers)
    }
}
