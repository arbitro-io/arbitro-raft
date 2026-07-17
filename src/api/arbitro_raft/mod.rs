use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use futures::channel::mpsc;

use crate::{
    DispatchContextView, DispatchHandle, DispatchSpec, LogIndex, PeerId, RaftError, RaftNode,
    RaftStorage, RaftTransport, Role, StateMachine,
};

mod abuse;
mod client;
mod run;
mod slot;
mod timers;

use client::{ClientProposal, CommitWaiter};
use slot::{SlotId, SlotRegistry};
use timers::seed;

// Re-export the public types from this module.
pub use client::ClientHandle;

// ---------------------------------------------------------------------------
// ArbitroRaft — execution loop, batching, timers, client backpressure.
// ---------------------------------------------------------------------------

pub struct ArbitroRaft<S, T, SM>
where
    S: RaftStorage,
    T: RaftTransport,
    SM: StateMachine,
{
    pub(crate) node: RaftNode<S, T>,
    /// User-supplied state machine — committed entries are applied to
    /// it in-order from inside the run loop.
    pub(crate) state_machine: SM,
    /// Reusable scratch for `RaftStorage::entry_at` during apply.
    /// Sized to match the storage-read scratch style used elsewhere
    /// (16MB). If a payload exceeds this, `entry_at` returns an error
    /// which is propagated.
    pub(crate) apply_buf: Vec<u8>,
    stopped: bool,
    pub(crate) next_election_at: Instant,
    pub(crate) next_heartbeat_at: Instant,
    pub(crate) election_state: u64,
    client_tx: mpsc::UnboundedSender<ClientProposal>,
    pub(crate) client_rx: mpsc::UnboundedReceiver<ClientProposal>,
    /// Shared registry for commit notifications.
    pub(crate) registry: Arc<SlotRegistry>,
    /// Scratch — payloads drained from client_rx this tick, cleared before each use.
    pub(crate) pending_batch: Vec<Vec<u8>>,
    /// Scratch — slots parallel to pending_batch, drained together.
    pub(crate) pending_slots: Vec<SlotId>,
    /// Entries replicated but not yet committed; resolved as commit_index advances.
    pub(crate) commit_waiters: Vec<CommitWaiter>,
    /// Long-lived inbound buffer to avoid per-frame allocations.
    pub(crate) inbound_buf: Box<[u8]>,
    /// Per-peer decode-error accounting + jail policy (D3). Frames from a
    /// jailed peer are shed before decode in `dispatch_inbound`.
    pub(crate) abuse: abuse::InboundAbuseGuard,
}

// User payloads whose first byte matches the config-change magic would be
// decoded as control entries by the apply path and silently mutate the
// voter set. Reject them at every user-facing propose entry; internal
// config-change proposals call `self.node.propose_once` directly and
// bypass this check.
#[inline]
fn reject_reserved_prefix(payload: &[u8]) -> Result<(), RaftError> {
    // Frame-size contract (P1-4): reject an over-large payload at propose time.
    // Without this, a payload that fits the leader but not a peer's fixed
    // `inbound_buf` would commit locally and then silently never replicate.
    if payload.len() > crate::protocol::codec::wire::MAX_ENTRY_PAYLOAD {
        return Err(RaftError::InvalidPayload(
            "payload exceeds MAX_ENTRY_PAYLOAD and would not fit a peer's frame buffer",
        ));
    }
    if payload.first().copied() == Some(crate::api::node::membership::CONFIG_CHANGE_MAGIC) {
        return Err(RaftError::InvalidPayload(
            "payload first byte collides with reserved config-change magic (0xC0)",
        ));
    }
    Ok(())
}

// --- Public API --------------------------------------------------------------

impl<S, T, SM> ArbitroRaft<S, T, SM>
where
    S: RaftStorage,
    T: RaftTransport,
    SM: StateMachine,
{
    pub fn new(node: RaftNode<S, T>, state_machine: SM) -> Self {
        let (client_tx, client_rx) = mpsc::unbounded();
        // 65k slots = 4MB RAM — absorbs extreme concurrent bursts without backpressure.
        let registry_cap = 65536;
        let mut raft = Self {
            election_state: seed(node.node_id()),
            node,
            state_machine,
            // 16MB matches the payload scratch style used inside RaftNode.
            apply_buf: vec![0u8; 16 * 1024 * 1024],
            stopped: false,
            next_election_at: Instant::now(),
            next_heartbeat_at: Instant::now(),
            client_tx,
            client_rx,
            registry: SlotRegistry::new(registry_cap),
            pending_batch: Vec::with_capacity(4096),
            pending_slots: Vec::with_capacity(4096),
            commit_waiters: Vec::with_capacity(4096),
            inbound_buf: vec![0u8; crate::protocol::codec::wire::MAX_FRAME_SIZE]
                .into_boxed_slice(),
            abuse: abuse::InboundAbuseGuard::default(),
        };
        raft.reset_election_deadline();
        raft.reset_heartbeat_deadline();
        raft
    }

    #[inline]
    pub fn node(&self) -> &RaftNode<S, T> {
        &self.node
    }
    #[inline]
    pub fn node_mut(&mut self) -> &mut RaftNode<S, T> {
        &mut self.node
    }
    /// Shared reference to the state machine.
    #[inline]
    pub fn state_machine(&self) -> &SM {
        &self.state_machine
    }
    /// Mutable reference to the state machine. Callers must NOT mutate
    /// applied state directly; use it for snapshot/introspection only.
    #[inline]
    pub fn state_machine_mut(&mut self) -> &mut SM {
        &mut self.state_machine
    }
    #[inline]
    pub fn role(&self) -> Role {
        self.node.role()
    }
    #[inline]
    pub fn commit_index(&self) -> LogIndex {
        self.node.commit_index()
    }
    /// Who this node currently believes leads the cluster (`None` if unknown).
    /// The getter an operator or a `NotLeader`-redirect path calls to locate
    /// the leader.
    #[inline]
    pub fn leader_id(&self) -> Option<PeerId> {
        self.node.leader_id()
    }
    /// Consistent point-in-time snapshot of this node's consensus state for
    /// health checks and dashboards — see [`crate::RaftStatus`].
    #[inline]
    pub fn status(&self) -> crate::RaftStatus {
        self.node.status()
    }
    /// Clone the node's lifecycle counters ([`crate::RaftMetrics`]). Clone this
    /// before moving the node into its run task to poll it concurrently.
    #[inline]
    pub fn metrics(&self) -> crate::RaftMetrics {
        self.node.metrics()
    }

    /// Cheaply-clonable read-only observer over the committed log index.
    ///
    /// Clone this before moving `self` into a background task so external
    /// consumers (for example an apply loop that lives in another task)
    /// can safely poll the current commit boundary without holding the
    /// node.
    #[inline]
    pub fn commit_index_observer(&self) -> crate::CommitIndexObserver {
        self.node.commit_index_observer()
    }
    #[inline]
    pub fn node_id(&self) -> PeerId {
        self.node.node_id()
    }
    /// Stop the run loop and unblock every writer waiting on a commit.
    ///
    /// Safe to call more than once — subsequent calls are no-ops. Without
    /// this, `WriteFuture`s parked on slots for proposals still sitting in
    /// `client_rx` or `commit_waiters` would poll `SLOT_PENDING` forever
    /// once the run loop stops ticking.
    pub fn stop(&mut self) {
        if self.stopped {
            return;
        }
        self.stopped = true;
        // Close the client channel so any late `ClientHandle::write` fails fast
        // with "raft node stopped" instead of parking its slot forever once the
        // run loop no longer ticks (PS10). Buffered proposals are still drainable
        // below.
        self.client_rx.close();
        while let Ok(proposal) = self.client_rx.try_recv() {
            self.registry.get(proposal.slot_id).notify_error();
        }
        self.fail_commit_waiters();
    }

    /// Gracefully drain this node for shutdown (A12) — the rolling-restart
    /// primitive. Where [`stop`](Self::stop) makes a stopping LEADER simply
    /// vanish (the cluster then eats a full election timeout before a new
    /// leader emerges), `drain` hands leadership off first:
    ///
    /// 1. **Freeze intake** — the client channel is closed immediately, so any
    ///    late [`ClientHandle::write`] fails fast with "raft node stopped".
    ///    Proposals already buffered in the channel are failed deterministically
    ///    (they were never acknowledged as appended).
    /// 2. **Leader handoff** — if this node leads, any batch parked from a
    ///    previous tick is replicated first (so its waiters register), then the
    ///    most caught-up voter is picked (highest `match_index`, the same
    ///    selection a self-removing leader uses) and
    ///    [`transfer_leadership`](Self::transfer_leadership) hands off to it.
    ///    The drain then keeps servicing inbound frames (bounded by twice the
    ///    election timeout) until the target's higher-term campaign deposes
    ///    this node — commit acks arriving in that window still resolve their
    ///    `commit_waiters` as COMMITTED.
    /// 3. **Fallback** — if no voter can take over (single-node cluster, no
    ///    reachable caught-up peer, transfer timeout), a warning is logged
    ///    that the cluster will eat a full election timeout, and the drain
    ///    degrades to a clean [`stop`](Self::stop).
    /// 4. **Deterministic waiter resolution** — after the handoff window every
    ///    waiter whose entry committed has been notified with its `LogIndex`;
    ///    [`stop`](Self::stop) then fails every remaining waiter. No
    ///    `WriteFuture` is ever left parked forever.
    ///
    /// Always finishes with the node stopped; the next
    /// [`run_once`](Self::run_once) returns `Ok(false)` and a surrounding
    /// [`run`](Self::run) loop returns `Ok(())` — a drained shutdown is a
    /// CLEAN exit, never an error.
    ///
    /// # Concurrency model (how a server wires this in — L10 seam)
    ///
    /// `drain` takes `&mut self`, so it cannot run concurrently with a
    /// `run()` call that owns `&mut self` across its whole loop. The
    /// supported deployment pattern is the tick-driver: hold the node in an
    /// `Arc<Mutex<ArbitroRaft<..>>>`, drive it with `run_once()` per lock
    /// acquisition, and on a shutdown signal lock the same mutex and call
    /// `drain().await`. The driver's next `run_once()` observes the stopped
    /// flag and returns `Ok(false)`, ending the loop cleanly. Calling `drain`
    /// twice (or after `stop`) is a no-op returning `Ok(())`.
    ///
    /// Errors: only [`ErrorClass::Fatal`](crate::ErrorClass::Fatal) errors
    /// (local storage failure / corrupt log) propagate — and even then the
    /// node is left stopped with all waiters failed. Every non-fatal problem
    /// (transfer timeout, transport hiccup, deposed mid-drain) degrades to the
    /// clean-stop fallback and returns `Ok(())`.
    pub async fn drain(&mut self) -> Result<(), RaftError> {
        if self.stopped {
            return Ok(());
        }
        // Freeze intake FIRST — nothing new may enter while we hand off.
        // Buffered proposals are failed by `stop()` at the end; failing (not
        // appending) them is deterministic: they were never acknowledged.
        self.client_rx.close();

        let mut fatal: Option<RaftError> = None;
        if self.node.is_leader() {
            fatal = self.drain_leader_handoff().await.err();
        }

        // Resolve every waiter whose entry is known committed, apply what
        // committed, then stop() fails the rest deterministically. Skipped
        // only on a fatal storage error — stop() still unparks everything.
        if fatal.is_none() {
            if let Err(e) = self.apply_committed_entries() {
                fatal = Some(e);
            } else {
                self.drain_commit_waiters();
            }
        }
        self.stop();
        match fatal {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Leader-side half of [`drain`](Self::drain): flush the parked batch,
    /// hand leadership to the most caught-up voter, then service inbound
    /// frames until deposed (bounded). Non-fatal failures degrade to the
    /// clean-stop fallback (with a loud warning); only Fatal errors propagate.
    async fn drain_leader_handoff(&mut self) -> Result<(), RaftError> {
        // Flush the batch parked from a previous tick so its waiters register
        // in `commit_waiters` before the handoff freeze.
        if !self.pending_batch.is_empty() {
            if let Err(e) = self.replicate_pending().await {
                if e.is_fatal() {
                    return Err(e);
                }
                tracing::warn!(
                    node_id = self.node.node_id().0,
                    error = %e,
                    "drain: failed to flush parked batch; its waiters were failed"
                );
            }
        }

        // Most caught-up voter — same selection rule as a self-removing
        // leader in `propose_config_change` (§4.2.3 / A2).
        let self_id = self.node.node_id();
        let target = self
            .node
            .peers()
            .iter()
            .copied()
            .filter(|p| *p != self_id)
            .max_by_key(|p| {
                self.node
                    .peer_progress(*p)
                    .map(|(_next, matched)| matched.0)
                    .unwrap_or(0)
            });
        let Some(target) = target else {
            // Single-node cluster — nobody to hand off to; clean stop is
            // already graceful (no other node waits on an election).
            return Ok(());
        };

        match self.transfer_leadership(target).await {
            Ok(()) => {}
            Err(e) if e.is_fatal() => return Err(e),
            Err(RaftError::NotLeader { .. }) => {
                // Deposed while catching the target up — leadership already
                // moved on its own; nothing left to hand off.
                return Ok(());
            }
            Err(e) => {
                tracing::warn!(
                    node_id = self.node.node_id().0,
                    target = target.0,
                    error = %e,
                    "drain: leadership transfer failed; stopping anyway — \
                     the cluster will eat a full election timeout"
                );
                return Ok(());
            }
        }

        // TimeoutNow sent — keep servicing inbound frames until the target's
        // higher-term campaign deposes us (or the handoff window expires).
        // Commit acks landing in this window still resolve their waiters as
        // COMMITTED instead of being failed by the final stop().
        let deadline = Instant::now() + self.node.election_timeout() * 2;
        while self.node.is_leader() && Instant::now() < deadline {
            let slice = deadline
                .saturating_duration_since(Instant::now())
                .min(std::time::Duration::from_millis(10));
            if let Some(n) = self.recv_inbound(slice).await? {
                self.dispatch_inbound(n).await?;
                self.node.try_advance_commit_index()?;
                self.drain_commit_waiters();
            }
        }
        if self.node.is_leader() {
            tracing::warn!(
                node_id = self.node.node_id().0,
                target = target.0,
                "drain: transfer target never campaigned within the handoff window; \
                 stopping as leader — the cluster will eat a full election timeout"
            );
        } else {
            tracing::info!(
                node_id = self.node.node_id().0,
                target = target.0,
                "drain: leadership handed off before shutdown"
            );
        }
        Ok(())
    }

    /// Returns a clonable [`ClientHandle`] for concurrent writes from multiple tasks.
    #[inline]
    pub fn client_handle(&self) -> ClientHandle {
        ClientHandle {
            tx: self.client_tx.clone(),
            registry: self.registry.clone(),
        }
    }

    /// Direct single-entry propose — caller holds `&mut self` (e.g. benchmarks, tests).
    #[inline]
    pub async fn propose_once(&mut self, payload: &[u8]) -> Result<LogIndex, RaftError> {
        reject_reserved_prefix(payload)?;
        self.node.propose_once(payload).await
    }

    /// Linearizable read point (Raft §6.4 ReadIndex) — A11.
    ///
    /// Leader-only. On success the returned `LogIndex` is a linearization
    /// point: this method has already applied every committed entry up to (at
    /// least) that index, so a subsequent [`state_machine`](Self::state_machine)
    /// read reflects every write that completed before this call. Callers
    /// that read a REMOTE state machine must instead wait until that SM has
    /// applied `>= read_index` before reading.
    ///
    /// Protocol: records `read_index = commit_index`, confirms
    /// still-leadership with a majority heartbeat round bounded by one
    /// election timeout, then applies committed entries locally. A fresh
    /// leader that has not yet committed an entry of its own term first
    /// commits an internal no-op (never visible to the user state machine)
    /// to make `commit_index` a safe read point — see
    /// [`crate::RaftNode::read_index`] for the full contract.
    ///
    /// Errors: [`RaftError::NotLeader`] (with redirect hint) on a follower,
    /// during a §4.2.3 transfer freeze, or when deposed mid-confirmation;
    /// [`RaftError::NoQuorum`] when a majority cannot be contacted in time —
    /// a partitioned or deposed leader REFUSES the read, it never serves a
    /// possibly-stale one.
    ///
    /// A11: lease-read variant is future work (needs a monotonic-clock
    /// lease; ReadIndex is the safe default).
    pub async fn read_index(&mut self) -> Result<LogIndex, RaftError> {
        let read_index = self.node.read_index(&mut self.inbound_buf).await?;
        // Apply wait (§6.4 step 4): bring the LOCAL state machine to the read
        // point. `commit_index >= read_index` already holds and committed
        // entries are locally durable, so one apply pass suffices (a snapshot
        // restore inside it re-anchors even further ahead).
        self.apply_committed_entries()?;
        // The internal no-op (or frames handled during confirmation) may have
        // advanced the commit index past parked client writes — resolve them
        // now rather than on the next tick.
        self.drain_commit_waiters();
        if self.node.last_applied() < read_index {
            // Unreachable by construction; refuse the read rather than let a
            // caller read a state machine that lags the confirmed point.
            return Err(RaftError::Protocol(
                "read_index: local apply lagged behind the confirmed read point".into(),
            ));
        }
        Ok(read_index)
    }

    /// Direct batch propose — caller holds `&mut self`.
    #[inline]
    pub async fn propose_batch_once(
        &mut self,
        payloads: &[&[u8]],
    ) -> Result<&[LogIndex], RaftError> {
        for p in payloads {
            reject_reserved_prefix(p)?;
        }
        self.node.propose_batch_once(payloads).await
    }

    /// Propose a joint-consensus membership change transitioning the
    /// cluster from the current voter set to `new_peers`.
    ///
    /// Semantics: appends TWO entries to the log — first the joint
    /// configuration `C_old_new` (safe under both quorums), then, once
    /// that entry commits, the final configuration `C_new`. Returns the
    /// `LogIndex` of the FINAL entry.
    ///
    /// This method drives the transition to completion before returning.
    /// If the leader steps down mid-transition, the underlying
    /// `propose_once` returns `RaftError::NotLeader` and the caller
    /// should retry against the new leader.
    pub async fn propose_config_change(
        &mut self,
        new_peers: Vec<PeerId>,
    ) -> Result<LogIndex, RaftError> {
        use crate::api::node::membership::{ConfigChangeEntry, ConfigChangePhase};

        if !self.node.is_leader() {
            return Err(RaftError::NotLeader {
                leader_hint: self
                    .node
                    .hard_state()
                    .voted_for
                    .map(|leader_id| crate::LeaderHint { leader_id }),
            });
        }

        // A10 / OPS-3: reject overlapping membership changes. The §4.3 joint
        // consensus safety argument covers exactly ONE transition at a time;
        // stacking a second change while `C_old,new` is still active (appended
        // but its `C_new` not yet applied locally) is outside the proof and
        // can elect leaders under disagreeing voter sets. The transition
        // completes when the Final entry is applied (clearing `joint_peers`),
        // or is superseded by a new leader's log truncation.
        if self.node.joint_peers.is_some() {
            return Err(RaftError::InvalidConfig(
                "config change already in progress",
            ));
        }

        // Reject empty new_peers up-front — a Final(C_new) with an empty
        // voter set is unrecoverable (no quorum can ever be formed).
        if new_peers.is_empty() {
            return Err(RaftError::InvalidConfig(
                "config-change new_peers must be non-empty",
            ));
        }

        let old_peers = self.node.peers().to_vec();

        // §4.2.3 — a leader that is removing ITSELF must not drive the
        // transition to completion: the moment it applies `C_new` it steps
        // down without a successor, and the cluster eats a full election
        // timeout mid-membership-change (worse: the resulting election may
        // be won by a peer that has not yet learned `C_new`). Instead, hand
        // leadership to the most caught-up voter that SURVIVES into the new
        // configuration and redirect the caller there — the removal then
        // commits under a leader that stays in `C_new`.
        let self_id = self.node.node_id();
        if !new_peers.contains(&self_id) {
            // B13: `new_peers` was rejected-if-empty above and `self_id` is
            // not in it, so the filtered max is always present.
            #[allow(clippy::expect_used)]
            let target = new_peers
                .iter()
                .copied()
                .filter(|p| *p != self_id)
                .max_by_key(|p| {
                    self.node
                        .peer_progress(*p)
                        .map(|(_next, matched)| matched.0)
                        .unwrap_or(0)
                })
                .expect("new_peers verified non-empty above");
            self.transfer_leadership(target).await?;
            return Err(RaftError::NotLeader {
                leader_hint: Some(crate::LeaderHint { leader_id: target }),
            });
        }

        // Require at least one voter to appear in both old and new sets.
        // A fully disjoint transition drops availability during the joint
        // phase to zero on either side and provides no witness carrying
        // committed history across the cut.
        let overlap = new_peers.iter().any(|p| old_peers.contains(p));
        if !overlap {
            return Err(RaftError::InvalidConfig(
                "config-change new_peers must overlap current peers",
            ));
        }

        // Phase 1 — joint entry (C_old_new).
        let joint = ConfigChangeEntry {
            phase: ConfigChangePhase::Joint,
            old_peers: old_peers.clone(),
            new_peers: new_peers.clone(),
        };
        let joint_bytes = joint.encode();

        // Append-time activation, leader side (§4.3, closes G4/A3).
        // Followers adopt a config-change entry the moment it is APPENDED
        // (see `apply_append_entries` in replication/handler.rs); the
        // leader must do the same so that the Joint entry's OWN commit is
        // decided under the dual quorum (majority-of-old AND
        // majority-of-new) — activating only after `propose_once` returns
        // would let the Joint entry commit under the old majority alone,
        // and an entry committed without a majority of `C_new` can be
        // lost to a future leader elected inside the new configuration.
        //
        // Activating here is append-time in effect: `propose_once`
        // performs the log append synchronously before its first await,
        // and we hold `&mut self`, so no other entry can interleave
        // between activation and append. If the propose fails BEFORE the
        // entry reaches the log (transfer freeze / lost leadership), the
        // activation is rolled back; if the entry IS in the log the joint
        // config stays active even on a commit timeout — the same rule a
        // follower applies to an appended-but-uncommitted config entry
        // (the entry may still commit, or a new leader's truncation
        // supersedes it).
        //
        // This cannot step the leader down: the joint union always
        // contains `self` (self ∈ new_peers was verified above), and a
        // self-removing leader already transferred leadership and
        // returned before this point (§4.2.3 / A2).
        let tip_before = self.node.cached_last_log.0;
        self.node.apply_config_change(&joint)?;
        if let Err(e) = self.node.propose_once(&joint_bytes).await {
            if self.node.cached_last_log.0 == tip_before {
                // Joint entry never appended — undo the activation.
                self.node.revert_joint_activation(old_peers);
            }
            return Err(e);
        }

        // Phase 2 — final entry (C_new). The joint voter set is already
        // active (append-time activation above), so this commit also
        // proceeds under the dual quorum, which is the safety
        // requirement of §4.3.
        let final_entry = ConfigChangeEntry {
            phase: ConfigChangePhase::Final,
            old_peers,
            new_peers,
        };
        let final_bytes = final_entry.encode();
        let final_idx = self.node.propose_once(&final_bytes).await?;

        // Farewell commit advertisement. `C_new` has committed but is NOT
        // yet applied locally, so `config.peers` still holds the joint
        // union — this heartbeat therefore reaches the peers being REMOVED
        // and carries `leader_commit >= final_idx`. Without it, the local
        // apply (next run-loop tick) drops removed peers from the fan-out
        // before they ever learn the final entry committed, and a removed
        // node is left forever carrying the stale voter set (it never
        // self-removes, times out, and disrupts the cluster with campaigns
        // that pre-vote must then contain). One frame on a healthy link is
        // enough; on a lossy link this stays best-effort — the removed
        // node's disruption is bounded by pre-vote either way.
        if self.node.is_leader() {
            self.send_heartbeat_once().await?;
        }
        Ok(final_idx)
    }

    /// Add `peer` to the cluster as a LEARNER — a non-voting member (A13).
    ///
    /// The leader replicates log entries to a learner exactly as to a
    /// follower (AppendEntries backlog repair, snapshot catch-up), but the
    /// learner is excluded from EVERY quorum: commit-index advancement,
    /// election vote counts, check-quorum contact, and ReadIndex
    /// confirmation. Adding a learner therefore changes NO commit or
    /// election threshold — a 3-voter cluster still commits on 2 voter acks
    /// with any number of learners attached. This is the safe staging step
    /// for growing a cluster: let the newcomer catch up as a learner, then
    /// [`promote_learner`](Self::promote_learner) once
    /// [`learner_caught_up`](Self::learner_caught_up) reports `true`.
    ///
    /// Replicated as a SINGLE control entry, not a §4.3 joint pair: joint
    /// consensus exists to bridge two configurations whose MAJORITIES could
    /// otherwise disagree, and a learner participates in no majority — the
    /// voter sets before and after this change are identical, so there is
    /// nothing for a joint phase to protect. (etcd's learner add is a
    /// single-entry change for the same reason.) The entry is adopted at
    /// append time on every node (§4.1), so a new leader keeps replicating
    /// to the learner across leadership changes.
    ///
    /// The learner node itself boots with its own id in
    /// [`crate::NodeConfig::learners`] (not in `peers`); it never campaigns.
    ///
    /// Errors: [`RaftError::NotLeader`] on a non-leader;
    /// [`RaftError::InvalidConfig`] while a joint voter transition is in
    /// flight (A10 one-change-at-a-time, conservatively extended to learner
    /// ops), or when `peer` is already a voter or a learner.
    pub async fn add_learner(&mut self, peer: PeerId) -> Result<LogIndex, RaftError> {
        self.propose_learner_change(peer, true).await
    }

    /// Remove learner `peer` from the cluster (A13). Single control entry —
    /// no quorum changes (see [`add_learner`](Self::add_learner)). The
    /// leader stops replicating to the peer once the entry is applied.
    ///
    /// Errors: [`RaftError::NotLeader`] on a non-leader;
    /// [`RaftError::InvalidConfig`] while a joint voter transition is in
    /// flight, or when `peer` is not a learner.
    pub async fn remove_learner(&mut self, peer: PeerId) -> Result<LogIndex, RaftError> {
        self.propose_learner_change(peer, false).await
    }

    /// Shared body of [`add_learner`] / [`remove_learner`]: validate,
    /// activate the learner-set change locally at append time (mirroring the
    /// joint activation in [`propose_config_change`]), then commit the
    /// control entry through the normal propose path. If the propose fails
    /// BEFORE the entry reaches the log, the local activation is reverted;
    /// if the entry IS in the log it stays active (§4.1 append-time rule).
    ///
    /// [`add_learner`]: Self::add_learner
    /// [`remove_learner`]: Self::remove_learner
    /// [`propose_config_change`]: Self::propose_config_change
    async fn propose_learner_change(
        &mut self,
        peer: PeerId,
        add: bool,
    ) -> Result<LogIndex, RaftError> {
        use crate::api::node::membership::{ConfigChangeEntry, ConfigChangePhase};

        if !self.node.is_leader() {
            return Err(RaftError::NotLeader {
                leader_hint: self
                    .node
                    .leader_id()
                    .map(|leader_id| crate::LeaderHint { leader_id }),
            });
        }
        // A10 one-change-at-a-time, conservatively extended: while a joint
        // voter transition is active the effective configuration is already
        // in a two-phase hand-off; interleaving a learner-set change adds no
        // value and needless proof surface. Learner changes are cheap to
        // retry after the transition completes.
        if self.node.joint_peers.is_some() {
            return Err(RaftError::InvalidConfig(
                "config change already in progress",
            ));
        }
        if add {
            if self.node.peers().contains(&peer) {
                return Err(RaftError::InvalidConfig(
                    "add_learner: peer is already a voter",
                ));
            }
            if self.node.learners().contains(&peer) {
                return Err(RaftError::InvalidConfig(
                    "add_learner: peer is already a learner",
                ));
            }
        } else if !self.node.learners().contains(&peer) {
            return Err(RaftError::InvalidConfig(
                "remove_learner: peer is not a learner",
            ));
        }

        let entry = ConfigChangeEntry {
            phase: if add {
                ConfigChangePhase::AddLearner
            } else {
                ConfigChangePhase::RemoveLearner
            },
            old_peers: Vec::new(),
            new_peers: vec![peer],
        };
        let bytes = entry.encode();

        // Append-time activation (§4.1), leader side: adopt the change NOW so
        // an added learner starts receiving replication with this very
        // propose's fan-out, and a removed learner stops immediately. Safe by
        // construction — no quorum reads the learner set, so an activation
        // that later loses to a truncation can never affect a commit or an
        // election. Revert only if the entry never reached the log.
        let prev_learners = self.node.learners().to_vec();
        let tip_before = self.node.cached_last_log.0;
        self.node.apply_config_change(&entry)?;
        match self.node.propose_once(&bytes).await {
            Ok(idx) => Ok(idx),
            Err(e) => {
                if self.node.cached_last_log.0 == tip_before {
                    self.node.revert_learner_activation(prev_learners);
                }
                Err(e)
            }
        }
    }

    /// Promote learner `peer` to a full VOTER (A13).
    ///
    /// This is the config change that actually grows the quorum, so it runs
    /// the full §4.3 joint-consensus voter transition
    /// ([`propose_config_change`](Self::propose_config_change) with
    /// `new_peers = current voters + peer`); the learner is stripped from
    /// the learner set the moment the Joint entry activates, keeping the
    /// two sets disjoint on every node.
    ///
    /// **Caught-up gate (operator contract)**: promotion is refused unless
    /// [`learner_caught_up`](Self::learner_caught_up) holds — i.e. the
    /// learner's `match_index` has reached the leader's `commit_index`.
    /// Promoting a caught-up learner never reduces availability: the new
    /// voter can immediately ack at the commit frontier, so the grown
    /// quorum is satisfiable at once. (Promoting a lagging member is the
    /// exact availability dip learners exist to avoid.) Poll the predicate
    /// and retry on refusal.
    ///
    /// Errors: [`RaftError::NotLeader`] on a non-leader;
    /// [`RaftError::InvalidConfig`] when `peer` is not a learner, when it
    /// is not yet caught up, or while another config change is in flight.
    pub async fn promote_learner(&mut self, peer: PeerId) -> Result<LogIndex, RaftError> {
        if !self.node.is_leader() {
            return Err(RaftError::NotLeader {
                leader_hint: self
                    .node
                    .leader_id()
                    .map(|leader_id| crate::LeaderHint { leader_id }),
            });
        }
        if !self.node.learners().contains(&peer) {
            return Err(RaftError::InvalidConfig(
                "promote_learner: peer is not a learner",
            ));
        }
        if !self.node.learner_caught_up(peer)? {
            return Err(RaftError::InvalidConfig(
                "promote_learner: learner not caught up (match_index < commit_index); retry later",
            ));
        }
        let mut new_peers = self.node.peers().to_vec();
        new_peers.push(peer);
        self.propose_config_change(new_peers).await
    }

    /// Learner (non-voting member) set this node currently knows (A13).
    #[inline]
    pub fn learners(&self) -> &[PeerId] {
        self.node.learners()
    }

    /// A13: whether learner `peer` has caught up far enough to be promoted —
    /// see [`crate::RaftNode::learner_caught_up`] for the exact predicate.
    #[inline]
    pub fn learner_caught_up(&self, peer: PeerId) -> Result<bool, RaftError> {
        self.node.learner_caught_up(peer)
    }

    /// Transfer leadership to `target` (Raft §4.2.3).
    ///
    /// Catches the target up (bounded by one election timeout), then sends it
    /// `TimeoutNow`; the target campaigns IMMEDIATELY at `term + 1`, skipping
    /// both the election-timeout wait and pre-vote (the transfer is
    /// leader-sanctioned). This node steps down through the normal
    /// higher-term path once the target's vote request arrives — keep driving
    /// [`run`](Self::run)/[`run_once`](Self::run_once) and observe
    /// [`status`](Self::status) for the outcome.
    ///
    /// While the handoff is pending, new proposals are rejected with
    /// [`RaftError::NotLeader`] whose hint points at `target`; proposals
    /// already queued on a [`ClientHandle`] stay parked until leadership
    /// resolves (identical to follower semantics).
    ///
    /// Errors: [`RaftError::NotLeader`] when not the leader,
    /// [`RaftError::PeerUnknown`] for a non-voter target, and
    /// [`RaftError::TransferTimeout`] when the target cannot catch up in time
    /// (leadership resumes unchanged — safe to retry).
    ///
    /// This is the primitive that clean node removal (a self-removing leader
    /// hands off before `C_new` commits) and graceful shutdown/drain build on.
    pub async fn transfer_leadership(&mut self, target: PeerId) -> Result<(), RaftError> {
        // Flush any batch parked from a previous tick first, so its waiters
        // cannot be stranded in `pending_batch` if the handoff succeeds and
        // this node stops leading.
        if self.node.is_leader() && !self.pending_batch.is_empty() {
            self.replicate_pending().await?;
        }
        self.node
            .transfer_leadership(target, &mut self.inbound_buf)
            .await
    }

    #[inline]
    pub async fn campaign_once(&mut self) -> Result<bool, RaftError> {
        let elected = self.node.campaign_once(&mut self.inbound_buf).await?;
        self.reset_election_deadline();
        if elected {
            self.reset_heartbeat_deadline();
        }
        Ok(elected)
    }

    #[inline]
    pub async fn send_heartbeat_once(&mut self) -> Result<(), RaftError> {
        let result = self.node.send_heartbeat_once().await;
        self.reset_heartbeat_deadline();
        result
    }

    #[inline]
    pub fn on_with<P, R, F>(&self, spec: DispatchSpec<P, R>, handler: F) -> Result<(), RaftError>
    where
        P: Send + 'static,
        R: 'static,
        F: for<'a> Fn(
                P,
                DispatchContextView<'a>,
            )
                -> Pin<Box<dyn Future<Output = Result<(), RaftError>> + Send + 'a>>
            + Send
            + Sync
            + 'static,
    {
        self.node.on_with(spec, handler)
    }

    #[inline]
    pub async fn dispatch<P, R>(
        &mut self,
        spec: DispatchSpec<P, R>,
        params: P,
    ) -> Result<DispatchHandle<R>, RaftError>
    where
        P: Send + 'static,
        R: Clone + Send + 'static,
    {
        self.node.dispatch(spec, params).await
    }

    /// Drive one tick of the consensus loop. Returns `Ok(false)` once the
    /// node has been stopped ([`stop`](Self::stop) / [`drain`](Self::drain)) —
    /// a clean shutdown, not an error.
    ///
    /// # Resource-exhaustion degradation (C8)
    ///
    /// A tick that fails with an [`ErrorClass::Resource`](crate::ErrorClass::Resource)
    /// error (disk full / quota / OOM — durable state intact, write refused,
    /// nothing acknowledged) does NOT terminate the node. Instead the tick:
    /// - increments [`RaftMetricsSnapshot::resource_exhausted`](crate::RaftMetricsSnapshot::resource_exhausted)
    ///   and logs at `error!` level,
    /// - if this node leads: demotes it to follower WITHOUT a hard-state
    ///   write (term kept, vote kept — the full disk cannot be required to
    ///   persist the step-down) and fails all pending commit waiters, so new
    ///   proposals are rejected with [`RaftError::NotLeader`] — read-only
    ///   survival,
    /// - returns `Ok(true)`: keep ticking.
    ///
    /// Recovery is automatic: the node keeps serving reads and inbound
    /// frames, and once a subsequent storage operation succeeds (space was
    /// freed) it campaigns/acks normally again — no restart needed. While
    /// storage stays exhausted, elections it starts fail at the hard-state
    /// persist and it simply remains a read-only follower. Corruption
    /// ([`RaftError::CorruptLog`] / [`RaftError::Storage`]) is untouched by
    /// this path — it stays [`ErrorClass::Fatal`](crate::ErrorClass::Fatal)
    /// and terminates the loop; degradation never masks corruption.
    pub async fn run_once(&mut self) -> Result<bool, RaftError> {
        if self.stopped {
            return Ok(false);
        }
        let tick = if self.node.is_leader() {
            self.run_leader_once().await
        } else {
            self.run_follower_once().await
        };
        match tick {
            Ok(()) => Ok(!self.stopped),
            Err(e) if e.is_resource_exhaustion() => {
                self.degrade_on_resource_exhaustion(&e);
                Ok(!self.stopped)
            }
            Err(e) => Err(e),
        }
    }

    /// C8: react to an [`ErrorClass::Resource`](crate::ErrorClass::Resource)
    /// tick failure — see [`run_once`](Self::run_once) for the full contract.
    fn degrade_on_resource_exhaustion(&mut self, e: &RaftError) {
        self.node.metrics.inc_resource_exhausted();
        if self.node.is_leader() {
            tracing::error!(
                node_id = self.node.node_id().0,
                term = self.node.current_term().0,
                error = %e,
                "storage resource exhaustion (disk full?): leader stepping down to \
                 read-only survival — rejecting proposals until storage recovers"
            );
            // Soft demotion: no hard-state write (the disk that just refused a
            // write cannot be required to persist a step-down), term and vote
            // untouched — always safe.
            self.node.demote_to_follower_keep_term();
            self.fail_commit_waiters();
        } else {
            tracing::error!(
                node_id = self.node.node_id().0,
                term = self.node.current_term().0,
                error = %e,
                "storage resource exhaustion (disk full?): degrading — this node \
                 will not ack or win elections until storage recovers"
            );
        }
    }

    /// Drive the consensus loop until stopped or a fatal error.
    ///
    /// Clean-stop contract (A12): after [`stop`](Self::stop) or
    /// [`drain`](Self::drain) the loop observes `run_once() == Ok(false)` and
    /// returns `Ok(())` — a graceful shutdown NEVER surfaces as an error.
    /// `Err(_)` exclusively means a fatal consensus/storage failure.
    ///
    /// Because `run` borrows `&mut self` for its whole lifetime, a shutdown
    /// path cannot call `drain` while `run` owns the node. Deployments that
    /// need graceful shutdown should use the tick-driver pattern instead:
    /// share the node behind `Arc<Mutex<..>>`, loop `run_once()` per lock
    /// acquisition, and call `drain().await` under the same mutex from the
    /// shutdown watch (see [`drain`](Self::drain)).
    pub async fn run(&mut self) -> Result<(), RaftError> {
        loop {
            match self.run_once().await {
                Ok(true) => continue,
                Ok(false) => return Ok(()),
                // A fatal error terminates the loop. It must be logged loudly —
                // callers commonly `tokio::spawn(raft.run())` and drop the
                // JoinHandle, so a silent return would leave a dead node the
                // process never notices.
                Err(e) => {
                    tracing::error!(
                        node_id = self.node.node_id().0,
                        term = self.node.current_term().0,
                        class = ?e.class(),
                        error = %e,
                        "raft run loop terminated"
                    );
                    return Err(e);
                }
            }
        }
    }
}

impl<S, T, SM> Drop for ArbitroRaft<S, T, SM>
where
    S: RaftStorage,
    T: RaftTransport,
    SM: StateMachine,
{
    fn drop(&mut self) {
        self.stop();
    }
}
