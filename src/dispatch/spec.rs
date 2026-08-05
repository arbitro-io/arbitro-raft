use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::dispatch::DispatchBuilder;
use crate::RaftError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispatchScope {
    All = 0,
    Others = 1,
    Followers = 2,
    Leader = 3,
    LocalOnly = 4,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispatchNodeRole {
    Leader,
    Follower,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DispatchRoute {
    pub role: DispatchNodeRole,
    pub is_origin: bool,
}

impl DispatchRoute {
    pub const fn leader(is_origin: bool) -> Self {
        Self {
            role: DispatchNodeRole::Leader,
            is_origin,
        }
    }

    pub const fn follower(is_origin: bool) -> Self {
        Self {
            role: DispatchNodeRole::Follower,
            is_origin,
        }
    }
}

impl DispatchScope {
    pub const fn allows(self, route: DispatchRoute) -> bool {
        match self {
            DispatchScope::All => true,
            DispatchScope::Others => !route.is_origin,
            DispatchScope::Followers => matches!(route.role, DispatchNodeRole::Follower),
            DispatchScope::Leader => matches!(route.role, DispatchNodeRole::Leader),
            DispatchScope::LocalOnly => route.is_origin,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispatchAckPolicy {
    All = 0,
    Quorum = 1,
    AtLeast = 2,
    Percent = 3,
    BestEffort = 4,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispatchFailPolicy {
    AllowFailures = 0,
    NoFailures = 1,
    MaxFailures = 2,
    MaxFailurePercent = 3,
    FailFast = 4,
}

/// Acknowledgement policy with its associated thresholds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DispatchAckOptions {
    pub policy: DispatchAckPolicy,
    /// Minimum number of accepts required (used by `AtLeast`).
    pub count: u16,
    /// Minimum percentage of accepts required (used by `Percent`).
    pub percent: u8,
}

/// Failure policy with its associated thresholds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DispatchFailOptions {
    pub policy: DispatchFailPolicy,
    /// Maximum number of failures allowed (used by `MaxFailures`).
    pub count: u16,
    /// Maximum percentage of failures allowed (used by `MaxFailurePercent`).
    pub percent: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DispatchOptions {
    pub scope: DispatchScope,
    pub ack: DispatchAckOptions,
    pub fail: DispatchFailOptions,
    pub timeout: Duration,
    pub trace: bool,
}

impl Default for DispatchAckOptions {
    fn default() -> Self {
        Self {
            policy: DispatchAckPolicy::Quorum,
            count: 1,
            percent: 100,
        }
    }
}

impl Default for DispatchFailOptions {
    fn default() -> Self {
        Self {
            policy: DispatchFailPolicy::AllowFailures,
            count: 0,
            percent: 0,
        }
    }
}

impl Default for DispatchOptions {
    fn default() -> Self {
        Self {
            scope: DispatchScope::All,
            ack: DispatchAckOptions::default(),
            fail: DispatchFailOptions::default(),
            timeout: Duration::from_secs(2),
            trace: false,
        }
    }
}

/// Weyl-style mixing constant (2^64 / golden ratio, odd). Scatters the
/// per-instance tx-id seeds across the id space so two independently
/// constructed specs never start on overlapping ranges. Because the
/// constant is odd, `n * TX_ID_SEED_MIX` is injective modulo 2^56: every
/// construction epoch maps to a distinct 56-bit starting offset.
const TX_ID_SEED_MIX: u64 = 0x9E37_79B9_7F4A_7C15;

/// Low 56 bits of a tx id hold the per-instance sequence; the high 8 bits
/// hold the command byte (see [`DispatchSpec::next_tx_id`]).
const TX_ID_SEQ_MASK: u64 = (1 << 56) - 1;

/// Construction-epoch allocator for tx-id seeds. Bumped exactly ONCE per
/// `DispatchSpec::new` (cold path: specs are built once per command per
/// group) and never touched again — `next_tx_id` only ever hits the
/// instance's own counter, so the H3 share-nothing property of the
/// dispatch hot path is intact: no cross-group cache-line contention,
/// no process-global id space.
static TX_ID_SEED_EPOCH: AtomicU64 = AtomicU64::new(0);

pub struct DispatchSpec<P, R> {
    command: u8,
    defaults: DispatchOptions,
    /// Per-instance transaction-id counter (H3 — share-nothing multi-raft).
    ///
    /// Every `DispatchSpec::new` call owns an independent id space: the cell
    /// is allocated once per construction and shared by refcount across
    /// clones (clones of one spec are the same logical instance and
    /// therefore continue the same sequence). It is freed when the last
    /// clone drops — nothing is leaked. There is NO process-global counter
    /// on the dispatch path — two specs built by two Raft groups never
    /// touch the same cache line when allocating ids.
    tx_ids: Arc<AtomicU64>,
    encode_params: fn(&P) -> Result<Vec<u8>, RaftError>,
    decode_params: fn(&[u8]) -> Result<P, RaftError>,
    encode_response: fn(&R) -> Result<Vec<u8>, RaftError>,
    decode_response: fn(&[u8]) -> Result<R, RaftError>,
    _marker: PhantomData<fn(P) -> R>,
}

// Manual impl: `P`/`R` appear only behind `fn` pointers and `PhantomData`,
// so cloning must not require `P: Clone` / `R: Clone` (a derive would).
impl<P, R> Clone for DispatchSpec<P, R> {
    fn clone(&self) -> Self {
        Self {
            command: self.command,
            defaults: self.defaults,
            tx_ids: Arc::clone(&self.tx_ids),
            encode_params: self.encode_params,
            decode_params: self.decode_params,
            encode_response: self.encode_response,
            decode_response: self.decode_response,
            _marker: PhantomData,
        }
    }
}

impl<P, R> DispatchSpec<P, R> {
    pub fn new(
        command: u8,
        encode_params: fn(&P) -> Result<Vec<u8>, RaftError>,
        decode_params: fn(&[u8]) -> Result<P, RaftError>,
        encode_response: fn(&R) -> Result<Vec<u8>, RaftError>,
        decode_response: fn(&[u8]) -> Result<R, RaftError>,
    ) -> Self {
        // Seed the sequence from a monotonically increasing construction
        // epoch, mixed to spread instances across the 56-bit space. Epochs
        // are unique for the process lifetime, so even two instances of the
        // SAME command on the same node — including one built after another
        // was dropped with proposals still in flight — never collide in a
        // shared pending map.
        let epoch = TX_ID_SEED_EPOCH.fetch_add(1, Ordering::Relaxed);
        let seed = epoch.wrapping_mul(TX_ID_SEED_MIX);
        Self {
            command,
            defaults: DispatchOptions::default(),
            tx_ids: Arc::new(AtomicU64::new(seed)),
            encode_params,
            decode_params,
            encode_response,
            decode_response,
            _marker: PhantomData,
        }
    }

    pub fn command(&self) -> u8 {
        self.command
    }

    pub fn defaults(&self) -> DispatchOptions {
        self.defaults
    }

    pub fn with_scope(mut self, scope: DispatchScope) -> Self {
        self.defaults.scope = scope;
        self
    }

    pub fn with_ack_policy(mut self, policy: DispatchAckPolicy) -> Self {
        self.defaults.ack.policy = policy;
        self
    }

    pub fn with_ack_count(mut self, count: u16) -> Self {
        self.defaults.ack.count = count.max(1);
        self
    }

    pub fn with_ack_percent(mut self, percent: u8) -> Self {
        self.defaults.ack.percent = percent.clamp(1, 100);
        self
    }

    pub fn with_fail_policy(mut self, policy: DispatchFailPolicy) -> Self {
        self.defaults.fail.policy = policy;
        self
    }

    pub fn with_fail_count(mut self, count: u16) -> Self {
        self.defaults.fail.count = count;
        self
    }

    pub fn with_fail_percent(mut self, percent: u8) -> Self {
        self.defaults.fail.percent = percent.min(100);
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.defaults.timeout = timeout;
        self
    }

    pub fn with_trace(mut self, trace: bool) -> Self {
        self.defaults.trace = trace;
        self
    }

    pub fn encode_params(&self, params: &P) -> Result<Vec<u8>, RaftError> {
        (self.encode_params)(params)
    }

    pub fn decode_params(&self, bytes: &[u8]) -> Result<P, RaftError> {
        (self.decode_params)(bytes)
    }

    pub fn encode_response(&self, value: &R) -> Result<Vec<u8>, RaftError> {
        (self.encode_response)(value)
    }

    pub fn decode_response(&self, bytes: &[u8]) -> Result<R, RaftError> {
        (self.decode_response)(bytes)
    }

    pub(crate) fn decode_response_fn(&self) -> fn(&[u8]) -> Result<R, RaftError> {
        self.decode_response
    }

    /// Allocate the next transaction id from THIS spec instance's id space.
    ///
    /// Layout: `command << 56 | (seed + n) & 56-bit mask`. The command byte
    /// in the high bits makes ids from different commands on one node
    /// disjoint by construction; the epoch-derived seed keeps two
    /// instances of the same command apart. One relaxed `fetch_add` on a
    /// per-instance cache line — no cross-group contention, no allocation.
    pub(crate) fn next_tx_id(&self) -> u64 {
        let seq = self.tx_ids.fetch_add(1, Ordering::Relaxed);
        ((self.command as u64) << 56) | (seq & TX_ID_SEQ_MASK)
    }

    pub fn dispatch<'a>(&'a self, params: P) -> DispatchBuilder<'a, P, R> {
        DispatchBuilder::new(self, params)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw_spec(command: u8) -> DispatchSpec<Vec<u8>, Vec<u8>> {
        DispatchSpec::new(
            command,
            |p| Ok(p.clone()),
            |b| Ok(b.to_vec()),
            |r| Ok(r.clone()),
            |b| Ok(b.to_vec()),
        )
    }

    /// The per-instance tx-id counter is refcounted, not leaked: clones
    /// share one allocation, and dropping the last clone frees it. This is
    /// the structural regression test for the old intentionally-leaked
    /// `&'static` cell, which lost 8 bytes forever on every construction.
    #[test]
    fn tx_counter_is_freed_when_last_clone_drops() {
        let spec = raw_spec(1);
        let clone = spec.clone();
        assert_eq!(
            Arc::strong_count(&spec.tx_ids),
            2,
            "a clone must share the counter allocation, not fork a new one"
        );

        let weak = Arc::downgrade(&spec.tx_ids);
        drop(clone);
        assert_eq!(Arc::strong_count(&spec.tx_ids), 1);
        drop(spec);
        assert!(
            weak.upgrade().is_none(),
            "tx-id counter must be deallocated once the last clone drops"
        );
    }

    /// N constructions + drops leave zero live counter allocations behind.
    #[test]
    fn repeated_construction_leaves_no_live_allocations() {
        let mut weaks = Vec::with_capacity(1000);
        for i in 0..1000u32 {
            let spec = raw_spec(i as u8);
            weaks.push(Arc::downgrade(&spec.tx_ids));
            drop(spec);
        }
        assert!(
            weaks.iter().all(|w| w.upgrade().is_none()),
            "every dropped spec must release its tx-id counter"
        );
    }
}
