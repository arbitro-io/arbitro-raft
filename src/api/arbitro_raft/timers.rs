use std::time::{Duration, Instant};

use crate::{RaftStorage, RaftTransport, StateMachine};

use super::ArbitroRaft;

// ---------------------------------------------------------------------------
// Timer helpers — deadline management for election and heartbeat.
// ---------------------------------------------------------------------------

/// Splitmix64 finalizer — fast, high-quality, inline-friendly.
#[inline]
pub(super) fn seed(node_id: crate::PeerId) -> u64 {
    mix64(node_id.0.wrapping_mul(0x9e37_79b9_7f4a_7c15))
}

#[inline]
pub(super) fn mix64(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

impl<S, T, SM> ArbitroRaft<S, T, SM>
where
    S: RaftStorage,
    T: RaftTransport,
    SM: StateMachine,
{
    #[inline]
    pub(super) fn reset_heartbeat_deadline(&mut self) {
        self.next_heartbeat_at = Instant::now() + self.heartbeat_interval();
    }

    #[inline]
    pub(super) fn reset_election_deadline(&mut self) {
        self.next_election_at = Instant::now() + self.next_election_timeout();
    }

    #[inline]
    pub(super) fn heartbeat_interval(&self) -> Duration {
        Duration::from_millis(self.node.timing().heartbeat_ms.max(1))
    }

    pub(super) fn next_election_timeout(&mut self) -> Duration {
        let timing = self.node.timing();
        let min_ms = timing.election_min_ms.max(1);
        let max_ms = timing.election_max_ms.max(min_ms);
        if max_ms == min_ms {
            return Duration::from_millis(min_ms);
        }
        self.election_state = mix64(self.election_state);
        let span = max_ms - min_ms + 1;
        let jitter = self.election_state % span;
        Duration::from_millis(min_ms + jitter)
    }
}
