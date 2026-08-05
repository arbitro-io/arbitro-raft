//! Per-peer inbound abuse guard (D3 — DoS / resource-exhaustion hardening).
//!
//! Tracks decode-error rates per claimed sender and temporarily "jails" a
//! peer that floods the run loop with undecodable garbage. While a peer is
//! jailed its frames are shed BEFORE decode (one header peek, one counter
//! bump — no decode, no per-frame log line), so a hostile peer spinning the
//! loop at line rate burns near-zero CPU on this node.
//!
//! Attribution: the claimed wire `from` id is used as the key. A transport
//! implementing the D2 identity-binding contract has already verified that
//! `from` matches the connection's authenticated identity, so the key is
//! trustworthy there. Frames whose claimed id is not a current cluster
//! member (or that are too short to carry a header) all share ONE
//! "unknown" bucket — a flood of garbage with random `from` values cannot
//! grow this map. The map is therefore bounded to `voters + 1` entries.
//!
//! NOTE (E1): this is the *minimal* per-peer state D3 needs. Full per-peer
//! observability (E1 in MASTER_TODO) remains a separate item.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::config::LimitsConfig;
use crate::PeerId;

/// Shared bucket for frames whose claimed sender is not a current member
/// (or that are too short to carry a frame header).
const UNKNOWN_SENDER_KEY: u64 = u64::MAX;

#[derive(Debug, Clone, Copy)]
struct PeerAbuseState {
    /// Start of the current fixed error-counting window.
    window_start: Instant,
    /// Decode errors observed inside the current window.
    errors_in_window: u32,
    /// When set, the peer is jailed until this deadline.
    jailed_until: Option<Instant>,
}

/// Per-peer decode-error accounting + jail policy. Owned by the run loop —
/// no locks; the run loop is the only reader of inbound frames.
#[derive(Debug, Default)]
pub(crate) struct InboundAbuseGuard {
    peers: HashMap<u64, PeerAbuseState>,
}

impl InboundAbuseGuard {
    /// Map a claimed wire `from` id (if the frame was long enough to carry
    /// one) to the accounting key: the id itself for current members, the
    /// shared unknown bucket for everything else. This is what bounds the
    /// map — random spoofed ids cannot allocate new entries.
    pub(crate) fn key_for(claimed_from: Option<u64>, members: &[PeerId]) -> u64 {
        match claimed_from {
            Some(id) if members.contains(&PeerId(id)) => id,
            _ => UNKNOWN_SENDER_KEY,
        }
    }

    /// True when the sender behind `key` is currently jailed. An expired
    /// jail is cleared here (lazy expiry — no timers), which also resets
    /// the peer's error window so it re-enters clean.
    pub(crate) fn is_jailed(&mut self, key: u64, now: Instant) -> bool {
        if let Some(state) = self.peers.get_mut(&key) {
            if let Some(until) = state.jailed_until {
                if now < until {
                    return true;
                }
                // Cooldown elapsed — release and start a fresh window.
                self.peers.remove(&key);
            }
        }
        false
    }

    /// Record one decode error attributed to `key`. Returns `true` exactly
    /// when this error crossed the threshold and the peer was NEWLY jailed
    /// (the caller bumps the `peers_jailed` metric and logs once).
    ///
    /// `limits.inbound_decode_error_jail_threshold == 0` disables jailing.
    pub(crate) fn record_decode_error(
        &mut self,
        key: u64,
        now: Instant,
        limits: &LimitsConfig,
    ) -> bool {
        let threshold = limits.inbound_decode_error_jail_threshold;
        if threshold == 0 {
            return false;
        }
        let window = Duration::from_millis(limits.inbound_decode_error_window_ms);
        let state = self.peers.entry(key).or_insert(PeerAbuseState {
            window_start: now,
            errors_in_window: 0,
            jailed_until: None,
        });
        if now.duration_since(state.window_start) > window {
            // Fixed window elapsed — restart the count.
            state.window_start = now;
            state.errors_in_window = 0;
        }
        state.errors_in_window = state.errors_in_window.saturating_add(1);
        if state.errors_in_window >= threshold {
            state.jailed_until = Some(now + Duration::from_millis(limits.inbound_jail_cooldown_ms));
            return true;
        }
        false
    }
}
