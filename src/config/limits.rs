#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LimitsConfig {
    pub append_batch_bytes: usize,
    pub append_batch_entries: usize,
    pub snapshot_chunk_bytes: usize,
    pub max_inflight_per_peer: usize,
    /// Hard cap on the byte size of an incoming snapshot transfer. Chunks
    /// past this threshold are dropped and the transfer is NACK-ed so a
    /// hostile or buggy peer cannot exhaust follower memory.
    pub max_snapshot_bytes: usize,
    /// Milliseconds an inbound pending-snapshot transfer may sit with no
    /// progress (no accepted chunk) before it is evicted and its buffer
    /// freed (C4 / P1-5). Eviction is timer-based: the run loop sweeps
    /// stalled transfers on every tick, so a leader that goes quiet
    /// mid-install cannot park a multi-GiB buffer forever. Default: 60 000.
    pub snapshot_stall_timeout_ms: u64,
    /// Leader-side cap on consecutive failed snapshot-install attempts per
    /// peer (PS7). Once a peer has burned this many attempts without a
    /// successful install, further installs to it are refused for
    /// [`snapshot_attempt_cooldown_ms`](Self::snapshot_attempt_cooldown_ms).
    /// Also bounds non-advancing response rounds inside a single install,
    /// so a follower that keeps NACK-ing from offset 0 cannot drive an
    /// unbounded re-stream loop. Default: 3.
    pub snapshot_max_attempts_per_peer: u32,
    /// Milliseconds a peer stays refused after exhausting
    /// [`snapshot_max_attempts_per_peer`](Self::snapshot_max_attempts_per_peer).
    /// When the cooldown elapses the attempt counter resets and installs
    /// are allowed again. Default: 5 000.
    pub snapshot_attempt_cooldown_ms: u64,
    /// Log-compaction trigger (C2): once this many entries have been APPLIED
    /// to the state machine since the last snapshot, the run loop snapshots
    /// the state machine and truncates the log prefix up to the conservative
    /// compaction horizon. `0` disables the entries trigger. Default: 4 096.
    pub compaction_threshold_entries: u64,
    /// Log-compaction trigger (C2): once this many payload BYTES have been
    /// applied since the last snapshot, compaction fires (whichever of the
    /// two thresholds trips first). `0` disables the bytes trigger.
    /// Default: 64 MiB.
    pub compaction_threshold_bytes: u64,
    /// Retention margin (C2): number of most-recent entries kept in the log
    /// BELOW the compaction horizon. A slightly-lagging follower can then be
    /// repaired with plain `AppendEntries` from the retained tail instead of
    /// a full snapshot install. Default: 64.
    pub compaction_min_retain: u64,
    /// Inbound-abuse cutoff (D3): decode errors tolerated from ONE peer
    /// inside a single [`inbound_decode_error_window_ms`](Self::inbound_decode_error_window_ms)
    /// window before the peer is jailed. While jailed, every frame claiming
    /// that peer's `from` id is shed BEFORE decode (a counter bump and
    /// nothing else), so a hostile peer spinning the loop at line rate with
    /// garbage burns near-zero CPU here instead of a full decode + log line
    /// per frame.
    ///
    /// Sizing: a well-behaved, version-matched peer produces ZERO decode
    /// errors — a decode error means bad magic/version or a malformed body,
    /// which no compatible node ever emits. The default (32) therefore only
    /// trips on garbage floods, while still absorbing a short burst of
    /// truncated frames from a crashing/reconnecting peer. `0` disables
    /// jailing entirely. Default: 32.
    pub inbound_decode_error_jail_threshold: u32,
    /// Width in milliseconds of the fixed window over which per-peer decode
    /// errors are counted toward
    /// [`inbound_decode_error_jail_threshold`](Self::inbound_decode_error_jail_threshold).
    /// Default: 10 000.
    pub inbound_decode_error_window_ms: u64,
    /// Milliseconds a jailed peer stays jailed. While jailed, its frames are
    /// shed pre-decode and counted in the `frames_shed_jailed` metric; when
    /// the cooldown elapses the peer's error window resets and its frames
    /// flow normally again. Kept short by default so a false positive (e.g.
    /// an operator rolling a node onto an incompatible wire version) recovers
    /// on its own within seconds. Default: 3 000.
    pub inbound_jail_cooldown_ms: u64,
    /// Capacity, in proposals, of the bounded client→node mailbox that
    /// `ClientHandle::write` / `write_bytes` enqueue into (H5). The send is a
    /// non-blocking `try_send`: when the run loop lags behind a client flood
    /// the mailbox fills and further writes fail FAST with the retryable
    /// `RaftError::Overloaded` instead of buffering without limit (the old
    /// unbounded channel's OOM path) or parking the caller indefinitely.
    /// Sizing: at the default `append_batch_entries` (1024) the leader drains
    /// up to one full batch per tick, so 8192 absorbs several ticks of burst
    /// while capping worst-case buffered payload memory. Values below 1 are
    /// clamped to 1. Default: 8 192.
    pub client_mailbox_capacity: usize,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            append_batch_bytes: 256 * 1024,
            append_batch_entries: 1024,
            snapshot_chunk_bytes: 256 * 1024,
            max_inflight_per_peer: 8,
            max_snapshot_bytes: (4usize)
                .saturating_mul(1024)
                .saturating_mul(1024)
                .saturating_mul(1024),
            snapshot_stall_timeout_ms: 60_000,
            snapshot_max_attempts_per_peer: 3,
            snapshot_attempt_cooldown_ms: 5_000,
            compaction_threshold_entries: 4_096,
            compaction_threshold_bytes: 64 * 1024 * 1024,
            compaction_min_retain: 64,
            inbound_decode_error_jail_threshold: 32,
            inbound_decode_error_window_ms: 10_000,
            inbound_jail_cooldown_ms: 3_000,
            client_mailbox_capacity: 8_192,
        }
    }
}
