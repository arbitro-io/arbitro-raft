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
        }
    }
}
