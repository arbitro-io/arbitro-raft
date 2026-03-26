#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LimitsConfig {
    pub append_batch_bytes: usize,
    pub append_batch_entries: usize,
    pub snapshot_chunk_bytes: usize,
    pub max_inflight_per_peer: usize,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            append_batch_bytes: 256 * 1024,
            append_batch_entries: 1024,
            snapshot_chunk_bytes: 256 * 1024,
            max_inflight_per_peer: 8,
        }
    }
}
