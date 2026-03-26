#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimingConfig {
    pub heartbeat_ms: u64,
    pub election_min_ms: u64,
    pub election_max_ms: u64,
}

impl Default for TimingConfig {
    fn default() -> Self {
        Self {
            heartbeat_ms: 50,
            election_min_ms: 300,
            election_max_ms: 600,
        }
    }
}
