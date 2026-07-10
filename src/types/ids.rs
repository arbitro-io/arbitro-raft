#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct PeerId(pub u64);

/// Identifies a Raft consensus group when a single transport multiplexes many
/// groups. `GroupId(0)` is reserved for the SINGLE-GROUP default (backwards
/// compatibility for existing single-group callers).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, PartialOrd, Ord)]
pub struct GroupId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct ClusterId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Term(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct LogIndex(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LeaderHint {
    pub leader_id: PeerId,
}
