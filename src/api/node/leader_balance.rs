// Leader placement heuristic across groups (B4).
// Kept as a self-contained heuristic module; not yet wired into the pre-vote path.
#![allow(dead_code)]

use std::time::Duration;

/// Fair share of groups per node when leaders are distributed evenly.
///
/// For example, 12 groups over 3 nodes gives a fair share of 4; over 4 nodes
/// gives a fair share of 3. Callers use this as the threshold above which
/// [`extra_pre_vote_delay`] returns a non-zero penalty.
#[inline]
pub fn fair_share(total_groups: usize, total_nodes: usize) -> usize {
    if total_nodes == 0 { return total_groups; }
    // Round up so we don't over-penalize the (leaders_held == ceil) case.
    total_groups.div_ceil(total_nodes)
}

/// Extra pre-vote delay this node should wait when it already leads more
/// groups than its fair share, biasing subsequent elections toward peers.
///
/// The penalty is linear in `leaders_held - fair_share`, capped at
/// `max_penalty`, and returns `Duration::ZERO` when the node is at or below
/// its share.
///
/// This is a heuristic-only knob — it does NOT delay or block elections
/// once they start, only adds jitter to the pre-vote deadline.
pub fn extra_pre_vote_delay(
    leaders_held: usize,
    total_groups: usize,
    total_nodes: usize,
    per_extra_leader_ms: u64,
    max_penalty: Duration,
) -> Duration {
    let share = fair_share(total_groups, total_nodes);
    if leaders_held <= share { return Duration::ZERO; }
    let excess = (leaders_held - share) as u64;
    let penalty = Duration::from_millis(excess.saturating_mul(per_extra_leader_ms));
    if penalty > max_penalty { max_penalty } else { penalty }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn zero_when_at_share() {
        assert_eq!(
            extra_pre_vote_delay(4, 12, 3, 25, Duration::from_millis(500)),
            Duration::ZERO,
        );
    }

    #[test]
    fn linear_penalty_above_share() {
        // Share = 4 (12 / 3). Held 6 → excess 2 → 2 * 25 = 50ms.
        assert_eq!(
            extra_pre_vote_delay(6, 12, 3, 25, Duration::from_millis(500)),
            Duration::from_millis(50),
        );
    }

    #[test]
    fn caps_at_max_penalty() {
        // Share = 1 (10 / 10). Held 10 → excess 9 → 9 * 500 = 4500ms, capped at 200ms.
        assert_eq!(
            extra_pre_vote_delay(10, 10, 10, 500, Duration::from_millis(200)),
            Duration::from_millis(200),
        );
    }

    #[test]
    fn zero_nodes_returns_share_equal_to_groups() {
        assert_eq!(fair_share(5, 0), 5);
    }
}
