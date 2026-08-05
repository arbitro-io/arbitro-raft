//! H3 — share-nothing dispatch tx-id spaces.
//!
//! Every `DispatchSpec::new` call must own an INDEPENDENT transaction-id
//! space: building envelopes on one spec instance must never advance (or
//! collide with) another instance's sequence. This is the observable
//! contract behind removing the process-global `NEXT_TX_ID` counter.

use arbitro_raft::DispatchSpec;

const SEQ_MASK: u64 = (1 << 56) - 1;

fn raw_spec(command: u8) -> DispatchSpec<Vec<u8>, Vec<u8>> {
    DispatchSpec::new(
        command,
        |p| Ok(p.clone()),
        |b| Ok(b.to_vec()),
        |r| Ok(r.clone()),
        |b| Ok(b.to_vec()),
    )
}

fn next_id(spec: &DispatchSpec<Vec<u8>, Vec<u8>>) -> u64 {
    spec.dispatch(Vec::new()).build().unwrap().tx_id()
}

/// Two separately constructed specs (even with the SAME command byte) have
/// independent id spaces: heavy traffic on one does not move the other.
#[test]
fn two_instances_have_independent_id_spaces() {
    let a = raw_spec(1);
    let b = raw_spec(1);

    let b_first = next_id(&b);

    // Drive A hard; with the old global counter this advanced B's ids too.
    for _ in 0..1000 {
        next_id(&a);
    }

    let b_second = next_id(&b);
    assert_eq!(
        b_second & SEQ_MASK,
        (b_first + 1) & SEQ_MASK,
        "B's sequence must be exactly consecutive — 1000 builds on A leaked \
         into B's id space (shared counter)"
    );
}

/// Two instances of the same command start on different (address-seeded)
/// sequences, so their ids do not collide even in one shared pending map.
#[test]
fn same_command_instances_do_not_collide() {
    let a = raw_spec(9);
    let b = raw_spec(9);

    let ids_a: Vec<u64> = (0..256).map(|_| next_id(&a)).collect();
    let ids_b: Vec<u64> = (0..256).map(|_| next_id(&b)).collect();

    for id in &ids_b {
        assert!(
            !ids_a.contains(id),
            "tx id {id:#x} produced by BOTH instances — seeds collided"
        );
    }
}

/// Clones of one spec are the same logical instance and share its sequence
/// (the documented `Clone` semantics — clones are not new instances; they
/// share the refcounted counter, which is freed when the last clone drops).
#[test]
fn clones_share_one_sequence() {
    let a = raw_spec(2);
    let a_clone = a.clone();

    let first = next_id(&a);
    let second = next_id(&a_clone);
    assert_eq!(
        second & SEQ_MASK,
        (first + 1) & SEQ_MASK,
        "a clone of a spec must continue the same sequence, not fork a new one"
    );
}

/// Different commands on one node can never produce the same tx id: the
/// command byte occupies the high 8 bits of every id.
#[test]
fn distinct_commands_are_disjoint_by_construction() {
    let a = raw_spec(3);
    let b = raw_spec(4);

    for _ in 0..64 {
        let id_a = next_id(&a);
        let id_b = next_id(&b);
        assert_eq!(id_a >> 56, 3, "command byte must sit in the high bits");
        assert_eq!(id_b >> 56, 4, "command byte must sit in the high bits");
        assert_ne!(id_a, id_b);
    }
}

/// Ids from a single instance are unique across many builds.
#[test]
fn one_instance_never_repeats_ids() {
    let spec = raw_spec(7);
    let mut seen = std::collections::HashSet::new();
    for _ in 0..10_000 {
        assert!(seen.insert(next_id(&spec)), "duplicate tx id from one spec");
    }
}
