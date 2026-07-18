//! Capacity-recycling scratch docks (B1 / B3 / B4).
//!
//! The hot replication paths need reusable `Vec`s of *borrowed* data
//! (`&[u8]` payload views, `LogEntry<'_>` batches). Storing such a `Vec`
//! directly in [`RaftNode`](super::RaftNode) forces a lifetime parameter on
//! the node, so the previous design laundered every reference to `&'static`
//! with `mem::transmute` and parked it in `self` — undefined behavior under
//! Stacked/Tree Borrows the moment `&mut self` was reborrowed while those
//! references were still live (audit items US3/US4/US5).
//!
//! A dock keeps only the **allocation**, never the data:
//!
//! * The docked `Vec` is stored with the `'static` lifetime substitution but
//!   is **always empty** — the invariant is that no element with the
//!   `'static` claim ever exists.
//! * [`take`](SliceScratch::take) moves the empty `Vec` out and re-types it
//!   to a caller-chosen lifetime. This is sound because the layout of a type
//!   does not depend on its lifetime parameters and an empty `Vec` contains
//!   no value that could carry the wrong lifetime.
//! * The caller fills the local `Vec` with references whose lifetimes the
//!   borrow checker verifies **normally** — nothing is laundered, and the
//!   `Vec` lives on the caller's frame, never inside `self`, so `&mut self`
//!   reborrows are ruled out by the compiler instead of by convention.
//! * [`put`](SliceScratch::put) clears the `Vec` and re-docks the empty
//!   allocation for the next call.
//!
//! Failure behavior (this is what subsumes the P0-6 "RAII ScratchGuard"
//! plan): if an early `return`/`?`/panic drops a taken `Vec` without
//! re-docking it, the `Vec` is simply dropped — its elements have true,
//! borrow-checked lifetimes, so the drop is safe and nothing stale is ever
//! parked in the node. The only cost of a missed `put` is losing the
//! recycled capacity for one call (the next `take` starts from an empty
//! `Vec` and re-grows). The same applies to a re-entrant `take` before the
//! matching `put`: the second caller receives a fresh empty `Vec`.

use crate::LogEntry;

/// Recyclable capacity for a `Vec<&[u8]>` (payload views / iovec lists).
///
/// See the module docs for the soundness argument and failure behavior.
pub(crate) struct SliceScratch {
    /// Invariant: always empty — only the heap capacity is retained.
    store: Vec<&'static [u8]>,
}

impl SliceScratch {
    pub(crate) fn with_capacity(cap: usize) -> Self {
        Self {
            store: Vec::with_capacity(cap),
        }
    }

    /// Move the docked (empty) allocation out as a `Vec<&'env [u8]>` for a
    /// caller-chosen lifetime `'env`.
    pub(crate) fn take<'env>(&mut self) -> Vec<&'env [u8]> {
        let v = std::mem::take(&mut self.store);
        debug_assert!(v.is_empty(), "SliceScratch invariant: docked vec is empty");
        let mut v = std::mem::ManuallyDrop::new(v);
        let (ptr, cap) = (v.as_mut_ptr(), v.capacity());
        // SAFETY: `&'x [u8]` has the same size/alignment for every lifetime
        // `'x` (lifetimes are erased; layout cannot depend on them), so the
        // allocation is valid for the re-typed element. `len == 0`, so no
        // element value is ever reinterpreted across lifetimes.
        unsafe { Vec::from_raw_parts(ptr.cast::<&'env [u8]>(), 0, cap) }
    }

    /// Clear `v` and re-dock its allocation for the next [`take`](Self::take).
    pub(crate) fn put(&mut self, mut v: Vec<&[u8]>) {
        v.clear();
        let mut v = std::mem::ManuallyDrop::new(v);
        let (ptr, cap) = (v.as_mut_ptr(), v.capacity());
        // SAFETY: same layout argument as `take`; `v` was cleared above.
        self.store = unsafe { Vec::from_raw_parts(ptr.cast::<&'static [u8]>(), 0, cap) };
    }
}

/// Recyclable capacity for a `Vec<LogEntry<'_>>` (append batches).
///
/// Identical mechanism to [`SliceScratch`]; `LogEntry<'x>` is a `Copy`
/// struct whose only lifetime-bearing field is a `&'x [u8]` payload view,
/// so its layout is likewise lifetime-independent.
pub(crate) struct EntryScratch {
    /// Invariant: always empty — only the heap capacity is retained.
    store: Vec<LogEntry<'static>>,
}

impl EntryScratch {
    pub(crate) fn with_capacity(cap: usize) -> Self {
        Self {
            store: Vec::with_capacity(cap),
        }
    }

    /// Move the docked (empty) allocation out as a `Vec<LogEntry<'env>>` for
    /// a caller-chosen lifetime `'env`.
    pub(crate) fn take<'env>(&mut self) -> Vec<LogEntry<'env>> {
        let v = std::mem::take(&mut self.store);
        debug_assert!(v.is_empty(), "EntryScratch invariant: docked vec is empty");
        let mut v = std::mem::ManuallyDrop::new(v);
        let (ptr, cap) = (v.as_mut_ptr(), v.capacity());
        // SAFETY: `LogEntry<'x>` has the same layout for every lifetime `'x`;
        // `len == 0`, so no element value crosses lifetimes.
        unsafe { Vec::from_raw_parts(ptr.cast::<LogEntry<'env>>(), 0, cap) }
    }

    /// Clear `v` and re-dock its allocation for the next [`take`](Self::take).
    pub(crate) fn put(&mut self, mut v: Vec<LogEntry<'_>>) {
        v.clear();
        let mut v = std::mem::ManuallyDrop::new(v);
        let (ptr, cap) = (v.as_mut_ptr(), v.capacity());
        // SAFETY: same layout argument as `take`; `v` was cleared above.
        self.store = unsafe { Vec::from_raw_parts(ptr.cast::<LogEntry<'static>>(), 0, cap) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EntryPayload, LogIndex, Term};

    #[test]
    fn slice_scratch_recycles_capacity_without_laundering() {
        let mut dock = SliceScratch::with_capacity(8);
        let data = [1u8, 2, 3];
        {
            let mut v = dock.take();
            v.push(&data[..]);
            v.push(&data[1..]);
            assert_eq!(v[0], &[1, 2, 3]);
            assert_eq!(v[1], &[2, 3]);
            let cap = v.capacity();
            dock.put(v);
            // Capacity survived the round-trip.
            let v2: Vec<&[u8]> = dock.take();
            assert!(v2.is_empty());
            assert_eq!(v2.capacity(), cap);
            dock.put(v2);
        }
    }

    #[test]
    fn slice_scratch_tolerates_dropped_take() {
        let mut dock = SliceScratch::with_capacity(4);
        let data = [9u8; 4];
        {
            let mut v = dock.take();
            v.push(&data[..]);
            // Simulate an early-`?` path: the vec is dropped, not `put` back.
        }
        // The dock hands out a fresh empty vec; nothing stale survives.
        let v: Vec<&[u8]> = dock.take();
        assert!(v.is_empty());
        dock.put(v);
    }

    #[test]
    fn entry_scratch_round_trip() {
        let mut dock = EntryScratch::with_capacity(4);
        let payload = vec![7u8; 16];
        let mut v = dock.take();
        v.push(LogEntry {
            term: Term(3),
            index: LogIndex(11),
            payload: EntryPayload(&payload),
        });
        assert_eq!(v[0].payload.0, &payload[..]);
        dock.put(v);
        let v2: Vec<LogEntry<'_>> = dock.take();
        assert!(v2.is_empty());
        dock.put(v2);
    }
}
