use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use futures::task::AtomicWaker;

use crate::{LogIndex, RaftError};

// ---------------------------------------------------------------------------
// Slot — lock-free single-use notification primitive.
//
// Replaces oneshot::channel for commit notifications. The raft task stores
// the committed LogIndex via an atomic and wakes the waiting client task.
// No mutex, no Arc<Mutex<Option<T>>> — just a few word-sized fields.
//
// `state` encoding — the top two bits act as a tag so a committed LogIndex
// can never collide with a sentinel (previously `index.0 == u64::MAX` was
// misread as SLOT_NOT_LEADER):
//   SLOT_PENDING (0)                    — not yet committed
//   bit 63 set                          — leader stepped down before commit
//   bit 62 set, low 62 bits = LogIndex  — committed
//
// `generation` guards against slot reuse. A cancelled WriteFuture's Drop
// releases its slot back to the free pool while the matching ClientProposal
// may still be sitting in the raft task's mpsc channel. If lease() hands
// that slot to a different writer before the stale proposal is processed,
// notify_committed must not deliver the old commit to the new lessee — the
// generation check (via SlotRef) is what catches exactly this case.
// ---------------------------------------------------------------------------

pub(super) const SLOT_PENDING: u64 = 0;

const TAG_NOT_LEADER: u64 = 1 << 63;
const TAG_COMMITTED: u64 = 1 << 62;
const INDEX_MASK: u64 = TAG_COMMITTED - 1;

#[repr(C, align(64))]
pub(super) struct Slot {
    pub(super) state: AtomicU64,
    pub(super) waker: AtomicWaker,
    generation: AtomicU64,
}

impl Default for Slot {
    fn default() -> Self {
        Self {
            state: AtomicU64::new(SLOT_PENDING),
            waker: AtomicWaker::new(),
            generation: AtomicU64::new(0),
        }
    }
}

impl Slot {
    #[inline]
    fn store_committed(&self, index: LogIndex) {
        debug_assert!(index.0 <= INDEX_MASK, "LogIndex exceeds encodable range");
        self.state
            .store(TAG_COMMITTED | (index.0 & INDEX_MASK), Ordering::Release);
        self.waker.wake();
    }

    #[inline]
    fn store_error(&self) {
        self.state.store(TAG_NOT_LEADER, Ordering::Release);
        self.waker.wake();
    }

    #[inline]
    pub(super) fn decode(v: u64) -> Result<LogIndex, RaftError> {
        if v & TAG_NOT_LEADER != 0 {
            Err(RaftError::NotLeader { leader_hint: None })
        } else {
            Ok(LogIndex(v & INDEX_MASK))
        }
    }
}

/// Opaque identifier for a leased notification slot.
///
/// Packs the arena index and a per-slot generation counter into a single
/// u64 so it stays trivially `Copy` and callers never see the internal
/// shape. The generation half is what lets [`SlotRegistry`] tell a stale,
/// cancelled waiter apart from the slot's current occupant after reuse.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotId(u64);

impl SlotId {
    #[inline]
    fn new(index: u32, generation: u32) -> Self {
        Self(((generation as u64) << 32) | index as u64)
    }

    #[inline]
    fn index(self) -> usize {
        (self.0 as u32) as usize
    }

    #[inline]
    fn generation(self) -> u32 {
        (self.0 >> 32) as u32
    }

    /// Encode as a raw u64, e.g. for storing alongside other wire data.
    pub fn to_u64(self) -> u64 {
        self.0
    }

    /// Decode a `SlotId` previously produced by [`SlotId::to_u64`].
    pub fn from_u64(v: u64) -> Self {
        Self(v)
    }
}

#[repr(align(64))]
struct AlignedCursor {
    value: AtomicU64,
}

pub(crate) struct SlotRegistry {
    arena: Box<[Slot]>,
    cursor: AlignedCursor,
}

impl SlotRegistry {
    pub fn new(capacity: usize) -> Arc<Self> {
        let mut slots = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            slots.push(Slot {
                state: AtomicU64::new(TAG_NOT_LEADER),
                waker: AtomicWaker::new(),
                generation: AtomicU64::new(0),
            });
        }
        Arc::new(Self {
            arena: slots.into_boxed_slice(),
            cursor: AlignedCursor {
                value: AtomicU64::new(0),
            },
        })
    }

    #[inline]
    pub fn lease(&self) -> Option<SlotId> {
        let cap = self.arena.len() as u64;
        let start = self.cursor.value.fetch_add(1, Ordering::Relaxed);

        for i in 0..cap {
            let idx = ((start + i) % cap) as usize;
            let slot = &self.arena[idx];

            // A slot is free if its state is NOT SLOT_PENDING.
            // When we lease it, we atomically move it to SLOT_PENDING.
            let current = slot.state.load(Ordering::Acquire);
            if current != SLOT_PENDING
                && slot
                    .state
                    .compare_exchange(current, SLOT_PENDING, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            {
                // Bump generation only on a successful lease. This is what
                // lets notify_committed recognize and drop a notification
                // meant for whoever held this slot before us.
                let prev = slot.generation.fetch_add(1, Ordering::AcqRel);
                let gen = prev.wrapping_add(1) as u32;
                return Some(SlotId::new(idx as u32, gen));
            }
        }
        None
    }

    #[inline]
    pub fn release(&self, id: SlotId) {
        // Technically, notify_committed/notify_error already moves state away from SLOT_PENDING.
        // Release is mainly for cleanup if a proposal never reaches the raft task.
        //
        // Generation is deliberately NOT bumped here — only lease() advances
        // it, so a proposal still in flight for this slot_id remains
        // recognizable as stale even after a later lease() reuses the slot.
        let slot = &self.arena[id.index()];
        if slot.state.load(Ordering::Acquire) == SLOT_PENDING {
            slot.state.store(TAG_NOT_LEADER, Ordering::Release);
        }
    }

    #[inline]
    pub(super) fn get(&self, id: SlotId) -> SlotRef<'_> {
        SlotRef {
            slot: &self.arena[id.index()],
            expected_generation: id.generation(),
        }
    }
}

/// A borrowed [`Slot`] paired with the generation its [`SlotId`] was leased
/// at.
///
/// Derefs to `Slot` for read-only field access (state polling in
/// WriteFuture), but shadows `notify_committed`/`notify_error` with
/// generation-checked versions: once the slot has been leased again by a
/// newer writer, notifications addressed to this (now stale) generation are
/// silently dropped instead of corrupting the new lessee's result.
pub(super) struct SlotRef<'a> {
    slot: &'a Slot,
    expected_generation: u32,
}

impl<'a> std::ops::Deref for SlotRef<'a> {
    type Target = Slot;

    fn deref(&self) -> &Slot {
        self.slot
    }
}

impl<'a> SlotRef<'a> {
    #[inline]
    fn is_current(&self) -> bool {
        self.slot.generation.load(Ordering::Acquire) as u32 == self.expected_generation
    }

    #[inline]
    pub(super) fn notify_committed(&self, index: LogIndex) {
        if self.is_current() {
            self.slot.store_committed(index);
        }
        // else: stale waiter — the slot has since been leased by another
        // writer, so delivering this commit would hand out the wrong index.
    }

    #[inline]
    pub(super) fn notify_error(&self) {
        if self.is_current() {
            self.slot.store_error();
        }
    }
}
