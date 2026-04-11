use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use futures::task::AtomicWaker;

use crate::{LogIndex, RaftError};

// ---------------------------------------------------------------------------
// Slot — lock-free single-use notification primitive.
//
// Replaces oneshot::channel for commit notifications. The raft task stores
// the committed LogIndex via an atomic and wakes the waiting client task.
// No mutex, no Arc<Mutex<Option<T>>> — just two word-sized fields.
//
// Sentinel values:
//   SLOT_PENDING    (0)        — not yet committed
//   SLOT_NOT_LEADER (u64::MAX) — leader stepped down before commit
//   any other value            — committed LogIndex
// ---------------------------------------------------------------------------

pub(super) const SLOT_PENDING: u64 = 0;
pub(super) const SLOT_NOT_LEADER: u64 = u64::MAX;

#[repr(C, align(64))]
pub(super) struct Slot {
    pub(super) state: AtomicU64,
    pub(super) waker: AtomicWaker,
}

impl Default for Slot {
    fn default() -> Self {
        Self {
            state: AtomicU64::new(SLOT_PENDING),
            waker: AtomicWaker::new(),
        }
    }
}

impl Slot {
    #[inline]
    pub(super) fn notify_committed(&self, index: LogIndex) {
        // Non-zero state marks it as "not pending" (free for lease after waker is done)
        self.state.store(index.0, Ordering::Release);
        self.waker.wake();
    }

    #[inline]
    pub(super) fn notify_error(&self) {
        self.state.store(SLOT_NOT_LEADER, Ordering::Release);
        self.waker.wake();
    }

    #[inline]
    pub(super) fn decode(v: u64) -> Result<LogIndex, RaftError> {
        if v == SLOT_NOT_LEADER {
            Err(RaftError::NotLeader { leader_hint: None })
        } else {
            Ok(LogIndex(v))
        }
    }
}

/// Opaque identifier for a leased notification slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotId(pub(super) u32);

pub(crate) struct SlotRegistry {
    arena: Box<[Slot]>,
    cursor: AtomicU64,
}

impl SlotRegistry {
    pub fn new(capacity: usize) -> Arc<Self> {
        let mut slots = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            slots.push(Slot {
                state: AtomicU64::new(SLOT_NOT_LEADER),
                waker: AtomicWaker::new(),
            });
        }
        Arc::new(Self {
            arena: slots.into_boxed_slice(),
            cursor: AtomicU64::new(0),
        })
    }

    #[inline]
    pub fn lease(&self) -> Option<SlotId> {
        let cap = self.arena.len() as u64;
        let start = self.cursor.fetch_add(1, Ordering::Relaxed);

        for i in 0..cap {
            let idx = ((start + i) % cap) as usize;
            let slot = &self.arena[idx];

            // A slot is free if its state is NOT SLOT_PENDING.
            // When we lease it, we atomically move it to SLOT_PENDING.
            let current = slot.state.load(Ordering::Acquire);
            if current != SLOT_PENDING {
                if slot
                    .state
                    .compare_exchange(current, SLOT_PENDING, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    return Some(SlotId(idx as u32));
                }
            }
        }
        None
    }

    #[inline]
    pub fn release(&self, id: SlotId) {
        // Technically, notify_committed/notify_error already moves state away from SLOT_PENDING.
        // Release is mainly for cleanup if a proposal never reaches the raft task.
        let slot = &self.arena[id.0 as usize];
        if slot.state.load(Ordering::Acquire) == SLOT_PENDING {
            slot.state.store(SLOT_NOT_LEADER, Ordering::Release);
        }
    }

    #[inline]
    pub(super) fn get(&self, id: SlotId) -> &Slot {
        &self.arena[id.0 as usize]
    }
}
