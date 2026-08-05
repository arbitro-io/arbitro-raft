use crate::{LogIndex, Term};

#[derive(Clone, Copy, Debug)]
struct LogMetadataSlot {
    term: Term,
    index: LogIndex,
    generation: u32,
}

/// A circular log metadata arena with generational tags.
/// Avoids dynamic memory allocations and expensive storage reads for active terms.
pub struct LogMetadataArena {
    slots: Box<[LogMetadataSlot]>,
    head: usize,
    len: usize,
    base_index: LogIndex,
    generation: u32,
}

impl LogMetadataArena {
    pub fn new(capacity: usize) -> Self {
        let mut slots = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            slots.push(LogMetadataSlot {
                term: Term(0),
                index: LogIndex(0),
                generation: 0,
            });
        }
        Self {
            slots: slots.into_boxed_slice(),
            head: 0,
            len: 0,
            base_index: LogIndex(0),
            generation: 1,
        }
    }

    pub fn append(&mut self, index: LogIndex, term: Term) {
        let cap = self.slots.len();
        if cap == 0 {
            return;
        }

        if self.len == 0 {
            self.base_index = index;
        }

        if index.0 != self.base_index.0.saturating_add(self.len as u64) {
            self.clear(index);
        }

        if self.len == cap {
            let head_slot = &mut self.slots[self.head];
            head_slot.generation = head_slot.generation.wrapping_add(1);
            self.head = (self.head + 1) % cap;
            self.base_index = LogIndex(self.base_index.0.saturating_add(1));
            self.len -= 1;
        }

        let write_idx = (self.head + self.len) % cap;
        let slot = &mut self.slots[write_idx];
        slot.term = term;
        slot.index = index;
        slot.generation = self.generation;
        self.len += 1;
    }

    pub fn get_term(&self, index: LogIndex) -> Option<Term> {
        let cap = self.slots.len();
        if cap == 0 || self.len == 0 {
            return None;
        }

        if index.0 >= self.base_index.0
            && index.0 < self.base_index.0.saturating_add(self.len as u64)
        {
            let offset = (index.0 - self.base_index.0) as usize;
            let read_idx = (self.head + offset) % cap;
            let slot = &self.slots[read_idx];
            if slot.index == index {
                return Some(slot.term);
            }
        }
        None
    }

    pub fn truncate_suffix(&mut self, from: LogIndex) {
        if self.len == 0 {
            return;
        }

        if from.0 <= self.base_index.0 {
            self.clear(from);
            return;
        }

        if from.0 < self.base_index.0.saturating_add(self.len as u64) {
            let cap = self.slots.len();
            let new_len = (from.0 - self.base_index.0) as usize;
            for i in new_len..self.len {
                let idx = (self.head + i) % cap;
                self.slots[idx].generation = self.slots[idx].generation.wrapping_add(1);
            }
            self.len = new_len;
        }
    }

    pub fn clear(&mut self, new_base: LogIndex) {
        let cap = self.slots.len();
        for i in 0..self.len {
            let idx = (self.head + i) % cap;
            self.slots[idx].generation = self.slots[idx].generation.wrapping_add(1);
        }
        self.head = 0;
        self.len = 0;
        self.base_index = new_base;
        self.generation = self.generation.wrapping_add(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{LogIndex, Term};

    /// P1-7 index sweep: a spoofed index near `u64::MAX` must never panic the
    /// arena's range or eviction arithmetic. Correctness AT `u64::MAX` is
    /// irrelevant (a range check may return `None`); the point is the node
    /// stays up — pre-fix, the eviction `base_index + 1` overflowed and, in
    /// debug, panicked the whole node on a single hostile frame.
    #[test]
    fn arena_arithmetic_saturates_near_u64_max() {
        let mut arena = LogMetadataArena::new(2);
        // Fill to capacity, then force the eviction path — `base_index + 1`
        // at `u64::MAX` must saturate, not overflow.
        arena.append(LogIndex(u64::MAX), Term(1));
        arena.append(LogIndex(u64::MAX), Term(1));
        arena.append(LogIndex(u64::MAX), Term(1)); // triggers eviction
        let _ = arena.get_term(LogIndex(u64::MAX)); // range check must not overflow
        let _ = arena.get_term(LogIndex(u64::MAX - 1));
        arena.truncate_suffix(LogIndex(u64::MAX)); // truncate math must not overflow

        // Normal usage is unaffected by the saturating guards.
        let mut normal = LogMetadataArena::new(8);
        normal.append(LogIndex(5), Term(3));
        normal.append(LogIndex(6), Term(3));
        assert_eq!(normal.get_term(LogIndex(5)), Some(Term(3)));
        assert_eq!(normal.get_term(LogIndex(6)), Some(Term(3)));
        assert_eq!(normal.get_term(LogIndex(7)), None);
    }
}
