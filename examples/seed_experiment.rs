use arbitro_raft::{AppendEntries, EntryHeader, EntryPayload, LogEntry, LogIndex, PeerId, Term};
use std::time::Instant;
use zerocopy::IntoBytes;

// ---------------------------------------------------------------------------
// 1. SEEDED ARENA (The Arbitro Pattern)
// ---------------------------------------------------------------------------

struct SeededArena {
    headers: Vec<EntryHeader>,
    data: Vec<u8>,
    payload_offsets: Vec<(usize, usize)>,
}

impl SeededArena {
    fn new(count: usize, payload_size: usize) -> Self {
        let mut headers = Vec::with_capacity(count);
        let mut data = Vec::with_capacity(count * payload_size);
        let mut payload_offsets = Vec::with_capacity(count);

        for i in 0..count {
            let offset = data.len();
            data.extend(vec![0xAA; payload_size]);
            headers.push(EntryHeader {
                term: 1.into(),
                index: (i as u64).into(),
                payload_len: (payload_size as u32).into(),
                _pad: 0.into(),
            });
            payload_offsets.push((offset, payload_size));
        }

        Self {
            headers,
            data,
            payload_offsets,
        }
    }

    // MAGIC ZEROCOPY HOT PATH: O(1) slice cast
    #[inline(always)]
    fn get_seeds(&self) -> (&[u8], Vec<&[u8]>) {
        // Here we transmute the entire slice of EntryHeader to &[u8] in O(1).
        // Zero loops. Zero cycles scaling with N.
        let header_bytes = self.headers.as_bytes();

        // For payloads, we still need references, but we should use a scratchpad
        // to be truly zero-alloc. For the experiment, let's show the O(1) header cast.
        let mut payloads = Vec::with_capacity(self.headers.len());
        for (off, len) in &self.payload_offsets {
            payloads.push(&self.data[*off..*off + *len]);
        }
        (header_bytes, payloads)
    }
}

// ---------------------------------------------------------------------------
// 2. ERGONOMIC STORAGE (Standard Vec)
// ---------------------------------------------------------------------------

struct ErgonomicStore<'a> {
    entries: Vec<LogEntry<'a>>,
}

impl<'a> ErgonomicStore<'a> {
    fn new(count: usize, payload_size: &'a [u8]) -> Self {
        let mut entries = Vec::with_capacity(count);
        for i in 0..count {
            entries.push(LogEntry {
                term: Term(1),
                index: LogIndex(i as u64),
                payload: EntryPayload(payload_size),
            });
        }
        Self { entries }
    }
}

// ---------------------------------------------------------------------------
// MAIN BENCHMARK
// ---------------------------------------------------------------------------

fn main() {
    let batch_sizes = [1, 10, 100, 1000];
    let payload_size = 128;
    let iterations = 100_000;

    println!("--- SEED PATTERN EXPERIMENT ---");
    println!(
        "Payload size: {} bytes, Iterations: {}\n",
        payload_size, iterations
    );

    for &batch in &batch_sizes {
        println!("BATCH SIZE: {}", batch);

        // Setup payload data
        let raw_payload = vec![0xBB; payload_size];

        // 1. BENCHMARK ERGONOMIC PATH
        // ---------------------------
        let ergo_store = ErgonomicStore::new(batch, &raw_payload);
        let mut dummy_buf = Vec::with_capacity(batch * 200);

        let start = Instant::now();
        for _ in 0..iterations {
            dummy_buf.clear();
            // Simulate encoding: iterating and copying
            for e in &ergo_store.entries {
                // Mock metadata construction (simulating codec)
                let h = EntryHeader {
                    term: e.term.0.into(),
                    index: e.index.0.into(),
                    payload_len: (e.payload.0.len() as u32).into(),
                    _pad: 0.into(),
                };
                dummy_buf.extend_from_slice(h.as_bytes());
                dummy_buf.extend_from_slice(e.payload.0);
            }
        }
        let ergo_dur = start.elapsed();
        let ergo_ns_per_op = ergo_dur.as_nanos() as f64 / (iterations as f64 * batch as f64);

        // 2. BENCHMARK SEED PATH
        // ----------------------
        let seed_store = SeededArena::new(batch, payload_size);
        let mut iov = Vec::with_capacity(batch + 1); // Scratchpad for iovec references

        let start = Instant::now();
        for _ in 0..iterations {
            iov.clear();
            // MAGIC ZEROCOPY: O(1) metadata preparation
            let (headers_chunk, payloads) = seed_store.get_seeds();

            // We push the ENTIRE header block as one pointer. Zero loops for metadata.
            iov.push(headers_chunk);

            // Only the payloads (raw bits) are iterated to pointers
            for p in payloads {
                iov.push(p);
            }
        }
        let seed_dur = start.elapsed();
        let seed_ns_per_op = seed_dur.as_nanos() as f64 / (iterations as f64 * batch as f64);

        println!("  Ergonomic: {:>8.2} ns/entry", ergo_ns_per_op);
        println!("  Seeded:    {:>8.2} ns/entry", seed_ns_per_op);
        println!("  SPEEDUP:   {:>8.2}x", ergo_ns_per_op / seed_ns_per_op);
        println!();
    }
}
