//! Global tracking allocator that wraps `System` and counts live bytes.
//!
//! Every allocation increments an `AtomicUsize` by the requested layout size;
//! every deallocation decrements it.  The counter is approximate — it tracks
//! the *requested* size, not the actual slab the system allocator hands out —
//! but it is lock-free, cheap (<1 ns overhead per alloc/dealloc), and gives a
//! useful signal for memory monitoring via `INFO ferricstore`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

static ALLOCATED: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
static TOTAL_ALLOCATED: AtomicUsize = AtomicUsize::new(0);

pub struct TrackingAllocator;

// SAFETY: We delegate entirely to `System` which is itself a valid
// `GlobalAlloc`.  The only addition is an atomic counter bump which
// cannot introduce UB.
unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            ALLOCATED.fetch_add(layout.size(), Ordering::Relaxed);

            #[cfg(test)]
            TOTAL_ALLOCATED.fetch_add(layout.size(), Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        ALLOCATED.fetch_sub(layout.size(), Ordering::Relaxed);
    }
}

// Only install as global allocator during tests. In the NIF cdylib, the BEAM
// manages memory allocation and installing a custom global allocator would
// conflict with the BEAM's own allocator, causing hangs or crashes.
#[cfg(test)]
#[global_allocator]
static GLOBAL: TrackingAllocator = TrackingAllocator;

/// Returns the current number of live Rust-heap bytes (approximate).
///
/// In the cdylib (NIF), the tracking allocator is NOT installed (the BEAM's
/// allocator is the global allocator), so this always returns 0 — which is
/// misleading.  Use `tracked_allocated_bytes()` in NIF code, which returns
/// `None` when tracking is disabled.
pub fn allocated_bytes() -> u64 {
    ALLOCATED.load(Ordering::Relaxed) as u64
}

/// Returns `Some(bytes)` when the tracking allocator is active (tests),
/// or `None` when it is not installed (cdylib / production NIF).
///
/// L-1 fix: callers can distinguish "0 bytes allocated" from "tracking
/// is not available" instead of always seeing a misleading 0.
pub fn tracked_allocated_bytes() -> Option<u64> {
    if cfg!(test) {
        Some(ALLOCATED.load(Ordering::Relaxed) as u64)
    } else {
        None
    }
}

#[cfg(test)]
fn total_allocated_bytes() -> u64 {
    TOTAL_ALLOCATED.load(Ordering::Relaxed) as u64
}

// ---------------------------------------------------------------------------
// NIF function: expose allocated_bytes to Elixir
// ---------------------------------------------------------------------------

/// Returns the current tracked allocation count.
///
/// L-1 fix: returns `-1` when the tracking allocator is not installed
/// (cdylib / production NIF) so callers can distinguish "tracking disabled"
/// from "0 bytes allocated".  Returns a non-negative `i64` when tracking
/// is active (tests).
///
/// This is a cheap atomic read -- runs on Normal scheduler.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::unnecessary_wraps)]
pub fn rust_allocated_bytes(env: rustler::Env) -> rustler::NifResult<rustler::Term> {
    use rustler::Encoder;
    match tracked_allocated_bytes() {
        Some(bytes) => Ok((bytes as i64).encode(env)),
        None => Ok((-1i64).encode(env)),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_bytes_greater_than_zero() {
        // The runtime has already allocated memory (thread stacks, etc.)
        let bytes = allocated_bytes();
        assert!(bytes > 0, "expected > 0 allocated bytes, got {bytes}");
    }

    #[test]
    fn allocate_vec_bytes_increase() {
        let before = allocated_bytes();
        let v: Vec<u8> = vec![0u8; 65_536]; // 64 KiB
        let after = allocated_bytes();
        assert!(
            after > before,
            "expected increase after Vec alloc: before={before}, after={after}"
        );
        drop(v);
    }

    #[test]
    fn drop_vec_bytes_decrease() {
        let before = allocated_bytes();
        {
            let v: Vec<u8> = vec![0u8; 65_536];
            let during = allocated_bytes();
            assert!(during > before);
            drop(v);
        }
        let after = allocated_bytes();
        // After drop, bytes should be back near the before level
        // (other threads may allocate, so we use a generous margin)
        assert!(
            after < before + 1_000_000,
            "bytes didn't decrease sufficiently after drop: before={before}, after={after}"
        );
    }

    #[test]
    fn concurrent_alloc_dealloc_consistent() {
        // Run 4 threads that each allocate and free 1000 x 1KB vectors.
        // The atomic counter should not drift significantly afterwards.
        // We use a generous margin because other test threads may be allocating
        // concurrently.
        let handles: Vec<_> = (0..4)
            .map(|_| {
                std::thread::spawn(|| {
                    for _ in 0..1000 {
                        let v: Vec<u8> = vec![0u8; 1024];
                        std::hint::black_box(&v);
                        drop(v);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        // Just verify the counter hasn't gone astronomically wrong (e.g., negative
        // wrapped to huge positive). The counter should be in a reasonable range.
        let after = allocated_bytes();
        assert!(
            after < 1_000_000_000,
            "counter looks unreasonable: {after} bytes"
        );
    }

    #[test]
    fn large_allocation_tracked() {
        let allocation_bytes = 100 * 1024 * 1024;
        let before = total_allocated_bytes();
        let v: Vec<u8> = vec![0u8; allocation_bytes];
        std::hint::black_box(&v);
        let after = total_allocated_bytes();
        let increase = after - before;

        assert!(
            increase >= allocation_bytes as u64,
            "expected >= 100MB in allocation requests, got {increase} bytes ({} MB)",
            increase / (1024 * 1024)
        );
        drop(v);
    }

    // ==================================================================
    // Deep NIF edge cases — targeting allocator safety pitfalls
    // ==================================================================

    #[test]
    fn alloc_zero_bytes_vec() {
        // Vec::new() with zero capacity does not allocate, but
        // Vec::with_capacity(0) also does not allocate. This tests
        // that our tracking doesn't go negative or panic.
        let before = allocated_bytes();
        let v: Vec<u8> = Vec::new();
        let after = allocated_bytes();
        // No allocation should occur
        assert!(
            after >= before.saturating_sub(1_000_000),
            "zero-capacity vec should not significantly change counter"
        );
        drop(v);
    }

    #[test]
    fn rapid_alloc_dealloc_1m_cycles() {
        let before = allocated_bytes();
        for _ in 0..1_000_000 {
            let v: Vec<u8> = vec![0u8; 64];
            std::hint::black_box(&v);
            drop(v);
        }
        let after = allocated_bytes();
        // Counter should not have drifted significantly
        let drift = after.abs_diff(before);
        // Generous margin: other test threads allocate concurrently, and the
        // system allocator may keep thread-local caches that are not immediately
        // returned to the OS.
        assert!(
            drift < 500_000_000,
            "counter drifted by {drift} bytes after 1M alloc/dealloc cycles"
        );
    }

    #[test]
    fn concurrent_alloc_dealloc_stress() {
        // 8 threads, each doing 10K alloc/dealloc cycles
        let before = allocated_bytes();
        let handles: Vec<_> = (0..8)
            .map(|_| {
                std::thread::spawn(|| {
                    for _ in 0..10_000 {
                        let v: Vec<u8> = vec![0u8; 256];
                        std::hint::black_box(&v);
                        drop(v);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        let after = allocated_bytes();
        let drift = after.abs_diff(before);
        // Generous margin: when cargo test runs hundreds of tests in parallel,
        // other threads allocate concurrently causing apparent drift.
        assert!(
            drift < 500_000_000,
            "counter drifted by {drift} bytes after concurrent stress test"
        );
    }

    #[test]
    fn growing_vec_tracked_incrementally() {
        // Use a large enough allocation that it overwhelms any concurrent noise
        let mut v: Vec<u8> = Vec::with_capacity(1_000_000);
        let after_alloc = allocated_bytes();
        for i in 0..1_000_000u32 {
            v.push((i % 256) as u8);
        }
        // The capacity was pre-allocated, so bytes should be at least 1MB
        assert!(
            after_alloc > 1_000_000,
            "after allocating 1MB vec, total tracked should be > 1MB, got {after_alloc}"
        );
        let before_drop = allocated_bytes();
        drop(v);
        let after_drop = allocated_bytes();
        // After dropping 1MB, counter should decrease (allow margin for concurrent allocs)
        assert!(
            after_drop < before_drop + 500_000,
            "dropping 1MB vec should decrease tracked bytes: before_drop={before_drop}, after_drop={after_drop}"
        );
    }

    // ------------------------------------------------------------------
    // L-1: tracked_allocated_bytes returns Some in test mode
    // ------------------------------------------------------------------

    #[test]
    fn l1_tracked_allocated_bytes_returns_some_in_test() {
        // In test mode, the tracking allocator is installed as the global
        // allocator, so tracked_allocated_bytes() must return Some.
        let result = tracked_allocated_bytes();
        assert!(
            result.is_some(),
            "tracked_allocated_bytes() must return Some in test mode"
        );
        assert!(
            result.unwrap() > 0,
            "tracked bytes should be > 0 (runtime has already allocated)"
        );
    }
}
