//! Tokio runtime for async IO NIFs.
//!
//! Provides a global Tokio multi-threaded runtime that is initialised once
//! (on first access) and lives for the entire BEAM VM lifetime. Async NIF
//! functions spawn tasks onto this runtime to perform blocking disk IO
//! without occupying BEAM scheduler threads.
//!
//! ## Design rationale
//!
//! When a cold GET happens (hot_cache miss), the BEAM Normal scheduler
//! thread would block during `pread()` for ~50-200us. With many concurrent
//! cold reads, all BEAM schedulers can be blocked simultaneously, stalling
//! ALL Elixir processes. By submitting IO work to Tokio worker threads,
//! the BEAM scheduler returns immediately and is free to run other processes.
//!
//! ## Safety
//!
//! The `OnceLock` ensures the runtime is created exactly once. The runtime
//! is never shut down — it lives until the BEAM process exits.

use std::{
    io,
    sync::{
        atomic::{AtomicUsize, Ordering},
        OnceLock,
    },
};
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;

static TOKIO_RT: OnceLock<Result<Runtime, String>> = OnceLock::new();
const DEFAULT_MAX_BLOCKING_THREADS: usize = 16;
const MIN_BLOCKING_THREADS: usize = 1;
const MAX_BLOCKING_THREADS: usize = 256;
const BLOCKING_THREADS_ENV: &str = "FERRICSTORE_TOKIO_BLOCKING_THREADS";
const BLOCKING_ADMISSION_MULTIPLIER: usize = 4;
const MIN_OUTSTANDING_BLOCKING_JOBS: usize = 128;
const MAX_OUTSTANDING_BLOCKING_JOBS: usize = 4_096;
const DEFAULT_MAX_OUTSTANDING_BLOCKING_BYTES: usize = 512 * 1024 * 1024;
const MIN_OUTSTANDING_BLOCKING_BYTES: usize = 16 * 1024 * 1024;
const MAX_OUTSTANDING_BLOCKING_BYTES: usize = 8 * 1024 * 1024 * 1024;
const BLOCKING_BYTES_ENV: &str = "FERRICSTORE_TOKIO_BLOCKING_BYTES";
pub const BLOCKING_OVERLOAD_ERROR: &str = "native async IO overloaded";
pub const ASYNC_INPUT_TOO_LARGE_ERROR: &str = "native async IO request too large";
pub const MAX_ASYNC_INPUT_BYTES: usize = 64 * 1024 * 1024;
const ASYNC_COPIED_INPUT_OVERHEAD_BYTES: usize = 64;

/// Returns a conservative estimate of the owned copy retained by a blocking
/// job. Charging each binary for its vector/tuple storage also bounds requests
/// containing very large numbers of empty values.
pub fn checked_input_bytes(
    lengths: impl IntoIterator<Item = usize>,
) -> Result<usize, &'static str> {
    lengths.into_iter().try_fold(0usize, |total, length| {
        total
            .checked_add(length)
            .and_then(|next| next.checked_add(ASYNC_COPIED_INPUT_OVERHEAD_BYTES))
            .filter(|next| *next <= MAX_ASYNC_INPUT_BYTES)
            .ok_or(ASYNC_INPUT_TOO_LARGE_ERROR)
    })
}

struct BlockingAdmission {
    active: AtomicUsize,
    active_bytes: AtomicUsize,
    limit: usize,
    byte_limit: usize,
}

impl BlockingAdmission {
    const fn new(limit: usize, byte_limit: usize) -> Self {
        Self {
            active: AtomicUsize::new(0),
            active_bytes: AtomicUsize::new(0),
            limit,
            byte_limit,
        }
    }

    fn try_acquire(&self, input_bytes: usize) -> Result<BlockingPermit<'_>, &'static str> {
        if input_bytes > self.byte_limit {
            return Err(BLOCKING_OVERLOAD_ERROR);
        }

        let mut active = self.active.load(Ordering::Acquire);

        loop {
            if active >= self.limit {
                return Err(BLOCKING_OVERLOAD_ERROR);
            }

            match self.active.compare_exchange_weak(
                active,
                active + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(observed) => active = observed,
            }
        }

        let mut active_bytes = self.active_bytes.load(Ordering::Acquire);
        loop {
            let Some(next_bytes) = active_bytes.checked_add(input_bytes) else {
                self.active.fetch_sub(1, Ordering::AcqRel);
                return Err(BLOCKING_OVERLOAD_ERROR);
            };
            if next_bytes > self.byte_limit {
                self.active.fetch_sub(1, Ordering::AcqRel);
                return Err(BLOCKING_OVERLOAD_ERROR);
            }

            match self.active_bytes.compare_exchange_weak(
                active_bytes,
                next_bytes,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(BlockingPermit {
                        admission: self,
                        input_bytes,
                    });
                }
                Err(observed) => active_bytes = observed,
            }
        }
    }
}

struct BlockingPermit<'a> {
    admission: &'a BlockingAdmission,
    input_bytes: usize,
}

impl Drop for BlockingPermit<'_> {
    fn drop(&mut self) {
        let previous_bytes = self
            .admission
            .active_bytes
            .fetch_sub(self.input_bytes, Ordering::AcqRel);
        debug_assert!(
            previous_bytes >= self.input_bytes,
            "blocking byte counter underflow"
        );
        let previous = self.admission.active.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "blocking admission counter underflow");
    }
}

static BLOCKING_ADMISSION: OnceLock<BlockingAdmission> = OnceLock::new();

/// Creates the global Tokio runtime, returning the startup error without
/// panicking when the operating system cannot create its worker threads.
///
/// H-8 fix: limits worker threads to `min(4, num_cpus)` and blocking IO
/// threads to a measured/configurable cap instead of Tokio's high default. The
/// runtime is only used for disk IO operations (pread, fsync), which are
/// limited by NVMe parallelism; under bursts, extra work should queue instead
/// of creating hundreds of competing OS threads beside BEAM schedulers.
///
/// `FERRICSTORE_TOKIO_BLOCKING_THREADS` is intentionally read once at runtime
/// creation. Benchmark 16/32/64 by starting a fresh BEAM for each value.
///
pub fn initialize() -> Result<&'static Runtime, &'static str> {
    TOKIO_RT
        .get_or_init(build_runtime)
        .as_ref()
        .map_err(String::as_str)
}

fn build_runtime() -> Result<Runtime, String> {
    build_runtime_with(|| {
        let num_cpus = std::thread::available_parallelism().map_or(4, std::num::NonZero::get);
        let workers = num_cpus.clamp(1, 4);
        let blocking_threads = blocking_thread_cap();
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(workers)
            .max_blocking_threads(blocking_threads)
            .thread_name("ferric-tokio")
            .enable_all()
            .build()
    })
}

fn build_runtime_with(builder: impl FnOnce() -> io::Result<Runtime>) -> Result<Runtime, String> {
    builder().map_err(|error| format!("Failed to create Tokio runtime: {error}"))
}

/// Returns the runtime after NIF load has initialised it successfully.
///
/// # Panics
///
/// Panics only when called outside the loaded NIF lifecycle after runtime
/// initialisation failed. The NIF load callback rejects that state.
pub fn runtime() -> &'static Runtime {
    initialize().expect("Tokio runtime was not initialised during NIF load")
}

/// Submits one blocking job only when the process-wide native IO budget has
/// capacity. Tokio's blocking pool bounds active threads but not its waiting
/// queue, so every async NIF must use this entry point to prevent unbounded
/// closure and request-payload retention during overload.
pub fn try_spawn_blocking<F, R>(job: F) -> Result<JoinHandle<R>, &'static str>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let permit = blocking_admission().try_acquire(0)?;
    Ok(runtime().spawn_blocking(move || {
        let _permit = permit;
        job()
    }))
}

pub fn try_spawn_blocking_with_input<P, I, F, R>(
    input_bytes: usize,
    prepare: P,
    job: F,
) -> Result<JoinHandle<R>, &'static str>
where
    P: FnOnce() -> I,
    I: Send + 'static,
    F: FnOnce(I) -> R + Send + 'static,
    R: Send + 'static,
{
    let (permit, input) = try_prepare_with_admission(blocking_admission(), input_bytes, prepare)?;
    Ok(runtime().spawn_blocking(move || {
        let _permit = permit;
        job(input)
    }))
}

fn try_prepare_with_admission<'a, P, I>(
    admission: &'a BlockingAdmission,
    input_bytes: usize,
    prepare: P,
) -> Result<(BlockingPermit<'a>, I), &'static str>
where
    P: FnOnce() -> I,
{
    let permit = admission.try_acquire(input_bytes)?;
    Ok((permit, prepare()))
}

fn blocking_admission() -> &'static BlockingAdmission {
    BLOCKING_ADMISSION.get_or_init(|| {
        BlockingAdmission::new(
            blocking_admission_limit(blocking_thread_cap()),
            blocking_byte_limit(),
        )
    })
}

fn blocking_admission_limit(blocking_threads: usize) -> usize {
    blocking_threads
        .saturating_mul(BLOCKING_ADMISSION_MULTIPLIER)
        .clamp(MIN_OUTSTANDING_BLOCKING_JOBS, MAX_OUTSTANDING_BLOCKING_JOBS)
}

fn blocking_thread_cap() -> usize {
    blocking_thread_cap_from_env_value(std::env::var(BLOCKING_THREADS_ENV).ok().as_deref())
}

fn blocking_thread_cap_from_env_value(raw: Option<&str>) -> usize {
    raw.and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(DEFAULT_MAX_BLOCKING_THREADS)
        .clamp(MIN_BLOCKING_THREADS, MAX_BLOCKING_THREADS)
}

fn blocking_byte_limit() -> usize {
    blocking_byte_limit_from_env_value(std::env::var(BLOCKING_BYTES_ENV).ok().as_deref())
}

fn blocking_byte_limit_from_env_value(raw: Option<&str>) -> usize {
    raw.and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(DEFAULT_MAX_OUTSTANDING_BLOCKING_BYTES)
        .clamp(
            MIN_OUTSTANDING_BLOCKING_BYTES,
            MAX_OUTSTANDING_BLOCKING_BYTES,
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_blocking_thread_cap_from_env_value() {
        assert_eq!(blocking_thread_cap_from_env_value(None), 16);
        assert_eq!(blocking_thread_cap_from_env_value(Some("")), 16);
        assert_eq!(blocking_thread_cap_from_env_value(Some("bad")), 16);
        assert_eq!(blocking_thread_cap_from_env_value(Some("32")), 32);
        assert_eq!(blocking_thread_cap_from_env_value(Some("0")), 1);
        assert_eq!(blocking_thread_cap_from_env_value(Some("9999")), 256);
    }

    #[test]
    fn parses_blocking_byte_limit_from_env_value() {
        assert_eq!(
            blocking_byte_limit_from_env_value(None),
            DEFAULT_MAX_OUTSTANDING_BLOCKING_BYTES
        );
        assert_eq!(
            blocking_byte_limit_from_env_value(Some("bad")),
            DEFAULT_MAX_OUTSTANDING_BLOCKING_BYTES
        );
        assert_eq!(
            blocking_byte_limit_from_env_value(Some("33554432")),
            32 * 1024 * 1024
        );
        assert_eq!(
            blocking_byte_limit_from_env_value(Some("0")),
            MIN_OUTSTANDING_BLOCKING_BYTES
        );
        assert_eq!(
            blocking_byte_limit_from_env_value(Some("18446744073709551615")),
            MAX_OUTSTANDING_BLOCKING_BYTES
        );
    }

    #[test]
    fn blocking_admission_limit_bounds_the_active_pool_and_waiting_queue() {
        assert_eq!(blocking_admission_limit(1), 128);
        assert_eq!(blocking_admission_limit(16), 128);
        assert_eq!(blocking_admission_limit(32), 128);
        assert_eq!(blocking_admission_limit(64), 256);
        assert_eq!(blocking_admission_limit(usize::MAX), 4_096);
    }

    #[test]
    fn blocking_admission_rejects_overload_and_recovers_when_permits_drop() {
        let admission = BlockingAdmission::new(2, 16);
        let first = admission.try_acquire(4).unwrap();
        let second = admission.try_acquire(4).unwrap();

        assert!(matches!(
            admission.try_acquire(4),
            Err(BLOCKING_OVERLOAD_ERROR)
        ));

        drop(first);
        let replacement = admission.try_acquire(4).unwrap();
        drop(second);
        drop(replacement);
        assert_eq!(admission.active.load(Ordering::Acquire), 0);
    }

    #[test]
    fn blocking_admission_enforces_and_releases_the_byte_budget() {
        let admission = BlockingAdmission::new(8, 10);
        let first = admission.try_acquire(6).unwrap();

        assert_eq!(admission.active_bytes.load(Ordering::Acquire), 6);
        assert!(matches!(
            admission.try_acquire(5),
            Err(BLOCKING_OVERLOAD_ERROR)
        ));
        assert_eq!(admission.active.load(Ordering::Acquire), 1);
        assert_eq!(admission.active_bytes.load(Ordering::Acquire), 6);

        drop(first);
        assert_eq!(admission.active.load(Ordering::Acquire), 0);
        assert_eq!(admission.active_bytes.load(Ordering::Acquire), 0);
        assert!(admission.try_acquire(10).is_ok());
    }

    #[test]
    fn rejected_input_is_not_prepared_or_copied() {
        let admission = BlockingAdmission::new(1, 4);
        let mut prepared = false;
        let result = try_prepare_with_admission(&admission, 5, || {
            prepared = true;
            vec![0; 5]
        });

        assert!(matches!(result, Err(BLOCKING_OVERLOAD_ERROR)));
        assert!(!prepared);
    }

    #[test]
    fn async_input_bytes_are_checked_before_copying() {
        let empty_copy_bytes = checked_input_bytes([0]).unwrap();
        assert!(empty_copy_bytes >= std::mem::size_of::<Vec<u8>>());
        assert_eq!(
            checked_input_bytes((0..=MAX_ASYNC_INPUT_BYTES / empty_copy_bytes).map(|_| 0)),
            Err(ASYNC_INPUT_TOO_LARGE_ERROR)
        );
        assert_eq!(
            checked_input_bytes([MAX_ASYNC_INPUT_BYTES - empty_copy_bytes]).unwrap(),
            MAX_ASYNC_INPUT_BYTES
        );
        assert_eq!(
            checked_input_bytes([MAX_ASYNC_INPUT_BYTES, 1]),
            Err(ASYNC_INPUT_TOO_LARGE_ERROR)
        );
        assert_eq!(
            checked_input_bytes([usize::MAX, 1]),
            Err(ASYNC_INPUT_TOO_LARGE_ERROR)
        );
    }

    #[test]
    fn runtime_builder_failure_is_returned_instead_of_panicking() {
        let result = build_runtime_with(|| Err(std::io::Error::other("thread limit")));

        assert_eq!(
            result.unwrap_err(),
            "Failed to create Tokio runtime: thread limit"
        );
    }

    #[test]
    fn runtime_creates_successfully() {
        let rt = runtime();
        // Verify we can spawn a task and get a result
        let handle = rt.spawn(async { 42 });
        let result = rt.block_on(handle).unwrap();
        assert_eq!(result, 42);
    }

    #[test]
    fn runtime_is_singleton() {
        let rt1 = std::ptr::from_ref::<Runtime>(runtime());
        let rt2 = std::ptr::from_ref::<Runtime>(runtime());
        assert_eq!(rt1, rt2, "runtime() must return the same instance");
    }

    #[test]
    fn multiple_concurrent_spawns() {
        let rt = runtime();
        let mut handles = Vec::new();
        for i in 0..100 {
            handles.push(rt.spawn(async move { i * 2 }));
        }
        for (i, handle) in handles.into_iter().enumerate() {
            let result = rt.block_on(handle).unwrap();
            assert_eq!(result, i * 2);
        }
    }

    // -----------------------------------------------------------------------
    // Edge-case tests
    // -----------------------------------------------------------------------

    #[test]
    fn runtime_survives_panic_in_spawned_task() {
        let rt = runtime();
        let panic_handle = rt.spawn(async {
            panic!("intentional panic in async task");
        });
        // The panic should be caught by tokio; the JoinHandle returns Err
        let result = rt.block_on(panic_handle);
        assert!(result.is_err(), "panicking task should return JoinError");

        // Runtime should still be functional
        let ok_handle = rt.spawn(async { 42 });
        let ok_result = rt.block_on(ok_handle).unwrap();
        assert_eq!(ok_result, 42);
    }

    #[test]
    fn multiple_concurrent_spawns_no_deadlock() {
        let _rt = runtime();
        // Spawn tasks from multiple threads concurrently
        let handles: Vec<_> = (0..4)
            .map(|t| {
                std::thread::spawn(move || {
                    let rt = runtime();
                    let mut tasks = Vec::new();
                    for i in 0..25 {
                        tasks.push(rt.spawn(async move { t * 100 + i }));
                    }
                    let mut results = Vec::new();
                    for task in tasks {
                        results.push(rt.block_on(task).unwrap());
                    }
                    results
                })
            })
            .collect();

        for h in handles {
            let results = h.join().unwrap();
            assert_eq!(results.len(), 25);
        }
    }

    #[test]
    fn spawn_after_runtime_creation() {
        // First call creates the runtime
        let _rt1 = runtime();
        // Second call reuses it and spawns more
        let rt2 = runtime();
        let h = rt2.spawn(async { 99 });
        let result = rt2.block_on(h).unwrap();
        assert_eq!(result, 99);
    }

    #[test]
    fn heavy_load_10k_concurrent_spawns() {
        let rt = runtime();
        let mut handles = Vec::with_capacity(10_000);
        for i in 0..10_000u64 {
            handles.push(rt.spawn(async move { i }));
        }
        let mut sum = 0u64;
        for h in handles {
            sum += rt.block_on(h).unwrap();
        }
        // Sum of 0..9999 = 9999*10000/2 = 49_995_000
        assert_eq!(sum, 49_995_000);
    }

    // ==================================================================
    // Deep NIF edge cases — targeting async runtime / FFI pitfalls
    // ==================================================================

    #[test]
    fn panic_in_tokio_task_does_not_crash_runtime() {
        let rt = runtime();
        // Spawn multiple panicking tasks
        let mut panic_handles = Vec::new();
        for i in 0..10 {
            panic_handles.push(rt.spawn(async move {
                assert!(i % 2 != 0, "deliberate panic in task {i}");
                i
            }));
        }

        // All panicking tasks should return JoinError
        for h in panic_handles {
            let _ = rt.block_on(h); // Ok or Err, but no crash
        }

        // Runtime must still be functional
        let ok_handle = rt.spawn(async { 999 });
        assert_eq!(rt.block_on(ok_handle).unwrap(), 999);
    }

    #[test]
    fn task_with_large_closure_1mb() {
        let rt = runtime();
        let large_data = vec![0xABu8; 1_024 * 1_024]; // 1 MB
        let handle = rt.spawn(async move { large_data.len() });
        let result = rt.block_on(handle).unwrap();
        assert_eq!(result, 1_024 * 1_024);
    }

    #[test]
    fn spawn_from_multiple_os_threads_concurrently() {
        let handles: Vec<_> = (0..20)
            .map(|t| {
                std::thread::spawn(move || {
                    let rt = runtime();
                    let mut tasks = Vec::new();
                    for i in 0..50 {
                        tasks.push(rt.spawn(async move { t * 1000 + i }));
                    }
                    let mut results = Vec::new();
                    for task in tasks {
                        results.push(rt.block_on(task).unwrap());
                    }
                    results
                })
            })
            .collect();

        let mut total = 0;
        for h in handles {
            let results = h.join().unwrap();
            assert_eq!(results.len(), 50);
            total += results.len();
        }
        assert_eq!(total, 1000);
    }

    #[test]
    fn nested_spawn_works() {
        let rt = runtime();
        let outer = rt.spawn(async {
            let inner = tokio::spawn(async { 42 });
            inner.await.unwrap()
        });
        let result = rt.block_on(outer).unwrap();
        assert_eq!(result, 42);
    }

    // ------------------------------------------------------------------
    // H-8: Tokio runtime limits worker threads to min(4, num_cpus)
    // ------------------------------------------------------------------

    #[test]
    fn h8_runtime_functional_with_limited_threads() {
        let rt = runtime();
        // Verify that spawning more tasks than worker threads still works
        // (tasks are multiplexed onto the worker pool).
        let mut handles = Vec::with_capacity(100);
        for i in 0..100u64 {
            handles.push(rt.spawn(async move { i * 2 }));
        }
        let mut sum = 0u64;
        for h in handles {
            sum += rt.block_on(h).unwrap();
        }
        assert_eq!(sum, 9900); // sum of 0*2 + 1*2 + ... + 99*2 = 2 * 4950 = 9900
    }

    #[test]
    fn h8_concurrent_io_tasks_complete() {
        let rt = runtime();
        // Spawn many tasks that do a small amount of compute work.
        let mut handles = Vec::new();
        for i in 0u64..20 {
            handles.push(rt.spawn(async move {
                // Simulate a small amount of work
                let mut sum = 0u64;
                for j in 0..100 {
                    sum += i + j;
                }
                sum
            }));
        }
        for h in handles {
            let result = rt.block_on(h).unwrap();
            assert!(result > 0);
        }
    }

    #[test]
    fn spawn_blocking_pool_is_bounded() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        use std::time::Duration;

        let rt = runtime();
        let active = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..64 {
            let active = Arc::clone(&active);
            let max_seen = Arc::clone(&max_seen);

            handles.push(rt.spawn_blocking(move || {
                let now = active.fetch_add(1, Ordering::SeqCst) + 1;

                max_seen
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |prev| {
                        Some(prev.max(now))
                    })
                    .ok();

                std::thread::sleep(Duration::from_millis(200));
                active.fetch_sub(1, Ordering::SeqCst);
            }));
        }

        for handle in handles {
            rt.block_on(handle).unwrap();
        }

        let expected_cap = blocking_thread_cap();
        assert!(
            max_seen.load(Ordering::SeqCst) <= expected_cap,
            "spawn_blocking pool must be capped; saw {} concurrent blocking jobs",
            max_seen.load(Ordering::SeqCst)
        );
    }

    // ------------------------------------------------------------------
    // Unrecoverable scenario resilience tests
    // ------------------------------------------------------------------

    // NOTE: Stack overflow in a Tokio worker thread causes SIGABRT (process death).
    // This is NOT recoverable — Tokio cannot catch or replace the aborted thread.
    // Tested and confirmed: `thread 'ferric-tokio' has overflowed its stack`
    // followed by `fatal runtime error: stack overflow, aborting`.
    //
    // Mitigation: worker_threads use 8MB stacks (Tokio default). Our tasks only
    // do flat IO operations (pread, fsync, put_batch) with bounded stack depth.
    // No recursive algorithms are used in spawned tasks.

    /// Verify that the Tokio runtime survives a task that returns `Err` from
    /// a fallible allocation and can still schedule subsequent tasks.
    ///
    /// We force the error path via `Vec::try_reserve_exact` for a
    /// `Vec<usize>` (8 bytes per element) with a count > `isize::MAX / 8`.
    /// `Vec` always validates `capacity * size_of::<T>() <= isize::MAX` and
    /// returns `TryReserveError::CapacityOverflow` without touching the
    /// allocator — this is guaranteed by the standard library contract and
    /// does not depend on platform/mode (previous `usize::MAX / 2` for
    /// `Vec<u8>` could succeed under release-mode overcommit and failed
    /// on macOS aarch64).
    #[test]
    fn large_allocation_failure_in_task_is_contained() {
        let rt = runtime();

        let alloc_handle = rt.spawn(async {
            // Vec<usize>: element size = 8, so capacity * 8 must fit in isize::MAX.
            // Request capacity = isize::MAX (so required bytes = 8 * isize::MAX,
            // which overflows isize::MAX). Guaranteed CapacityOverflow.
            let mut v: Vec<usize> = Vec::new();
            match v.try_reserve_exact(isize::MAX as usize) {
                Ok(()) => panic!("try_reserve_exact with capacity-overflow must fail"),
                Err(_) => "allocation_failed_gracefully",
            }
        });

        let result = rt.block_on(alloc_handle).unwrap();
        assert_eq!(result, "allocation_failed_gracefully");

        // Runtime still alive
        let health = rt.spawn(async { 99 });
        assert_eq!(rt.block_on(health).unwrap(), 99);
    }

    /// Verify that blocking a Tokio worker thread for an extended period
    /// doesn't prevent other tasks from completing (Tokio has work-stealing).
    #[test]
    fn blocking_task_does_not_starve_other_tasks() {
        let rt = runtime();

        // Spawn a blocking task on a worker thread
        let blocker = rt.spawn(async {
            // This blocks the worker for 500ms
            std::thread::sleep(std::time::Duration::from_millis(500));
            "blocker_done"
        });

        // Spawn 10 quick tasks — they should complete even while one worker is blocked
        let mut quick_handles = Vec::new();
        for i in 0..10u64 {
            quick_handles.push(rt.spawn(async move { i * 3 }));
        }

        // All quick tasks should complete within 1 second
        for (i, h) in quick_handles.into_iter().enumerate() {
            let val = rt.block_on(h).unwrap();
            assert_eq!(
                val,
                i as u64 * 3,
                "quick task {i} should complete even with a blocked worker"
            );
        }

        // Blocker should also eventually finish
        let blocker_result = rt.block_on(blocker).unwrap();
        assert_eq!(blocker_result, "blocker_done");
    }

    /// Verify that many simultaneous panics across tasks don't crash the runtime.
    #[test]
    fn mass_panic_50_tasks_runtime_survives() {
        let rt = runtime();

        let mut handles = Vec::new();
        for i in 0..50 {
            handles.push(rt.spawn(async move {
                panic!("mass panic task {i}");
            }));
        }

        // All tasks should return JoinError
        for h in handles {
            let result = rt.block_on(h);
            assert!(result.is_err());
        }

        // Runtime must still be functional
        let health = rt.spawn(async { 777 });
        assert_eq!(rt.block_on(health).unwrap(), 777);
    }
}
