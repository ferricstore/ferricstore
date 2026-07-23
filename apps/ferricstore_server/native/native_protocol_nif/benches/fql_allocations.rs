use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};

#[allow(dead_code, unused_imports)]
#[path = "../src/fql.rs"]
mod fql;
#[allow(dead_code)]
#[path = "support/fql_workloads.rs"]
mod workloads;

struct CountingAllocator;

thread_local! {
    static TRACK_ALLOCATIONS: Cell<bool> = const { Cell::new(false) };
}

static ALLOCATION_COUNT: AtomicU64 = AtomicU64::new(0);
static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if tracking() {
            ALLOCATION_COUNT.fetch_add(1, Ordering::Relaxed);
            ALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if tracking() {
            ALLOCATION_COUNT.fetch_add(1, Ordering::Relaxed);
            ALLOCATED_BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

fn tracking() -> bool {
    TRACK_ALLOCATIONS.with(Cell::get)
}

fn allocation_profile(name: &str, query: &[u8], iterations: u64) -> (u64, u64) {
    let _ = black_box(fql::parse_with_diagnostic(query));
    ALLOCATION_COUNT.store(0, Ordering::Relaxed);
    ALLOCATED_BYTES.store(0, Ordering::Relaxed);
    TRACK_ALLOCATIONS.with(|tracking| tracking.set(true));

    for _ in 0..iterations {
        let _ = black_box(fql::parse_with_diagnostic(black_box(query)));
    }

    TRACK_ALLOCATIONS.with(|tracking| tracking.set(false));
    let allocations = ALLOCATION_COUNT.load(Ordering::Relaxed) / iterations;
    let bytes = ALLOCATED_BYTES.load(Ordering::Relaxed) / iterations;
    println!(
        "allocation_profile name={name} iterations={iterations} allocations_per_op={allocations} bytes_per_op={bytes}"
    );
    (allocations, bytes)
}

fn enforce_ceiling(name: &str, actual: (u64, u64), ceiling: (u64, u64)) {
    assert!(
        actual.0 <= ceiling.0,
        "{name} allocation count regression: {} > {}",
        actual.0,
        ceiling.0
    );
    assert!(
        actual.1 <= ceiling.1,
        "{name} allocated-byte regression: {} > {}",
        actual.1,
        ceiling.1
    );
}

fn shape_ceiling(name: &str) -> (u64, u64) {
    match name {
        "point" => (4, 2_300),
        "collection" => (10, 2_450),
        "count" => (8, 2_250),
        "history" => (4, 2_250),
        "explain" | "explain_analyze" => (7, 2_300),
        _ => panic!("missing allocation ceiling for {name}"),
    }
}

fn adversarial_ceiling(name: &str) -> Option<(u64, u64)> {
    match name {
        "max_malformed" => None,
        "quote_storm" => Some((12, 21_500)),
        "long_identifier" => Some((2, 2_000)),
        "huge_integer" => Some((3, 2_150)),
        "max_tokens" => Some((5, 28_000)),
        _ => panic!("missing adversarial allocation ceiling for {name}"),
    }
}

fn main() {
    assert_eq!(workloads::MAX_QUERY_BYTES, fql::MAX_QUERY_BYTES);
    assert_eq!(workloads::MAX_TOKENS, fql::MAX_TOKENS);
    assert_eq!(workloads::MAX_PREDICATES, fql::MAX_PREDICATES);
    assert_eq!(workloads::MAX_IN_VALUES, fql::MAX_IN_VALUES);

    let iterations = std::env::var("BENCH_ALLOC_ITERATIONS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(10_000);

    for (name, query) in workloads::parser_shapes() {
        let actual = allocation_profile(name, &query, iterations);
        enforce_ceiling(name, actual, shape_ceiling(name));
    }

    let max_valid = workloads::max_valid_query();
    let actual = allocation_profile("max_valid", &max_valid, iterations);
    enforce_ceiling("max_valid", actual, (16, 56_000));

    let max_malformed = workloads::max_malformed_query();
    let actual = allocation_profile("max_malformed", &max_malformed, iterations);
    enforce_ceiling("max_malformed", actual, (14, 42_000));

    for (predicates, ceiling) in [
        (1, (4, 2_250)),
        (4, (4, 2_250)),
        (workloads::MAX_PREDICATES, (8, 8_500)),
        (workloads::MAX_PREDICATES + 1, (7, 8_500)),
        (16, (7, 8_500)),
        (48, (9, 29_000)),
    ] {
        let name = format!("predicate_scaling/{predicates}");
        let query = workloads::predicate_scaling_query(predicates);
        let actual = allocation_profile(&name, &query, iterations);
        enforce_ceiling(&name, actual, ceiling);
    }

    for (cardinality, ceiling) in [
        (workloads::MAX_IN_VALUES, (52, 8_500)),
        (workloads::MAX_IN_VALUES + 1, (52, 8_500)),
        (96, (140, 32_000)),
    ] {
        let name = format!("in_cardinality/{cardinality}");
        let query = workloads::in_cardinality_query(cardinality);
        let actual = allocation_profile(&name, &query, iterations);
        enforce_ceiling(&name, actual, ceiling);
    }

    let query = workloads::escaped_string_query(512);
    let actual = allocation_profile("escaped_strings/512", &query, iterations);
    enforce_ceiling("escaped_strings/512", actual, (12, 5_600));

    for (name, query) in workloads::adversarial_queries() {
        if let Some(ceiling) = adversarial_ceiling(name) {
            let actual = allocation_profile(name, &query, iterations);
            enforce_ceiling(name, actual, ceiling);
        }
    }
}
