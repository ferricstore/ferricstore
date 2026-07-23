use criterion::{black_box, BenchmarkId, Criterion, Throughput};

#[allow(dead_code, unused_imports)]
#[path = "../src/fql.rs"]
mod fql;
#[path = "support/fql_workloads.rs"]
mod workloads;

fn parse_any(query: &[u8]) {
    let _ = black_box(fql::parse_with_diagnostic(black_box(query)));
}

fn parse_valid(query: &[u8]) {
    black_box(
        fql::parse_with_diagnostic(black_box(query)).expect("benchmark query must remain valid"),
    );
}

fn shapes(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("fql_parser/shapes");
    for (name, query) in workloads::parser_shapes() {
        group.throughput(Throughput::Bytes(query.len() as u64));
        group.bench_with_input(
            BenchmarkId::new(name, query.len()),
            &query,
            |bencher, query| {
                bencher.iter(|| parse_valid(query));
            },
        );
    }
    group.finish();
}

fn maximum_inputs(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("fql_parser/maximum_inputs");
    for (name, query, valid) in [
        ("max_valid", workloads::max_valid_query(), true),
        ("max_malformed", workloads::max_malformed_query(), false),
    ] {
        group.throughput(Throughput::Bytes(query.len() as u64));
        group.bench_with_input(
            BenchmarkId::new(name, query.len()),
            &query,
            |bencher, query| {
                if valid {
                    bencher.iter(|| parse_valid(query));
                } else {
                    bencher.iter(|| parse_any(query));
                }
            },
        );
    }
    group.finish();
}

fn token_scaling(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("fql_parser/token_scaling");
    for tokens in [1, 16, 64, 128, workloads::MAX_TOKENS] {
        let query = workloads::token_scaling_query(tokens);
        group.throughput(Throughput::Bytes(query.len() as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(tokens),
            &query,
            |bencher, query| {
                bencher.iter(|| parse_any(query));
            },
        );
    }
    group.finish();
}

fn predicate_scaling(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("fql_parser/predicate_scaling");
    for predicates in [
        1,
        4,
        workloads::MAX_PREDICATES,
        workloads::MAX_PREDICATES + 1,
        16,
        48,
    ] {
        let query = workloads::predicate_scaling_query(predicates);
        group.throughput(Throughput::Bytes(query.len() as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(predicates),
            &query,
            |bencher, query| bencher.iter(|| parse_any(query)),
        );
    }
    group.finish();
}

fn in_cardinality(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("fql_parser/in_cardinality");
    for cardinality in [
        1,
        4,
        16,
        workloads::MAX_IN_VALUES,
        workloads::MAX_IN_VALUES + 1,
        64,
        96,
    ] {
        let query = workloads::in_cardinality_query(cardinality);
        group.throughput(Throughput::Bytes(query.len() as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(cardinality),
            &query,
            |bencher, query| bencher.iter(|| parse_any(query)),
        );
    }
    group.finish();
}

fn metadata_fields(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("fql_parser/metadata_fields");
    for (name, bracketed) in [("dotted", false), ("bracketed", true)] {
        let query = workloads::metadata_field_query(bracketed);
        group.throughput(Throughput::Bytes(query.len() as u64));
        group.bench_with_input(
            BenchmarkId::new(name, query.len()),
            &query,
            |bencher, query| {
                bencher.iter(|| parse_valid(query));
            },
        );
    }
    group.finish();
}

fn escaped_strings(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("fql_parser/escaped_strings");
    for escaped_quotes in [1, 8, 64, 512] {
        let query = workloads::escaped_string_query(escaped_quotes);
        group.throughput(Throughput::Bytes(query.len() as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(escaped_quotes),
            &query,
            |bencher, query| bencher.iter(|| parse_valid(query)),
        );
    }
    group.finish();
}

fn adversarial(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("fql_parser/adversarial");
    for (name, query) in workloads::adversarial_queries() {
        group.throughput(Throughput::Bytes(query.len() as u64));
        group.bench_with_input(
            BenchmarkId::new(name, query.len()),
            &query,
            |bencher, query| {
                bencher.iter(|| parse_any(query));
            },
        );
    }
    group.finish();
}

fn main() {
    assert_eq!(workloads::MAX_QUERY_BYTES, fql::MAX_QUERY_BYTES);
    assert_eq!(workloads::MAX_TOKENS, fql::MAX_TOKENS);
    assert_eq!(workloads::MAX_PREDICATES, fql::MAX_PREDICATES);
    assert_eq!(workloads::MAX_IN_VALUES, fql::MAX_IN_VALUES);

    let mut criterion = Criterion::default().configure_from_args();
    shapes(&mut criterion);
    maximum_inputs(&mut criterion);
    token_scaling(&mut criterion);
    predicate_scaling(&mut criterion);
    in_cardinality(&mut criterion);
    metadata_fields(&mut criterion);
    escaped_strings(&mut criterion);
    adversarial(&mut criterion);
    criterion.final_summary();
}
