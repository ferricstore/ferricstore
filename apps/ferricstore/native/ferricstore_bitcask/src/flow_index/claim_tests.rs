use super::*;

#[test]
fn claim_due_candidates_matches_reference_for_merge_edges() {
    let long_key = format!("due:{}", "key".repeat(96));
    let mut index = FlowOrderedIndex::default();

    for (key, member, score) in [
        (b"due:queued".as_slice(), b"queued-z".as_slice(), 20.0),
        (b"due:queued".as_slice(), b"queued-a".as_slice(), 10.0),
        (b"due:queued".as_slice(), b"same".as_slice(), 5.0),
        (b"due:retry".as_slice(), b"retry-a".as_slice(), 10.0),
        (b"due:retry".as_slice(), b"retry-z".as_slice(), 30.0),
        (b"due:retry".as_slice(), b"same".as_slice(), 5.0),
        (long_key.as_bytes(), b"long-a".as_slice(), 15.0),
        (b"due:numeric".as_slice(), b"negative-zero".as_slice(), -0.0),
        (b"due:numeric".as_slice(), b"positive-zero".as_slice(), 0.0),
        (
            b"due:numeric".as_slice(),
            b"infinity".as_slice(),
            f64::INFINITY,
        ),
        (
            b"due:numeric".as_slice(),
            b"not-due-nan".as_slice(),
            f64::NAN,
        ),
    ] {
        index.put(key, member, score, false);
    }

    let cases = [
        (
            vec![
                b"due:queued".as_slice(),
                b"due:retry".as_slice(),
                b"missing".as_slice(),
            ],
            25.0,
            16,
            16,
        ),
        (
            vec![
                b"due:queued".as_slice(),
                b"due:queued".as_slice(),
                long_key.as_bytes(),
                b"due:numeric".as_slice(),
            ],
            20.0,
            9,
            4,
        ),
        (vec![b"due:numeric".as_slice()], 0.0, usize::MAX, usize::MAX),
        (
            vec![b"due:queued".as_slice(), b"due:retry".as_slice()],
            f64::NAN,
            16,
            16,
        ),
        (Vec::new(), 25.0, 16, 16),
        (vec![b"due:queued".as_slice()], 25.0, 0, 16),
        (vec![b"due:queued".as_slice()], 25.0, 16, 0),
    ];

    for (keys, max_score, limit, max_scan) in cases {
        assert_eq!(
            index.claim_due_candidates(&keys, max_score, limit, max_scan),
            reference_claim_due_candidates(&index, &keys, max_score, limit, max_scan),
            "keys={keys:?}, max_score={max_score:?}, limit={limit}, max_scan={max_scan}"
        );
    }
}

fn reference_claim_due_candidates(
    index: &FlowOrderedIndex,
    keys: &[&[u8]],
    max_score: f64,
    limit: usize,
    max_scan: usize,
) -> Vec<(Vec<u8>, Vec<u8>, f64)> {
    let mut candidates = Vec::new();
    for (key_index, key) in keys.iter().enumerate() {
        candidates.extend(
            index
                .ordered
                .iter()
                .filter(|entry| entry.key.as_slice() == *key && entry.score.0 <= max_score)
                .map(|entry| (key_index, entry)),
        );
    }

    candidates.sort_unstable_by(|(left_index, left), (right_index, right)| {
        Score(left.score.0)
            .cmp(&Score(right.score.0))
            .then_with(|| left.member.cmp(&right.member))
            .then_with(|| left.key.cmp(&right.key))
            .then_with(|| left_index.cmp(right_index))
    });

    candidates
        .into_iter()
        .take(limit.min(max_scan))
        .map(|(_key_index, entry)| (entry.key.clone(), entry.member.clone(), entry.score.0))
        .collect()
}

#[test]
#[ignore = "manual release-mode claim benchmark"]
fn bench_claim_due_candidates_core_methods() {
    let scenarios = [
        ("single-short", 1, 1, 1_024, 24, 256, 512, 1_000),
        ("dense-limit1", 8, 8, 1_024, 24, 1, 1, 5_000),
        ("dense-short", 8, 8, 1_024, 24, 256, 512, 1_000),
        ("dense-long", 8, 8, 1_024, 256, 256, 512, 500),
        ("sparse", 32, 2, 1_024, 64, 256, 512, 1_000),
        ("empty", 256, 0, 0, 64, 256, 512, 10_000),
        ("all-missing", 257, 1, 1_024, 64, 256, 512, 1_000),
        ("all-missing-limit1", 257, 1, 1_024, 64, 1, 1, 5_000),
    ];

    for (
        name,
        requested_key_count,
        populated_key_count,
        entries_per_key,
        key_length,
        limit,
        max_scan,
        iterations,
    ) in scenarios
    {
        let (index, keys) = benchmark_claim_index(
            requested_key_count,
            populated_key_count,
            entries_per_key,
            key_length,
        );
        let key_refs = if name.starts_with("all-missing") {
            keys.iter().skip(1).map(Vec::as_slice).collect::<Vec<_>>()
        } else {
            keys.iter().map(Vec::as_slice).collect::<Vec<_>>()
        };

        let baseline_rows =
            baseline_claim_due_candidates(&index, &key_refs, f64::INFINITY, limit, max_scan);
        assert_eq!(
            index.claim_due_candidates(&key_refs, f64::INFINITY, limit, max_scan),
            baseline_rows,
            "benchmark scenario={name} changed result"
        );

        for _ in 0..10 {
            std::hint::black_box(baseline_claim_due_candidates(
                &index,
                &key_refs,
                f64::INFINITY,
                limit,
                max_scan,
            ));
            std::hint::black_box(index.claim_due_candidates(
                &key_refs,
                f64::INFINITY,
                limit,
                max_scan,
            ));
        }

        let baseline_started = std::time::Instant::now();
        let mut checksum = 0usize;
        for _ in 0..iterations {
            let rows = std::hint::black_box(baseline_claim_due_candidates(
                &index,
                &key_refs,
                f64::INFINITY,
                limit,
                max_scan,
            ));
            checksum = checksum.wrapping_add(rows.len());
        }
        let baseline_elapsed_ns = baseline_started.elapsed().as_nanos();

        let candidate_started = std::time::Instant::now();
        let mut candidate_checksum = 0usize;
        for _ in 0..iterations {
            let rows = std::hint::black_box(index.claim_due_candidates(
                &key_refs,
                f64::INFINITY,
                limit,
                max_scan,
            ));
            candidate_checksum = candidate_checksum.wrapping_add(rows.len());
        }
        let candidate_elapsed_ns = candidate_started.elapsed().as_nanos();

        println!(
            "BENCH claim_due_candidates scenario={name} iterations={iterations} baseline_ns={baseline_elapsed_ns} candidate_ns={candidate_elapsed_ns} checksums={checksum}/{candidate_checksum}"
        );
    }
}

fn benchmark_claim_index(
    requested_key_count: usize,
    populated_key_count: usize,
    entries_per_key: usize,
    key_length: usize,
) -> (FlowOrderedIndex, Vec<Vec<u8>>) {
    let mut index = FlowOrderedIndex::default();
    let mut keys = Vec::with_capacity(requested_key_count);

    for key_index in 0..requested_key_count {
        let mut key = format!("due:{key_index}:").into_bytes();
        key.resize(key_length, b'k');
        keys.push(key);
    }

    for (key_index, key) in keys
        .iter()
        .enumerate()
        .take(populated_key_count.min(requested_key_count))
    {
        for entry_index in 0..entries_per_key {
            let member = format!("member:{entry_index:08}");
            let score = (entry_index * requested_key_count + key_index) as f64;
            index.put(key, member.as_bytes(), score, false);
        }
    }

    (index, keys)
}

#[derive(Debug, Eq, PartialEq)]
struct BaselineDueCandidate {
    key_index: usize,
    entry: OrderedEntry,
}

impl Ord for BaselineDueCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .entry
            .score
            .cmp(&self.entry.score)
            .then_with(|| other.entry.member.cmp(&self.entry.member))
            .then_with(|| other.entry.key.cmp(&self.entry.key))
            .then_with(|| other.key_index.cmp(&self.key_index))
    }
}

impl PartialOrd for BaselineDueCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn baseline_claim_due_candidates(
    index: &FlowOrderedIndex,
    keys: &[&[u8]],
    max_score: f64,
    limit: usize,
    max_scan: usize,
) -> Vec<(Vec<u8>, Vec<u8>, f64)> {
    if keys.is_empty() || limit == 0 || max_scan == 0 {
        return Vec::new();
    }

    if keys.len() == 1 {
        let mut rows = Vec::with_capacity(limit.min(max_scan).min(index.ordered.len()));
        let lower = OrderedEntry {
            key: keys[0].to_vec(),
            score: Score(f64::NEG_INFINITY),
            member: Vec::new(),
        };

        let mut scanned = 0;
        for entry in index.ordered.range(lower..) {
            if entry.key.as_slice() != keys[0] {
                break;
            }
            if entry.score.0 > max_score {
                break;
            }

            scanned += 1;
            rows.push((entry.key.clone(), entry.member.clone(), entry.score.0));
            if rows.len() >= limit || scanned >= max_scan {
                return rows;
            }
        }
        return rows;
    }

    let mut rows = Vec::with_capacity(limit.min(max_scan).min(index.ordered.len()));
    let mut scanned = 0usize;
    let mut heap = BinaryHeap::with_capacity(keys.len().min(index.ordered.len()));

    for (key_index, key) in keys.iter().enumerate() {
        if let Some(entry) = baseline_first_due_entry_for_key(index, key, max_score) {
            heap.push(BaselineDueCandidate { key_index, entry });
        }
    }

    while rows.len() < limit && scanned < max_scan {
        let Some(candidate) = heap.pop() else {
            break;
        };

        scanned += 1;
        rows.push((
            candidate.entry.key.clone(),
            candidate.entry.member.clone(),
            candidate.entry.score.0,
        ));

        if let Some(next) = baseline_next_due_entry_for_key(
            index,
            keys[candidate.key_index],
            max_score,
            &candidate.entry,
        ) {
            heap.push(BaselineDueCandidate {
                key_index: candidate.key_index,
                entry: next,
            });
        }
    }

    rows
}

fn baseline_first_due_entry_for_key(
    index: &FlowOrderedIndex,
    key: &[u8],
    max_score: f64,
) -> Option<OrderedEntry> {
    let lower = OrderedEntry {
        key: key.to_vec(),
        score: Score(f64::NEG_INFINITY),
        member: Vec::new(),
    };

    for entry in index.ordered.range(lower..) {
        match due_entry_match(entry, key, max_score) {
            DueEntryMatch::Match => return Some(entry.clone()),
            DueEntryMatch::Continue => continue,
            DueEntryMatch::Stop => return None,
        }
    }
    None
}

fn baseline_next_due_entry_for_key(
    index: &FlowOrderedIndex,
    key: &[u8],
    max_score: f64,
    previous: &OrderedEntry,
) -> Option<OrderedEntry> {
    for entry in index.ordered.range((Excluded(previous.clone()), Unbounded)) {
        match due_entry_match(entry, key, max_score) {
            DueEntryMatch::Match => return Some(entry.clone()),
            DueEntryMatch::Continue => continue,
            DueEntryMatch::Stop => return None,
        }
    }
    None
}
