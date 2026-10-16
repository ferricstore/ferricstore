use std::collections::BinaryHeap;

/// A borrowed LMDB row together with the source that owns it.
///
/// The tuple order is part of the prefix-merge contract: keys sort first,
/// duplicate keys sort by source, and a duplicate from the same source sorts
/// by value. Keeping that order in one named helper prevents the scan loop
/// from drifting from the final heap ordering.
pub type Candidate<'a> = (&'a [u8], usize, &'a [u8]);

/// Add one source's sorted rows to a bounded global selection heap.
///
/// Rows are borrowed from the source transaction. When the heap is full, a
/// row whose key is strictly greater than the current largest selected key
/// proves that all later rows from this sorted source lose as well. Equal keys
/// continue through the full tuple comparator so source/value tie-breaking
/// remains exact.
#[inline]
pub fn scan_source<'a, I, E>(
    selected: &mut BinaryHeap<Candidate<'a>>,
    source: usize,
    cap: usize,
    items: I,
) -> Result<usize, E>
where
    I: IntoIterator<Item = Result<(&'a [u8], &'a [u8]), E>>,
{
    if cap == 0 {
        return Ok(0);
    }

    let mut scanned = 0usize;
    for item in items.into_iter().take(cap) {
        let (key, value) = item?;
        scanned = scanned.saturating_add(1);
        let candidate = (key, source, value);

        if selected.len() < cap {
            selected.push(candidate);
            continue;
        }

        let largest = *selected
            .peek()
            .expect("a full positive-capacity selection heap is non-empty");
        if key > largest.0 {
            break;
        }
        if candidate < largest {
            selected.pop();
            selected.push(candidate);
        }
    }

    Ok(scanned)
}

/// Check the exact selected rows against the caller's key-plus-value byte cap.
pub fn within_byte_cap(selected: &[Candidate<'_>], byte_cap: usize) -> bool {
    selected
        .iter()
        .try_fold(0usize, |bytes, (key, _, value)| {
            bytes
                .checked_add(key.len())
                .and_then(|bytes| bytes.checked_add(value.len()))
        })
        .is_some_and(|bytes| bytes <= byte_cap)
}

#[cfg(test)]
mod tests {
    use super::{scan_source, within_byte_cap, Candidate};
    use std::collections::BinaryHeap;

    fn sorted<'a>(heap: BinaryHeap<Candidate<'a>>) -> Vec<(Vec<u8>, usize, Vec<u8>)> {
        heap.into_sorted_vec()
            .into_iter()
            .map(|(key, source, value)| (key.to_vec(), source, value.to_vec()))
            .collect()
    }

    fn rows(
        entries: &[(&'static [u8], &'static [u8])],
    ) -> Vec<Result<(&'static [u8], &'static [u8]), ()>> {
        entries.iter().copied().map(Ok).collect()
    }

    #[test]
    fn preserves_key_source_and_value_tie_order() {
        let mut selected = BinaryHeap::new();
        let source_zero: Vec<Result<(&'static [u8], &'static [u8]), ()>> = vec![
            Ok((b"merge:a".as_slice(), b"a-0".as_slice())),
            Ok((b"merge:c".as_slice(), b"c-0".as_slice())),
            Ok((b"merge:e".as_slice(), b"e-0".as_slice())),
        ];
        let source_one: Vec<Result<(&'static [u8], &'static [u8]), ()>> = vec![
            Ok((b"merge:a".as_slice(), b"a-1".as_slice())),
            Ok((b"merge:b".as_slice(), b"b-1".as_slice())),
            Ok((b"merge:d".as_slice(), b"d-1".as_slice())),
        ];

        assert_eq!(scan_source(&mut selected, 0, 4, source_zero).unwrap(), 3);
        assert_eq!(scan_source(&mut selected, 1, 4, source_one).unwrap(), 3);
        assert_eq!(
            sorted(selected),
            vec![
                (b"merge:a".to_vec(), 0, b"a-0".to_vec()),
                (b"merge:a".to_vec(), 1, b"a-1".to_vec()),
                (b"merge:b".to_vec(), 1, b"b-1".to_vec()),
                (b"merge:c".to_vec(), 0, b"c-0".to_vec()),
            ]
        );
    }

    #[test]
    fn examines_equal_key_duplicates_after_the_heap_fills() {
        let mut selected = BinaryHeap::new();
        let initial: Vec<Result<(&'static [u8], &'static [u8]), ()>> = vec![
            Ok((b"merge:k".as_slice(), b"z".as_slice())),
            Ok((b"merge:m".as_slice(), b"m".as_slice())),
        ];
        let rows: Vec<Result<(&'static [u8], &'static [u8]), ()>> = vec![
            Ok((b"merge:k".as_slice(), b"z".as_slice())),
            Ok((b"merge:k".as_slice(), b"a".as_slice())),
        ];

        assert_eq!(scan_source(&mut selected, 0, 2, initial).unwrap(), 2);
        assert_eq!(scan_source(&mut selected, 1, 2, rows).unwrap(), 2);
        assert_eq!(
            sorted(selected),
            vec![
                (b"merge:k".to_vec(), 0, b"z".to_vec()),
                (b"merge:k".to_vec(), 1, b"a".to_vec())
            ]
        );
    }

    #[test]
    fn stops_after_a_key_larger_than_the_heap_threshold() {
        let mut selected = BinaryHeap::new();
        let first: Vec<Result<(&'static [u8], &'static [u8]), ()>> = vec![
            Ok((b"merge:a".as_slice(), b"a".as_slice())),
            Ok((b"merge:b".as_slice(), b"b".as_slice())),
        ];
        let losing_source: Vec<Result<(&'static [u8], &'static [u8]), ()>> = vec![
            Ok((b"merge:c".as_slice(), b"c".as_slice())),
            Ok((b"merge:d".as_slice(), b"d".as_slice())),
        ];

        assert_eq!(scan_source(&mut selected, 0, 2, first).unwrap(), 2);
        assert_eq!(scan_source(&mut selected, 1, 2, losing_source).unwrap(), 1);
        assert_eq!(
            sorted(selected),
            vec![
                (b"merge:a".to_vec(), 0, b"a".to_vec()),
                (b"merge:b".to_vec(), 0, b"b".to_vec())
            ]
        );
    }

    #[test]
    fn stops_when_a_later_key_exceeds_the_heap_threshold() {
        let mut selected = BinaryHeap::new();
        let initial = rows(&[
            (b"merge:a".as_slice(), b"a".as_slice()),
            (b"merge:m".as_slice(), b"m".as_slice()),
            (b"merge:n".as_slice(), b"n".as_slice()),
        ]);
        let source = rows(&[
            (b"merge:b".as_slice(), b"b".as_slice()),
            (b"merge:z".as_slice(), b"z".as_slice()),
            (b"merge:zz".as_slice(), b"zz".as_slice()),
        ]);

        assert_eq!(scan_source(&mut selected, 0, 3, initial).unwrap(), 3);
        assert_eq!(scan_source(&mut selected, 1, 3, source).unwrap(), 2);
        assert_eq!(
            sorted(selected),
            vec![
                (b"merge:a".to_vec(), 0, b"a".to_vec()),
                (b"merge:b".to_vec(), 1, b"b".to_vec()),
                (b"merge:m".to_vec(), 0, b"m".to_vec())
            ]
        );
    }

    #[test]
    fn caps_each_source_when_no_early_stop_is_proven() {
        let mut selected = BinaryHeap::new();
        let rows: Vec<Result<(&'static [u8], &'static [u8]), ()>> = vec![
            Ok((b"merge:a".as_slice(), b"a".as_slice())),
            Ok((b"merge:b".as_slice(), b"b".as_slice())),
            Ok((b"merge:c".as_slice(), b"c".as_slice())),
        ];

        assert_eq!(scan_source(&mut selected, 0, 2, rows).unwrap(), 2);
    }

    #[test]
    fn source_order_permutations_match_the_flattened_reference() {
        let sources = [
            [
                (b"merge:a".as_slice(), b"a-0".as_slice()),
                (b"merge:d".as_slice(), b"d-0".as_slice()),
                (b"merge:h".as_slice(), b"h-0".as_slice()),
            ],
            [
                (b"merge:a".as_slice(), b"a-1".as_slice()),
                (b"merge:c".as_slice(), b"c-1".as_slice()),
                (b"merge:g".as_slice(), b"g-1".as_slice()),
            ],
            [
                (b"merge:b".as_slice(), b"b-2".as_slice()),
                (b"merge:e".as_slice(), b"e-2".as_slice()),
                (b"merge:f".as_slice(), b"f-2".as_slice()),
            ],
        ];
        let expected = vec![
            (b"merge:a".to_vec(), 0, b"a-0".to_vec()),
            (b"merge:a".to_vec(), 1, b"a-1".to_vec()),
            (b"merge:b".to_vec(), 2, b"b-2".to_vec()),
            (b"merge:c".to_vec(), 1, b"c-1".to_vec()),
        ];

        for order in [[0, 1, 2], [2, 0, 1], [1, 2, 0], [2, 1, 0]] {
            let mut selected = BinaryHeap::new();
            for source in order {
                let scanned =
                    scan_source(&mut selected, source, 4, rows(&sources[source])).unwrap();
                assert!(scanned <= 3);
            }
            assert_eq!(sorted(selected), expected);
        }
    }

    #[test]
    fn randomized_source_orders_match_the_flattened_reference() {
        let sources = [
            vec![
                (b"merge:00".to_vec(), b"a-0".to_vec()),
                (b"merge:01".to_vec(), b"b-0".to_vec()),
                (b"merge:01".to_vec(), b"c-0".to_vec()),
                (b"merge:04".to_vec(), b"d-0".to_vec()),
                (b"merge:09".to_vec(), b"e-0".to_vec()),
            ],
            vec![
                (b"merge:00".to_vec(), b"a-1".to_vec()),
                (b"merge:02".to_vec(), b"b-1".to_vec()),
                (b"merge:03".to_vec(), b"c-1".to_vec()),
                (b"merge:04".to_vec(), b"d-1".to_vec()),
                (b"merge:08".to_vec(), b"e-1".to_vec()),
            ],
            vec![
                (b"merge:01".to_vec(), b"a-2".to_vec()),
                (b"merge:02".to_vec(), b"b-2".to_vec()),
                (b"merge:05".to_vec(), b"c-2".to_vec()),
                (b"merge:06".to_vec(), b"d-2".to_vec()),
                (b"merge:10".to_vec(), b"e-2".to_vec()),
            ],
            Vec::new(),
        ];
        let mut seed = 0xfeed_face_cafe_beefu64;

        for _ in 0..64 {
            let cap = (next_random(&mut seed) % 6) as usize;
            let mut order = [0usize, 1, 2, 3];
            for index in (1..order.len()).rev() {
                let swap_with = next_random(&mut seed) as usize % (index + 1);
                order.swap(index, swap_with);
            }

            let mut selected = BinaryHeap::new();
            for source in order {
                let borrowed = sources[source]
                    .iter()
                    .map(|(key, value)| Ok::<_, ()>((key.as_slice(), value.as_slice())));
                scan_source(&mut selected, source, cap, borrowed).unwrap();
            }

            let mut expected = sources
                .iter()
                .enumerate()
                .flat_map(|(source, rows)| {
                    rows.iter()
                        .take(cap)
                        .map(move |(key, value)| (key.clone(), source, value.clone()))
                })
                .collect::<Vec<_>>();
            expected.sort();
            expected.truncate(cap);
            assert_eq!(sorted(selected), expected);
        }
    }

    #[test]
    fn zero_capacity_does_not_evaluate_or_retain_source_rows() {
        let mut selected = BinaryHeap::new();
        let rows: Vec<Result<(&'static [u8], &'static [u8]), &str>> =
            vec![Err("must not be evaluated")];

        assert_eq!(scan_source(&mut selected, 0, 0, rows).unwrap(), 0);
        assert!(selected.is_empty());
    }

    #[test]
    fn empty_sources_and_capacity_one_keep_duplicate_source_order() {
        let mut selected = BinaryHeap::new();
        let empty: Vec<Result<(&'static [u8], &'static [u8]), ()>> = Vec::new();
        assert_eq!(scan_source(&mut selected, 0, 1, empty).unwrap(), 0);

        let source_zero = rows(&[(b"merge:k".as_slice(), b"z".as_slice())]);
        let source_one = rows(&[(b"merge:k".as_slice(), b"a".as_slice())]);
        assert_eq!(scan_source(&mut selected, 0, 1, source_zero).unwrap(), 1);
        assert_eq!(scan_source(&mut selected, 1, 1, source_one).unwrap(), 1);
        assert_eq!(
            sorted(selected),
            vec![(b"merge:k".to_vec(), 0, b"z".to_vec())]
        );
    }

    #[test]
    fn propagates_source_errors_before_the_cutoff() {
        let mut selected = BinaryHeap::new();
        let rows: Vec<Result<(&'static [u8], &'static [u8]), &str>> = vec![
            Ok((b"merge:a".as_slice(), b"a".as_slice())),
            Err("source failure"),
        ];

        assert_eq!(
            scan_source(&mut selected, 0, 2, rows),
            Err("source failure")
        );
    }

    #[test]
    fn ignores_errors_after_an_early_cutoff() {
        let mut selected = BinaryHeap::new();
        let initial = rows(&[
            (b"merge:a".as_slice(), b"a".as_slice()),
            (b"merge:b".as_slice(), b"b".as_slice()),
        ]);
        let rows: Vec<Result<(&'static [u8], &'static [u8]), &str>> = vec![
            Ok((b"merge:c".as_slice(), b"c".as_slice())),
            Err("must not be evaluated"),
        ];

        assert_eq!(scan_source(&mut selected, 0, 2, initial).unwrap(), 2);
        assert_eq!(scan_source(&mut selected, 1, 2, rows).unwrap(), 1);
    }

    #[test]
    fn byte_cap_accepts_exact_sum_and_rejects_overflow() {
        let selected = [
            (b"merge:a".as_slice(), 0, b"one".as_slice()),
            (b"merge:b".as_slice(), 1, b"two".as_slice()),
        ];
        let exact = selected
            .iter()
            .map(|(key, _, value)| key.len() + value.len())
            .sum();

        assert!(within_byte_cap(&selected, exact));
        assert!(!within_byte_cap(&selected, exact - 1));
    }

    fn next_random(seed: &mut u64) -> u64 {
        *seed = (*seed)
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        *seed
    }
}
