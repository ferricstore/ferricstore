use super::*;

#[test]
fn cms_fnv1a_deterministic() {
    let h1 = fnv1a(b"hello", 0x811c_9dc5);
    let h2 = fnv1a(b"hello", 0x811c_9dc5);
    assert_eq!(h1, h2);

    let h3 = fnv1a(b"world", 0x811c_9dc5);
    assert_ne!(h1, h3);
}

// -----------------------------------------------------------------------
// Edge case tests
// -----------------------------------------------------------------------

/// Helper: create a valid TopK file and return the path string.
fn create_topk_file(
    dir: &std::path::Path,
    name: &str,
    k: u32,
    width: u32,
    depth: u32,
) -> String {
    let path = dir.join(name);
    let file_size = TOPK_HEADER_SIZE
        + (width as usize * depth as usize * 8)
        + (k as usize * HEAP_ENTRY_SIZE)
        + crate::prob_txn::TOKEN_SIZE;

    let mut file = File::create(&path).unwrap();
    let mut header = [0u8; TOPK_HEADER_SIZE];
    header[0..8].copy_from_slice(&TOPK_MAGIC);
    header[8..12].copy_from_slice(&k.to_le_bytes());
    header[12..16].copy_from_slice(&width.to_le_bytes());
    header[16..20].copy_from_slice(&depth.to_le_bytes());
    // heap_len=0 and reserved=0 (already zeroed)

    file.write_all(&header).unwrap();
    let zeros = vec![0u8; file_size - TOPK_HEADER_SIZE];
    file.write_all(&zeros).unwrap();
    file.sync_all().unwrap();
    path.to_str().unwrap().to_string()
}

#[test]
fn empty_element_fnv1a() {
    let h = fnv1a(b"", 0x811c_9dc5);
    // Should be the offset basis XOR'd with nothing = offset basis
    assert_eq!(h, 0x811c_9dc5);
}

#[test]
fn large_element_fnv1a() {
    let big = vec![0xAAu8; 1_000_000];
    let h = fnv1a(&big, 0x811c_9dc5);
    // Just verify it doesn't panic and returns a non-zero value.
    assert_ne!(h, 0);
}

#[test]
fn truncated_header_returns_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("truncated.topk");
    std::fs::write(&path, [0u8; 32]).unwrap();
    let file = File::open(&path).unwrap();
    assert!(v2_read_header(&file).is_err());
}

#[test]
fn complete_header_rejects_truncated_cms_and_heap_regions() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("missing_regions.topk");
    let mut header = [0u8; TOPK_HEADER_SIZE];
    header[0..8].copy_from_slice(&TOPK_MAGIC);
    header[8..12].copy_from_slice(&1u32.to_le_bytes());
    header[12..16].copy_from_slice(&1u32.to_le_bytes());
    header[16..20].copy_from_slice(&1u32.to_le_bytes());
    std::fs::write(&path, header).unwrap();

    assert!(v2_read_header(&File::open(path).unwrap()).is_err());
}

#[test]
fn read_cms_rejects_truncated_counter_region() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("short_cms.topk");
    std::fs::write(&path, [0u8; TOPK_HEADER_SIZE]).unwrap();
    let file = File::open(&path).unwrap();

    let err = v2_read_cms(&file, 8, 7).unwrap_err();

    assert!(err.contains("truncated"));
}

#[test]
fn wrong_magic_returns_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bad_magic.topk");
    let mut data = [0u8; TOPK_HEADER_SIZE + 64];
    data[0..8].copy_from_slice(&0xDEAD_BEEF_u64.to_le_bytes());
    std::fs::write(&path, data).unwrap();
    let file = File::open(&path).unwrap();
    let result = v2_read_header(&file);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("magic"));
}

#[test]
fn invalid_header_dimensions_return_error() {
    for (name, k, width, depth) in [
        ("zero_k.topk", 0u32, 8u32, 3u32),
        ("zero_width.topk", 5u32, 0u32, 3u32),
        ("zero_depth.topk", 5u32, 8u32, 0u32),
        ("oversized_cms.topk", 5u32, u32::MAX, u32::MAX),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(name);
        let mut data = [0u8; TOPK_HEADER_SIZE];
        data[0..8].copy_from_slice(&TOPK_MAGIC);
        data[8..12].copy_from_slice(&k.to_le_bytes());
        data[12..16].copy_from_slice(&width.to_le_bytes());
        data[16..20].copy_from_slice(&depth.to_le_bytes());
        std::fs::write(&path, data).unwrap();
        let file = File::open(&path).unwrap();

        let result = v2_read_header(&file);

        assert!(result.is_err(), "{name} should be rejected");
    }
}

#[test]
fn topk_header_rejects_old_decay_bearing_format() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("old-decay.topk");
    let mut data = vec![0u8; TOPK_HEADER_SIZE + 8 + HEAP_ENTRY_SIZE];
    data[0..8].copy_from_slice(&0x544F_504B_4D4D_5031u64.to_le_bytes());
    data[8..12].copy_from_slice(&1u32.to_le_bytes());
    data[12..16].copy_from_slice(&1u32.to_le_bytes());
    data[16..20].copy_from_slice(&1u32.to_le_bytes());
    data[20..28].copy_from_slice(&0.0f64.to_le_bytes());
    std::fs::write(&path, data).unwrap();

    let error = v2_read_header(&File::open(path).unwrap()).unwrap_err();

    assert!(error.contains("magic"));
}

#[test]
fn topk_header_rejects_nonzero_reserved_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("reserved.topk");
    let mut data = vec![0u8; TOPK_HEADER_SIZE + 8 + HEAP_ENTRY_SIZE];
    data[0..8].copy_from_slice(&TOPK_MAGIC);
    data[8..12].copy_from_slice(&1u32.to_le_bytes());
    data[12..16].copy_from_slice(&1u32.to_le_bytes());
    data[16..20].copy_from_slice(&1u32.to_le_bytes());
    data[24] = 1;
    std::fs::write(&path, data).unwrap();

    let error = v2_read_header(&File::open(path).unwrap()).unwrap_err();

    assert!(error.contains("reserved"));
}

#[test]
fn valid_header_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let path_str = create_topk_file(dir.path(), "header.topk", 5, 8, 7);
    let file = File::open(&path_str).unwrap();
    let (k, width, depth, heap_len) = v2_read_header(&file).unwrap();
    assert_eq!(k, 5);
    assert_eq!(width, 8);
    assert_eq!(depth, 7);
    assert_eq!(heap_len, 0);
}

#[test]
fn heap_reader_rejects_duplicate_elements() {
    let dir = tempfile::tempdir().unwrap();
    let path = create_topk_file(dir.path(), "duplicates.topk", 2, 1, 1);
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();

    crate::write_all_at(&file, &2u32.to_le_bytes(), 20, "heap length").unwrap();
    let mut heap = vec![0u8; 2 * HEAP_ENTRY_SIZE];
    for slot in 0..2 {
        let base = slot * HEAP_ENTRY_SIZE;
        heap[base..base + 8].copy_from_slice(&1i64.to_le_bytes());
        heap[base + 8..base + 12].copy_from_slice(&3u32.to_le_bytes());
        heap[base + 12..base + 15].copy_from_slice(b"dup");
    }
    crate::write_all_at(
        &file,
        &heap,
        heap_offset(1, 1) as u64,
        "duplicate heap",
    )
    .unwrap();

    let error = match v2_read_heap(&file, 1, 1, 2, 2) {
        Ok(_) => panic!("duplicate heap elements were accepted"),
        Err(error) => error,
    };
    assert!(error.contains("duplicate"));
}

#[test]
fn cms_increment_and_estimate() {
    let width = 10;
    let depth = 3;
    let mut counters = vec![0i64; width * depth];

    // Increment "apple" by 5
    let est = v2_cms_increment(&mut counters, width, depth, b"apple", 5).unwrap();
    assert_eq!(est, 5);

    // Increment "apple" by 3 more
    let est2 = v2_cms_increment(&mut counters, width, depth, b"apple", 3).unwrap();
    assert_eq!(est2, 8);

    // Estimate should match
    let est3 = v2_cms_estimate(&counters, width, depth, b"apple");
    assert_eq!(est3, 8);

    // Unseen element should be 0
    let est4 = v2_cms_estimate(&counters, width, depth, b"banana");
    assert_eq!(est4, 0);
}

#[test]
fn cms_increment_rejects_overflow_without_mutating() {
    let width = 1;
    let depth = 3;
    let mut counters = vec![i64::MAX; width * depth];
    let before = counters.clone();

    let err = v2_cms_increment(&mut counters, width, depth, b"apple", 1).unwrap_err();

    assert!(err.contains("overflow"));
    assert_eq!(counters, before);
}

#[test]
fn heap_add_and_eviction() {
    let k = 3;
    let mut heap = V2IndexedHeap::new(Vec::new(), k, 4).unwrap();

    // Add 3 elements (heap has room, no eviction)
    assert_eq!(heap.add(b"a", 10), None);
    assert_eq!(heap.add(b"b", 20), None);
    assert_eq!(heap.add(b"c", 30), None);
    assert_eq!(heap.entries.len(), 3);

    // Add a 4th element with higher count => evicts "a" (count=10, the min)
    let evicted = heap.add(b"d", 40);
    assert_eq!(evicted, Some(b"a".to_vec()));
    assert_eq!(heap.entries.len(), 3);
    assert!(!heap.positions.contains_key(b"a".as_slice()));
    assert!(heap.positions.contains_key(b"d".as_slice()));
}

#[test]
fn heap_add_no_eviction_when_new_is_too_small() {
    let k = 2;
    let mut heap = V2IndexedHeap::new(Vec::new(), k, 3).unwrap();

    heap.add(b"a", 100);
    heap.add(b"b", 200);

    // New element with count=50 is less than min(100), no eviction
    let evicted = heap.add(b"c", 50);
    assert_eq!(evicted, None);
    assert_eq!(heap.entries.len(), 2);
    assert!(!heap.positions.contains_key(b"c".as_slice()));
}

#[test]
fn heap_add_update_existing() {
    let k = 3;
    let mut heap = V2IndexedHeap::new(Vec::new(), k, 2).unwrap();

    heap.add(b"a", 10);
    heap.add(b"b", 20);

    // Update "a" with new count
    let evicted = heap.add(b"a", 50);
    assert_eq!(evicted, None); // no eviction, just update
    assert_eq!(heap.entries.len(), 2);

    // Find "a" and verify count was updated
    let a_entry = heap
        .entries
        .iter()
        .find(|e| e.element == b"a")
        .unwrap();
    assert_eq!(a_entry.count, 50);
}

#[test]
fn heap_add_keeps_the_minimum_at_the_root() {
    let k = 4;
    let mut heap = V2IndexedHeap::new(Vec::new(), k, 3).unwrap();

    heap.add(b"high", 50);
    heap.add(b"low", 10);
    heap.add(b"middle", 30);

    assert_eq!(heap.entries[0].element, b"low");

    heap.add(b"low", 60);

    assert_eq!(heap.entries[0].element, b"middle");
    assert_eq!(heap.entries[0].count, 30);
}

#[test]
fn indexed_heap_rebuilds_unsorted_entries_and_keeps_positions_consistent() {
    let entries = vec![
        V2HeapEntry {
            element: b"high".to_vec(),
            count: 90,
        },
        V2HeapEntry {
            element: b"low".to_vec(),
            count: 10,
        },
        V2HeapEntry {
            element: b"middle".to_vec(),
            count: 50,
        },
    ];
    let mut heap = V2IndexedHeap::new(entries, 3, 2).unwrap();

    assert_eq!(heap.entries[0].element, b"low");
    heap.add(b"low", 100);
    assert_eq!(heap.entries[0].element, b"middle");
    assert_eq!(heap.add(b"new", 75), Some(b"middle".to_vec()));

    for (index, entry) in heap.entries.iter().enumerate() {
        assert_eq!(heap.positions.get(entry.element.as_slice()), Some(&index));
        let left = index * 2 + 1;
        let right = left + 1;
        if left < heap.entries.len() {
            assert!(!V2IndexedHeap::entry_precedes(
                &heap.entries[left],
                entry
            ));
        }
        if right < heap.entries.len() {
            assert!(!V2IndexedHeap::entry_precedes(
                &heap.entries[right],
                entry
            ));
        }
    }
}

#[test]
fn heap_read_write_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let path_str = create_topk_file(dir.path(), "heap_rw.topk", 5, 8, 3);
    let file = crate::open_random_rw(std::path::Path::new(&path_str)).unwrap();
    let (_, width, depth, _) = v2_read_header(&file).unwrap();

    // Write some heap entries
    let entries = vec![
        V2HeapEntry {
            element: b"alpha".to_vec(),
            count: 100,
        },
        V2HeapEntry {
            element: b"beta".to_vec(),
            count: 50,
        },
        V2HeapEntry {
            element: b"gamma".to_vec(),
            count: 25,
        },
    ];
    v2_write_heap(&file, width, depth, &entries).unwrap();

    // Read them back
    let read_entries = v2_read_heap(&file, width, depth, 3, 5).unwrap();
    assert_eq!(read_entries.len(), 3);
    assert_eq!(read_entries[0].element, b"alpha");
    assert_eq!(read_entries[0].count, 100);
    assert_eq!(read_entries[1].element, b"beta");
    assert_eq!(read_entries[1].count, 50);
    assert_eq!(read_entries[2].element, b"gamma");
    assert_eq!(read_entries[2].count, 25);
}

#[test]
fn empty_heap_read() {
    let dir = tempfile::tempdir().unwrap();
    let path_str = create_topk_file(dir.path(), "empty_heap.topk", 5, 8, 3);
    let file = crate::open_random_read(std::path::Path::new(&path_str)).unwrap();
    let (k, width, depth, heap_len) = v2_read_header(&file).unwrap();
    assert_eq!(heap_len, 0);

    let entries = v2_read_heap(&file, width, depth, heap_len, k).unwrap();
    assert_eq!(entries.len(), 0);
}

#[test]
fn query_fingerprints_borrow_heap_element_storage() {
    let entries = vec![
        V2HeapEntry {
            element: b"alpha".to_vec(),
            count: 2,
        },
        V2HeapEntry {
            element: b"beta".to_vec(),
            count: 1,
        },
    ];

    let fingerprints = v2_query_fingerprints(&entries);
    let alpha = fingerprints.get(b"alpha".as_slice()).unwrap();

    assert_eq!(alpha.as_ptr(), entries[0].element.as_ptr());
    assert!(fingerprints.contains(b"beta".as_slice()));
}

#[test]
fn cms_write_read_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let path_str = create_topk_file(dir.path(), "cms_rw.topk", 3, 10, 5);
    let file = crate::open_random_rw(std::path::Path::new(&path_str)).unwrap();
    let (_, width, depth, _) = v2_read_header(&file).unwrap();

    let mut counters = vec![0i64; width * depth];
    v2_cms_increment(&mut counters, width, depth, b"test", 42).unwrap();

    v2_write_cms(&file, &counters).unwrap();

    let read_counters = v2_read_cms(&file, width, depth).unwrap();
    assert_eq!(counters, read_counters);

    let est = v2_cms_estimate(&read_counters, width, depth, b"test");
    assert_eq!(est, 42);
}

#[test]
fn nonexistent_file_returns_not_found() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("nope.topk");
    let result = crate::open_random_read(&path);
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::NotFound);
}

#[test]
fn element_over_max_length_is_rejected() {
    let long_element = "x".repeat(300);
    let dir = tempfile::tempdir().unwrap();
    let path_str = create_topk_file(dir.path(), "long.topk", 3, 8, 3);
    let file = crate::open_random_rw(std::path::Path::new(&path_str)).unwrap();
    let (_, width, depth, _) = v2_read_header(&file).unwrap();

    let entries = vec![V2HeapEntry {
        element: long_element.into_bytes(),
        count: 10,
    }];
    let error = v2_write_heap(&file, width, depth, &entries).unwrap_err();
    assert!(error.contains("element length"));
}

// -----------------------------------------------------------------------
// End-to-end file I/O tests (heap + CMS coordination)
// -----------------------------------------------------------------------

/// Helper: add elements to a TopK file using the same logic as the NIF,
/// returning evicted element names (None = no eviction).
fn topk_add_elements(path: &str, elements: &[&str]) -> Vec<Option<String>> {
    let file = crate::open_random_rw(std::path::Path::new(path)).unwrap();
    let (k, width, depth, heap_len) = v2_read_header(&file).unwrap();
    let mut counters = v2_read_cms(&file, width, depth).unwrap();
    let heap_entries = v2_read_heap(&file, width, depth, heap_len, k).unwrap();
    let mut heap = V2IndexedHeap::new(heap_entries, k, elements.len()).unwrap();

    let mut results = Vec::new();
    for &elem in elements {
        let estimated = v2_cms_increment(&mut counters, width, depth, elem.as_bytes(), 1).unwrap();
        let evicted = heap.add(elem.as_bytes(), estimated);
        results.push(evicted.map(|value| String::from_utf8(value).unwrap()));
    }

    v2_write_cms(&file, &counters).unwrap();
    v2_write_heap(&file, width, depth, &heap.entries).unwrap();
    results
}

/// Helper: incrby a single element, return evicted name if any.
fn topk_incrby_one(path: &str, element: &str, count: i64) -> Option<String> {
    let file = crate::open_random_rw(std::path::Path::new(path)).unwrap();
    let (k, width, depth, heap_len) = v2_read_header(&file).unwrap();
    let mut counters = v2_read_cms(&file, width, depth).unwrap();
    let heap_entries = v2_read_heap(&file, width, depth, heap_len, k).unwrap();
    let mut heap = V2IndexedHeap::new(heap_entries, k, 1).unwrap();

    let estimated =
        v2_cms_increment(&mut counters, width, depth, element.as_bytes(), count).unwrap();
    let evicted = heap
        .add(element.as_bytes(), estimated)
    .map(|value| String::from_utf8(value).unwrap());

    v2_write_cms(&file, &counters).unwrap();
    v2_write_heap(&file, width, depth, &heap.entries).unwrap();
    evicted
}

/// Helper: query whether an element is in the heap.
fn topk_query(path: &str, element: &str) -> bool {
    let file = crate::open_random_read(std::path::Path::new(path)).unwrap();
    let (k, width, depth, heap_len) = v2_read_header(&file).unwrap();
    let heap_entries = v2_read_heap(&file, width, depth, heap_len, k).unwrap();
    heap_entries.iter().any(|e| e.element == element.as_bytes())
}

/// Helper: get CMS count estimate for an element.
fn topk_count(path: &str, element: &str) -> i64 {
    let file = crate::open_random_read(std::path::Path::new(path)).unwrap();
    let (_k, width, depth, _heap_len) = v2_read_header(&file).unwrap();
    let counters = v2_read_cms(&file, width, depth).unwrap();
    v2_cms_estimate(&counters, width, depth, element.as_bytes())
}

/// Helper: list heap entries sorted descending by count.
fn topk_list(path: &str) -> Vec<(String, i64)> {
    let file = crate::open_random_read(std::path::Path::new(path)).unwrap();
    let (k, width, depth, heap_len) = v2_read_header(&file).unwrap();
    let mut entries = v2_read_heap(&file, width, depth, heap_len, k).unwrap();
    entries.sort_by(|a, b| {
        b.count
            .cmp(&a.count)
            .then_with(|| a.element.cmp(&b.element))
    });
    entries
        .into_iter()
        .map(|e| (String::from_utf8(e.element).unwrap(), e.count))
        .collect()
}

#[test]
fn create_add_and_query() {
    let dir = tempfile::tempdir().unwrap();
    let path = create_topk_file(dir.path(), "add_query.topk", 5, 64, 5);
    topk_add_elements(&path, &["hello"]);
    assert!(topk_query(&path, "hello"));
    assert!(!topk_query(&path, "world"));
}

#[test]
fn topk_eviction() {
    let dir = tempfile::tempdir().unwrap();
    let path = create_topk_file(dir.path(), "evict.topk", 3, 64, 5);

    // Add 3 elements — fills the heap
    topk_add_elements(&path, &["a", "b", "c"]);
    assert!(topk_query(&path, "a"));
    assert!(topk_query(&path, "b"));
    assert!(topk_query(&path, "c"));

    // Bump "d" so its CMS estimate exceeds the heap minimum (all at 1).
    // Increment "d" by 10 so it clearly beats the min.
    topk_incrby_one(&path, "d", 10);
    assert!(
        topk_query(&path, "d"),
        "d should have evicted a min element"
    );

    // One of a/b/c got evicted
    let in_heap: Vec<bool> = ["a", "b", "c"]
        .iter()
        .map(|e| topk_query(&path, e))
        .collect();
    let still_in = in_heap.iter().filter(|&&v| v).count();
    assert_eq!(still_in, 2, "exactly one of a/b/c should be evicted");
}

#[test]
fn add_same_element_accumulates_count() {
    let dir = tempfile::tempdir().unwrap();
    let path = create_topk_file(dir.path(), "accum.topk", 5, 64, 5);

    topk_add_elements(&path, &["x", "x", "x", "x", "x"]);

    let count = topk_count(&path, "x");
    assert_eq!(count, 5);
    assert!(topk_query(&path, "x"));
}

#[test]
fn query_nonexistent_returns_zero_count() {
    let dir = tempfile::tempdir().unwrap();
    let path = create_topk_file(dir.path(), "noexist.topk", 5, 64, 5);

    let count = topk_count(&path, "never_added");
    assert_eq!(count, 0);
    assert!(!topk_query(&path, "never_added"));
}

#[test]
fn list_returns_sorted_descending() {
    let dir = tempfile::tempdir().unwrap();
    let path = create_topk_file(dir.path(), "list.topk", 5, 64, 5);

    topk_incrby_one(&path, "low", 1);
    topk_incrby_one(&path, "mid", 5);
    topk_incrby_one(&path, "high", 10);

    let list = topk_list(&path);
    assert_eq!(list.len(), 3);
    assert_eq!(list[0].0, "high");
    assert_eq!(list[0].1, 10);
    assert_eq!(list[1].0, "mid");
    assert_eq!(list[1].1, 5);
    assert_eq!(list[2].0, "low");
    assert_eq!(list[2].1, 1);
}

#[test]
fn info_metadata_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let path = create_topk_file(dir.path(), "info.topk", 10, 128, 7);
    let file = crate::open_random_read(std::path::Path::new(&path)).unwrap();
    let (k, width, depth, heap_len) = v2_read_header(&file).unwrap();
    assert_eq!(k, 10);
    assert_eq!(width, 128);
    assert_eq!(depth, 7);
    assert_eq!(heap_len, 0);
}

#[test]
fn large_k_stress() {
    let dir = tempfile::tempdir().unwrap();
    let k = 100u32;
    let path = create_topk_file(dir.path(), "stress.topk", k, 256, 5);

    // Add 200 distinct elements
    for i in 0..200u32 {
        let name = format!("elem_{i:04}");
        topk_incrby_one(&path, &name, (i + 1) as i64);
    }

    let list = topk_list(&path);
    assert_eq!(list.len(), k as usize);

    // All entries in the list should have count >= 100 (bottom half evicted).
    // CMS can overcount due to hash collisions, so use >= not >.
    for (name, count) in &list {
        assert!(
            *count >= 100,
            "element {name} with count {count} should have been evicted"
        );
    }

    // The top element's CMS estimate should be >= 200 (may overcount
    // due to collisions, so we only check the lower bound).
    assert!(
        list[0].1 >= 200,
        "top element count {} should be >= 200",
        list[0].1
    );
}

#[test]
fn incrby_with_large_increment() {
    let dir = tempfile::tempdir().unwrap();
    let path = create_topk_file(dir.path(), "big_incr.topk", 5, 64, 5);
    topk_incrby_one(&path, "big", 1_000_000);
    let count = topk_count(&path, "big");
    assert_eq!(count, 1_000_000);
}

#[test]
fn eviction_preserves_higher_counts() {
    let dir = tempfile::tempdir().unwrap();
    let path = create_topk_file(dir.path(), "pres.topk", 2, 64, 5);

    topk_incrby_one(&path, "keep_a", 100);
    topk_incrby_one(&path, "keep_b", 50);

    // This has a lower count than both, should NOT evict
    topk_incrby_one(&path, "loser", 1);
    assert!(!topk_query(&path, "loser"));
    assert!(topk_query(&path, "keep_a"));
    assert!(topk_query(&path, "keep_b"));
}
