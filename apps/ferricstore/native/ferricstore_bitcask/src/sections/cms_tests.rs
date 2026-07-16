use super::*;
use std::fs;

    // -----------------------------------------------------------------------
    // Stateless pread/pwrite file tests
    // -----------------------------------------------------------------------

    #[test]
    fn file_create_and_info() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("create_info.cms");
        let path_str = path.to_str().unwrap().to_string();

        {
            let p = Path::new(&path_str);
            if let Some(parent) = p.parent() {
                fs::create_dir_all(parent).unwrap();
            }
            let counter_bytes = 100usize * 7 * 8;
            let mut file = File::create(p).unwrap();
            let mut header = [0u8; MMAP_HEADER_SIZE];
            header[0..8].copy_from_slice(&MMAP_MAGIC.to_le_bytes());
            header[8..16].copy_from_slice(&100u64.to_le_bytes());
            header[16..24].copy_from_slice(&7u64.to_le_bytes());
            file.write_all(&header).unwrap();
            file.write_all(&vec![0u8; counter_bytes]).unwrap();
            file.sync_all().unwrap();
        }

        let file = File::open(&path_str).unwrap();
        let (w, d, c) = cms_file_read_header(&file).unwrap();
        assert_eq!(w, 100);
        assert_eq!(d, 7);
        assert_eq!(c, 0);
    }

    #[test]
    fn file_query_unseen_returns_zero() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("unseen.cms");
        let width = 100u64;
        let depth = 5u64;

        {
            let counter_bytes = (width as usize) * (depth as usize) * 8;
            let mut file = File::create(&path).unwrap();
            let mut header = [0u8; MMAP_HEADER_SIZE];
            header[0..8].copy_from_slice(&MMAP_MAGIC.to_le_bytes());
            header[8..16].copy_from_slice(&width.to_le_bytes());
            header[16..24].copy_from_slice(&depth.to_le_bytes());
            file.write_all(&header).unwrap();
            file.write_all(&vec![0u8; counter_bytes]).unwrap();
            file.sync_all().unwrap();
        }

        let file = File::open(&path).unwrap();
        let (w, d, _) = cms_file_read_header(&file).unwrap();
        let indices = hash_indices_standalone(b"never_seen", w, d);
        let mut buf = [0u8; 8];
        let mut min_val = i64::MAX;
        for (row, &col) in indices.iter().enumerate() {
            let offset = MMAP_HEADER_SIZE as u64 + (row as u64 * w + col) * 8;
            file.read_at(&mut buf, offset).unwrap();
            min_val = min_val.min(i64::from_le_bytes(buf));
        }
        assert_eq!(min_val, 0);
    }

    #[test]
    fn file_read_header_nonexistent_returns_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nonexistent.cms");
        let result = File::open(&path);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::NotFound);
    }

    #[test]
    fn standalone_hash_deterministic() {
        let element = b"test_element";
        let h1 = fnv1a_standalone(element);
        let h2 = fnv1a_standalone(element);
        assert_eq!(h1, h2, "fnv1a must be deterministic");

        let s1 = fnv1a_salted_standalone(element);
        let s2 = fnv1a_salted_standalone(element);
        assert_eq!(s1, s2, "fnv1a_salted must be deterministic");

        // salted and unsalted must differ
        assert_ne!(h1, s1, "salted and unsalted hashes must differ");
    }

    #[test]
    fn hash_indices_within_bounds() {
        let width = 100u64;
        let depth = 7u64;
        let indices = hash_indices_standalone(b"test", width, depth);
        assert_eq!(indices.len(), depth as usize);
        for &idx in &indices {
            assert!(idx < width, "index {idx} >= width {width}");
        }
    }

    // -----------------------------------------------------------------------
    // Edge case tests
    // -----------------------------------------------------------------------

    /// Helper: create a valid CMS file, returning the path string.
    fn create_cms_file(dir: &std::path::Path, name: &str, width: u64, depth: u64) -> String {
        let path = dir.join(name);
        let counter_bytes = (width as usize) * (depth as usize) * 8;
        let mut file = File::create(&path).unwrap();
        let mut header = [0u8; MMAP_HEADER_SIZE];
        header[0..8].copy_from_slice(&MMAP_MAGIC.to_le_bytes());
        header[8..16].copy_from_slice(&width.to_le_bytes());
        header[16..24].copy_from_slice(&depth.to_le_bytes());
        file.write_all(&header).unwrap();
        file.write_all(&vec![0u8; counter_bytes]).unwrap();
        file.sync_all().unwrap();
        path.to_str().unwrap().to_string()
    }

    #[test]
    fn empty_element_hashing() {
        // Zero-length element should hash without panic
        let h1 = fnv1a_standalone(b"");
        let h2 = fnv1a_salted_standalone(b"");
        // Should produce valid hashes (not necessarily different for empty input)
        assert!(h1 != 0 || h2 != 0, "at least one hash should be non-zero");

        let indices = hash_indices_standalone(b"", 100, 5);
        assert_eq!(indices.len(), 5);
        for &idx in &indices {
            assert!(idx < 100);
        }
    }

    #[test]
    fn large_element_hashing() {
        // 1MB element
        let big = vec![0xCDu8; 1_000_000];
        let indices = hash_indices_standalone(&big, 1000, 7);
        assert_eq!(indices.len(), 7);
        for &idx in &indices {
            assert!(idx < 1000);
        }
    }

    #[test]
    fn truncated_header_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("truncated.cms");
        std::fs::write(&path, [0u8; 16]).unwrap();
        let file = File::open(&path).unwrap();
        assert!(cms_file_read_header(&file).is_err());
    }

    #[test]
    fn complete_header_rejects_a_truncated_counter_region() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("missing_counters.cms");
        let mut header = [0u8; MMAP_HEADER_SIZE];
        header[0..8].copy_from_slice(&MMAP_MAGIC.to_le_bytes());
        header[8..16].copy_from_slice(&4u64.to_le_bytes());
        header[16..24].copy_from_slice(&2u64.to_le_bytes());
        std::fs::write(&path, header).unwrap();

        assert!(cms_file_read_header(&File::open(path).unwrap()).is_err());
    }

    #[test]
    fn read_exact_at_rejects_short_counter() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("short_counter.cms");
        std::fs::write(&path, [0u8; 4]).unwrap();
        let file = File::open(&path).unwrap();
        let mut buf = [0u8; 8];

        let err = cms_read_exact_at(&file, &mut buf, 0, "counter").unwrap_err();

        assert!(err.contains("truncated"));
    }

    #[test]
    fn wrong_magic_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad_magic.cms");
        let mut data = [0u8; MMAP_HEADER_SIZE + 64];
        data[0..8].copy_from_slice(&0xBAAD_F00D_u64.to_le_bytes());
        data[8..16].copy_from_slice(&10u64.to_le_bytes());
        data[16..24].copy_from_slice(&5u64.to_le_bytes());
        std::fs::write(&path, data).unwrap();
        let file = File::open(&path).unwrap();
        let result = cms_file_read_header(&file);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("magic"));
    }

    #[test]
    fn header_with_zero_width_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("zero_width.cms");
        let mut header = [0u8; MMAP_HEADER_SIZE];
        header[0..8].copy_from_slice(&MMAP_MAGIC.to_le_bytes());
        header[8..16].copy_from_slice(&0u64.to_le_bytes()); // width=0
        header[16..24].copy_from_slice(&5u64.to_le_bytes());
        std::fs::write(&path, header).unwrap();
        let file = File::open(&path).unwrap();
        let result = cms_file_read_header(&file);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("width and depth must be > 0"));
    }

    #[test]
    fn header_with_oversized_counter_region_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("oversized.cms");
        let mut header = [0u8; MMAP_HEADER_SIZE];
        header[0..8].copy_from_slice(&MMAP_MAGIC.to_le_bytes());
        header[8..16].copy_from_slice(&u64::MAX.to_le_bytes());
        header[16..24].copy_from_slice(&2u64.to_le_bytes());
        std::fs::write(&path, header).unwrap();
        let file = File::open(&path).unwrap();

        let result = cms_file_read_header(&file);

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("counter"));
    }

    #[test]
    fn minimum_size_cms() {
        // width=1, depth=1 -- smallest possible CMS
        let dir = tempfile::tempdir().unwrap();
        let path_str = create_cms_file(dir.path(), "min.cms", 1, 1);
        let file = File::open(&path_str).unwrap();
        let (w, d, c) = cms_file_read_header(&file).unwrap();
        assert_eq!(w, 1);
        assert_eq!(d, 1);
        assert_eq!(c, 0);
    }

    #[test]
    fn incrby_roundtrip() {
        // Create, increment, then query
        let dir = tempfile::tempdir().unwrap();
        let path_str = create_cms_file(dir.path(), "incr.cms", 100, 5);

        let file = crate::open_random_rw(Path::new(&path_str)).unwrap();
        let (width, depth, _) = cms_file_read_header(&file).unwrap();

        // Increment "hello" by 3
        let indices = hash_indices_standalone(b"hello", width, depth);
        let mut buf = [0u8; 8];
        for (row, &col) in indices.iter().enumerate() {
            let offset = MMAP_HEADER_SIZE as u64 + (row as u64 * width + col) * 8;
            file.read_at(&mut buf, offset).unwrap();
            let mut val = i64::from_le_bytes(buf);
            val += 3;
            file.write_at(&val.to_le_bytes(), offset).unwrap();
        }
        drop(file);

        // Query
        let file = crate::open_random_read(Path::new(&path_str)).unwrap();
        let (width, depth, _) = cms_file_read_header(&file).unwrap();
        let indices = hash_indices_standalone(b"hello", width, depth);
        let mut min_val = i64::MAX;
        for (row, &col) in indices.iter().enumerate() {
            let offset = MMAP_HEADER_SIZE as u64 + (row as u64 * width + col) * 8;
            file.read_at(&mut buf, offset).unwrap();
            let val = i64::from_le_bytes(buf);
            min_val = min_val.min(val);
        }
        assert_eq!(min_val, 3);
    }

    #[test]
    fn merge_additive() {
        // Create two CMS files, increment differently, merge, verify additive result.
        let dir = tempfile::tempdir().unwrap();
        let dst_str = create_cms_file(dir.path(), "dst.cms", 50, 3);
        let src_str = create_cms_file(dir.path(), "src.cms", 50, 3);

        // Increment "foo" by 5 in dst
        {
            let file = crate::open_random_rw(Path::new(&dst_str)).unwrap();
            let (width, depth, _) = cms_file_read_header(&file).unwrap();
            let indices = hash_indices_standalone(b"foo", width, depth);
            for (row, &col) in indices.iter().enumerate() {
                let offset = MMAP_HEADER_SIZE as u64 + (row as u64 * width + col) * 8;
                file.write_at(&5i64.to_le_bytes(), offset).unwrap();
            }
            // Set count=5
            file.write_at(&5u64.to_le_bytes(), 24).unwrap();
        }

        // Increment "foo" by 3 in src
        {
            let file = crate::open_random_rw(Path::new(&src_str)).unwrap();
            let (width, depth, _) = cms_file_read_header(&file).unwrap();
            let indices = hash_indices_standalone(b"foo", width, depth);
            for (row, &col) in indices.iter().enumerate() {
                let offset = MMAP_HEADER_SIZE as u64 + (row as u64 * width + col) * 8;
                file.write_at(&3i64.to_le_bytes(), offset).unwrap();
            }
            file.write_at(&3u64.to_le_bytes(), 24).unwrap();
        }

        // Merge: dst += src * 1
        {
            let dst_file = crate::open_random_rw(Path::new(&dst_str)).unwrap();
            let src_file = crate::open_random_read(Path::new(&src_str)).unwrap();
            let (width, depth, _) = cms_file_read_header(&dst_file).unwrap();
            let total_counters = width * depth;
            let mut dst_buf = [0u8; 8];
            let mut src_buf = [0u8; 8];
            for i in 0..total_counters {
                let offset = MMAP_HEADER_SIZE as u64 + i * 8;
                dst_file.read_at(&mut dst_buf, offset).unwrap();
                src_file.read_at(&mut src_buf, offset).unwrap();
                let val = i64::from_le_bytes(dst_buf) + i64::from_le_bytes(src_buf);
                dst_file.write_at(&val.to_le_bytes(), offset).unwrap();
            }
        }

        // Verify: "foo" should have count 8 (5+3)
        let file = crate::open_random_read(Path::new(&dst_str)).unwrap();
        let (width, depth, _) = cms_file_read_header(&file).unwrap();
        let indices = hash_indices_standalone(b"foo", width, depth);
        let mut min_val = i64::MAX;
        let mut buf = [0u8; 8];
        for (row, &col) in indices.iter().enumerate() {
            let offset = MMAP_HEADER_SIZE as u64 + (row as u64 * width + col) * 8;
            file.read_at(&mut buf, offset).unwrap();
            min_val = min_val.min(i64::from_le_bytes(buf));
        }
        assert_eq!(min_val, 8);
    }

    #[test]
    fn nonexistent_file_returns_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nope.cms");
        let result = crate::open_random_read(&path);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::NotFound);
    }

    #[test]
    fn null_bytes_in_element() {
        let element = b"foo\x00bar\x00baz";
        let indices = hash_indices_standalone(element, 100, 5);
        assert_eq!(indices.len(), 5);
        for &idx in &indices {
            assert!(idx < 100);
        }
        // Should differ from element without nulls
        let indices2 = hash_indices_standalone(b"foobarbaz", 100, 5);
        assert_ne!(indices, indices2);
    }

    // -----------------------------------------------------------------------
    // Incrby / query file I/O tests
    // -----------------------------------------------------------------------

    /// Helper: do pread/pwrite incrby for a single element, return min count.
    fn file_incrby_one(path: &str, element: &[u8], count: i64) -> i64 {
        let file = crate::open_random_rw_locked(Path::new(path)).unwrap();
        let (width, depth, mut total_count) = cms_file_read_header(&file).unwrap();
        let indices = hash_indices_standalone(element, width, depth);
        let mut buf = [0u8; 8];
        let mut min_val = i64::MAX;
        for (row, &col) in indices.iter().enumerate() {
            let offset = MMAP_HEADER_SIZE as u64 + (row as u64 * width + col) * 8;
            file.read_at(&mut buf, offset).unwrap();
            let mut val = i64::from_le_bytes(buf);
            val += count;
            file.write_at(&val.to_le_bytes(), offset).unwrap();
            min_val = min_val.min(val);
        }
        total_count = total_count.wrapping_add(count as u64);
        file.write_at(&total_count.to_le_bytes(), 24).unwrap();
        min_val
    }

    /// Helper: query a single element via pread, return min count.
    fn file_query_one(path: &str, element: &[u8]) -> i64 {
        let file = crate::open_random_read_locked(Path::new(path)).unwrap();
        let (width, depth, _) = cms_file_read_header(&file).unwrap();
        let indices = hash_indices_standalone(element, width, depth);
        let mut buf = [0u8; 8];
        let mut min_val = i64::MAX;
        for (row, &col) in indices.iter().enumerate() {
            let offset = MMAP_HEADER_SIZE as u64 + (row as u64 * width + col) * 8;
            file.read_at(&mut buf, offset).unwrap();
            min_val = min_val.min(i64::from_le_bytes(buf));
        }
        min_val
    }

    #[test]
    fn incrby_by_one() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_cms_file(dir.path(), "incr1.cms", 100, 5);
        let min = file_incrby_one(&path, b"elem", 1);
        assert_eq!(min, 1);
    }

    #[test]
    fn incrby_by_ten() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_cms_file(dir.path(), "incr10.cms", 100, 5);
        let min = file_incrby_one(&path, b"elem", 10);
        assert_eq!(min, 10);
    }

    #[test]
    fn incrby_negative() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_cms_file(dir.path(), "incr_neg.cms", 100, 5);
        // Increment up first, then decrement
        file_incrby_one(&path, b"x", 5);
        let min = file_incrby_one(&path, b"x", -3);
        assert_eq!(min, 2);
    }

    #[test]
    fn incrby_accumulates() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_cms_file(dir.path(), "accum.cms", 100, 5);
        file_incrby_one(&path, b"item", 3);
        file_incrby_one(&path, b"item", 7);
        file_incrby_one(&path, b"item", 2);
        let count = file_query_one(&path, b"item");
        assert_eq!(count, 12);
    }

    #[test]
    fn batch_staging_accumulates_duplicate_elements_before_any_write() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_cms_file(dir.path(), "batch_duplicates.cms", 100, 5);
        let file = crate::open_random_rw(Path::new(&path)).unwrap();

        let plan = cms_stage_increments(
            &file,
            100,
            5,
            0,
            [(b"same".as_slice(), 2), (b"same".as_slice(), 3)],
        )
        .unwrap();

        assert_eq!(plan.counts, vec![2, 5]);
        assert_eq!(plan.total_count, 5);
        assert!(plan.updates.iter().all(|(_, value)| *value == 5));

        let mut bytes = [0u8; 8];
        for (offset, _) in &plan.updates {
            cms_read_exact_at(&file, &mut bytes, *offset, "counter").unwrap();
            assert_eq!(i64::from_le_bytes(bytes), 0, "staging must not mutate disk");
        }
    }

    #[test]
    fn query_multiple_independent_elements() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_cms_file(dir.path(), "multi.cms", 200, 7);
        file_incrby_one(&path, b"alpha", 10);
        file_incrby_one(&path, b"beta", 20);
        file_incrby_one(&path, b"gamma", 5);

        let a = file_query_one(&path, b"alpha");
        let b = file_query_one(&path, b"beta");
        let g = file_query_one(&path, b"gamma");
        let unseen = file_query_one(&path, b"delta");

        // CMS can overcount due to collisions, but never undercount
        assert!(a >= 10, "alpha: expected >=10, got {a}");
        assert!(b >= 20, "beta: expected >=20, got {b}");
        assert!(g >= 5, "gamma: expected >=5, got {g}");
        assert_eq!(unseen, 0);
    }

    #[test]
    fn merge_two_files_additive() {
        let dir = tempfile::tempdir().unwrap();
        let dst = create_cms_file(dir.path(), "merge_dst.cms", 50, 3);
        let src = create_cms_file(dir.path(), "merge_src.cms", 50, 3);

        file_incrby_one(&dst, b"key", 10);
        file_incrby_one(&src, b"key", 7);

        // Merge src into dst with weight 1
        {
            let dst_file = crate::open_random_rw(Path::new(&dst)).unwrap();
            let src_file = crate::open_random_read(Path::new(&src)).unwrap();
            let (width, depth, _) = cms_file_read_header(&dst_file).unwrap();
            let total = width * depth;
            let mut db = [0u8; 8];
            let mut sb = [0u8; 8];
            for i in 0..total {
                let offset = MMAP_HEADER_SIZE as u64 + i * 8;
                dst_file.read_at(&mut db, offset).unwrap();
                src_file.read_at(&mut sb, offset).unwrap();
                let val = i64::from_le_bytes(db) + i64::from_le_bytes(sb);
                dst_file.write_at(&val.to_le_bytes(), offset).unwrap();
            }
        }

        let count = file_query_one(&dst, b"key");
        assert_eq!(count, 17);
    }

    #[test]
    fn merge_with_weight() {
        let dir = tempfile::tempdir().unwrap();
        let dst = create_cms_file(dir.path(), "mw_dst.cms", 50, 3);
        let src = create_cms_file(dir.path(), "mw_src.cms", 50, 3);

        file_incrby_one(&dst, b"w", 4);
        file_incrby_one(&src, b"w", 3);

        // Merge src into dst with weight 2: dst += src * 2
        {
            let dst_file = crate::open_random_rw(Path::new(&dst)).unwrap();
            let src_file = crate::open_random_read(Path::new(&src)).unwrap();
            let (width, depth, _) = cms_file_read_header(&dst_file).unwrap();
            let total = width * depth;
            let mut db = [0u8; 8];
            let mut sb = [0u8; 8];
            let weight: i64 = 2;
            for i in 0..total {
                let offset = MMAP_HEADER_SIZE as u64 + i * 8;
                dst_file.read_at(&mut db, offset).unwrap();
                src_file.read_at(&mut sb, offset).unwrap();
                let val = i64::from_le_bytes(db) + i64::from_le_bytes(sb) * weight;
                dst_file.write_at(&val.to_le_bytes(), offset).unwrap();
            }
        }

        // 4 + 3*2 = 10
        let count = file_query_one(&dst, b"w");
        assert_eq!(count, 10);
    }

    #[test]
    fn merge_total_count_rejects_positive_overflow() {
        let err = cms_next_merge_total_count(u64::MAX, 1, 1).unwrap_err();

        assert!(err.contains("overflow"));
    }

    #[test]
    fn merge_total_count_clamps_negative_delta_to_zero() {
        assert_eq!(cms_next_merge_total_count(5, 10, -1).unwrap(), 0);
    }

    #[test]
    fn merge_counter_rejects_positive_overflow() {
        let err = cms_finalize_merge_counter(i128::from(i64::MAX) + 1).unwrap_err();

        assert!(err.contains("overflow"));
    }

    #[test]
    fn merge_uses_bounded_chunks_instead_of_one_syscall_per_counter() {
        let source = std::fs::read_to_string("src/cms.rs").unwrap();
        let merge = source
            .split("pub fn cms_file_merge(")
            .nth(1)
            .unwrap()
            .split("// Async variants of read NIFs")
            .next()
            .unwrap();

        assert!(merge.contains("CMS_MERGE_CHUNK_COUNTERS"));
        assert!(!merge.contains("\"destination counter\""));
        assert!(!merge.contains("\"source counter\""));
    }

    #[test]
    fn counter_large_values() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_cms_file(dir.path(), "large.cms", 100, 5);
        // Increment near i64 max / 2 twice — should not panic
        let big = i64::MAX / 2;
        let min1 = file_incrby_one(&path, b"big", big);
        assert_eq!(min1, big);
        // Second increment wraps — just verify no panic
        let min2 = file_incrby_one(&path, b"big", big);
        assert_eq!(min2, big.wrapping_add(big));
    }

    #[test]
    fn concurrent_reads_during_incrby() {
        use std::sync::Arc;
        use std::thread;

        let dir = tempfile::tempdir().unwrap();
        let path = create_cms_file(dir.path(), "conc.cms", 200, 5);
        let path_arc = Arc::new(path);

        // Writer thread
        let pw = Arc::clone(&path_arc);
        let writer = thread::spawn(move || {
            for _ in 0..100 {
                file_incrby_one(&pw, b"concurrent", 1);
            }
        });

        // Reader threads
        let mut readers = Vec::new();
        for _ in 0..4 {
            let pr = Arc::clone(&path_arc);
            readers.push(thread::spawn(move || {
                for _ in 0..50 {
                    let count = file_query_one(&pr, b"concurrent");
                    // count must be non-negative (no corruption)
                    assert!(count >= 0, "negative count during concurrent read: {count}");
                }
            }));
        }

        writer.join().unwrap();
        for r in readers {
            r.join().unwrap();
        }

        // Final count must be exactly 100
        let final_count = file_query_one(&path_arc, b"concurrent");
        assert_eq!(final_count, 100);
    }

    #[test]
    fn concurrent_writers_preserve_every_increment() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        const WRITERS: usize = 8;
        const INCREMENTS_PER_WRITER: usize = 200;

        let dir = tempfile::tempdir().unwrap();
        let path = Arc::new(create_cms_file(dir.path(), "writers.cms", 200, 5));
        let barrier = Arc::new(Barrier::new(WRITERS));
        let writers = (0..WRITERS)
            .map(|_| {
                let writer_path = Arc::clone(&path);
                let writer_barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    writer_barrier.wait();
                    for _ in 0..INCREMENTS_PER_WRITER {
                        file_incrby_one(&writer_path, b"contended", 1);
                    }
                })
            })
            .collect::<Vec<_>>();

        for writer in writers {
            writer.join().unwrap();
        }

        assert_eq!(
            file_query_one(&path, b"contended"),
            (WRITERS * INCREMENTS_PER_WRITER) as i64
        );
    }

    #[test]
    fn total_count_in_header_tracks_increments() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_cms_file(dir.path(), "hdr_count.cms", 100, 5);
        file_incrby_one(&path, b"a", 3);
        file_incrby_one(&path, b"b", 7);

        let file = crate::open_random_read(Path::new(&path)).unwrap();
        let (_, _, total_count) = cms_file_read_header(&file).unwrap();
        assert_eq!(total_count, 10);
    }
