    #[test]
    fn add_and_exists_via_helpers() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_cuckoo_file(dir.path(), "add_exists.cuckoo", 128, 4);

        test_add(&path, b"hello").unwrap();
        assert!(test_exists(&path, b"hello").unwrap());
        assert!(!test_exists(&path, b"world").unwrap());
    }

    #[test]
    fn add_multiple_elements() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_cuckoo_file(dir.path(), "multi.cuckoo", 256, 4);

        let elements: Vec<Vec<u8>> = (0..50).map(|i| format!("elem_{i}").into_bytes()).collect();

        for elem in &elements {
            test_add(&path, elem).unwrap();
        }

        for elem in &elements {
            assert!(
                test_exists(&path, elem).unwrap(),
                "element {:?} should exist after add",
                std::str::from_utf8(elem).unwrap()
            );
        }

        // Verify something never added does not exist.
        assert!(!test_exists(&path, b"never_added").unwrap());
    }

    #[test]
    fn delete_element() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_cuckoo_file(dir.path(), "delete.cuckoo", 128, 4);

        test_add(&path, b"todelete").unwrap();
        assert!(test_exists(&path, b"todelete").unwrap());

        let deleted = test_del(&path, b"todelete").unwrap();
        assert!(deleted);

        assert!(!test_exists(&path, b"todelete").unwrap());
    }

    #[test]
    fn addnx_already_exists() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_cuckoo_file(dir.path(), "addnx.cuckoo", 128, 4);

        // First add should succeed.
        let r1 = test_addnx(&path, b"unique").unwrap();
        assert_eq!(r1, 1);

        // Second addnx of same element should return 0.
        let r2 = test_addnx(&path, b"unique").unwrap();
        assert_eq!(r2, 0);

        // Different element should still add.
        let r3 = test_addnx(&path, b"different").unwrap();
        assert_eq!(r3, 1);
    }

    #[test]
    fn count_duplicates() {
        // Cuckoo filters allow duplicates via regular add.
        let dir = tempfile::tempdir().unwrap();
        let path = create_cuckoo_file(dir.path(), "count.cuckoo", 128, 4);

        assert_eq!(test_count(&path, b"dup").unwrap(), 0);

        test_add(&path, b"dup").unwrap();
        assert_eq!(test_count(&path, b"dup").unwrap(), 1);

        test_add(&path, b"dup").unwrap();
        assert_eq!(test_count(&path, b"dup").unwrap(), 2);

        test_add(&path, b"dup").unwrap();
        assert_eq!(test_count(&path, b"dup").unwrap(), 3);
    }

    #[test]
    fn info_matches_creation_params() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_cuckoo_file(dir.path(), "info.cuckoo", 200, 4);

        let (num_buckets, bucket_size, fp_size, num_items, num_deletes, total_slots, max_kicks) =
            test_info(&path).unwrap();

        assert_eq!(num_buckets, 200);
        assert_eq!(bucket_size, 4);
        assert_eq!(fp_size, FILE_DEFAULT_FINGERPRINT_SIZE as u64);
        assert_eq!(num_items, 0);
        assert_eq!(num_deletes, 0);
        assert_eq!(total_slots, 200 * 4);
        assert_eq!(max_kicks, FILE_DEFAULT_MAX_KICKS as u64);

        // Add some elements and verify counters update.
        test_add(&path, b"a").unwrap();
        test_add(&path, b"b").unwrap();
        let (_, _, _, items2, _, _, _) = test_info(&path).unwrap();
        assert_eq!(items2, 2);

        test_del(&path, b"a").unwrap();
        let (_, _, _, items3, deletes3, _, _) = test_info(&path).unwrap();
        assert_eq!(items3, 1);
        assert_eq!(deletes3, 1);
    }

    #[test]
    fn nonexistent_file_errors_for_all_ops() {
        let dir = tempfile::tempdir().unwrap();
        let bad = dir
            .path()
            .join("missing.cuckoo")
            .to_str()
            .unwrap()
            .to_string();

        assert!(test_exists(&bad, b"x").is_err());
        assert!(test_add(&bad, b"x").is_err());
        assert!(test_del(&bad, b"x").is_err());
        assert!(test_addnx(&bad, b"x").is_err());
        assert!(test_count(&bad, b"x").is_err());
        assert!(test_info(&bad).is_err());
    }

    #[test]
    fn empty_filter_exists_and_count() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_cuckoo_file(dir.path(), "empty.cuckoo", 64, 4);

        assert!(!test_exists(&path, b"anything").unwrap());
        assert_eq!(test_count(&path, b"anything").unwrap(), 0);
    }

    #[test]
    fn zero_fingerprint_bypass() {
        // The code maps all-zero fingerprints to [1, 0, ...].
        // Verify with different fingerprint sizes.
        for fp_size in 1..=4 {
            for i in 0..5000 {
                let (fp, _) = cuckoo_file_fingerprint_and_bucket(
                    format!("zfp_{fp_size}_{i}").as_bytes(),
                    fp_size,
                    1024,
                );
                assert!(
                    !fp.iter().all(|&b| b == 0),
                    "fingerprint must never be all zeros (fp_size={fp_size}, i={i})"
                );
            }
        }
    }

    #[test]
    fn kick_chain_triggered_and_elements_findable() {
        // Use a small filter that forces eviction kicks.
        // capacity=8 buckets, bucket_size=2 => 16 total slots.
        // Inserting ~12 elements should trigger kicks since collisions
        // are inevitable with only 8 buckets.
        let dir = tempfile::tempdir().unwrap();
        let path = create_cuckoo_file(dir.path(), "kick.cuckoo", 8, 2);

        let mut added = Vec::new();
        for i in 0..12 {
            let elem = format!("kick_{i}").into_bytes();
            match test_add(&path, &elem) {
                Ok(_) => added.push(elem),
                Err(_) => break, // filter full, stop
            }
        }

        // Every successfully added element must be findable.
        for elem in &added {
            assert!(
                test_exists(&path, elem).unwrap(),
                "element {:?} was added but not found",
                std::str::from_utf8(elem).unwrap()
            );
        }
        assert!(added.len() >= 2, "should have added at least some elements");
    }

    #[test]
    fn filter_full_returns_error() {
        // Tiny filter: 2 buckets, 1 slot each => 2 total slots.
        let dir = tempfile::tempdir().unwrap();
        let path = create_cuckoo_file(dir.path(), "full2.cuckoo", 2, 1);

        // Keep adding until we get "filter is full".
        let mut count = 0;
        for i in 0..1000 {
            let elem = format!("fill_{i}").into_bytes();
            match test_add(&path, &elem) {
                Ok(_) => count += 1,
                Err(e) => {
                    assert!(e.contains("filter is full"), "unexpected error: {e}");
                    break;
                }
            }
        }
        // With 2 slots, we can hold at most 2 elements (could be fewer
        // due to bucket collisions with the eviction chain failing).
        assert!(count <= 2, "should not exceed total slot count");
        assert!(count >= 1, "should have added at least one element");
    }

    #[test]
    fn delete_non_existent_element() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_cuckoo_file(dir.path(), "del_none.cuckoo", 64, 4);

        // Delete something never added.
        let deleted = test_del(&path, b"ghost").unwrap();
        assert!(!deleted);

        // Counters should be unchanged.
        let (_, _, _, items, deletes, _, _) = test_info(&path).unwrap();
        assert_eq!(items, 0);
        assert_eq!(deletes, 0);
    }

    #[test]
    fn large_number_of_insertions() {
        // Stress test: 1024 buckets * 4 slots = 4096 total slots.
        // At ~95% load cuckoo filters start failing, so insert up to 3500.
        let dir = tempfile::tempdir().unwrap();
        let path = create_cuckoo_file(dir.path(), "stress.cuckoo", 1024, 4);

        let mut inserted = Vec::new();
        for i in 0..3500 {
            let elem = format!("stress_{i}").into_bytes();
            match test_add(&path, &elem) {
                Ok(_) => inserted.push(elem),
                Err(_) => break,
            }
        }

        // All successfully inserted elements must be findable.
        for elem in &inserted {
            assert!(
                test_exists(&path, elem).unwrap(),
                "element {:?} lost after stress insert (total inserted: {})",
                std::str::from_utf8(elem).unwrap(),
                inserted.len()
            );
        }

        // Verify info counters.
        let (_, _, _, items, _, _, _) = test_info(&path).unwrap();
        assert_eq!(items, inserted.len() as u64);

        // We should have managed to insert a substantial number.
        assert!(
            inserted.len() >= 500,
            "expected at least 500 insertions, got {}",
            inserted.len()
        );
    }

    #[test]
    fn concurrent_reads_from_same_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_cuckoo_file(dir.path(), "conc.cuckoo", 128, 4);

        // Pre-populate with some elements.
        for i in 0..20 {
            test_add(&path, format!("conc_{i}").as_bytes()).unwrap();
        }

        let path_clone = path.clone();
        let handles: Vec<_> = (0..4)
            .map(|t| {
                let p = path_clone.clone();
                std::thread::spawn(move || {
                    for i in 0..20 {
                        let elem = format!("conc_{i}");
                        let exists = test_exists(&p, elem.as_bytes()).unwrap();
                        assert!(exists, "thread {t}: element {elem} should exist");
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().expect("reader thread panicked");
        }
    }

    #[test]
    fn add_delete_readd_cycle() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_cuckoo_file(dir.path(), "cycle.cuckoo", 64, 4);

        test_add(&path, b"cycle").unwrap();
        assert!(test_exists(&path, b"cycle").unwrap());

        test_del(&path, b"cycle").unwrap();
        assert!(!test_exists(&path, b"cycle").unwrap());

        // Re-add after delete should work.
        test_add(&path, b"cycle").unwrap();
        assert!(test_exists(&path, b"cycle").unwrap());
    }

    #[test]
    fn delete_only_removes_one_occurrence() {
        // Add duplicates, delete one, verify count decreases by 1.
        let dir = tempfile::tempdir().unwrap();
        let path = create_cuckoo_file(dir.path(), "del_one.cuckoo", 128, 4);

        test_add(&path, b"multi").unwrap();
        test_add(&path, b"multi").unwrap();
        test_add(&path, b"multi").unwrap();
        assert_eq!(test_count(&path, b"multi").unwrap(), 3);

        test_del(&path, b"multi").unwrap();
        assert_eq!(test_count(&path, b"multi").unwrap(), 2);

        test_del(&path, b"multi").unwrap();
        assert_eq!(test_count(&path, b"multi").unwrap(), 1);

        // Still exists.
        assert!(test_exists(&path, b"multi").unwrap());

        test_del(&path, b"multi").unwrap();
        assert_eq!(test_count(&path, b"multi").unwrap(), 0);
        assert!(!test_exists(&path, b"multi").unwrap());
    }

    #[test]
    fn slot_offset_calculation() {
        // Verify byte offsets are calculated correctly.
        // bucket_idx=0, slot_idx=0 => HEADER_SIZE
        assert_eq!(cuckoo_file_slot_offset(0, 0, 4, 1), HEADER_SIZE as u64);

        // bucket_idx=1, slot_idx=0 with bucket_size=4, fp_size=1
        // => HEADER_SIZE + (1*4 + 0)*1 = HEADER_SIZE + 4
        assert_eq!(cuckoo_file_slot_offset(1, 0, 4, 1), HEADER_SIZE as u64 + 4);

        // bucket_idx=0, slot_idx=2 with bucket_size=4, fp_size=2
        // => HEADER_SIZE + (0*4 + 2)*2 = HEADER_SIZE + 4
        assert_eq!(cuckoo_file_slot_offset(0, 2, 4, 2), HEADER_SIZE as u64 + 4);

        // bucket_idx=3, slot_idx=1 with bucket_size=4, fp_size=1
        // => HEADER_SIZE + (3*4 + 1)*1 = HEADER_SIZE + 13
        assert_eq!(cuckoo_file_slot_offset(3, 1, 4, 1), HEADER_SIZE as u64 + 13);
    }

    #[test]
    fn num_items_counter_increments_per_add() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_cuckoo_file(dir.path(), "counter.cuckoo", 128, 4);

        for i in 0..10 {
            test_add(&path, format!("cnt_{i}").as_bytes()).unwrap();
            let (_, _, _, items, _, _, _) = test_info(&path).unwrap();
            assert_eq!(items, (i + 1) as u64);
        }
    }

    #[test]
    fn addnx_after_delete_readds() {
        // addnx after deletion should re-insert since element is gone.
        let dir = tempfile::tempdir().unwrap();
        let path = create_cuckoo_file(dir.path(), "addnx_del.cuckoo", 128, 4);

        assert_eq!(test_addnx(&path, b"reinsert").unwrap(), 1);
        assert_eq!(test_addnx(&path, b"reinsert").unwrap(), 0);

        test_del(&path, b"reinsert").unwrap();
        assert!(!test_exists(&path, b"reinsert").unwrap());

        // Now addnx should succeed again.
        assert_eq!(test_addnx(&path, b"reinsert").unwrap(), 1);
        assert!(test_exists(&path, b"reinsert").unwrap());
    }
