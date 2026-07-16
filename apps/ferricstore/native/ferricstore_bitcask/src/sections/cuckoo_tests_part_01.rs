    use super::*;

    #[test]
    fn file_create_and_read_header() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.cuckoo");
        let path_str = path.to_str().unwrap().to_string();

        // Create file manually (same logic as NIF but without Env).
        let capacity: u32 = 1024;
        let bucket_size: u8 = 4;
        let fingerprint_size: u8 = FILE_DEFAULT_FINGERPRINT_SIZE as u8;
        let max_kicks = FILE_DEFAULT_MAX_KICKS;
        let bucket_bytes =
            (capacity as usize) * (bucket_size as usize) * (fingerprint_size as usize);
        let file_size = HEADER_SIZE + bucket_bytes;

        let mut file = File::create(&path).unwrap();
        let mut header = [0u8; HEADER_SIZE];
        header[0..2].copy_from_slice(&MAGIC);
        header[2] = VERSION;
        header[3..7].copy_from_slice(&capacity.to_le_bytes());
        header[7] = bucket_size;
        header[8] = fingerprint_size;
        header[9..11].copy_from_slice(&max_kicks.to_le_bytes());

        let mut buf = Vec::with_capacity(file_size);
        buf.extend_from_slice(&header);
        buf.resize(file_size, 0);
        file.write_all(&buf).unwrap();
        file.sync_all().unwrap();
        drop(file);

        // Read back and validate header.
        let file = cuckoo_file_open_read(&path_str).unwrap();
        let hdr = cuckoo_read_header(&file).unwrap();
        assert_eq!(hdr.num_buckets, 1024);
        assert_eq!(hdr.bucket_size, 4);
        assert_eq!(hdr.fingerprint_size, FILE_DEFAULT_FINGERPRINT_SIZE as u8);
        assert_eq!(hdr.num_items, 0);
        assert_eq!(hdr.num_deletes, 0);
    }

    #[test]
    fn file_slot_read_write_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("slot.cuckoo");
        let path_str = path.to_str().unwrap().to_string();

        let capacity: u32 = 64;
        let bucket_size: u8 = 4;
        let fingerprint_size: u8 = 1;
        let bucket_bytes =
            (capacity as usize) * (bucket_size as usize) * (fingerprint_size as usize);
        let file_size = HEADER_SIZE + bucket_bytes;

        let mut file = File::create(&path).unwrap();
        let mut header = [0u8; HEADER_SIZE];
        header[0..2].copy_from_slice(&MAGIC);
        header[2] = VERSION;
        header[3..7].copy_from_slice(&capacity.to_le_bytes());
        header[7] = bucket_size;
        header[8] = fingerprint_size;
        header[9..11].copy_from_slice(&FILE_DEFAULT_MAX_KICKS.to_le_bytes());
        let mut buf = Vec::with_capacity(file_size);
        buf.extend_from_slice(&header);
        buf.resize(file_size, 0);
        file.write_all(&buf).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let file = cuckoo_file_open_rw(&path_str).unwrap();

        // Write a fingerprint and read it back.
        let fp = vec![0x42u8];
        cuckoo_file_write_slot(&file, 3, 2, bucket_size, fingerprint_size, &fp).unwrap();
        let read_back = cuckoo_file_read_slot(&file, 3, 2, bucket_size, fingerprint_size).unwrap();
        assert_eq!(read_back, fp);

        // Verify other slots are still empty.
        let empty = cuckoo_file_read_slot(&file, 3, 0, bucket_size, fingerprint_size).unwrap();
        assert!(empty.iter().all(|&b| b == 0));
    }

    #[test]
    fn file_fingerprint_never_zero() {
        for i in 0..10_000 {
            let (fp, _) = cuckoo_file_fingerprint_and_bucket(
                format!("elem_{i}").as_bytes(),
                FILE_DEFAULT_FINGERPRINT_SIZE,
                1024,
            );
            assert!(
                !fp.iter().all(|&b| b == 0),
                "fingerprint was all zeros for elem_{i}"
            );
        }
    }

    #[test]
    fn file_alternate_bucket_is_involution() {
        // alt(alt(b, fp)) == b  (the cuckoo property)
        for num_buckets in [1, 3, 10, 127, 1024] {
            for i in 0..1000 {
                let elem = format!("invol_{num_buckets}_{i}");
                let (fp, b1) =
                    cuckoo_file_fingerprint_and_bucket(elem.as_bytes(), 1, num_buckets);
                let b2 = cuckoo_file_alternate_bucket(b1, &fp, num_buckets);
                let b1_again = cuckoo_file_alternate_bucket(b2, &fp, num_buckets);
                assert_eq!(
                    b1, b1_again,
                    "alternate_bucket must be an involution for {num_buckets} buckets, elem {i}"
                );
            }
        }
    }

    // -----------------------------------------------------------------------
    // Edge case tests
    // -----------------------------------------------------------------------

    /// Helper: create a valid cuckoo file and return the path string.
    fn create_cuckoo_file(
        dir: &std::path::Path,
        name: &str,
        capacity: u32,
        bucket_size: u8,
    ) -> String {
        let path = dir.join(name);
        let fingerprint_size: u8 = FILE_DEFAULT_FINGERPRINT_SIZE as u8;
        let max_kicks = FILE_DEFAULT_MAX_KICKS;
        let bucket_bytes =
            (capacity as usize) * (bucket_size as usize) * (fingerprint_size as usize);
        let file_size = HEADER_SIZE + bucket_bytes;

        let mut file = File::create(&path).unwrap();
        let mut header = [0u8; HEADER_SIZE];
        header[0..2].copy_from_slice(&MAGIC);
        header[2] = VERSION;
        header[3..7].copy_from_slice(&capacity.to_le_bytes());
        header[7] = bucket_size;
        header[8] = fingerprint_size;
        header[9..11].copy_from_slice(&max_kicks.to_le_bytes());

        let mut buf = Vec::with_capacity(file_size);
        buf.extend_from_slice(&header);
        buf.resize(file_size, 0);
        file.write_all(&buf).unwrap();
        file.sync_all().unwrap();
        path.to_str().unwrap().to_string()
    }

    #[test]
    fn empty_element_fingerprint() {
        // Zero-length element should produce a valid non-zero fingerprint.
        let (fp, bucket) = cuckoo_file_fingerprint_and_bucket(b"", 1, 1024);
        assert!(
            !fp.iter().all(|&b| b == 0),
            "fingerprint should never be all zeros"
        );
        assert!(bucket < 1024);
    }

    #[test]
    fn large_element_fingerprint() {
        // 1MB element should work without panic.
        let big = vec![0xEFu8; 1_000_000];
        let (fp, bucket) = cuckoo_file_fingerprint_and_bucket(&big, 1, 1024);
        assert!(!fp.iter().all(|&b| b == 0));
        assert!(bucket < 1024);
    }

    #[test]
    fn counter_helpers_reject_overflow_and_underflow() {
        assert_eq!(cuckoo_num_items_after_insert(0).unwrap(), 1);
        assert!(cuckoo_num_items_after_insert(u64::MAX).is_err());
        assert_eq!(cuckoo_num_items_after_delete(1).unwrap(), 0);
        assert!(cuckoo_num_items_after_delete(0).is_err());
        assert_eq!(cuckoo_num_deletes_after_delete(0).unwrap(), 1);
        assert!(cuckoo_num_deletes_after_delete(u64::MAX).is_err());
    }

    #[test]
    fn identical_primary_and_alternate_bucket_is_visited_once() {
        assert_eq!(
            cuckoo_file_candidate_buckets(7, 7).collect::<Vec<_>>(),
            vec![7]
        );
        assert_eq!(
            cuckoo_file_candidate_buckets(7, 9).collect::<Vec<_>>(),
            vec![7, 9]
        );
    }

    #[test]
    fn truncated_header_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("truncated.cuckoo");
        std::fs::write(&path, [0u8; 10]).unwrap();
        let file = File::open(&path).unwrap();
        assert!(cuckoo_read_header(&file).is_err());
    }

    #[test]
    fn complete_header_rejects_a_truncated_bucket_region() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("missing_buckets.cuckoo");
        let mut header = [0u8; HEADER_SIZE];
        header[0..2].copy_from_slice(&MAGIC);
        header[2] = VERSION;
        header[3..7].copy_from_slice(&4u32.to_le_bytes());
        header[7] = 2;
        header[8] = 1;
        header[9..11].copy_from_slice(&500u16.to_le_bytes());
        std::fs::write(&path, header).unwrap();

        assert!(cuckoo_read_header(&File::open(path).unwrap()).is_err());
    }

    #[test]
    fn header_rejects_num_items_larger_than_total_slots() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("impossible_items.cuckoo");
        let mut data = [0u8; HEADER_SIZE + 1];
        data[0..2].copy_from_slice(&MAGIC);
        data[2] = VERSION;
        data[3..7].copy_from_slice(&1u32.to_le_bytes());
        data[7] = 1;
        data[8] = 1;
        data[9..11].copy_from_slice(&1u16.to_le_bytes());
        data[11..19].copy_from_slice(&2u64.to_le_bytes());
        std::fs::write(&path, data).unwrap();

        let error = cuckoo_read_header(&File::open(path).unwrap())
            .err()
            .expect("impossible num_items must be rejected");

        assert!(error.contains("num_items"));
    }

    #[test]
    fn read_slot_rejects_truncated_bucket_region() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("short_slot.cuckoo");
        std::fs::write(&path, [0u8; HEADER_SIZE]).unwrap();
        let file = File::open(&path).unwrap();

        let err = cuckoo_file_read_slot(&file, 0, 0, 4, 1).unwrap_err();

        assert!(err.contains("truncated"));
    }

    #[test]
    fn wrong_magic_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad_magic.cuckoo");
        let mut data = [0u8; HEADER_SIZE + 64];
        data[0] = 0xFF;
        data[1] = 0xFF;
        std::fs::write(&path, data).unwrap();
        let file = File::open(&path).unwrap();
        let result = cuckoo_read_header(&file);
        assert!(result.is_err());
        match result {
            Err(msg) => assert!(msg.contains("magic"), "expected magic error, got: {msg}"),
            Ok(_) => panic!("expected error"),
        }
    }

    #[test]
    fn wrong_version_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad_version.cuckoo");
        let mut data = [0u8; HEADER_SIZE + 64];
        data[0..2].copy_from_slice(&MAGIC);
        data[2] = 99; // wrong version
        std::fs::write(&path, data).unwrap();
        let file = File::open(&path).unwrap();
        let result = cuckoo_read_header(&file);
        assert!(result.is_err());
        match result {
            Err(msg) => assert!(
                msg.contains("version"),
                "expected version error, got: {msg}"
            ),
            Ok(_) => panic!("expected error"),
        }
    }

    #[test]
    fn invalid_dimensions_return_error() {
        for (name, num_buckets, bucket_size, fingerprint_size) in [
            ("zero_buckets.cuckoo", 0u32, 1u8, 1u8),
            ("zero_bucket_size.cuckoo", 1u32, 0u8, 1u8),
            ("zero_fingerprint.cuckoo", 1u32, 1u8, 0u8),
            ("oversized_fingerprint.cuckoo", 1u32, 1u8, 9u8),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join(name);
            let mut data = [0u8; HEADER_SIZE + 64];
            data[0..2].copy_from_slice(&MAGIC);
            data[2] = VERSION;
            data[3..7].copy_from_slice(&num_buckets.to_le_bytes());
            data[7] = bucket_size;
            data[8] = fingerprint_size;
            data[9..11].copy_from_slice(&FILE_DEFAULT_MAX_KICKS.to_le_bytes());
            std::fs::write(&path, data).unwrap();
            let file = File::open(&path).unwrap();

            let result = cuckoo_read_header(&file);

            assert!(result.is_err(), "{name} should be rejected");
        }
    }

    #[test]
    fn minimum_capacity_cuckoo() {
        // capacity=1, bucket_size=1 -- smallest possible cuckoo filter
        let dir = tempfile::tempdir().unwrap();
        let path_str = create_cuckoo_file(dir.path(), "min.cuckoo", 1, 1);
        let file = cuckoo_file_open_read(&path_str).unwrap();
        let hdr = cuckoo_read_header(&file).unwrap();
        assert_eq!(hdr.num_buckets, 1);
        assert_eq!(hdr.bucket_size, 1);
        assert_eq!(hdr.num_items, 0);
    }

    #[test]
    fn add_and_exists_roundtrip() {
        // Full roundtrip: create, add an element, check it exists.
        let dir = tempfile::tempdir().unwrap();
        let path_str = create_cuckoo_file(dir.path(), "roundtrip.cuckoo", 64, 4);

        let file = cuckoo_file_open_rw(&path_str).unwrap();
        let hdr = cuckoo_read_header(&file).unwrap();

        let (fp, b1) = cuckoo_file_fingerprint_and_bucket(
            b"hello",
            hdr.fingerprint_size as usize,
            hdr.num_buckets,
        );

        // Write fingerprint to first slot of primary bucket
        cuckoo_file_write_slot(&file, b1, 0, hdr.bucket_size, hdr.fingerprint_size, &fp).unwrap();
        cuckoo_file_write_num_items(&file, 1).unwrap();
        drop(file);

        // Check exists
        let file = cuckoo_file_open_read(&path_str).unwrap();
        let hdr = cuckoo_read_header(&file).unwrap();
        assert_eq!(hdr.num_items, 1);

        let (fp2, b1_2) = cuckoo_file_fingerprint_and_bucket(
            b"hello",
            hdr.fingerprint_size as usize,
            hdr.num_buckets,
        );
        assert_eq!(fp, fp2);
        assert_eq!(b1, b1_2);

        let read_fp =
            cuckoo_file_read_slot(&file, b1, 0, hdr.bucket_size, hdr.fingerprint_size).unwrap();
        assert_eq!(read_fp, fp);
    }

    #[test]
    fn delete_decrements_items() {
        let dir = tempfile::tempdir().unwrap();
        let path_str = create_cuckoo_file(dir.path(), "del.cuckoo", 64, 4);
        let file = cuckoo_file_open_rw(&path_str).unwrap();
        let hdr = cuckoo_read_header(&file).unwrap();

        let (fp, b1) = cuckoo_file_fingerprint_and_bucket(
            b"deleteme",
            hdr.fingerprint_size as usize,
            hdr.num_buckets,
        );

        // Add the element
        cuckoo_file_write_slot(&file, b1, 0, hdr.bucket_size, hdr.fingerprint_size, &fp).unwrap();
        cuckoo_file_write_num_items(&file, 1).unwrap();

        // Delete it
        let empty = vec![0u8; hdr.fingerprint_size as usize];
        cuckoo_file_write_slot(&file, b1, 0, hdr.bucket_size, hdr.fingerprint_size, &empty)
            .unwrap();
        cuckoo_file_write_num_items(&file, 0).unwrap();
        cuckoo_file_write_num_deletes(&file, 1).unwrap();

        // Verify header
        let hdr2 = cuckoo_read_header(&file).unwrap();
        assert_eq!(hdr2.num_items, 0);
        assert_eq!(hdr2.num_deletes, 1);

        // Verify slot is empty
        let read_fp =
            cuckoo_file_read_slot(&file, b1, 0, hdr.bucket_size, hdr.fingerprint_size).unwrap();
        assert!(read_fp.iter().all(|&b| b == 0));
    }

    #[test]
    fn filter_full_when_capacity_1_and_bucket_size_1() {
        // With capacity=1 and bucket_size=1, after inserting 1 element,
        // the next insert may require eviction. With only 1 bucket and 1 slot,
        // the eviction loop should terminate (bounded by max_kicks).
        let dir = tempfile::tempdir().unwrap();
        let path_str = create_cuckoo_file(dir.path(), "full.cuckoo", 1, 1);
        let file = cuckoo_file_open_rw(&path_str).unwrap();
        let hdr = cuckoo_read_header(&file).unwrap();

        // Add first element
        let (fp1, b1) = cuckoo_file_fingerprint_and_bucket(
            b"first",
            hdr.fingerprint_size as usize,
            hdr.num_buckets,
        );
        cuckoo_file_write_slot(&file, b1, 0, hdr.bucket_size, hdr.fingerprint_size, &fp1).unwrap();
        cuckoo_file_write_num_items(&file, 1).unwrap();

        // All slots are now full. The eviction loop is tested via the NIF in
        // Elixir tests. Here we just verify max_kicks is bounded.
        assert_eq!(hdr.max_kicks, FILE_DEFAULT_MAX_KICKS);
        assert!(hdr.max_kicks <= 500);
    }

    #[test]
    fn null_bytes_in_element() {
        let element = b"test\x00with\x00nulls";
        let (fp, bucket) = cuckoo_file_fingerprint_and_bucket(element, 1, 1024);
        assert!(!fp.iter().all(|&b| b == 0));
        assert!(bucket < 1024);

        // Should differ from element without nulls
        let (fp2, bucket2) = cuckoo_file_fingerprint_and_bucket(b"testwithnulls", 1, 1024);
        assert!(
            fp != fp2 || bucket != bucket2,
            "null bytes should affect hash"
        );
    }

    #[test]
    fn nonexistent_file_returns_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let path_str = dir.path().join("nope.cuckoo").to_str().unwrap().to_string();
        match cuckoo_file_open_read(&path_str) {
            Err(FileOpenError::NotFound) => {} // expected
            other => panic!("expected NotFound, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // Test helpers — replicate NIF logic without Env/Term
    // -----------------------------------------------------------------------

    /// Add an element to a cuckoo file. Returns Ok(true) on success,
    /// Err("filter is full") when eviction chain exhausts max_kicks.
    fn test_add(path: &str, element: &[u8]) -> Result<bool, String> {
        let file = cuckoo_file_open_rw(path).map_err(|e| format!("{e:?}"))?;
        let hdr = cuckoo_read_header(&file)?;
        let (fp, b1) = cuckoo_file_fingerprint_and_bucket(
            element,
            hdr.fingerprint_size as usize,
            hdr.num_buckets,
        );
        let b2 = cuckoo_file_alternate_bucket(b1, &fp, hdr.num_buckets);
        let next_num_items = cuckoo_num_items_after_insert(hdr.num_items)?;

        // Try primary bucket.
        for slot in 0..hdr.bucket_size {
            let s = cuckoo_file_read_slot(
                &file,
                b1,
                slot as usize,
                hdr.bucket_size,
                hdr.fingerprint_size,
            )?;
            if s.iter().all(|&b| b == 0) {
                cuckoo_file_write_slot(
                    &file,
                    b1,
                    slot as usize,
                    hdr.bucket_size,
                    hdr.fingerprint_size,
                    &fp,
                )?;
                cuckoo_file_write_num_items(&file, next_num_items)?;
                return Ok(true);
            }
        }

        // Try alternate bucket.
        for slot in 0..hdr.bucket_size {
            let s = cuckoo_file_read_slot(
                &file,
                b2,
                slot as usize,
                hdr.bucket_size,
                hdr.fingerprint_size,
            )?;
            if s.iter().all(|&b| b == 0) {
                cuckoo_file_write_slot(
                    &file,
                    b2,
                    slot as usize,
                    hdr.bucket_size,
                    hdr.fingerprint_size,
                    &fp,
                )?;
                cuckoo_file_write_num_items(&file, next_num_items)?;
                return Ok(true);
            }
        }

        // Cuckoo eviction chain.
        let mut cur_fp = fp;
        let mut cur_bucket = b1;
        for kicks in 0..(hdr.max_kicks as u32) {
            let slot_idx = (kicks as usize) % (hdr.bucket_size as usize);
            let evicted = cuckoo_file_read_slot(
                &file,
                cur_bucket,
                slot_idx,
                hdr.bucket_size,
                hdr.fingerprint_size,
            )?;
            cuckoo_file_write_slot(
                &file,
                cur_bucket,
                slot_idx,
                hdr.bucket_size,
                hdr.fingerprint_size,
                &cur_fp,
            )?;
            let alt = cuckoo_file_alternate_bucket(cur_bucket, &evicted, hdr.num_buckets);
            for slot in 0..hdr.bucket_size {
                let s = cuckoo_file_read_slot(
                    &file,
                    alt,
                    slot as usize,
                    hdr.bucket_size,
                    hdr.fingerprint_size,
                )?;
                if s.iter().all(|&b| b == 0) {
                    cuckoo_file_write_slot(
                        &file,
                        alt,
                        slot as usize,
                        hdr.bucket_size,
                        hdr.fingerprint_size,
                        &evicted,
                    )?;
                    cuckoo_file_write_num_items(&file, next_num_items)?;
                    return Ok(true);
                }
            }
            cur_fp = evicted;
            cur_bucket = alt;
        }
        Err("filter is full".into())
    }

    /// Check if an element exists. Returns Ok(true/false).
    fn test_exists(path: &str, element: &[u8]) -> Result<bool, String> {
        let file = cuckoo_file_open_read(path).map_err(|e| format!("{e:?}"))?;
        let hdr = cuckoo_read_header(&file)?;
        let (fp, b1) = cuckoo_file_fingerprint_and_bucket(
            element,
            hdr.fingerprint_size as usize,
            hdr.num_buckets,
        );
        let b2 = cuckoo_file_alternate_bucket(b1, &fp, hdr.num_buckets);
        for bucket in &[b1, b2] {
            for slot in 0..hdr.bucket_size {
                let s = cuckoo_file_read_slot(
                    &file,
                    *bucket,
                    slot as usize,
                    hdr.bucket_size,
                    hdr.fingerprint_size,
                )?;
                if s == fp {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Delete one occurrence. Returns Ok(true) if deleted, Ok(false) if not found.
    fn test_del(path: &str, element: &[u8]) -> Result<bool, String> {
        let file = cuckoo_file_open_rw(path).map_err(|e| format!("{e:?}"))?;
        let hdr = cuckoo_read_header(&file)?;
        let (fp, b1) = cuckoo_file_fingerprint_and_bucket(
            element,
            hdr.fingerprint_size as usize,
            hdr.num_buckets,
        );
        let b2 = cuckoo_file_alternate_bucket(b1, &fp, hdr.num_buckets);
        let empty = vec![0u8; hdr.fingerprint_size as usize];
        for bucket in &[b1, b2] {
            for slot in 0..hdr.bucket_size {
                let s = cuckoo_file_read_slot(
                    &file,
                    *bucket,
                    slot as usize,
                    hdr.bucket_size,
                    hdr.fingerprint_size,
                )?;
                if s == fp {
                    let next_num_items = cuckoo_num_items_after_delete(hdr.num_items)?;
                    let next_num_deletes = cuckoo_num_deletes_after_delete(hdr.num_deletes)?;
                    cuckoo_file_write_slot(
                        &file,
                        *bucket,
                        slot as usize,
                        hdr.bucket_size,
                        hdr.fingerprint_size,
                        &empty,
                    )?;
                    cuckoo_file_write_num_items(&file, next_num_items)?;
                    cuckoo_file_write_num_deletes(&file, next_num_deletes)?;
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Add only if not exists. Returns Ok(1) if added, Ok(0) if already present.
    #[allow(clippy::too_many_lines)]
    fn test_addnx(path: &str, element: &[u8]) -> Result<u64, String> {
        let file = cuckoo_file_open_rw(path).map_err(|e| format!("{e:?}"))?;
        let hdr = cuckoo_read_header(&file)?;
        let (fp, b1) = cuckoo_file_fingerprint_and_bucket(
            element,
            hdr.fingerprint_size as usize,
            hdr.num_buckets,
        );
        let b2 = cuckoo_file_alternate_bucket(b1, &fp, hdr.num_buckets);

        // Check existence first.
        for bucket in &[b1, b2] {
            for slot in 0..hdr.bucket_size {
                let s = cuckoo_file_read_slot(
                    &file,
                    *bucket,
                    slot as usize,
                    hdr.bucket_size,
                    hdr.fingerprint_size,
                )?;
                if s == fp {
                    return Ok(0);
                }
            }
        }

        let next_num_items = cuckoo_num_items_after_insert(hdr.num_items)?;

        // Not found — insert (try primary, then alternate, then eviction).
        for slot in 0..hdr.bucket_size {
            let s = cuckoo_file_read_slot(
                &file,
                b1,
                slot as usize,
                hdr.bucket_size,
                hdr.fingerprint_size,
            )?;
            if s.iter().all(|&b| b == 0) {
                cuckoo_file_write_slot(
                    &file,
                    b1,
                    slot as usize,
                    hdr.bucket_size,
                    hdr.fingerprint_size,
                    &fp,
                )?;
                cuckoo_file_write_num_items(&file, next_num_items)?;
                return Ok(1);
            }
        }
        for slot in 0..hdr.bucket_size {
            let s = cuckoo_file_read_slot(
                &file,
                b2,
                slot as usize,
                hdr.bucket_size,
                hdr.fingerprint_size,
            )?;
            if s.iter().all(|&b| b == 0) {
                cuckoo_file_write_slot(
                    &file,
                    b2,
                    slot as usize,
                    hdr.bucket_size,
                    hdr.fingerprint_size,
                    &fp,
                )?;
                cuckoo_file_write_num_items(&file, next_num_items)?;
                return Ok(1);
            }
        }

        // Eviction chain.
        let mut cur_fp = fp;
        let mut cur_bucket = b1;
        for kicks in 0..(hdr.max_kicks as u32) {
            let slot_idx = (kicks as usize) % (hdr.bucket_size as usize);
            let evicted = cuckoo_file_read_slot(
                &file,
                cur_bucket,
                slot_idx,
                hdr.bucket_size,
                hdr.fingerprint_size,
            )?;
            cuckoo_file_write_slot(
                &file,
                cur_bucket,
                slot_idx,
                hdr.bucket_size,
                hdr.fingerprint_size,
                &cur_fp,
            )?;
            let alt = cuckoo_file_alternate_bucket(cur_bucket, &evicted, hdr.num_buckets);
            for slot in 0..hdr.bucket_size {
                let s = cuckoo_file_read_slot(
                    &file,
                    alt,
                    slot as usize,
                    hdr.bucket_size,
                    hdr.fingerprint_size,
                )?;
                if s.iter().all(|&b| b == 0) {
                    cuckoo_file_write_slot(
                        &file,
                        alt,
                        slot as usize,
                        hdr.bucket_size,
                        hdr.fingerprint_size,
                        &evicted,
                    )?;
                    cuckoo_file_write_num_items(&file, next_num_items)?;
                    return Ok(1);
                }
            }
            cur_fp = evicted;
            cur_bucket = alt;
        }
        Err("filter is full".into())
    }

    /// Count occurrences of an element's fingerprint.
    fn test_count(path: &str, element: &[u8]) -> Result<u64, String> {
        let file = cuckoo_file_open_read(path).map_err(|e| format!("{e:?}"))?;
        let hdr = cuckoo_read_header(&file)?;
        let (fp, b1) = cuckoo_file_fingerprint_and_bucket(
            element,
            hdr.fingerprint_size as usize,
            hdr.num_buckets,
        );
        let b2 = cuckoo_file_alternate_bucket(b1, &fp, hdr.num_buckets);
        let mut total = 0u64;
        for bucket in &[b1, b2] {
            for slot in 0..hdr.bucket_size {
                let s = cuckoo_file_read_slot(
                    &file,
                    *bucket,
                    slot as usize,
                    hdr.bucket_size,
                    hdr.fingerprint_size,
                )?;
                if s == fp {
                    total += 1;
                }
            }
        }
        Ok(total)
    }

    type InfoTuple = (u64, u64, u64, u64, u64, u64, u64);

    /// Read filter info. Returns (num_buckets, bucket_size, fp_size, num_items, num_deletes, total_slots, max_kicks).
    fn test_info(path: &str) -> Result<InfoTuple, String> {
        let file = cuckoo_file_open_read(path).map_err(|e| format!("{e:?}"))?;
        let hdr = cuckoo_read_header(&file)?;
        let total_slots = (hdr.num_buckets as u64) * (hdr.bucket_size as u64);
        Ok((
            hdr.num_buckets as u64,
            hdr.bucket_size as u64,
            hdr.fingerprint_size as u64,
            hdr.num_items,
            hdr.num_deletes,
            total_slots,
            hdr.max_kicks as u64,
        ))
    }

    // -----------------------------------------------------------------------
    // Full integration tests using test helpers
    // -----------------------------------------------------------------------
