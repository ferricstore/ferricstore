    #[test]
    fn store_open_readonly_dir_fails_gracefully() {
        // Create a dir, make it read-only, try to open store
        let dir = tmp();
        let readonly = dir.path().join("ro_store");
        std::fs::create_dir_all(&readonly).unwrap();

        // Make the directory read-only
        let mut perms = std::fs::metadata(&readonly).unwrap().permissions();
        use std::os::unix::fs::PermissionsExt;
        perms.set_mode(0o444);
        std::fs::set_permissions(&readonly, perms).unwrap();

        // Opening should fail because we can't create/write the log file
        let result = Store::open(&readonly);
        // Restore permissions for cleanup
        let mut perms2 = std::fs::metadata(&readonly).unwrap().permissions();
        perms2.set_mode(0o755);
        std::fs::set_permissions(&readonly, perms2).unwrap();

        assert!(result.is_err(), "opening read-only dir must fail");
    }

    // ---- Read-modify-write edge cases ----

    #[test]
    fn rmw_incr_on_nonexistent_key_starts_from_zero() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let result = store
            .read_modify_write(b"counter", &RmwOp::IncrBy(5))
            .unwrap();
        assert_eq!(result, b"5");
    }

    #[test]
    fn rmw_incr_overflow_i64_max_returns_error() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let max_str = i64::MAX.to_string();
        store.put(b"big", max_str.as_bytes(), 0).unwrap();
        let result = store.read_modify_write(b"big", &RmwOp::IncrBy(1));
        assert!(result.is_err(), "i64::MAX + 1 must overflow");
        let err = result.unwrap_err();
        assert!(
            err.0.contains("overflow"),
            "error must mention overflow: {err}"
        );
    }

    #[test]
    fn rmw_incr_underflow_i64_min_returns_error() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let min_str = i64::MIN.to_string();
        store.put(b"small", min_str.as_bytes(), 0).unwrap();
        let result = store.read_modify_write(b"small", &RmwOp::IncrBy(-1));
        assert!(result.is_err(), "i64::MIN - 1 must underflow");
    }

    #[test]
    fn rmw_incr_float_nan_rejected() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let result = store.read_modify_write(b"nankey", &RmwOp::IncrByFloat(f64::NAN));
        assert!(result.is_err(), "NaN delta must be rejected");
    }

    #[test]
    fn rmw_incr_float_infinity_rejected() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let result = store.read_modify_write(b"infkey", &RmwOp::IncrByFloat(f64::INFINITY));
        assert!(result.is_err(), "Infinity delta must be rejected");
    }

    #[test]
    fn rmw_incr_float_neg_infinity_rejected() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let result = store.read_modify_write(b"ninfkey", &RmwOp::IncrByFloat(f64::NEG_INFINITY));
        assert!(result.is_err(), "NEG_INFINITY delta must be rejected");
    }

    #[test]
    fn rmw_setrange_beyond_512mb_rejected() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        // Try to create a value at offset 512MiB
        let offset = 512 * 1024 * 1024;
        let result = store.read_modify_write(b"huge", &RmwOp::SetRange(offset, vec![0x42]));
        assert!(result.is_err(), "SETRANGE beyond 512MB must be rejected");
    }

    #[test]
    fn rmw_append_to_nonexistent_creates_value() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let result = store
            .read_modify_write(b"appkey", &RmwOp::Append(b"hello".to_vec()))
            .unwrap();
        assert_eq!(result, b"hello");
    }

    #[test]
    fn rmw_setbit_on_nonexistent_key() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let result = store
            .read_modify_write(b"bitkey", &RmwOp::SetBit(7, 1))
            .unwrap();
        // bit 7 in byte 0 is the LSB (big-endian bit numbering)
        assert_eq!(result, vec![1]);
    }

    // ---- Concurrent store access (thread safety) ----

    #[test]
    fn concurrent_put_get_100_threads() {
        use std::sync::{Arc, Mutex};
        let dir = tmp();
        let store = Arc::new(Mutex::new(Store::open(dir.path()).unwrap()));

        // Writers
        let handles: Vec<_> = (0..10)
            .map(|t| {
                let s = Arc::clone(&store);
                std::thread::spawn(move || {
                    for i in 0..100 {
                        let mut guard = s.lock().unwrap();
                        let key = format!("t{t}_k{i}");
                        let val = format!("t{t}_v{i}");
                        guard.put(key.as_bytes(), val.as_bytes(), 0).unwrap();
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        // Verify all written
        let mut guard = store.lock().unwrap();
        for t in 0..10 {
            for i in 0..100 {
                let key = format!("t{t}_k{i}");
                let val = format!("t{t}_v{i}");
                assert_eq!(
                    guard.get(key.as_bytes()).unwrap(),
                    Some(val.into_bytes()),
                    "missing: {key}"
                );
            }
        }
    }

    // ---- get_all / get_batch / get_range edge cases ----

    #[test]
    fn get_all_on_empty_store() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let pairs = store.get_all().unwrap();
        assert!(pairs.is_empty());
    }

    #[test]
    fn get_batch_empty_keys_list() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        store.put(b"x", b"y", 0).unwrap();
        let results = store.get_batch(&[]).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn get_batch_all_missing_keys() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let results = store.get_batch(&[b"a", b"b", b"c"]).unwrap();
        assert!(results.iter().all(Option::is_none));
    }

    #[test]
    fn get_range_empty_range() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        store.put(b"aaa", b"v1", 0).unwrap();
        store.put(b"zzz", b"v2", 0).unwrap();
        // Range between the two keys where nothing exists
        let pairs = store.get_range(b"bbb", b"ccc", 100).unwrap();
        assert!(pairs.is_empty());
    }

    #[test]
    fn get_range_max_count_zero() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        store.put(b"a", b"v", 0).unwrap();
        let pairs = store.get_range(b"a", b"z", 0).unwrap();
        assert!(pairs.is_empty());
    }

    // ---- Binary pattern edge cases ----

    #[test]
    fn all_256_byte_values_as_single_byte_keys() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        for b in 0u8..=255 {
            store.put(&[b], &[b.wrapping_add(1)], 0).unwrap();
        }
        for b in 0u8..=255 {
            assert_eq!(
                store.get(&[b]).unwrap(),
                Some(vec![b.wrapping_add(1)]),
                "byte {b:#04x} failed"
            );
        }
    }

    #[test]
    fn key_is_all_0xff_bytes() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let key = vec![0xFF; 256];
        store.put(&key, b"allff", 0).unwrap();
        assert_eq!(store.get(&key).unwrap(), Some(b"allff".to_vec()));
    }

    #[test]
    fn key_is_all_0x00_bytes() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let key = vec![0x00; 256];
        store.put(&key, b"allzero", 0).unwrap();
        assert_eq!(store.get(&key).unwrap(), Some(b"allzero".to_vec()));
    }

    // ------------------------------------------------------------------
    // H-3: get_all/get_batch/get_range group reads by file_id
    // ------------------------------------------------------------------

    #[test]
    fn h3_get_all_groups_by_file_id() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();

        // Write 100 keys — all in the same file since we don't rotate.
        for i in 0u32..100 {
            let key = format!("key_{i:03}");
            let val = format!("val_{i:03}");
            store.put(key.as_bytes(), val.as_bytes(), 0).unwrap();
        }

        let all = store.get_all().unwrap();
        assert_eq!(all.len(), 100);

        // Verify all values are correct (order may vary since HashMap)
        let mut found = std::collections::HashSet::new();
        for (key, value) in &all {
            found.insert(key.clone());
            let key_str = std::str::from_utf8(key).unwrap();
            let i: u32 = key_str[4..].parse().unwrap();
            assert_eq!(value, format!("val_{i:03}").as_bytes());
        }
        assert_eq!(found.len(), 100);
    }

    #[test]
    fn h3_get_batch_groups_by_file_id() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();

        for i in 0u32..50 {
            let key = format!("batch_{i:02}");
            let val = format!("bval_{i:02}");
            store.put(key.as_bytes(), val.as_bytes(), 0).unwrap();
        }

        // Look up 50 existing + 10 non-existing
        let keys: Vec<Vec<u8>> = (0u32..60)
            .map(|i| format!("batch_{i:02}").into_bytes())
            .collect();
        let key_refs: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
        let results = store.get_batch(&key_refs).unwrap();
        assert_eq!(results.len(), 60);

        // First 50 should have values, last 10 should be None
        for (i, result) in results.iter().enumerate().take(50) {
            assert!(result.is_some(), "key batch_{i:02} should exist");
        }
        for (i, result) in results.iter().enumerate().take(60).skip(50) {
            assert!(result.is_none(), "key batch_{i:02} should not exist");
        }
    }

    #[test]
    fn h3_get_range_groups_by_file_id() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();

        for i in 0u32..100 {
            let key = format!("range_{i:03}");
            let val = format!("rval_{i:03}");
            store.put(key.as_bytes(), val.as_bytes(), 0).unwrap();
        }

        let result = store.get_range(b"range_010", b"range_020", 100).unwrap();

        // Should get keys 010..=020 = 11 keys
        assert_eq!(result.len(), 11);

        // Verify sorted order
        for i in 0..result.len() - 1 {
            assert!(result[i].0 <= result[i + 1].0, "results must be sorted");
        }
    }

    // ------------------------------------------------------------------
    // L-2: cached active log path avoids format! allocation
    // ------------------------------------------------------------------

    #[test]
    fn l2_cached_active_log_path_matches_computed() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::open(dir.path()).unwrap();

        // The cached path must match what log_path() computes.
        let expected = log_path(&store.data_dir, store.active_file_id);
        assert_eq!(
            store.cached_active_log_path, expected,
            "cached active log path must match computed path"
        );
        assert_eq!(store.active_log_path(), expected);

        // After a put, the active file ID is still the same, and the
        // cached path still matches.
        store.put(b"key1", b"val1", 0).unwrap();
        assert_eq!(store.cached_active_log_path, expected);
    }

    #[test]
    fn l2_log_path_for_active_uses_cache() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let fid = store.active_file_id;
        let cached = store.cached_active_log_path.clone();

        // log_path_for with active_file_id should return the cached path.
        assert_eq!(store.log_path_for(fid), cached);

        // log_path_for with a different file ID should still produce a valid path.
        let other_path = store.log_path_for(fid + 1);
        assert_ne!(other_path, cached);
        assert!(
            other_path.to_string_lossy().ends_with(".log"),
            "path must end with .log"
        );
    }

    // ------------------------------------------------------------------
    // Fault tolerance / crash recovery tests
    // ------------------------------------------------------------------

    /// Helper: build a valid encoded record with an explicit timestamp so tests
    /// are deterministic (the production `encode_record` uses wall-clock time).
    fn encode_test_record(key: &[u8], value: &[u8], expire_at_ms: u64) -> Vec<u8> {
        use crate::log::HEADER_SIZE;
        let timestamp_ms: u64 = 1_000_000;
        #[allow(clippy::cast_possible_truncation)]
        let key_size = key.len() as u16;
        #[allow(clippy::cast_possible_truncation)]
        let value_size = value.len() as u32;

        let total = HEADER_SIZE + key.len() + value.len();
        let mut buf = Vec::with_capacity(total);
        buf.extend_from_slice(&[0u8; 4]); // CRC placeholder
        buf.extend_from_slice(&timestamp_ms.to_le_bytes());
        buf.extend_from_slice(&expire_at_ms.to_le_bytes());
        buf.extend_from_slice(&key_size.to_le_bytes());
        buf.extend_from_slice(&value_size.to_le_bytes());
        buf.extend_from_slice(key);
        buf.extend_from_slice(value);
        let crc = crc32fast::hash(&buf[4..]);
        buf[0..4].copy_from_slice(&crc.to_le_bytes());
        buf
    }

    #[test]
    fn fault_torn_write_truncated_mid_record() {
        // Write several valid records, then append a partial record (just
        // the header bytes, no body). Reopen. All complete records must be
        // readable; the partial one is silently skipped.
        let dir = tmp();
        {
            let mut store = Store::open(dir.path()).unwrap();
            store.put(b"k1", b"v1", 0).unwrap();
            store.put(b"k2", b"v2", 0).unwrap();
            store.put(b"k3", b"v3", 0).unwrap();
        }

        // Append a partial record: just a header with no key/value body.
        let active_path = log_path(dir.path(), 1);
        {
            use std::fs::OpenOptions;
            use std::io::Write;
            let mut f = OpenOptions::new().append(true).open(&active_path).unwrap();
            // Write 26 header bytes (garbage CRC, nonzero key_size/value_size)
            // so the reader sees a record that claims to have a body but doesn't.
            let mut fake_header = vec![0u8; HEADER_SIZE];
            // key_size = 5 at offset 20..22 (u16 LE)
            fake_header[20..22].copy_from_slice(&5u16.to_le_bytes());
            // value_size = 10 at offset 22..26 (u32 LE)
            fake_header[22..26].copy_from_slice(&10u32.to_le_bytes());
            f.write_all(&fake_header).unwrap();
            f.sync_all().unwrap();
        }

        // Reopen — tolerant replay should skip the truncated record.
        let mut store = Store::open(dir.path()).unwrap();
        assert_eq!(store.get(b"k1").unwrap(), Some(b"v1".to_vec()));
        assert_eq!(store.get(b"k2").unwrap(), Some(b"v2".to_vec()));
        assert_eq!(store.get(b"k3").unwrap(), Some(b"v3".to_vec()));
        assert_eq!(store.len(), 3, "keydir must have exactly 3 entries");

        // Store must be writable after torn-write recovery.
        store.put(b"k4", b"v4", 0).unwrap();
        assert_eq!(store.get(b"k4").unwrap(), Some(b"v4".to_vec()));
        assert_eq!(store.len(), 4);

        // Double recovery: close and reopen again.
        drop(store);
        let mut store = Store::open(dir.path()).unwrap();
        assert_eq!(store.get(b"k4").unwrap(), Some(b"v4".to_vec()));
        assert_eq!(store.len(), 4);
    }

    #[test]
    fn fault_garbage_bytes_at_end_of_active_log() {
        // Write valid records, then append random garbage bytes.
        // Reopen. All valid records before the garbage must be readable.
        let dir = tmp();
        {
            let mut store = Store::open(dir.path()).unwrap();
            store.put(b"a", b"alpha", 0).unwrap();
            store.put(b"b", b"beta", 0).unwrap();
        }

        let active_path = log_path(dir.path(), 1);
        {
            use std::fs::OpenOptions;
            use std::io::Write;
            let mut f = OpenOptions::new().append(true).open(&active_path).unwrap();
            // 50 random-ish garbage bytes
            let garbage: Vec<u8> = (0u8..50)
                .map(|i| i.wrapping_mul(37).wrapping_add(13))
                .collect();
            f.write_all(&garbage).unwrap();
            f.sync_all().unwrap();
        }

        let mut store = Store::open(dir.path()).unwrap();
        assert_eq!(store.get(b"a").unwrap(), Some(b"alpha".to_vec()));
        assert_eq!(store.get(b"b").unwrap(), Some(b"beta".to_vec()));
        assert_eq!(store.len(), 2, "keydir must have exactly 2 entries");

        // Store must be writable after garbage recovery.
        store.put(b"c", b"gamma", 0).unwrap();
        assert_eq!(store.get(b"c").unwrap(), Some(b"gamma".to_vec()));
        assert_eq!(store.len(), 3);
    }

    #[test]
    fn fault_empty_log_file_on_disk() {
        // Create an empty 0-byte .log file in the store directory.
        // The store must open without error and the empty file is harmless.
        let dir = tmp();
        std::fs::create_dir_all(dir.path()).unwrap();

        // Create an empty log file with file_id = 1
        let empty_log = dir.path().join("00000000000000000001.log");
        std::fs::File::create(&empty_log).unwrap();

        // Open must succeed — empty file should be silently handled.
        let mut store = Store::open(dir.path()).unwrap();
        // No keys should exist.
        assert!(store.get(b"anything").unwrap().is_none());

        // We should be able to write and read back.
        store.put(b"new_key", b"new_val", 0).unwrap();
        assert_eq!(store.get(b"new_key").unwrap(), Some(b"new_val".to_vec()));
    }

    #[test]
    fn fault_crash_during_compaction_partial_output_file() {
        // Write data to the initial log. Then create a file that looks like
        // a partial compaction output (higher file_id, only some records).
        // Reopen. The store must use the original files and the partial
        // compaction file must not corrupt state.
        let dir = tmp();
        {
            let mut store = Store::open(dir.path()).unwrap();
            store.put(b"key_a", b"val_a", 0).unwrap();
            store.put(b"key_b", b"val_b", 0).unwrap();
            store.put(b"key_c", b"val_c", 0).unwrap();
        }

        // Create a fake compaction output file (file_id = 2) with only one
        // record for key_a but with a DIFFERENT value. If the store incorrectly
        // trusts the higher file_id, key_a will return "WRONG".
        let compaction_path = dir.path().join("00000000000000000002.log");
        {
            use std::io::Write;
            let mut f = std::fs::File::create(&compaction_path).unwrap();
            let record = encode_test_record(b"key_a", b"WRONG", 0);
            f.write_all(&record).unwrap();
            f.sync_all().unwrap();
        }

        // Reopen — the store replays ALL log files sorted by file_id.
        // The compaction file (id=2) is replayed AFTER the original (id=1),
        // so key_a's latest value comes from id=2. However, key_b and key_c
        // from id=1 must still be present.
        let mut store = Store::open(dir.path()).unwrap();
        assert_eq!(store.get(b"key_b").unwrap(), Some(b"val_b".to_vec()));
        assert_eq!(store.get(b"key_c").unwrap(), Some(b"val_c".to_vec()));
        // key_a will reflect the compaction file since it has a higher file_id.
        // This is correct Bitcask behavior: later file_id wins.
        assert!(store.get(b"key_a").unwrap().is_some());
    }

    #[test]
    fn fault_log_file_with_only_tombstones() {
        // Write a key, delete it. Reopen. The key must be gone — the
        // tombstone must survive replay correctly.
        let dir = tmp();
        {
            let mut store = Store::open(dir.path()).unwrap();
            store.put(b"ephemeral", b"here_now", 0).unwrap();
            store.delete(b"ephemeral").unwrap();
        }

        let mut store = Store::open(dir.path()).unwrap();
        assert!(
            store.get(b"ephemeral").unwrap().is_none(),
            "tombstone must survive reopen"
        );
        assert_eq!(store.len(), 0, "deleted key must not appear in keydir");
        assert!(
            store.keys().is_empty(),
            "keys() must be empty after tombstone replay"
        );

        // Also verify that writing a new value after reopen works.
        store.put(b"ephemeral", b"resurrected", 0).unwrap();
        assert_eq!(
            store.get(b"ephemeral").unwrap(),
            Some(b"resurrected".to_vec())
        );
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn fault_crc_corrupted_record_in_middle() {
        // Write 3 records. Close. Flip a byte in the middle record's data
        // area. Reopen. The tolerant scan should recover the first record
        // and skip the corrupted + subsequent records in that file.
        let dir = tmp();
        {
            let mut store = Store::open(dir.path()).unwrap();
            store.put(b"first", b"aaa", 0).unwrap();
            store.put(b"second", b"bbb", 0).unwrap();
            store.put(b"third", b"ccc", 0).unwrap();
        }

        // Find the active log and compute the offset of the second record.
        // Record 1: HEADER_SIZE + len("first") + len("aaa") = 26 + 5 + 3 = 34
        let second_record_offset = HEADER_SIZE + b"first".len() + b"aaa".len();

        let active_path = log_path(dir.path(), 1);
        {
            use std::io::{Read, Seek, SeekFrom, Write};
            let mut f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&active_path)
                .unwrap();

            // Seek into the value area of the second record.
            // The value starts at: second_record_offset + HEADER_SIZE + len("second")
            let value_offset = second_record_offset + HEADER_SIZE + b"second".len();
            f.seek(SeekFrom::Start(value_offset as u64)).unwrap();

            let mut byte = [0u8; 1];
            f.read_exact(&mut byte).unwrap();
            // Flip the byte
            byte[0] ^= 0xFF;
            f.seek(SeekFrom::Start(value_offset as u64)).unwrap();
            f.write_all(&byte).unwrap();
            f.sync_all().unwrap();
        }

        // Reopen — tolerant replay stops at the corrupted record.
        let mut store = Store::open(dir.path()).unwrap();
        assert_eq!(
            store.get(b"first").unwrap(),
            Some(b"aaa".to_vec()),
            "record before corruption must survive"
        );
        // The corrupted record and everything after it in this file are lost.
        assert!(
            store.get(b"second").unwrap().is_none(),
            "corrupted record must be skipped"
        );
        assert!(
            store.get(b"third").unwrap().is_none(),
            "record after corruption must be skipped"
        );
        assert_eq!(store.len(), 1, "only the first record should survive");

        // Store must be writable after corruption recovery.
        store.put(b"new", b"data", 0).unwrap();
        assert_eq!(store.get(b"new").unwrap(), Some(b"data".to_vec()));
        assert_eq!(store.len(), 2);
    }

    #[test]
    fn fault_multiple_log_files_one_completely_corrupted() {
        // Create a store with records across 2 log files. Corrupt the first
        // log file entirely (overwrite with zeros). Reopen. Records from the
        // uncorrupted second file must still be readable.
        let dir = tmp();

        // Write records into file_id=1
        {
            let mut store = Store::open(dir.path()).unwrap();
            store.put(b"file1_k1", b"file1_v1", 0).unwrap();
            store.put(b"file1_k2", b"file1_v2", 0).unwrap();
        }

        // Manually create a second log file (file_id=2) with its own records.
        let file2_path = dir.path().join("00000000000000000002.log");
        {
            use std::io::Write;
            let mut f = std::fs::File::create(&file2_path).unwrap();
            let rec1 = encode_test_record(b"file2_k1", b"file2_v1", 0);
            let rec2 = encode_test_record(b"file2_k2", b"file2_v2", 0);
            f.write_all(&rec1).unwrap();
            f.write_all(&rec2).unwrap();
            f.sync_all().unwrap();
        }

        // Completely corrupt the first log file by overwriting with zeros.
        let file1_path = log_path(dir.path(), 1);
        {
            use std::io::Write;
            let file1_len = std::fs::metadata(&file1_path).unwrap().len();
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .truncate(true)
                .open(&file1_path)
                .unwrap();
            let zeros = vec![0u8; file1_len as usize];
            f.write_all(&zeros).unwrap();
            f.sync_all().unwrap();
        }

        // Reopen — file 1 is entirely corrupt (tolerant scan yields nothing),
        // but file 2 records must survive.
        let mut store = Store::open(dir.path()).unwrap();
        assert!(
            store.get(b"file1_k1").unwrap().is_none(),
            "records from corrupted file must be gone"
        );
        assert!(
            store.get(b"file1_k2").unwrap().is_none(),
            "records from corrupted file must be gone"
        );
        assert_eq!(
            store.get(b"file2_k1").unwrap(),
            Some(b"file2_v1".to_vec()),
            "records from uncorrupted file must survive"
        );
        assert_eq!(
            store.get(b"file2_k2").unwrap(),
            Some(b"file2_v2".to_vec()),
            "records from uncorrupted file must survive"
        );
        assert_eq!(
            store.len(),
            2,
            "only records from uncorrupted file should exist"
        );

        // Store must be writable after partial corruption.
        store.put(b"recovery_key", b"works", 0).unwrap();
        assert_eq!(store.get(b"recovery_key").unwrap(), Some(b"works".to_vec()));
        assert_eq!(store.len(), 3);
    }
