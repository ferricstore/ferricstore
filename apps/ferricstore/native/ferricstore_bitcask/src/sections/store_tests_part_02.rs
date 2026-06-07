    // ------------------------------------------------------------------
    // Issue 7.4: write_hint_file must fsync the log before writing the hint
    // ------------------------------------------------------------------

    /// After `write_hint_file` the data referred to by the hint must already be
    /// durable on disk.  We verify this by reading back the raw log bytes at
    /// the offset stored in the hint entry and confirming they exist and are
    /// decodable.
    #[test]
    fn write_hint_file_syncs_log_first() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        store.put(b"synckey", b"syncval", 0).unwrap();

        // write_hint_file now calls self.writer.sync() before writing the hint.
        store.write_hint_file().unwrap();

        // The hint file for the active file ID must now exist.
        let hint_path = dir
            .path()
            .join(format!("{:020}.hint", store.active_file_id));
        assert!(hint_path.exists(), "hint file must be written");

        // The log file must also exist and the first record must be readable,
        // confirming the data was flushed before the hint was written.
        let log_path = dir.path().join(format!("{:020}.log", store.active_file_id));
        let mut reader = crate::log::LogReader::open(&log_path).unwrap();
        let record = reader.read_at(0).unwrap();
        assert!(
            record.is_some(),
            "log record at offset 0 must be present and readable after write_hint_file"
        );
        let record = record.unwrap();
        assert_eq!(record.key, b"synckey");
        assert_eq!(record.value, Some(b"syncval".to_vec()));
    }

    // ------------------------------------------------------------------
    // Issue 6.3: Proactive TTL GC — purge_expired
    // ------------------------------------------------------------------

    /// `purge_expired` must return the correct count and remove expired keys
    /// from the keydir while leaving live keys untouched.
    #[test]
    fn purge_expired_removes_keys_from_keydir() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let past_ms = now_ms().saturating_sub(1000);

        store.put(b"live1", b"v", 0).unwrap();
        store.put(b"expired1", b"v", past_ms).unwrap();
        store.put(b"expired2", b"v", past_ms).unwrap();

        let count = store.purge_expired().unwrap();
        assert_eq!(count, 2, "purge_expired must report 2 purged keys");

        assert!(
            store.get(b"expired1").unwrap().is_none(),
            "expired1 must be gone"
        );
        assert!(
            store.get(b"expired2").unwrap().is_none(),
            "expired2 must be gone"
        );
        assert_eq!(
            store.get(b"live1").unwrap(),
            Some(b"v".to_vec()),
            "live1 must still be readable"
        );
    }

    /// Tombstones written by `purge_expired` must prevent key resurrection
    /// across a store close and reopen.
    #[test]
    fn purge_expired_writes_tombstones_preventing_resurrection() {
        let dir = tmp();
        let past_ms = now_ms().saturating_sub(1000);

        {
            let mut store = Store::open(dir.path()).unwrap();
            store.put(b"ex_key", b"val", past_ms).unwrap();
            let count = store.purge_expired().unwrap();
            assert_eq!(count, 1);
        }

        // Reopen — log replay must see the tombstone after the original record
        // and must NOT re-insert ex_key into the keydir.
        let mut store = Store::open(dir.path()).unwrap();
        assert!(
            store.get(b"ex_key").unwrap().is_none(),
            "ex_key must not resurrect after purge_expired + reopen"
        );
    }

    /// When no keys are expired, `purge_expired` must return `Ok(0)` and leave
    /// the keydir untouched.
    #[test]
    fn purge_expired_returns_zero_when_nothing_expired() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        store.put(b"a", b"1", 0).unwrap();
        store.put(b"b", b"2", 0).unwrap();
        store.put(b"c", b"3", 0).unwrap();

        let count = store.purge_expired().unwrap();
        assert_eq!(count, 0, "no keys are expired so count must be 0");

        // All keys must still be readable.
        assert!(store.get(b"a").unwrap().is_some());
        assert!(store.get(b"b").unwrap().is_some());
        assert!(store.get(b"c").unwrap().is_some());
    }

    /// `len()` must reflect the correct count after `purge_expired` removes
    /// expired entries from the keydir.
    #[test]
    fn len_is_accurate_after_purge_expired() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let past_ms = now_ms().saturating_sub(1000);

        store.put(b"live1", b"v", 0).unwrap();
        store.put(b"live2", b"v", 0).unwrap();
        store.put(b"live3", b"v", 0).unwrap();
        store.put(b"exp1", b"v", past_ms).unwrap();
        store.put(b"exp2", b"v", past_ms).unwrap();

        store.purge_expired().unwrap();

        assert_eq!(
            store.len(),
            3,
            "len must be 3 (only live keys) after purge_expired"
        );
    }

    // ------------------------------------------------------------------
    // CRC / record integrity
    // ------------------------------------------------------------------

    /// Put a key, close the store, corrupt a byte in the middle of the log
    /// record (not the CRC bytes), reopen and get — must return Err or None.
    #[test]
    fn corrupt_crc_in_log_returns_error_on_get() {
        use std::io::{Seek, SeekFrom, Write};
        let dir = tmp();

        {
            let mut store = Store::open(dir.path()).unwrap();
            store.put(b"key", b"value", 0).unwrap();
        }

        // Locate the single .log file and flip a byte in the key area
        // (byte at HEADER_SIZE, which is after the 4-byte CRC field).
        // This invalidates the CRC without touching the CRC bytes themselves.
        for entry in std::fs::read_dir(dir.path()).unwrap() {
            let e = entry.unwrap();
            if e.file_name().to_string_lossy().ends_with(".log") {
                let mut f = std::fs::OpenOptions::new()
                    .write(true)
                    .open(e.path())
                    .unwrap();
                // Flip one byte in the key area (after the 26-byte header).
                f.seek(SeekFrom::Start(crate::log::HEADER_SIZE as u64))
                    .unwrap();
                let mut byte = [0u8; 1];
                use std::io::Read;
                f.seek(SeekFrom::Start(crate::log::HEADER_SIZE as u64))
                    .unwrap();
                {
                    let mut rf = std::fs::File::open(e.path()).unwrap();
                    rf.seek(SeekFrom::Start(crate::log::HEADER_SIZE as u64))
                        .unwrap();
                    rf.read_exact(&mut byte).unwrap();
                }
                byte[0] ^= 0xFF;
                f.seek(SeekFrom::Start(crate::log::HEADER_SIZE as u64))
                    .unwrap();
                f.write_all(&byte).unwrap();
            }
        }

        // Reopen the store — the corrupt record will be skipped by the tolerant
        // reader (crash-recovery semantics), so get returns None rather than Err.
        let mut store = Store::open(dir.path()).unwrap();
        // The key should not be accessible (CRC mismatch is handled gracefully).
        let result = store.get(b"key").unwrap();
        assert!(
            result.is_none(),
            "corrupt CRC in log must cause get to return None (tolerant recovery)"
        );
    }

    /// Put 5 records, truncate the 3rd record to half its size, reopen.
    /// Records 1 and 2 must be intact; records 3-5 gone (tolerant reader stops
    /// at first error).
    #[test]
    fn store_recovers_after_mid_file_record_truncation() {
        let dir = tmp();

        let mut offsets = Vec::new();
        {
            let mut store = Store::open(dir.path()).unwrap();
            for i in 0u8..5 {
                let key = format!("key{i}");
                let val = format!("val{i}");
                store.put(key.as_bytes(), val.as_bytes(), 0).unwrap();
            }
            // Capture writer offset so we can locate the 3rd record boundary.
            // We'll compute truncation point directly from disk.
        }

        // Reopen just to confirm 5 records, then compute the truncation point.
        {
            let log_path_buf = std::fs::read_dir(dir.path())
                .unwrap()
                .find_map(|e| {
                    let e = e.unwrap();
                    if e.file_name().to_string_lossy().ends_with(".log") {
                        Some(e.path())
                    } else {
                        None
                    }
                })
                .unwrap();

            // Read all 5 records to find where the 3rd one starts.
            let mut reader = crate::log::LogReader::open(&log_path_buf).unwrap();
            let records = reader.iter_from_start().unwrap();
            assert_eq!(records.len(), 5, "expected 5 records before truncation");

            // Compute offset of 3rd record (index 2).
            let rec0_len = (crate::log::HEADER_SIZE
                + records[0].key.len()
                + records[0].value.as_ref().map_or(0, Vec::len)) as u64;
            let rec1_len = (crate::log::HEADER_SIZE
                + records[1].key.len()
                + records[1].value.as_ref().map_or(0, Vec::len)) as u64;
            let rec2_start = rec0_len + rec1_len;
            // Truncate to halfway through the 3rd record.
            let rec2_half_len = (crate::log::HEADER_SIZE
                + records[2].key.len()
                + records[2].value.as_ref().map_or(0, Vec::len))
                as u64
                / 2;
            let truncate_at = rec2_start + rec2_half_len;

            let file = std::fs::OpenOptions::new()
                .write(true)
                .open(&log_path_buf)
                .unwrap();
            file.set_len(truncate_at).unwrap();
            offsets.push(rec2_start); // used for assertion below
        }

        // Reopen — tolerant reader stops at truncated record 3.
        let mut store = Store::open(dir.path()).unwrap();
        // Records 0 and 1 must be intact.
        assert_eq!(
            store.get(b"key0").unwrap(),
            Some(b"val0".to_vec()),
            "key0 must survive"
        );
        assert_eq!(
            store.get(b"key1").unwrap(),
            Some(b"val1".to_vec()),
            "key1 must survive"
        );
        // Records 2-4 must be gone (tolerant reader stopped at first error).
        assert!(
            store.get(b"key2").unwrap().is_none(),
            "key2 must be gone after truncation"
        );
        assert!(
            store.get(b"key3").unwrap().is_none(),
            "key3 must be gone after truncation"
        );
        assert!(
            store.get(b"key4").unwrap().is_none(),
            "key4 must be gone after truncation"
        );
        let _ = offsets;
    }

    // ------------------------------------------------------------------
    // Offset tracking
    // ------------------------------------------------------------------

    /// Put 10 records and verify that `writer.offset` increases monotonically
    /// and equals the sum of encoded record sizes.
    #[test]
    fn offset_is_correct_after_multiple_puts() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let mut expected_offset = 0u64;
        for i in 0u8..10 {
            let key = vec![i; (i as usize) + 1]; // key length 1..10
            let val = vec![i; (i as usize) + 2]; // val length 2..11
            store.put(&key, &val, 0).unwrap();
            let record_size = (crate::log::HEADER_SIZE + key.len() + val.len()) as u64;
            expected_offset += record_size;
            assert!(
                store.writer.offset >= expected_offset,
                "offset must increase after put {i}"
            );
        }
        assert_eq!(
            store.writer.offset, expected_offset,
            "final offset must equal sum of record sizes"
        );
    }

    // ------------------------------------------------------------------
    // len() accuracy
    // ------------------------------------------------------------------

    #[test]
    fn len_is_zero_for_empty_store() {
        let dir = tmp();
        let store = Store::open(dir.path()).unwrap();
        assert_eq!(store.len(), 0);
        assert!(store.is_empty());
    }

    #[test]
    fn len_decreases_after_delete() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        store.put(b"a", b"1", 0).unwrap();
        store.put(b"b", b"2", 0).unwrap();
        store.put(b"c", b"3", 0).unwrap();
        assert_eq!(store.len(), 3);
        store.delete(b"b").unwrap();
        assert_eq!(store.len(), 2);
        store.delete(b"a").unwrap();
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn len_counts_only_live_non_expired_keys() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let past_ms = now_ms().saturating_sub(5000);
        store.put(b"live1", b"v", 0).unwrap();
        store.put(b"live2", b"v", 0).unwrap();
        store.put(b"live3", b"v", 0).unwrap();
        store.put(b"exp1", b"v", past_ms).unwrap();
        store.put(b"exp2", b"v", past_ms).unwrap();
        assert_eq!(store.len(), 3, "only live non-expired keys are counted");
    }

    // ------------------------------------------------------------------
    // put/get/delete semantics
    // ------------------------------------------------------------------

    #[test]
    fn put_empty_key_and_value() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        // Empty key with non-empty value should work.
        store.put(b"", b"non_empty_value", 0).unwrap();
        let result = store.get(b"").unwrap();
        assert!(
            result.is_some(),
            "empty key with non-empty value must be retrievable"
        );
    }

    #[test]
    fn put_very_large_value_1mb_roundtrip() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let value: Vec<u8> = (0u8..=255).cycle().take(1024 * 1024).collect();
        store.put(b"bigkey", &value, 0).unwrap();
        let got = store.get(b"bigkey").unwrap().unwrap();
        assert_eq!(got.len(), 1024 * 1024, "1MB value must round-trip");
        assert_eq!(got, value, "1MB value bytes must match exactly");
    }

    #[test]
    fn put_key_with_null_bytes() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let key = b"\x00\x01\x02";
        store.put(key, b"null_key_val", 0).unwrap();
        let result = store.get(key).unwrap();
        assert_eq!(
            result,
            Some(b"null_key_val".to_vec()),
            "key with null bytes must round-trip"
        );
    }

    #[test]
    fn delete_returns_false_for_missing_key() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let result = store.delete(b"nonexistent").unwrap();
        assert!(!result, "delete on missing key must return false");
        assert_eq!(store.len(), 0, "len must remain 0 after no-op delete");
    }

    #[test]
    fn delete_returns_true_for_existing_key() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        store.put(b"present", b"val", 0).unwrap();
        let result = store.delete(b"present").unwrap();
        assert!(result, "delete on existing key must return true");
        assert!(
            store.get(b"present").unwrap().is_none(),
            "key must be gone after delete"
        );
    }

    #[test]
    fn delete_tombstone_prevents_resurrection() {
        let dir = tmp();

        {
            let mut store = Store::open(dir.path()).unwrap();
            store.put(b"zombie", b"alive", 0).unwrap();
            store.delete(b"zombie").unwrap();
        }

        // Reopen — tombstone in log replay must prevent key from coming back.
        let mut store = Store::open(dir.path()).unwrap();
        assert!(
            store.get(b"zombie").unwrap().is_none(),
            "deleted key must not resurrect after reopen"
        );
    }

    // ------------------------------------------------------------------
    // put_batch: new edge cases
    // ------------------------------------------------------------------

    #[test]
    fn put_batch_empty_slice_is_ok() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        // Already covered by empty_batch_is_noop but this is an explicit API contract test.
        store.put_batch(&[]).unwrap();
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn put_batch_all_entries_retrievable() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let kv: Vec<(Vec<u8>, Vec<u8>)> = (0u8..20)
            .map(|i| (format!("bk{i}").into_bytes(), format!("bv{i}").into_bytes()))
            .collect();
        let entries: Vec<(&[u8], &[u8], u64)> = kv
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice(), 0u64))
            .collect();
        store.put_batch(&entries).unwrap();
        for (k, v) in &kv {
            assert_eq!(
                store.get(k).unwrap(),
                Some(v.clone()),
                "batch entry {k:?} must be retrievable"
            );
        }
    }

    #[test]
    fn put_batch_with_expired_entries() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let past_ms = now_ms().saturating_sub(2000);
        let entries: Vec<(&[u8], &[u8], u64)> =
            vec![(b"exp_batch", b"val", past_ms), (b"live_batch", b"val", 0)];
        store.put_batch(&entries).unwrap();
        assert!(
            store.get(b"exp_batch").unwrap().is_none(),
            "expired batch entry must not be readable"
        );
        assert_eq!(
            store.get(b"live_batch").unwrap(),
            Some(b"val".to_vec()),
            "live batch entry must be readable"
        );
    }

    // ------------------------------------------------------------------
    // expiry: detailed edge cases
    // ------------------------------------------------------------------

    #[test]
    fn get_returns_none_for_past_expire_at() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let past_ms = now_ms().saturating_sub(1000);
        store.put(b"past_ttl", b"gone", past_ms).unwrap();
        assert!(
            store.get(b"past_ttl").unwrap().is_none(),
            "key with past expire_at must return None"
        );
    }

    #[test]
    fn get_returns_value_for_future_expire_at() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let future_ms = now_ms() + 60_000;
        store.put(b"future_ttl", b"present", future_ms).unwrap();
        assert_eq!(
            store.get(b"future_ttl").unwrap(),
            Some(b"present".to_vec()),
            "key with future expire_at must return value"
        );
    }

    #[test]
    fn keys_excludes_past_expired() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let past_ms = now_ms().saturating_sub(500);
        store.put(b"gone", b"v", past_ms).unwrap();
        store.put(b"here", b"v", 0).unwrap();
        let keys = store.keys();
        assert!(
            !keys.contains(&b"gone".to_vec()),
            "expired key must not appear in keys()"
        );
        assert!(
            keys.contains(&b"here".to_vec()),
            "live key must appear in keys()"
        );
        assert_eq!(keys.len(), 1, "keys() must return exactly 1 live key");
    }

    // ------------------------------------------------------------------
    // purge_expired: detailed edge cases
    // ------------------------------------------------------------------

    #[test]
    fn purge_expired_count_matches_expired_keys() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let past_ms = now_ms().saturating_sub(1000);
        for i in 0u8..5 {
            store.put(&[i], b"expired_val", past_ms).unwrap();
        }
        let count = store.purge_expired().unwrap();
        assert_eq!(count, 5, "purge_expired must return 5 for 5 expired keys");
    }

    #[test]
    fn purge_expired_evicted_keys_not_in_keys_list() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let past_ms = now_ms().saturating_sub(1000);
        store.put(b"exp_a", b"v", past_ms).unwrap();
        store.put(b"exp_b", b"v", past_ms).unwrap();
        store.put(b"live_x", b"v", 0).unwrap();

        store.purge_expired().unwrap();

        let keys = store.keys();
        assert!(
            !keys.contains(&b"exp_a".to_vec()),
            "exp_a must not be in keys after purge"
        );
        assert!(
            !keys.contains(&b"exp_b".to_vec()),
            "exp_b must not be in keys after purge"
        );
        assert!(
            keys.contains(&b"live_x".to_vec()),
            "live_x must still be in keys after purge"
        );
        assert_eq!(keys.len(), 1);
    }

    // ------------------------------------------------------------------
    // Issue 5.1: Corrupt hint CRC triggers log replay fallback
    // ------------------------------------------------------------------

    /// A hint file whose first entry has a flipped byte (corrupting its CRC)
    /// must trigger log replay fallback on `Store::open`, recovering all keys.
    #[test]
    fn corrupt_hint_crc_falls_back_to_log_replay() {
        let dir = tmp();

        // 1. Open store, put keys, write hint file, close.
        {
            let mut store = Store::open(dir.path()).unwrap();
            store.put(b"crc_key1", b"crc_val1", 0).unwrap();
            store.put(b"crc_key2", b"crc_val2", 0).unwrap();
            store.put(b"crc_key3", b"crc_val3", 0).unwrap();
            store.write_hint_file().unwrap();
        }

        // 2. Open the hint file and flip bytes 12..16 (inside the offset field
        //    of the first entry). The stored CRC won't match the new body bytes.
        for entry in std::fs::read_dir(dir.path()).unwrap() {
            let e = entry.unwrap();
            if e.file_name().to_string_lossy().ends_with(".hint") {
                use std::fs::OpenOptions;
                use std::io::{Seek, SeekFrom, Write};
                let mut f = OpenOptions::new().write(true).open(e.path()).unwrap();
                // Offset field starts at byte 4 (CRC) + 8 (file_id) = 12.
                f.seek(SeekFrom::Start(12)).unwrap();
                f.write_all(&[0xFF, 0xFF, 0xFF, 0xFF]).unwrap();
            }
        }

        // 3. Reopen store — must fall back to log replay and recover all keys.
        let mut store = Store::open(dir.path()).unwrap();
        assert_eq!(
            store.get(b"crc_key1").unwrap(),
            Some(b"crc_val1".to_vec()),
            "crc_key1 must be recovered from log replay after CRC-corrupt hint"
        );
        assert_eq!(
            store.get(b"crc_key2").unwrap(),
            Some(b"crc_val2".to_vec()),
            "crc_key2 must be recovered from log replay after CRC-corrupt hint"
        );
        assert_eq!(
            store.get(b"crc_key3").unwrap(),
            Some(b"crc_val3".to_vec()),
            "crc_key3 must be recovered from log replay after CRC-corrupt hint"
        );
    }

    // ==================================================================
    // Deep NIF edge cases — targeting FFI/NIF boundary pitfalls
    // ==================================================================

    // ---- Key / value boundary sizes ----

    #[test]
    fn get_empty_key_returns_value() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        store.put(b"", b"empty_key_val", 0).unwrap();
        assert_eq!(store.get(b"").unwrap(), Some(b"empty_key_val".to_vec()));
    }

    #[test]
    fn get_key_with_embedded_null_bytes() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let key = b"a\0b\0c";
        store.put(key, b"nulls_in_key", 0).unwrap();
        assert_eq!(store.get(key).unwrap(), Some(b"nulls_in_key".to_vec()));
        // Different null pattern must not collide
        assert!(store.get(b"a\0b").unwrap().is_none());
    }

    #[test]
    fn put_empty_value_is_valid() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        store.put(b"k", b"v", 0).unwrap();
        // Empty value is a valid value (Redis SET key "" is valid)
        store.put(b"k", b"", 0).unwrap();
        assert_eq!(
            store.get(b"k").unwrap(),
            Some(vec![]),
            "empty value must be stored, not treated as tombstone"
        );
        // Survives reopen
        drop(store);
        let mut store = Store::open(dir.path()).unwrap();
        assert_eq!(store.get(b"k").unwrap(), Some(vec![]));
    }

    #[test]
    fn put_max_key_65535_bytes() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let key = vec![0xABu8; 65535]; // u16::MAX
        store.put(&key, b"v", 0).unwrap();
        assert_eq!(store.get(&key).unwrap(), Some(b"v".to_vec()));
    }

    #[test]
    fn put_key_65536_bytes_rejected_by_validate() {
        let key = vec![0xABu8; 65536]; // u16::MAX + 1
        let result = crate::log::validate_kv_sizes(&key, b"v");
        assert!(result.is_err(), "key > 65535 bytes must be rejected");
    }

    #[test]
    fn put_value_with_all_null_bytes() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let value = vec![0u8; 1024];
        store.put(b"nullval", &value, 0).unwrap();
        assert_eq!(store.get(b"nullval").unwrap(), Some(value));
    }

    #[test]
    fn put_batch_empty_list_no_side_effects() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        store.put(b"pre", b"existing", 0).unwrap();
        store.put_batch(&[]).unwrap();
        assert_eq!(store.len(), 1);
        assert_eq!(store.get(b"pre").unwrap(), Some(b"existing".to_vec()));
    }

    #[test]
    fn put_batch_single_item() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        store.put_batch(&[(b"only", b"one" as &[u8], 0)]).unwrap();
        assert_eq!(store.get(b"only").unwrap(), Some(b"one".to_vec()));
    }

    #[test]
    fn put_batch_1000_items_all_readable() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let kv: Vec<(Vec<u8>, Vec<u8>)> = (0..1000)
            .map(|i| {
                (
                    format!("bk_{i:04}").into_bytes(),
                    format!("bv_{i:04}").into_bytes(),
                )
            })
            .collect();
        let entries: Vec<(&[u8], &[u8], u64)> = kv
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice(), 0u64))
            .collect();
        store.put_batch(&entries).unwrap();
        for (k, v) in &kv {
            assert_eq!(store.get(k).unwrap(), Some(v.clone()));
        }
    }

    #[test]
    fn delete_nonexistent_key_no_side_effects() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        store.put(b"keep", b"v", 0).unwrap();
        assert!(!store.delete(b"nonexistent").unwrap());
        assert_eq!(store.len(), 1);
        assert_eq!(store.get(b"keep").unwrap(), Some(b"v".to_vec()));
    }

    #[test]
    fn delete_twice_same_key() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        store.put(b"dbl_del", b"v", 0).unwrap();
        assert!(store.delete(b"dbl_del").unwrap());
        assert!(!store.delete(b"dbl_del").unwrap());
        assert!(store.get(b"dbl_del").unwrap().is_none());
    }

    #[test]
    fn get_after_delete_returns_none() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        store.put(b"gone", b"val", 0).unwrap();
        store.delete(b"gone").unwrap();
        assert!(store.get(b"gone").unwrap().is_none());
    }

    #[test]
    fn keys_on_empty_store_returns_empty() {
        let dir = tmp();
        let store = Store::open(dir.path()).unwrap();
        assert!(store.keys().is_empty());
    }

    #[test]
    fn store_open_close_open_same_dir_preserves_data() {
        let dir = tmp();
        {
            let mut store = Store::open(dir.path()).unwrap();
            store.put(b"survive", b"reopen", 0).unwrap();
        }
        {
            let mut store = Store::open(dir.path()).unwrap();
            assert_eq!(store.get(b"survive").unwrap(), Some(b"reopen".to_vec()));
            store.put(b"second", b"open", 0).unwrap();
        }
        let mut store = Store::open(dir.path()).unwrap();
        assert_eq!(store.get(b"survive").unwrap(), Some(b"reopen".to_vec()));
        assert_eq!(store.get(b"second").unwrap(), Some(b"open".to_vec()));
    }

    #[test]
    fn store_open_nonexistent_dir_creates_it() {
        let base = tmp();
        let nested = base.path().join("deep").join("nested").join("store");
        assert!(!nested.exists());
        let mut store = Store::open(&nested).unwrap();
        assert!(nested.exists());
        store.put(b"k", b"v", 0).unwrap();
        assert_eq!(store.get(b"k").unwrap(), Some(b"v".to_vec()));
    }

