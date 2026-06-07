    #[test]
    fn c4_encode_empty_key_and_value() {
        let encoded = encode_record(b"", b"", 0);
        assert_eq!(encoded.len(), HEADER_SIZE);
        let mut cursor = io::Cursor::new(&encoded);
        let r = read_next_record(&mut cursor).unwrap().unwrap();
        assert!(r.key.is_empty());
        // Empty value is valid (value_size=0), not a tombstone (value_size=TOMBSTONE)
        assert_eq!(r.value, Some(vec![]));
    }

    #[test]
    fn c4_encode_max_key_size() {
        let key = vec![0x42u8; 65535]; // u16::MAX
        let value = b"v";
        let encoded = encode_record(&key, value, 0);
        assert_eq!(encoded.len(), HEADER_SIZE + 65535 + 1);

        let mut cursor = io::Cursor::new(&encoded);
        let r = read_next_record(&mut cursor).unwrap().unwrap();
        assert_eq!(r.key.len(), 65535);
        assert_eq!(r.value, Some(b"v".to_vec()));
    }

    #[test]
    fn c4_encode_decode_10k_records_identical_output() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();

        for i in 0u32..10_000 {
            let key = format!("k{i}");
            let value = vec![i as u8; (i % 256) as usize + 1];
            w.write(key.as_bytes(), &value, i as u64 * 1000).unwrap();
        }
        w.sync().unwrap();

        let mut reader = LogReader::open(&path).unwrap();
        let records = reader.iter_from_start().unwrap();
        assert_eq!(records.len(), 10_000);
        for (i, rec) in records.iter().enumerate() {
            let expected_key = format!("k{i}");
            let expected_value = vec![i as u8; (i % 256) + 1];
            assert_eq!(rec.key, expected_key.as_bytes());
            assert_eq!(rec.value, Some(expected_value));
            assert_eq!(rec.expire_at_ms, i as u64 * 1000);
        }
    }

    // ------------------------------------------------------------------
    // C-5: CRC verification uses streaming hasher (no throwaway Vec)
    // ------------------------------------------------------------------

    #[test]
    fn c5_corrupted_crc_still_detected() {
        let encoded = encode_record(b"hello", b"world", 0);
        let mut corrupted = encoded.clone();
        let last = corrupted.len() - 1;
        corrupted[last] ^= 0xFF;

        let mut cursor = io::Cursor::new(&corrupted);
        assert!(
            read_next_record(&mut cursor).is_err(),
            "corrupted record must be detected"
        );
    }

    #[test]
    fn c5_corrupted_header_byte_detected() {
        let encoded = encode_record(b"hello", b"world", 0);
        let mut corrupted = encoded.clone();
        corrupted[5] ^= 0x01;

        let mut cursor = io::Cursor::new(&corrupted);
        assert!(
            read_next_record(&mut cursor).is_err(),
            "corrupted header must be detected"
        );
    }

    #[test]
    fn c5_backward_compat_records_validate() {
        let test_cases: Vec<(&[u8], &[u8], u64)> = vec![
            (b"", b"val", 0),
            (b"key", b"", 0),
            (b"k", b"v", 999),
            (&[0xFF; 100], &[0x00; 200], u64::MAX),
            (b"a", &[0xAB; 10000], 42),
        ];

        for (key, value, expire) in &test_cases {
            let encoded = encode_record(key, value, *expire);
            let mut cursor = io::Cursor::new(&encoded);
            let result = read_next_record(&mut cursor);
            assert!(
                result.is_ok(),
                "record (key_len={}, value_len={}, expire={}) should decode",
                key.len(),
                value.len(),
                expire
            );
        }
    }

    // ------------------------------------------------------------------
    // C-7: sync_data (fdatasync) — test at io_backend level
    // ------------------------------------------------------------------

    #[test]
    fn c7_write_sync_data_survives_reopen() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");

        {
            let mut w = LogWriter::open(&path, 1).unwrap();
            w.write(b"k1", b"v1", 0).unwrap();
            w.write(b"k2", b"v2", 0).unwrap();
            w.sync().unwrap(); // now uses sync_data/fdatasync
        }

        let mut reader = LogReader::open(&path).unwrap();
        let records = reader.iter_from_start().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].key, b"k1");
        assert_eq!(records[0].value, Some(b"v1".to_vec()));
        assert_eq!(records[1].key, b"k2");
        assert_eq!(records[1].value, Some(b"v2".to_vec()));
    }

    #[test]
    fn c7_sync_data_after_file_extend() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();

        for i in 0u32..100 {
            let key = format!("key_{i:03}");
            let value = vec![0xABu8; 1024]; // 1KB values
            w.write(key.as_bytes(), &value, 0).unwrap();
        }
        w.sync().unwrap();

        let file_size = fs::metadata(&path).unwrap().len();
        assert!(
            file_size > 100_000,
            "file should be >100KB, got {file_size}"
        );

        let mut reader = LogReader::open(&path).unwrap();
        let records = reader.iter_from_start().unwrap();
        assert_eq!(records.len(), 100);
    }

    // ------------------------------------------------------------------
    // C-2/C-6: pread_record_from_file — direct file + pread path
    // ------------------------------------------------------------------

    #[test]
    fn c2_c6_pread_record_from_file_reads_correctly() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        let off1 = w.write(b"first", b"aaa", 0).unwrap();
        let off2 = w.write(b"second", b"bbb", 42).unwrap();
        w.sync().unwrap();

        let file = File::open(&path).unwrap();
        let r1 = pread_record_from_file(&file, off1).unwrap().unwrap();
        assert_eq!(r1.key, b"first");
        assert_eq!(r1.value, Some(b"aaa".to_vec()));

        let r2 = pread_record_from_file(&file, off2).unwrap().unwrap();
        assert_eq!(r2.key, b"second");
        assert_eq!(r2.value, Some(b"bbb".to_vec()));
        assert_eq!(r2.expire_at_ms, 42);
    }

    #[test]
    fn c2_c6_pread_record_from_file_eof() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        w.write(b"k", b"v", 0).unwrap();
        w.sync().unwrap();
        let file_size = fs::metadata(&path).unwrap().len();

        let file = File::open(&path).unwrap();
        assert!(pread_record_from_file(&file, file_size).unwrap().is_none());
        assert!(pread_record_from_file(&file, file_size + 1000)
            .unwrap()
            .is_none());
    }

    #[test]
    fn c2_c6_pread_record_from_file_truncated_header_is_error() {
        let dir = temp_dir();
        let path = dir.path().join("truncated_header.log");
        fs::write(&path, vec![0u8; HEADER_SIZE - 1]).unwrap();

        let file = File::open(&path).unwrap();
        let err = pread_record_from_file(&file, 0).unwrap_err();

        assert!(
            err.to_string().contains("short read"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn keyed_pread_mismatch_ignores_corrupt_huge_value_size() {
        use std::os::unix::fs::FileExt;

        let dir = temp_dir();
        let path = dir.path().join("wrong_key_huge_value.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        let offset = w.write(b"actual", b"small", 0).unwrap();
        w.sync().unwrap();

        let file = fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.write_at(&(u32::MAX - 1).to_le_bytes(), offset + 22)
            .unwrap();
        file.sync_data().unwrap();

        let file = File::open(&path).unwrap();
        assert_eq!(
            pread_value_for_key_from_file(&file, offset, b"expected").unwrap(),
            None
        );
    }

    #[test]
    fn c2_c6_pread_1000_sequential_reads_correct() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        let mut offsets = Vec::new();
        for i in 0u32..1000 {
            let key = format!("k{i:04}");
            let value = format!("v{i:04}");
            let off = w.write(key.as_bytes(), value.as_bytes(), 0).unwrap();
            offsets.push((off, key, value));
        }
        w.sync().unwrap();

        let file = File::open(&path).unwrap();
        for (off, key, value) in &offsets {
            let record = pread_record_from_file(&file, *off).unwrap().unwrap();
            assert_eq!(record.key, key.as_bytes());
            assert_eq!(record.value, Some(value.as_bytes().to_vec()));
        }
    }

    // ------------------------------------------------------------------
    // H-2: write_batch_nosync combines records into single write
    // ------------------------------------------------------------------

    #[test]
    fn h2_batch_nosync_100_records_correct_offsets() {
        let dir = temp_dir();
        let path = dir.path().join("h2.log");
        let mut w = LogWriter::open(&path, 1).unwrap();

        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0u32..100)
            .map(|i| {
                (
                    format!("key_{i:03}").into_bytes(),
                    format!("val_{i:03}_padding").into_bytes(),
                )
            })
            .collect();

        let refs: Vec<(&[u8], &[u8], u64)> = entries
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice(), 0u64))
            .collect();

        let offsets = w.write_batch_nosync(&refs).unwrap();
        assert_eq!(offsets.len(), 100);

        // Verify offsets are monotonically increasing
        for i in 1..offsets.len() {
            assert!(
                offsets[i].0 > offsets[i - 1].0,
                "offset {i} must be > offset {}",
                i - 1
            );
        }

        // Verify all records are readable
        let mut reader = LogReader::open(&path).unwrap();
        let records = reader.iter_from_start().unwrap();
        assert_eq!(records.len(), 100);
        for (i, rec) in records.iter().enumerate() {
            assert_eq!(rec.key, format!("key_{i:03}").as_bytes());
            assert!(rec.value.is_some());
        }
    }

    #[test]
    fn h2_batch_nosync_empty_batch() {
        let dir = temp_dir();
        let path = dir.path().join("h2_empty.log");
        let mut w = LogWriter::open(&path, 1).unwrap();

        let offsets = w.write_batch_nosync(&[]).unwrap();
        assert!(offsets.is_empty());
        assert_eq!(w.offset, 0);
    }

    #[test]
    fn h2_batch_nosync_single_record() {
        let dir = temp_dir();
        let path = dir.path().join("h2_single.log");
        let mut w = LogWriter::open(&path, 1).unwrap();

        let entries: Vec<(&[u8], &[u8], u64)> = vec![(b"k", b"v", 0)];
        let offsets = w.write_batch_nosync(&entries).unwrap();
        assert_eq!(offsets.len(), 1);
        assert_eq!(offsets[0].0, 0);

        let mut reader = LogReader::open(&path).unwrap();
        let records = reader.iter_from_start().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key, b"k");
    }

    #[test]
    fn h2_batch_nosync_large_batch_1000_records() {
        let dir = temp_dir();
        let path = dir.path().join("h2_large.log");
        let mut w = LogWriter::open(&path, 1).unwrap();

        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0u32..1000)
            .map(|i| (format!("k{i}").into_bytes(), vec![0xABu8; 256]))
            .collect();

        let refs: Vec<(&[u8], &[u8], u64)> = entries
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice(), 0u64))
            .collect();

        let offsets = w.write_batch_nosync(&refs).unwrap();
        assert_eq!(offsets.len(), 1000);

        // Verify all records are readable and correct
        let mut reader = LogReader::open(&path).unwrap();
        let records = reader.iter_from_start().unwrap();
        assert_eq!(records.len(), 1000);
    }

    // ------------------------------------------------------------------
    // Tombstone write to file and read back by offset
    // ------------------------------------------------------------------

    #[test]
    fn tombstone_write_read_at_offset() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        let off = w.write_tombstone(b"deleted_key").unwrap();
        w.sync().unwrap();

        let mut reader = LogReader::open(&path).unwrap();
        let record = reader.read_at(off).unwrap().unwrap();
        assert_eq!(record.key, b"deleted_key");
        assert!(record.value.is_none(), "tombstone must read back as None");
        assert_eq!(record.expire_at_ms, 0, "tombstone expire_at must be 0");
    }

    // ------------------------------------------------------------------
    // Expiry field preserved through file write + read_at
    // ------------------------------------------------------------------

    #[test]
    fn expiry_preserved_through_file_roundtrip() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        let off = w.write(b"ttl_key", b"ttl_val", 1_700_000_000_000).unwrap();
        w.sync().unwrap();

        let mut reader = LogReader::open(&path).unwrap();
        let record = reader.read_at(off).unwrap().unwrap();
        assert_eq!(record.key, b"ttl_key");
        assert_eq!(record.value, Some(b"ttl_val".to_vec()));
        assert_eq!(record.expire_at_ms, 1_700_000_000_000);
    }

    #[test]
    fn expiry_u64_max_roundtrips() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        let off = w.write(b"k", b"v", u64::MAX).unwrap();
        w.sync().unwrap();

        let mut reader = LogReader::open(&path).unwrap();
        let record = reader.read_at(off).unwrap().unwrap();
        assert_eq!(record.expire_at_ms, u64::MAX);
    }

    // ------------------------------------------------------------------
    // open_small produces valid writer identical to open
    // ------------------------------------------------------------------

    #[test]
    fn open_small_write_and_read_back() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");

        let mut w = LogWriter::open_small(&path, 42).unwrap();
        assert_eq!(w.file_id, 42);
        assert_eq!(w.offset, 0);

        let off = w.write(b"small_key", b"small_val", 0).unwrap();
        w.sync().unwrap();

        let mut reader = LogReader::open(&path).unwrap();
        let record = reader.read_at(off).unwrap().unwrap();
        assert_eq!(record.key, b"small_key");
        assert_eq!(record.value, Some(b"small_val".to_vec()));
    }

    #[test]
    fn open_small_batch_write() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open_small(&path, 1).unwrap();

        let entries: Vec<(&[u8], &[u8], u64)> = vec![
            (b"sk1", b"sv1", 0),
            (b"sk2", b"sv2", 100),
            (b"sk3", b"sv3", 200),
        ];
        let results = w.write_batch(&entries).unwrap();
        assert_eq!(results.len(), 3);

        let mut reader = LogReader::open(&path).unwrap();
        for (i, (off, _)) in results.iter().enumerate() {
            let record = reader.read_at(*off).unwrap().unwrap();
            assert_eq!(record.key, entries[i].0);
            assert_eq!(record.value, Some(entries[i].1.to_vec()));
            assert_eq!(record.expire_at_ms, entries[i].2);
        }
    }

    #[test]
    fn open_small_and_open_produce_identical_records() {
        let dir = temp_dir();
        let path_small = dir.path().join("small.log");
        let path_normal = dir.path().join("normal.log");

        // Write same data via both
        {
            let mut ws = LogWriter::open_small(&path_small, 1).unwrap();
            ws.write(b"key", b"value", 12345).unwrap();
            ws.write_tombstone(b"tomb").unwrap();
            ws.sync().unwrap();
        }
        {
            let mut wn = LogWriter::open(&path_normal, 1).unwrap();
            wn.write(b"key", b"value", 12345).unwrap();
            wn.write_tombstone(b"tomb").unwrap();
            wn.sync().unwrap();
        }

        // Both files should have identical sizes
        let size_s = fs::metadata(&path_small).unwrap().len();
        let size_n = fs::metadata(&path_normal).unwrap().len();
        assert_eq!(
            size_s, size_n,
            "open_small and open must produce same file size"
        );

        // Both should decode identically
        let mut rs = LogReader::open(&path_small).unwrap();
        let mut rn = LogReader::open(&path_normal).unwrap();
        let recs_s = rs.iter_from_start().unwrap();
        let recs_n = rn.iter_from_start().unwrap();
        assert_eq!(recs_s.len(), recs_n.len());
        for (a, b) in recs_s.iter().zip(recs_n.iter()) {
            assert_eq!(a.key, b.key);
            assert_eq!(a.value, b.value);
            assert_eq!(a.expire_at_ms, b.expire_at_ms);
        }
    }

    #[test]
    fn open_small_reopen_appends() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");

        {
            let mut w = LogWriter::open_small(&path, 1).unwrap();
            w.write(b"first", b"v1", 0).unwrap();
            w.sync().unwrap();
        }

        {
            let mut w = LogWriter::open_small(&path, 1).unwrap();
            assert!(w.offset > 0, "reopened writer must start at end of file");
            w.write(b"second", b"v2", 0).unwrap();
            w.sync().unwrap();
        }

        let mut reader = LogReader::open(&path).unwrap();
        let records = reader.iter_from_start().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].key, b"first");
        assert_eq!(records[1].key, b"second");
    }

    #[test]
    fn concurrent_open_small_writers_return_distinct_offsets() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");

        let mut w1 = LogWriter::open_small(&path, 1).unwrap();
        let mut w2 = LogWriter::open_small(&path, 1).unwrap();

        let off1 = w1.write(b"first", b"v1", 0).unwrap();
        w1.sync().unwrap();

        let off2 = w2.write(b"second", b"v2", 0).unwrap();
        w2.sync().unwrap();

        assert_ne!(
            off1, off2,
            "concurrently opened append handles must not publish duplicate offsets"
        );

        let mut reader = LogReader::open(&path).unwrap();
        let first = reader.read_at(off1).unwrap().unwrap();
        let second = reader.read_at(off2).unwrap().unwrap();
        assert_eq!(first.key, b"first");
        assert_eq!(second.key, b"second");
    }

    // ------------------------------------------------------------------
    // CRC validation through file-level write + corrupt + read_at
    // ------------------------------------------------------------------

    #[test]
    fn crc_corruption_detected_via_read_at() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        w.write(b"mykey", b"myvalue", 0).unwrap();
        w.sync().unwrap();
        drop(w);

        // Corrupt a byte in the value region
        {
            use io::Write as _;
            let flip_pos = HEADER_SIZE as u64 + 5 + 3; // inside value
            let mut f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
            f.seek(io::SeekFrom::Start(flip_pos)).unwrap();
            f.write_all(&[0xFF]).unwrap();
        }

        let mut reader = LogReader::open(&path).unwrap();
        let result = reader.read_at(0);
        assert!(
            result.is_err(),
            "corrupted record must fail CRC check via read_at"
        );
        let err_msg = result.unwrap_err().0;
        assert!(
            err_msg.contains("CRC mismatch"),
            "error must mention CRC mismatch, got: {err_msg}"
        );
    }

    // ------------------------------------------------------------------
    // Partial/torn write: truncate mid-record, tolerant iter skips it
    // ------------------------------------------------------------------

    #[test]
    fn torn_write_tolerant_iter_discards_partial() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        w.write(b"good1", b"val1", 0).unwrap();
        w.write(b"good2", b"val2", 0).unwrap();
        // Third record will be truncated to simulate crash
        let off3 = w.write(b"partial", b"this_will_be_cut", 0).unwrap();
        w.sync().unwrap();
        drop(w);

        // Truncate file to cut the third record in half (header + partial key)
        let truncate_at = off3 + (HEADER_SIZE as u64) + 3; // 3 bytes into key
        {
            let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
            f.set_len(truncate_at).unwrap();
        }

        let mut reader = LogReader::open(&path).unwrap();
        let records = reader.iter_from_start_tolerant().unwrap();
        assert_eq!(
            records.len(),
            2,
            "tolerant iter must discard the torn third record"
        );
        assert_eq!(records[0].key, b"good1");
        assert_eq!(records[1].key, b"good2");
    }

    // ------------------------------------------------------------------
    // Mixed scan: live records + tombstones + varying expiry
    // ------------------------------------------------------------------

    #[test]
    fn scan_mixed_live_tombstone_expiry() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();

        // Interleave live records and tombstones with varied expiry
        let off0 = w.write(b"key_a", b"val_a", 0).unwrap();
        let off1 = w.write_tombstone(b"key_b").unwrap();
        let off2 = w.write(b"key_c", b"val_c", 5000).unwrap();
        let off3 = w.write_tombstone(b"key_d").unwrap();
        let off4 = w.write(b"key_e", b"", 0).unwrap(); // empty value, not tombstone
        let off5 = w.write(b"key_f", b"val_f", u64::MAX).unwrap();
        w.sync().unwrap();

        // Verify via iter_from_start
        let mut reader = LogReader::open(&path).unwrap();
        let records = reader.iter_from_start().unwrap();
        assert_eq!(records.len(), 6);

        // Record 0: live, no expiry
        assert_eq!(records[0].key, b"key_a");
        assert_eq!(records[0].value, Some(b"val_a".to_vec()));
        assert_eq!(records[0].expire_at_ms, 0);

        // Record 1: tombstone
        assert_eq!(records[1].key, b"key_b");
        assert!(records[1].value.is_none());

        // Record 2: live with expiry
        assert_eq!(records[2].key, b"key_c");
        assert_eq!(records[2].value, Some(b"val_c".to_vec()));
        assert_eq!(records[2].expire_at_ms, 5000);

        // Record 3: tombstone
        assert_eq!(records[3].key, b"key_d");
        assert!(records[3].value.is_none());

        // Record 4: live, empty value (not tombstone!)
        assert_eq!(records[4].key, b"key_e");
        assert_eq!(records[4].value, Some(vec![]));

        // Record 5: live, max expiry
        assert_eq!(records[5].key, b"key_f");
        assert_eq!(records[5].value, Some(b"val_f".to_vec()));
        assert_eq!(records[5].expire_at_ms, u64::MAX);

        // Also verify each record is accessible by offset via read_at
        let offsets = [off0, off1, off2, off3, off4, off5];
        for (i, off) in offsets.iter().enumerate() {
            let record = reader.read_at(*off).unwrap().unwrap();
            assert_eq!(
                record.key, records[i].key,
                "read_at offset {off} key mismatch"
            );
            assert_eq!(
                record.value, records[i].value,
                "read_at offset {off} value mismatch"
            );
            assert_eq!(
                record.expire_at_ms, records[i].expire_at_ms,
                "read_at offset {off} expiry mismatch"
            );
        }
    }

    // ------------------------------------------------------------------
    // Large key and value through file write/read
    // ------------------------------------------------------------------

    #[test]
    fn large_key_and_value_file_roundtrip() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let big_key = vec![0x42u8; 1000];
        let big_value = vec![0xABu8; 64 * 1024]; // 64 KiB
        let mut w = LogWriter::open(&path, 1).unwrap();
        let off = w.write(&big_key, &big_value, 999).unwrap();
        w.sync().unwrap();

        let mut reader = LogReader::open(&path).unwrap();
        let record = reader.read_at(off).unwrap().unwrap();
        assert_eq!(record.key, big_key);
        assert_eq!(record.value.as_deref(), Some(big_value.as_slice()));
        assert_eq!(record.expire_at_ms, 999);
    }

    // ------------------------------------------------------------------
    // Zero-length value through file write/read (not tombstone)
    // ------------------------------------------------------------------

    #[test]
    fn zero_length_value_file_roundtrip() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        let off = w.write(b"empty_val_key", b"", 0).unwrap();
        w.sync().unwrap();

        let mut reader = LogReader::open(&path).unwrap();
        let record = reader.read_at(off).unwrap().unwrap();
        assert_eq!(record.key, b"empty_val_key");
        assert_eq!(
            record.value,
            Some(vec![]),
            "zero-length value must be Some(vec![]), not None (tombstone)"
        );
    }

    // ------------------------------------------------------------------
    // Seek-based sequential read (seek_to + read_next)
    // ------------------------------------------------------------------

    #[test]
    fn seek_to_and_read_next() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        w.write(b"r1", b"v1", 0).unwrap();
        let off2 = w.write(b"r2", b"v2", 0).unwrap();
        w.write(b"r3", b"v3", 0).unwrap();
        w.sync().unwrap();

        let mut reader = LogReader::open(&path).unwrap();
        reader.seek_to(off2).unwrap();
        let record = reader.read_next().unwrap().unwrap();
        assert_eq!(record.key, b"r2");
        assert_eq!(record.value, Some(b"v2".to_vec()));

        // read_next should return r3
        let record3 = reader.read_next().unwrap().unwrap();
        assert_eq!(record3.key, b"r3");

        // read_next at EOF should return None
        assert!(reader.read_next().unwrap().is_none());
    }

    // ------------------------------------------------------------------
    // validate_kv_sizes
    // ------------------------------------------------------------------

    #[test]
    fn validate_kv_sizes_accepts_valid() {
        assert!(validate_kv_sizes(b"key", b"value").is_ok());
        assert!(validate_kv_sizes(b"", b"").is_ok());
        assert!(validate_kv_sizes(&vec![0u8; 65535], b"v").is_ok()); // max key
    }

    #[test]
    fn validate_kv_sizes_rejects_oversized_key() {
        let big_key = vec![0u8; 65536]; // u16::MAX + 1
        let result = validate_kv_sizes(&big_key, b"v");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("key too large"));
    }

    #[test]
    fn validate_kv_sizes_rejects_oversized_value() {
        let big_value = vec![0u8; 512 * 1024 * 1024 + 1]; // 512 MiB + 1
        let result = validate_kv_sizes(b"k", &big_value);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("value too large"));
    }

    // ------------------------------------------------------------------
    // write_batch_preencoded
    // ------------------------------------------------------------------

    #[test]
    fn write_batch_preencoded_roundtrip() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();

        let enc1 = encode_record(b"pre1", b"val1", 0);
        let enc2 = encode_record(b"pre2", b"val2", 42);
        let enc3 = encode_tombstone(b"pre3");

        let refs: Vec<&[u8]> = vec![&enc1, &enc2, &enc3];
        let offsets = w.write_batch_preencoded(&refs).unwrap();
        assert_eq!(offsets.len(), 3);

        let mut reader = LogReader::open(&path).unwrap();
        let r1 = reader.read_at(offsets[0]).unwrap().unwrap();
        assert_eq!(r1.key, b"pre1");
        assert_eq!(r1.value, Some(b"val1".to_vec()));

        let r2 = reader.read_at(offsets[1]).unwrap().unwrap();
        assert_eq!(r2.key, b"pre2");
        assert_eq!(r2.expire_at_ms, 42);

        let r3 = reader.read_at(offsets[2]).unwrap().unwrap();
        assert_eq!(r3.key, b"pre3");
        assert!(
            r3.value.is_none(),
            "preencoded tombstone must decode as None"
        );
    }

    // ------------------------------------------------------------------
    // File ID preserved in LogWriter
    // ------------------------------------------------------------------

    #[test]
    fn file_id_preserved() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let w = LogWriter::open(&path, 12345).unwrap();
        assert_eq!(w.file_id, 12345);

        let w2 = LogWriter::open_small(&path, 99999).unwrap();
        assert_eq!(w2.file_id, 99999);
    }
