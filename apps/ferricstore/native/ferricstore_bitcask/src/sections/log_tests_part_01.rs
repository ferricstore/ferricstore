    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn temp_dir() -> TempDir {
        tempfile::TempDir::new().expect("tmp dir")
    }

    // --- encoding round-trips ---

    #[test]
    fn encode_decode_live_record() {
        let key = b"hello";
        let value = b"world";
        let encoded = encode_record(key, value, 0);

        let mut cursor = io::Cursor::new(&encoded);
        let record = read_next_record(&mut cursor).unwrap().unwrap();

        assert_eq!(record.key, key);
        assert_eq!(record.value, Some(value.to_vec()));
        assert_eq!(record.expire_at_ms, 0);
    }

    #[test]
    fn encode_decode_tombstone() {
        let key = b"dead";
        let encoded = encode_tombstone(key);

        let mut cursor = io::Cursor::new(&encoded);
        let record = read_next_record(&mut cursor).unwrap().unwrap();

        assert_eq!(record.key, key);
        assert!(record.value.is_none(), "tombstone must have None value");
    }

    #[test]
    fn encode_decode_with_expiry() {
        let encoded = encode_record(b"ttl", b"val", 99_999);
        let mut cursor = io::Cursor::new(&encoded);
        let record = read_next_record(&mut cursor).unwrap().unwrap();
        assert_eq!(record.expire_at_ms, 99_999);
    }

    #[test]
    fn crc_mismatch_returns_error() {
        let mut encoded = encode_record(b"k", b"v", 0);
        // flip a byte in the value area
        let last = encoded.len() - 1;
        encoded[last] ^= 0xFF;
        let mut cursor = io::Cursor::new(&encoded);
        assert!(read_next_record(&mut cursor).is_err());
    }

    #[test]
    fn empty_reader_returns_none() {
        let mut cursor = io::Cursor::new(Vec::<u8>::new());
        assert!(read_next_record(&mut cursor).unwrap().is_none());
    }

    // --- LogWriter + LogReader ---

    #[test]
    fn writer_creates_file_and_reader_reads_back() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");

        let mut writer = LogWriter::open(&path, 1).unwrap();
        let offset = writer.write(b"foo", b"bar", 0).unwrap();
        writer.sync().unwrap();

        assert_eq!(offset, 0, "first write starts at offset 0");

        let mut reader = LogReader::open(&path).unwrap();
        let record = reader.read_at(0).unwrap().unwrap();
        assert_eq!(record.key, b"foo");
        assert_eq!(record.value, Some(b"bar".to_vec()));
    }

    #[test]
    fn writer_returns_correct_offsets() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut writer = LogWriter::open(&path, 1).unwrap();

        let off0 = writer.write(b"a", b"1", 0).unwrap();
        let off1 = writer.write(b"bb", b"22", 0).unwrap();
        writer.sync().unwrap();

        assert_eq!(off0, 0);
        // second record starts right after first: HEADER(26) + key(1) + value(1) = 28
        assert_eq!(off1, (HEADER_SIZE + 1 + 1) as u64);
    }

    #[test]
    fn reader_read_at_specific_offset() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut writer = LogWriter::open(&path, 1).unwrap();
        writer.write(b"first", b"aaa", 0).unwrap();
        let off2 = writer.write(b"second", b"bbb", 0).unwrap();
        writer.sync().unwrap();

        let mut reader = LogReader::open(&path).unwrap();
        let record = reader.read_at(off2).unwrap().unwrap();
        assert_eq!(record.key, b"second");
        assert_eq!(record.value, Some(b"bbb".to_vec()));
    }

    #[test]
    fn reader_read_at_eof_returns_none() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut writer = LogWriter::open(&path, 1).unwrap();
        writer.write(b"k", b"v", 0).unwrap();
        writer.sync().unwrap();

        let file_size = fs::metadata(&path).unwrap().len();
        let mut reader = LogReader::open(&path).unwrap();
        assert!(reader.read_at(file_size).unwrap().is_none());
    }

    #[test]
    fn iter_from_start_returns_all_records_in_order() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut writer = LogWriter::open(&path, 1).unwrap();
        writer.write(b"a", b"1", 0).unwrap();
        writer.write(b"b", b"2", 0).unwrap();
        writer.write_tombstone(b"a").unwrap();
        writer.sync().unwrap();

        let mut reader = LogReader::open(&path).unwrap();
        let records = reader.iter_from_start().unwrap();

        assert_eq!(records.len(), 3);
        assert_eq!(records[0].key, b"a");
        assert_eq!(records[0].value, Some(b"1".to_vec()));
        assert_eq!(records[1].key, b"b");
        assert_eq!(records[2].key, b"a");
        assert!(records[2].value.is_none(), "tombstone");
    }

    #[test]
    fn writer_open_existing_file_appends() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");

        {
            let mut w = LogWriter::open(&path, 1).unwrap();
            w.write(b"existing", b"val", 0).unwrap();
            w.sync().unwrap();
        }

        // reopen and write more
        {
            let mut w = LogWriter::open(&path, 1).unwrap();
            w.write(b"new", b"val2", 0).unwrap();
            w.sync().unwrap();
        }

        let mut reader = LogReader::open(&path).unwrap();
        let records = reader.iter_from_start().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].key, b"existing");
        assert_eq!(records[1].key, b"new");
    }

    // ------------------------------------------------------------------
    // Edge cases
    // ------------------------------------------------------------------

    #[test]
    fn zero_length_key_roundtrips() {
        let encoded = encode_record(b"", b"value", 0);
        let mut cursor = io::Cursor::new(&encoded);
        let r = read_next_record(&mut cursor).unwrap().unwrap();
        assert!(r.key.is_empty());
        assert_eq!(r.value, Some(b"value".to_vec()));
    }

    #[test]
    fn zero_length_value_is_valid_not_tombstone() {
        // value_size=0 is a genuine empty value. Tombstones use TOMBSTONE (u32::MAX).
        let encoded = encode_record(b"k", b"", 0);
        let mut cursor = io::Cursor::new(&encoded);
        let r = read_next_record(&mut cursor).unwrap().unwrap();
        assert_eq!(
            r.value,
            Some(vec![]),
            "empty value must roundtrip as Some(vec![])"
        );
    }

    #[test]
    fn large_value_roundtrips() {
        let key = b"bigkey";
        let value = vec![0xABu8; 64 * 1024]; // 64 KiB
        let encoded = encode_record(key, &value, 0);
        let mut cursor = io::Cursor::new(&encoded);
        let r = read_next_record(&mut cursor).unwrap().unwrap();
        assert_eq!(r.key, key);
        assert_eq!(r.value.as_deref(), Some(value.as_slice()));
    }

    #[test]
    fn non_utf8_key_and_value_roundtrip() {
        let key = vec![0xFF, 0x00, 0xFE];
        let value = vec![0x01, 0x02, 0x03];
        let encoded = encode_record(&key, &value, 12345);
        let mut cursor = io::Cursor::new(&encoded);
        let r = read_next_record(&mut cursor).unwrap().unwrap();
        assert_eq!(r.key, key);
        assert_eq!(r.value, Some(value));
        assert_eq!(r.expire_at_ms, 12345);
    }

    #[test]
    fn truncated_header_is_not_silently_treated_as_clean_eof() {
        // A partial header is a torn record, not a clean EOF. Tolerant iterators
        // may stop at this condition, but strict readers must surface it.
        let partial = vec![0u8; 10];
        let mut cursor = io::Cursor::new(partial);
        let error = read_next_record(&mut cursor).unwrap_err();

        assert!(error.to_string().contains("truncated record header"));
    }

    #[test]
    fn truncated_after_header_returns_error_or_none() {
        // Write a valid header claiming key_size=5, value_size=5, but provide
        // only the header bytes — reading key should fail or return None.
        let mut header = vec![0u8; HEADER_SIZE];
        // key_size = 5 at bytes 20..22
        header[20] = 5;
        header[21] = 0;
        // value_size = 5 at bytes 22..26
        header[22] = 5;
        header[23] = 0;
        header[24] = 0;
        header[25] = 0;
        // Don't provide key/value bytes — truncated
        let mut cursor = io::Cursor::new(header);
        // read_exact on key will hit UnexpectedEof → error
        assert!(read_next_record(&mut cursor).is_err());
    }

    #[test]
    fn read_next_record_rejects_large_truncated_body_before_full_allocation() {
        let mut header = vec![0u8; HEADER_SIZE];
        header[22..26].copy_from_slice(&(1024_u32 * 1024).to_le_bytes());

        let mut cursor = io::Cursor::new(header);
        let err = read_next_record(&mut cursor).unwrap_err();

        assert!(
            err.to_string().contains("truncated record value"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn pread_record_rejects_large_body_beyond_file_end_before_allocation() {
        let dir = temp_dir();
        let path = dir.path().join("huge_truncated.log");
        let mut header = vec![0u8; HEADER_SIZE];
        header[22..26].copy_from_slice(&(1024_u32 * 1024).to_le_bytes());
        fs::write(&path, &header).unwrap();

        let file = File::open(&path).unwrap();
        let err = pread_record_from_file(&file, 0).unwrap_err();

        assert!(
            err.to_string().contains("extends past end"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn metadata_iter_does_not_materialize_large_values() {
        let dir = temp_dir();
        let path = dir.path().join("metadata_scan.log");
        let mut writer = LogWriter::open(&path, 1).unwrap();

        let large = vec![0xAB; 2 * 1024 * 1024];
        writer.write(b"large", &large, 123).unwrap();
        writer.write(b"small", b"value", 456).unwrap();
        drop(writer);

        let mut reader = LogReader::open(&path).unwrap();
        let records = reader.iter_metadata_from_start_tolerant().unwrap();

        assert_eq!(records.len(), 2);
        assert_eq!(records[0].key, b"large");
        assert_eq!(records[0].value_size, large.len() as u32);
        assert_eq!(records[0].expire_at_ms, 123);
        assert!(!records[0].is_tombstone);
        assert_eq!(
            records[0].record_size,
            HEADER_SIZE as u64 + 5 + large.len() as u64
        );

        assert_eq!(records[1].key, b"small");
        assert_eq!(records[1].value_size, 5);
        assert_eq!(records[1].expire_at_ms, 456);
        assert_eq!(records[1].offset, records[0].record_size);
    }

    #[test]
    fn metadata_iter_from_offset_uses_absolute_offsets() {
        let dir = temp_dir();
        let path = dir.path().join("metadata_scan_offset.log");
        let mut writer = LogWriter::open(&path, 1).unwrap();

        let first = writer.write(b"k1", b"v1", 0).unwrap();
        let second = writer.write(b"k2", b"v2", 789).unwrap();
        drop(writer);

        let mut reader = LogReader::open(&path).unwrap();
        let records = reader.iter_metadata_from_offset_tolerant(second).unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key, b"k2");
        assert_eq!(records[0].offset, second);
        assert_eq!(records[0].value_size, 2);
        assert_eq!(records[0].expire_at_ms, 789);
        assert_eq!(records[0].record_size, HEADER_SIZE as u64 + 2 + 2);
        assert!(second > first);
    }

    #[test]
    fn writer_offset_tracks_correctly_across_multiple_writes() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();

        let mut expected_offset = 0u64;
        for i in 0u8..10 {
            let key = vec![i];
            let value = vec![i; i as usize + 1]; // variable-length values
            let off = w.write(&key, &value, 0).unwrap();
            assert_eq!(off, expected_offset);
            expected_offset += (HEADER_SIZE + 1 + value.len()) as u64;
        }
        assert_eq!(w.offset, expected_offset);
    }

    #[test]
    fn multiple_syncs_do_not_corrupt_data() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();

        for i in 0u8..5 {
            w.write(&[i], &[i, i], 0).unwrap();
            w.sync().unwrap(); // fsync after every write
        }

        let mut r = LogReader::open(&path).unwrap();
        let records = r.iter_from_start().unwrap();
        assert_eq!(records.len(), 5);
        for (i, rec) in records.iter().enumerate() {
            assert_eq!(rec.key, vec![i as u8]);
            assert_eq!(rec.value, Some(vec![i as u8, i as u8]));
        }
    }

    #[test]
    fn crc_corruption_in_key_area_detected() {
        let mut encoded = encode_record(b"hello", b"world", 0);
        // Flip a byte in the key area (after the 26-byte header)
        encoded[HEADER_SIZE] ^= 0xFF;
        let mut cursor = io::Cursor::new(encoded);
        assert!(read_next_record(&mut cursor).is_err());
    }

    #[test]
    fn writer_reports_correct_offset_after_reopen() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");

        let first_offset;
        {
            let mut w = LogWriter::open(&path, 1).unwrap();
            first_offset = w.write(b"key", b"val", 0).unwrap();
            w.sync().unwrap();
            // offset after write
            assert_eq!(w.offset, (HEADER_SIZE + 3 + 3) as u64);
        }

        // Reopen — writer should resume at the end of the file
        let w2 = LogWriter::open(&path, 1).unwrap();
        assert_eq!(w2.offset, (HEADER_SIZE + 3 + 3) as u64);
        let _ = first_offset;
    }

    #[test]
    fn tombstone_followed_by_live_record_both_readable() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        w.write_tombstone(b"k").unwrap();
        w.write(b"k", b"v", 0).unwrap();
        w.sync().unwrap();

        let mut reader = LogReader::open(&path).unwrap();
        let records = reader.iter_from_start().unwrap();

        assert_eq!(records.len(), 2);
        assert!(records[0].value.is_none(), "first record is tombstone");
        assert_eq!(records[1].key, b"k");
        assert_eq!(
            records[1].value,
            Some(b"v".to_vec()),
            "second record is live"
        );
    }

    #[test]
    fn iter_from_start_empty_file_returns_empty_vec() {
        let dir = temp_dir();
        let path = dir.path().join("empty.log");
        // Create the file with no writes
        LogWriter::open(&path, 1).unwrap();

        let mut reader = LogReader::open(&path).unwrap();
        let records = reader.iter_from_start().unwrap();
        assert_eq!(records.len(), 0);
    }

    #[test]
    fn read_at_middle_of_record_returns_error() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        w.write(b"k", b"v", 0).unwrap();
        w.sync().unwrap();

        // Offset 1 is in the middle of the header — CRC will not match
        // Either an Err (CRC mismatch) or Ok(None) if it looks like EOF are
        // acceptable. What must NOT happen is a successful decode of a valid record.
        let mut reader2 = LogReader::open(&path).unwrap();
        // Must be either an error OR None — never Ok(Some(valid record))
        match reader2.read_at(1) {
            Err(_) | Ok(None) => {} // CRC mismatch, IO error, or EOF — all acceptable
            Ok(Some(rec)) => panic!(
                "unexpected successful decode at mid-record: key={:?}",
                rec.key
            ),
        }
    }

    #[test]
    fn write_max_key_size_65535_bytes() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let big_key = vec![0x42u8; 65535]; // u16::MAX bytes
        let mut w = LogWriter::open(&path, 1).unwrap();
        w.write(&big_key, b"v", 0).unwrap();
        w.sync().unwrap();

        let mut reader = LogReader::open(&path).unwrap();
        let records = reader.iter_from_start().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key.len(), 65535);
        assert_eq!(records[0].value, Some(b"v".to_vec()));
    }

    #[test]
    fn multiple_tombstones_for_same_key_all_decoded() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        w.write_tombstone(b"k").unwrap();
        w.write_tombstone(b"k").unwrap();
        w.write_tombstone(b"k").unwrap();
        w.sync().unwrap();

        let mut reader = LogReader::open(&path).unwrap();
        let records = reader.iter_from_start().unwrap();
        assert_eq!(records.len(), 3);
        for rec in &records {
            assert!(rec.value.is_none(), "all three records must be tombstones");
        }
    }

    #[test]
    fn timestamp_is_nonzero_after_write() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        w.write(b"k", b"v", 0).unwrap();
        w.sync().unwrap();

        let mut reader = LogReader::open(&path).unwrap();
        let record = reader.read_at(0).unwrap().unwrap();
        assert!(
            record.timestamp_ms > 0,
            "timestamp_ms must be non-zero after a write"
        );
    }

    // ------------------------------------------------------------------
    // CRC32 function properties
    // ------------------------------------------------------------------

    #[test]
    fn crc32_different_bytes_produce_different_checksums() {
        assert_ne!(crc32(b"hello"), crc32(b"world"));
    }

    #[test]
    fn crc32_empty_slice_is_consistent() {
        assert_eq!(
            crc32(b""),
            crc32(b""),
            "crc32 of empty slice must be deterministic"
        );
    }

    #[test]
    fn crc32_single_byte_flip_changes_checksum() {
        let mut data = b"abcdef".to_vec();
        let original = crc32(&data);
        data[3] ^= 0xFF;
        assert_ne!(
            crc32(&data),
            original,
            "flipping one byte must change the checksum"
        );
    }

    // ------------------------------------------------------------------
    // Record encoding/decoding
    // ------------------------------------------------------------------

    #[test]
    fn encode_and_decode_record_round_trip() {
        let key = b"roundtrip_key";
        let value = b"roundtrip_value";
        let expire_at = 987_654_321u64;
        let encoded = encode_record(key, value, expire_at);

        let mut cursor = io::Cursor::new(&encoded);
        let record = read_next_record(&mut cursor).unwrap().unwrap();

        assert_eq!(record.key, key, "key must round-trip");
        assert_eq!(record.value, Some(value.to_vec()), "value must round-trip");
        assert_eq!(
            record.expire_at_ms, expire_at,
            "expire_at_ms must round-trip"
        );
        assert!(record.timestamp_ms > 0, "timestamp_ms must be set");
    }

    #[test]
    fn encode_tombstone_value_size_is_sentinel() {
        // encode_tombstone writes value_size = TOMBSTONE (u32::MAX) as sentinel.
        let key = b"tomb_key";
        let encoded = encode_tombstone(key);
        let value_size = u32::from_le_bytes(encoded[22..26].try_into().unwrap());
        assert_eq!(
            value_size, TOMBSTONE,
            "tombstone must encode value_size=TOMBSTONE"
        );
        // No value bytes after the key
        assert_eq!(encoded.len(), HEADER_SIZE + key.len());
    }

    #[test]
    fn decode_truncated_record_returns_error() {
        let encoded = encode_record(b"key_trunc", b"val_trunc", 0);
        // Truncate by 1 byte from the end.
        let truncated = &encoded[..encoded.len() - 1];
        let mut cursor = io::Cursor::new(truncated);
        // Should return Err (CRC mismatch or UnexpectedEof on value read).
        // The header is intact with key_size and value_size set, so it will try
        // to read the full value and hit UnexpectedEof → Err.
        assert!(
            read_next_record(&mut cursor).is_err(),
            "truncated record must return Err"
        );
    }

    #[test]
    fn decode_empty_buffer_returns_none() {
        let mut cursor = io::Cursor::new(Vec::<u8>::new());
        assert!(
            read_next_record(&mut cursor).unwrap().is_none(),
            "empty buffer must return Ok(None)"
        );
    }

    #[test]
    fn decode_record_with_flipped_crc_field_returns_error() {
        let mut encoded = encode_record(b"flip_crc", b"val", 0);
        // Flip byte 0 — this is the first byte of the stored CRC field.
        encoded[0] ^= 0xFF;
        let mut cursor = io::Cursor::new(&encoded);
        assert!(
            read_next_record(&mut cursor).is_err(),
            "flipped CRC field byte must trigger CRC mismatch error"
        );
    }

    // ------------------------------------------------------------------
    // iter_from_start_tolerant
    // ------------------------------------------------------------------

    #[test]
    fn tolerant_iter_rejects_crc_corruption() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        let off3 = w.write(b"k1", b"v1", 0).unwrap();
        let off4 = w.write(b"k2", b"v2", 0).unwrap();
        let off5 = w.write(b"k3", b"v3", 0).unwrap();
        w.sync().unwrap();
        drop(w);
        let _ = (off3, off4, off5);

        // Corrupt the start of the 2nd record's key area to break its CRC.
        // The 1st record occupies bytes 0..(HEADER_SIZE+2+2)=30.
        {
            use io::Write as _;
            use std::io::Seek;
            let flip_pos = (HEADER_SIZE + 2 + 2) as u64 + HEADER_SIZE as u64;
            let mut f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
            f.seek(io::SeekFrom::Start(flip_pos)).unwrap();
            f.write_all(&[0xFF]).unwrap();
        }

        let mut reader = LogReader::open(&path).unwrap();
        let error = reader.iter_from_start_tolerant().unwrap_err();
        assert!(
            error.0.contains("CRC mismatch"),
            "integrity failures must not be classified as a torn tail: {error}"
        );
    }

    #[test]
    fn tolerant_iter_stops_at_truncated_tail() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        w.write(b"r1", b"v1", 0).unwrap();
        w.write(b"r2", b"v2", 0).unwrap();
        let _off3 = w.write(b"r3", b"v3", 0).unwrap();
        w.sync().unwrap();
        drop(w);

        // Truncate the last 5 bytes — partial tail record.
        let size = fs::metadata(&path).unwrap().len();
        {
            let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
            f.set_len(size - 5).unwrap();
        }

        let mut reader = LogReader::open(&path).unwrap();
        let records = reader.iter_from_start_tolerant().unwrap();
        // Either 2 (r1, r2) or 3 depending on whether the truncation hits the
        // 3rd record's header or body. In either case must be ≤ 3 and ≥ 2.
        assert!(
            records.len() == 2 || records.len() == 3,
            "tolerant iter must return 2 or 3 records with truncated tail; got {}",
            records.len()
        );
        // The first two must be intact.
        assert_eq!(records[0].key, b"r1");
        assert_eq!(records[1].key, b"r2");
    }

    #[test]
    fn tolerant_iter_returns_all_records_when_no_corruption() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        for i in 0u8..10 {
            w.write(&[i], &[i, i], 0).unwrap();
        }
        w.sync().unwrap();

        let mut reader = LogReader::open(&path).unwrap();
        let records = reader.iter_from_start_tolerant().unwrap();
        assert_eq!(
            records.len(),
            10,
            "tolerant iter must return all 10 valid records"
        );
    }

    #[test]
    fn tolerant_iter_from_offset_returns_only_tail_records() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        let off1 = w.write(b"k1", b"v1", 0).unwrap();
        let off2 = w.write(b"k2", b"v2", 0).unwrap();
        let off3 = w.write(b"k3", b"v3", 0).unwrap();
        w.sync().unwrap();

        assert_eq!(off1, 0);

        let mut reader = LogReader::open(&path).unwrap();
        let records = reader.iter_from_offset_tolerant(off2).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].key, b"k2");
        assert_eq!(records[0].value.as_deref(), Some(&b"v2"[..]));
        assert_eq!(records[1].key, b"k3");
        assert_eq!(records[1].value.as_deref(), Some(&b"v3"[..]));

        let mut reader = LogReader::open(&path).unwrap();
        let records = reader.iter_from_offset_tolerant(off3).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key, b"k3");
    }

    // ------------------------------------------------------------------
    // write_batch
    // ------------------------------------------------------------------

    #[test]
    fn write_batch_empty_is_ok() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        let results = w.write_batch(&[]).unwrap();
        assert!(
            results.is_empty(),
            "write_batch with empty slice must return empty vec"
        );
        assert_eq!(w.offset, 0, "offset must stay 0 after empty batch");
    }

    #[test]
    fn write_batch_offsets_are_monotonically_increasing() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        let entries: Vec<(&[u8], &[u8], u64)> = vec![
            (b"a", b"1", 0),
            (b"bb", b"22", 0),
            (b"ccc", b"333", 0),
            (b"dddd", b"4444", 0),
            (b"eeeee", b"55555", 0),
        ];
        let results = w.write_batch(&entries).unwrap();
        assert_eq!(results.len(), 5);
        let offsets: Vec<u64> = results.iter().map(|(off, _)| *off).collect();
        for window in offsets.windows(2) {
            assert!(
                window[1] > window[0],
                "batch offsets must be strictly increasing: {offsets:?}"
            );
        }
    }

    #[test]
    fn write_batch_all_records_readable() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        let entries: Vec<(&[u8], &[u8], u64)> = vec![
            (b"batch_k1", b"batch_v1", 0),
            (b"batch_k2", b"batch_v2", 1_000_000),
            (b"batch_k3", b"batch_v3", 0),
        ];
        let results = w.write_batch(&entries).unwrap();

        let mut reader = LogReader::open(&path).unwrap();
        for (i, (off, _)) in results.iter().enumerate() {
            let record = reader.read_at(*off).unwrap().unwrap();
            assert_eq!(record.key, entries[i].0, "key at offset {off} must match");
            assert_eq!(
                record.value,
                Some(entries[i].1.to_vec()),
                "value at offset {off} must match"
            );
        }
    }
