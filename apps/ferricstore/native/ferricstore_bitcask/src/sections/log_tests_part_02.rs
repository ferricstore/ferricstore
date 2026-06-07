    #[test]
    fn write_batch_rejects_oversized_key_without_writing() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        let too_large_key = vec![0x42; usize::from(u16::MAX) + 1];
        let entries: Vec<(&[u8], &[u8], u64)> =
            vec![(b"valid", b"v", 0), (&too_large_key, b"v", 0)];

        let err = w.write_batch(&entries).unwrap_err();

        assert!(err.to_string().contains("key too large"));
        assert_eq!(w.offset, 0);
        assert_eq!(fs::metadata(&path).unwrap().len(), 0);
    }

    // ------------------------------------------------------------------
    // write_batch_nosync
    // ------------------------------------------------------------------

    #[test]
    fn write_batch_nosync_returns_correct_offsets() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        let entries: Vec<(&[u8], &[u8], u64)> = vec![(b"nk1", b"nv1", 0), (b"nk2", b"nv22", 0)];
        let results = w.write_batch_nosync(&entries).unwrap();
        assert_eq!(results.len(), 2);

        let offsets: Vec<u64> = results.iter().map(|(off, _)| *off).collect();
        assert_eq!(offsets[0], 0);
        assert!(offsets[1] > offsets[0], "second offset must be after first");

        // Values should be readable (flushed to page cache)
        let mut reader = LogReader::open(&path).unwrap();
        let r1 = reader.read_at(offsets[0]).unwrap().unwrap();
        assert_eq!(r1.key, b"nk1");
        assert_eq!(r1.value, Some(b"nv1".to_vec()));

        let r2 = reader.read_at(offsets[1]).unwrap().unwrap();
        assert_eq!(r2.key, b"nk2");
        assert_eq!(r2.value, Some(b"nv22".to_vec()));
    }

    #[test]
    fn write_batch_nosync_empty_is_ok() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        let results = w.write_batch_nosync(&[]).unwrap();
        assert!(results.is_empty());
        assert_eq!(w.offset, 0);
    }

    #[test]
    fn write_batch_nosync_then_sync_makes_durable() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        let entries: Vec<(&[u8], &[u8], u64)> = vec![(b"dk1", b"dv1", 0), (b"dk2", b"dv2", 0)];
        let results = w.write_batch_nosync(&entries).unwrap();
        // Now fsync
        w.sync().unwrap();

        // Verify data persists
        let mut reader = LogReader::open(&path).unwrap();
        let records = reader.iter_from_start().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].key, b"dk1");
        assert_eq!(records[1].key, b"dk2");
        let _ = results;
    }

    #[test]
    fn write_batch_nosync_rejects_oversized_key_without_writing() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        let too_large_key = vec![0x42; usize::from(u16::MAX) + 1];
        let entries: Vec<(&[u8], &[u8], u64)> =
            vec![(b"valid", b"v", 0), (&too_large_key, b"v", 0)];

        let err = w.write_batch_nosync(&entries).unwrap_err();

        assert!(err.to_string().contains("key too large"));
        assert_eq!(w.offset, 0);
        assert_eq!(fs::metadata(&path).unwrap().len(), 0);
    }

    #[test]
    fn write_ops_batch_nosync_supports_tombstones_in_order() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        let entries = vec![
            BatchWrite::Put {
                key: b"k1",
                value: b"v1",
                expire_at_ms: 0,
            },
            BatchWrite::Delete { key: b"k1" },
            BatchWrite::Put {
                key: b"empty",
                value: b"",
                expire_at_ms: 0,
            },
        ];

        let results = w.write_ops_batch_nosync(&entries).unwrap();
        assert_eq!(results.len(), 3);

        let off1 = match results[0] {
            BatchWriteResult::Put { offset, value_len } => {
                assert_eq!(value_len, 2);
                offset
            }
            BatchWriteResult::Delete { .. } => panic!("first op must be put"),
        };

        let off2 = match results[1] {
            BatchWriteResult::Delete {
                offset,
                record_size,
            } => {
                assert_eq!(record_size, HEADER_SIZE + 2);
                offset
            }
            BatchWriteResult::Put { .. } => panic!("second op must be delete"),
        };

        let off3 = match results[2] {
            BatchWriteResult::Put { offset, value_len } => {
                assert_eq!(value_len, 0);
                offset
            }
            BatchWriteResult::Delete { .. } => panic!("third op must be put"),
        };

        assert_eq!(off1, 0);
        assert!(off2 > off1);
        assert!(off3 > off2);

        let mut reader = LogReader::open(&path).unwrap();
        let records = reader.iter_from_start().unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].key, b"k1");
        assert_eq!(records[0].value, Some(b"v1".to_vec()));
        assert_eq!(records[1].key, b"k1");
        assert_eq!(records[1].value, None);
        assert_eq!(records[2].key, b"empty");
        assert_eq!(records[2].value, Some(Vec::new()));
    }

    #[test]
    fn write_ops_batch_nosync_empty_preserves_offset() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();

        let results = w.write_ops_batch_nosync(&[]).unwrap();

        assert!(results.is_empty());
        assert_eq!(w.offset, 0);
    }

    #[test]
    fn write_ops_batch_nosync_single_tombstone_is_readable_as_none() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        let entries = vec![BatchWrite::Delete { key: b"gone" }];

        let results = w.write_ops_batch_nosync(&entries).unwrap();

        assert_eq!(
            results,
            vec![BatchWriteResult::Delete {
                offset: 0,
                record_size: HEADER_SIZE + 4,
            }]
        );

        let mut reader = LogReader::open(&path).unwrap();
        let record = reader.read_at(0).unwrap().unwrap();
        assert_eq!(record.key, b"gone");
        assert_eq!(record.value, None);
        assert_eq!(record.expire_at_ms, 0);
    }

    #[test]
    fn write_ops_batch_nosync_appends_after_existing_records() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        let first_offset = w.write(b"seed", b"value", 0).unwrap();
        assert_eq!(first_offset, 0);
        let expected_next = (HEADER_SIZE + b"seed".len() + b"value".len()) as u64;

        let entries = vec![
            BatchWrite::Delete { key: b"seed" },
            BatchWrite::Put {
                key: b"next",
                value: b"v",
                expire_at_ms: 123,
            },
        ];

        let results = w.write_ops_batch_nosync(&entries).unwrap();

        assert_eq!(
            results[0],
            BatchWriteResult::Delete {
                offset: expected_next,
                record_size: HEADER_SIZE + 4,
            }
        );

        let second_offset = match results[1] {
            BatchWriteResult::Put { offset, value_len } => {
                assert_eq!(value_len, 1);
                offset
            }
            BatchWriteResult::Delete { .. } => panic!("second op must be put"),
        };

        assert!(second_offset > expected_next);

        let mut reader = LogReader::open(&path).unwrap();
        let records = reader.iter_from_start().unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].key, b"seed");
        assert_eq!(records[0].value, Some(b"value".to_vec()));
        assert_eq!(records[1].key, b"seed");
        assert_eq!(records[1].value, None);
        assert_eq!(records[2].key, b"next");
        assert_eq!(records[2].value, Some(b"v".to_vec()));
        assert_eq!(records[2].expire_at_ms, 123);
    }

    #[test]
    fn write_ops_batch_nosync_repeated_key_preserves_last_record_order() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        let entries = vec![
            BatchWrite::Put {
                key: b"k",
                value: b"v1",
                expire_at_ms: 0,
            },
            BatchWrite::Delete { key: b"k" },
            BatchWrite::Put {
                key: b"k",
                value: b"v2",
                expire_at_ms: 0,
            },
            BatchWrite::Delete { key: b"k" },
        ];

        let results = w.write_ops_batch_nosync(&entries).unwrap();
        assert_eq!(results.len(), 4);

        let mut offsets = Vec::new();
        for result in results {
            offsets.push(match result {
                BatchWriteResult::Put { offset, .. } | BatchWriteResult::Delete { offset, .. } => {
                    offset
                }
            });
        }

        assert!(offsets.windows(2).all(|pair| pair[0] < pair[1]));

        let mut reader = LogReader::open(&path).unwrap();
        let records = reader.iter_from_start().unwrap();
        assert_eq!(records.len(), 4);
        assert_eq!(records[0].value, Some(b"v1".to_vec()));
        assert_eq!(records[1].value, None);
        assert_eq!(records[2].value, Some(b"v2".to_vec()));
        assert_eq!(records[3].value, None);
    }

    #[test]
    fn write_ops_batch_nosync_empty_value_is_not_tombstone() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        let entries = vec![
            BatchWrite::Put {
                key: b"empty",
                value: b"",
                expire_at_ms: 0,
            },
            BatchWrite::Delete { key: b"deleted" },
        ];

        let results = w.write_ops_batch_nosync(&entries).unwrap();

        assert_eq!(
            results[0],
            BatchWriteResult::Put {
                offset: 0,
                value_len: 0,
            }
        );

        let mut reader = LogReader::open(&path).unwrap();
        let records = reader.iter_from_start().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].key, b"empty");
        assert_eq!(records[0].value, Some(Vec::new()));
        assert_eq!(records[1].key, b"deleted");
        assert_eq!(records[1].value, None);
    }

    #[test]
    fn write_ops_batch_nosync_offsets_match_encoded_lengths_and_file_size() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        let entries = vec![
            BatchWrite::Put {
                key: b"aa",
                value: b"111",
                expire_at_ms: 10,
            },
            BatchWrite::Delete { key: b"bbb" },
            BatchWrite::Put {
                key: b"c",
                value: b"2222",
                expire_at_ms: 20,
            },
        ];

        let results = w.write_ops_batch_nosync(&entries).unwrap();

        let first_len = (HEADER_SIZE + 2 + 3) as u64;
        let second_len = (HEADER_SIZE + 3) as u64;
        let third_len = (HEADER_SIZE + 1 + 4) as u64;
        assert_eq!(
            results,
            vec![
                BatchWriteResult::Put {
                    offset: 0,
                    value_len: 3,
                },
                BatchWriteResult::Delete {
                    offset: first_len,
                    record_size: HEADER_SIZE + 3,
                },
                BatchWriteResult::Put {
                    offset: first_len + second_len,
                    value_len: 4,
                },
            ]
        );
        assert_eq!(w.offset, first_len + second_len + third_len);
        assert_eq!(
            fs::metadata(&path).unwrap().len(),
            first_len + second_len + third_len
        );
    }

    #[test]
    fn write_ops_batch_nosync_preserves_empty_binary_keys() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        let entries = vec![
            BatchWrite::Put {
                key: b"",
                value: b"present",
                expire_at_ms: 0,
            },
            BatchWrite::Delete { key: b"" },
        ];

        let results = w.write_ops_batch_nosync(&entries).unwrap();

        assert_eq!(
            results,
            vec![
                BatchWriteResult::Put {
                    offset: 0,
                    value_len: 7,
                },
                BatchWriteResult::Delete {
                    offset: (HEADER_SIZE + 7) as u64,
                    record_size: HEADER_SIZE,
                },
            ]
        );

        let mut reader = LogReader::open(&path).unwrap();
        let records = reader.iter_from_start().unwrap();
        assert_eq!(records.len(), 2);
        assert!(records[0].key.is_empty());
        assert_eq!(records[0].value, Some(b"present".to_vec()));
        assert!(records[1].key.is_empty());
        assert_eq!(records[1].value, None);
    }

    #[test]
    fn write_ops_batch_nosync_pure_delete_batch_returns_all_tombstones() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        let entries = vec![
            BatchWrite::Delete { key: b"a" },
            BatchWrite::Delete { key: b"bb" },
            BatchWrite::Delete { key: b"ccc" },
        ];

        let results = w.write_ops_batch_nosync(&entries).unwrap();

        assert_eq!(
            results,
            vec![
                BatchWriteResult::Delete {
                    offset: 0,
                    record_size: HEADER_SIZE + 1,
                },
                BatchWriteResult::Delete {
                    offset: (HEADER_SIZE + 1) as u64,
                    record_size: HEADER_SIZE + 2,
                },
                BatchWriteResult::Delete {
                    offset: (HEADER_SIZE + 1 + HEADER_SIZE + 2) as u64,
                    record_size: HEADER_SIZE + 3,
                },
            ]
        );

        let mut reader = LogReader::open(&path).unwrap();
        let records = reader.iter_from_start().unwrap();
        assert_eq!(records.len(), 3);
        assert!(records.iter().all(|record| record.value.is_none()));
    }

    #[test]
    fn write_ops_batch_nosync_retains_put_expiry_around_deletes() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        let entries = vec![
            BatchWrite::Put {
                key: b"ttl-max",
                value: b"v1",
                expire_at_ms: u64::MAX,
            },
            BatchWrite::Delete { key: b"ttl-max" },
            BatchWrite::Put {
                key: b"ttl-later",
                value: b"v2",
                expire_at_ms: 1_700_000_000_000,
            },
        ];

        let results = w.write_ops_batch_nosync(&entries).unwrap();
        let mut reader = LogReader::open(&path).unwrap();
        let records = results
            .iter()
            .map(|result| match result {
                BatchWriteResult::Put { offset, .. } | BatchWriteResult::Delete { offset, .. } => {
                    reader.read_at(*offset).unwrap().unwrap()
                }
            })
            .collect::<Vec<_>>();

        assert_eq!(records[0].expire_at_ms, u64::MAX);
        assert_eq!(records[1].expire_at_ms, 0);
        assert_eq!(records[1].value, None);
        assert_eq!(records[2].expire_at_ms, 1_700_000_000_000);
    }

    #[test]
    fn write_ops_batch_nosync_empty_after_existing_record_preserves_offset() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        w.write(b"seed", b"value", 0).unwrap();
        w.sync().unwrap();
        let offset_before = w.offset;

        let results = w.write_ops_batch_nosync(&[]).unwrap();

        assert!(results.is_empty());
        assert_eq!(w.offset, offset_before);
        assert_eq!(fs::metadata(&path).unwrap().len(), offset_before);
    }

    #[test]
    fn write_ops_batch_nosync_accepts_max_key_for_put_and_delete() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        let key = vec![0x42; usize::from(u16::MAX)];
        let entries = vec![
            BatchWrite::Put {
                key: &key,
                value: b"v",
                expire_at_ms: 0,
            },
            BatchWrite::Delete { key: &key },
        ];

        let results = w.write_ops_batch_nosync(&entries).unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(
            results[1],
            BatchWriteResult::Delete {
                offset: (HEADER_SIZE + key.len() + 1) as u64,
                record_size: HEADER_SIZE + key.len(),
            }
        );

        let mut reader = LogReader::open(&path).unwrap();
        let records = reader.iter_from_start().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].key, key);
        assert_eq!(records[0].value, Some(b"v".to_vec()));
        assert_eq!(records[1].key, records[0].key);
        assert_eq!(records[1].value, None);
    }

    #[test]
    fn write_ops_batch_nosync_rejects_oversized_put_key_without_writing() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        let too_large_key = vec![0x42; usize::from(u16::MAX) + 1];
        let entries = vec![
            BatchWrite::Delete { key: b"valid" },
            BatchWrite::Put {
                key: &too_large_key,
                value: b"v",
                expire_at_ms: 0,
            },
        ];

        let err = w.write_ops_batch_nosync(&entries).unwrap_err();

        assert!(err.to_string().contains("key too large"));
        assert_eq!(w.offset, 0);
        assert_eq!(fs::metadata(&path).unwrap().len(), 0);
    }

    #[test]
    fn write_ops_batch_nosync_rejects_oversized_delete_key_without_writing() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        let too_large_key = vec![0x42; usize::from(u16::MAX) + 1];
        let entries = vec![
            BatchWrite::Put {
                key: b"valid",
                value: b"v",
                expire_at_ms: 0,
            },
            BatchWrite::Delete {
                key: &too_large_key,
            },
        ];

        let err = w.write_ops_batch_nosync(&entries).unwrap_err();

        assert!(err.to_string().contains("key too large"));
        assert_eq!(w.offset, 0);
        assert_eq!(fs::metadata(&path).unwrap().len(), 0);
    }

    #[test]
    fn write_tombstone_rejects_oversized_key_without_writing() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        let too_large_key = vec![0x42; usize::from(u16::MAX) + 1];

        let err = w.write_tombstone(&too_large_key).unwrap_err();

        assert!(err.to_string().contains("key too large"));
        assert_eq!(w.offset, 0);
        assert_eq!(fs::metadata(&path).unwrap().len(), 0);
    }

    // ------------------------------------------------------------------
    // write_raw
    // ------------------------------------------------------------------

    #[test]
    fn write_raw_appends_pre_encoded_bytes() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();

        // Encode a record manually
        let encoded = encode_record(b"rawkey", b"rawval", 0);
        let off = w.write_raw(&encoded).unwrap();
        w.sync().unwrap();

        assert_eq!(off, 0);
        assert_eq!(w.offset, encoded.len() as u64);

        // Should be readable
        let mut reader = LogReader::open(&path).unwrap();
        let record = reader.read_at(0).unwrap().unwrap();
        assert_eq!(record.key, b"rawkey");
        assert_eq!(record.value, Some(b"rawval".to_vec()));
    }

    #[test]
    fn write_raw_multiple_records() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();

        let enc1 = encode_record(b"k1", b"v1", 0);
        let enc2 = encode_record(b"k2", b"v2", 42);

        let off1 = w.write_raw(&enc1).unwrap();
        let off2 = w.write_raw(&enc2).unwrap();
        w.sync().unwrap();

        assert_eq!(off1, 0);
        assert_eq!(off2, enc1.len() as u64);

        let mut reader = LogReader::open(&path).unwrap();
        let records = reader.iter_from_start().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].key, b"k1");
        assert_eq!(records[1].key, b"k2");
        assert_eq!(records[1].expire_at_ms, 42);
    }

    // ==================================================================
    // Performance audit fix verification tests (C-1 through C-7)
    // ==================================================================

    // ------------------------------------------------------------------
    // C-1: CRC32 uses crc32fast (hardware-accelerated)
    // ------------------------------------------------------------------

    #[test]
    fn c1_crc32_matches_known_value() {
        // crc32fast uses CRC-32/ISO-HDLC polynomial — verify known test vector
        // The standard CRC-32 of "123456789" is 0xCBF43926
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    }

    #[test]
    fn c1_crc32_empty_data() {
        // CRC-32 of empty input is 0x00000000
        assert_eq!(crc32(b""), 0x0000_0000);
    }

    #[test]
    fn c1_crc32_all_zeros() {
        let data = vec![0u8; 1024];
        let c = crc32(&data);
        // Just verify deterministic and non-zero
        assert_eq!(crc32(&data), c);
        assert_ne!(c, 0);
    }

    #[test]
    fn c1_crc32_all_0xff() {
        let data = vec![0xFFu8; 1024];
        let c = crc32(&data);
        assert_eq!(crc32(&data), c);
        assert_ne!(c, 0);
    }

    #[test]
    fn c1_crc32_single_bit_difference() {
        let a = vec![0u8; 256];
        let mut b = a.clone();
        b[128] = 1;
        assert_ne!(crc32(&a), crc32(&b), "single bit flip must change CRC");
    }

    #[test]
    fn c1_write_read_10k_records_crc_validates() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        for i in 0u32..10_000 {
            let key = format!("key_{i:05}");
            let value = format!("value_{i:05}_padding");
            w.write(key.as_bytes(), value.as_bytes(), 0).unwrap();
        }
        w.sync().unwrap();

        let mut r = LogReader::open(&path).unwrap();
        let records = r.iter_from_start().unwrap();
        assert_eq!(records.len(), 10_000);
        for (i, rec) in records.iter().enumerate() {
            let expected_key = format!("key_{i:05}");
            let expected_value = format!("value_{i:05}_padding");
            assert_eq!(rec.key, expected_key.as_bytes());
            assert_eq!(rec.value, Some(expected_value.into_bytes()));
        }
    }

    #[test]
    fn c1_max_size_data_crc_validates() {
        let key = vec![0x42u8; 65535]; // max key size (u16::MAX)
        let value = vec![0xABu8; 64 * 1024]; // 64 KB value
        let encoded = encode_record(&key, &value, 42);
        let mut cursor = io::Cursor::new(&encoded);
        let r = read_next_record(&mut cursor).unwrap().unwrap();
        assert_eq!(r.key, key);
        assert_eq!(r.value.as_deref(), Some(value.as_slice()));
        assert_eq!(r.expire_at_ms, 42);
    }

    // ------------------------------------------------------------------
    // C-3: read_at uses pread (1 syscall instead of 2)
    // ------------------------------------------------------------------

    #[test]
    fn c3_pread_at_offset_zero() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        w.write(b"k", b"v", 0).unwrap();
        w.sync().unwrap();

        let mut reader = LogReader::open(&path).unwrap();
        let record = reader.read_at(0).unwrap().unwrap();
        assert_eq!(record.key, b"k");
        assert_eq!(record.value, Some(b"v".to_vec()));
    }

    #[test]
    fn c3_pread_at_exact_eof() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        w.write(b"k", b"v", 0).unwrap();
        w.sync().unwrap();
        let file_size = fs::metadata(&path).unwrap().len();

        let mut reader = LogReader::open(&path).unwrap();
        assert!(reader.read_at(file_size).unwrap().is_none());
    }

    #[test]
    fn c3_pread_at_past_eof() {
        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        w.write(b"k", b"v", 0).unwrap();
        w.sync().unwrap();
        let file_size = fs::metadata(&path).unwrap().len();

        let mut reader = LogReader::open(&path).unwrap();
        assert!(reader.read_at(file_size + 1000).unwrap().is_none());
    }

    #[test]
    fn c3_concurrent_reads_different_offsets() {
        use std::sync::Arc;
        use std::thread;

        let dir = temp_dir();
        let path = dir.path().join("data.log");
        let mut w = LogWriter::open(&path, 1).unwrap();
        let mut offsets = Vec::new();
        for i in 0u32..100 {
            let key = format!("key_{i:03}");
            let value = format!("val_{i:03}");
            let off = w.write(key.as_bytes(), value.as_bytes(), 0).unwrap();
            offsets.push((off, key, value));
        }
        w.sync().unwrap();

        let path = Arc::new(path);
        let offsets = Arc::new(offsets);

        let handles: Vec<_> = (0..8)
            .map(|t| {
                let path = Arc::clone(&path);
                let offsets = Arc::clone(&offsets);
                thread::spawn(move || {
                    let mut reader = LogReader::open(&path).unwrap();
                    for i in (t..100).step_by(8) {
                        let (off, ref key, ref value) = offsets[i as usize];
                        let record = reader.read_at(off).unwrap().unwrap();
                        assert_eq!(record.key, key.as_bytes());
                        assert_eq!(record.value, Some(value.as_bytes().to_vec()));
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
    }

    // ------------------------------------------------------------------
    // C-4: encode_record uses a single Vec allocation
    // ------------------------------------------------------------------

