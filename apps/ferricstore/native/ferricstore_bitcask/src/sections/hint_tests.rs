    use super::*;
    use tempfile::TempDir;

    fn tmp() -> TempDir {
        tempfile::TempDir::new().unwrap()
    }

    fn sample_entry(key: &[u8], file_id: u64, offset: u64) -> HintEntry {
        HintEntry {
            file_id,
            offset,
            value_size: 42,
            expire_at_ms: 0,
            key: key.to_vec(),
        }
    }

    #[test]
    fn write_and_read_single_entry() {
        let dir = tmp();
        let path = dir.path().join("data.hint");

        let original = sample_entry(b"hello", 3, 128);

        let mut writer = HintWriter::open(&path).unwrap();
        writer.write_entry(&original).unwrap();
        writer.commit().unwrap();

        let mut reader = HintReader::open(&path).unwrap();
        let entries = reader.read_all().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0], original);
    }

    #[test]
    fn write_and_read_multiple_entries() {
        let dir = tmp();
        let path = dir.path().join("data.hint");

        let entries_in = vec![
            sample_entry(b"alpha", 1, 0),
            sample_entry(b"beta", 1, 100),
            sample_entry(b"gamma", 2, 0),
        ];

        let mut writer = HintWriter::open(&path).unwrap();
        for e in &entries_in {
            writer.write_entry(e).unwrap();
        }
        writer.commit().unwrap();

        let mut reader = HintReader::open(&path).unwrap();
        let entries_out = reader.read_all().unwrap();
        assert_eq!(entries_out, entries_in);
    }

    #[test]
    fn load_into_keydir_populates_entries() {
        let dir = tmp();
        let path = dir.path().join("data.hint");

        let mut writer = HintWriter::open(&path).unwrap();
        writer
            .write_entry(&HintEntry {
                file_id: 5,
                offset: 256,
                value_size: 20,
                expire_at_ms: 99_000,
                key: b"mykey".to_vec(),
            })
            .unwrap();
        writer.commit().unwrap();

        let mut kd = KeyDir::new();
        let mut reader = HintReader::open(&path).unwrap();
        reader.load_into(&mut kd).unwrap();

        let entry = kd.get(b"mykey").unwrap();
        assert_eq!(entry.file_id, 5);
        assert_eq!(entry.offset, 256);
        assert_eq!(entry.value_size, 20);
        assert_eq!(entry.expire_at_ms, 99_000);
    }

    #[test]
    fn load_into_keydir_skips_tombstones() {
        let dir = tmp();
        let path = dir.path().join("data.hint");

        let mut writer = HintWriter::open(&path).unwrap();
        writer
            .write_entry(&HintEntry {
                file_id: 1,
                offset: 0,
                value_size: 10,
                expire_at_ms: 0,
                key: b"live".to_vec(),
            })
            .unwrap();
        writer
            .write_entry(&HintEntry {
                file_id: 2,
                offset: 0,
                value_size: 0, // tombstone
                expire_at_ms: 0,
                key: b"dead".to_vec(),
            })
            .unwrap();
        writer.commit().unwrap();

        let mut kd = KeyDir::new();
        let mut reader = HintReader::open(&path).unwrap();
        reader.load_into(&mut kd).unwrap();

        assert!(kd.get(b"live").is_some());
        assert!(kd.get(b"dead").is_none());
    }

    #[test]
    fn load_into_keydir_later_entry_overwrites_earlier() {
        let dir = tmp();
        let path = dir.path().join("data.hint");

        let mut writer = HintWriter::open(&path).unwrap();
        writer
            .write_entry(&HintEntry {
                file_id: 1,
                offset: 0,
                value_size: 5,
                expire_at_ms: 0,
                key: b"k".to_vec(),
            })
            .unwrap();
        writer
            .write_entry(&HintEntry {
                file_id: 2,
                offset: 500,
                value_size: 8,
                expire_at_ms: 0,
                key: b"k".to_vec(),
            })
            .unwrap();
        writer.commit().unwrap();

        let mut kd = KeyDir::new();
        let mut reader = HintReader::open(&path).unwrap();
        reader.load_into(&mut kd).unwrap();

        let entry = kd.get(b"k").unwrap();
        assert_eq!(entry.file_id, 2);
        assert_eq!(entry.offset, 500);
    }

    #[test]
    fn empty_hint_file_loads_empty_keydir() {
        let dir = tmp();
        let path = dir.path().join("empty.hint");

        let writer = HintWriter::open(&path).unwrap();
        writer.commit().unwrap();

        let mut kd = KeyDir::new();
        let mut reader = HintReader::open(&path).unwrap();
        reader.load_into(&mut kd).unwrap();

        assert!(kd.is_empty());
    }

    // ------------------------------------------------------------------
    // Atomicity tests (Issue 5.2)
    // ------------------------------------------------------------------

    /// Dropping a `HintWriter` without calling `commit` must not leave any
    /// file at the final `.hint` path, and must also clean up the `.hint.tmp`
    /// file (best-effort via `Drop`).
    #[test]
    fn hint_write_is_atomic_on_crash() {
        let dir = tmp();
        let path = dir.path().join("data.hint");
        let tmp_path = dir.path().join("data.hint.tmp");

        // Open a writer and write some entries, but never call commit().
        {
            let mut writer = HintWriter::open(&path).unwrap();
            writer.write_entry(&sample_entry(b"key1", 1, 0)).unwrap();
            writer.write_entry(&sample_entry(b"key2", 1, 64)).unwrap();
            // writer drops here without commit — simulates a crash
        }

        // The final .hint file must NOT exist (was never renamed into place).
        assert!(
            !path.exists(),
            "final .hint must not exist when commit was never called"
        );

        // The .hint.tmp file must also be gone (cleaned up by Drop).
        assert!(
            !tmp_path.exists(),
            ".hint.tmp must be cleaned up by Drop when commit was never called"
        );
    }

    /// Writing a second hint file over an existing one must atomically replace
    /// the content: only the new keys are visible after commit, and no `.tmp`
    /// file remains.
    #[test]
    fn hint_commit_replaces_existing_hint_atomically() {
        let dir = tmp();
        let path = dir.path().join("data.hint");
        let tmp_path = dir.path().join("data.hint.tmp");

        // Write the first hint file with "old_key".
        {
            let mut writer = HintWriter::open(&path).unwrap();
            writer.write_entry(&sample_entry(b"old_key", 1, 0)).unwrap();
            writer.commit().unwrap();
        }

        // Verify "old_key" is readable.
        {
            let mut reader = HintReader::open(&path).unwrap();
            let entries = reader.read_all().unwrap();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].key, b"old_key");
        }

        // Write a second hint file with "new_key" over the same path.
        {
            let mut writer = HintWriter::open(&path).unwrap();
            writer
                .write_entry(&sample_entry(b"new_key", 2, 128))
                .unwrap();
            writer.commit().unwrap();
        }

        // Only "new_key" must be present.
        let mut reader = HintReader::open(&path).unwrap();
        let entries = reader.read_all().unwrap();
        assert_eq!(
            entries.len(),
            1,
            "only new_key should be present after second commit"
        );
        assert_eq!(
            entries[0].key, b"new_key",
            "old_key must be replaced by new_key"
        );

        // No .tmp file should remain after a successful commit.
        assert!(
            !tmp_path.exists(),
            ".hint.tmp must not exist after a successful commit"
        );
    }

    // ------------------------------------------------------------------
    // CRC integrity tests (Issue 5.1)
    // ------------------------------------------------------------------

    /// A valid hint entry round-trips correctly; flipping a byte in the offset
    /// field must cause a CRC mismatch error on re-read.
    #[test]
    fn hint_entry_crc_validated_on_read() {
        let dir = tmp();
        let path = dir.path().join("data.hint");

        let original = sample_entry(b"hello", 3, 128);
        {
            let mut writer = HintWriter::open(&path).unwrap();
            writer.write_entry(&original).unwrap();
            writer.commit().unwrap();
        }

        // Clean read — must succeed.
        {
            let mut reader = HintReader::open(&path).unwrap();
            let entries = reader.read_all().unwrap();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0], original);
        }

        // Corrupt the offset field. The entry layout on disk is:
        //   [crc32: 4][file_id: 8][offset: 8][value_size: 4][expire_at_ms: 8][key_size: 2][key]
        // The offset field starts at byte 4 + 8 = 12.
        {
            use std::fs::OpenOptions;
            use std::io::{Seek, SeekFrom, Write};
            let mut f = OpenOptions::new().write(true).open(&path).unwrap();
            f.seek(SeekFrom::Start(12)).unwrap();
            f.write_all(&[0xFF, 0xFF, 0xFF, 0xFF]).unwrap();
        }

        // Re-read — must return a CRC error.
        let mut reader = HintReader::open(&path).unwrap();
        assert!(
            reader.read_all().is_err(),
            "corrupted offset field must trigger a CRC mismatch error"
        );
    }

    /// Corrupting the `file_id` field (not the key) must also be caught by the CRC.
    #[test]
    fn hint_entry_crc_covers_all_fields() {
        let dir = tmp();
        let path = dir.path().join("data.hint");

        let entry = HintEntry {
            file_id: 42,
            offset: 1024,
            value_size: 7,
            expire_at_ms: 999_000,
            key: b"integrity".to_vec(),
        };
        {
            let mut writer = HintWriter::open(&path).unwrap();
            writer.write_entry(&entry).unwrap();
            writer.commit().unwrap();
        }

        // Verify clean read.
        {
            let mut reader = HintReader::open(&path).unwrap();
            let entries = reader.read_all().unwrap();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0], entry);
        }

        // Corrupt the file_id field. It starts at byte 4 (right after the 4-byte CRC).
        {
            use std::fs::OpenOptions;
            use std::io::{Seek, SeekFrom, Write};
            let mut f = OpenOptions::new().write(true).open(&path).unwrap();
            f.seek(SeekFrom::Start(4)).unwrap();
            f.write_all(&[0xDE, 0xAD, 0xBE, 0xEF]).unwrap();
        }

        let mut reader = HintReader::open(&path).unwrap();
        assert!(
            reader.read_all().is_err(),
            "corrupted file_id field must trigger a CRC mismatch error"
        );
    }

    /// Writing 10 entries and reading them all back must succeed and produce
    /// identical entries in the same order.
    #[test]
    fn hint_multiple_entries_round_trip() {
        let dir = tmp();
        let path = dir.path().join("data.hint");

        let entries_in: Vec<HintEntry> = (0..10)
            .map(|i| HintEntry {
                file_id: i,
                offset: i * 64,
                value_size: (i as u32) + 1,
                expire_at_ms: i * 1_000,
                key: format!("key-{i}").into_bytes(),
            })
            .collect();

        {
            let mut writer = HintWriter::open(&path).unwrap();
            for e in &entries_in {
                writer.write_entry(e).unwrap();
            }
            writer.commit().unwrap();
        }

        let mut reader = HintReader::open(&path).unwrap();
        let entries_out = reader.read_all().unwrap();

        assert_eq!(entries_out.len(), entries_in.len());
        for (a, b) in entries_in.iter().zip(entries_out.iter()) {
            assert_eq!(a, b);
        }
    }

    // ------------------------------------------------------------------
    // CRC integrity (additional)
    // ------------------------------------------------------------------

    /// Write a valid entry, flip a byte in the key area, re-read returns error.
    #[test]
    fn crc32_in_hint_entry_is_validated() {
        let dir = tmp();
        let path = dir.path().join("data.hint");

        let entry = sample_entry(b"validate_me", 7, 512);
        {
            let mut writer = HintWriter::open(&path).unwrap();
            writer.write_entry(&entry).unwrap();
            writer.commit().unwrap();
        }

        // Compute where the key starts:
        // Layout: [crc32: 4][file_id: 8][offset: 8][value_size: 4][expire_at_ms: 8][key_size: 2][key]
        // Key starts at byte 4 + 8 + 8 + 4 + 8 + 2 = 34.
        {
            use std::fs::OpenOptions;
            use std::io::{Seek, SeekFrom, Write};
            let mut f = OpenOptions::new().write(true).open(&path).unwrap();
            f.seek(SeekFrom::Start(34)).unwrap();
            f.write_all(&[0xAB]).unwrap();
        }

        let mut reader = HintReader::open(&path).unwrap();
        assert!(
            reader.read_all().is_err(),
            "flipped byte in key area must trigger CRC mismatch"
        );
    }

    /// Empty key round-trips through `write_entry` / `read_all`.
    #[test]
    fn hint_entry_empty_key_round_trips() {
        let dir = tmp();
        let path = dir.path().join("data.hint");

        let entry = HintEntry {
            file_id: 1,
            offset: 0,
            value_size: 5,
            expire_at_ms: 0,
            key: Vec::new(), // empty key
        };
        {
            let mut writer = HintWriter::open(&path).unwrap();
            writer.write_entry(&entry).unwrap();
            writer.commit().unwrap();
        }

        let mut reader = HintReader::open(&path).unwrap();
        let entries = reader.read_all().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].key,
            Vec::<u8>::new(),
            "empty key must round-trip"
        );
        assert_eq!(entries[0].file_id, 1);
        assert_eq!(entries[0].value_size, 5);
    }

    /// Write 20 entries and verify all read back without CRC error.
    #[test]
    fn hint_multiple_entries_all_have_valid_crcs() {
        let dir = tmp();
        let path = dir.path().join("data.hint");

        let entries_in: Vec<HintEntry> = (0u64..20)
            .map(|i| HintEntry {
                file_id: i % 3,
                offset: i * 128,
                value_size: (i as u32) * 10 + 1,
                expire_at_ms: if i % 2 == 0 { 0 } else { i * 1000 },
                key: format!("crc_check_key_{i}").into_bytes(),
            })
            .collect();

        {
            let mut writer = HintWriter::open(&path).unwrap();
            for e in &entries_in {
                writer.write_entry(e).unwrap();
            }
            writer.commit().unwrap();
        }

        // All 20 entries must be readable without CRC error.
        let mut reader = HintReader::open(&path).unwrap();
        let entries_out = reader.read_all().unwrap();
        assert_eq!(entries_out.len(), 20, "all 20 entries must be present");
        for (a, b) in entries_in.iter().zip(entries_out.iter()) {
            assert_eq!(a, b, "entry must survive round-trip with correct CRC");
        }
    }

    // ------------------------------------------------------------------
    // commit() atomicity (additional)
    // ------------------------------------------------------------------

    /// After `commit()`, no `.hint.tmp` file exists and `.hint` file does.
    #[test]
    fn commit_renames_tmp_to_final() {
        let dir = tmp();
        let path = dir.path().join("data.hint");
        let tmp_path = dir.path().join("data.hint.tmp");

        {
            let mut writer = HintWriter::open(&path).unwrap();
            writer
                .write_entry(&sample_entry(b"committed", 1, 0))
                .unwrap();
            writer.commit().unwrap();
        }

        assert!(path.exists(), "final .hint file must exist after commit");
        assert!(
            !tmp_path.exists(),
            ".hint.tmp file must not exist after commit"
        );
    }

    /// Dropping the writer without calling commit must remove the .tmp file
    /// and not create the final .hint file.
    #[test]
    fn drop_without_commit_cleans_up_tmp() {
        let dir = tmp();
        let path = dir.path().join("data.hint");
        let tmp_path = dir.path().join("data.hint.tmp");

        {
            let mut writer = HintWriter::open(&path).unwrap();
            writer
                .write_entry(&sample_entry(b"uncommitted", 1, 0))
                .unwrap();
            // Drop without commit.
        }

        assert!(
            !path.exists(),
            "final .hint must not exist when commit was never called"
        );
        assert!(!tmp_path.exists(), ".hint.tmp must be cleaned up by Drop");
    }

    // ------------------------------------------------------------------
    // load_into: detailed edge cases
    // ------------------------------------------------------------------

    /// Loading an empty hint file results in an empty keydir.
    #[test]
    fn load_into_empty_file_returns_empty_keydir() {
        let dir = tmp();
        let path = dir.path().join("empty2.hint");

        let writer = HintWriter::open(&path).unwrap();
        writer.commit().unwrap();

        let mut kd = KeyDir::new();
        let mut reader = HintReader::open(&path).unwrap();
        reader.load_into(&mut kd).unwrap();

        assert!(kd.is_empty(), "empty hint file must produce empty keydir");
        assert_eq!(kd.len(), 0);
    }

    /// Writing 15 entries and loading them populates the keydir with 15 entries.
    #[test]
    fn load_into_all_entries_added_to_keydir() {
        let dir = tmp();
        let path = dir.path().join("data.hint");

        let entries: Vec<HintEntry> = (0u64..15)
            .map(|i| HintEntry {
                file_id: 1,
                offset: i * 50,
                value_size: 10,
                expire_at_ms: 0,
                key: format!("load_key_{i}").into_bytes(),
            })
            .collect();

        {
            let mut writer = HintWriter::open(&path).unwrap();
            for e in &entries {
                writer.write_entry(e).unwrap();
            }
            writer.commit().unwrap();
        }

        let mut kd = KeyDir::new();
        let mut reader = HintReader::open(&path).unwrap();
        reader.load_into(&mut kd).unwrap();

        assert_eq!(kd.len(), 15, "all 15 entries must be loaded into keydir");
        for e in &entries {
            assert!(kd.get(&e.key).is_some(), "keydir must contain {:?}", e.key);
        }
    }

    /// A hint file with a corrupt CRC in the first entry causes `load_into` to return Err.
    #[test]
    fn load_into_corrupt_crc_returns_error() {
        let dir = tmp();
        let path = dir.path().join("data.hint");

        {
            let mut writer = HintWriter::open(&path).unwrap();
            writer
                .write_entry(&sample_entry(b"crc_load_test", 1, 0))
                .unwrap();
            writer.commit().unwrap();
        }

        // Flip a byte in the key area (byte 34) to invalidate the CRC.
        {
            use std::fs::OpenOptions;
            use std::io::{Seek, SeekFrom, Write};
            let mut f = OpenOptions::new().write(true).open(&path).unwrap();
            f.seek(SeekFrom::Start(34)).unwrap();
            f.write_all(&[0xCC]).unwrap();
        }

        let mut kd = KeyDir::new();
        let mut reader = HintReader::open(&path).unwrap();
        assert!(
            reader.load_into(&mut kd).is_err(),
            "load_into on corrupt hint must return Err"
        );
    }

    // ------------------------------------------------------------------
    // H-REMAIN-1: crc32fast backward compatibility
    // ------------------------------------------------------------------

    /// Verify that `crc32fast::hash()` produces the same output as the
    /// old hand-rolled CRC-32/ISO-HDLC implementation. This ensures
    /// existing hint files written with the old code remain readable.
    #[test]
    fn crc32fast_matches_old_hand_rolled_implementation() {
        /// Original hand-rolled CRC-32/ISO-HDLC for reference.
        fn old_crc32(data: &[u8]) -> u32 {
            let mut crc: u32 = 0xFFFF_FFFF;
            for &byte in data {
                crc ^= u32::from(byte);
                for _ in 0..8 {
                    if crc & 1 == 1 {
                        crc = (crc >> 1) ^ 0xEDB8_8320;
                    } else {
                        crc >>= 1;
                    }
                }
            }
            crc ^ 0xFFFF_FFFF
        }

        // Empty data
        assert_eq!(crc32(&[]), old_crc32(&[]));

        // Small data
        assert_eq!(crc32(b"hello"), old_crc32(b"hello"));
        assert_eq!(crc32(b"ferricstore"), old_crc32(b"ferricstore"));

        // Binary data
        let binary_data: Vec<u8> = (0u8..=255).collect();
        assert_eq!(crc32(&binary_data), old_crc32(&binary_data));

        // Large data (simulating a hint entry body)
        let large_data = vec![0xABu8; 10_000];
        assert_eq!(crc32(&large_data), old_crc32(&large_data));
    }

    /// Write 1000 hint entries and verify all round-trip correctly with
    /// hardware-accelerated CRC. Exercises the hot path that H-REMAIN-1 fixes.
    #[test]
    fn hint_1000_entries_round_trip_with_crc32fast() {
        let dir = tmp();
        let path = dir.path().join("large.hint");

        let entries_in: Vec<HintEntry> = (0u64..1000)
            .map(|i| HintEntry {
                file_id: i % 10,
                offset: i * 256,
                value_size: (i as u32) * 3 + 1,
                expire_at_ms: if i % 3 == 0 { 0 } else { i * 5000 },
                key: format!("key_{i:06}").into_bytes(),
            })
            .collect();

        {
            let mut writer = HintWriter::open(&path).unwrap();
            for e in &entries_in {
                writer.write_entry(e).unwrap();
            }
            writer.commit().unwrap();
        }

        let mut reader = HintReader::open(&path).unwrap();
        let entries_out = reader.read_all().unwrap();
        assert_eq!(entries_out.len(), 1000);
        for (a, b) in entries_in.iter().zip(entries_out.iter()) {
            assert_eq!(a, b, "entry must survive round-trip with crc32fast CRC");
        }
    }

    /// Corrupted CRC detection still works after switching to crc32fast.
    #[test]
    fn crc32fast_detects_corruption() {
        let dir = tmp();
        let path = dir.path().join("corrupt_detect.hint");

        let entry = sample_entry(b"detect_me", 42, 1024);
        {
            let mut writer = HintWriter::open(&path).unwrap();
            writer.write_entry(&entry).unwrap();
            writer.commit().unwrap();
        }

        // Clean read succeeds.
        {
            let mut reader = HintReader::open(&path).unwrap();
            let entries = reader.read_all().unwrap();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0], entry);
        }

        // Corrupt the value_size field (starts at byte 4+8+8=20).
        {
            use std::fs::OpenOptions;
            use std::io::{Seek, SeekFrom, Write};
            let mut f = OpenOptions::new().write(true).open(&path).unwrap();
            f.seek(SeekFrom::Start(20)).unwrap();
            f.write_all(&[0xFF, 0xFF]).unwrap();
        }

        let mut reader = HintReader::open(&path).unwrap();
        assert!(
            reader.read_all().is_err(),
            "corruption must still be detected after crc32fast migration"
        );
    }

    // -------------------------------------------------------------------
    // Durability tests for HintWriter::commit
    // -------------------------------------------------------------------
    //
    // These lock in the contract that the design doc requires:
    //
    //   1. After commit() returns Ok, the final `.hint` file exists on
    //      the filesystem with all the entries readable.
    //   2. The temporary `.hint.tmp` file no longer exists (it was
    //      renamed to final_path).
    //   3. commit() does NOT error with a read-back of the file (data
    //      was flushed + sync_data'd before rename).
    //
    // We can't test "fsync actually hit the disk" from userspace without
    // a crash injector, but we can assert the full round-trip works
    // bit-for-bit immediately after commit() returns — which is the
    // observable contract sync_data() is supposed to provide.

    #[test]
    fn commit_leaves_final_file_readable_and_removes_tmp() {
        let dir = tmp();
        let final_path = dir.path().join("data.hint");
        let tmp_path = final_path.with_extension("hint.tmp");

        let entry = HintEntry {
            file_id: 9,
            offset: 1024,
            value_size: 64,
            expire_at_ms: 123_456,
            key: b"committed".to_vec(),
        };

        let mut writer = HintWriter::open(&final_path).unwrap();
        writer.write_entry(&entry).unwrap();
        writer.commit().unwrap();

        assert!(
            final_path.exists(),
            "commit() must leave the final .hint file on disk"
        );
        assert!(
            !tmp_path.exists(),
            "commit() must rename away the .hint.tmp file"
        );

        let mut reader = HintReader::open(&final_path).unwrap();
        let out = reader.read_all().unwrap();
        assert_eq!(out, vec![entry]);
    }

    #[test]
    fn commit_with_many_entries_fully_round_trips() {
        // Stress-test the flush + sync_data + rename sequence with a
        // payload larger than a BufWriter default buffer (8 KiB).
        let dir = tmp();
        let path = dir.path().join("big.hint");

        let entries: Vec<HintEntry> = (0..2_000)
            .map(|i| HintEntry {
                file_id: i as u64,
                offset: (i as u64) * 32,
                value_size: 16,
                expire_at_ms: 0,
                key: format!("key_{:08}", i).into_bytes(),
            })
            .collect();

        let mut writer = HintWriter::open(&path).unwrap();
        for e in &entries {
            writer.write_entry(e).unwrap();
        }
        writer.commit().unwrap();

        let mut reader = HintReader::open(&path).unwrap();
        let out = reader.read_all().unwrap();
        assert_eq!(
            out, entries,
            "every entry written before commit must be readable after commit"
        );
    }

    #[test]
    fn drop_without_commit_leaves_no_final_file() {
        // Documents the other half of the contract: if commit() is not
        // called (crash / early error), the final file must NOT appear
        // and the tmp file is cleaned up by Drop.
        let dir = tmp();
        let path = dir.path().join("aborted.hint");
        let tmp_path = path.with_extension("hint.tmp");

        {
            let mut writer = HintWriter::open(&path).unwrap();
            writer
                .write_entry(&HintEntry {
                    file_id: 1,
                    offset: 0,
                    value_size: 4,
                    expire_at_ms: 0,
                    key: b"abc".to_vec(),
                })
                .unwrap();
            // Deliberately do not call commit(). Drop runs here.
        }

        assert!(
            !path.exists(),
            "no commit means the final .hint file must not appear"
        );
        assert!(
            !tmp_path.exists(),
            "Drop must clean up the .hint.tmp file when commit was skipped"
        );
    }
