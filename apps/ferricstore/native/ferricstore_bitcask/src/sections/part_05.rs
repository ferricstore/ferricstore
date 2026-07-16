/// Async variant of `v2_append_batch`: copies BEAM binaries into owned
/// memory, then submits validation, record encoding, write, and fsync to Tokio.
/// Returns `:ok` immediately. When IO completes, sends
/// `{:tokio_complete, correlation_id, :ok, [{offset, value_size}, ...]}` or
/// `{:tokio_complete, correlation_id, :error, reason}` to `caller_pid`.
///
/// ## Scheduler contract
///
/// Runs on a dirty CPU scheduler while decoding and copying the unbounded
/// batch. CRC/record encoding and file write + fsync run on a Tokio blocking
/// worker.
#[rustler::nif(schedule = "DirtyCpu")]
#[allow(clippy::needless_pass_by_value)]
fn v2_append_batch_async<'a>(
    env: Env<'a>,
    caller_pid: LocalPid,
    correlation_id: u64,
    path: String,
    records: Vec<(Binary<'a>, Binary<'a>, u64)>,
) -> NifResult<Term<'a>> {
    // Step 1: Copy BEAM binaries into owned Vecs before spawning to Tokio
    // because Binary<'a> borrows from the NIF env which is destroyed when
    // this function returns. Validation and record encoding happen in the
    // blocking worker below so large batches do not burn normal BEAM scheduler time.
    let entries: Vec<(Vec<u8>, Vec<u8>, u64)> = records
        .iter()
        .map(|(k, v, exp)| (k.as_slice().to_vec(), v.as_slice().to_vec(), *exp))
        .collect();

    let owned_path = path;

    let blocking_task = match async_io::try_spawn_blocking(move || {
        let p = std::path::Path::new(&owned_path);
        let file_id = parse_file_id(p);

        match log::LogWriter::open(p, file_id) {
            Ok(mut writer) => {
                let batch: Vec<(&[u8], &[u8], u64)> = entries
                    .iter()
                    .map(|(key, value, expire_at_ms)| {
                        (key.as_slice(), value.as_slice(), *expire_at_ms)
                    })
                    .collect();

                writer.write_batch(&batch).map_err(|e| e.to_string())
            }
            Err(e) => Err(e.to_string()),
        }
    }) {
        Ok(task) => task,
        Err(reason) => return Ok((atoms::error(), reason).encode(env)),
    };

    // Step 2: Spawn CPU encoding + IO to Tokio blocking thread pool — BEAM
    // scheduler returns immediately.
    async_io::runtime().spawn(async move {
        let result = blocking_task
        .await
        .unwrap_or_else(|e| Err(format!("spawn_blocking: {e}")));

        // Step 3: Send result to the BEAM caller.
        let mut msg_env = rustler::OwnedEnv::new();
        let _ = msg_env.send_and_clear(&caller_pid, |env| match result {
            Ok(locations) => (
                atoms::tokio_complete(),
                correlation_id,
                atoms::ok(),
                locations,
            )
                .encode(env),
            Err(reason) => (
                atoms::tokio_complete(),
                correlation_id,
                atoms::error(),
                reason.as_str(),
            )
                .encode(env),
        });
    });

    Ok(atoms::ok().encode(env))
}

// ===========================================================================
// Audit fix tests
// ===========================================================================

#[cfg(test)]
mod audit_fix_tests {
    use super::*;
    use tempfile::TempDir;

    fn tmp() -> TempDir {
        tempfile::TempDir::new().unwrap()
    }

    // ------------------------------------------------------------------
    // L-NEW-1: parse_file_id handles all-zeros and edge cases
    // ------------------------------------------------------------------

    #[test]
    fn parse_file_id_normal_filename() {
        let path = std::path::Path::new("/data/00000000000000000001.log");
        assert_eq!(parse_file_id(path), 1);
    }

    #[test]
    fn parse_file_id_all_zeros() {
        let path = std::path::Path::new("/data/00000000000000000000.log");
        assert_eq!(
            parse_file_id(path),
            0,
            "all-zeros filename must produce file_id 0"
        );
    }

    #[test]
    fn parse_file_id_large_number() {
        let path = std::path::Path::new("/data/00000000000000012345.log");
        assert_eq!(parse_file_id(path), 12345);
    }

    #[test]
    fn parse_file_id_max_u64() {
        // 18446744073709551615 is u64::MAX
        let path = std::path::Path::new("/data/18446744073709551615.log");
        assert_eq!(parse_file_id(path), u64::MAX);
    }

    #[test]
    fn parse_file_id_no_extension() {
        let path = std::path::Path::new("/data/00000000000000000042");
        assert_eq!(parse_file_id(path), 42);
    }

    #[test]
    fn parse_file_id_non_numeric_returns_zero() {
        let path = std::path::Path::new("/data/notanumber.log");
        assert_eq!(
            parse_file_id(path),
            0,
            "non-numeric filename must produce file_id 0"
        );
    }

    #[test]
    fn parse_file_id_single_digit() {
        let path = std::path::Path::new("/data/7.log");
        assert_eq!(parse_file_id(path), 7);
    }

    // ------------------------------------------------------------------
    // L-REMAIN-1: v2_fsync opens with write permission
    // ------------------------------------------------------------------

    #[test]
    fn fsync_with_write_permission_works() {
        let dir = tmp();
        let path = dir.path().join("fsync_test.log");

        // Write some data using LogWriter
        {
            let mut writer = log::LogWriter::open(&path, 0).unwrap();
            writer.write(b"key", b"value", 0).unwrap();
            writer.sync().unwrap();
        }

        // Open with write permission and sync — should succeed
        let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        assert!(
            f.sync_data().is_ok(),
            "sync_data on write-opened file must succeed"
        );
    }

    #[test]
    fn fsync_nonexistent_file_returns_error() {
        let dir = tmp();
        let path = dir.path().join("nonexistent.log");
        assert!(
            std::fs::OpenOptions::new().write(true).open(&path).is_err(),
            "opening nonexistent file for write must fail"
        );
    }

    // ------------------------------------------------------------------
    // M-NEW-1: small buffer LogWriter for single-record writes
    // ------------------------------------------------------------------

    #[test]
    fn open_small_writes_correctly() {
        let dir = tmp();
        let path = dir.path().join("00000000000000000001.log");

        // Write a record using open_small
        {
            let mut writer = log::LogWriter::open_small(&path, 1).unwrap();
            let offset = writer.write(b"testkey", b"testvalue", 0).unwrap();
            writer.sync().unwrap();
            assert_eq!(offset, 0, "first record must be at offset 0");
        }

        // Verify we can read it back
        let file = std::fs::File::open(&path).unwrap();
        let record = log::pread_record_from_file(&file, 0).unwrap().unwrap();
        assert_eq!(&record.key, b"testkey");
        assert_eq!(record.value.as_ref().unwrap(), b"testvalue");
    }

    #[test]
    fn value_ref_validation_rejects_mismatched_key_without_reading_value() {
        let dir = tmp();
        let path = dir.path().join("00000000000000000001.log");
        let offset;

        {
            let mut writer = log::LogWriter::open_small(&path, 1).unwrap();
            offset = writer.write(b"key_b", b"value_b", 0).unwrap();
            writer.sync().unwrap();
        }

        let file = std::fs::File::open(&path).unwrap();
        let result = validate_value_ref_from_file(&file, offset, b"key_a", 7).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn value_ref_validation_returns_value_slice_for_matching_key_and_size() {
        let dir = tmp();
        let path = dir.path().join("00000000000000000001.log");
        let offset;

        {
            let mut writer = log::LogWriter::open_small(&path, 1).unwrap();
            offset = writer.write(b"key_a", b"value_a", 0).unwrap();
            writer.sync().unwrap();
        }

        let file = std::fs::File::open(&path).unwrap();
        let result = validate_value_ref_from_file(&file, offset, b"key_a", 7).unwrap();
        assert_eq!(
            result,
            Some((offset + log::HEADER_SIZE as u64 + b"key_a".len() as u64, 7))
        );
    }

    #[test]
    fn value_ref_validation_rejects_value_crc_corruption() {
        let dir = tmp();
        let path = dir.path().join("00000000000000000001.log");
        let offset;

        {
            let mut writer = log::LogWriter::open_small(&path, 1).unwrap();
            offset = writer.write(b"key_a", b"value_a", 0).unwrap();
            writer.sync().unwrap();
        }

        let value_offset = offset + log::HEADER_SIZE as u64 + b"key_a".len() as u64;
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.write_all_at(b"X", value_offset + 2).unwrap();
        drop(file);

        let file = std::fs::File::open(&path).unwrap();
        let err = validate_value_ref_from_file(&file, offset, b"key_a", 7).unwrap_err();
        assert!(err.contains("CRC mismatch"));
    }

    #[test]
    fn value_ref_validation_rejects_crc_valid_header_with_missing_zero_body() {
        let dir = tmp();
        let path = dir.path().join("00000000000000000001.log");
        let mut header = [0u8; log::HEADER_SIZE];
        header[20..22].copy_from_slice(&1u16.to_le_bytes());
        header[22..26].copy_from_slice(&1u32.to_le_bytes());

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&header[4..]);
        hasher.update(&[0, 0]);
        header[0..4].copy_from_slice(&hasher.finalize().to_le_bytes());
        std::fs::write(&path, header).unwrap();

        let file = std::fs::File::open(&path).unwrap();
        let err = validate_value_ref_from_file(&file, 0, &[0], 1).unwrap_err();
        assert!(err.contains("short read"), "unexpected error: {err}");
    }

    #[test]
    fn open_small_1000_sequential_writes_no_corruption() {
        let dir = tmp();
        let path = dir.path().join("00000000000000000001.log");

        // Write 1000 records using open_small (one per open, simulating v2 NIF pattern)
        let mut expected_offsets = Vec::new();
        for i in 0u64..1000 {
            let mut writer = log::LogWriter::open_small(&path, 1).unwrap();
            let key = format!("k{i:04}").into_bytes();
            let value = format!("v{i:04}").into_bytes();
            let offset = writer.write(&key, &value, 0).unwrap();
            writer.sync().unwrap();
            expected_offsets.push((offset, key, value));
        }

        // Verify all records are readable
        let file = std::fs::File::open(&path).unwrap();
        for (offset, key, value) in &expected_offsets {
            let record = log::pread_record_from_file(&file, *offset)
                .unwrap()
                .unwrap();
            assert_eq!(&record.key, key);
            assert_eq!(record.value.as_ref().unwrap(), value);
        }
    }

    #[test]
    fn grouped_batch_pread_preserves_input_order_across_paths_and_missing_offsets() {
        let dir = tmp();
        let path_a = dir.path().join("00000000000000000001.log");
        let path_b = dir.path().join("00000000000000000002.log");

        let mut writer_a = log::LogWriter::open(&path_a, 1).unwrap();
        let a0 = writer_a.write(b"a0", b"va0", 0).unwrap();
        let a1 = writer_a.write(b"a1", b"va1", 0).unwrap();
        writer_a.sync().unwrap();

        let mut writer_b = log::LogWriter::open(&path_b, 2).unwrap();
        let b0 = writer_b.write(b"b0", b"vb0", 0).unwrap();
        writer_b.sync().unwrap();

        let values = pread_batch_grouped(vec![
            (path_b.to_string_lossy().into_owned(), b0),
            (path_a.to_string_lossy().into_owned(), a1),
            (path_a.to_string_lossy().into_owned(), 999_999),
            (path_a.to_string_lossy().into_owned(), a0),
        ])
        .unwrap();

        assert_eq!(values[0].as_deref(), Some(&b"vb0"[..]));
        assert_eq!(values[1].as_deref(), Some(&b"va1"[..]));
        assert_eq!(values[2], BatchReadValue::Nil);
        assert_eq!(values[3].as_deref(), Some(&b"va0"[..]));
    }

    #[test]
    fn grouped_batch_pread_isolates_missing_files_to_their_indexes() {
        let dir = tmp();
        let good_path = dir.path().join("00000000000000000001.log");
        let missing_path = dir.path().join("00000000000000000002.log");

        let mut writer = log::LogWriter::open(&good_path, 1).unwrap();
        let good_offset = writer.write(b"good", b"value", 0).unwrap();
        writer.sync().unwrap();

        let values = pread_batch_grouped(vec![
            (missing_path.to_string_lossy().into_owned(), 0),
            (good_path.to_string_lossy().into_owned(), good_offset),
        ])
        .unwrap();

        assert!(matches!(values[0], BatchReadValue::Error(_)));
        assert_eq!(values[1].as_deref(), Some(&b"value"[..]));
    }

    #[cfg(unix)]
    #[test]
    fn grouped_batch_pread_does_not_misclassify_symlink_rejection_as_missing() {
        use std::os::unix::fs::symlink;

        let dir = tmp();
        let target = dir.path().join("00000000000000000001.log");
        let link = dir.path().join("00000000000000000002.log");
        std::fs::write(&target, b"protected").unwrap();
        symlink(&target, &link).unwrap();

        let values = pread_batch_grouped(vec![(link.to_string_lossy().into_owned(), 0)]).unwrap();

        let BatchReadValue::Error(reason) = &values[0] else {
            panic!("symlink rejection must be reported as an error");
        };
        assert!(
            !reason.contains("missing_file"),
            "only ENOENT may be classified as missing_file: {reason}"
        );
    }

    #[test]
    fn grouped_batch_pread_isolates_corrupt_files_to_their_indexes() {
        use std::os::unix::fs::FileExt;

        let dir = tmp();
        let bad_path = dir.path().join("00000000000000000001.log");
        let good_path = dir.path().join("00000000000000000002.log");

        let mut bad_writer = log::LogWriter::open(&bad_path, 1).unwrap();
        let bad_good = bad_writer.write(b"bad_good", b"value", 0).unwrap();
        let corrupt = bad_writer.write(b"corrupt", b"bad_value", 0).unwrap();
        bad_writer.sync().unwrap();

        let mut good_writer = log::LogWriter::open(&good_path, 2).unwrap();
        let good = good_writer.write(b"good", b"value", 0).unwrap();
        good_writer.sync().unwrap();

        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&bad_path)
            .unwrap();
        let corrupt_value_byte = corrupt + log::HEADER_SIZE as u64 + b"corrupt".len() as u64;
        file.write_at(b"X", corrupt_value_byte).unwrap();
        file.sync_data().unwrap();

        let values = pread_batch_grouped(vec![
            (bad_path.to_string_lossy().into_owned(), bad_good),
            (good_path.to_string_lossy().into_owned(), good),
            (bad_path.to_string_lossy().into_owned(), corrupt),
        ])
        .unwrap();

        assert_eq!(values[0].as_deref(), Some(&b"value"[..]));
        assert_eq!(values[1].as_deref(), Some(&b"value"[..]));
        assert!(matches!(values[2], BatchReadValue::Error(_)));
    }

    #[test]
    fn keyed_batch_pread_filters_mismatched_offsets_without_reordering() {
        let dir = tmp();
        let path_a = dir.path().join("00000000000000000001.log");
        let path_b = dir.path().join("00000000000000000002.log");

        let mut writer_a = log::LogWriter::open(&path_a, 1).unwrap();
        let a0 = writer_a.write(b"a0", b"va0", 0).unwrap();
        let a1 = writer_a.write(b"a1", b"va1", 0).unwrap();
        writer_a.sync().unwrap();

        let mut writer_b = log::LogWriter::open(&path_b, 2).unwrap();
        let b0 = writer_b.write(b"b0", b"vb0", 0).unwrap();
        writer_b.sync().unwrap();

        let values = pread_batch_grouped_keyed(vec![
            (path_b.to_string_lossy().into_owned(), b0, b"b0".to_vec()),
            (path_a.to_string_lossy().into_owned(), a1, b"a0".to_vec()),
            (path_a.to_string_lossy().into_owned(), a0, b"a0".to_vec()),
            (
                path_a.to_string_lossy().into_owned(),
                999_999,
                b"missing".to_vec(),
            ),
        ])
        .unwrap();

        assert_eq!(values[0].as_deref(), Some(&b"vb0"[..]));
        assert_eq!(values[1], BatchReadValue::Nil);
        assert_eq!(values[2].as_deref(), Some(&b"va0"[..]));
        assert_eq!(values[3], BatchReadValue::Nil);
    }

    #[test]
    fn keyed_batch_pread_isolates_missing_files_to_their_indexes() {
        let dir = tmp();
        let good_path = dir.path().join("00000000000000000001.log");
        let missing_path = dir.path().join("00000000000000000002.log");

        let mut writer = log::LogWriter::open(&good_path, 1).unwrap();
        let good_offset = writer.write(b"good", b"value", 0).unwrap();
        writer.sync().unwrap();

        let values = pread_batch_grouped_keyed(vec![
            (
                missing_path.to_string_lossy().into_owned(),
                0,
                b"missing".to_vec(),
            ),
            (
                good_path.to_string_lossy().into_owned(),
                good_offset,
                b"good".to_vec(),
            ),
        ])
        .unwrap();

        assert!(matches!(values[0], BatchReadValue::Error(_)));
        assert_eq!(values[1].as_deref(), Some(&b"value"[..]));
    }

    #[test]
    fn keyed_batch_pread_isolates_corrupt_files_to_their_indexes() {
        use std::os::unix::fs::FileExt;

        let dir = tmp();
        let bad_path = dir.path().join("00000000000000000001.log");
        let good_path = dir.path().join("00000000000000000002.log");

        let mut bad_writer = log::LogWriter::open(&bad_path, 1).unwrap();
        let bad_good = bad_writer.write(b"bad_good", b"value_before", 0).unwrap();
        let corrupt = bad_writer.write(b"corrupt", b"bad_value", 0).unwrap();
        bad_writer.sync().unwrap();

        let mut good_writer = log::LogWriter::open(&good_path, 2).unwrap();
        let good = good_writer.write(b"good", b"value", 0).unwrap();
        good_writer.sync().unwrap();

        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&bad_path)
            .unwrap();
        let corrupt_value_byte = corrupt + log::HEADER_SIZE as u64 + b"corrupt".len() as u64;
        file.write_at(b"X", corrupt_value_byte).unwrap();
        file.sync_data().unwrap();

        let values = pread_batch_grouped_keyed(vec![
            (
                bad_path.to_string_lossy().into_owned(),
                bad_good,
                b"bad_good".to_vec(),
            ),
            (
                good_path.to_string_lossy().into_owned(),
                good,
                b"good".to_vec(),
            ),
            (
                bad_path.to_string_lossy().into_owned(),
                corrupt,
                b"corrupt".to_vec(),
            ),
        ])
        .unwrap();

        assert_eq!(values[0].as_deref(), Some(&b"value_before"[..]));
        assert_eq!(values[1].as_deref(), Some(&b"value"[..]));
        assert!(matches!(values[2], BatchReadValue::Error(_)));
    }

    #[test]
    fn keyed_batch_pread_mismatched_key_does_not_decode_huge_value() {
        use std::os::unix::fs::FileExt;

        let dir = tmp();
        let path = dir.path().join("00000000000000000001.log");

        let mut writer = log::LogWriter::open(&path, 1).unwrap();
        let offset = writer.write(b"actual", b"small", 0).unwrap();
        writer.sync().unwrap();

        let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.write_at(&(u32::MAX - 1).to_le_bytes(), offset + 22)
            .unwrap();
        file.sync_data().unwrap();

        let values = pread_batch_grouped_keyed(vec![(
            path.to_string_lossy().into_owned(),
            offset,
            b"expected".to_vec(),
        )])
        .unwrap();

        assert_eq!(values, vec![BatchReadValue::Nil]);
    }

    #[test]
    fn tombstone_scan_errors_on_corrupt_live_payload_after_tombstone() {
        use std::os::unix::fs::FileExt;

        let dir = tmp();
        let path = dir.path().join("00000000000000000001.log");

        let mut writer = log::LogWriter::open(&path, 1).unwrap();
        let corrupt = writer.write(b"live", b"value", 0).unwrap();
        writer.write_tombstone(b"deleted").unwrap();
        writer.sync().unwrap();

        let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        let corrupt_value_byte = corrupt + log::HEADER_SIZE as u64 + b"live".len() as u64;
        file.write_at(b"X", corrupt_value_byte).unwrap();
        file.sync_data().unwrap();

        let err = scan_tombstones_from_path(&path).unwrap_err();
        assert!(err.contains("CRC mismatch"));
    }

    #[test]
    fn tombstone_scan_errors_on_truncated_key_after_tombstone() {
        let dir = tmp();
        let path = dir.path().join("00000000000000000001.log");

        let mut writer = log::LogWriter::open(&path, 1).unwrap();
        writer.write_tombstone(b"deleted").unwrap();
        writer.write_tombstone(b"truncated").unwrap();
        writer.sync().unwrap();

        let len = std::fs::metadata(&path).unwrap().len();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(len - 3)
            .unwrap();

        let err = scan_tombstones_from_path(&path).unwrap_err();
        assert!(err.contains("unexpected EOF") || err.contains("failed to read key"));
    }

    #[test]
    fn tombstone_scan_rejects_values_above_the_bitcask_limit_before_streaming() {
        use std::os::unix::fs::FileExt;

        let dir = tmp();
        let path = dir.path().join("00000000000000000001.log");

        let mut writer = log::LogWriter::open(&path, 1).unwrap();
        let offset = writer.write(b"oversized", b"small", 0).unwrap();
        writer.sync().unwrap();

        let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.write_at(&(512_u32 * 1024 * 1024 + 1).to_le_bytes(), offset + 22)
            .unwrap();
        file.sync_data().unwrap();

        let err = scan_tombstones_from_path(&path).unwrap_err();
        assert!(
            err.contains("value too large in log record"),
            "scanner must enforce the normal Bitcask value ceiling, got {err:?}"
        );
    }

    #[test]
    fn tombstone_scan_page_counts_physical_records_and_reports_exact_cursor() {
        let dir = tmp();
        let path = dir.path().join("00000000000000000001.log");

        let mut writer = log::LogWriter::open(&path, 1).unwrap();
        writer.write(b"live-1", b"value-1", 11).unwrap();
        writer.write_tombstone(b"deleted-1").unwrap();
        let second_page_offset = writer.write(b"live-2", b"value-2", 22).unwrap();
        writer.write_tombstone(b"deleted-2").unwrap();
        writer.sync().unwrap();

        let file_len = std::fs::metadata(&path).unwrap().len();
        let (first, first_cursor, first_done) =
            scan_tombstones_page_from_path(&path, 0, 2).unwrap();

        assert_eq!(first.len(), 1);
        assert_eq!(first[0].key, b"deleted-1");
        assert_eq!(first_cursor, second_page_offset);
        assert!(!first_done);

        let (second, second_cursor, second_done) =
            scan_tombstones_page_from_path(&path, first_cursor, 2).unwrap();

        assert_eq!(second.len(), 1);
        assert_eq!(second[0].key, b"deleted-2");
        assert_eq!(second_cursor, file_len);
        assert!(second_done);

        let (eof, eof_cursor, eof_done) =
            scan_tombstones_page_from_path(&path, second_cursor, 2).unwrap();
        assert!(eof.is_empty());
        assert_eq!(eof_cursor, file_len);
        assert!(eof_done);
    }

    #[test]
    fn tombstone_scan_page_rejects_invalid_bounds() {
        let dir = tmp();
        let path = dir.path().join("00000000000000000001.log");

        let mut writer = log::LogWriter::open(&path, 1).unwrap();
        writer.write_tombstone(b"deleted").unwrap();
        writer.sync().unwrap();

        assert!(scan_tombstones_page_from_path(&path, 0, 0)
            .unwrap_err()
            .contains("max_records must be positive"));
        assert!(scan_tombstones_page_from_path(&path, 0, 65_537)
            .unwrap_err()
            .contains("max_records exceeds maximum 65536"));

        let file_len = std::fs::metadata(&path).unwrap().len();
        assert!(scan_tombstones_page_from_path(&path, file_len + 1, 1)
            .unwrap_err()
            .contains("exceeds file length"));
    }

    #[test]
    fn tombstone_scan_page_rejects_corrupt_live_record_without_partial_results() {
        use std::os::unix::fs::FileExt;

        let dir = tmp();
        let path = dir.path().join("00000000000000000001.log");

        let mut writer = log::LogWriter::open(&path, 1).unwrap();
        writer.write_tombstone(b"deleted").unwrap();
        let corrupt = writer.write(b"live", b"value", 0).unwrap();
        writer.sync().unwrap();

        let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        let corrupt_value_byte = corrupt + log::HEADER_SIZE as u64 + b"live".len() as u64;
        file.write_at(b"X", corrupt_value_byte).unwrap();
        file.sync_data().unwrap();

        let err = scan_tombstones_page_from_path(&path, 0, 2).unwrap_err();
        assert!(err.contains("CRC mismatch"));
    }

    #[test]
    fn key_state_scan_returns_latest_masked_state_without_values() {
        let dir = tmp();
        let path = dir.path().join("00000000000000000001.log");

        let mut writer = log::LogWriter::open(&path, 1).unwrap();
        writer.write(b"a", &vec![b'x'; 128 * 1024], 11).unwrap();
        writer
            .write(b"ignored", &vec![b'y'; 128 * 1024], 0)
            .unwrap();
        writer.write_tombstone(b"a").unwrap();
        writer.write(b"b", b"live", 22).unwrap();
        writer.sync().unwrap();

        let states =
            scan_key_states_from_path(&path, &[b"a".to_vec(), b"b".to_vec(), b"missing".to_vec()])
                .unwrap();

        assert!(states
            .iter()
            .any(|state| { state.key == b"a" && state.expire_at_ms == 0 && state.is_tombstone }));

        assert!(states
            .iter()
            .any(|state| { state.key == b"b" && state.expire_at_ms == 22 && !state.is_tombstone }));

        assert!(!states.iter().any(|state| state.key == b"missing"));
    }

    #[test]
    fn key_state_scan_errors_on_truncated_live_payload_before_later_tombstone() {
        let dir = tmp();
        let path = dir.path().join("00000000000000000001.log");

        let mut writer = log::LogWriter::open(&path, 1).unwrap();
        writer.write(b"a", b"live", 7).unwrap();
        writer.write(b"truncated", &vec![b'x'; 1024], 0).unwrap();
        writer.write_tombstone(b"later_delete").unwrap();
        writer.sync().unwrap();

        let len = std::fs::metadata(&path).unwrap().len();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(len - 100)
            .unwrap();

        let err = scan_key_states_from_path(
            &path,
            &[
                b"a".to_vec(),
                b"truncated".to_vec(),
                b"later_delete".to_vec(),
            ],
        )
        .unwrap_err();

        assert!(err.contains("unexpected EOF in value"));
    }

    #[test]
    fn key_state_scan_rejects_values_above_the_bitcask_limit_before_streaming() {
        use std::os::unix::fs::FileExt;

        let dir = tmp();
        let path = dir.path().join("00000000000000000001.log");

        let mut writer = log::LogWriter::open(&path, 1).unwrap();
        let offset = writer.write(b"oversized", b"small", 0).unwrap();
        writer.sync().unwrap();

        let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.write_at(&(512_u32 * 1024 * 1024 + 1).to_le_bytes(), offset + 22)
            .unwrap();
        file.sync_data().unwrap();

        let err = scan_key_states_from_path(&path, &[b"oversized".to_vec()]).unwrap_err();
        assert!(
            err.contains("value too large in log record"),
            "scanner must enforce the normal Bitcask value ceiling, got {err:?}"
        );
    }

    #[test]
    fn key_state_scan_errors_when_live_value_size_swallows_later_record() {
        use std::os::unix::fs::FileExt;

        let dir = tmp();
        let path = dir.path().join("00000000000000000001.log");

        let mut writer = log::LogWriter::open(&path, 1).unwrap();
        let corrupt_offset = writer.write(b"corrupt", b"value", 0).unwrap();
        writer.write(b"deleted", b"live", 0).unwrap();
        writer.sync().unwrap();

        let file_len = std::fs::metadata(&path).unwrap().len();
        let corrupt_value_start =
            corrupt_offset + log::HEADER_SIZE as u64 + b"corrupt".len() as u64;
        let oversized_value_len = u32::try_from(file_len - corrupt_value_start).unwrap();

        let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.write_at(&oversized_value_len.to_le_bytes(), corrupt_offset + 22)
            .unwrap();
        file.sync_data().unwrap();

        let err = scan_key_states_from_path(&path, &[b"deleted".to_vec()]).unwrap_err();

        assert!(
            err.contains("CRC mismatch"),
            "corrupt live record must fail the key-state scan, got {err:?}"
        );
    }

    #[test]
    fn key_state_scan_does_not_trust_corrupt_tombstone() {
        use std::io::{Read, Seek, SeekFrom, Write};

        let dir = tmp();
        let path = dir.path().join("00000000000000000001.log");

        let mut writer = log::LogWriter::open(&path, 1).unwrap();
        writer.write_tombstone(b"deleted").unwrap();
        writer.sync().unwrap();

        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();

        let mut header = [0u8; log::HEADER_SIZE];
        file.read_exact(&mut header).unwrap();
        let key_size = u16::from_le_bytes(header[20..22].try_into().unwrap()) as i64;
        file.seek(SeekFrom::Current(key_size - 1)).unwrap();

        let mut byte = [0u8; 1];
        file.read_exact(&mut byte).unwrap();
        byte[0] ^= 0x01;
        file.seek(SeekFrom::Current(-1)).unwrap();
        file.write_all(&byte).unwrap();
        file.sync_all().unwrap();

        let err = scan_key_states_from_path(&path, &[b"deleted".to_vec()]).unwrap_err();

        assert!(
            err.contains("CRC mismatch"),
            "corrupt tombstone must fail the key-state scan, got {err:?}"
        );
    }

    #[test]
    fn keyed_batch_pread_sorts_by_offset_but_preserves_result_order() {
        let reads = vec![
            (0usize, 300u64, b"third".to_vec()),
            (1usize, 100u64, b"first".to_vec()),
            (2usize, 200u64, b"second".to_vec()),
        ];

        let sorted = sort_keyed_reads_by_offset(reads);

        assert_eq!(sorted[0].1, 100);
        assert_eq!(sorted[1].1, 200);
        assert_eq!(sorted[2].1, 300);
        assert_eq!(sorted[0].0, 1);
        assert_eq!(sorted[1].0, 2);
        assert_eq!(sorted[2].0, 0);
    }

    #[test]
    fn grouped_batch_pread_validation_rejects_sparse_and_duplicate_indexes() {
        let groups = vec![("a.log".to_string(), vec![(0, 10), (2, 20)])];
        let err = validate_grouped_pread_groups(&groups).unwrap_err();
        assert!(err.contains("out of range"));

        let groups = vec![
            ("a.log".to_string(), vec![(0, 10)]),
            ("b.log".to_string(), vec![(0, 20)]),
        ];
        let err = validate_grouped_pread_groups(&groups).unwrap_err();
        assert!(err.contains("duplicate"));
    }

    #[test]
    fn grouped_keyed_batch_pread_validation_rejects_sparse_and_duplicate_indexes() {
        let groups = vec![(
            "a.log".to_string(),
            vec![(0, 10, b"a".to_vec()), (2, 20, b"b".to_vec())],
        )];
        let err = validate_grouped_keyed_pread_groups(&groups).unwrap_err();
        assert!(err.contains("out of range"));

        let groups = vec![
            ("a.log".to_string(), vec![(0, 10, b"a".to_vec())]),
            ("b.log".to_string(), vec![(0, 20, b"b".to_vec())]),
        ];
        let err = validate_grouped_keyed_pread_groups(&groups).unwrap_err();
        assert!(err.contains("duplicate"));
    }

    #[test]
    fn lmdb_cache_rejects_a_conflicting_map_size() {
        let dir = tmp();
        let path = dir.path().join("lmdb");
        let path = path.to_string_lossy();
        let initial_size = 4 * 1024 * 1024;

        let store = lmdb_store(path.as_ref(), initial_size).unwrap();
        let error = match lmdb_store(path.as_ref(), initial_size * 2) {
            Ok(_) => panic!("conflicting map size silently reused the cached environment"),
            Err(error) => error,
        };

        assert!(
            error.contains("map_size mismatch"),
            "conflicting map size must not silently reuse the cached environment: {error}"
        );
        drop(store);
    }

    #[cfg(unix)]
    #[test]
    fn lmdb_store_rejects_a_symlinked_parent_directory() {
        use std::os::unix::fs::symlink;

        let dir = tmp();
        let outside = dir.path().join("outside");
        let redirected = dir.path().join("redirected");
        std::fs::create_dir(&outside).unwrap();
        symlink(&outside, &redirected).unwrap();
        let path = redirected.join("lmdb");
        let path_string = path.to_string_lossy();

        let error = match lmdb_store(path_string.as_ref(), 4 * 1024 * 1024) {
            Ok(_) => panic!("LMDB path followed a symlinked parent"),
            Err(error) => error,
        };

        assert!(!error.is_empty());
        assert!(!outside.join("lmdb").exists());
    }

    // ------------------------------------------------------------------
    // H-NEW-1: Poisoned mutex recovery
    // ------------------------------------------------------------------

    #[test]
    fn poisoned_mutex_recovery_with_unwrap_or_else() {
        use std::sync::{Arc, Mutex};

        let m = Arc::new(Mutex::new(42u64));

        // Poison the mutex by panicking while holding the lock
        let m2 = m.clone();
        let result = std::panic::catch_unwind(move || {
            let _guard = m2.lock().unwrap();
            panic!("deliberate panic to poison the mutex");
        });
        assert!(result.is_err(), "panic should have been caught");

        // Verify the mutex is poisoned
        assert!(m.lock().is_err(), "mutex should be poisoned after panic");

        // Verify unwrap_or_else recovers the inner value
        let guard = m.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(*guard, 42, "recovered value must be intact");
    }
}

// ===========================================================================
// prob_fsync edge-case tests
// ---------------------------------------------------------------------------
// These tests cover the `prob_fsync()` helper that was added to ensure
// bloom/cuckoo/cms/topk writes are durable before the NIF returns :ok.
//
// Failure modes we want to verify:
//
//   * fsync on a newly-created empty file returns Ok and is cheap.
//   * fsync on a file opened read-only returns Err (sync_data requires write).
//   * fsync on a deleted-but-still-open file does NOT panic.
//   * Repeated fsync on the same open file is idempotent (no FD leak).
//   * Concurrent fsyncs across threads on the same file are safe.
//   * File descriptor stability: after many open+write+fsync+close cycles,
//     the number of open FDs on the process stays bounded.
// ===========================================================================
