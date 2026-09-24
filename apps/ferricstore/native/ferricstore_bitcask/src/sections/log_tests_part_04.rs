#[test]
fn recover_torn_tail_streams_large_zero_suffix() {
    let dir = temp_dir();
    let path = dir.path().join("large_zero_torn_tail.log");

    let valid_end = {
        let mut writer = LogWriter::open(&path, 1).unwrap();
        let offset = writer.write(b"before", b"value", 0).unwrap();
        writer.sync().unwrap();
        offset + (HEADER_SIZE + b"before".len() + b"value".len()) as u64
    };

    // The header advertises a value longer than the bytes that made it to
    // disk. The zero-filled prefix is larger than one probe window and has no
    // later record, so recovery should stream it and truncate it.
    let declared_value_len = TORN_TAIL_PROBE_WINDOW_BYTES * 3 + 128;
    let partial_value_len = TORN_TAIL_PROBE_WINDOW_BYTES * 2 + 64;
    let torn_value = vec![0u8; declared_value_len];
    let torn = encode_record(b"torn", &torn_value, 0);
    let prefix_len = HEADER_SIZE + b"torn".len() + partial_value_len;
    assert!(prefix_len < torn.len());

    {
        use std::io::Write as _;

        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        file.write_all(&torn[..prefix_len]).unwrap();
        file.sync_data().unwrap();
    }

    let recovered_end = recover_torn_tail(&path).unwrap();
    assert_eq!(recovered_end, valid_end);
    assert_eq!(std::fs::metadata(&path).unwrap().len(), valid_end);

    let mut reader = LogReader::open(&path).unwrap();
    let records = reader.iter_from_start_tolerant().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].key, b"before");
}

#[test]
fn recover_torn_tail_finds_large_record_after_window_boundary() {
    let dir = temp_dir();
    let path = dir.path().join("large_record_after_torn_tail.log");

    let valid_end = {
        let mut writer = LogWriter::open(&path, 1).unwrap();
        let offset = writer.write(b"before", b"value", 0).unwrap();
        writer.sync().unwrap();
        offset + (HEADER_SIZE + b"before".len() + b"value".len()) as u64
    };

    // Leave a large incomplete body before a complete record. The complete
    // record starts beyond the first probe window and its body crosses the
    // next window, exercising streamed candidate CRC validation.
    let declared_value_len = TORN_TAIL_PROBE_WINDOW_BYTES * 3 + 128;
    let partial_value_len = TORN_TAIL_PROBE_WINDOW_BYTES + 128;
    let torn_value = vec![0u8; declared_value_len];
    let torn = encode_record(b"torn", &torn_value, 0);
    let prefix_len = HEADER_SIZE + b"torn".len() + partial_value_len;
    assert!(prefix_len < torn.len());

    let after_value = vec![0x5Au8; TORN_TAIL_PROBE_WINDOW_BYTES + 123];
    let after = encode_record(b"after", &after_value, 0);
    let after_offset = valid_end + prefix_len as u64;

    {
        use std::io::Write as _;

        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        file.write_all(&torn[..prefix_len]).unwrap();
        file.write_all(&after).unwrap();
        file.sync_data().unwrap();
    }

    let before_recovery = std::fs::read(&path).unwrap();
    let error = recover_torn_tail(&path).unwrap_err();
    assert!(
        error.0.contains("later record candidate"),
        "unexpected error: {error}"
    );
    assert_eq!(
        std::fs::read(&path).unwrap(),
        before_recovery,
        "ambiguous recovery must preserve the later record"
    );

    let mut reader = LogReader::open(&path).unwrap();
    let recovered = reader.read_at(after_offset).unwrap().unwrap();
    assert_eq!(recovered.key, b"after");
    assert_eq!(recovered.value, Some(after_value));
}

#[test]
fn recover_torn_tail_finds_header_straddling_window_boundary() {
    let dir = temp_dir();
    let path = dir.path().join("header_straddles_torn_tail_window.log");

    let valid_end = {
        let mut writer = LogWriter::open(&path, 1).unwrap();
        let offset = writer.write(b"before", b"value", 0).unwrap();
        writer.sync().unwrap();
        offset + (HEADER_SIZE + b"before".len() + b"value".len()) as u64
    };

    let torn_key = b"torn";
    let target_after_offset = TORN_TAIL_PROBE_WINDOW_BYTES - 5;
    let partial_value_len = target_after_offset - HEADER_SIZE - torn_key.len();
    let declared_value_len = partial_value_len + 128;
    let torn_value = vec![0u8; declared_value_len];
    let torn = encode_record(torn_key, &torn_value, 0);
    let prefix_len = HEADER_SIZE + torn_key.len() + partial_value_len;
    assert_eq!(prefix_len, target_after_offset);

    // The one-byte body keeps the complete record inside the probe halo while
    // its 26-byte header crosses the first window boundary.
    let after = encode_record(b"x", b"", 0);
    let after_offset = valid_end + prefix_len as u64;

    {
        use std::io::Write as _;

        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        file.write_all(&torn[..prefix_len]).unwrap();
        file.write_all(&after).unwrap();
        file.sync_data().unwrap();
    }

    let before_recovery = std::fs::read(&path).unwrap();
    let error = recover_torn_tail(&path).unwrap_err();
    assert!(
        error.0.contains("later record candidate"),
        "unexpected error: {error}"
    );
    assert_eq!(std::fs::read(&path).unwrap(), before_recovery);

    let mut reader = LogReader::open(&path).unwrap();
    let recovered = reader.read_at(after_offset).unwrap().unwrap();
    assert_eq!(recovered.key, b"x");
    assert_eq!(recovered.value, Some(Vec::new()));
}

#[test]
fn validated_history_prefix_binds_complete_records_and_detects_corruption() {
    use sha2::Digest as _;
    use std::io::{Seek as _, Write as _};

    let dir = temp_dir();
    let path = dir.path().join("history_prefix.log");

    let prefix_bytes = {
        let mut writer = LogWriter::open(&path, 1).unwrap();
        writer.write(b"first", b"value-one", 0).unwrap();
        writer.sync().unwrap();
        std::fs::metadata(&path).unwrap().len()
    };

    let digest = validated_prefix_digest(&path, prefix_bytes).unwrap();
    let expected: [u8; 32] = sha2::Sha256::digest(std::fs::read(&path).unwrap()).into();
    assert_eq!(digest, expected);

    {
        let mut writer = LogWriter::open(&path, 1).unwrap();
        writer.write(b"second", b"value-two", 0).unwrap();
        writer.sync().unwrap();
    }

    assert_eq!(validated_prefix_digest(&path, prefix_bytes).unwrap(), digest);
    assert!(validated_prefix_digest(&path, prefix_bytes + 1).is_err());

    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    file.seek(SeekFrom::Start(HEADER_SIZE as u64 + 1)).unwrap();
    file.write_all(&[0xFF]).unwrap();
    file.sync_data().unwrap();

    assert!(validated_prefix_digest(&path, prefix_bytes).is_err());
}
