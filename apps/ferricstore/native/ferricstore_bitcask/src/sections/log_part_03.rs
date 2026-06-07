/// Read a record at `offset` using `pread` (1 syscall instead of seek+read = 2).
///
/// C-3 fix: `pread` is atomic, does not modify the file offset, and is
/// thread-safe. This allows concurrent reads from the same fd without a mutex.
///
/// Public alias for use by NIF functions that open a `File` directly.
#[cfg(unix)]
pub fn pread_record_from_file(file: &File, offset: u64) -> Result<Option<Record>> {
    pread_record(file, offset)
}

pub fn pread_value_for_key_from_file(
    file: &File,
    offset: u64,
    expected_key: &[u8],
) -> Result<Option<Option<Vec<u8>>>> {
    pread_value_for_key(file, offset, expected_key)
}

pub fn copy_record_raw_from_file(
    file: &File,
    writer: &mut LogWriter,
    offset: u64,
    copy_tombstone: bool,
) -> Result<Option<RawCopyResult>> {
    copy_record_raw(file, writer, offset, copy_tombstone)
}

#[cfg(unix)]
fn pread_record(file: &File, offset: u64) -> Result<Option<Record>> {
    // Step 1: pread the header
    let mut header = [0u8; HEADER_SIZE];
    match read_exact_at_or_eof(file, &mut header, offset, "header")? {
        false => return Ok(None),
        true => {}
    }

    let stored_crc = u32::from_le_bytes(header[0..4].try_into().unwrap());
    let timestamp_ms = u64::from_le_bytes(header[4..12].try_into().unwrap());
    let expire_at_ms = u64::from_le_bytes(header[12..20].try_into().unwrap());
    let key_size = u16::from_le_bytes(header[20..22].try_into().unwrap()) as usize;
    let value_size_raw = u32::from_le_bytes(header[22..26].try_into().unwrap());
    let is_tombstone = value_size_raw == TOMBSTONE;
    let value_size = decoded_value_size(value_size_raw, is_tombstone)?;

    // Step 2: pread key + value in a single call
    let actual_value_size = value_size;
    let body_len = checked_record_body_len(key_size, actual_value_size)?;
    if body_len > BODY_LEN_FILE_SIZE_CHECK_THRESHOLD {
        let body_start = offset
            .checked_add(HEADER_SIZE as u64)
            .ok_or_else(|| LogError("record body offset overflow".into()))?;
        let body_end = body_start
            .checked_add(body_len as u64)
            .ok_or_else(|| LogError("record body end offset overflow".into()))?;
        let file_len = file.metadata()?.len();
        if body_end > file_len {
            return Err(LogError(format!(
                "record body extends past end of file: end={body_end}, file_len={file_len}"
            )));
        }
    }

    let mut body = vec![0u8; body_len];
    if body_len > 0 {
        let body_offset = offset + HEADER_SIZE as u64;
        read_exact_at_or_eof(file, &mut body, body_offset, "body")?;
    }

    let key = body[..key_size].to_vec();
    let value = body[key_size..].to_vec();

    // C-5 fix: verify CRC incrementally (no throwaway Vec).
    // For tombstones, the CRC covers only the header + key (no value bytes).
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&header[4..]);
    hasher.update(&key);
    if !is_tombstone {
        hasher.update(&value);
    }
    let computed_crc = hasher.finalize();

    if computed_crc != stored_crc {
        return Err(LogError(format!(
            "CRC mismatch: stored={stored_crc}, computed={computed_crc}"
        )));
    }

    let record = Record {
        timestamp_ms,
        expire_at_ms,
        key,
        value: if is_tombstone { None } else { Some(value) },
    };

    Ok(Some(record))
}

#[cfg(unix)]
fn read_exact_at_or_eof(file: &File, buf: &mut [u8], offset: u64, label: &str) -> Result<bool> {
    let mut read_total = 0usize;

    while read_total < buf.len() {
        let read_offset = offset
            .checked_add(read_total as u64)
            .ok_or_else(|| LogError(format!("pread {label} offset overflow")))?;

        match file.read_at(&mut buf[read_total..], read_offset) {
            Ok(0) if read_total == 0 => return Ok(false),
            Ok(0) => {
                return Err(LogError(format!(
                    "pread {label} short read: expected {} B, got {read_total} B",
                    buf.len()
                )));
            }
            Ok(n) => read_total += n,
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof && read_total == 0 => {
                return Ok(false);
            }
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                return Err(LogError(format!(
                    "pread {label} short read: expected {} B, got {read_total} B",
                    buf.len()
                )));
            }
            Err(e) => return Err(e.into()),
        }
    }

    Ok(true)
}

fn read_next_record(reader: &mut impl Read) -> Result<Option<Record>> {
    let mut header = [0u8; HEADER_SIZE];
    match reader.read_exact(&mut header) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }

    let stored_crc = u32::from_le_bytes(header[0..4].try_into().unwrap());
    let timestamp_ms = u64::from_le_bytes(header[4..12].try_into().unwrap());
    let expire_at_ms = u64::from_le_bytes(header[12..20].try_into().unwrap());
    let key_size = u16::from_le_bytes(header[20..22].try_into().unwrap()) as usize;
    let value_size_raw = u32::from_le_bytes(header[22..26].try_into().unwrap());
    let is_tombstone = value_size_raw == TOMBSTONE;
    let value_size = decoded_value_size(value_size_raw, is_tombstone)?;

    let key = read_exact_vec(reader, key_size, "key")?;

    let value = if is_tombstone {
        vec![]
    } else {
        read_exact_vec(reader, value_size, "value")?
    };

    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&header[4..]);
    hasher.update(&key);
    if !is_tombstone {
        hasher.update(&value);
    }
    let computed_crc = hasher.finalize();

    if computed_crc != stored_crc {
        return Err(LogError(format!(
            "CRC mismatch: stored={stored_crc}, computed={computed_crc}"
        )));
    }

    let record = Record {
        timestamp_ms,
        expire_at_ms,
        key,
        value: if is_tombstone { None } else { Some(value) },
    };

    Ok(Some(record))
}

#[cfg(unix)]
fn pread_value_for_key(
    file: &File,
    offset: u64,
    expected_key: &[u8],
) -> Result<Option<Option<Vec<u8>>>> {
    let mut header = [0u8; HEADER_SIZE];
    match read_exact_at_or_eof(file, &mut header, offset, "header")? {
        false => return Ok(None),
        true => {}
    }

    let stored_crc = u32::from_le_bytes(header[0..4].try_into().unwrap());
    let key_size = u16::from_le_bytes(header[20..22].try_into().unwrap()) as usize;
    let value_size_raw = u32::from_le_bytes(header[22..26].try_into().unwrap());

    let mut key = vec![0u8; key_size];
    if key_size > 0 {
        let key_offset = offset
            .checked_add(HEADER_SIZE as u64)
            .ok_or_else(|| LogError("keyed pread key offset overflow".into()))?;
        read_exact_at_or_eof(file, &mut key, key_offset, "key")?;
    }

    if key != expected_key {
        return Ok(None);
    }

    let is_tombstone = value_size_raw == TOMBSTONE;
    let value_size = decoded_value_size(value_size_raw, is_tombstone)?;

    if !is_tombstone && value_size > BODY_LEN_FILE_SIZE_CHECK_THRESHOLD {
        let value_start = offset
            .checked_add(HEADER_SIZE as u64)
            .and_then(|off| off.checked_add(key_size as u64))
            .ok_or_else(|| LogError("keyed pread value offset overflow".into()))?;
        let value_end = value_start
            .checked_add(value_size as u64)
            .ok_or_else(|| LogError("keyed pread value end overflow".into()))?;
        let file_len = file.metadata()?.len();
        if value_end > file_len {
            return Err(LogError(format!(
                "record value extends past end of file: end={value_end}, file_len={file_len}"
            )));
        }
    }

    let value = if is_tombstone {
        Vec::new()
    } else {
        let mut value = vec![0u8; value_size];
        if value_size > 0 {
            let value_offset = offset
                .checked_add(HEADER_SIZE as u64)
                .and_then(|off| off.checked_add(key_size as u64))
                .ok_or_else(|| LogError("keyed pread value offset overflow".into()))?;
            read_exact_at_or_eof(file, &mut value, value_offset, "value")?;
        }
        value
    };

    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&header[4..]);
    hasher.update(&key);
    if !is_tombstone {
        hasher.update(&value);
    }
    let computed_crc = hasher.finalize();

    if computed_crc != stored_crc {
        return Err(LogError(format!(
            "CRC mismatch: stored={stored_crc}, computed={computed_crc}"
        )));
    }

    if is_tombstone {
        Ok(Some(None))
    } else {
        Ok(Some(Some(value)))
    }
}

#[cfg(unix)]
fn copy_record_raw(
    file: &File,
    writer: &mut LogWriter,
    offset: u64,
    copy_tombstone: bool,
) -> Result<Option<RawCopyResult>> {
    let mut header = [0u8; HEADER_SIZE];
    match read_exact_at_or_eof(file, &mut header, offset, "header")? {
        false => return Ok(None),
        true => {}
    }

    let stored_crc = u32::from_le_bytes(header[0..4].try_into().unwrap());
    let key_size = u16::from_le_bytes(header[20..22].try_into().unwrap()) as usize;
    let value_size_raw = u32::from_le_bytes(header[22..26].try_into().unwrap());
    let is_tombstone = value_size_raw == TOMBSTONE;
    let value_size = decoded_value_size(value_size_raw, is_tombstone)?;

    if is_tombstone && !copy_tombstone {
        return Ok(None);
    }

    let body_len = checked_record_body_len(key_size, value_size)?;
    let record_size = HEADER_SIZE as u64 + body_len as u64;

    if body_len > BODY_LEN_FILE_SIZE_CHECK_THRESHOLD {
        let body_start = offset
            .checked_add(HEADER_SIZE as u64)
            .ok_or_else(|| LogError("raw copy body offset overflow".into()))?;
        let body_end = body_start
            .checked_add(body_len as u64)
            .ok_or_else(|| LogError("raw copy body end overflow".into()))?;
        let file_len = file.metadata()?.len();
        if body_end > file_len {
            return Err(LogError(format!(
                "record body extends past end of file: end={body_end}, file_len={file_len}"
            )));
        }
    }

    let new_offset = writer.offset;
    writer.write_raw(&header)?;

    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&header[4..]);

    let key_offset = offset
        .checked_add(HEADER_SIZE as u64)
        .ok_or_else(|| LogError("raw copy key offset overflow".into()))?;
    copy_hashed_range(file, writer, key_offset, key_size, &mut hasher, "key")?;

    if !is_tombstone {
        let value_offset = key_offset
            .checked_add(key_size as u64)
            .ok_or_else(|| LogError("raw copy value offset overflow".into()))?;
        copy_hashed_range(file, writer, value_offset, value_size, &mut hasher, "value")?;
    }

    let computed_crc = hasher.finalize();
    if computed_crc != stored_crc {
        return Err(LogError(format!(
            "CRC mismatch: stored={stored_crc}, computed={computed_crc}"
        )));
    }

    Ok(Some(RawCopyResult {
        offset: new_offset,
        record_size,
        is_tombstone,
    }))
}

#[cfg(unix)]
fn copy_hashed_range(
    file: &File,
    writer: &mut LogWriter,
    offset: u64,
    len: usize,
    hasher: &mut crc32fast::Hasher,
    label: &str,
) -> Result<()> {
    let mut remaining = len;
    let mut read_offset = offset;
    let mut buf = vec![0u8; STREAM_READ_CHUNK_SIZE.min(len.max(1))];

    while remaining > 0 {
        let chunk_len = remaining.min(STREAM_READ_CHUNK_SIZE);
        let chunk = &mut buf[..chunk_len];

        if !read_exact_at_or_eof(file, chunk, read_offset, label)? {
            return Err(LogError(format!(
                "pread {label} short read: expected {chunk_len} B, got 0 B"
            )));
        }

        hasher.update(chunk);
        writer.write_raw(chunk)?;
        remaining -= chunk_len;
        read_offset = read_offset
            .checked_add(chunk_len as u64)
            .ok_or_else(|| LogError(format!("raw copy {label} offset overflow")))?;
    }

    Ok(())
}

fn iter_metadata_tolerant(
    reader: &mut impl Read,
    start_offset: u64,
) -> Result<Vec<RecordMetadata>> {
    let mut records = Vec::new();
    let mut offset = start_offset;

    while let Ok(Some(record)) = read_next_record_metadata(reader, offset) {
        offset = offset
            .checked_add(record.record_size)
            .ok_or_else(|| LogError("record offset overflow".into()))?;
        records.push(record);
    }

    Ok(records)
}

fn iter_metadata_page_tolerant(
    reader: &mut impl Read,
    start_offset: u64,
    limit: usize,
) -> Result<(Vec<RecordMetadata>, u64, bool)> {
    let mut records = Vec::new();
    let mut offset = start_offset;

    if limit == 0 {
        return Ok((records, offset, false));
    }

    while records.len() < limit {
        match read_next_record_metadata(reader, offset) {
            Ok(Some(record)) => {
                offset = offset
                    .checked_add(record.record_size)
                    .ok_or_else(|| LogError("record offset overflow".into()))?;
                records.push(record);
            }
            Ok(None) => return Ok((records, offset, true)),
            Err(_err) => return Ok((records, offset, true)),
        }
    }

    Ok((records, offset, false))
}

fn read_next_record_metadata(
    reader: &mut impl Read,
    offset: u64,
) -> Result<Option<RecordMetadata>> {
    let mut header = [0u8; HEADER_SIZE];
    match reader.read_exact(&mut header) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }

    let stored_crc = u32::from_le_bytes(header[0..4].try_into().unwrap());
    let timestamp_ms = u64::from_le_bytes(header[4..12].try_into().unwrap());
    let expire_at_ms = u64::from_le_bytes(header[12..20].try_into().unwrap());
    let key_size = u16::from_le_bytes(header[20..22].try_into().unwrap()) as usize;
    let value_size_raw = u32::from_le_bytes(header[22..26].try_into().unwrap());
    let is_tombstone = value_size_raw == TOMBSTONE;
    let value_size = decoded_value_size(value_size_raw, is_tombstone)?;

    let key = read_exact_vec(reader, key_size, "key")?;

    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&header[4..]);
    hasher.update(&key);
    if !is_tombstone {
        hash_exact(reader, value_size, &mut hasher, "value")?;
    }
    let computed_crc = hasher.finalize();

    if computed_crc != stored_crc {
        return Err(LogError(format!(
            "CRC mismatch: stored={stored_crc}, computed={computed_crc}"
        )));
    }

    let record_size =
        HEADER_SIZE as u64 + key_size as u64 + if is_tombstone { 0 } else { value_size as u64 };

    Ok(Some(RecordMetadata {
        timestamp_ms,
        expire_at_ms,
        key,
        value_size: if is_tombstone { 0 } else { value_size_raw },
        is_tombstone,
        offset,
        record_size,
    }))
}

/// CRC32 using hardware acceleration (SSE4.2 on x86, ARM CRC on aarch64).
///
/// C-1 fix: replaces the hand-rolled byte-at-a-time CRC32 with `crc32fast`
/// which auto-detects and uses hardware CRC32 instructions at runtime.
/// This is ~50x faster for typical value sizes (256B-4KB).
///
/// Note: `crc32fast` uses the same CRC-32/ISO-HDLC polynomial (0xEDB88320)
/// as the previous hand-rolled implementation, so existing data files remain
/// compatible — no migration needed.
fn crc32(data: &[u8]) -> u32 {
    crc32fast::hash(data)
}

fn now_ms() -> u64 {
    #[allow(clippy::cast_possible_truncation)]
    // millis won't exceed u64::MAX until year 584 million
    let ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    ms
}

