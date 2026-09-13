// Tail probing is only used after a structural truncation. Read one bounded
// window at a time so a corrupt header cannot force the whole suffix into
// memory. The overlap lets candidate headers straddle adjacent windows.
const TORN_TAIL_PROBE_WINDOW_BYTES: usize = 1024 * 1024;
const MAX_TORN_TAIL_CRC_BYTES_PER_WINDOW: usize = 32 * 1024 * 1024;

/// Reads records from a log file at arbitrary offsets or sequentially.
pub struct LogReader {
    file: File,
}

impl LogReader {
    /// # Errors
    ///
    /// Returns a `LogError` if the file cannot be opened.
    pub fn open(path: &Path) -> Result<Self> {
        let file = crate::open_random_read(path)?;
        Ok(Self { file })
    }

    /// Read the record at `offset`. Returns `None` at EOF.
    ///
    /// Uses `pread` (1 syscall) instead of `seek + read` (2 syscalls).
    /// `pread` is atomic, does not modify the file offset, and is thread-safe.
    ///
    /// # Errors
    ///
    /// Returns a `LogError` if the file cannot be read or the record is malformed.
    pub fn read_at(&mut self, offset: u64) -> Result<Option<Record>> {
        pread_record(&self.file, offset)
    }

    /// Iterate all records from the start of the file.
    ///
    /// # Errors
    ///
    /// Returns a `LogError` if the file cannot be read or contains a malformed record.
    pub fn iter_from_start(&mut self) -> Result<Vec<Record>> {
        self.file.seek(SeekFrom::Start(0))?;
        let mut records = Vec::new();
        while let Some(record) = read_next_record(&mut self.file)? {
            records.push(record);
        }
        Ok(records)
    }

    /// Seek to the given offset in the log file.
    ///
    /// # Errors
    ///
    /// Returns a `LogError` if the seek fails.
    pub fn seek_to(&mut self, offset: u64) -> Result<()> {
        self.file.seek(SeekFrom::Start(offset))?;
        Ok(())
    }

    /// Read the next record at the current file position. Returns `None` at EOF.
    ///
    /// # Errors
    ///
    /// Returns a `LogError` if the record is malformed.
    pub fn read_next(&mut self) -> Result<Option<Record>> {
        read_next_record(&mut self.file)
    }

    /// Read the next record's metadata at the current file position without
    /// materializing its value bytes.
    pub fn read_next_metadata(&mut self, offset: u64) -> Result<Option<RecordMetadata>> {
        read_next_record_metadata(&mut self.file, offset)
    }

    /// Iterate records tolerating a truncated tail (crash-recovery mode).
    ///
    /// Like `iter_from_start`, but stops silently at a truncated final record.
    /// Integrity and format errors are returned so recovery cannot turn
    /// mid-file corruption into silent truncation and data loss.
    ///
    /// # Errors
    ///
    /// Returns a `LogError` for I/O, integrity, and format errors. A structurally
    /// truncated final record is the only error tolerated as crash residue.
    pub fn iter_from_start_tolerant(&mut self) -> Result<Vec<Record>> {
        self.file.seek(SeekFrom::Start(0))?;
        let mut records = Vec::new();
        loop {
            match read_next_record(&mut self.file) {
                Ok(Some(record)) => records.push(record),
                Ok(None) => break,
                Err(error) if error.is_truncated_record() => break,
                Err(error) => return Err(error),
            }
        }
        Ok(records)
    }

    /// Iterate records from an exact byte offset, tolerating a truncated tail.
    ///
    /// Used after hint recovery to replay only records appended after the hint
    /// boundary, instead of rescanning the whole hinted active file.
    pub fn iter_from_offset_tolerant(&mut self, offset: u64) -> Result<Vec<Record>> {
        self.file.seek(SeekFrom::Start(offset))?;
        let mut records = Vec::new();
        loop {
            match read_next_record(&mut self.file) {
                Ok(Some(record)) => records.push(record),
                Ok(None) => break,
                Err(error) if error.is_truncated_record() => break,
                Err(error) => return Err(error),
            }
        }
        Ok(records)
    }

    /// Iterate record metadata without materializing value bytes.
    ///
    /// Used by startup, recovery, and compaction keydir scans where only key,
    /// offset, value size, expiry, and tombstone state are needed.
    #[cfg(test)]
    pub fn iter_metadata_from_start_tolerant(&mut self) -> Result<Vec<RecordMetadata>> {
        self.file.seek(SeekFrom::Start(0))?;
        iter_metadata_tolerant(&mut self.file, 0)
    }

    /// Iterate record metadata from an exact byte offset without materializing values.
    #[cfg(test)]
    pub fn iter_metadata_from_offset_tolerant(
        &mut self,
        offset: u64,
    ) -> Result<Vec<RecordMetadata>> {
        self.file.seek(SeekFrom::Start(offset))?;
        iter_metadata_tolerant(&mut self.file, offset)
    }

    /// Read a bounded page of record metadata from an exact byte offset.
    ///
    /// This keeps BEAM startup recovery from materializing a whole large
    /// Bitcask file scan in one NIF result. `done=true` means EOF or a tolerant
    /// structurally truncated tail was reached; integrity errors are returned.
    pub fn iter_metadata_page_from_offset_tolerant(
        &mut self,
        offset: u64,
        limit: usize,
    ) -> Result<(Vec<RecordMetadata>, u64, bool)> {
        self.file.seek(SeekFrom::Start(offset))?;
        iter_metadata_page_tolerant(&mut self.file, offset, limit)
    }
}

/// Recover the append boundary of an active log after a structurally torn tail.
///
/// A short final header is unambiguous: no complete record can fit in fewer
/// than `HEADER_SIZE` bytes, so the bytes can be discarded. Once a complete
/// header exists but its key/value body is short, probe the bounded suffix for
/// a later CRC-valid record. A candidate, or an exceeded probe budget, fails
/// closed and leaves the file untouched. CRC and format errors remain fatal as
/// well. Any truncation is serialized with all in-process appenders and synced
/// before the repaired boundary is returned.
pub fn recover_torn_tail(path: &Path) -> Result<u64> {
    let write_file = crate::open_write_nofollow(path)?;
    let append_lock = io_backend::append_lock_for_file(path, &write_file)?;
    let _guard = io_backend::lock_append(&append_lock)?;

    // Re-read the size after taking the append lock. A writer may have
    // completed while this recovery call was waiting for the lock.
    let file_len = write_file.metadata()?.len();
    let read_file = crate::open_random_read(path)?;
    if !same_file_identity(&write_file, &read_file)? {
        return Err(LogError("log path changed while recovering".into()));
    }

    let mut reader = std::io::BufReader::new(read_file);
    let mut valid_end = 0u64;
    let mut saw_truncated_tail = false;

    loop {
        match read_next_record_metadata(&mut reader, valid_end) {
            Ok(Some(record)) => {
                valid_end = valid_end
                    .checked_add(record.record_size)
                    .ok_or_else(|| LogError("record offset overflow".into()))?;
            }
            Ok(None) => break,
            Err(error) if error.is_truncated_record() => {
                saw_truncated_tail = true;
                break;
            }
            Err(error) => return Err(error),
        }
    }

    let mut read_file = reader.into_inner();

    let current_file_len = write_file.metadata()?.len();
    if current_file_len != file_len {
        return Err(LogError(format!(
            "log changed while recovering: started at {file_len} bytes, ended at {current_file_len}"
        )));
    }

    if valid_end > current_file_len {
        return Err(LogError(format!(
            "log grew while recovering: scanned through {valid_end} bytes, expected {current_file_len}"
        )));
    }

    if saw_truncated_tail && current_file_len - valid_end >= HEADER_SIZE as u64 {
        match probe_torn_tail(&mut read_file, valid_end, current_file_len)? {
            TornTailProbe::NoLaterRecord => {}
            TornTailProbe::LaterRecord => {
                return Err(LogError(format!(
                    "ambiguous truncated tail at {valid_end} bytes; preserving {current_file_len} bytes (later record candidate)"
                )));
            }
            TornTailProbe::BudgetExceeded => {
                return Err(LogError(format!(
                    "ambiguous truncated tail at {valid_end} bytes; preserving {current_file_len} bytes (probe budget exceeded)"
                )));
            }
        }
    }

    if valid_end < current_file_len {
        let final_file_len = write_file.metadata()?.len();
        if final_file_len != current_file_len {
            return Err(LogError(format!(
                "log changed while recovering: started at {current_file_len} bytes, ended at {final_file_len}"
            )));
        }
        write_file.set_len(valid_end)?;
        write_file.sync_data()?;
    }

    Ok(valid_end)
}

#[cfg(unix)]
fn same_file_identity(first: &File, second: &File) -> std::io::Result<bool> {
    use std::os::unix::fs::MetadataExt;

    let first_metadata = first.metadata()?;
    let second_metadata = second.metadata()?;
    Ok(first_metadata.dev() == second_metadata.dev()
        && first_metadata.ino() == second_metadata.ino())
}

#[cfg(not(unix))]
fn same_file_identity(_first: &File, _second: &File) -> std::io::Result<bool> {
    // The supported non-Unix targets do not expose a portable file identity
    // through std::fs. Both opens still reject final-component symlinks.
    Ok(true)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TornTailProbe {
    NoLaterRecord,
    LaterRecord,
    BudgetExceeded,
}

/// Search a torn suffix for any complete CRC-valid record candidate.
///
/// There is no sync marker in the record format, so every byte is a possible
/// candidate boundary. Scan one MiB windows with a header-sized overlap and
/// reset the CRC-work budget for each window. This keeps work linear in the
/// suffix length while still allowing an ordinary large crash tail to be
/// repaired. Candidate bodies are hashed directly from the window when they
/// fit; otherwise they are streamed through one fixed-size buffer.
fn probe_torn_tail(
    file: &mut File,
    suffix_start: u64,
    file_len: u64,
) -> Result<TornTailProbe> {
    let suffix_len = file_len
        .checked_sub(suffix_start)
        .ok_or_else(|| LogError("torn tail starts past end of file".into()))?;
    if suffix_len < HEADER_SIZE as u64 {
        return Ok(TornTailProbe::NoLaterRecord);
    }

    let max_candidate_start = file_len - HEADER_SIZE as u64;
    let overlap = HEADER_SIZE - 1;
    let window_capacity = TORN_TAIL_PROBE_WINDOW_BYTES + overlap;
    let suffix_len_usize = usize::try_from(suffix_len).unwrap_or(window_capacity);
    let mut window = vec![0u8; window_capacity.min(suffix_len_usize)];
    let mut chunk_start = suffix_start;

    while chunk_start <= max_candidate_start {
        let read_start = chunk_start;
        let read_end = chunk_start
            .checked_add(window.len() as u64)
            .unwrap_or(file_len)
            .min(file_len);
        let window_len = usize::try_from(read_end - read_start)
            .map_err(|_| LogError("torn tail probe window is too large".into()))?;

        file.seek(SeekFrom::Start(read_start))?;
        file.read_exact(&mut window[..window_len])?;

        let next_chunk_start = chunk_start
            .checked_add(TORN_TAIL_PROBE_WINDOW_BYTES as u64)
            .unwrap_or(max_candidate_start + 1);
        let candidate_end = next_chunk_start.min(max_candidate_start + 1);
        let mut crc_work = 0usize;

        let mut candidate_start = chunk_start;
        while candidate_start < candidate_end {
            let header_offset = usize::try_from(candidate_start - read_start)
                .map_err(|_| LogError("torn tail candidate offset does not fit".into()))?;
            let header_end = header_offset + HEADER_SIZE;
            let header = &window[header_offset..header_end];
            let key_size = u16::from_le_bytes(header[20..22].try_into().unwrap()) as usize;
            let value_size_raw = u32::from_le_bytes(header[22..26].try_into().unwrap());
            let value_size = if value_size_raw == TOMBSTONE {
                0
            } else {
                let value_size = value_size_raw as usize;
                if value_size > MAX_VALUE_SIZE {
                    candidate_start += 1;
                    continue;
                }
                value_size
            };
            let Some(body_len) = key_size.checked_add(value_size) else {
                candidate_start += 1;
                continue;
            };
            let Some(record_len) = HEADER_SIZE.checked_add(body_len) else {
                candidate_start += 1;
                continue;
            };
            let Some(record_end) = candidate_start.checked_add(record_len as u64) else {
                candidate_start += 1;
                continue;
            };
            if record_end > file_len {
                candidate_start += 1;
                continue;
            }

            let crc_len = record_len - std::mem::size_of::<u32>();
            if crc_len > MAX_TORN_TAIL_CRC_BYTES_PER_WINDOW
                || crc_work > MAX_TORN_TAIL_CRC_BYTES_PER_WINDOW - crc_len
            {
                return Ok(TornTailProbe::BudgetExceeded);
            }
            crc_work += crc_len;

            let stored_crc = u32::from_le_bytes(header[0..4].try_into().unwrap());
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&header[4..]);

            let body_offset = header_end;
            let body_end = body_offset + body_len;
            let window_end = window_len;
            if body_end <= window_end {
                hasher.update(&window[body_offset..body_end]);
            } else {
                let body_file_offset = candidate_start
                    .checked_add(HEADER_SIZE as u64)
                    .ok_or_else(|| LogError("torn tail body offset overflow".into()))?;
                file.seek(SeekFrom::Start(body_file_offset))?;
                hash_exact(file, body_len, &mut hasher, "torn tail candidate")?;
            }

            if hasher.finalize() == stored_crc {
                return Ok(TornTailProbe::LaterRecord);
            }

            candidate_start += 1;
        }

        if candidate_end > max_candidate_start {
            break;
        }
        chunk_start = next_chunk_start;
    }

    Ok(TornTailProbe::NoLaterRecord)
}

// ---------------------------------------------------------------------------
// Encoding helpers
// ---------------------------------------------------------------------------

/// Validates key and value sizes before encoding. Returns Ok(()) or an error
/// message if either exceeds the on-disk format limits (key: u16, value: u32).
pub(crate) fn validate_kv_sizes(key: &[u8], value: &[u8]) -> std::result::Result<(), String> {
    let max_key = usize::from(u16::MAX);
    if key.len() > max_key {
        return Err(format!(
            "key too large: {} bytes (max {max_key})",
            key.len()
        ));
    }
    if value.len() > MAX_VALUE_SIZE {
        return Err(format!(
            "value too large: {} bytes (max {MAX_VALUE_SIZE})",
            value.len()
        ));
    }
    Ok(())
}

pub(crate) fn decoded_value_size(value_size_raw: u32, is_tombstone: bool) -> Result<usize> {
    if is_tombstone {
        return Ok(0);
    }

    let value_size = value_size_raw as usize;
    if value_size > MAX_VALUE_SIZE {
        return Err(LogError(format!(
            "value too large in log record: {value_size} bytes (max {MAX_VALUE_SIZE})"
        )));
    }
    Ok(value_size)
}

fn checked_record_body_len(key_size: usize, value_size: usize) -> Result<usize> {
    key_size
        .checked_add(value_size)
        .ok_or_else(|| LogError("record body length overflow".into()))
}

impl LogError {
    pub(crate) fn is_truncated_record(&self) -> bool {
        self.0.starts_with("truncated record ")
    }
}

fn read_exact_vec(reader: &mut impl Read, len: usize, label: &str) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(len.min(STREAM_READ_CHUNK_SIZE));
    let mut chunk = [0u8; STREAM_READ_CHUNK_SIZE];
    let mut remaining = len;

    while remaining > 0 {
        let to_read = remaining.min(STREAM_READ_CHUNK_SIZE);
        match reader.read_exact(&mut chunk[..to_read]) {
            Ok(()) => {
                out.extend_from_slice(&chunk[..to_read]);
                remaining -= to_read;
            }
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                return Err(LogError(format!(
                    "truncated record {label}: expected {len} bytes"
                )));
            }
            Err(e) => return Err(e.into()),
        }
    }

    Ok(out)
}

fn hash_exact(
    reader: &mut impl Read,
    len: usize,
    hasher: &mut crc32fast::Hasher,
    label: &str,
) -> Result<()> {
    let mut chunk = [0u8; STREAM_READ_CHUNK_SIZE];
    let mut remaining = len;

    while remaining > 0 {
        let to_read = remaining.min(STREAM_READ_CHUNK_SIZE);
        match reader.read_exact(&mut chunk[..to_read]) {
            Ok(()) => {
                hasher.update(&chunk[..to_read]);
                remaining -= to_read;
            }
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                return Err(LogError(format!(
                    "truncated record {label}: expected {len} bytes"
                )));
            }
            Err(e) => return Err(e.into()),
        }
    }

    Ok(())
}

/// Encode a record into a single `Vec` allocation.
///
/// C-4 fix: pre-allocate a single `Vec` with a CRC placeholder, write the
/// body directly, then compute CRC over `buf[4..]` and patch the first 4
/// bytes. This eliminates the second `Vec` that was previously needed.
///
/// C-1 fix: uses `crc32fast` for hardware-accelerated CRC32 (SSE4.2 / ARM CRC).
pub(crate) fn encode_record(key: &[u8], value: &[u8], expire_at_ms: u64) -> Vec<u8> {
    let total = record_len(key.len(), value.len());
    let mut buf = Vec::with_capacity(total);
    encode_record_into(&mut buf, key, value, expire_at_ms);
    buf
}

fn encode_tombstone(key: &[u8]) -> Vec<u8> {
    let total = tombstone_len(key.len());
    let mut buf = Vec::with_capacity(total);
    encode_tombstone_into(&mut buf, key);
    buf
}

fn record_len(key_len: usize, value_len: usize) -> usize {
    HEADER_SIZE + key_len + value_len
}

fn tombstone_len(key_len: usize) -> usize {
    HEADER_SIZE + key_len
}

fn encode_record_into(buf: &mut Vec<u8>, key: &[u8], value: &[u8], expire_at_ms: u64) {
    let start = buf.len();
    let now_ms = now_ms();
    #[allow(clippy::cast_possible_truncation)]
    let key_size = key.len() as u16;
    #[allow(clippy::cast_possible_truncation)]
    let value_size = value.len() as u32;

    buf.extend_from_slice(&[0u8; 4]);
    buf.extend_from_slice(&now_ms.to_le_bytes());
    buf.extend_from_slice(&expire_at_ms.to_le_bytes());
    buf.extend_from_slice(&key_size.to_le_bytes());
    buf.extend_from_slice(&value_size.to_le_bytes());
    buf.extend_from_slice(key);
    buf.extend_from_slice(value);

    let crc = crc32(&buf[start + 4..]);
    buf[start..start + 4].copy_from_slice(&crc.to_le_bytes());
}

fn encode_tombstone_into(buf: &mut Vec<u8>, key: &[u8]) {
    // Tombstone: value_size = TOMBSTONE (u32::MAX), no value bytes.
    let start = buf.len();
    let now_ms = now_ms();
    #[allow(clippy::cast_possible_truncation)]
    let key_size = key.len() as u16;

    buf.extend_from_slice(&[0u8; 4]); // CRC placeholder
    buf.extend_from_slice(&now_ms.to_le_bytes());
    buf.extend_from_slice(&0u64.to_le_bytes()); // expire_at_ms = 0
    buf.extend_from_slice(&key_size.to_le_bytes());
    buf.extend_from_slice(&TOMBSTONE.to_le_bytes()); // sentinel
    buf.extend_from_slice(key);

    let crc = crc32(&buf[start + 4..]);
    buf[start..start + 4].copy_from_slice(&crc.to_le_bytes());
}
