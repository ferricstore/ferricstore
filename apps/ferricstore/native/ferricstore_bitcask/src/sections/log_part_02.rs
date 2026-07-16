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
    /// truncated/corrupt tail was reached, matching `iter_metadata_*_tolerant`.
    pub fn iter_metadata_page_from_offset_tolerant(
        &mut self,
        offset: u64,
        limit: usize,
    ) -> Result<(Vec<RecordMetadata>, u64, bool)> {
        self.file.seek(SeekFrom::Start(offset))?;
        iter_metadata_page_tolerant(&mut self.file, offset, limit)
    }
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
