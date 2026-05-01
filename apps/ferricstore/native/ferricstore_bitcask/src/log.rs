//! Append-only log file for Bitcask.
//!
//! On-disk record format (little-endian):
//!
//! ```text
//! [ crc32: u32 | timestamp_ms: u64 | expire_at_ms: u64 | key_size: u16 | value_size: u32 | key: [u8] | value: [u8] ]
//! ```
//!
//! A tombstone record has `value_size = 0` and no value bytes. It signals a
//! logical delete and is used during compaction.
//!
//! The CRC32 covers everything after the checksum field:
//!   `timestamp_ms || expire_at_ms || key_size || value_size || key || value`

use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
#[cfg(unix)]
use std::os::unix::fs::FileExt;
use std::path::Path;

use crate::io_backend::{self, IoBackend};

/// Number of header bytes before key+value data.
/// `crc32`(4) + `timestamp_ms`(8) + `expire_at_ms`(8) + `key_size`(2) + `value_size`(4) = 26
pub const HEADER_SIZE: usize = 26;

/// Sentinel `value_size` marking a tombstone (deleted key).
/// Uses `u32::MAX` so that `value_size = 0` can represent a genuine empty value.
pub const TOMBSTONE: u32 = u32::MAX;

#[derive(Debug)]
pub struct LogError(pub String);

impl std::fmt::Display for LogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LogError: {}", self.0)
    }
}

impl std::error::Error for LogError {}

impl From<io::Error> for LogError {
    fn from(e: io::Error) -> Self {
        LogError(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, LogError>;

/// A single decoded record read from the log.
#[derive(Debug, PartialEq, Eq)]
pub struct Record {
    pub timestamp_ms: u64,
    pub expire_at_ms: u64,
    pub key: Vec<u8>,
    /// `None` means tombstone (deleted).
    pub value: Option<Vec<u8>>,
}

/// Writes new records to a log file (always appends).
///
/// Uses the best available I/O backend selected at startup:
/// - On Linux with kernel ≥ 5.1: `UringBackend` (`io_uring`)
/// - Otherwise: `SyncBackend` (`BufWriter<File>`)
///
/// The `write_batch` method is the preferred high-throughput path: it
/// submits all writes in a single kernel call (on `io_uring`) then fsyncs
/// once, reducing per-batch syscall overhead from N+1 to 2.
pub struct LogWriter {
    backend: Box<dyn IoBackend>,
    /// Current write position (= file size so far).
    pub offset: u64,
    pub file_id: u64,
}

/// A record to append in a mixed Bitcask batch.
pub enum BatchWrite<'a> {
    Put {
        key: &'a [u8],
        value: &'a [u8],
        expire_at_ms: u64,
    },
    Delete {
        key: &'a [u8],
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BatchWriteResult {
    Put { offset: u64, value_len: usize },
    Delete { offset: u64, record_size: usize },
}

impl LogWriter {
    /// Open (or create) a data file for appending.
    ///
    /// Selects the best available I/O backend automatically (see module doc).
    ///
    /// # Errors
    ///
    /// Returns a `LogError` if the file cannot be opened or its metadata cannot be read.
    pub fn open(path: &Path, file_id: u64) -> Result<Self> {
        let backend = io_backend::create_backend(path).map_err(|e| LogError(e.to_string()))?;
        let offset = backend.offset();
        Ok(Self {
            backend,
            offset,
            file_id,
        })
    }

    /// Open (or create) a data file for appending with a small write buffer.
    ///
    /// M-NEW-1 fix: the v2 stateless NIF path creates a LogWriter per call
    /// and drops it after a single write or small batch. This variant uses an
    /// 8KB buffer instead of 256KB, reducing allocator churn for short-lived
    /// writers.
    ///
    /// # Errors
    ///
    /// Returns a `LogError` if the file cannot be opened or its metadata cannot be read.
    pub fn open_small(path: &Path, file_id: u64) -> Result<Self> {
        let backend =
            io_backend::create_backend_small(path).map_err(|e| LogError(e.to_string()))?;
        let offset = backend.offset();
        Ok(Self {
            backend,
            offset,
            file_id,
        })
    }

    /// Append a live record. Returns the byte offset at which it was written.
    ///
    /// # Errors
    ///
    /// Returns a `LogError` if the record cannot be encoded or written to disk.
    pub fn write(&mut self, key: &[u8], value: &[u8], expire_at_ms: u64) -> Result<u64> {
        validate_kv_sizes(key, value).map_err(LogError)?;
        let record = encode_record(key, value, expire_at_ms);
        let start = self
            .backend
            .append(&record)
            .map_err(|e| LogError(e.to_string()))?;
        self.offset = self.backend.offset();
        Ok(start)
    }

    /// Append a tombstone record (logical delete).
    ///
    /// # Errors
    ///
    /// Returns a `LogError` if the tombstone record cannot be written to disk.
    pub fn write_tombstone(&mut self, key: &[u8]) -> Result<u64> {
        validate_kv_sizes(key, &[]).map_err(LogError)?;
        let record = encode_tombstone(key);
        let start = self
            .backend
            .append(&record)
            .map_err(|e| LogError(e.to_string()))?;
        self.offset = self.backend.offset();
        Ok(start)
    }

    /// Append pre-encoded raw bytes to the log file. Returns the byte offset
    /// at which the data was written. Does NOT fsync — the caller is
    /// responsible for calling `sync()` afterwards.
    ///
    /// This is the building block for `v2_append_batch_async`: records are
    /// encoded on the NIF (BEAM Normal scheduler) thread, then the raw bytes
    /// are written on a Tokio worker thread using this method.
    ///
    /// # Errors
    ///
    /// Returns a `LogError` if the write fails.
    pub fn write_raw(&mut self, data: &[u8]) -> Result<u64> {
        let start = self
            .backend
            .append(data)
            .map_err(|e| LogError(e.to_string()))?;
        self.offset = self.backend.offset();
        Ok(start)
    }

    /// Advance the write offset without writing data. Used by the async
    /// `io_uring` path to reserve file space that will be written by an
    /// `AsyncUringBackend`.
    pub fn advance_offset(&mut self, bytes: u64) {
        self.backend.advance_offset(bytes);
        self.offset += bytes;
    }

    /// Flush the write buffer and fsync the file to durable storage.
    ///
    /// # Errors
    ///
    /// Returns a `LogError` if the flush or fsync fails.
    pub fn sync(&mut self) -> Result<()> {
        self.backend.sync().map_err(|e| LogError(e.to_string()))
    }

    /// Write multiple records in a single batch and fsync once.
    ///
    /// This is the preferred write path for `put_batch`. On `io_uring` all
    /// writes are submitted in a single `io_uring_enter` call (2 syscalls
    /// total: one for writes, one for fsync). On `SyncBackend` it falls back
    /// to N individual writes + 1 fsync.
    ///
    /// Returns `(offset, value_len)` for each entry in the same order as
    /// `entries`.
    ///
    /// # Errors
    ///
    /// Returns a `LogError` if any write or the final sync fails.
    pub fn write_batch(&mut self, entries: &[(&[u8], &[u8], u64)]) -> Result<Vec<(u64, usize)>> {
        for (key, value, _) in entries {
            validate_kv_sizes(key, value).map_err(LogError)?;
        }

        // Encode all records first (owned Vecs, so lifetimes are clear).
        let encoded: Vec<Vec<u8>> = entries
            .iter()
            .map(|(key, value, expire_at_ms)| encode_record(key, value, *expire_at_ms))
            .collect();

        let buf_refs: Vec<&[u8]> = encoded.iter().map(Vec::as_slice).collect();

        let offsets = self
            .backend
            .append_batch_and_sync(&buf_refs)
            .map_err(|e| LogError(e.to_string()))?;

        self.offset = self.backend.offset();

        Ok(offsets
            .into_iter()
            .zip(entries.iter())
            .map(|(off, (_, value, _))| (off, value.len()))
            .collect())
    }

    /// Write multiple records in a single batch **without** fsync.
    ///
    /// The data is written to the page cache (~1-10us for typical batches)
    /// but not forced to durable storage. The caller is responsible for
    /// calling `sync()` or `v2_fsync_async` later to guarantee durability.
    ///
    /// This is the fast path for the split write+fsync architecture where
    /// writes go to page cache immediately and fsync happens on a timer.
    ///
    /// Returns `(offset, value_len)` for each entry in the same order as
    /// `entries`.
    ///
    /// # Errors
    ///
    /// Returns a `LogError` if any write fails.
    pub fn write_batch_nosync(
        &mut self,
        entries: &[(&[u8], &[u8], u64)],
    ) -> Result<Vec<(u64, usize)>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }

        for (key, value, _) in entries {
            validate_kv_sizes(key, value).map_err(LogError)?;
        }

        let encoded: Vec<Vec<u8>> = entries
            .iter()
            .map(|(key, value, expire_at_ms)| encode_record(key, value, *expire_at_ms))
            .collect();

        // H-2 fix: combine all encoded records into a single buffer and write
        // once, instead of calling append() N times through BufWriter.
        let total_len: usize = encoded.iter().map(Vec::len).sum();
        let mut combined = Vec::with_capacity(total_len);
        let mut offsets = Vec::with_capacity(encoded.len());
        let mut running = self.backend.offset();
        for buf in &encoded {
            offsets.push(running);
            combined.extend_from_slice(buf);
            running += buf.len() as u64;
        }
        self.backend
            .append(&combined)
            .map_err(|e| LogError(e.to_string()))?;

        // Flush the BufWriter to the OS page cache (but NOT fsync to disk).
        // This ensures the data is visible to subsequent reads via pread.
        self.backend
            .flush_no_sync()
            .map_err(|e| LogError(e.to_string()))?;
        self.offset = self.backend.offset();

        Ok(offsets
            .into_iter()
            .zip(entries.iter())
            .map(|(off, (_, value, _))| (off, value.len()))
            .collect())
    }

    /// Write mixed put and delete records in a single batch **without** fsync.
    ///
    /// Preserves input order and combines all encoded records into one append.
    /// The caller is responsible for a later fsync/checkpoint.
    pub fn write_ops_batch_nosync(
        &mut self,
        entries: &[BatchWrite<'_>],
    ) -> Result<Vec<BatchWriteResult>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }

        for entry in entries {
            match entry {
                BatchWrite::Put { key, value, .. } => {
                    validate_kv_sizes(key, value).map_err(LogError)?;
                }
                BatchWrite::Delete { key } => {
                    validate_kv_sizes(key, &[]).map_err(LogError)?;
                }
            }
        }

        let encoded: Vec<Vec<u8>> = entries
            .iter()
            .map(|entry| match entry {
                BatchWrite::Put {
                    key,
                    value,
                    expire_at_ms,
                } => encode_record(key, value, *expire_at_ms),
                BatchWrite::Delete { key } => encode_tombstone(key),
            })
            .collect();

        let total_len: usize = encoded.iter().map(Vec::len).sum();
        let mut combined = Vec::with_capacity(total_len);
        let mut results = Vec::with_capacity(encoded.len());
        let mut running = self.backend.offset();

        for (entry, buf) in entries.iter().zip(&encoded) {
            match entry {
                BatchWrite::Put { value, .. } => {
                    results.push(BatchWriteResult::Put {
                        offset: running,
                        value_len: value.len(),
                    });
                }
                BatchWrite::Delete { key } => {
                    results.push(BatchWriteResult::Delete {
                        offset: running,
                        record_size: HEADER_SIZE + key.len(),
                    });
                }
            }

            combined.extend_from_slice(buf);
            running += buf.len() as u64;
        }

        self.backend
            .append(&combined)
            .map_err(|e| LogError(e.to_string()))?;

        self.backend
            .flush_no_sync()
            .map_err(|e| LogError(e.to_string()))?;
        self.offset = self.backend.offset();

        Ok(results)
    }

    /// Write pre-encoded record buffers and fsync. Returns the file offset
    /// at which each buffer was written.
    ///
    /// M-6 fix: allows callers to pre-encode records on one thread (e.g., the
    /// NIF thread where Binary refs are available) and then write the encoded
    /// bytes on another thread (e.g., Tokio) without re-encoding.
    ///
    /// # Errors
    ///
    /// Returns a `LogError` if any write or the final sync fails.
    pub fn write_batch_preencoded(&mut self, encoded: &[&[u8]]) -> Result<Vec<u64>> {
        let offsets = self
            .backend
            .append_batch_and_sync(encoded)
            .map_err(|e| LogError(e.to_string()))?;

        self.offset = self.backend.offset();
        Ok(offsets)
    }
}

/// Reads records from a log file at arbitrary offsets or sequentially.
pub struct LogReader {
    file: File,
}

impl LogReader {
    /// # Errors
    ///
    /// Returns a `LogError` if the file cannot be opened.
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
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

    /// Iterate records tolerating a truncated tail (crash-recovery mode).
    ///
    /// Like `iter_from_start`, but stops silently at the first CRC error or
    /// truncated record rather than returning `Err`. This matches the Bitcask
    /// paper's crash-recovery semantics: a partial record at the end of the
    /// log (written during a crash before fsync) is discarded; all complete
    /// records before it are valid.
    ///
    /// # Errors
    ///
    /// Returns a `LogError` only for I/O errors (seek/read failures), not for
    /// record-level corruption or truncation.
    pub fn iter_from_start_tolerant(&mut self) -> Result<Vec<Record>> {
        self.file.seek(SeekFrom::Start(0))?;
        let mut records = Vec::new();
        // while let stops at EOF (Ok(None)) or any error — crash-recovery semantics:
        // a truncated/corrupt tail record is silently discarded.
        while let Ok(Some(record)) = read_next_record(&mut self.file) {
            records.push(record);
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
        while let Ok(Some(record)) = read_next_record(&mut self.file) {
            records.push(record);
        }
        Ok(records)
    }
}

// ---------------------------------------------------------------------------
// Encoding helpers
// ---------------------------------------------------------------------------

/// Validates key and value sizes before encoding. Returns Ok(()) or an error
/// message if either exceeds the on-disk format limits (key: u16, value: u32).
pub(crate) fn validate_kv_sizes(key: &[u8], value: &[u8]) -> std::result::Result<(), String> {
    let max_key = usize::from(u16::MAX);
    // Spec section 2G.4: max_value_size_bytes defaults to 512 MiB.
    // The on-disk format supports u32 (4 GiB) but we enforce a tighter
    // default to prevent accidental large-value writes that would
    // degrade cache performance.
    const MAX_VALUE_SIZE: usize = 512 * 1024 * 1024; // 512 MiB
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

/// Encode a record into a single `Vec` allocation.
///
/// C-4 fix: pre-allocate a single `Vec` with a CRC placeholder, write the
/// body directly, then compute CRC over `buf[4..]` and patch the first 4
/// bytes. This eliminates the second `Vec` that was previously needed.
///
/// C-1 fix: uses `crc32fast` for hardware-accelerated CRC32 (SSE4.2 / ARM CRC).
pub(crate) fn encode_record(key: &[u8], value: &[u8], expire_at_ms: u64) -> Vec<u8> {
    let now_ms = now_ms();
    #[allow(clippy::cast_possible_truncation)]
    let key_size = key.len() as u16;
    #[allow(clippy::cast_possible_truncation)]
    let value_size = value.len() as u32;

    let total = HEADER_SIZE + key.len() + value.len();
    let mut buf = Vec::with_capacity(total);
    // Reserve CRC slot (will be patched below)
    buf.extend_from_slice(&[0u8; 4]);
    // Write body directly into the single buffer
    buf.extend_from_slice(&now_ms.to_le_bytes());
    buf.extend_from_slice(&expire_at_ms.to_le_bytes());
    buf.extend_from_slice(&key_size.to_le_bytes());
    buf.extend_from_slice(&value_size.to_le_bytes());
    buf.extend_from_slice(key);
    buf.extend_from_slice(value);
    // Compute CRC over body (everything after the 4-byte CRC field)
    let crc = crc32(&buf[4..]);
    buf[0..4].copy_from_slice(&crc.to_le_bytes());
    buf
}

fn encode_tombstone(key: &[u8]) -> Vec<u8> {
    // Tombstone: value_size = TOMBSTONE (u32::MAX), no value bytes.
    let now_ms = now_ms();
    #[allow(clippy::cast_possible_truncation)]
    let key_size = key.len() as u16;
    let total = HEADER_SIZE + key.len();
    let mut buf = Vec::with_capacity(total);
    buf.extend_from_slice(&[0u8; 4]); // CRC placeholder
    buf.extend_from_slice(&now_ms.to_le_bytes());
    buf.extend_from_slice(&0u64.to_le_bytes()); // expire_at_ms = 0
    buf.extend_from_slice(&key_size.to_le_bytes());
    buf.extend_from_slice(&TOMBSTONE.to_le_bytes()); // sentinel
    buf.extend_from_slice(key);
    let crc = crc32(&buf[4..]);
    buf[0..4].copy_from_slice(&crc.to_le_bytes());
    buf
}

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

#[cfg(unix)]
fn pread_record(file: &File, offset: u64) -> Result<Option<Record>> {
    // Step 1: pread the header
    let mut header = [0u8; HEADER_SIZE];
    match file.read_at(&mut header, offset) {
        Ok(0) => return Ok(None),                    // EOF
        Ok(n) if n < HEADER_SIZE => return Ok(None), // truncated header = EOF
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }

    let stored_crc = u32::from_le_bytes(header[0..4].try_into().unwrap());
    let timestamp_ms = u64::from_le_bytes(header[4..12].try_into().unwrap());
    let expire_at_ms = u64::from_le_bytes(header[12..20].try_into().unwrap());
    let key_size = u16::from_le_bytes(header[20..22].try_into().unwrap()) as usize;
    let value_size_raw = u32::from_le_bytes(header[22..26].try_into().unwrap());
    let is_tombstone = value_size_raw == TOMBSTONE;
    let value_size = if is_tombstone {
        0
    } else {
        value_size_raw as usize
    };

    // Step 2: pread key + value in a single call
    let actual_value_size = value_size;
    let body_len = key_size + actual_value_size;
    let mut body = vec![0u8; body_len];
    if body_len > 0 {
        let body_offset = offset + HEADER_SIZE as u64;
        let n = file.read_at(&mut body, body_offset)?;
        if n < body_len {
            return Err(LogError(format!(
                "pread short read: expected {body_len} B, got {n} B"
            )));
        }
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

    let mut key = vec![0u8; key_size];
    reader.read_exact(&mut key)?;

    let value = if is_tombstone {
        vec![]
    } else {
        let mut v = vec![0u8; value_size_raw as usize];
        reader.read_exact(&mut v)?;
        v
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

#[cfg(test)]
mod tests {
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
    fn truncated_header_returns_none() {
        // Only 10 bytes — less than HEADER_SIZE (26) — should return None (EOF)
        let partial = vec![0u8; 10];
        let mut cursor = io::Cursor::new(partial);
        let result = read_next_record(&mut cursor);
        // Truncated header looks like EOF → Ok(None)
        assert!(result.unwrap().is_none());
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
    fn tolerant_iter_stops_at_first_crc_error() {
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
        let records = reader.iter_from_start_tolerant().unwrap();
        // Only the first record is valid; tolerant iterator stops at 2nd CRC error.
        assert_eq!(
            records.len(),
            1,
            "tolerant iter must stop at first corrupt record"
        );
        assert_eq!(records[0].key, b"k1");
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
}
