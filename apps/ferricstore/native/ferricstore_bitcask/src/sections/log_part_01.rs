
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
// Spec section 2G.4: max_value_size_bytes defaults to 512 MiB.
// The on-disk format supports u32 (4 GiB) but FerricStore enforces a tighter
// default to prevent accidental large-value writes that degrade cache behavior.
const MAX_VALUE_SIZE: usize = 512 * 1024 * 1024;
const BODY_LEN_FILE_SIZE_CHECK_THRESHOLD: usize = 64 * 1024;
const STREAM_READ_CHUNK_SIZE: usize = 64 * 1024;

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

/// Metadata for a record whose value bytes were validated but not materialized.
#[derive(Debug, PartialEq, Eq)]
pub struct RecordMetadata {
    pub timestamp_ms: u64,
    pub expire_at_ms: u64,
    pub key: Vec<u8>,
    pub value_size: u32,
    pub is_tombstone: bool,
    pub offset: u64,
    pub record_size: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RawCopyResult {
    pub offset: u64,
    pub record_size: u64,
    pub is_tombstone: bool,
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
    /// encoded on a Tokio blocking worker thread, then the raw bytes are
    /// written by the same worker using this method.
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

        // Encode directly into one combined buffer. This keeps the H-2 single
        // append syscall, but avoids one Vec allocation and one memcpy per
        // record on hot batched paths (Flow create_many/transition_many).
        let total_len = entries.iter().try_fold(0usize, |acc, (key, value, _)| {
            acc.checked_add(record_len(key.len(), value.len()))
                .ok_or_else(|| LogError("batch record length overflow".into()))
        })?;
        let mut combined = Vec::with_capacity(total_len);
        let mut offsets = Vec::with_capacity(entries.len());
        let mut running = self.backend.offset();

        for (key, value, expire_at_ms) in entries {
            offsets.push(running);
            encode_record_into(&mut combined, key, value, *expire_at_ms);
            running += record_len(key.len(), value.len()) as u64;
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

        let total_len = entries.iter().try_fold(0usize, |acc, entry| {
            let len = match entry {
                BatchWrite::Put { key, value, .. } => record_len(key.len(), value.len()),
                BatchWrite::Delete { key } => tombstone_len(key.len()),
            };

            acc.checked_add(len)
                .ok_or_else(|| LogError("batch record length overflow".into()))
        })?;
        let mut combined = Vec::with_capacity(total_len);
        let mut results = Vec::with_capacity(entries.len());
        let mut running = self.backend.offset();

        for entry in entries {
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

            match entry {
                BatchWrite::Put {
                    key,
                    value,
                    expire_at_ms,
                } => {
                    encode_record_into(&mut combined, key, value, *expire_at_ms);
                    running += record_len(key.len(), value.len()) as u64;
                }
                BatchWrite::Delete { key } => {
                    encode_tombstone_into(&mut combined, key);
                    running += tombstone_len(key.len()) as u64;
                }
            }
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

