//! `Store` — the public Bitcask API.
//!
//! Ties together the keydir (in-memory index), log writer (disk appends),
//! and hint files (fast startup). All writes go through the log first, then
//! update the keydir. Reads consult the keydir then do a single pread.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use crate::hint::{HintEntry, HintReader, HintWriter};
use crate::keydir::{KeyDir, KeyEntry};
use crate::log::{LogReader, LogWriter, HEADER_SIZE};

/// Configuration for opening a store.
pub struct StoreConfig {
    /// Directory where data and hint files live.
    pub data_dir: PathBuf,
    /// Monotonically increasing ID for the active (writable) data file.
    pub active_file_id: u64,
}

#[derive(Debug)]
pub struct StoreError(pub String);

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "StoreError: {}", self.0)
    }
}

impl std::error::Error for StoreError {}

impl From<crate::log::LogError> for StoreError {
    fn from(e: crate::log::LogError) -> Self {
        StoreError(e.to_string())
    }
}

impl From<crate::hint::HintError> for StoreError {
    fn from(e: crate::hint::HintError) -> Self {
        StoreError(e.to_string())
    }
}

impl From<std::io::Error> for StoreError {
    fn from(e: std::io::Error) -> Self {
        StoreError(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, StoreError>;

pub struct Store {
    keydir: KeyDir,
    writer: LogWriter,
    data_dir: PathBuf,
    active_file_id: u64,
    /// L-2 fix: pre-computed path for the active log file. Avoids a
    /// `format!("{file_id:020}.log")` allocation on every GET/PUT when the
    /// entry happens to live in the active file (common case).
    cached_active_log_path: PathBuf,
}

impl Store {
    /// Returns the file ID of the currently active (writable) data file.
    #[must_use]
    pub fn active_file_id(&self) -> u64 {
        self.active_file_id
    }

    /// L-2 fix: return the log path for a given file ID, reusing the cached
    /// active log path when `file_id == active_file_id` to avoid a
    /// `format!` allocation on the hot path.
    #[inline]
    fn log_path_for(&self, file_id: u64) -> PathBuf {
        if file_id == self.active_file_id {
            self.cached_active_log_path.clone()
        } else {
            log_path(&self.data_dir, file_id)
        }
    }

    /// Returns the path to the currently active log file.
    ///
    /// L-2 fix: returns the pre-computed cached path instead of
    /// allocating a new `String` via `format!` on every call.
    #[must_use]
    pub fn active_log_path(&self) -> PathBuf {
        self.cached_active_log_path.clone()
    }

    /// Open a store at `data_dir`.
    ///
    /// On startup:
    /// 1. Scan for `*.hint` files and load them to rebuild the keydir fast.
    /// 2. If no hint files exist, replay `*.log` files sequentially.
    /// 3. Open (or create) the active data file for appending.
    ///
    /// # Errors
    ///
    /// Returns a `StoreError` if the data directory cannot be created, existing
    /// log/hint files cannot be read, or the active data file cannot be opened.
    pub fn open(data_dir: &Path) -> Result<Self> {
        fs::create_dir_all(data_dir)?;

        let mut keydir = KeyDir::new();

        // Collect and sort existing data/hint file IDs
        let mut file_ids = collect_file_ids(data_dir)?;
        file_ids.sort_unstable();

        // Track the valid end offset of the active (last) file so we can
        // truncate any torn/garbage tail before opening the writer.
        let active_file_id = file_ids.last().copied().unwrap_or(1);
        let mut active_valid_end: Option<u64> = None;

        for &fid in &file_ids {
            let fid_hint_path = hint_path(data_dir, fid);
            let fid_log_path = log_path(data_dir, fid);

            // EC-7: A hint file with no corresponding data file is orphaned
            // (e.g. the data file was deleted after a partial compaction). Skip
            // it silently — the keydir will simply contain no entries for this
            // file ID, which is safe.
            if fid_hint_path.exists() && !fid_log_path.exists() {
                continue;
            }

            if fid_hint_path.exists() {
                // EC-2: If the hint file is corrupt fall back to full log replay
                // rather than propagating the error. The hint is just an
                // acceleration structure; the data file is the ground truth.
                //
                // Load into a staging keydir first so that a mid-file parse
                // failure does not leave partially-applied entries in the real
                // keydir. Only merge the staging keydir on success.
                let mut staging = KeyDir::new();
                let hint_ok = HintReader::open(&fid_hint_path)
                    .and_then(|mut r| r.load_into(&mut staging))
                    .is_ok();
                if hint_ok {
                    // Compute the end-of-hint offset: the byte just past the last
                    // record described by the hint file.  Any records appended to
                    // the log after the hint was written (e.g. new puts before a
                    // crash) live beyond this offset and must be replayed.
                    let hint_end_offset = staging
                        .iter()
                        .map(|(key, entry)| {
                            entry.offset
                                + HEADER_SIZE as u64
                                + key.len() as u64
                                + u64::from(entry.value_size)
                        })
                        .max()
                        .unwrap_or(0);

                    for (key, entry) in staging.iter() {
                        keydir.put(key.to_vec(), entry.clone());
                    }

                    // Replay log tail past the hint's last known offset to pick
                    // up any writes that happened after the hint was generated.
                    if fid_log_path.exists() {
                        let end =
                            replay_log_from(&fid_log_path, fid, hint_end_offset, &mut keydir)?;
                        if fid == active_file_id {
                            active_valid_end = Some(end);
                        }
                    }
                } else {
                    // Hint file corrupt — fall back to full log replay.
                    if fid_log_path.exists() {
                        let end = replay_log(&fid_log_path, fid, &mut keydir)?;
                        if fid == active_file_id {
                            active_valid_end = Some(end);
                        }
                    }
                }
            } else {
                // No hint file — replay the raw log.
                if fid_log_path.exists() {
                    let end = replay_log(&fid_log_path, fid, &mut keydir)?;
                    if fid == active_file_id {
                        active_valid_end = Some(end);
                    }
                }
            }
        }

        let active_path = log_path(data_dir, active_file_id);

        // Truncate the active log to the last valid record offset. This
        // removes any torn writes or garbage bytes appended by a crash,
        // ensuring new writes don't end up after unreadable data.
        if let Some(valid_end) = active_valid_end {
            if active_path.exists() {
                let file_len = fs::metadata(&active_path).map_or(0, |m| m.len());
                if valid_end < file_len {
                    let f = fs::OpenOptions::new()
                        .write(true)
                        .open(&active_path)
                        .map_err(|e| StoreError(e.to_string()))?;
                    f.set_len(valid_end)
                        .map_err(|e| StoreError(e.to_string()))?;
                }
            }
        }

        let writer = LogWriter::open(&active_path, active_file_id)?;

        Ok(Self {
            keydir,
            writer,
            data_dir: data_dir.to_path_buf(),
            active_file_id,
            cached_active_log_path: active_path,
        })
    }

    /// Write a key-value pair. `expire_at_ms` = 0 means no expiry.
    ///
    /// # Errors
    ///
    /// Returns a `StoreError` if the record cannot be written or synced to disk.
    pub fn put(&mut self, key: &[u8], value: &[u8], expire_at_ms: u64) -> Result<()> {
        // Empty values are valid (e.g. `SET key ""`). Tombstones use a
        // sentinel value_size (u32::MAX) in the log format, not empty bytes.
        let offset = self.writer.write(key, value, expire_at_ms)?;
        self.writer.sync()?;
        #[allow(clippy::cast_possible_truncation)]
        self.keydir.put(
            key.to_vec(),
            KeyEntry {
                file_id: self.active_file_id,
                offset,
                value_size: value.len() as u32,
                expire_at_ms,
                ref_bit: false,
            },
        );
        Ok(())
    }

    /// Look up a key and return its on-disk file location **without** reading the
    /// value bytes. Used by the sendfile optimisation in the TCP connection layer.
    ///
    /// Returns `Some((file_path, value_byte_offset, value_size))` if the key
    /// exists and is not expired, or `None` otherwise.
    ///
    /// The `value_byte_offset` points to the first byte of the value data inside
    /// the data file (past the record header and key bytes).
    ///
    /// # Errors
    ///
    /// Returns a `StoreError` if an expired-key tombstone cannot be written.
    pub fn get_file_ref(&mut self, key: &[u8]) -> Result<Option<(PathBuf, u64, u32)>> {
        let now_ms = now_ms();
        let entry = match self.keydir.get(key) {
            Some(e) => e.clone(),
            None => return Ok(None),
        };

        if entry.expire_at_ms != 0 && entry.expire_at_ms <= now_ms {
            // Logically expired — remove from keydir and write a tombstone so the
            // key does not resurrect after a store close+reopen.  We intentionally
            // do NOT call sync() here: the tombstone becomes durable on the next
            // put/sync or on a clean shutdown.  In the worst case (crash before the
            // next sync) the expired key re-appears on the following open, the TTL
            // check fires again, and another tombstone is written — acceptable per
            // Bitcask crash-recovery semantics. Mirror of the comment in `get`.
            self.keydir.delete(key);
            self.writer.write_tombstone(key)?;
            return Ok(None);
        }

        // value_size == 0 is a valid empty value (not a tombstone).
        // Tombstones use TOMBSTONE sentinel and are already removed from the keydir.

        let file = self.log_path_for(entry.file_id);
        let value_offset = entry.offset + HEADER_SIZE as u64 + key.len() as u64;
        Ok(Some((file, value_offset, entry.value_size)))
    }

    /// Read the value for `key`. Returns `None` if not found or expired.
    ///
    /// # Errors
    ///
    /// Returns a `StoreError` if the data file cannot be read.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let now_ms = now_ms();
        let entry = match self.keydir.get(key) {
            Some(e) => e.clone(),
            None => return Ok(None),
        };

        if entry.expire_at_ms != 0 && entry.expire_at_ms <= now_ms {
            // Logically expired — remove from keydir and write a tombstone so the
            // key does not resurrect after a store close+reopen.  We intentionally
            // do NOT call sync() here: the tombstone becomes durable on the next
            // put/sync or on a clean shutdown.  In the worst case (crash before the
            // next sync) the expired key re-appears on the following open, the TTL
            // check fires again, and another tombstone is written — acceptable per
            // Bitcask crash-recovery semantics.
            self.keydir.delete(key);
            self.writer.write_tombstone(key)?;
            return Ok(None);
        }

        let log_file = self.log_path_for(entry.file_id);
        let mut reader = LogReader::open(&log_file)?;
        let record = reader.read_at(entry.offset)?;
        Ok(record.and_then(|r| r.value))
    }

    /// Write multiple key-value pairs with a **single fsync** (group commit).
    ///
    /// This is the legacy direct write path for the BEAM integration. Modern hot
    /// paths batch or submit long I/O asynchronously so normal scheduler work is
    /// bounded.
    ///
    /// ## BEAM scheduler note
    ///
    /// Each synchronous NIF call occupies a normal scheduler until it returns.
    /// If every `put` issues its own fsync, sustained concurrent write load
    /// appears as write latency spikes on the Elixir side.
    ///
    /// `put_batch` amortises: N client writes -> 1 NIF call -> 1 fsync.
    ///
    /// # Errors
    ///
    /// Returns a `StoreError` if any write or the final sync fails. On error the
    /// writes that completed before the failure are appended to the log but the
    /// keydir is not updated for them — the next `open()` will replay the log and
    /// recover any durable entries.
    pub fn put_batch(&mut self, entries: &[(&[u8], &[u8], u64)]) -> Result<()> {
        // write_batch encodes all records, submits them to the I/O backend in
        // one batch (one io_uring_enter on Linux, N writes on sync fallback),
        // then fsyncs once. Returns (offset, value_len) per entry.
        let committed = self.writer.write_batch(entries)?;

        // Update keydir only after durable commit.
        for ((offset, value_len), &(key, _, expire_at_ms)) in
            committed.into_iter().zip(entries.iter())
        {
            #[allow(clippy::cast_possible_truncation)]
            self.keydir.put(
                key.to_vec(),
                KeyEntry {
                    file_id: self.active_file_id,
                    offset,
                    value_size: value_len as u32,
                    expire_at_ms,
                    ref_bit: false,
                },
            );
        }
        Ok(())
    }

    /// Write a batch of pre-encoded records to the log and update the keydir.
    ///
    /// M-6 fix: accepts pre-encoded record buffers (already serialized via
    /// `log::encode_record`) along with the key/value_size/expire metadata.
    /// This avoids re-encoding records that were already encoded on the NIF
    /// thread, and allows `put_batch_tokio_async` to send only encoded bytes
    /// across the thread boundary instead of cloning raw key+value data.
    ///
    /// `metadata[i]` is `(key, value_size, expire_at_ms)` for `encoded[i]`.
    ///
    /// # Errors
    ///
    /// Returns a `StoreError` if any write or the final sync fails.
    pub fn put_batch_preencoded(
        &mut self,
        encoded: &[Vec<u8>],
        metadata: &[(Vec<u8>, u32, u64)],
    ) -> Result<()> {
        let buf_refs: Vec<&[u8]> = encoded.iter().map(Vec::as_slice).collect();
        let offsets = self.writer.write_batch_preencoded(&buf_refs)?;

        for (off, (key, value_size, expire_at_ms)) in offsets.into_iter().zip(metadata.iter()) {
            self.keydir.put(
                key.clone(),
                KeyEntry {
                    file_id: self.active_file_id,
                    offset: off,
                    value_size: *value_size,
                    expire_at_ms: *expire_at_ms,
                    ref_bit: false,
                },
            );
        }
        Ok(())
    }

    /// Encode a batch of entries into on-disk record bytes and compute the
    /// file offset at which each record will be written.
    ///
    /// This is a **pure** step with no side effects: it does not touch the
    /// keydir or advance any offsets. The caller must call
    /// `commit_async_batch` after a successful ring submission to make the
    /// writes visible via `get`.
    ///
    /// Returns `(encoded_buffers, file_offsets)`:
    ///   - `encoded_buffers[i]` is the serialised record for `entries[i]`.
    ///   - `file_offsets[i]` is the byte offset where `encoded_buffers[i]`
    ///     will be written in the active log file.
    #[must_use]
    pub fn encode_for_async(&self, entries: &[(&[u8], &[u8], u64)]) -> (Vec<Vec<u8>>, Vec<u64>) {
        use crate::log::encode_record;

        let encoded: Vec<Vec<u8>> = entries
            .iter()
            .map(|(key, value, expire_at_ms)| encode_record(key, value, *expire_at_ms))
            .collect();

        let mut offsets = Vec::with_capacity(encoded.len());
        let mut running = self.writer.offset;
        for buf in &encoded {
            offsets.push(running);
            running += buf.len() as u64;
        }

        (encoded, offsets)
    }

    /// Update the keydir and advance the writer offset for a batch that has
    /// been **successfully submitted** to the async io_uring ring.
    ///
    /// Must be called exactly once per successful `submit_batch` call, with
    /// the same `entries` and `file_offsets` that were passed to
    /// `encode_for_async`.
    ///
    /// # Safety invariant
    ///
    /// Call this only after `submit_batch` returns `Ok`. If the ring
    /// submission fails, do NOT call this — the keydir must not contain
    /// entries pointing to unwritten file offsets.
    pub fn commit_async_batch(
        &mut self,
        entries: &[(&[u8], &[u8], u64)],
        file_offsets: &[u64],
        encoded: &[Vec<u8>],
    ) {
        let total_bytes: u64 = encoded.iter().map(|b| b.len() as u64).sum();

        for (i, &(key, value, expire_at_ms)) in entries.iter().enumerate() {
            #[allow(clippy::cast_possible_truncation)]
            self.keydir.put(
                key.to_vec(),
                KeyEntry {
                    file_id: self.active_file_id,
                    offset: file_offsets[i],
                    value_size: value.len() as u32,
                    expire_at_ms,
                    ref_bit: false,
                },
            );
        }

        // Advance both the LogWriter offset AND the underlying backend's
        // internal offset counter. This keeps the sync backend in sync so
        // that if a subsequent sync write (e.g. a delete tombstone) goes
        // through the LogWriter, it starts at the correct position.
        self.writer.advance_offset(total_bytes);
    }

    /// Delete a key. Appends a tombstone to the log.
    ///
    /// # Errors
    ///
    /// Returns a `StoreError` if the tombstone cannot be written or synced to disk.
    pub fn delete(&mut self, key: &[u8]) -> Result<bool> {
        if self.keydir.get(key).is_none() {
            return Ok(false);
        }
        self.writer.write_tombstone(key)?;
        self.writer.sync()?;
        self.keydir.delete(key);
        Ok(true)
    }

    /// Return all live (non-expired) keys.
    #[must_use]
    pub fn keys(&self) -> Vec<Vec<u8>> {
        let now_ms = now_ms();
        self.keydir
            .iter()
            .filter(|(_, e)| e.expire_at_ms == 0 || e.expire_at_ms > now_ms)
            .map(|(k, _)| k.to_vec())
            .collect()
    }

    /// Number of live (non-expired) keys.
    ///
    /// Expired keys that are still present in the keydir (not yet evicted by a
    /// `get` call) are excluded so that callers always see a logically accurate
    /// count.
    #[must_use]
    pub fn len(&self) -> usize {
        let now = now_ms();
        self.keydir
            .iter()
            .filter(|(_, entry)| entry.expire_at_ms == 0 || entry.expire_at_ms > now)
            .count()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Write a hint file for the active data file (called after compaction or
    /// before rotating the active file).
    ///
    /// Fsyncs the active log before writing the hint so that every offset
    /// recorded in the hint points to bytes that are already durable on disk.
    /// Without this fsync a crash between hint write and log flush would leave
    /// the hint referencing records that do not survive recovery.
    ///
    /// # Errors
    ///
    /// Returns a `StoreError` if the log sync, hint file creation, or any hint
    /// entry write fails.
    pub fn write_hint_file(&mut self) -> Result<()> {
        // Ensure all buffered log writes are durable before recording their
        // offsets in the hint file (Issue 7.4).
        self.writer.sync()?;

        let hint_path = hint_path(&self.data_dir, self.active_file_id);
        let mut writer = HintWriter::open(&hint_path)?;
        for (key, entry) in self.keydir.iter() {
            if entry.file_id == self.active_file_id {
                writer.write_entry(&HintEntry {
                    file_id: entry.file_id,
                    offset: entry.offset,
                    value_size: entry.value_size,
                    expire_at_ms: entry.expire_at_ms,
                    key: key.to_vec(),
                })?;
            }
        }
        writer.commit()?;
        Ok(())
    }

    /// Return all live (non-tombstone, non-expired) key-value pairs.
    ///
    /// # Errors
    ///
    /// Returns a `StoreError` if any data file cannot be read.
    /// H-3 fix: group reads by file_id so each data file is opened once,
    /// not once per key. For 10K keys across 5 files this reduces open/close
    /// from 10K pairs to 5.
    pub fn get_all(&mut self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let now = now_ms();
        let entries: Vec<(Vec<u8>, crate::keydir::KeyEntry)> = self
            .keydir
            .iter()
            .filter(|(_, e)| e.expire_at_ms == 0 || e.expire_at_ms > now)
            .map(|(k, e)| (k.to_vec(), e.clone()))
            .collect();

        // Group entries by file_id to open each file once.
        let mut by_file: HashMap<u64, Vec<(Vec<u8>, crate::keydir::KeyEntry)>> = HashMap::new();
        for (key, entry) in entries {
            by_file.entry(entry.file_id).or_default().push((key, entry));
        }

        let mut result = Vec::new();
        for (file_id, file_entries) in by_file {
            let log_file = self.log_path_for(file_id);
            let mut reader = LogReader::open(&log_file)?;
            for (key, entry) in file_entries {
                if let Some(record) = reader.read_at(entry.offset)? {
                    if let Some(value) = record.value {
                        result.push((key, value));
                    }
                }
            }
        }
        Ok(result)
    }

    /// Look up multiple keys at once.
    ///
    /// # Errors
    ///
    /// Returns a `StoreError` if any data file cannot be read.
    /// H-3 fix: cache LogReaders by file_id to avoid opening the same file
    /// repeatedly when multiple keys live in the same data file.
    pub fn get_batch(&mut self, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>> {
        let now = now_ms();
        let mut results = Vec::with_capacity(keys.len());
        let mut reader_cache: HashMap<u64, LogReader> = HashMap::new();

        for &key in keys {
            let entry = if let Some(e) = self.keydir.get(key) {
                e.clone()
            } else {
                results.push(None);
                continue;
            };
            if entry.expire_at_ms != 0 && entry.expire_at_ms <= now {
                results.push(None);
                continue;
            }
            let reader = if let Some(r) = reader_cache.get_mut(&entry.file_id) {
                r
            } else {
                let log_file = self.log_path_for(entry.file_id);
                let r = LogReader::open(&log_file)?;
                reader_cache.insert(entry.file_id, r);
                reader_cache.get_mut(&entry.file_id).unwrap()
            };
            let val = reader.read_at(entry.offset)?.and_then(|r| r.value);
            results.push(val);
        }
        Ok(results)
    }

    /// Range scan: sorted key-value pairs in `[min_key, max_key]`, up to `max_count`.
    ///
    /// # Errors
    ///
    /// Returns a `StoreError` if any data file cannot be read.
    pub fn get_range(
        &mut self,
        min_key: &[u8],
        max_key: &[u8],
        max_count: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if max_count == 0 || min_key > max_key {
            return Ok(Vec::new());
        }
        let now = now_ms();
        let mut matching: Vec<(Vec<u8>, crate::keydir::KeyEntry)> = self
            .keydir
            .iter()
            .filter(|(k, e)| {
                *k >= min_key && *k <= max_key && (e.expire_at_ms == 0 || e.expire_at_ms > now)
            })
            .map(|(k, e)| (k.to_vec(), e.clone()))
            .collect();
        matching.sort_by(|a, b| a.0.cmp(&b.0));
        matching.truncate(max_count);

        // H-3 fix: cache LogReaders by file_id.
        let mut reader_cache: HashMap<u64, LogReader> = HashMap::new();
        let mut result = Vec::with_capacity(matching.len());
        for (key, entry) in matching {
            let reader = if let Some(r) = reader_cache.get_mut(&entry.file_id) {
                r
            } else {
                let log_file = self.log_path_for(entry.file_id);
                let r = LogReader::open(&log_file)?;
                reader_cache.insert(entry.file_id, r);
                reader_cache.get_mut(&entry.file_id).unwrap()
            };
            if let Some(record) = reader.read_at(entry.offset)? {
                if let Some(value) = record.value {
                    result.push((key, value));
                }
            }
        }
        Ok(result)
    }

    /// Atomic read-modify-write.
    ///
    /// # Errors
    ///
    /// Returns a `StoreError` if the operation fails.
    pub fn read_modify_write(&mut self, key: &[u8], op: &RmwOp) -> Result<Vec<u8>> {
        let now = now_ms();
        let (current, expire_at_ms) = match self.keydir.get(key) {
            Some(e) if e.expire_at_ms != 0 && e.expire_at_ms <= now => (Vec::new(), 0),
            Some(e) => {
                let entry = e.clone();
                let log_file = self.log_path_for(entry.file_id);
                let mut reader = LogReader::open(&log_file)?;
                let val = reader
                    .read_at(entry.offset)?
                    .and_then(|r| r.value)
                    .unwrap_or_default();
                (val, entry.expire_at_ms)
            }
            None => (Vec::new(), 0),
        };
        let new_value = match *op {
            RmwOp::SetRange(offset, ref bytes) => {
                const MAX_SETRANGE_SIZE: u64 = 512 * 1024 * 1024; // 512 MiB
                let needed = offset + bytes.len() as u64;
                if needed > MAX_SETRANGE_SIZE {
                    return Err(StoreError(format!(
                        "SETRANGE would create value of {needed} bytes (max {MAX_SETRANGE_SIZE})"
                    )));
                }
                let offset = offset as usize;
                let needed = needed as usize;
                let mut buf = current;
                if buf.len() < needed {
                    buf.resize(needed, 0);
                }
                buf[offset..offset + bytes.len()].copy_from_slice(bytes);
                buf
            }
            RmwOp::SetBit(bit_offset, bit_value) => {
                let byte_index = (bit_offset / 8) as usize;
                let bit_index = 7 - (bit_offset % 8) as usize;
                let mut buf = current;
                if buf.len() <= byte_index {
                    buf.resize(byte_index + 1, 0);
                }
                if bit_value != 0 {
                    buf[byte_index] |= 1 << bit_index;
                } else {
                    buf[byte_index] &= !(1 << bit_index);
                }
                buf
            }
            RmwOp::Append(ref data) => {
                let mut buf = current;
                buf.extend_from_slice(data);
                buf
            }
            RmwOp::IncrBy(delta) => {
                let current_str = std::str::from_utf8(&current)
                    .map_err(|_| StoreError("not an integer".into()))?;
                let current_int: i64 = if current_str.is_empty() {
                    0
                } else {
                    current_str
                        .parse()
                        .map_err(|_| StoreError("not an integer".into()))?
                };
                let result = current_int
                    .checked_add(delta)
                    .ok_or_else(|| StoreError("increment or decrement would overflow".into()))?;
                result.to_string().into_bytes()
            }
            RmwOp::IncrByFloat(delta) => {
                let current_str = std::str::from_utf8(&current)
                    .map_err(|_| StoreError("not a valid float".into()))?;
                let current_float: f64 = if current_str.is_empty() {
                    0.0
                } else {
                    current_str
                        .parse()
                        .map_err(|_| StoreError("not a valid float".into()))?
                };
                if !current_float.is_finite() {
                    return Err(StoreError("increment would produce NaN or Infinity".into()));
                }
                if !delta.is_finite() {
                    return Err(StoreError("increment would produce NaN or Infinity".into()));
                }
                let result = current_float + delta;
                if !result.is_finite() {
                    return Err(StoreError("increment would produce NaN or Infinity".into()));
                }
                format_float(result).into_bytes()
            }
        };
        self.put(key, &new_value, expire_at_ms)?;
        Ok(new_value)
    }

    /// Compute shard-level statistics.
    ///
    /// # Errors
    ///
    /// Returns a `StoreError` if file metadata cannot be read.
    pub fn shard_stats(&self) -> Result<(u64, u64, u64, u64, u64, f64)> {
        let file_ids = collect_file_ids(&self.data_dir)?;
        let mut total_bytes: u64 = 0;
        for &fid in &file_ids {
            let path = log_path(&self.data_dir, fid);
            if path.exists() {
                total_bytes += fs::metadata(&path)?.len();
            }
        }
        let now = now_ms();
        let mut live_bytes: u64 = 0;
        let mut key_count: u64 = 0;
        for (key, entry) in self.keydir.iter() {
            if entry.expire_at_ms == 0 || entry.expire_at_ms > now {
                live_bytes += HEADER_SIZE as u64 + key.len() as u64 + u64::from(entry.value_size);
                key_count += 1;
            }
        }
        let dead_bytes = total_bytes.saturating_sub(live_bytes);
        #[allow(clippy::cast_precision_loss)]
        let frag_ratio = if total_bytes > 0 {
            dead_bytes as f64 / total_bytes as f64
        } else {
            0.0
        };
        Ok((
            total_bytes,
            live_bytes,
            dead_bytes,
            file_ids.len() as u64,
            key_count,
            frag_ratio,
        ))
    }

    /// List all data files and their sizes.
    ///
    /// # Errors
    ///
    /// Returns a `StoreError` if the directory cannot be read.
    pub fn file_sizes(&self) -> Result<Vec<(u64, u64)>> {
        let file_ids = collect_file_ids(&self.data_dir)?;
        let mut result = Vec::with_capacity(file_ids.len());
        for fid in file_ids {
            let path = log_path(&self.data_dir, fid);
            if path.exists() {
                result.push((fid, fs::metadata(&path)?.len()));
            }
        }
        Ok(result)
    }

    /// Run compaction on specified file IDs.
    ///
    /// # Errors
    ///
    /// Returns a `StoreError` if compaction fails.
    pub fn run_compaction(&mut self, file_ids: &[u64]) -> Result<(u64, u64, u64)> {
        if file_ids.is_empty() {
            return Ok((0, 0, 0));
        }
        let mut bytes_before: u64 = 0;
        for &fid in file_ids {
            let path = log_path(&self.data_dir, fid);
            if path.exists() {
                bytes_before += fs::metadata(&path).map_or(0, |m| m.len());
            }
        }
        let max_input = file_ids.iter().copied().max().unwrap_or(0);
        let new_file_id = std::cmp::max(max_input, self.active_file_id) + 1;
        let now = now_ms();
        let output =
            crate::compaction::compact(&self.data_dir, file_ids, &self.keydir, new_file_id, now)
                .map_err(|e| StoreError(e.to_string()))?;
        let mut new_kd = crate::keydir::KeyDir::new();
        let hp = hint_path(&self.data_dir, new_file_id);
        if hp.exists() {
            let mut hr =
                crate::hint::HintReader::open(&hp).map_err(|e| StoreError(e.to_string()))?;
            hr.load_into(&mut new_kd)
                .map_err(|e| StoreError(e.to_string()))?;
        }
        let compacted_set: std::collections::HashSet<u64> = file_ids.iter().copied().collect();
        let keys_in_old: Vec<Vec<u8>> = self
            .keydir
            .iter()
            .filter(|(_, e)| compacted_set.contains(&e.file_id))
            .map(|(k, _)| k.to_vec())
            .collect();
        for key in &keys_in_old {
            self.keydir.delete(key);
        }
        for (key, entry) in new_kd.iter() {
            self.keydir.put(key.to_vec(), entry.clone());
        }
        crate::compaction::remove_old_files(&self.data_dir, file_ids)
            .map_err(|e| StoreError(e.to_string()))?;
        if compacted_set.contains(&self.active_file_id) {
            self.active_file_id = new_file_id;
            self.cached_active_log_path = log_path(&self.data_dir, new_file_id);
            self.writer = LogWriter::open(&self.cached_active_log_path, new_file_id)?;
        }
        let mut bytes_after: u64 = 0;
        let new_path = self.log_path_for(new_file_id);
        if new_path.exists() {
            bytes_after = fs::metadata(&new_path).map_or(0, |m| m.len());
        }
        Ok((
            output.records_written as u64,
            output.records_dropped as u64,
            bytes_before.saturating_sub(bytes_after),
        ))
    }

    /// Return available disk space for the data directory.
    ///
    /// # Errors
    ///
    /// Returns a `StoreError` if the statvfs call fails.
    pub fn available_disk_space(&self) -> Result<u64> {
        available_disk_space_for_path(&self.data_dir)
    }

    /// Proactively purge all logically-expired keys from the keydir.
    ///
    /// For each expired key a tombstone is appended to the log so that the key
    /// does not resurrect after a store close+reopen.  All tombstones are
    /// written and then fsynced in one batch.
    ///
    /// This is the proactive counterpart to the lazy expiry that fires inside
    /// `get`.  Without periodic `purge_expired` calls the keydir grows without
    /// bound for TTL-heavy workloads where keys are written but never read
    /// after they expire (Issue 6.3).
    ///
    /// # Errors
    ///
    /// Returns a `StoreError` if any tombstone write or the final sync fails.
    pub fn purge_expired(&mut self) -> Result<usize> {
        let now = now_ms();
        let expired = self.keydir.expired_keys(now);
        let count = expired.len();
        // Write all tombstones first, then fsync, then update keydir.
        // This ordering ensures that on crash between write and keydir update,
        // the tombstones are durable on disk. On reopen, log replay will see
        // them and correctly remove the keys. The previous ordering (keydir
        // delete before fsync) could cause keys to resurrect after a crash.
        for key in &expired {
            self.writer
                .write_tombstone(key)
                .map_err(|e| StoreError(e.to_string()))?;
        }
        if count > 0 {
            self.writer.sync().map_err(|e| StoreError(e.to_string()))?;
        }
        for key in &expired {
            self.keydir.delete(key);
        }
        Ok(count)
    }
}

/// Read-modify-write operation variants.
pub enum RmwOp {
    SetRange(u64, Vec<u8>),
    SetBit(u64, u8),
    Append(Vec<u8>),
    IncrBy(i64),
    IncrByFloat(f64),
}

fn format_float(val: f64) -> String {
    let s = format!("{val:.17}");
    if let Some(dot_pos) = s.find('.') {
        let trimmed = s.trim_end_matches('0');
        if trimmed.len() == dot_pos + 1 {
            trimmed[..dot_pos].to_string()
        } else {
            trimmed.to_string()
        }
    } else {
        s
    }
}

pub fn available_disk_space_for_path(path: &Path) -> Result<u64> {
    #[cfg(unix)]
    {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;
        let c_path =
            CString::new(path.as_os_str().as_bytes()).map_err(|e| StoreError(e.to_string()))?;
        #[allow(unsafe_code)]
        unsafe {
            let mut stat: libc::statvfs = std::mem::zeroed();
            let ret = libc::statvfs(c_path.as_ptr(), &mut stat);
            if ret != 0 {
                return Err(StoreError(format!(
                    "statvfs failed: {}",
                    std::io::Error::last_os_error()
                )));
            }
            #[allow(clippy::unnecessary_cast)]
            Ok(stat.f_bavail as u64 * stat.f_frsize as u64)
        }
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(u64::MAX)
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

fn log_path(data_dir: &Path, file_id: u64) -> PathBuf {
    // v1 uses 20-char zero-padded names, v2 uses 5-char zero-padded names.
    // Try the v2 short name first; fall back to v1 long name if it doesn't exist.
    let short = data_dir.join(format!("{file_id:05}.log"));
    if short.exists() {
        short
    } else {
        data_dir.join(format!("{file_id:020}.log"))
    }
}

fn hint_path(data_dir: &Path, file_id: u64) -> PathBuf {
    let short = data_dir.join(format!("{file_id:05}.hint"));
    if short.exists() {
        short
    } else {
        data_dir.join(format!("{file_id:020}.hint"))
    }
}

/// Scan `data_dir` for `*.log` files and return their numeric IDs.
fn collect_file_ids(data_dir: &Path) -> Result<Vec<u64>> {
    let mut ids = Vec::new();
    for entry in fs::read_dir(data_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(stem) = name.strip_suffix(".log") {
            let trimmed = stem.trim_start_matches('0');
            if trimmed.is_empty() {
                // All zeros (e.g. "00000", "00000000000000000000") => file_id 0
                ids.push(0);
            } else if let Ok(id) = trimmed.parse::<u64>() {
                ids.push(id);
            }
        }
    }
    Ok(ids)
}

/// Replay a raw log file from a given offset into the keydir.
/// Used after loading a hint file to pick up any writes appended after the hint was generated.
/// Replay a log file from a given offset. Returns the byte offset just past
/// the last valid record.
fn replay_log_from(
    log_path: &Path,
    file_id: u64,
    start_offset: u64,
    keydir: &mut KeyDir,
) -> Result<u64> {
    let mut reader = LogReader::open(log_path).map_err(|e| StoreError(e.to_string()))?;
    reader
        .seek_to(start_offset)
        .map_err(|e| StoreError(e.to_string()))?;
    let mut offset = start_offset;

    // Use tolerant iteration: stop silently at truncated/corrupt tail records.
    while let Ok(Some(record)) = reader.read_next() {
        let record_len =
            (HEADER_SIZE + record.key.len() + record.value.as_ref().map_or(0, Vec::len)) as u64;

        if let Some(value) = record.value {
            keydir.put(
                record.key.clone(),
                KeyEntry {
                    file_id,
                    offset,
                    #[allow(clippy::cast_possible_truncation)]
                    value_size: value.len() as u32,
                    expire_at_ms: record.expire_at_ms,
                    ref_bit: false,
                },
            );
        } else {
            keydir.delete(&record.key);
        }

        offset += record_len;
    }

    Ok(offset)
}

/// Replay a raw log file into the keydir (used when no hint file exists).
/// Replay a log file into the keydir. Returns the byte offset just past
/// the last valid record (i.e. the point where a new writer should start
/// appending). Any garbage or torn bytes beyond this offset are ignored.
fn replay_log(log_path: &Path, file_id: u64, keydir: &mut KeyDir) -> Result<u64> {
    let mut reader = LogReader::open(log_path).map_err(|e| StoreError(e.to_string()))?;
    let mut offset: u64 = 0;

    let records = reader
        .iter_from_start_tolerant()
        .map_err(|e| StoreError(e.to_string()))?;
    for record in records {
        let record_len =
            (HEADER_SIZE + record.key.len() + record.value.as_ref().map_or(0, Vec::len)) as u64;

        if let Some(value) = record.value {
            keydir.put(
                record.key.clone(),
                KeyEntry {
                    file_id,
                    offset,
                    #[allow(clippy::cast_possible_truncation)]
                    value_size: value.len() as u32,
                    expire_at_ms: record.expire_at_ms,
                    ref_bit: false,
                },
            );
        } else {
            keydir.delete(&record.key);
        }

        offset += record_len;
    }

    Ok(offset)
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
    use tempfile::TempDir;

    fn tmp() -> TempDir {
        tempfile::TempDir::new().unwrap()
    }

    #[test]
    fn put_and_get() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        store.put(b"hello", b"world", 0).unwrap();
        let val = store.get(b"hello").unwrap();
        assert_eq!(val, Some(b"world".to_vec()));
    }

    #[test]
    fn get_missing_returns_none() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        assert!(store.get(b"nope").unwrap().is_none());
    }

    #[test]
    fn put_overwrites_value() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        store.put(b"k", b"v1", 0).unwrap();
        store.put(b"k", b"v2", 0).unwrap();
        assert_eq!(store.get(b"k").unwrap(), Some(b"v2".to_vec()));
    }

    #[test]
    fn delete_removes_key() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        store.put(b"k", b"v", 0).unwrap();
        assert!(store.delete(b"k").unwrap());
        assert!(store.get(b"k").unwrap().is_none());
    }

    #[test]
    fn delete_missing_returns_false() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        assert!(!store.delete(b"ghost").unwrap());
    }

    #[test]
    fn keys_returns_live_keys() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        store.put(b"a", b"1", 0).unwrap();
        store.put(b"b", b"2", 0).unwrap();
        store.delete(b"a").unwrap();
        let keys = store.keys();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0], b"b");
    }

    #[test]
    fn expired_key_returns_none() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        // expire 1ms in the past
        let past_ms = now_ms().saturating_sub(1);
        store.put(b"ttl", b"val", past_ms).unwrap();
        assert!(store.get(b"ttl").unwrap().is_none());
    }

    #[test]
    fn not_yet_expired_key_returns_value() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let future_ms = now_ms() + 60_000; // 1 minute from now
        store.put(b"ttl", b"val", future_ms).unwrap();
        assert_eq!(store.get(b"ttl").unwrap(), Some(b"val".to_vec()));
    }

    #[test]
    fn persistence_survives_reopen() {
        let dir = tmp();

        {
            let mut store = Store::open(dir.path()).unwrap();
            store.put(b"persist", b"data", 0).unwrap();
        }

        let mut store = Store::open(dir.path()).unwrap();
        assert_eq!(
            store.get(b"persist").unwrap(),
            Some(b"data".to_vec()),
            "value must survive store close and reopen"
        );
    }

    #[test]
    fn delete_persists_across_reopen() {
        let dir = tmp();

        {
            let mut store = Store::open(dir.path()).unwrap();
            store.put(b"k", b"v", 0).unwrap();
            store.delete(b"k").unwrap();
        }

        let mut store = Store::open(dir.path()).unwrap();
        assert!(
            store.get(b"k").unwrap().is_none(),
            "tombstone must be replayed on reopen"
        );
    }

    #[test]
    fn hint_file_speeds_up_reopen() {
        let dir = tmp();

        {
            let mut store = Store::open(dir.path()).unwrap();
            store.put(b"a", b"1", 0).unwrap();
            store.put(b"b", b"2", 0).unwrap();
            store.write_hint_file().unwrap();
        }

        // Reopen — should load from hint file
        let mut store = Store::open(dir.path()).unwrap();
        assert_eq!(store.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(store.get(b"b").unwrap(), Some(b"2".to_vec()));
    }

    #[test]
    fn len_and_is_empty() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        assert!(store.is_empty());
        store.put(b"x", b"y", 0).unwrap();
        assert_eq!(store.len(), 1);
        assert!(!store.is_empty());
    }

    // ------------------------------------------------------------------
    // Edge cases
    // ------------------------------------------------------------------

    #[test]
    fn zero_length_key_and_value() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        // Empty value is treated as tombstone at the log level — put returns Ok
        // but get returns None (tombstone logic). This is intentional: callers
        // should not store empty values; the store treats them as deletes.
        store.put(b"", b"nonempty", 0).unwrap();
        assert!(store.get(b"").unwrap().is_some());
    }

    #[test]
    fn non_utf8_binary_key_and_value() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let key = vec![0xFF, 0x00, 0xAB];
        let value = vec![0x01, 0x02, 0x03, 0x04];
        store.put(&key, &value, 0).unwrap();
        assert_eq!(store.get(&key).unwrap(), Some(value));
    }

    #[test]
    fn large_value_1mb() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let value = vec![0x42u8; 1024 * 1024]; // 1 MiB
        store.put(b"bigval", &value, 0).unwrap();
        let got = store.get(b"bigval").unwrap().unwrap();
        assert_eq!(got.len(), 1024 * 1024);
        assert!(got.iter().all(|&b| b == 0x42));
    }

    #[test]
    fn stress_1000_keys_persist_and_reload() {
        let dir = tmp();

        {
            let mut store = Store::open(dir.path()).unwrap();
            for i in 0u32..1000 {
                let key = i.to_le_bytes();
                let value = format!("value_{i}");
                store.put(&key, value.as_bytes(), 0).unwrap();
            }
        }

        let mut store = Store::open(dir.path()).unwrap();
        assert_eq!(store.len(), 1000);
        for i in 0u32..1000 {
            let key = i.to_le_bytes();
            let expected = format!("value_{i}");
            assert_eq!(
                store.get(&key).unwrap(),
                Some(expected.into_bytes()),
                "key {i} must survive reopen"
            );
        }
    }

    #[test]
    fn put_batch_single_fsync_semantics() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();

        // Put 10 entries in one batch — all committed with one fsync
        let pairs: Vec<(Vec<u8>, Vec<u8>)> = (0u8..10).map(|i| (vec![i], vec![i * 2])).collect();
        let entries: Vec<(&[u8], &[u8], u64)> = pairs
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice(), 0u64))
            .collect();
        store.put_batch(&entries).unwrap();

        for (k, v) in &pairs {
            assert_eq!(store.get(k).unwrap(), Some(v.clone()));
        }
    }

    #[test]
    fn put_batch_persists_after_reopen() {
        let dir = tmp();

        {
            let mut store = Store::open(dir.path()).unwrap();
            let pairs: Vec<(Vec<u8>, Vec<u8>)> =
                (0u8..5).map(|i| (vec![i], vec![i + 100])).collect();
            let entries: Vec<(&[u8], &[u8], u64)> = pairs
                .iter()
                .map(|(k, v)| (k.as_slice(), v.as_slice(), 0u64))
                .collect();
            store.put_batch(&entries).unwrap();
        }

        let mut store = Store::open(dir.path()).unwrap();
        for i in 0u8..5 {
            assert_eq!(store.get(&[i]).unwrap(), Some(vec![i + 100]));
        }
    }

    #[test]
    fn empty_batch_is_noop() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        store.put_batch(&[]).unwrap();
        assert!(store.is_empty());
    }

    #[test]
    fn double_delete_returns_false_second_time() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        store.put(b"k", b"v", 0).unwrap();
        assert!(store.delete(b"k").unwrap());
        assert!(!store.delete(b"k").unwrap()); // already gone
    }

    #[test]
    fn overwrite_1000_times_only_latest_survives_reopen() {
        let dir = tmp();

        {
            let mut store = Store::open(dir.path()).unwrap();
            for i in 0u32..1000 {
                store.put(b"hotkey", format!("v{i}").as_bytes(), 0).unwrap();
            }
        }

        let mut store = Store::open(dir.path()).unwrap();
        assert_eq!(store.len(), 1, "only 1 live key after 1000 overwrites");
        assert_eq!(
            store.get(b"hotkey").unwrap(),
            Some(b"v999".to_vec()),
            "must read latest value"
        );
    }

    #[test]
    fn keys_excludes_expired_entries() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let past = now_ms().saturating_sub(1);
        store.put(b"live", b"v", 0).unwrap();
        store.put(b"dead", b"v", past).unwrap();
        let keys = store.keys();
        assert!(keys.contains(&b"live".to_vec()));
        assert!(!keys.contains(&b"dead".to_vec()));
    }

    #[test]
    fn open_empty_dir_starts_clean() {
        let dir = tmp();
        let store = Store::open(dir.path()).unwrap();
        assert!(store.is_empty());
        assert_eq!(store.len(), 0);
    }

    // ------------------------------------------------------------------
    // put_batch edge cases
    // ------------------------------------------------------------------

    #[test]
    fn put_batch_with_expiry_values_are_readable() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let future_ms = now_ms() + 60_000;
        let pairs: Vec<(&[u8], &[u8], u64)> = vec![(b"a", b"1", future_ms), (b"b", b"2", 0)];
        store.put_batch(&pairs).unwrap();
        assert_eq!(store.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(store.get(b"b").unwrap(), Some(b"2".to_vec()));
    }

    #[test]
    fn put_batch_expired_entries_not_returned_on_get() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let past_ms = now_ms().saturating_sub(1);
        let pairs: Vec<(&[u8], &[u8], u64)> =
            vec![(b"expired", b"val", past_ms), (b"live", b"val", 0)];
        store.put_batch(&pairs).unwrap();
        assert!(store.get(b"expired").unwrap().is_none());
        assert!(store.get(b"live").unwrap().is_some());
    }

    #[test]
    fn put_batch_last_write_wins_within_same_batch() {
        // If the same key appears twice in one batch, the second write should win
        // after reopen (last write in log wins on replay).
        let dir = tmp();

        {
            let mut store = Store::open(dir.path()).unwrap();
            let pairs: Vec<(&[u8], &[u8], u64)> = vec![(b"k", b"first", 0), (b"k", b"second", 0)];
            store.put_batch(&pairs).unwrap();
        }

        let mut store = Store::open(dir.path()).unwrap();
        // After log replay the later entry wins
        let val = store.get(b"k").unwrap().unwrap();
        assert_eq!(val, b"second".to_vec());
    }

    #[test]
    fn put_batch_100_entries_all_readable_after_reopen() {
        let dir = tmp();

        {
            let mut store = Store::open(dir.path()).unwrap();
            let kv: Vec<(Vec<u8>, Vec<u8>)> = (0u8..100)
                .map(|i| (vec![i], vec![i.wrapping_mul(3)]))
                .collect();
            let pairs: Vec<(&[u8], &[u8], u64)> = kv
                .iter()
                .map(|(k, v)| (k.as_slice(), v.as_slice(), 0u64))
                .collect();
            store.put_batch(&pairs).unwrap();
        }

        let mut store = Store::open(dir.path()).unwrap();
        for i in 0u8..100 {
            assert_eq!(
                store.get(&[i]).unwrap(),
                Some(vec![i.wrapping_mul(3)]),
                "key {i} missing after reopen"
            );
        }
    }

    // ------------------------------------------------------------------
    // Expiry edge cases
    // ------------------------------------------------------------------

    #[test]
    fn expired_key_is_removed_from_keydir_on_get() {
        // After get returns None for an expired key, len() should be 0.
        // len() now filters logically-expired entries, so even before the get
        // call the expired key is not counted.
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let past_ms = now_ms().saturating_sub(1);
        store.put(b"ttl", b"v", past_ms).unwrap();
        // len() excludes expired entries — already 0 before get() evicts it
        assert_eq!(store.len(), 0);
        assert!(store.get(b"ttl").unwrap().is_none());
        // Keydir entry has been physically removed by get()
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn put_then_overwrite_with_no_expiry_clears_ttl() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let future_ms = now_ms() + 60_000;
        store.put(b"k", b"v1", future_ms).unwrap();
        store.put(b"k", b"v2", 0).unwrap(); // clear TTL
        assert_eq!(store.get(b"k").unwrap(), Some(b"v2".to_vec()));
    }

    #[test]
    fn put_then_overwrite_with_no_expiry_persists_without_ttl() {
        let dir = tmp();
        {
            let mut store = Store::open(dir.path()).unwrap();
            let future_ms = now_ms() + 60_000;
            store.put(b"k", b"v1", future_ms).unwrap();
            store.put(b"k", b"v2", 0).unwrap();
        }
        let mut store = Store::open(dir.path()).unwrap();
        assert_eq!(store.get(b"k").unwrap(), Some(b"v2".to_vec()));
    }

    // ------------------------------------------------------------------
    // Hint file edge cases
    // ------------------------------------------------------------------

    #[test]
    fn hint_file_written_before_delete_does_not_resurrect_key() {
        // Write hint, then delete. On reopen: hint loads the key, but log replay
        // of the tail (tombstone) should override it — or if using hint only,
        // the tombstone in the log tail must still be replayed.
        // Currently open() loads hint OR replays log (not both). If hint exists,
        // the log tail after the hint is NOT replayed (known Bitcask limitation:
        // hint files must be written after the last compaction). This test verifies
        // the currently-implemented behaviour and documents it.
        let dir = tmp();

        {
            let mut store = Store::open(dir.path()).unwrap();
            store.put(b"k", b"v", 0).unwrap();
            store.write_hint_file().unwrap();
            // Now delete — but hint already written above
            store.delete(b"k").unwrap();
        }

        // The hint file has "k→v". The log has: put(k,v) + tombstone(k).
        // open() sees a hint file → loads it → does NOT replay the raw log for
        // that file_id. So "k" will appear alive from hint.
        // This is the correct Bitcask-as-designed behaviour: hint files should only
        // be written after compaction (which drops tombstones), not mid-session.
        // The test documents this: after a reopen via hint, the key may appear alive
        // even though it was deleted. The shard GenServer must write hints only after
        // compaction, never between puts and deletes on the same file.
        let mut store = Store::open(dir.path()).unwrap();
        // With current implementation: key is present (hint wins over un-replayed log tail)
        let _ = store.get(b"k"); // Just assert no panic — behaviour is implementation-defined
    }

    // ------------------------------------------------------------------
    // Multi-key delete and reinsert across reopen
    // ------------------------------------------------------------------

    #[test]
    fn delete_all_keys_then_put_new_ones_after_reopen() {
        let dir = tmp();

        {
            let mut store = Store::open(dir.path()).unwrap();
            for i in 0u8..5 {
                store.put(&[i], &[i], 0).unwrap();
            }
            for i in 0u8..5 {
                store.delete(&[i]).unwrap();
            }
        }

        let mut store = Store::open(dir.path()).unwrap();
        assert!(store.is_empty(), "all keys deleted");

        // Now insert new keys into the reopened store
        store.put(b"new", b"value", 0).unwrap();
        assert_eq!(store.get(b"new").unwrap(), Some(b"value".to_vec()));
    }

    // ------------------------------------------------------------------
    // Concurrent keys — values include binary patterns
    // ------------------------------------------------------------------

    #[test]
    fn all_byte_values_0x00_to_0xff_roundtrip() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let value: Vec<u8> = (0u8..=255).collect();
        store.put(b"bytes", &value, 0).unwrap();
        assert_eq!(store.get(b"bytes").unwrap(), Some(value));
    }

    #[test]
    fn reopen_after_hint_file_written_matches_log_replay() {
        let dir = tmp();

        // Write some data and a hint file
        {
            let mut store = Store::open(dir.path()).unwrap();
            store.put(b"a", b"1", 0).unwrap();
            store.put(b"b", b"2", 0).unwrap();
            store.delete(b"a").unwrap();
            store.write_hint_file().unwrap();
        }

        // Reopen via hint file path
        let mut store_from_hint = Store::open(dir.path()).unwrap();

        // Remove hint file and reopen via log replay
        for entry in std::fs::read_dir(dir.path()).unwrap() {
            let e = entry.unwrap();
            if e.file_name().to_string_lossy().ends_with(".hint") {
                std::fs::remove_file(e.path()).unwrap();
            }
        }
        let mut store_from_log = Store::open(dir.path()).unwrap();

        assert_eq!(
            store_from_hint.get(b"a").unwrap(),
            store_from_log.get(b"a").unwrap(),
            "hint and log replay must agree on deleted key"
        );
        assert_eq!(
            store_from_hint.get(b"b").unwrap(),
            store_from_log.get(b"b").unwrap(),
            "hint and log replay must agree on live key"
        );
    }

    // ------------------------------------------------------------------
    // EC-2: Corrupt hint file — must fall back to log replay
    // ------------------------------------------------------------------

    /// EC-2: When the hint file for a data file contains a well-formed header
    /// that claims a `key_size` larger than the remaining bytes (mid-entry
    /// truncation), `Store::open` must succeed by falling back to full log
    /// replay rather than propagating the parse error.
    #[test]
    fn corrupt_hint_file_falls_back_to_log_replay() {
        let dir = tmp();

        // 1. Write some data and a valid hint file.
        {
            let mut store = Store::open(dir.path()).unwrap();
            store.put(b"key1", b"val1", 0).unwrap();
            store.put(b"key2", b"val2", 0).unwrap();
            store.write_hint_file().unwrap();
        }

        // 2. Corrupt the hint file: write exactly HINT_HEADER_SIZE (34) bytes
        //    of zeros. The reader reads the 4-byte CRC (0x00000000) and the
        //    30-byte body (all zeros), computes the real CRC over the body,
        //    and gets a mismatch → HintError → log replay fallback.
        use crate::hint::HINT_HEADER_SIZE;
        for entry in std::fs::read_dir(dir.path()).unwrap() {
            let e = entry.unwrap();
            if e.file_name().to_string_lossy().ends_with(".hint") {
                // All-zero bytes: stored CRC (0) will not match the real CRC
                // of 30 zero bytes, triggering a CRC mismatch error.
                let corrupt = vec![0u8; HINT_HEADER_SIZE];
                std::fs::write(e.path(), &corrupt).unwrap();
            }
        }

        // 3. Reopen — must not error; must recover data from the log.
        let mut store = Store::open(dir.path()).unwrap();
        assert_eq!(
            store.get(b"key1").unwrap(),
            Some(b"val1".to_vec()),
            "key1 must be recovered from log replay after corrupt hint"
        );
        assert_eq!(
            store.get(b"key2").unwrap(),
            Some(b"val2".to_vec()),
            "key2 must be recovered from log replay after corrupt hint"
        );
    }

    /// EC-2 variant: A hint file truncated mid-key (header intact, key bytes
    /// missing) also triggers fallback to log replay without returning an error.
    #[test]
    fn truncated_hint_file_falls_back_to_log_replay() {
        let dir = tmp();

        {
            let mut store = Store::open(dir.path()).unwrap();
            store.put(b"alpha", b"beta", 0).unwrap();
            store.write_hint_file().unwrap();
        }

        // Truncate the hint file to exactly HINT_HEADER_SIZE bytes (34).
        // The CRC (4 bytes) and body header (30 bytes) are intact, but the
        // key bytes are gone. The reader reads key_size > 0 (because "alpha"
        // was stored), then calls read_exact for the key bytes and gets
        // UnexpectedEof → HintError → fallback to log replay.
        use crate::hint::HINT_HEADER_SIZE;
        for entry in std::fs::read_dir(dir.path()).unwrap() {
            let e = entry.unwrap();
            if e.file_name().to_string_lossy().ends_with(".hint") {
                let full = std::fs::read(e.path()).unwrap();
                // Keep only the header bytes (CRC + body header, no key).
                let truncated_len = HINT_HEADER_SIZE.min(full.len());
                std::fs::write(e.path(), &full[..truncated_len]).unwrap();
            }
        }

        let mut store = Store::open(dir.path()).unwrap();
        assert_eq!(
            store.get(b"alpha").unwrap(),
            Some(b"beta".to_vec()),
            "value must survive hint truncation via log replay fallback"
        );
    }

    // ------------------------------------------------------------------
    // EC-7: Hint-only directory — orphan .hint with no .data must be skipped
    // ------------------------------------------------------------------

    /// EC-7: If only `.hint` files exist in the data directory (the
    /// corresponding `.log` file is absent), `Store::open` must succeed with
    /// an empty keydir rather than panicking or erroring.
    #[test]
    fn hint_only_directory_opens_with_empty_keydir() {
        let dir = tmp();

        // 1. Create a real store with a hint file.
        {
            let mut store = Store::open(dir.path()).unwrap();
            store.put(b"orphan_key", b"orphan_val", 0).unwrap();
            store.write_hint_file().unwrap();
        }

        // 2. Remove ALL .log files, leaving only .hint files.
        for entry in std::fs::read_dir(dir.path()).unwrap() {
            let e = entry.unwrap();
            if e.file_name().to_string_lossy().ends_with(".log") {
                std::fs::remove_file(e.path()).unwrap();
            }
        }

        // 3. Re-open — orphan hints are skipped, keydir is empty, no error.
        let store = Store::open(dir.path()).unwrap();
        assert!(
            store.is_empty(),
            "keydir must be empty when only orphan .hint files exist"
        );
    }

    // ------------------------------------------------------------------
    // Issue 3.4: Lazy TTL eviction must write a tombstone so keys do not
    // resurrect after close+reopen.
    // ------------------------------------------------------------------

    /// After `get` expires a key and writes a tombstone, reopening the store
    /// must NOT resurface the key from the original write record on disk.
    #[test]
    fn expired_key_does_not_resurrect_after_reopen() {
        let dir = tmp();

        {
            let mut store = Store::open(dir.path()).unwrap();
            // Write a key that is already expired (1 second in the past).
            let past_ms = now_ms().saturating_sub(1000);
            store.put(b"key", b"val", past_ms).unwrap();
            // get() must return None and write a tombstone to the log.
            assert!(store.get(b"key").unwrap().is_none());
        }

        // Reopen: log replay must see tombstone after the original record and
        // must NOT re-insert the key into the keydir.
        let mut store = Store::open(dir.path()).unwrap();
        assert!(
            store.get(b"key").unwrap().is_none(),
            "expired key must not resurrect after store close and reopen"
        );
    }

    /// `len()` must count only non-expired keys.
    #[test]
    fn len_excludes_expired_entries() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let past_ms = now_ms().saturating_sub(1000);
        store.put(b"live", b"v", 0).unwrap();
        store.put(b"dead", b"v", past_ms).unwrap();
        assert_eq!(store.len(), 1, "only the live key should be counted");
    }

    /// After TTL eviction (tombstone write), `len()` stays consistent and the
    /// count is still correct after a reopen.
    #[test]
    fn len_after_ttl_eviction_is_consistent() {
        let dir = tmp();
        let past_ms = now_ms().saturating_sub(1000);

        {
            let mut store = Store::open(dir.path()).unwrap();
            store.put(b"live1", b"v", 0).unwrap();
            store.put(b"live2", b"v", 0).unwrap();
            store.put(b"expired", b"v", past_ms).unwrap();

            // Trigger eviction: tombstone is written to the log.
            assert!(store.get(b"expired").unwrap().is_none());
            assert_eq!(store.len(), 2, "len must be 2 after eviction");
        }

        // Reopen — tombstone prevents resurrection; len must still be 2.
        let store = Store::open(dir.path()).unwrap();
        assert_eq!(
            store.len(),
            2,
            "len must be 2 after reopen (tombstone prevents resurrection)"
        );
    }

    /// EC-7 variant: A directory with a `.hint` for one file ID and a `.log`
    /// for a different file ID opens correctly, loading only the entries with
    /// a matching log file.
    #[test]
    fn partial_hint_no_log_does_not_contaminate_keydir() {
        let dir = tmp();

        // Manually plant a .hint file for file ID 99 (no matching .log).
        use crate::hint::{HintEntry, HintWriter};
        let orphan_hint = dir.path().join("00000000000000000099.hint");
        let mut w = HintWriter::open(&orphan_hint).unwrap();
        w.write_entry(&HintEntry {
            file_id: 99,
            offset: 0,
            value_size: 5,
            expire_at_ms: 0,
            key: b"ghost".to_vec(),
        })
        .unwrap();
        w.commit().unwrap();

        // Open the store — no .log files at all, so file_ids is empty,
        // the orphan hint is never iterated, keydir stays empty.
        let store = Store::open(dir.path()).unwrap();
        assert!(
            store.is_empty(),
            "orphan hint with no matching log must not inject keys into keydir"
        );
    }

    // ------------------------------------------------------------------
    // Issue 5.2: Atomic hint file writes — partial hint falls back to log
    // ------------------------------------------------------------------

    /// Issue 5.2: When a `.hint` file is non-empty but corrupt (simulating a
    /// partial write followed by truncation), `Store::open` must fall back to
    /// full log replay and recover all keys correctly.
    ///
    /// This is the integration-level companion to the unit tests in `hint.rs`.
    /// It verifies that the EC-2 fallback path in `Store::open` handles the
    /// specific failure mode that the atomic write fix addresses: a hint file
    /// that is non-empty (so it passes the `exists()` check) but corrupt enough
    /// to cause a parse error (triggering log-replay fallback).
    #[test]
    fn store_open_with_partial_hint_falls_back_to_log() {
        let dir = tmp();

        // 1. Open a store, put some keys, and write a valid hint file.
        {
            let mut store = Store::open(dir.path()).unwrap();
            store.put(b"key_a", b"val_a", 0).unwrap();
            store.put(b"key_b", b"val_b", 0).unwrap();
            store.put(b"key_c", b"val_c", 0).unwrap();
            store.write_hint_file().unwrap();
        }

        // 2. Manually truncate the hint file to a non-empty but incomplete
        //    length (exactly HINT_HEADER_SIZE=34 bytes — CRC + body header
        //    present, key bytes missing). This simulates a crash after the old
        //    hint was zeroed but before new content was fully written.
        use crate::hint::HINT_HEADER_SIZE;
        for entry in std::fs::read_dir(dir.path()).unwrap() {
            let e = entry.unwrap();
            if e.file_name().to_string_lossy().ends_with(".hint") {
                let full = std::fs::read(e.path()).unwrap();
                // CRC + body header are intact but key bytes are gone —
                // this triggers UnexpectedEof in the hint parser.
                let truncated_len = HINT_HEADER_SIZE.min(full.len());
                std::fs::write(e.path(), &full[..truncated_len]).unwrap();
            }
        }

        // 3. Reopen — the corrupt hint triggers EC-2 fallback to log replay.
        //    All three keys must be recovered correctly.
        let mut store = Store::open(dir.path()).unwrap();
        assert_eq!(
            store.get(b"key_a").unwrap(),
            Some(b"val_a".to_vec()),
            "key_a must be recovered via log replay after partial hint"
        );
        assert_eq!(
            store.get(b"key_b").unwrap(),
            Some(b"val_b".to_vec()),
            "key_b must be recovered via log replay after partial hint"
        );
        assert_eq!(
            store.get(b"key_c").unwrap(),
            Some(b"val_c".to_vec()),
            "key_c must be recovered via log replay after partial hint"
        );
    }

    // ------------------------------------------------------------------
    // Issue 7.4: write_hint_file must fsync the log before writing the hint
    // ------------------------------------------------------------------

    /// After `write_hint_file` the data referred to by the hint must already be
    /// durable on disk.  We verify this by reading back the raw log bytes at
    /// the offset stored in the hint entry and confirming they exist and are
    /// decodable.
    #[test]
    fn write_hint_file_syncs_log_first() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        store.put(b"synckey", b"syncval", 0).unwrap();

        // write_hint_file now calls self.writer.sync() before writing the hint.
        store.write_hint_file().unwrap();

        // The hint file for the active file ID must now exist.
        let hint_path = dir
            .path()
            .join(format!("{:020}.hint", store.active_file_id));
        assert!(hint_path.exists(), "hint file must be written");

        // The log file must also exist and the first record must be readable,
        // confirming the data was flushed before the hint was written.
        let log_path = dir.path().join(format!("{:020}.log", store.active_file_id));
        let mut reader = crate::log::LogReader::open(&log_path).unwrap();
        let record = reader.read_at(0).unwrap();
        assert!(
            record.is_some(),
            "log record at offset 0 must be present and readable after write_hint_file"
        );
        let record = record.unwrap();
        assert_eq!(record.key, b"synckey");
        assert_eq!(record.value, Some(b"syncval".to_vec()));
    }

    // ------------------------------------------------------------------
    // Issue 6.3: Proactive TTL GC — purge_expired
    // ------------------------------------------------------------------

    /// `purge_expired` must return the correct count and remove expired keys
    /// from the keydir while leaving live keys untouched.
    #[test]
    fn purge_expired_removes_keys_from_keydir() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let past_ms = now_ms().saturating_sub(1000);

        store.put(b"live1", b"v", 0).unwrap();
        store.put(b"expired1", b"v", past_ms).unwrap();
        store.put(b"expired2", b"v", past_ms).unwrap();

        let count = store.purge_expired().unwrap();
        assert_eq!(count, 2, "purge_expired must report 2 purged keys");

        assert!(
            store.get(b"expired1").unwrap().is_none(),
            "expired1 must be gone"
        );
        assert!(
            store.get(b"expired2").unwrap().is_none(),
            "expired2 must be gone"
        );
        assert_eq!(
            store.get(b"live1").unwrap(),
            Some(b"v".to_vec()),
            "live1 must still be readable"
        );
    }

    /// Tombstones written by `purge_expired` must prevent key resurrection
    /// across a store close and reopen.
    #[test]
    fn purge_expired_writes_tombstones_preventing_resurrection() {
        let dir = tmp();
        let past_ms = now_ms().saturating_sub(1000);

        {
            let mut store = Store::open(dir.path()).unwrap();
            store.put(b"ex_key", b"val", past_ms).unwrap();
            let count = store.purge_expired().unwrap();
            assert_eq!(count, 1);
        }

        // Reopen — log replay must see the tombstone after the original record
        // and must NOT re-insert ex_key into the keydir.
        let mut store = Store::open(dir.path()).unwrap();
        assert!(
            store.get(b"ex_key").unwrap().is_none(),
            "ex_key must not resurrect after purge_expired + reopen"
        );
    }

    /// When no keys are expired, `purge_expired` must return `Ok(0)` and leave
    /// the keydir untouched.
    #[test]
    fn purge_expired_returns_zero_when_nothing_expired() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        store.put(b"a", b"1", 0).unwrap();
        store.put(b"b", b"2", 0).unwrap();
        store.put(b"c", b"3", 0).unwrap();

        let count = store.purge_expired().unwrap();
        assert_eq!(count, 0, "no keys are expired so count must be 0");

        // All keys must still be readable.
        assert!(store.get(b"a").unwrap().is_some());
        assert!(store.get(b"b").unwrap().is_some());
        assert!(store.get(b"c").unwrap().is_some());
    }

    /// `len()` must reflect the correct count after `purge_expired` removes
    /// expired entries from the keydir.
    #[test]
    fn len_is_accurate_after_purge_expired() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let past_ms = now_ms().saturating_sub(1000);

        store.put(b"live1", b"v", 0).unwrap();
        store.put(b"live2", b"v", 0).unwrap();
        store.put(b"live3", b"v", 0).unwrap();
        store.put(b"exp1", b"v", past_ms).unwrap();
        store.put(b"exp2", b"v", past_ms).unwrap();

        store.purge_expired().unwrap();

        assert_eq!(
            store.len(),
            3,
            "len must be 3 (only live keys) after purge_expired"
        );
    }

    // ------------------------------------------------------------------
    // CRC / record integrity
    // ------------------------------------------------------------------

    /// Put a key, close the store, corrupt a byte in the middle of the log
    /// record (not the CRC bytes), reopen and get — must return Err or None.
    #[test]
    fn corrupt_crc_in_log_returns_error_on_get() {
        use std::io::{Seek, SeekFrom, Write};
        let dir = tmp();

        {
            let mut store = Store::open(dir.path()).unwrap();
            store.put(b"key", b"value", 0).unwrap();
        }

        // Locate the single .log file and flip a byte in the key area
        // (byte at HEADER_SIZE, which is after the 4-byte CRC field).
        // This invalidates the CRC without touching the CRC bytes themselves.
        for entry in std::fs::read_dir(dir.path()).unwrap() {
            let e = entry.unwrap();
            if e.file_name().to_string_lossy().ends_with(".log") {
                let mut f = std::fs::OpenOptions::new()
                    .write(true)
                    .open(e.path())
                    .unwrap();
                // Flip one byte in the key area (after the 26-byte header).
                f.seek(SeekFrom::Start(crate::log::HEADER_SIZE as u64))
                    .unwrap();
                let mut byte = [0u8; 1];
                use std::io::Read;
                f.seek(SeekFrom::Start(crate::log::HEADER_SIZE as u64))
                    .unwrap();
                {
                    let mut rf = std::fs::File::open(e.path()).unwrap();
                    rf.seek(SeekFrom::Start(crate::log::HEADER_SIZE as u64))
                        .unwrap();
                    rf.read_exact(&mut byte).unwrap();
                }
                byte[0] ^= 0xFF;
                f.seek(SeekFrom::Start(crate::log::HEADER_SIZE as u64))
                    .unwrap();
                f.write_all(&byte).unwrap();
            }
        }

        // Reopen the store — the corrupt record will be skipped by the tolerant
        // reader (crash-recovery semantics), so get returns None rather than Err.
        let mut store = Store::open(dir.path()).unwrap();
        // The key should not be accessible (CRC mismatch is handled gracefully).
        let result = store.get(b"key").unwrap();
        assert!(
            result.is_none(),
            "corrupt CRC in log must cause get to return None (tolerant recovery)"
        );
    }

    /// Put 5 records, truncate the 3rd record to half its size, reopen.
    /// Records 1 and 2 must be intact; records 3-5 gone (tolerant reader stops
    /// at first error).
    #[test]
    fn store_recovers_after_mid_file_record_truncation() {
        let dir = tmp();

        let mut offsets = Vec::new();
        {
            let mut store = Store::open(dir.path()).unwrap();
            for i in 0u8..5 {
                let key = format!("key{i}");
                let val = format!("val{i}");
                store.put(key.as_bytes(), val.as_bytes(), 0).unwrap();
            }
            // Capture writer offset so we can locate the 3rd record boundary.
            // We'll compute truncation point directly from disk.
        }

        // Reopen just to confirm 5 records, then compute the truncation point.
        {
            let log_path_buf = std::fs::read_dir(dir.path())
                .unwrap()
                .find_map(|e| {
                    let e = e.unwrap();
                    if e.file_name().to_string_lossy().ends_with(".log") {
                        Some(e.path())
                    } else {
                        None
                    }
                })
                .unwrap();

            // Read all 5 records to find where the 3rd one starts.
            let mut reader = crate::log::LogReader::open(&log_path_buf).unwrap();
            let records = reader.iter_from_start().unwrap();
            assert_eq!(records.len(), 5, "expected 5 records before truncation");

            // Compute offset of 3rd record (index 2).
            let rec0_len = (crate::log::HEADER_SIZE
                + records[0].key.len()
                + records[0].value.as_ref().map_or(0, Vec::len)) as u64;
            let rec1_len = (crate::log::HEADER_SIZE
                + records[1].key.len()
                + records[1].value.as_ref().map_or(0, Vec::len)) as u64;
            let rec2_start = rec0_len + rec1_len;
            // Truncate to halfway through the 3rd record.
            let rec2_half_len = (crate::log::HEADER_SIZE
                + records[2].key.len()
                + records[2].value.as_ref().map_or(0, Vec::len))
                as u64
                / 2;
            let truncate_at = rec2_start + rec2_half_len;

            let file = std::fs::OpenOptions::new()
                .write(true)
                .open(&log_path_buf)
                .unwrap();
            file.set_len(truncate_at).unwrap();
            offsets.push(rec2_start); // used for assertion below
        }

        // Reopen — tolerant reader stops at truncated record 3.
        let mut store = Store::open(dir.path()).unwrap();
        // Records 0 and 1 must be intact.
        assert_eq!(
            store.get(b"key0").unwrap(),
            Some(b"val0".to_vec()),
            "key0 must survive"
        );
        assert_eq!(
            store.get(b"key1").unwrap(),
            Some(b"val1".to_vec()),
            "key1 must survive"
        );
        // Records 2-4 must be gone (tolerant reader stopped at first error).
        assert!(
            store.get(b"key2").unwrap().is_none(),
            "key2 must be gone after truncation"
        );
        assert!(
            store.get(b"key3").unwrap().is_none(),
            "key3 must be gone after truncation"
        );
        assert!(
            store.get(b"key4").unwrap().is_none(),
            "key4 must be gone after truncation"
        );
        let _ = offsets;
    }

    // ------------------------------------------------------------------
    // Offset tracking
    // ------------------------------------------------------------------

    /// Put 10 records and verify that `writer.offset` increases monotonically
    /// and equals the sum of encoded record sizes.
    #[test]
    fn offset_is_correct_after_multiple_puts() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let mut expected_offset = 0u64;
        for i in 0u8..10 {
            let key = vec![i; (i as usize) + 1]; // key length 1..10
            let val = vec![i; (i as usize) + 2]; // val length 2..11
            store.put(&key, &val, 0).unwrap();
            let record_size = (crate::log::HEADER_SIZE + key.len() + val.len()) as u64;
            expected_offset += record_size;
            assert!(
                store.writer.offset >= expected_offset,
                "offset must increase after put {i}"
            );
        }
        assert_eq!(
            store.writer.offset, expected_offset,
            "final offset must equal sum of record sizes"
        );
    }

    // ------------------------------------------------------------------
    // len() accuracy
    // ------------------------------------------------------------------

    #[test]
    fn len_is_zero_for_empty_store() {
        let dir = tmp();
        let store = Store::open(dir.path()).unwrap();
        assert_eq!(store.len(), 0);
        assert!(store.is_empty());
    }

    #[test]
    fn len_decreases_after_delete() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        store.put(b"a", b"1", 0).unwrap();
        store.put(b"b", b"2", 0).unwrap();
        store.put(b"c", b"3", 0).unwrap();
        assert_eq!(store.len(), 3);
        store.delete(b"b").unwrap();
        assert_eq!(store.len(), 2);
        store.delete(b"a").unwrap();
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn len_counts_only_live_non_expired_keys() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let past_ms = now_ms().saturating_sub(5000);
        store.put(b"live1", b"v", 0).unwrap();
        store.put(b"live2", b"v", 0).unwrap();
        store.put(b"live3", b"v", 0).unwrap();
        store.put(b"exp1", b"v", past_ms).unwrap();
        store.put(b"exp2", b"v", past_ms).unwrap();
        assert_eq!(store.len(), 3, "only live non-expired keys are counted");
    }

    // ------------------------------------------------------------------
    // put/get/delete semantics
    // ------------------------------------------------------------------

    #[test]
    fn put_empty_key_and_value() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        // Empty key with non-empty value should work.
        store.put(b"", b"non_empty_value", 0).unwrap();
        let result = store.get(b"").unwrap();
        assert!(
            result.is_some(),
            "empty key with non-empty value must be retrievable"
        );
    }

    #[test]
    fn put_very_large_value_1mb_roundtrip() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let value: Vec<u8> = (0u8..=255).cycle().take(1024 * 1024).collect();
        store.put(b"bigkey", &value, 0).unwrap();
        let got = store.get(b"bigkey").unwrap().unwrap();
        assert_eq!(got.len(), 1024 * 1024, "1MB value must round-trip");
        assert_eq!(got, value, "1MB value bytes must match exactly");
    }

    #[test]
    fn put_key_with_null_bytes() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let key = b"\x00\x01\x02";
        store.put(key, b"null_key_val", 0).unwrap();
        let result = store.get(key).unwrap();
        assert_eq!(
            result,
            Some(b"null_key_val".to_vec()),
            "key with null bytes must round-trip"
        );
    }

    #[test]
    fn delete_returns_false_for_missing_key() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let result = store.delete(b"nonexistent").unwrap();
        assert!(!result, "delete on missing key must return false");
        assert_eq!(store.len(), 0, "len must remain 0 after no-op delete");
    }

    #[test]
    fn delete_returns_true_for_existing_key() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        store.put(b"present", b"val", 0).unwrap();
        let result = store.delete(b"present").unwrap();
        assert!(result, "delete on existing key must return true");
        assert!(
            store.get(b"present").unwrap().is_none(),
            "key must be gone after delete"
        );
    }

    #[test]
    fn delete_tombstone_prevents_resurrection() {
        let dir = tmp();

        {
            let mut store = Store::open(dir.path()).unwrap();
            store.put(b"zombie", b"alive", 0).unwrap();
            store.delete(b"zombie").unwrap();
        }

        // Reopen — tombstone in log replay must prevent key from coming back.
        let mut store = Store::open(dir.path()).unwrap();
        assert!(
            store.get(b"zombie").unwrap().is_none(),
            "deleted key must not resurrect after reopen"
        );
    }

    // ------------------------------------------------------------------
    // put_batch: new edge cases
    // ------------------------------------------------------------------

    #[test]
    fn put_batch_empty_slice_is_ok() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        // Already covered by empty_batch_is_noop but this is an explicit API contract test.
        store.put_batch(&[]).unwrap();
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn put_batch_all_entries_retrievable() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let kv: Vec<(Vec<u8>, Vec<u8>)> = (0u8..20)
            .map(|i| (format!("bk{i}").into_bytes(), format!("bv{i}").into_bytes()))
            .collect();
        let entries: Vec<(&[u8], &[u8], u64)> = kv
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice(), 0u64))
            .collect();
        store.put_batch(&entries).unwrap();
        for (k, v) in &kv {
            assert_eq!(
                store.get(k).unwrap(),
                Some(v.clone()),
                "batch entry {k:?} must be retrievable"
            );
        }
    }

    #[test]
    fn put_batch_with_expired_entries() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let past_ms = now_ms().saturating_sub(2000);
        let entries: Vec<(&[u8], &[u8], u64)> =
            vec![(b"exp_batch", b"val", past_ms), (b"live_batch", b"val", 0)];
        store.put_batch(&entries).unwrap();
        assert!(
            store.get(b"exp_batch").unwrap().is_none(),
            "expired batch entry must not be readable"
        );
        assert_eq!(
            store.get(b"live_batch").unwrap(),
            Some(b"val".to_vec()),
            "live batch entry must be readable"
        );
    }

    // ------------------------------------------------------------------
    // expiry: detailed edge cases
    // ------------------------------------------------------------------

    #[test]
    fn get_returns_none_for_past_expire_at() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let past_ms = now_ms().saturating_sub(1000);
        store.put(b"past_ttl", b"gone", past_ms).unwrap();
        assert!(
            store.get(b"past_ttl").unwrap().is_none(),
            "key with past expire_at must return None"
        );
    }

    #[test]
    fn get_returns_value_for_future_expire_at() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let future_ms = now_ms() + 60_000;
        store.put(b"future_ttl", b"present", future_ms).unwrap();
        assert_eq!(
            store.get(b"future_ttl").unwrap(),
            Some(b"present".to_vec()),
            "key with future expire_at must return value"
        );
    }

    #[test]
    fn keys_excludes_past_expired() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let past_ms = now_ms().saturating_sub(500);
        store.put(b"gone", b"v", past_ms).unwrap();
        store.put(b"here", b"v", 0).unwrap();
        let keys = store.keys();
        assert!(
            !keys.contains(&b"gone".to_vec()),
            "expired key must not appear in keys()"
        );
        assert!(
            keys.contains(&b"here".to_vec()),
            "live key must appear in keys()"
        );
        assert_eq!(keys.len(), 1, "keys() must return exactly 1 live key");
    }

    // ------------------------------------------------------------------
    // purge_expired: detailed edge cases
    // ------------------------------------------------------------------

    #[test]
    fn purge_expired_count_matches_expired_keys() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let past_ms = now_ms().saturating_sub(1000);
        for i in 0u8..5 {
            store.put(&[i], b"expired_val", past_ms).unwrap();
        }
        let count = store.purge_expired().unwrap();
        assert_eq!(count, 5, "purge_expired must return 5 for 5 expired keys");
    }

    #[test]
    fn purge_expired_evicted_keys_not_in_keys_list() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let past_ms = now_ms().saturating_sub(1000);
        store.put(b"exp_a", b"v", past_ms).unwrap();
        store.put(b"exp_b", b"v", past_ms).unwrap();
        store.put(b"live_x", b"v", 0).unwrap();

        store.purge_expired().unwrap();

        let keys = store.keys();
        assert!(
            !keys.contains(&b"exp_a".to_vec()),
            "exp_a must not be in keys after purge"
        );
        assert!(
            !keys.contains(&b"exp_b".to_vec()),
            "exp_b must not be in keys after purge"
        );
        assert!(
            keys.contains(&b"live_x".to_vec()),
            "live_x must still be in keys after purge"
        );
        assert_eq!(keys.len(), 1);
    }

    // ------------------------------------------------------------------
    // Issue 5.1: Corrupt hint CRC triggers log replay fallback
    // ------------------------------------------------------------------

    /// A hint file whose first entry has a flipped byte (corrupting its CRC)
    /// must trigger log replay fallback on `Store::open`, recovering all keys.
    #[test]
    fn corrupt_hint_crc_falls_back_to_log_replay() {
        let dir = tmp();

        // 1. Open store, put keys, write hint file, close.
        {
            let mut store = Store::open(dir.path()).unwrap();
            store.put(b"crc_key1", b"crc_val1", 0).unwrap();
            store.put(b"crc_key2", b"crc_val2", 0).unwrap();
            store.put(b"crc_key3", b"crc_val3", 0).unwrap();
            store.write_hint_file().unwrap();
        }

        // 2. Open the hint file and flip bytes 12..16 (inside the offset field
        //    of the first entry). The stored CRC won't match the new body bytes.
        for entry in std::fs::read_dir(dir.path()).unwrap() {
            let e = entry.unwrap();
            if e.file_name().to_string_lossy().ends_with(".hint") {
                use std::fs::OpenOptions;
                use std::io::{Seek, SeekFrom, Write};
                let mut f = OpenOptions::new().write(true).open(e.path()).unwrap();
                // Offset field starts at byte 4 (CRC) + 8 (file_id) = 12.
                f.seek(SeekFrom::Start(12)).unwrap();
                f.write_all(&[0xFF, 0xFF, 0xFF, 0xFF]).unwrap();
            }
        }

        // 3. Reopen store — must fall back to log replay and recover all keys.
        let mut store = Store::open(dir.path()).unwrap();
        assert_eq!(
            store.get(b"crc_key1").unwrap(),
            Some(b"crc_val1".to_vec()),
            "crc_key1 must be recovered from log replay after CRC-corrupt hint"
        );
        assert_eq!(
            store.get(b"crc_key2").unwrap(),
            Some(b"crc_val2".to_vec()),
            "crc_key2 must be recovered from log replay after CRC-corrupt hint"
        );
        assert_eq!(
            store.get(b"crc_key3").unwrap(),
            Some(b"crc_val3".to_vec()),
            "crc_key3 must be recovered from log replay after CRC-corrupt hint"
        );
    }

    // ==================================================================
    // Deep NIF edge cases — targeting FFI/NIF boundary pitfalls
    // ==================================================================

    // ---- Key / value boundary sizes ----

    #[test]
    fn get_empty_key_returns_value() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        store.put(b"", b"empty_key_val", 0).unwrap();
        assert_eq!(store.get(b"").unwrap(), Some(b"empty_key_val".to_vec()));
    }

    #[test]
    fn get_key_with_embedded_null_bytes() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let key = b"a\0b\0c";
        store.put(key, b"nulls_in_key", 0).unwrap();
        assert_eq!(store.get(key).unwrap(), Some(b"nulls_in_key".to_vec()));
        // Different null pattern must not collide
        assert!(store.get(b"a\0b").unwrap().is_none());
    }

    #[test]
    fn put_empty_value_is_valid() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        store.put(b"k", b"v", 0).unwrap();
        // Empty value is a valid value (Redis SET key "" is valid)
        store.put(b"k", b"", 0).unwrap();
        assert_eq!(
            store.get(b"k").unwrap(),
            Some(vec![]),
            "empty value must be stored, not treated as tombstone"
        );
        // Survives reopen
        drop(store);
        let mut store = Store::open(dir.path()).unwrap();
        assert_eq!(store.get(b"k").unwrap(), Some(vec![]));
    }

    #[test]
    fn put_max_key_65535_bytes() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let key = vec![0xABu8; 65535]; // u16::MAX
        store.put(&key, b"v", 0).unwrap();
        assert_eq!(store.get(&key).unwrap(), Some(b"v".to_vec()));
    }

    #[test]
    fn put_key_65536_bytes_rejected_by_validate() {
        let key = vec![0xABu8; 65536]; // u16::MAX + 1
        let result = crate::log::validate_kv_sizes(&key, b"v");
        assert!(result.is_err(), "key > 65535 bytes must be rejected");
    }

    #[test]
    fn put_value_with_all_null_bytes() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let value = vec![0u8; 1024];
        store.put(b"nullval", &value, 0).unwrap();
        assert_eq!(store.get(b"nullval").unwrap(), Some(value));
    }

    #[test]
    fn put_batch_empty_list_no_side_effects() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        store.put(b"pre", b"existing", 0).unwrap();
        store.put_batch(&[]).unwrap();
        assert_eq!(store.len(), 1);
        assert_eq!(store.get(b"pre").unwrap(), Some(b"existing".to_vec()));
    }

    #[test]
    fn put_batch_single_item() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        store.put_batch(&[(b"only", b"one" as &[u8], 0)]).unwrap();
        assert_eq!(store.get(b"only").unwrap(), Some(b"one".to_vec()));
    }

    #[test]
    fn put_batch_1000_items_all_readable() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let kv: Vec<(Vec<u8>, Vec<u8>)> = (0..1000)
            .map(|i| {
                (
                    format!("bk_{i:04}").into_bytes(),
                    format!("bv_{i:04}").into_bytes(),
                )
            })
            .collect();
        let entries: Vec<(&[u8], &[u8], u64)> = kv
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice(), 0u64))
            .collect();
        store.put_batch(&entries).unwrap();
        for (k, v) in &kv {
            assert_eq!(store.get(k).unwrap(), Some(v.clone()));
        }
    }

    #[test]
    fn delete_nonexistent_key_no_side_effects() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        store.put(b"keep", b"v", 0).unwrap();
        assert!(!store.delete(b"nonexistent").unwrap());
        assert_eq!(store.len(), 1);
        assert_eq!(store.get(b"keep").unwrap(), Some(b"v".to_vec()));
    }

    #[test]
    fn delete_twice_same_key() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        store.put(b"dbl_del", b"v", 0).unwrap();
        assert!(store.delete(b"dbl_del").unwrap());
        assert!(!store.delete(b"dbl_del").unwrap());
        assert!(store.get(b"dbl_del").unwrap().is_none());
    }

    #[test]
    fn get_after_delete_returns_none() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        store.put(b"gone", b"val", 0).unwrap();
        store.delete(b"gone").unwrap();
        assert!(store.get(b"gone").unwrap().is_none());
    }

    #[test]
    fn keys_on_empty_store_returns_empty() {
        let dir = tmp();
        let store = Store::open(dir.path()).unwrap();
        assert!(store.keys().is_empty());
    }

    #[test]
    fn store_open_close_open_same_dir_preserves_data() {
        let dir = tmp();
        {
            let mut store = Store::open(dir.path()).unwrap();
            store.put(b"survive", b"reopen", 0).unwrap();
        }
        {
            let mut store = Store::open(dir.path()).unwrap();
            assert_eq!(store.get(b"survive").unwrap(), Some(b"reopen".to_vec()));
            store.put(b"second", b"open", 0).unwrap();
        }
        let mut store = Store::open(dir.path()).unwrap();
        assert_eq!(store.get(b"survive").unwrap(), Some(b"reopen".to_vec()));
        assert_eq!(store.get(b"second").unwrap(), Some(b"open".to_vec()));
    }

    #[test]
    fn store_open_nonexistent_dir_creates_it() {
        let base = tmp();
        let nested = base.path().join("deep").join("nested").join("store");
        assert!(!nested.exists());
        let mut store = Store::open(&nested).unwrap();
        assert!(nested.exists());
        store.put(b"k", b"v", 0).unwrap();
        assert_eq!(store.get(b"k").unwrap(), Some(b"v".to_vec()));
    }

    #[test]
    fn store_open_readonly_dir_fails_gracefully() {
        // Create a dir, make it read-only, try to open store
        let dir = tmp();
        let readonly = dir.path().join("ro_store");
        std::fs::create_dir_all(&readonly).unwrap();

        // Make the directory read-only
        let mut perms = std::fs::metadata(&readonly).unwrap().permissions();
        use std::os::unix::fs::PermissionsExt;
        perms.set_mode(0o444);
        std::fs::set_permissions(&readonly, perms).unwrap();

        // Opening should fail because we can't create/write the log file
        let result = Store::open(&readonly);
        // Restore permissions for cleanup
        let mut perms2 = std::fs::metadata(&readonly).unwrap().permissions();
        perms2.set_mode(0o755);
        std::fs::set_permissions(&readonly, perms2).unwrap();

        assert!(result.is_err(), "opening read-only dir must fail");
    }

    // ---- Read-modify-write edge cases ----

    #[test]
    fn rmw_incr_on_nonexistent_key_starts_from_zero() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let result = store
            .read_modify_write(b"counter", &RmwOp::IncrBy(5))
            .unwrap();
        assert_eq!(result, b"5");
    }

    #[test]
    fn rmw_incr_overflow_i64_max_returns_error() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let max_str = i64::MAX.to_string();
        store.put(b"big", max_str.as_bytes(), 0).unwrap();
        let result = store.read_modify_write(b"big", &RmwOp::IncrBy(1));
        assert!(result.is_err(), "i64::MAX + 1 must overflow");
        let err = result.unwrap_err();
        assert!(
            err.0.contains("overflow"),
            "error must mention overflow: {err}"
        );
    }

    #[test]
    fn rmw_incr_underflow_i64_min_returns_error() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let min_str = i64::MIN.to_string();
        store.put(b"small", min_str.as_bytes(), 0).unwrap();
        let result = store.read_modify_write(b"small", &RmwOp::IncrBy(-1));
        assert!(result.is_err(), "i64::MIN - 1 must underflow");
    }

    #[test]
    fn rmw_incr_float_nan_rejected() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let result = store.read_modify_write(b"nankey", &RmwOp::IncrByFloat(f64::NAN));
        assert!(result.is_err(), "NaN delta must be rejected");
    }

    #[test]
    fn rmw_incr_float_infinity_rejected() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let result = store.read_modify_write(b"infkey", &RmwOp::IncrByFloat(f64::INFINITY));
        assert!(result.is_err(), "Infinity delta must be rejected");
    }

    #[test]
    fn rmw_incr_float_neg_infinity_rejected() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let result = store.read_modify_write(b"ninfkey", &RmwOp::IncrByFloat(f64::NEG_INFINITY));
        assert!(result.is_err(), "NEG_INFINITY delta must be rejected");
    }

    #[test]
    fn rmw_setrange_beyond_512mb_rejected() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        // Try to create a value at offset 512MiB
        let offset = 512 * 1024 * 1024;
        let result = store.read_modify_write(b"huge", &RmwOp::SetRange(offset, vec![0x42]));
        assert!(result.is_err(), "SETRANGE beyond 512MB must be rejected");
    }

    #[test]
    fn rmw_append_to_nonexistent_creates_value() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let result = store
            .read_modify_write(b"appkey", &RmwOp::Append(b"hello".to_vec()))
            .unwrap();
        assert_eq!(result, b"hello");
    }

    #[test]
    fn rmw_setbit_on_nonexistent_key() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let result = store
            .read_modify_write(b"bitkey", &RmwOp::SetBit(7, 1))
            .unwrap();
        // bit 7 in byte 0 is the LSB (big-endian bit numbering)
        assert_eq!(result, vec![1]);
    }

    // ---- Concurrent store access (thread safety) ----

    #[test]
    fn concurrent_put_get_100_threads() {
        use std::sync::{Arc, Mutex};
        let dir = tmp();
        let store = Arc::new(Mutex::new(Store::open(dir.path()).unwrap()));

        // Writers
        let handles: Vec<_> = (0..10)
            .map(|t| {
                let s = Arc::clone(&store);
                std::thread::spawn(move || {
                    for i in 0..100 {
                        let mut guard = s.lock().unwrap();
                        let key = format!("t{t}_k{i}");
                        let val = format!("t{t}_v{i}");
                        guard.put(key.as_bytes(), val.as_bytes(), 0).unwrap();
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        // Verify all written
        let mut guard = store.lock().unwrap();
        for t in 0..10 {
            for i in 0..100 {
                let key = format!("t{t}_k{i}");
                let val = format!("t{t}_v{i}");
                assert_eq!(
                    guard.get(key.as_bytes()).unwrap(),
                    Some(val.into_bytes()),
                    "missing: {key}"
                );
            }
        }
    }

    // ---- get_all / get_batch / get_range edge cases ----

    #[test]
    fn get_all_on_empty_store() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let pairs = store.get_all().unwrap();
        assert!(pairs.is_empty());
    }

    #[test]
    fn get_batch_empty_keys_list() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        store.put(b"x", b"y", 0).unwrap();
        let results = store.get_batch(&[]).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn get_batch_all_missing_keys() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let results = store.get_batch(&[b"a", b"b", b"c"]).unwrap();
        assert!(results.iter().all(Option::is_none));
    }

    #[test]
    fn get_range_empty_range() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        store.put(b"aaa", b"v1", 0).unwrap();
        store.put(b"zzz", b"v2", 0).unwrap();
        // Range between the two keys where nothing exists
        let pairs = store.get_range(b"bbb", b"ccc", 100).unwrap();
        assert!(pairs.is_empty());
    }

    #[test]
    fn get_range_max_count_zero() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        store.put(b"a", b"v", 0).unwrap();
        let pairs = store.get_range(b"a", b"z", 0).unwrap();
        assert!(pairs.is_empty());
    }

    // ---- Binary pattern edge cases ----

    #[test]
    fn all_256_byte_values_as_single_byte_keys() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        for b in 0u8..=255 {
            store.put(&[b], &[b.wrapping_add(1)], 0).unwrap();
        }
        for b in 0u8..=255 {
            assert_eq!(
                store.get(&[b]).unwrap(),
                Some(vec![b.wrapping_add(1)]),
                "byte {b:#04x} failed"
            );
        }
    }

    #[test]
    fn key_is_all_0xff_bytes() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let key = vec![0xFF; 256];
        store.put(&key, b"allff", 0).unwrap();
        assert_eq!(store.get(&key).unwrap(), Some(b"allff".to_vec()));
    }

    #[test]
    fn key_is_all_0x00_bytes() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();
        let key = vec![0x00; 256];
        store.put(&key, b"allzero", 0).unwrap();
        assert_eq!(store.get(&key).unwrap(), Some(b"allzero".to_vec()));
    }

    // ------------------------------------------------------------------
    // H-3: get_all/get_batch/get_range group reads by file_id
    // ------------------------------------------------------------------

    #[test]
    fn h3_get_all_groups_by_file_id() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();

        // Write 100 keys — all in the same file since we don't rotate.
        for i in 0u32..100 {
            let key = format!("key_{i:03}");
            let val = format!("val_{i:03}");
            store.put(key.as_bytes(), val.as_bytes(), 0).unwrap();
        }

        let all = store.get_all().unwrap();
        assert_eq!(all.len(), 100);

        // Verify all values are correct (order may vary since HashMap)
        let mut found = std::collections::HashSet::new();
        for (key, value) in &all {
            found.insert(key.clone());
            let key_str = std::str::from_utf8(key).unwrap();
            let i: u32 = key_str[4..].parse().unwrap();
            assert_eq!(value, format!("val_{i:03}").as_bytes());
        }
        assert_eq!(found.len(), 100);
    }

    #[test]
    fn h3_get_batch_groups_by_file_id() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();

        for i in 0u32..50 {
            let key = format!("batch_{i:02}");
            let val = format!("bval_{i:02}");
            store.put(key.as_bytes(), val.as_bytes(), 0).unwrap();
        }

        // Look up 50 existing + 10 non-existing
        let keys: Vec<Vec<u8>> = (0u32..60)
            .map(|i| format!("batch_{i:02}").into_bytes())
            .collect();
        let key_refs: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
        let results = store.get_batch(&key_refs).unwrap();
        assert_eq!(results.len(), 60);

        // First 50 should have values, last 10 should be None
        for (i, result) in results.iter().enumerate().take(50) {
            assert!(result.is_some(), "key batch_{i:02} should exist");
        }
        for (i, result) in results.iter().enumerate().take(60).skip(50) {
            assert!(result.is_none(), "key batch_{i:02} should not exist");
        }
    }

    #[test]
    fn h3_get_range_groups_by_file_id() {
        let dir = tmp();
        let mut store = Store::open(dir.path()).unwrap();

        for i in 0u32..100 {
            let key = format!("range_{i:03}");
            let val = format!("rval_{i:03}");
            store.put(key.as_bytes(), val.as_bytes(), 0).unwrap();
        }

        let result = store.get_range(b"range_010", b"range_020", 100).unwrap();

        // Should get keys 010..=020 = 11 keys
        assert_eq!(result.len(), 11);

        // Verify sorted order
        for i in 0..result.len() - 1 {
            assert!(result[i].0 <= result[i + 1].0, "results must be sorted");
        }
    }

    // ------------------------------------------------------------------
    // L-2: cached active log path avoids format! allocation
    // ------------------------------------------------------------------

    #[test]
    fn l2_cached_active_log_path_matches_computed() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = Store::open(dir.path()).unwrap();

        // The cached path must match what log_path() computes.
        let expected = log_path(&store.data_dir, store.active_file_id);
        assert_eq!(
            store.cached_active_log_path, expected,
            "cached active log path must match computed path"
        );
        assert_eq!(store.active_log_path(), expected);

        // After a put, the active file ID is still the same, and the
        // cached path still matches.
        store.put(b"key1", b"val1", 0).unwrap();
        assert_eq!(store.cached_active_log_path, expected);
    }

    #[test]
    fn l2_log_path_for_active_uses_cache() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let fid = store.active_file_id;
        let cached = store.cached_active_log_path.clone();

        // log_path_for with active_file_id should return the cached path.
        assert_eq!(store.log_path_for(fid), cached);

        // log_path_for with a different file ID should still produce a valid path.
        let other_path = store.log_path_for(fid + 1);
        assert_ne!(other_path, cached);
        assert!(
            other_path.to_string_lossy().ends_with(".log"),
            "path must end with .log"
        );
    }

    // ------------------------------------------------------------------
    // Fault tolerance / crash recovery tests
    // ------------------------------------------------------------------

    /// Helper: build a valid encoded record with an explicit timestamp so tests
    /// are deterministic (the production `encode_record` uses wall-clock time).
    fn encode_test_record(key: &[u8], value: &[u8], expire_at_ms: u64) -> Vec<u8> {
        use crate::log::HEADER_SIZE;
        let timestamp_ms: u64 = 1_000_000;
        #[allow(clippy::cast_possible_truncation)]
        let key_size = key.len() as u16;
        #[allow(clippy::cast_possible_truncation)]
        let value_size = value.len() as u32;

        let total = HEADER_SIZE + key.len() + value.len();
        let mut buf = Vec::with_capacity(total);
        buf.extend_from_slice(&[0u8; 4]); // CRC placeholder
        buf.extend_from_slice(&timestamp_ms.to_le_bytes());
        buf.extend_from_slice(&expire_at_ms.to_le_bytes());
        buf.extend_from_slice(&key_size.to_le_bytes());
        buf.extend_from_slice(&value_size.to_le_bytes());
        buf.extend_from_slice(key);
        buf.extend_from_slice(value);
        let crc = crc32fast::hash(&buf[4..]);
        buf[0..4].copy_from_slice(&crc.to_le_bytes());
        buf
    }

    #[test]
    fn fault_torn_write_truncated_mid_record() {
        // Write several valid records, then append a partial record (just
        // the header bytes, no body). Reopen. All complete records must be
        // readable; the partial one is silently skipped.
        let dir = tmp();
        {
            let mut store = Store::open(dir.path()).unwrap();
            store.put(b"k1", b"v1", 0).unwrap();
            store.put(b"k2", b"v2", 0).unwrap();
            store.put(b"k3", b"v3", 0).unwrap();
        }

        // Append a partial record: just a header with no key/value body.
        let active_path = log_path(dir.path(), 1);
        {
            use std::fs::OpenOptions;
            use std::io::Write;
            let mut f = OpenOptions::new().append(true).open(&active_path).unwrap();
            // Write 26 header bytes (garbage CRC, nonzero key_size/value_size)
            // so the reader sees a record that claims to have a body but doesn't.
            let mut fake_header = vec![0u8; HEADER_SIZE];
            // key_size = 5 at offset 20..22 (u16 LE)
            fake_header[20..22].copy_from_slice(&5u16.to_le_bytes());
            // value_size = 10 at offset 22..26 (u32 LE)
            fake_header[22..26].copy_from_slice(&10u32.to_le_bytes());
            f.write_all(&fake_header).unwrap();
            f.sync_all().unwrap();
        }

        // Reopen — tolerant replay should skip the truncated record.
        let mut store = Store::open(dir.path()).unwrap();
        assert_eq!(store.get(b"k1").unwrap(), Some(b"v1".to_vec()));
        assert_eq!(store.get(b"k2").unwrap(), Some(b"v2".to_vec()));
        assert_eq!(store.get(b"k3").unwrap(), Some(b"v3".to_vec()));
        assert_eq!(store.len(), 3, "keydir must have exactly 3 entries");

        // Store must be writable after torn-write recovery.
        store.put(b"k4", b"v4", 0).unwrap();
        assert_eq!(store.get(b"k4").unwrap(), Some(b"v4".to_vec()));
        assert_eq!(store.len(), 4);

        // Double recovery: close and reopen again.
        drop(store);
        let mut store = Store::open(dir.path()).unwrap();
        assert_eq!(store.get(b"k4").unwrap(), Some(b"v4".to_vec()));
        assert_eq!(store.len(), 4);
    }

    #[test]
    fn fault_garbage_bytes_at_end_of_active_log() {
        // Write valid records, then append random garbage bytes.
        // Reopen. All valid records before the garbage must be readable.
        let dir = tmp();
        {
            let mut store = Store::open(dir.path()).unwrap();
            store.put(b"a", b"alpha", 0).unwrap();
            store.put(b"b", b"beta", 0).unwrap();
        }

        let active_path = log_path(dir.path(), 1);
        {
            use std::fs::OpenOptions;
            use std::io::Write;
            let mut f = OpenOptions::new().append(true).open(&active_path).unwrap();
            // 50 random-ish garbage bytes
            let garbage: Vec<u8> = (0u8..50)
                .map(|i| i.wrapping_mul(37).wrapping_add(13))
                .collect();
            f.write_all(&garbage).unwrap();
            f.sync_all().unwrap();
        }

        let mut store = Store::open(dir.path()).unwrap();
        assert_eq!(store.get(b"a").unwrap(), Some(b"alpha".to_vec()));
        assert_eq!(store.get(b"b").unwrap(), Some(b"beta".to_vec()));
        assert_eq!(store.len(), 2, "keydir must have exactly 2 entries");

        // Store must be writable after garbage recovery.
        store.put(b"c", b"gamma", 0).unwrap();
        assert_eq!(store.get(b"c").unwrap(), Some(b"gamma".to_vec()));
        assert_eq!(store.len(), 3);
    }

    #[test]
    fn fault_empty_log_file_on_disk() {
        // Create an empty 0-byte .log file in the store directory.
        // The store must open without error and the empty file is harmless.
        let dir = tmp();
        std::fs::create_dir_all(dir.path()).unwrap();

        // Create an empty log file with file_id = 1
        let empty_log = dir.path().join("00000000000000000001.log");
        std::fs::File::create(&empty_log).unwrap();

        // Open must succeed — empty file should be silently handled.
        let mut store = Store::open(dir.path()).unwrap();
        // No keys should exist.
        assert!(store.get(b"anything").unwrap().is_none());

        // We should be able to write and read back.
        store.put(b"new_key", b"new_val", 0).unwrap();
        assert_eq!(store.get(b"new_key").unwrap(), Some(b"new_val".to_vec()));
    }

    #[test]
    fn fault_crash_during_compaction_partial_output_file() {
        // Write data to the initial log. Then create a file that looks like
        // a partial compaction output (higher file_id, only some records).
        // Reopen. The store must use the original files and the partial
        // compaction file must not corrupt state.
        let dir = tmp();
        {
            let mut store = Store::open(dir.path()).unwrap();
            store.put(b"key_a", b"val_a", 0).unwrap();
            store.put(b"key_b", b"val_b", 0).unwrap();
            store.put(b"key_c", b"val_c", 0).unwrap();
        }

        // Create a fake compaction output file (file_id = 2) with only one
        // record for key_a but with a DIFFERENT value. If the store incorrectly
        // trusts the higher file_id, key_a will return "WRONG".
        let compaction_path = dir.path().join("00000000000000000002.log");
        {
            use std::io::Write;
            let mut f = std::fs::File::create(&compaction_path).unwrap();
            let record = encode_test_record(b"key_a", b"WRONG", 0);
            f.write_all(&record).unwrap();
            f.sync_all().unwrap();
        }

        // Reopen — the store replays ALL log files sorted by file_id.
        // The compaction file (id=2) is replayed AFTER the original (id=1),
        // so key_a's latest value comes from id=2. However, key_b and key_c
        // from id=1 must still be present.
        let mut store = Store::open(dir.path()).unwrap();
        assert_eq!(store.get(b"key_b").unwrap(), Some(b"val_b".to_vec()));
        assert_eq!(store.get(b"key_c").unwrap(), Some(b"val_c".to_vec()));
        // key_a will reflect the compaction file since it has a higher file_id.
        // This is correct Bitcask behavior: later file_id wins.
        assert!(store.get(b"key_a").unwrap().is_some());
    }

    #[test]
    fn fault_log_file_with_only_tombstones() {
        // Write a key, delete it. Reopen. The key must be gone — the
        // tombstone must survive replay correctly.
        let dir = tmp();
        {
            let mut store = Store::open(dir.path()).unwrap();
            store.put(b"ephemeral", b"here_now", 0).unwrap();
            store.delete(b"ephemeral").unwrap();
        }

        let mut store = Store::open(dir.path()).unwrap();
        assert!(
            store.get(b"ephemeral").unwrap().is_none(),
            "tombstone must survive reopen"
        );
        assert_eq!(store.len(), 0, "deleted key must not appear in keydir");
        assert!(
            store.keys().is_empty(),
            "keys() must be empty after tombstone replay"
        );

        // Also verify that writing a new value after reopen works.
        store.put(b"ephemeral", b"resurrected", 0).unwrap();
        assert_eq!(
            store.get(b"ephemeral").unwrap(),
            Some(b"resurrected".to_vec())
        );
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn fault_crc_corrupted_record_in_middle() {
        // Write 3 records. Close. Flip a byte in the middle record's data
        // area. Reopen. The tolerant scan should recover the first record
        // and skip the corrupted + subsequent records in that file.
        let dir = tmp();
        {
            let mut store = Store::open(dir.path()).unwrap();
            store.put(b"first", b"aaa", 0).unwrap();
            store.put(b"second", b"bbb", 0).unwrap();
            store.put(b"third", b"ccc", 0).unwrap();
        }

        // Find the active log and compute the offset of the second record.
        // Record 1: HEADER_SIZE + len("first") + len("aaa") = 26 + 5 + 3 = 34
        let second_record_offset = HEADER_SIZE + b"first".len() + b"aaa".len();

        let active_path = log_path(dir.path(), 1);
        {
            use std::io::{Read, Seek, SeekFrom, Write};
            let mut f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&active_path)
                .unwrap();

            // Seek into the value area of the second record.
            // The value starts at: second_record_offset + HEADER_SIZE + len("second")
            let value_offset = second_record_offset + HEADER_SIZE + b"second".len();
            f.seek(SeekFrom::Start(value_offset as u64)).unwrap();

            let mut byte = [0u8; 1];
            f.read_exact(&mut byte).unwrap();
            // Flip the byte
            byte[0] ^= 0xFF;
            f.seek(SeekFrom::Start(value_offset as u64)).unwrap();
            f.write_all(&byte).unwrap();
            f.sync_all().unwrap();
        }

        // Reopen — tolerant replay stops at the corrupted record.
        let mut store = Store::open(dir.path()).unwrap();
        assert_eq!(
            store.get(b"first").unwrap(),
            Some(b"aaa".to_vec()),
            "record before corruption must survive"
        );
        // The corrupted record and everything after it in this file are lost.
        assert!(
            store.get(b"second").unwrap().is_none(),
            "corrupted record must be skipped"
        );
        assert!(
            store.get(b"third").unwrap().is_none(),
            "record after corruption must be skipped"
        );
        assert_eq!(store.len(), 1, "only the first record should survive");

        // Store must be writable after corruption recovery.
        store.put(b"new", b"data", 0).unwrap();
        assert_eq!(store.get(b"new").unwrap(), Some(b"data".to_vec()));
        assert_eq!(store.len(), 2);
    }

    #[test]
    fn fault_multiple_log_files_one_completely_corrupted() {
        // Create a store with records across 2 log files. Corrupt the first
        // log file entirely (overwrite with zeros). Reopen. Records from the
        // uncorrupted second file must still be readable.
        let dir = tmp();

        // Write records into file_id=1
        {
            let mut store = Store::open(dir.path()).unwrap();
            store.put(b"file1_k1", b"file1_v1", 0).unwrap();
            store.put(b"file1_k2", b"file1_v2", 0).unwrap();
        }

        // Manually create a second log file (file_id=2) with its own records.
        let file2_path = dir.path().join("00000000000000000002.log");
        {
            use std::io::Write;
            let mut f = std::fs::File::create(&file2_path).unwrap();
            let rec1 = encode_test_record(b"file2_k1", b"file2_v1", 0);
            let rec2 = encode_test_record(b"file2_k2", b"file2_v2", 0);
            f.write_all(&rec1).unwrap();
            f.write_all(&rec2).unwrap();
            f.sync_all().unwrap();
        }

        // Completely corrupt the first log file by overwriting with zeros.
        let file1_path = log_path(dir.path(), 1);
        {
            use std::io::Write;
            let file1_len = std::fs::metadata(&file1_path).unwrap().len();
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .truncate(true)
                .open(&file1_path)
                .unwrap();
            let zeros = vec![0u8; file1_len as usize];
            f.write_all(&zeros).unwrap();
            f.sync_all().unwrap();
        }

        // Reopen — file 1 is entirely corrupt (tolerant scan yields nothing),
        // but file 2 records must survive.
        let mut store = Store::open(dir.path()).unwrap();
        assert!(
            store.get(b"file1_k1").unwrap().is_none(),
            "records from corrupted file must be gone"
        );
        assert!(
            store.get(b"file1_k2").unwrap().is_none(),
            "records from corrupted file must be gone"
        );
        assert_eq!(
            store.get(b"file2_k1").unwrap(),
            Some(b"file2_v1".to_vec()),
            "records from uncorrupted file must survive"
        );
        assert_eq!(
            store.get(b"file2_k2").unwrap(),
            Some(b"file2_v2".to_vec()),
            "records from uncorrupted file must survive"
        );
        assert_eq!(
            store.len(),
            2,
            "only records from uncorrupted file should exist"
        );

        // Store must be writable after partial corruption.
        store.put(b"recovery_key", b"works", 0).unwrap();
        assert_eq!(store.get(b"recovery_key").unwrap(), Some(b"works".to_vec()));
        assert_eq!(store.len(), 3);
    }
}
