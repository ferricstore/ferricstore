
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

