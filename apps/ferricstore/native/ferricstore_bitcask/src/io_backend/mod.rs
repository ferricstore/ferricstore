//! I/O backend abstraction for the Bitcask log writer.
//!
//! `IoBackend` is a trait that abstracts over two implementations:
//! - `SyncBackend`: standard `BufWriter<File>` — works on all platforms
//! - `UringBackend` (Linux only): `io_uring` via the `io-uring` crate
//!
//! The selection happens at runtime via `create_backend`. On macOS the
//! `io-uring` crate is not even compiled (it is behind a
//! `cfg(target_os = "linux")` dependency gate), so the macro-expansion of
//! `create_backend` always returns `SyncBackend` on non-Linux targets.
//!
//! All methods block the calling thread until I/O is complete. This matches
//! the BEAM dirty-scheduler contract: the NIF occupies a dirty thread for
//! the full duration of the write, but never blocks a normal scheduler.

use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError, Weak};

// On Linux, bring in the synchronous uring backend.
#[cfg(target_os = "linux")]
pub mod uring;

/// Synchronous, blocking I/O interface for the append-only log.
///
/// Implementors must block until all bytes are durable when `sync` returns.
/// The `offset` method must accurately reflect the number of bytes written
/// since the file was opened (i.e. the current end-of-file position).
pub trait IoBackend: Send {
    /// Append `data` to the file. Returns the byte offset where the write
    /// started (i.e. the value callers should store as the record's offset
    /// in the keydir).
    ///
    /// If this returns an error, the implementation must roll back any bytes
    /// written by this call before returning. Callers may have a stale cached
    /// offset because multiple backend instances can append to the same file.
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` if the write fails.
    fn append(&mut self, data: &[u8]) -> io::Result<u64>;

    /// Flush any internal write buffer, then fsync the underlying file to
    /// durable storage.
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` if the flush or fsync fails.
    fn sync(&mut self) -> io::Result<()>;

    /// Current write offset (the byte position immediately after the last
    /// appended byte). This equals the file size at the time of the last
    /// successful `append`.
    fn offset(&self) -> u64;

    /// Discard every byte at or after `offset` and durably publish the shorter
    /// file length. This is used only on failed append transactions.
    fn rollback_to(&mut self, offset: u64) -> io::Result<()>;

    /// Flush any internal write buffer to the OS page cache **without**
    /// calling fsync. This makes the data visible to subsequent reads via
    /// pread but does not guarantee durability on crash.
    ///
    /// The default implementation calls `sync()` (flush + fsync). Backends
    /// that can flush without fsync (like `SyncBackend`) should override this
    /// for better performance on the write+deferred-fsync path.
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` if the flush fails.
    fn flush_no_sync(&mut self) -> io::Result<()> {
        self.sync()
    }

    /// Append multiple buffers as a single atomic batch, then fsync once.
    ///
    /// Implementations must hold their file-level append lock across the full
    /// append + sync transaction. Building this from separate `append` calls
    /// is unsafe because another backend instance can interleave bytes before
    /// rollback.
    ///
    /// Returns the starting offset of each buffer in the same order as the
    /// input slice.
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` if any write or the final sync fails.
    fn append_batch_and_sync(&mut self, buffers: &[&[u8]]) -> io::Result<Vec<u64>>;

    fn append_and_sync(&mut self, data: &[u8]) -> io::Result<u64> {
        self.append_batch_and_sync(&[data])
            .map(|offsets| offsets[0])
    }
}

pub(super) fn rollback_failure(cause: io::Error, rollback: io::Error) -> io::Error {
    io::Error::new(
        cause.kind(),
        format!("append failed: {cause}; durable rollback failed: {rollback}"),
    )
}

pub(super) fn checked_append_end(offset: u64, len: usize) -> io::Result<u64> {
    let len = u64::try_from(len).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "append length exceeds the file-offset domain",
        )
    })?;
    offset
        .checked_add(len)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "append file offset overflow"))
}

/// Validate one submitted io_uring write batch while consuming every CQE.
/// Draining is required even after the first error because the ring is reused.
#[cfg(any(test, target_os = "linux"))]
pub(super) fn validate_uring_batch_completions(
    completions: impl IntoIterator<Item = (u64, i32)>,
    expected_lengths: &[usize],
) -> io::Result<()> {
    let mut seen = vec![false; expected_lengths.len()];
    let mut completed = 0usize;
    let mut first_error = None;

    for (tag, result) in completions {
        completed = completed.saturating_add(1);

        let Ok(index) = usize::try_from(tag) else {
            first_error.get_or_insert_with(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "io_uring completion index exceeds platform size",
                )
            });
            continue;
        };

        let Some(&expected_len) = expected_lengths.get(index) else {
            first_error.get_or_insert_with(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("io_uring returned unknown completion index {index}"),
                )
            });
            continue;
        };

        if std::mem::replace(&mut seen[index], true) {
            first_error.get_or_insert_with(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("io_uring returned duplicate completion index {index}"),
                )
            });
            continue;
        }

        if result < 0 {
            first_error.get_or_insert_with(|| io::Error::from_raw_os_error(-result));
        } else if usize::try_from(result).ok() != Some(expected_len) {
            first_error.get_or_insert_with(|| {
                io::Error::new(
                    io::ErrorKind::WriteZero,
                    format!(
                        "io_uring short write: expected {expected_len} B for entry {index}, wrote {result} B"
                    ),
                )
            });
        }
    }

    if completed != expected_lengths.len() {
        first_error.get_or_insert_with(|| {
            io::Error::other(format!(
                "io_uring batch: expected {} completions, got {completed}",
                expected_lengths.len()
            ))
        });
    }

    first_error.map_or(Ok(()), Err)
}

/// Validate one synchronous io_uring operation while draining every CQE.
///
/// A duplicate or stale CQE must not remain in the reusable ring where the
/// next operation could mistake it for its own completion.
#[cfg(any(test, target_os = "linux"))]
pub(super) fn validate_uring_single_completion(
    completions: impl IntoIterator<Item = (u64, i32)>,
    expected_tag: u64,
    expected_result: usize,
    operation: &str,
) -> io::Result<()> {
    let mut completed = 0usize;
    let mut first_error = None;

    for (tag, result) in completions {
        completed = completed.saturating_add(1);

        if tag != expected_tag {
            first_error.get_or_insert_with(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "io_uring {operation} returned completion tag {tag}, expected {expected_tag}"
                    ),
                )
            });
            continue;
        }

        if result < 0 {
            first_error.get_or_insert_with(|| io::Error::from_raw_os_error(-result));
        } else if usize::try_from(result).ok() != Some(expected_result) {
            first_error.get_or_insert_with(|| {
                io::Error::new(
                    io::ErrorKind::WriteZero,
                    format!(
                        "io_uring {operation}: expected result {expected_result}, got {result}"
                    ),
                )
            });
        }
    }

    if completed != 1 {
        first_error.get_or_insert_with(|| {
            io::Error::other(format!(
                "io_uring {operation}: expected one completion, got {completed}"
            ))
        });
    }

    first_error.map_or(Ok(()), Err)
}

type AppendLock = Arc<Mutex<()>>;

#[derive(Debug)]
struct AppendLockKey {
    #[cfg(any(test, not(unix)))]
    path: std::path::PathBuf,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
}

impl PartialEq for AppendLockKey {
    fn eq(&self, other: &Self) -> bool {
        #[cfg(unix)]
        {
            self.device == other.device && self.inode == other.inode
        }
        #[cfg(not(unix))]
        {
            self.path == other.path
        }
    }
}

impl Eq for AppendLockKey {}

impl std::hash::Hash for AppendLockKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        #[cfg(unix)]
        {
            self.device.hash(state);
            self.inode.hash(state);
        }
        #[cfg(not(unix))]
        self.path.hash(state);
    }
}

fn append_lock_key(path: &Path, file: &File) -> io::Result<AppendLockKey> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;

        #[cfg(not(test))]
        let _ = path;
        let metadata = file.metadata()?;
        Ok(AppendLockKey {
            #[cfg(test)]
            path: path.to_path_buf(),
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }
    #[cfg(not(unix))]
    {
        let _ = file;
        Ok(AppendLockKey {
            path: path.canonicalize().unwrap_or_else(|_| path.to_path_buf()),
        })
    }
}

static APPEND_LOCKS: OnceLock<Mutex<HashMap<AppendLockKey, Weak<Mutex<()>>>>> = OnceLock::new();

pub(crate) fn append_lock_for_file(path: &Path, file: &File) -> io::Result<AppendLock> {
    let key = append_lock_key(path, file)?;
    let locks = APPEND_LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut locks = locks.lock().unwrap_or_else(PoisonError::into_inner);

    if let Some(lock) = locks.get(&key).and_then(Weak::upgrade) {
        return Ok(lock);
    }

    locks.retain(|_, lock| lock.strong_count() > 0);
    let lock = Arc::new(Mutex::new(()));
    locks.insert(key, Arc::downgrade(&lock));
    Ok(lock)
}

pub(crate) fn lock_append(lock: &AppendLock) -> io::Result<MutexGuard<'_, ()>> {
    lock.lock()
        .map_err(|_| io::Error::other("append lock poisoned"))
}

// ---------------------------------------------------------------------------
// SyncBackend — standard BufWriter<File>, always available
// ---------------------------------------------------------------------------

/// Standard synchronous I/O backend using `BufWriter<File>`.
///
/// This is the fallback backend used on all platforms. On macOS (and any
/// platform where `io_uring` is unavailable) this is the only backend.
pub struct SyncBackend {
    writer: Option<BufWriter<File>>,
    offset: u64,
    append_lock: AppendLock,
}

impl SyncBackend {
    /// Open (or create) the file at `path` for appending with a 256KB buffer.
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` if the file cannot be opened or its size cannot
    /// be determined.
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = crate::open_append_nofollow(path)?;
        let offset = file.metadata()?.len();
        let append_lock = append_lock_for_file(path, &file)?;
        Ok(Self {
            writer: Some(BufWriter::with_capacity(256 * 1024, file)), // H-1: 256KB buffer for batch writes
            offset,
            append_lock,
        })
    }

    /// Open (or create) the file at `path` for appending with an 8KB buffer.
    ///
    /// M-NEW-1 fix: the v2 stateless NIF path opens a LogWriter per call and
    /// drops it after a single write. The 256KB BufWriter is wasteful for this
    /// use case. An 8KB buffer (Rust's default BufWriter capacity) is large
    /// enough for any single Bitcask record (max key 64KB + max value 1MB,
    /// but the buffer flushes on write_all anyway when data exceeds capacity).
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` if the file cannot be opened or its size cannot
    /// be determined.
    pub fn open_small_buffer(path: &Path) -> io::Result<Self> {
        let file = crate::open_append_nofollow(path)?;
        let offset = file.metadata()?.len();
        let append_lock = append_lock_for_file(path, &file)?;
        Ok(Self {
            writer: Some(BufWriter::with_capacity(8 * 1024, file)),
            offset,
            append_lock,
        })
    }
}

impl IoBackend for SyncBackend {
    fn append(&mut self, data: &[u8]) -> io::Result<u64> {
        let append_lock = Arc::clone(&self.append_lock);
        let _guard = lock_append(&append_lock)?;
        let start = self.writer().get_ref().metadata()?.len();
        let end = checked_append_end(start, data.len())?;

        let result = (|| {
            self.writer().flush()?;
            self.writer().write_all(data)?;
            self.writer().flush()?;
            Ok(start)
        })();

        match result {
            Ok(start) => {
                self.offset = end;
                Ok(start)
            }
            Err(cause) => match self.rollback_locked(start) {
                Ok(()) => Err(cause),
                Err(rollback) => Err(rollback_failure(cause, rollback)),
            },
        }
    }

    /// C-7 fix: use `sync_data()` (`fdatasync`) instead of `sync_all()` (`fsync`).
    /// `fdatasync` skips flushing non-critical metadata (mtime, atime) which
    /// avoids an extra journal write on ext4/xfs, making it 2-10x faster on HDD
    /// and 5-50us faster on NVMe. For append-only logs the file size is the only
    /// critical metadata, and `fdatasync` syncs size changes on Linux.
    fn sync(&mut self) -> io::Result<()> {
        self.writer().flush()?;
        self.writer().get_ref().sync_data()?;
        Ok(())
    }

    fn flush_no_sync(&mut self) -> io::Result<()> {
        self.writer().flush()?;
        Ok(())
    }

    fn offset(&self) -> u64 {
        self.offset
    }

    fn rollback_to(&mut self, offset: u64) -> io::Result<()> {
        let append_lock = Arc::clone(&self.append_lock);
        let _guard = lock_append(&append_lock)?;
        self.rollback_locked(offset)
    }

    fn append_batch_and_sync(&mut self, buffers: &[&[u8]]) -> io::Result<Vec<u64>> {
        let append_lock = Arc::clone(&self.append_lock);
        let _guard = lock_append(&append_lock)?;
        let start = self.writer().get_ref().metadata()?.len();
        let mut running = start;
        let mut offsets = Vec::with_capacity(buffers.len());

        for buf in buffers {
            running = checked_append_end(running, buf.len())?;
        }
        let end = running;
        running = start;

        let result = (|| {
            self.writer().flush()?;

            for buf in buffers {
                offsets.push(running);
                self.writer().write_all(buf)?;
                running = checked_append_end(running, buf.len())?;
            }

            self.writer().flush()?;
            self.writer().get_ref().sync_data()?;
            Ok(offsets)
        })();

        match result {
            Ok(offsets) => {
                self.offset = end;
                Ok(offsets)
            }
            Err(cause) => match self.rollback_locked(start) {
                Ok(()) => Err(cause),
                Err(rollback) => Err(rollback_failure(cause, rollback)),
            },
        }
    }
}

impl SyncBackend {
    fn writer(&mut self) -> &mut BufWriter<File> {
        self.writer.as_mut().expect("SyncBackend writer missing")
    }

    fn rollback_locked(&mut self, offset: u64) -> io::Result<()> {
        let writer = self.writer.take().expect("SyncBackend writer missing");
        let capacity = writer.capacity();
        let (file, _buffer) = writer.into_parts();
        let rollback_result = file.set_len(offset).and_then(|()| file.sync_data());
        self.writer = Some(BufWriter::with_capacity(capacity, file));

        if rollback_result.is_ok() {
            self.offset = offset;
        }

        rollback_result
    }
}

// ---------------------------------------------------------------------------
// Backend factory
// ---------------------------------------------------------------------------

/// Detect whether `io_uring` is available on this system.
///
/// On non-Linux platforms this always returns `false`. On Linux it probes the
/// kernel by attempting to create a minimal ring. Returns `false` if the
/// kernel is too old (< 5.1), if `io_uring` is disabled by seccomp policy,
/// or if it is otherwise unavailable.
#[must_use]
pub fn detect_io_uring() -> bool {
    #[cfg(target_os = "linux")]
    {
        io_uring::IoUring::<io_uring::squeue::Entry, io_uring::cqueue::Entry>::builder()
            .build(1)
            .is_ok()
    }
    #[cfg(not(target_os = "linux"))]
    {
        false
    }
}

/// Create the best available `IoBackend` for `path`.
///
/// On Linux with a kernel that supports `io_uring` (≥ 5.1), returns a
/// `UringBackend`. Otherwise returns a `SyncBackend`.
///
/// # Errors
///
/// Returns an `io::Error` if the file cannot be opened.
pub fn create_backend(path: &Path) -> io::Result<Box<dyn IoBackend>> {
    #[cfg(target_os = "linux")]
    if detect_io_uring() {
        if let Ok(backend) = uring::UringBackend::open(path) {
            return Ok(Box::new(backend));
            // Ring probe succeeded but open failed (unlikely): fall through.
        }
    }

    Ok(Box::new(SyncBackend::open(path)?))
}

/// Create a backend with a smaller write buffer (8KB instead of 256KB).
///
/// M-NEW-1 fix: the v2 stateless NIF path opens a LogWriter per call and
/// drops it immediately after one write. Using a 256KB BufWriter per call
/// wastes allocator bandwidth (256KB alloc + dealloc per NIF call). This
/// factory uses an 8KB buffer which is sufficient for single-record writes.
///
/// On Linux with `io_uring`, falls back to `create_backend` since the uring
/// backend does not use `BufWriter`.
///
/// # Errors
///
/// Returns an `io::Error` if the file cannot be opened.
pub fn create_backend_small(path: &Path) -> io::Result<Box<dyn IoBackend>> {
    #[cfg(target_os = "linux")]
    if detect_io_uring() {
        if let Ok(backend) = uring::UringBackend::open(path) {
            return Ok(Box::new(backend));
        }
    }

    Ok(Box::new(SyncBackend::open_small_buffer(path)?))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn tmp() -> TempDir {
        tempfile::TempDir::new().unwrap()
    }

    #[test]
    fn detect_io_uring_returns_bool_without_panic() {
        // Just ensure the function runs without panicking on any platform.
        let _ = detect_io_uring();
    }

    #[test]
    fn append_end_rejects_file_offset_overflow() {
        let error = checked_append_end(u64::MAX, 1).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn sync_backend_append_and_sync() {
        let dir = tmp();
        let path = dir.path().join("test.log");
        let mut backend = SyncBackend::open(&path).unwrap();

        let off0 = backend.append(b"hello").unwrap();
        let off1 = backend.append(b"world").unwrap();
        backend.sync().unwrap();

        assert_eq!(off0, 0);
        assert_eq!(off1, 5);
        assert_eq!(backend.offset(), 10);
    }

    #[test]
    fn sync_backend_offset_resumes_after_reopen() {
        let dir = tmp();
        let path = dir.path().join("test.log");

        {
            let mut backend = SyncBackend::open(&path).unwrap();
            backend.append(b"data").unwrap();
            backend.sync().unwrap();
        }

        let backend = SyncBackend::open(&path).unwrap();
        assert_eq!(
            backend.offset(),
            4,
            "offset must resume at file size after reopen"
        );
    }

    #[cfg(unix)]
    #[test]
    fn sync_backend_rejects_a_final_component_symlink() {
        use std::os::unix::fs::symlink;

        let dir = tmp();
        let target = dir.path().join("outside.log");
        let link = dir.path().join("active.log");
        std::fs::write(&target, b"outside").unwrap();
        symlink(&target, &link).unwrap();

        assert!(SyncBackend::open(&link).is_err());
        assert_eq!(std::fs::read(target).unwrap(), b"outside");
    }

    #[test]
    fn sync_backend_batch_writes_offsets_are_correct() {
        let dir = tmp();
        let path = dir.path().join("test.log");
        let mut backend = SyncBackend::open(&path).unwrap();

        let bufs: &[&[u8]] = &[b"aaa", b"bb", b"c"];
        let offsets = backend.append_batch_and_sync(bufs).unwrap();

        assert_eq!(offsets, vec![0, 3, 5]);
        assert_eq!(backend.offset(), 6);
    }

    #[test]
    fn create_backend_returns_a_working_backend() {
        let dir = tmp();
        let path = dir.path().join("test.log");
        let mut backend = create_backend(&path).unwrap();

        let off = backend.append(b"test").unwrap();
        backend.sync().unwrap();

        assert_eq!(off, 0);
        assert_eq!(backend.offset(), 4);
    }

    // ------------------------------------------------------------------
    // SyncBackend: additional coverage
    // ------------------------------------------------------------------

    /// A newly created file starts at offset 0.
    #[test]
    fn sync_backend_offset_is_zero_for_new_file() {
        let dir = tmp();
        let path = dir.path().join("newfile.log");
        let backend = SyncBackend::open(&path).unwrap();
        assert_eq!(backend.offset(), 0, "offset must be 0 for a brand-new file");
    }

    /// Three appends of 10, 20, 30 bytes result in offset = 60.
    #[test]
    fn sync_backend_offset_accumulates_correctly() {
        let dir = tmp();
        let path = dir.path().join("accum.log");
        let mut backend = SyncBackend::open(&path).unwrap();

        let data10 = vec![0u8; 10];
        let data20 = vec![1u8; 20];
        let data30 = vec![2u8; 30];

        let off0 = backend.append(&data10).unwrap();
        assert_eq!(off0, 0, "first append must start at 0");
        assert_eq!(
            backend.offset(),
            10,
            "offset must be 10 after 10-byte append"
        );

        let off1 = backend.append(&data20).unwrap();
        assert_eq!(off1, 10, "second append must start at 10");
        assert_eq!(
            backend.offset(),
            30,
            "offset must be 30 after 20-byte append"
        );

        let off2 = backend.append(&data30).unwrap();
        assert_eq!(off2, 30, "third append must start at 30");
        assert_eq!(backend.offset(), 60, "final offset must be 60");
    }

    /// After append + sync, the data is readable from disk starting at offset 0.
    #[test]
    fn sync_backend_data_is_readable_after_sync() {
        let dir = tmp();
        let path = dir.path().join("readable.log");
        let mut backend = SyncBackend::open(&path).unwrap();

        backend.append(b"hello").unwrap();
        backend.sync().unwrap();

        let contents = std::fs::read(&path).unwrap();
        assert_eq!(
            contents, b"hello",
            "data must be readable from disk after sync"
        );
    }

    /// `append_batch_and_sync` with an empty slice returns an empty offsets vec.
    #[test]
    fn sync_backend_batch_empty_is_ok() {
        let dir = tmp();
        let path = dir.path().join("empty_batch.log");
        let mut backend = SyncBackend::open(&path).unwrap();

        let offsets = backend.append_batch_and_sync(&[]).unwrap();
        assert!(
            offsets.is_empty(),
            "empty batch must return empty offset vec"
        );
        assert_eq!(
            backend.offset(),
            0,
            "offset must remain 0 after empty batch"
        );
    }

    /// Batch ["abc", "de", "f"] → starting offsets [0, 3, 5].
    #[test]
    fn sync_backend_batch_offsets_match_cumulative_lengths() {
        let dir = tmp();
        let path = dir.path().join("cumulative.log");
        let mut backend = SyncBackend::open(&path).unwrap();

        let bufs: &[&[u8]] = &[b"abc", b"de", b"f"];
        let offsets = backend.append_batch_and_sync(bufs).unwrap();

        assert_eq!(
            offsets,
            vec![0, 3, 5],
            "batch offsets must match cumulative lengths"
        );
        assert_eq!(backend.offset(), 6, "final offset must be 6 (3+2+1)");
    }

    /// `flush_no_sync` flushes BufWriter without calling fsync.
    #[test]
    fn sync_backend_flush_no_sync_makes_data_readable() {
        let dir = tmp();
        let path = dir.path().join("nosync.log");
        let mut backend = SyncBackend::open(&path).unwrap();

        backend.append(b"hello").unwrap();
        backend.flush_no_sync().unwrap();

        // Data should be readable from disk (flushed to page cache).
        let contents = std::fs::read(&path).unwrap();
        assert_eq!(
            contents, b"hello",
            "data must be readable after flush_no_sync"
        );
    }

    /// `flush_no_sync` followed by `sync` should still work correctly.
    #[test]
    fn sync_backend_flush_no_sync_then_sync() {
        let dir = tmp();
        let path = dir.path().join("nosync_then_sync.log");
        let mut backend = SyncBackend::open(&path).unwrap();

        backend.append(b"part1").unwrap();
        backend.flush_no_sync().unwrap();
        backend.append(b"part2").unwrap();
        backend.sync().unwrap();

        let contents = std::fs::read(&path).unwrap();
        assert_eq!(contents, b"part1part2");
        assert_eq!(backend.offset(), 10);
    }

    #[test]
    fn uring_backend_overrides_flush_no_sync() {
        let source = std::fs::read_to_string("src/io_backend/uring.rs").unwrap();

        assert!(
            source.contains("fn flush_no_sync(&mut self) -> io::Result<()>"),
            "UringBackend must override flush_no_sync; the IoBackend default calls sync()/fdatasync"
        );
    }

    // ------------------------------------------------------------------
    // H-1: BufWriter uses 256KB buffer instead of default 8KB
    // ------------------------------------------------------------------

    /// H-1 verification: a batch of 100 records (each ~300 bytes = 30KB total)
    /// should be absorbed by the 256KB buffer in a single flush, producing
    /// correct data on disk.
    #[test]
    fn h1_large_batch_fits_in_buffer() {
        let dir = tmp();
        let path = dir.path().join("h1_batch.log");
        let mut backend = SyncBackend::open(&path).unwrap();

        // Write 100 records of 300 bytes each = 30KB total.
        // With the old 8KB buffer this would overflow multiple times.
        // With the new 256KB buffer it fits in a single write.
        let record = vec![0xABu8; 300];
        let mut expected_offset = 0u64;
        for _ in 0..100 {
            let off = backend.append(&record).unwrap();
            assert_eq!(off, expected_offset);
            expected_offset += 300;
        }
        backend.sync().unwrap();

        let contents = std::fs::read(&path).unwrap();
        assert_eq!(contents.len(), 30_000);
        assert_eq!(backend.offset(), 30_000);
    }

    /// H-1 verification: very small writes (1 byte each) should still work
    /// correctly with the larger buffer.
    #[test]
    fn h1_small_writes_still_correct() {
        let dir = tmp();
        let path = dir.path().join("h1_small.log");
        let mut backend = SyncBackend::open(&path).unwrap();

        for i in 0u8..255 {
            let off = backend.append(&[i]).unwrap();
            assert_eq!(off, i as u64);
        }
        backend.sync().unwrap();

        let contents = std::fs::read(&path).unwrap();
        assert_eq!(contents.len(), 255);
        for i in 0u8..255 {
            assert_eq!(contents[i as usize], i);
        }
    }

    // ------------------------------------------------------------------
    // M-NEW-1: open_small_buffer uses 8KB buffer
    // ------------------------------------------------------------------

    #[test]
    fn small_buffer_backend_works_correctly() {
        let dir = tmp();
        let path = dir.path().join("small_buf.log");
        let mut backend = SyncBackend::open_small_buffer(&path).unwrap();

        let off0 = backend.append(b"hello").unwrap();
        let off1 = backend.append(b"world").unwrap();
        backend.sync().unwrap();

        assert_eq!(off0, 0);
        assert_eq!(off1, 5);
        assert_eq!(backend.offset(), 10);

        let contents = std::fs::read(&path).unwrap();
        assert_eq!(contents, b"helloworld");
    }

    #[test]
    fn small_buffer_backend_offset_resumes_after_reopen() {
        let dir = tmp();
        let path = dir.path().join("small_buf_reopen.log");

        {
            let mut backend = SyncBackend::open_small_buffer(&path).unwrap();
            backend.append(b"data").unwrap();
            backend.sync().unwrap();
        }

        let backend = SyncBackend::open_small_buffer(&path).unwrap();
        assert_eq!(
            backend.offset(),
            4,
            "offset must resume at file size after reopen"
        );
    }

    #[test]
    fn create_backend_small_returns_working_backend() {
        let dir = tmp();
        let path = dir.path().join("factory_small.log");
        let mut backend = super::create_backend_small(&path).unwrap();

        let off = backend.append(b"test").unwrap();
        backend.sync().unwrap();

        assert_eq!(off, 0);
        assert_eq!(backend.offset(), 4);
    }

    #[test]
    fn append_lock_registry_prunes_closed_segment_paths() {
        let dir = tmp();

        for index in 0..64 {
            let path = dir.path().join(format!("{index}.log"));
            drop(SyncBackend::open(&path).unwrap());
        }

        let active_path = dir.path().join("active.log");
        let _active = SyncBackend::open(&active_path).unwrap();

        let locks = APPEND_LOCKS
            .get()
            .unwrap()
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let retained = locks
            .keys()
            .filter(|key| key.path.starts_with(dir.path()))
            .count();

        assert_eq!(retained, 1);
    }

    #[cfg(unix)]
    #[test]
    fn hardlink_aliases_share_one_append_lock() {
        let dir = tmp();
        let path = dir.path().join("segment.log");
        let alias = dir.path().join("segment-alias.log");
        std::fs::write(&path, b"").unwrap();
        std::fs::hard_link(&path, &alias).unwrap();

        let primary = SyncBackend::open(&path).unwrap();
        let secondary = SyncBackend::open(&alias).unwrap();

        assert!(Arc::ptr_eq(&primary.append_lock, &secondary.append_lock));
    }

    #[test]
    fn poisoned_append_lock_returns_io_error() {
        let lock = Arc::new(Mutex::new(()));
        let poison = Arc::clone(&lock);

        let _ = std::panic::catch_unwind(move || {
            let _guard = poison.lock().unwrap();
            panic!("poison append lock");
        });

        assert!(lock_append(&lock).is_err());
    }

    #[test]
    fn uring_batch_validation_drains_completions_after_early_error() {
        use std::cell::Cell;

        let visited = Cell::new(0usize);
        let completions = [(0, -libc::EIO), (1, 4)]
            .into_iter()
            .inspect(|_completion| {
                visited.set(visited.get() + 1);
            });

        let error = validate_uring_batch_completions(completions, &[4, 4]).unwrap_err();

        assert_eq!(error.raw_os_error(), Some(libc::EIO));
        assert_eq!(visited.get(), 2, "all submitted CQEs must be drained");
    }

    #[test]
    fn uring_single_validation_drains_and_rejects_duplicate_or_wrong_tags() {
        use std::cell::Cell;

        let visited = Cell::new(0usize);
        let completions = [(0x01, 4), (0x01, 4)]
            .into_iter()
            .inspect(|_completion| visited.set(visited.get() + 1));

        assert!(validate_uring_single_completion(completions, 0x01, 4, "write").is_err());
        assert_eq!(visited.get(), 2, "all unexpected CQEs must be drained");

        assert!(validate_uring_single_completion([(0x02, 4)], 0x01, 4, "write").is_err());
    }
}
