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
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};

// On Linux, bring in the uring backend modules.
#[cfg(target_os = "linux")]
pub mod async_uring;
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

    /// Advance the internal write offset by `bytes` without actually writing
    /// any data. Used by the async `io_uring` path to reserve file space that
    /// will be written by an `AsyncUringBackend` operating on the same file.
    ///
    /// The default implementation does nothing. Backends that track an
    /// internal offset counter (like `SyncBackend`) override this to keep
    /// their counter in sync with externally-written bytes.
    fn advance_offset(&mut self, _bytes: u64) {}

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
    /// The default implementation calls `append` for each buffer and then
    /// `sync`. Platform-specific backends may override this to submit all
    /// writes in a single syscall before fsyncing (e.g. `io_uring`'s batch
    /// submission path).
    ///
    /// Returns the starting offset of each buffer in the same order as the
    /// input slice.
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` if any write or the final sync fails.
    fn append_batch_and_sync(&mut self, buffers: &[&[u8]]) -> io::Result<Vec<u64>> {
        let mut offsets = Vec::with_capacity(buffers.len());
        for buf in buffers {
            offsets.push(self.append(buf)?);
        }
        self.sync()?;
        Ok(offsets)
    }
}

type AppendLock = Arc<Mutex<()>>;

static APPEND_LOCKS: OnceLock<Mutex<HashMap<PathBuf, AppendLock>>> = OnceLock::new();

pub(crate) fn append_lock_for_path(path: &Path) -> AppendLock {
    let key = path.to_path_buf();
    let locks = APPEND_LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut locks = locks.lock().unwrap_or_else(PoisonError::into_inner);
    Arc::clone(locks.entry(key).or_insert_with(|| Arc::new(Mutex::new(()))))
}

pub(crate) fn lock_append_guard(lock: &AppendLock) -> io::Result<MutexGuard<'_, ()>> {
    lock.lock()
        .map_err(|_| io::Error::new(io::ErrorKind::Other, "append lock poisoned"))
}

// ---------------------------------------------------------------------------
// SyncBackend — standard BufWriter<File>, always available
// ---------------------------------------------------------------------------

/// Standard synchronous I/O backend using `BufWriter<File>`.
///
/// This is the fallback backend used on all platforms. On macOS (and any
/// platform where `io_uring` is unavailable) this is the only backend.
pub struct SyncBackend {
    writer: BufWriter<File>,
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
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let offset = file.metadata()?.len();
        Ok(Self {
            writer: BufWriter::with_capacity(256 * 1024, file), // H-1: 256KB buffer for batch writes
            offset,
            append_lock: append_lock_for_path(path),
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
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let offset = file.metadata()?.len();
        Ok(Self {
            writer: BufWriter::with_capacity(8 * 1024, file),
            offset,
            append_lock: append_lock_for_path(path),
        })
    }
}

impl IoBackend for SyncBackend {
    fn append(&mut self, data: &[u8]) -> io::Result<u64> {
        let append_lock = Arc::clone(&self.append_lock);
        let _guard = lock_append_guard(&append_lock)?;

        self.writer.flush()?;
        let start = self.writer.get_ref().metadata()?.len();
        self.writer.write_all(data)?;
        self.writer.flush()?;
        self.offset = start + data.len() as u64;
        Ok(start)
    }

    /// C-7 fix: use `sync_data()` (`fdatasync`) instead of `sync_all()` (`fsync`).
    /// `fdatasync` skips flushing non-critical metadata (mtime, atime) which
    /// avoids an extra journal write on ext4/xfs, making it 2-10x faster on HDD
    /// and 5-50us faster on NVMe. For append-only logs the file size is the only
    /// critical metadata, and `fdatasync` syncs size changes on Linux.
    fn sync(&mut self) -> io::Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_data()?;
        Ok(())
    }

    fn flush_no_sync(&mut self) -> io::Result<()> {
        self.writer.flush()?;
        Ok(())
    }

    fn offset(&self) -> u64 {
        self.offset
    }

    fn advance_offset(&mut self, bytes: u64) {
        self.offset += bytes;
    }

    fn append_batch_and_sync(&mut self, buffers: &[&[u8]]) -> io::Result<Vec<u64>> {
        let append_lock = Arc::clone(&self.append_lock);
        let _guard = lock_append_guard(&append_lock)?;

        self.writer.flush()?;
        let mut running = self.writer.get_ref().metadata()?.len();
        let mut offsets = Vec::with_capacity(buffers.len());

        for buf in buffers {
            offsets.push(running);
            self.writer.write_all(buf)?;
            running += buf.len() as u64;
        }

        self.writer.flush()?;
        self.writer.get_ref().sync_data()?;
        self.offset = running;
        Ok(offsets)
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
    fn append_lock_guard_reports_poison_without_panicking() {
        let lock = Arc::new(Mutex::new(()));
        let lock_to_poison = Arc::clone(&lock);
        let result = std::panic::catch_unwind(move || {
            let _guard = lock_to_poison.lock().unwrap();
            panic!("poison append lock");
        });

        assert!(result.is_err());
        let err = lock_append_guard(&lock).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::Other);
        assert!(err.to_string().contains("append lock poisoned"));
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
}
