//! `UringBackend` — `io_uring`-based I/O for the Bitcask log writer.
//!
//! This module is **only compiled on Linux** (`#[cfg(target_os = "linux")]`).
//! On all other platforms the `io-uring` crate is not even a dependency.
//!
//! ## Safety contract
//!
//! Every `unsafe` block in this file is for `io_uring`'s SQE submission.
//! The invariant is: the buffer pointer passed to the kernel is valid for
//! the entire duration of the I/O operation. We guarantee this because all
//! methods call `ring.submit_and_wait(n)` immediately after pushing SQEs,
//! which blocks the calling thread until the kernel signals completion.
//! The buffers live on the Rust stack/heap and cannot be freed while the
//! thread is blocked inside `submit_and_wait`.
//!
//! This is *synchronous* use of an asynchronous kernel interface — there is
//! no tokio, no `Future`, no `Poll`. The BEAM dirty-scheduler thread blocks
//! inside `submit_and_wait`, just as it would block inside `write(2)` +
//! `fsync(2)`, but with fewer system calls per batch.

#![allow(unsafe_code)]
// SAFETY: see module-level doc comment.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::sync::{Arc, Mutex};

use io_uring::{opcode, types, IoUring};

use super::{append_lock_for_path, lock_append_guard, IoBackend};

/// Ring capacity in submission-queue entries. Must be a power of two.
///
/// 64 slots covers the largest realistic `put_batch` payload (a `GenServer`
/// batch window collecting writes over ~1 ms). Each slot occupies ~64 bytes
/// in the ring, so this is ~4 KB of ring memory per store shard — negligible.
const RING_SIZE: u32 = 64;

/// `io_uring`-backed append-only log writer.
///
/// Uses `io_uring` as a batched I/O submission interface. Despite operating
/// through an asynchronous kernel interface, all methods block the calling
/// thread until the kernel signals completion, making the API fully
/// synchronous from the caller's perspective.
///
/// ## Performance advantage over `SyncBackend`
///
/// For a batch of N writes followed by one fsync, `SyncBackend` issues N+1
/// system calls (`write(2)` × N + `fsync(2)` × 1). `UringBackend` issues 2
/// system calls (`io_uring_enter` × 2: one to submit all N writes and wait,
/// one to submit the fsync and wait). For typical batch sizes of 10–100
/// entries this reduces kernel-crossing overhead by an order of magnitude.
pub struct UringBackend {
    ring: IoUring,
    file: File,
    offset: u64,
    append_lock: Arc<Mutex<()>>,
}

impl UringBackend {
    /// Open (or create) the file at `path` for appending and initialise an
    /// `io_uring` ring with `RING_SIZE` submission-queue entries.
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` if the file cannot be opened, its metadata
    /// cannot be read, or the ring cannot be initialised.
    pub fn open(path: &Path) -> io::Result<Self> {
        // Open without O_APPEND so that explicit pwrite offsets (.offset() on
        // SQEs) are respected. On some kernel versions O_APPEND causes
        // IORING_OP_WRITE to ignore the provided offset and always write at
        // EOF, which corrupts the log layout when advance_offset has logically
        // reserved space that hasn't yet been physically written by an in-flight
        // async batch.
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)?;
        let offset = file.metadata()?.len();

        let ring = IoUring::builder()
            .build(RING_SIZE)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        Ok(Self {
            ring,
            file,
            offset,
            append_lock: append_lock_for_path(path),
        })
    }

    /// Submit a single positioned write SQE and wait for one completion.
    ///
    /// # Safety
    ///
    /// `data` must remain valid and unmoved until `submit_and_wait` returns.
    /// The caller guarantees this because `data` is a borrow that lives for
    /// the duration of the enclosing function, and `submit_and_wait` blocks
    /// the thread until the kernel completes the I/O.
    fn submit_write_at(&mut self, data: &[u8], file_offset: u64) -> io::Result<()> {
        let fd = types::Fd(self.file.as_raw_fd());

        // Build a positioned-write SQE (equivalent to pwrite64).
        // SAFETY: `data` outlives this call — see method doc.
        let sqe = opcode::Write::new(
            fd,
            data.as_ptr(),
            u32::try_from(data.len()).unwrap_or(u32::MAX),
        )
        .offset(file_offset)
        .build()
        .user_data(0x01);

        // SAFETY: buffer is valid for the duration of submit_and_wait.
        unsafe {
            self.ring
                .submission()
                .push(&sqe)
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "io_uring: SQ full"))?;
        }

        self.ring.submit_and_wait(1)?;

        let cqe =
            self.ring.completion().next().ok_or_else(|| {
                io::Error::new(io::ErrorKind::Other, "io_uring: no CQE after write")
            })?;

        let res = cqe.result();
        if res < 0 {
            return Err(io::Error::from_raw_os_error(-res));
        }
        if usize::try_from(res).ok() != Some(data.len()) {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                format!(
                    "io_uring short write: expected {} B, wrote {} B",
                    data.len(),
                    res
                ),
            ));
        }
        Ok(())
    }

    /// Submit a single fdatasync SQE and wait for one completion.
    ///
    /// C-7 fix: uses `FsyncFlags::DATASYNC` (fdatasync) instead of plain
    /// `Fsync` (fsync). fdatasync skips non-critical metadata (mtime/atime),
    /// which is 2-10x faster on ext4/xfs.
    fn submit_fsync(&mut self) -> io::Result<()> {
        let fd = types::Fd(self.file.as_raw_fd());

        let sqe = opcode::Fsync::new(fd)
            .flags(types::FsyncFlags::DATASYNC)
            .build()
            .user_data(0x02);

        // SAFETY: no buffer involved; fsync SQE only carries the fd.
        unsafe {
            self.ring
                .submission()
                .push(&sqe)
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "io_uring: SQ full on fsync"))?;
        }

        self.ring.submit_and_wait(1)?;

        let cqe =
            self.ring.completion().next().ok_or_else(|| {
                io::Error::new(io::ErrorKind::Other, "io_uring: no CQE after fsync")
            })?;

        let res = cqe.result();
        if res < 0 {
            return Err(io::Error::from_raw_os_error(-res));
        }
        Ok(())
    }
}

impl IoBackend for UringBackend {
    fn append(&mut self, data: &[u8]) -> io::Result<u64> {
        let append_lock = Arc::clone(&self.append_lock);
        let _guard = lock_append_guard(&append_lock)?;

        let start = self.file.metadata()?.len();
        self.submit_write_at(data, start)?;
        self.offset = start + data.len() as u64;
        Ok(start)
    }

    fn sync(&mut self) -> io::Result<()> {
        self.submit_fsync()
    }

    fn offset(&self) -> u64 {
        self.offset
    }

    fn advance_offset(&mut self, bytes: u64) {
        self.offset += bytes;
    }

    fn flush_no_sync(&mut self) -> io::Result<()> {
        // UringBackend has no userspace buffer: append() submits a positioned
        // write and waits for its CQE before returning. Data is already in the
        // kernel page cache, so nosync must not issue fdatasync.
        Ok(())
    }

    /// Optimised batch path: submit ALL write SQEs in one `io_uring_enter`
    /// call, wait for all completions, then issue fsync in a second call.
    ///
    /// For a batch of N writes, this is **2 system calls** (submit+wait the
    /// writes, then submit+wait the fsync), regardless of N. With the default
    /// `SyncBackend` it would be N+1 system calls.
    ///
    /// If the batch exceeds `RING_SIZE` entries it is split into chunks, each
    /// chunk submitted and waited individually, with a single fsync at the end.
    ///
    /// ## Offset-advancement contract
    ///
    /// `self.offset` is **not** advanced until every CQE in a chunk has been
    /// validated (no error, no short write). This ensures that a partial
    /// failure never leaves `self.offset` pointing past un-confirmed bytes.
    fn append_batch_and_sync(&mut self, buffers: &[&[u8]]) -> io::Result<Vec<u64>> {
        let append_lock = Arc::clone(&self.append_lock);
        let _guard = lock_append_guard(&append_lock)?;

        if buffers.is_empty() {
            return Ok(Vec::new());
        }

        let fd = types::Fd(self.file.as_raw_fd());
        let mut all_offsets = Vec::with_capacity(buffers.len());
        self.offset = self.file.metadata()?.len();

        // Split into chunks that fit in the ring.
        for chunk in buffers.chunks(RING_SIZE as usize) {
            // Step 1: compute write offsets for this chunk from the CURRENT
            // self.offset without advancing it yet.  If anything below fails,
            // self.offset remains correct for the caller to retry or handle the
            // error.
            let mut chunk_offsets = Vec::with_capacity(chunk.len());
            let mut running = self.offset;
            for buf in chunk {
                chunk_offsets.push(running);
                running += buf.len() as u64;
            }
            let chunk_total_bytes: u64 = running - self.offset;

            // Build a map from file-offset → expected byte count so we can
            // validate each CQE by its user_data tag (which carries the file
            // offset).  Using the file offset as the tag is safe because every
            // buffer in a single chunk starts at a unique position.
            let expected: HashMap<u64, usize> = chunk_offsets
                .iter()
                .zip(chunk.iter())
                .map(|(&off, buf)| (off, buf.len()))
                .collect();

            // Step 2: push all SQEs for this chunk.
            {
                let mut sq = self.ring.submission();
                for (buf, &file_offset) in chunk.iter().zip(chunk_offsets.iter()) {
                    let sqe = opcode::Write::new(
                        fd,
                        buf.as_ptr(),
                        u32::try_from(buf.len()).unwrap_or(u32::MAX),
                    )
                    .offset(file_offset)
                    .build()
                    // Store the file offset as user_data so we can look up
                    // the expected length when draining completions.
                    .user_data(file_offset);

                    // SAFETY: `buf` is borrowed from the caller's `buffers`
                    // slice, which lives for the entire duration of this method
                    // call.  We call `submit_and_wait` immediately after this
                    // loop (step 3), which blocks the thread until the kernel
                    // signals completion for every SQE in this chunk.  The
                    // buffer pointers therefore remain valid for as long as the
                    // kernel needs them.
                    unsafe {
                        sq.push(&sqe).map_err(|_| {
                            io::Error::new(io::ErrorKind::Other, "io_uring: SQ full in batch")
                        })?;
                    }
                }
            }

            // Step 3: wait for all completions in this chunk.
            let chunk_len = chunk.len();
            self.ring.submit_and_wait(chunk_len)?;

            // Step 4: drain and validate every CQE.
            // Check both error (res < 0) and short write (res != expected_len).
            // If any CQE is invalid we return immediately WITHOUT advancing
            // self.offset, preserving the offset invariant.
            let mut completed = 0usize;
            for cqe in self.ring.completion() {
                let res = cqe.result();
                if res < 0 {
                    return Err(io::Error::from_raw_os_error(-res));
                }
                let file_offset = cqe.user_data();
                if let Some(&expected_len) = expected.get(&file_offset) {
                    if usize::try_from(res).ok() != Some(expected_len) {
                        return Err(io::Error::new(
                            io::ErrorKind::WriteZero,
                            format!(
                                "io_uring short write: expected {expected_len} B at offset {file_offset}, wrote {res} B"
                            ),
                        ));
                    }
                }
                completed += 1;
            }

            if completed != chunk_len {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("io_uring batch: expected {chunk_len} completions, got {completed}"),
                ));
            }

            // Step 5: all CQEs validated — NOW advance self.offset and record
            // the confirmed offsets for this chunk.
            self.offset += chunk_total_bytes;
            all_offsets.extend_from_slice(&chunk_offsets);
        }

        // Single fsync after all chunks.
        self.submit_fsync()?;

        Ok(all_offsets)
    }
}

// ---------------------------------------------------------------------------
// Tests (Linux only)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn tmp() -> TempDir {
        tempfile::TempDir::new().unwrap()
    }

    /// Skip the test if `io_uring` is not available (old kernel / CI without
    /// the feature). This prevents false failures in restricted environments.
    fn requires_io_uring() -> bool {
        IoUring::<io_uring::squeue::Entry, io_uring::cqueue::Entry>::builder()
            .build(1)
            .is_ok()
    }

    #[test]
    fn uring_backend_single_write_and_sync() {
        if !requires_io_uring() {
            return;
        }

        let dir = tmp();
        let path = dir.path().join("test.log");
        let mut backend = UringBackend::open(&path).unwrap();

        let off = backend.append(b"hello").unwrap();
        backend.sync().unwrap();

        assert_eq!(off, 0);
        assert_eq!(backend.offset(), 5);

        // Read back via std::fs to verify the data is on disk.
        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(&bytes, b"hello");
    }

    #[test]
    fn uring_backend_batch_writes_correct_offsets() {
        if !requires_io_uring() {
            return;
        }

        let dir = tmp();
        let path = dir.path().join("test.log");
        let mut backend = UringBackend::open(&path).unwrap();

        let bufs: &[&[u8]] = &[b"aaa", b"bb", b"c"];
        let offsets = backend.append_batch_and_sync(bufs).unwrap();

        assert_eq!(offsets, vec![0, 3, 5]);
        assert_eq!(backend.offset(), 6);

        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(&bytes, b"aaabbc");
    }

    #[test]
    fn uring_backend_offset_resumes_after_reopen() {
        if !requires_io_uring() {
            return;
        }

        let dir = tmp();
        let path = dir.path().join("test.log");

        {
            let mut backend = UringBackend::open(&path).unwrap();
            backend.append(b"data").unwrap();
            backend.sync().unwrap();
        }

        let backend = UringBackend::open(&path).unwrap();
        assert_eq!(backend.offset(), 4, "offset must resume at file size");
    }

    #[test]
    fn batch_offset_not_advanced_on_success_accumulates_correctly() {
        // Verifies that self.offset is advanced only after I/O confirms,
        // and that the accumulation across multiple batches is correct.
        if !requires_io_uring() {
            return;
        }

        let dir = tmp();
        let path = dir.path().join("test.log");
        let mut backend = UringBackend::open(&path).unwrap();

        // First batch: 3 entries of 2, 3, 4 bytes = 9 bytes total.
        let bufs1: &[&[u8]] = &[b"ab", b"cde", b"fghi"];
        let offsets1 = backend.append_batch_and_sync(bufs1).unwrap();

        assert_eq!(offsets1, vec![0, 2, 5]);
        assert_eq!(
            backend.offset(),
            9,
            "offset after first batch must equal sum of all buffer lengths"
        );

        // Second batch: 2 entries of 1, 6 bytes = 7 bytes total.
        let bufs2: &[&[u8]] = &[b"j", b"klmnop"];
        let offsets2 = backend.append_batch_and_sync(bufs2).unwrap();

        assert_eq!(offsets2, vec![9, 10]);
        assert_eq!(
            backend.offset(),
            16,
            "offset must accumulate correctly across multiple batches"
        );
    }

    #[test]
    fn batch_writes_correct_offsets_and_data_are_readable() {
        // Verifies that returned offsets match the actual file positions and
        // that the data written can be read back from those offsets.
        if !requires_io_uring() {
            return;
        }

        let dir = tmp();
        let path = dir.path().join("test.log");
        let mut backend = UringBackend::open(&path).unwrap();

        let bufs: &[&[u8]] = &[b"hello", b"world", b"!"];
        let offsets = backend.append_batch_and_sync(bufs).unwrap();

        // Offsets must be [0, 5, 10].
        assert_eq!(offsets, vec![0, 5, 10]);

        // Read back the whole file and verify each slice at the reported offset.
        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(bytes.len(), 11);
        assert_eq!(
            &bytes[offsets[0] as usize..offsets[0] as usize + 5],
            b"hello"
        );
        assert_eq!(
            &bytes[offsets[1] as usize..offsets[1] as usize + 5],
            b"world"
        );
        assert_eq!(&bytes[(offsets[2] as usize)..=(offsets[2] as usize)], b"!");
    }

    #[test]
    fn uring_backend_large_batch_over_ring_size() {
        if !requires_io_uring() {
            return;
        }

        let dir = tmp();
        let path = dir.path().join("test.log");
        let mut backend = UringBackend::open(&path).unwrap();

        // 100 entries > RING_SIZE (64), exercising the chunk-split path.
        let data: Vec<Vec<u8>> = (0u8..100).map(|i| vec![i; 4]).collect();
        let bufs: Vec<&[u8]> = data.iter().map(Vec::as_slice).collect();

        let offsets = backend.append_batch_and_sync(&bufs).unwrap();

        assert_eq!(offsets.len(), 100);
        assert_eq!(offsets[0], 0);
        assert_eq!(offsets[1], 4);
        assert_eq!(backend.offset(), 400);

        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(bytes.len(), 400);
        for (i, chunk) in bytes.chunks(4).enumerate() {
            assert!(chunk.iter().all(|&b| b == i as u8), "chunk {i} corrupted");
        }
    }
}
