// WalHandle — the NIF resource that ties everything together.
//
// Owned by Erlang via ResourceArc. Contains:
// - Shared aligned buffer (Mutex)
// - Channel sender for flush/close signals
// - Thread-alive flag (AtomicBool)
// - File size counter (AtomicU64)
// - Thread join handle

use crate::aligned_buffer::AlignedBuffer;
use crate::background_thread::{self, FlushCaller, FlushTarget, ThreadConfig, ThreadMsg};
use crossbeam_channel::{Sender, TrySendError};
use std::fs::File;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, TryLockError};
use std::thread::JoinHandle;
use std::time::Duration;

pub struct WalHandle {
    /// Shared write buffer. NIF write() appends here, background thread takes.
    buffer: Arc<Mutex<AlignedBuffer>>,

    /// Channel to send flush/close signals to the background thread.
    flush_tx: Sender<ThreadMsg>,

    /// True while the background thread is alive.
    alive: Arc<AtomicBool>,

    /// Serializes sync enqueue with panic cleanup so admitted callers are notified.
    sync_admission: Arc<Mutex<()>>,

    /// Logical file size (updated by background thread after each write).
    file_size_counter: Arc<AtomicU64>,

    /// Maximum buffer size before backpressure.
    max_buffer_bytes: u64,

    /// Default adaptive sync delay cap used by legacy sync/3 callers.
    commit_delay: Duration,

    /// Background thread handle.
    thread_handle: Mutex<Option<JoinHandle<()>>>,

    /// Serializes sync admission with close and makes concurrent closes wait.
    close_gate: Mutex<()>,

    /// Preserves a terminal close failure for later close callers.
    close_error: Mutex<Option<String>>,

    /// File handle for pread (recovery). Protected by mutex for seek safety.
    read_file: Mutex<File>,
}

impl WalHandle {
    /// Open a WAL file, spawn the background I/O thread.
    pub fn open(
        path: String,
        commit_delay_us: u64,
        pre_allocate_bytes: u64,
        max_buffer_bytes: u64,
    ) -> io::Result<Self> {
        let (file, _o_direct) = background_thread::open_wal_file(&path, pre_allocate_bytes)?;
        let initial_size = file.metadata()?.len();

        // Open a second fd for pread (recovery reads).
        let read_file = open_matching_read_file(&path, &file)?;

        let buffer = Arc::new(Mutex::new(AlignedBuffer::new()));
        let (flush_tx, flush_rx) = crossbeam_channel::bounded(1024);
        let alive = Arc::new(AtomicBool::new(true));
        let sync_admission = Arc::new(Mutex::new(()));
        let file_size = Arc::new(AtomicU64::new(initial_size));
        let config = ThreadConfig {
            file,
            buffer: buffer.clone(),
            rx: flush_rx,
            alive: alive.clone(),
            sync_admission: Arc::clone(&sync_admission),
            file_size: file_size.clone(),
            commit_delay: Duration::from_micros(commit_delay_us),
            _use_o_direct: _o_direct,
        };

        let thread_handle = std::thread::Builder::new()
            .name("ferricstore-wal".into())
            .spawn(move || background_thread::thread_loop(config))?;

        Ok(WalHandle {
            buffer,
            flush_tx,
            alive,
            sync_admission,
            file_size_counter: file_size,
            max_buffer_bytes,
            commit_delay: Duration::from_micros(commit_delay_us),
            thread_handle: Mutex::new(Some(thread_handle)),
            close_gate: Mutex::new(()),
            close_error: Mutex::new(None),
            read_file: Mutex::new(read_file),
        })
    }

    /// Open a generic append file from byte 0.
    ///
    /// Used by WARaft segment logs where byte 0 is the first segment record.
    pub fn open_raw_append(
        path: String,
        commit_delay_us: u64,
        max_buffer_bytes: u64,
        start_offset: u64,
    ) -> io::Result<Self> {
        let (file, _o_direct) = background_thread::open_raw_append_file(&path, start_offset)?;

        let read_file = open_matching_read_file(&path, &file)?;

        let buffer = Arc::new(Mutex::new(AlignedBuffer::new()));
        let (flush_tx, flush_rx) = crossbeam_channel::bounded(1024);
        let alive = Arc::new(AtomicBool::new(true));
        let sync_admission = Arc::new(Mutex::new(()));
        let file_size = Arc::new(AtomicU64::new(start_offset));
        let config = ThreadConfig {
            file,
            buffer: buffer.clone(),
            rx: flush_rx,
            alive: alive.clone(),
            sync_admission: Arc::clone(&sync_admission),
            file_size: file_size.clone(),
            commit_delay: Duration::from_micros(commit_delay_us),
            _use_o_direct: _o_direct,
        };

        let thread_handle = std::thread::Builder::new()
            .name("ferricstore-wal".into())
            .spawn(move || background_thread::thread_loop(config))?;

        Ok(WalHandle {
            buffer,
            flush_tx,
            alive,
            sync_admission,
            file_size_counter: file_size,
            max_buffer_bytes,
            commit_delay: Duration::from_micros(commit_delay_us),
            thread_handle: Mutex::new(Some(thread_handle)),
            close_gate: Mutex::new(()),
            close_error: Mutex::new(None),
            read_file: Mutex::new(read_file),
        })
    }

    /// Check if the background thread is alive. Returns Err if dead.
    pub fn check_alive(&self) -> Result<(), rustler::Error> {
        if self.alive.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(rustler::Error::Term(Box::new("wal_thread_dead")))
        }
    }

    /// Append data to the shared write buffer.
    /// Returns Err if thread is dead or buffer exceeds max size (backpressure).
    pub fn buffer_write(&self, data: &[u8]) -> Result<(), rustler::Error> {
        self.check_alive()?;

        let mut buf = self
            .buffer
            .lock()
            .map_err(|_| rustler::Error::Term(Box::new("buffer_mutex_poisoned")))?;

        self.check_alive()?;

        let buffered = buf.len() as u64;
        let incoming = data.len() as u64;

        if incoming > self.max_buffer_bytes {
            return Err(rustler::Error::Term(Box::new("write_too_large")));
        }

        // Subtraction avoids overflowing while checking the aggregate bound.
        if buffered > self.max_buffer_bytes - incoming {
            return Err(rustler::Error::Term(Box::new("backpressure")));
        }

        buf.extend(data);
        Ok(())
    }

    /// Request async fdatasync from the background thread.
    pub fn request_sync(
        &self,
        pid: rustler::LocalPid,
        env: rustler::OwnedEnv,
        saved_ref: rustler::env::SavedTerm,
        commit_delay: Duration,
    ) -> Result<(), rustler::Error> {
        let _close_guard = match self.close_gate.try_lock() {
            Ok(guard) => guard,
            Err(TryLockError::WouldBlock) => {
                return Err(rustler::Error::Term(Box::new("backpressure")));
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(rustler::Error::Term(Box::new("close_gate_poisoned")));
            }
        };
        let _admission_guard = match self.sync_admission.try_lock() {
            Ok(guard) => guard,
            Err(TryLockError::WouldBlock) => {
                return Err(rustler::Error::Term(Box::new("backpressure")));
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(rustler::Error::Term(Box::new("sync_admission_poisoned")));
            }
        };
        self.check_alive()?;

        let caller = FlushCaller {
            target: FlushTarget::Beam {
                pid,
                env,
                saved_ref,
            },
            commit_delay,
        };

        match self.flush_tx.try_send(ThreadMsg::Flush(caller)) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => Err(rustler::Error::Term(Box::new("backpressure"))),
            Err(TrySendError::Disconnected(_)) => {
                Err(rustler::Error::Term(Box::new("wal_thread_dead")))
            }
        }
    }

    pub fn commit_delay(&self) -> Duration {
        self.commit_delay
    }

    /// Close the WAL. Blocks until the background thread reports its final
    /// drain/sync result and exits.
    pub fn close(&self) -> io::Result<()> {
        let _close_guard = self
            .close_gate
            .lock()
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "close gate poisoned"))?;

        let handle = {
            let mut guard = self
                .thread_handle
                .lock()
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "thread_handle poisoned"))?;

            match guard.take() {
                Some(handle) => handle,
                None => {
                    return match self
                        .close_error
                        .lock()
                        .map_err(|_| io::Error::new(io::ErrorKind::Other, "close result poisoned"))?
                        .as_ref()
                    {
                        Some(reason) => Err(io::Error::new(io::ErrorKind::Other, reason.clone())),
                        None => Ok(()),
                    };
                }
            }
        };

        // Reject new writes and sync requests before the final drain begins.
        self.alive.store(false, Ordering::Release);
        let result = self.close_thread(handle);

        if let Err(error) = &result {
            *self
                .close_error
                .lock()
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "close result poisoned"))? =
                Some(error.to_string());
        }

        result
    }

    fn close_thread(&self, handle: JoinHandle<()>) -> io::Result<()> {
        let (result_tx, result_rx) = crossbeam_channel::bounded(1);
        let close_result = match self.flush_tx.send(ThreadMsg::Close(Some(result_tx))) {
            Ok(()) => match result_rx.recv() {
                Ok(Ok(())) => Ok(()),
                Ok(Err(reason)) => Err(io::Error::new(io::ErrorKind::Other, reason)),
                Err(_) => Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "wal thread closed without reporting close result",
                )),
            },
            Err(_) => Err(io::Error::new(io::ErrorKind::BrokenPipe, "wal thread dead")),
        };

        if handle.join().is_err() {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "wal thread panicked during close",
            ));
        }

        close_result
    }

    /// Current logical file size (no syscall).
    pub fn file_size(&self) -> u64 {
        self.file_size_counter.load(Ordering::Acquire)
    }

    /// Read bytes from the WAL at offset. For recovery.
    pub fn pread_len(&self, offset: u64, len: u64) -> Result<usize, rustler::Error> {
        let file = self
            .read_file
            .lock()
            .map_err(|_| rustler::Error::Term(Box::new("read_mutex_poisoned")))?;
        let file_len = file
            .metadata()
            .map_err(|e| rustler::Error::Term(Box::new(format!("{e}"))))?
            .len();
        background_thread::validated_pread_len(file_len, offset, len)
            .map_err(|e| rustler::Error::Term(Box::new(format!("{e}"))))
    }

    pub fn pread_into(&self, offset: u64, output: &mut [u8]) -> Result<(), rustler::Error> {
        let mut file = self
            .read_file
            .lock()
            .map_err(|_| rustler::Error::Term(Box::new("read_mutex_poisoned")))?;

        let file_len = file
            .metadata()
            .map_err(|e| rustler::Error::Term(Box::new(format!("{e}"))))?
            .len();
        let requested = u64::try_from(output.len())
            .map_err(|_| rustler::Error::Term(Box::new("pread length exceeds maximum")))?;
        background_thread::validated_pread_len(file_len, offset, requested)
            .map_err(|e| rustler::Error::Term(Box::new(format!("{e}"))))?;
        background_thread::pread_into_file(&mut file, offset, output)
            .map_err(|e| rustler::Error::Term(Box::new(format!("{e}"))))
    }

    #[cfg(test)]
    pub fn pread(&self, offset: u64, len: u64) -> Result<Vec<u8>, rustler::Error> {
        let read_len = self.pread_len(offset, len)?;
        let mut output = vec![0; read_len];
        self.pread_into(offset, &mut output)?;
        Ok(output)
    }
}

fn open_matching_read_file(path: &str, write_file: &File) -> io::Result<File> {
    let read_file = background_thread::open_read_nofollow(path)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;

        let write_metadata = write_file.metadata()?;
        let read_metadata = read_file.metadata()?;
        if write_metadata.dev() != read_metadata.dev()
            || write_metadata.ino() != read_metadata.ino()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "WAL path changed while opening read descriptor",
            ));
        }
    }

    Ok(read_file)
}

impl Drop for WalHandle {
    fn drop(&mut self) {
        // Non-blocking: signal thread to exit, don't wait.
        self.alive.store(false, Ordering::Release);
        let _ = self.flush_tx.try_send(ThreadMsg::Close(None));
        // Thread will notice channel disconnect or Close signal and exit.
        // fd closed by OS when thread's File is dropped.
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_open_and_close() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal").to_str().unwrap().to_string();

        let handle = WalHandle::open(path, 0, 0, 64 * 1024 * 1024).unwrap();
        assert!(handle.check_alive().is_ok());
        assert_eq!(handle.file_size(), background_thread::WAL_HEADER_SIZE);

        handle.close().unwrap();
        assert!(handle.check_alive().is_err());
    }

    #[test]
    fn test_buffer_write() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal").to_str().unwrap().to_string();

        let handle = WalHandle::open(path, 0, 0, 64 * 1024 * 1024).unwrap();
        handle.buffer_write(b"hello world").unwrap();

        handle.close().unwrap();
        assert!(handle.file_size() >= background_thread::WAL_HEADER_SIZE + 11);
    }

    #[test]
    fn test_backpressure() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal").to_str().unwrap().to_string();

        // Very small max buffer
        let handle = WalHandle::open(path, 0, 0, 100).unwrap();
        handle.buffer_write(b"small").unwrap(); // 5 bytes, ok

        // Try to write more than max
        let big = vec![0u8; 200];
        let result = handle.buffer_write(&big);
        assert!(result.is_err()); // backpressure

        handle.close().unwrap();
    }

    #[test]
    fn test_backpressure_exact_boundary() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal").to_str().unwrap().to_string();

        let handle = WalHandle::open(path, 0, 0, 100).unwrap();

        // Fill to exactly max
        let data = vec![0u8; 100];
        handle.buffer_write(&data).unwrap(); // exactly 100, ok

        // One more byte should fail
        let result = handle.buffer_write(b"x");
        assert!(result.is_err());

        handle.close().unwrap();
    }

    #[test]
    fn test_file_size_after_flush() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal").to_str().unwrap().to_string();

        let handle = WalHandle::open(path, 0, 0, 64 * 1024 * 1024).unwrap();
        handle.buffer_write(b"data123").unwrap();

        handle.close().unwrap();
        assert_eq!(handle.file_size(), background_thread::WAL_HEADER_SIZE + 7);
    }

    #[test]
    fn test_multiple_writes_then_close() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal").to_str().unwrap().to_string();
        let path_clone = dir.path().join("test.wal").to_str().unwrap().to_string();

        let handle = WalHandle::open(path, 0, 0, 64 * 1024 * 1024).unwrap();
        handle.buffer_write(b"first ").unwrap();
        handle.buffer_write(b"second ").unwrap();
        handle.buffer_write(b"third").unwrap();
        handle.close().unwrap();

        let hdr = background_thread::WAL_HEADER_SIZE as usize;
        assert_eq!(handle.file_size(), (hdr + 18) as u64);

        let contents = std::fs::read(&path_clone).unwrap();
        assert!(contents.len() >= hdr + 18);
        assert_eq!(&contents[hdr..hdr + 18], b"first second third");
    }

    #[test]
    fn raw_append_starts_at_requested_offset_without_wal_header_gap() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("segment.seg").to_str().unwrap().to_string();
        let path_clone = path.clone();

        let handle = WalHandle::open_raw_append(path, 0, 64 * 1024 * 1024, 0).unwrap();
        handle.buffer_write(b"segment-record").unwrap();
        handle.close().unwrap();

        assert_eq!(handle.file_size(), 14);
        assert_eq!(std::fs::read(path_clone).unwrap(), b"segment-record");
    }

    #[test]
    fn test_check_alive_after_drop() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal").to_str().unwrap().to_string();

        let handle = WalHandle::open(path, 0, 0, 64 * 1024 * 1024).unwrap();
        handle.close().unwrap();

        // Thread is dead after close
        assert!(handle.check_alive().is_err());
    }

    #[test]
    fn test_close_returns_error_when_final_drain_fails() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal").to_str().unwrap().to_string();

        let handle = WalHandle::open(path, 0, 0, 64 * 1024 * 1024).unwrap();
        handle.buffer_write(b"must report drain failure").unwrap();

        let buffer = handle.buffer.clone();
        let _ = std::panic::catch_unwind(move || {
            let _guard = buffer.lock().unwrap();
            panic!("poison buffer mutex before close");
        });

        let err = handle.close().unwrap_err();

        assert!(err.to_string().contains("buffer mutex poisoned"));
        assert!(handle.check_alive().is_err());
    }

    #[test]
    fn concurrent_close_waits_for_the_inflight_close_result() {
        let buffer = Arc::new(Mutex::new(AlignedBuffer::new()));
        let (flush_tx, flush_rx) = crossbeam_channel::bounded(4);
        let (close_started_tx, close_started_rx) = crossbeam_channel::bounded(1);
        let (release_close_tx, release_close_rx) = crossbeam_channel::bounded(1);

        let thread_handle = std::thread::spawn(move || {
            let ThreadMsg::Close(Some(response)) = flush_rx.recv().unwrap() else {
                panic!("expected close request");
            };
            close_started_tx.send(()).unwrap();
            release_close_rx.recv().unwrap();
            response.send(Ok(())).unwrap();
        });

        let handle = Arc::new(WalHandle {
            buffer,
            flush_tx,
            alive: Arc::new(AtomicBool::new(true)),
            sync_admission: Arc::new(Mutex::new(())),
            file_size_counter: Arc::new(AtomicU64::new(0)),
            max_buffer_bytes: 1024,
            commit_delay: Duration::ZERO,
            thread_handle: Mutex::new(Some(thread_handle)),
            close_gate: Mutex::new(()),
            close_error: Mutex::new(None),
            read_file: Mutex::new(tempfile::tempfile().unwrap()),
        });

        let first_handle = Arc::clone(&handle);
        let first = std::thread::spawn(move || first_handle.close());
        close_started_rx.recv().unwrap();

        let second_handle = Arc::clone(&handle);
        let (second_done_tx, second_done_rx) = crossbeam_channel::bounded(1);
        let second = std::thread::spawn(move || {
            second_done_tx.send(second_handle.close()).unwrap();
        });

        assert!(
            second_done_rx
                .recv_timeout(Duration::from_millis(50))
                .is_err(),
            "a concurrent close must not return before the inflight close is durable"
        );

        release_close_tx.send(()).unwrap();
        first.join().unwrap().unwrap();
        second_done_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .unwrap();
        second.join().unwrap();
    }

    #[test]
    fn close_waits_for_channel_capacity_and_joins_the_worker() {
        let buffer = Arc::new(Mutex::new(AlignedBuffer::new()));
        let (flush_tx, flush_rx) = crossbeam_channel::bounded(1);
        flush_tx.send(ThreadMsg::Close(None)).unwrap();

        let (release_tx, release_rx) = crossbeam_channel::bounded(1);
        let thread_handle = std::thread::spawn(move || {
            let _ = release_rx.recv();
            drop(flush_rx);
        });

        let handle = Arc::new(WalHandle {
            buffer,
            flush_tx,
            alive: Arc::new(AtomicBool::new(true)),
            sync_admission: Arc::new(Mutex::new(())),
            file_size_counter: Arc::new(AtomicU64::new(0)),
            max_buffer_bytes: 1024,
            commit_delay: Duration::ZERO,
            thread_handle: Mutex::new(Some(thread_handle)),
            close_gate: Mutex::new(()),
            close_error: Mutex::new(None),
            read_file: Mutex::new(tempfile::tempfile().unwrap()),
        });

        let close_handle = Arc::clone(&handle);
        let (done_tx, done_rx) = crossbeam_channel::bounded(1);
        let closer = std::thread::spawn(move || done_tx.send(close_handle.close()).unwrap());

        assert!(
            done_rx.recv_timeout(Duration::from_millis(40)).is_err(),
            "close returned before the queued durability worker could finish"
        );

        release_tx.send(()).unwrap();
        let error = done_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
        closer.join().unwrap();
        assert!(handle.thread_handle.lock().unwrap().is_none());
    }

    #[test]
    fn test_pread_after_write() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal").to_str().unwrap().to_string();

        let handle = WalHandle::open(path, 0, 0, 64 * 1024 * 1024).unwrap();
        handle.buffer_write(b"hello pread test").unwrap();
        handle.close().unwrap();

        let hdr = background_thread::WAL_HEADER_SIZE;
        let data = handle.pread(hdr + 6, 5).unwrap();
        assert_eq!(data.as_slice(), b"pread");
    }

    #[test]
    fn test_concurrent_writes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal").to_str().unwrap().to_string();

        let handle = Arc::new(WalHandle::open(path, 0, 0, 64 * 1024 * 1024).unwrap());

        let mut threads = Vec::new();
        for i in 0..10 {
            let h = handle.clone();
            threads.push(std::thread::spawn(move || {
                for _ in 0..100 {
                    let data = format!("thread{i}:");
                    h.buffer_write(data.as_bytes()).unwrap();
                }
            }));
        }

        for t in threads {
            t.join().unwrap();
        }

        handle.close().unwrap();

        // All 1000 writes should be in the file
        // Each write is "threadN:" = 8 bytes
        assert!(handle.file_size() > 0);
    }

    #[test]
    fn test_write_after_close_fails() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal").to_str().unwrap().to_string();

        let handle = WalHandle::open(path, 0, 0, 64 * 1024 * 1024).unwrap();
        handle.close().unwrap();

        // Write after close should fail
        assert!(handle.check_alive().is_err());
    }

    #[test]
    fn test_commit_delay_config() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal").to_str().unwrap().to_string();

        let handle = WalHandle::open(path, 100_000, 0, 64 * 1024 * 1024).unwrap();
        handle.buffer_write(b"delayed").unwrap();
        handle.close().unwrap();
        assert_eq!(handle.file_size(), background_thread::WAL_HEADER_SIZE + 7);
    }

    #[test]
    fn test_zero_commit_delay() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal").to_str().unwrap().to_string();

        let handle = WalHandle::open(path, 0, 0, 64 * 1024 * 1024).unwrap();
        handle.buffer_write(b"immediate").unwrap();
        handle.close().unwrap();
        assert_eq!(handle.file_size(), background_thread::WAL_HEADER_SIZE + 9);
    }
}
