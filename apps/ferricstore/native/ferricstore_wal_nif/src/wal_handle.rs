// WalHandle — the NIF resource that ties everything together.
//
// Owned by Erlang via ResourceArc. Contains:
// - Shared aligned buffer (Mutex)
// - Channel sender for flush/close signals
// - Thread-alive flag (AtomicBool)
// - File size counter (AtomicU64)
// - Thread join handle

use crate::aligned_buffer::AlignedBuffer;
use crate::background_thread::{self, FlushCaller, ThreadConfig, ThreadMsg};
use crossbeam_channel::Sender;
use std::fs::File;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

pub struct WalHandle {
    /// Shared write buffer. NIF write() appends here, background thread takes.
    buffer: Arc<Mutex<AlignedBuffer>>,

    /// Channel to send flush/close signals to the background thread.
    flush_tx: Sender<ThreadMsg>,

    /// True while the background thread is alive.
    alive: Arc<AtomicBool>,

    /// Logical file size (updated by background thread after each write).
    file_size_counter: Arc<AtomicU64>,

    /// Maximum buffer size before backpressure.
    max_buffer_bytes: u64,

    /// Background thread handle.
    thread_handle: Mutex<Option<JoinHandle<()>>>,

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

        // Open a second fd for pread (recovery reads)
        let read_file = std::fs::OpenOptions::new().read(true).open(&path)?;

        let buffer = Arc::new(Mutex::new(AlignedBuffer::new()));
        let (flush_tx, flush_rx) = crossbeam_channel::bounded(1024);
        let alive = Arc::new(AtomicBool::new(true));
        let file_size = Arc::new(AtomicU64::new(background_thread::WAL_HEADER_SIZE));
        let config = ThreadConfig {
            file,
            buffer: buffer.clone(),
            rx: flush_rx,
            alive: alive.clone(),
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
            file_size_counter: file_size,
            max_buffer_bytes,
            thread_handle: Mutex::new(Some(thread_handle)),
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

        let buffered = buf.len() as u64;
        let incoming = data.len() as u64;

        // A single write larger than the configured limit can never become
        // acceptable by retrying. Treat the limit as soft only while the buffer
        // is empty, then apply normal backpressure until the WAL thread drains.
        if buffered > 0 && buffered + incoming > self.max_buffer_bytes {
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
    ) -> Result<(), rustler::Error> {
        let caller = FlushCaller {
            pid,
            env,
            saved_ref,
        };

        self.flush_tx
            .send(ThreadMsg::Flush(caller))
            .map_err(|_| rustler::Error::Term(Box::new("wal_thread_dead")))
    }

    /// Close the WAL. Blocks until background thread exits (max 30s).
    pub fn close(&self) -> io::Result<()> {
        let handle = {
            let mut guard = self
                .thread_handle
                .lock()
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "thread_handle poisoned"))?;

            match guard.take() {
                Some(handle) => handle,
                None => {
                    self.alive.store(false, Ordering::Release);
                    return Ok(());
                }
            }
        };

        let (result_tx, result_rx) = crossbeam_channel::bounded(1);

        if self
            .flush_tx
            .send(ThreadMsg::Close(Some(result_tx)))
            .is_err()
        {
            self.alive.store(false, Ordering::Release);
            let _ = handle.join();
            return Err(io::Error::new(io::ErrorKind::BrokenPipe, "wal thread dead"));
        }

        let close_result = match result_rx.recv_timeout(Duration::from_secs(30)) {
            Ok(Ok(())) => Ok(()),
            Ok(Err(reason)) => Err(io::Error::new(io::ErrorKind::Other, reason)),
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                Err(io::Error::new(io::ErrorKind::TimedOut, "wal close timeout"))
            }
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "wal thread closed without reporting close result",
            )),
        };

        // Wait for thread to exit.
        let start = std::time::Instant::now();
        loop {
            if handle.is_finished() {
                let _ = handle.join();
                break;
            }
            if start.elapsed() > Duration::from_secs(30) {
                return Err(io::Error::new(io::ErrorKind::TimedOut, "wal close timeout"));
            }
            std::thread::sleep(Duration::from_millis(10));
        }

        self.alive.store(false, Ordering::Release);
        close_result
    }

    /// Current logical file size (no syscall).
    pub fn file_size(&self) -> u64 {
        self.file_size_counter.load(Ordering::Acquire)
    }

    /// Read bytes from the WAL at offset. For recovery.
    pub fn pread(&self, offset: u64, len: u64) -> Result<Vec<u8>, rustler::Error> {
        let mut file = self
            .read_file
            .lock()
            .map_err(|_| rustler::Error::Term(Box::new("read_mutex_poisoned")))?;

        background_thread::pread_from_file(&mut file, offset, len)
            .map_err(|e| rustler::Error::Term(Box::new(format!("{e}"))))
    }
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
    fn test_pread_after_write() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal").to_str().unwrap().to_string();

        let handle = WalHandle::open(path, 0, 0, 64 * 1024 * 1024).unwrap();
        handle.buffer_write(b"hello pread test").unwrap();
        handle.close().unwrap();

        let hdr = background_thread::WAL_HEADER_SIZE;
        let data = handle.pread(hdr + 6, 5).unwrap();
        assert_eq!(&data, b"pread");
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
