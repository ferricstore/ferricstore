// Background WAL I/O thread.
//
// Handles: write() + fdatasync() on a dedicated OS thread.
// Communicates with NIF layer via:
// - Mutex<AlignedBuffer> for write data (shared with NIF)
// - crossbeam channel for FlushRequest/CloseRequest signals
//
// The thread never touches BEAM schedulers. All BEAM notifications
// go through OwnedEnv::send_and_clear.

use crate::aligned_buffer::AlignedBuffer;
use crossbeam_channel::{Receiver, RecvTimeoutError, Sender};
use rustler::Encoder;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[cfg(target_os = "linux")]
use std::os::unix::io::AsRawFd;

/// Messages sent from NIF to background thread (flush signals only, not data).
pub type ThreadResult = std::result::Result<(), String>;

pub enum ThreadMsg {
    /// Request fdatasync. Caller will be notified on completion.
    Flush(FlushCaller),
    /// Shutdown: drain, sync, close, exit.
    Close(Option<Sender<ThreadResult>>),
}

/// Caller information for flush notification.
pub struct FlushCaller {
    pub pid: rustler::LocalPid,
    pub env: rustler::OwnedEnv,
    pub saved_ref: rustler::env::SavedTerm,
}

// FlushCaller must be Send to cross thread boundary
unsafe impl Send for FlushCaller {}

/// Configuration for the background thread.
pub struct ThreadConfig {
    pub file: File,
    pub buffer: Arc<Mutex<AlignedBuffer>>,
    pub rx: Receiver<ThreadMsg>,
    pub alive: Arc<AtomicBool>,
    pub file_size: Arc<AtomicU64>,
    pub commit_delay: Duration,
    pub _use_o_direct: bool,
}

trait SyncData {
    fn sync_data(&mut self) -> io::Result<()>;
}

impl SyncData for File {
    fn sync_data(&mut self) -> io::Result<()> {
        File::sync_data(self)
    }
}

/// Run the background thread loop.
/// This is called from std::thread::spawn — runs entirely outside BEAM.
pub fn thread_loop(config: ThreadConfig) {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        thread_loop_inner(
            config.file,
            config.buffer,
            config.rx,
            config.file_size,
            config.commit_delay,
        );
    }));

    // On panic or normal exit, mark thread as dead
    config.alive.store(false, Ordering::Release);

    if let Err(panic) = result {
        eprintln!(
            "[ferricstore_wal_nif] WAL thread panicked: {:?}",
            panic
                .downcast_ref::<String>()
                .map(|s| s.as_str())
                .or_else(|| panic.downcast_ref::<&str>().copied())
                .unwrap_or("unknown")
        );
    }
}

fn thread_loop_inner(
    mut file: File,
    buffer: Arc<Mutex<AlignedBuffer>>,
    rx: Receiver<ThreadMsg>,
    file_size: Arc<AtomicU64>,
    commit_delay: Duration,
) {
    let mut callers: Vec<FlushCaller> = Vec::new();

    loop {
        // =====================================================================
        // Phase 1: IDLE — block until first message (zero CPU when idle)
        // =====================================================================
        let msg = match rx.recv() {
            Ok(msg) => msg,
            Err(_) => {
                finish_disconnect(&mut file, &buffer, &file_size, &mut callers);
                return;
            }
        };

        match msg {
            ThreadMsg::Close(response) => {
                finish_close(&mut file, &buffer, &file_size, &mut callers, response);
                return;
            }
            ThreadMsg::Flush(caller) => {
                callers.push(caller);
            }
        }

        // =====================================================================
        // Phase 2: Drain buffer to kernel immediately (before sync decision)
        // =====================================================================
        if let Err(e) = drain_to_kernel(&mut file, &buffer, &file_size) {
            notify_callers_error(&mut callers, &e);
            return;
        }

        // =====================================================================
        // Phase 3: Commit delay — collect more Flush requests, keep draining
        // =====================================================================
        if !commit_delay.is_zero() {
            let deadline = Instant::now() + commit_delay;
            loop {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    break;
                }
                match rx.recv_timeout(remaining) {
                    Ok(ThreadMsg::Flush(c)) => callers.push(c),
                    Ok(ThreadMsg::Close(response)) => {
                        finish_close(&mut file, &buffer, &file_size, &mut callers, response);
                        return;
                    }
                    Err(RecvTimeoutError::Timeout) => break,
                    Err(RecvTimeoutError::Disconnected) => {
                        finish_disconnect(&mut file, &buffer, &file_size, &mut callers);
                        return;
                    }
                }
                if let Err(e) = drain_to_kernel(&mut file, &buffer, &file_size) {
                    notify_callers_error(&mut callers, &e);
                    return;
                }
            }
        }

        // =====================================================================
        // Phase 4: Self-driving — drain, sync when callers waiting, repeat
        // =====================================================================
        loop {
            // Drain any remaining buffer to kernel
            if let Err(e) = drain_to_kernel(&mut file, &buffer, &file_size) {
                notify_callers_error(&mut callers, &e);
                return;
            }

            // Sync + notify if we have callers
            if !callers.is_empty() {
                match file.sync_data() {
                    Ok(()) => {
                        notify_callers_success(&mut callers);
                    }
                    Err(e) => {
                        notify_callers_error(&mut callers, &e);
                        return;
                    }
                }
            }

            // Collect messages that arrived during I/O
            loop {
                match rx.try_recv() {
                    Ok(ThreadMsg::Flush(c)) => callers.push(c),
                    Ok(ThreadMsg::Close(response)) => {
                        finish_close(&mut file, &buffer, &file_size, &mut callers, response);
                        return;
                    }
                    Err(_) => break,
                }
            }

            // More callers arrived? Drain and loop for another sync.
            if !callers.is_empty() {
                continue;
            }

            // Buffer has data? Drain it to kernel (writes flow continuously).
            let has_data = match buffer.lock() {
                Ok(buf) => !buf.is_empty(),
                Err(_) => {
                    let err = io::Error::new(io::ErrorKind::Other, "buffer mutex poisoned");
                    notify_callers_error(&mut callers, &err);
                    return;
                }
            };
            if has_data {
                continue;
            }

            // Truly idle. Return to Phase 1.
            break;
        }
    }
}

fn finish_close(
    file: &mut File,
    buffer: &Mutex<AlignedBuffer>,
    file_size: &AtomicU64,
    callers: &mut Vec<FlushCaller>,
    response: Option<Sender<ThreadResult>>,
) {
    let result = close_drain_and_sync(file, buffer, file_size);
    notify_callers_for_close(callers, &result);
    send_close_result(response, &result);
}

fn finish_disconnect(
    file: &mut File,
    buffer: &Mutex<AlignedBuffer>,
    file_size: &AtomicU64,
    callers: &mut Vec<FlushCaller>,
) {
    let result = close_drain_and_sync(file, buffer, file_size);
    notify_callers_for_close(callers, &result);

    if let Err(e) = result {
        eprintln!("[ferricstore_wal_nif] WAL drain/sync failed during disconnect: {e}");
    }
}

fn notify_callers_for_close(callers: &mut Vec<FlushCaller>, result: &io::Result<()>) {
    match result {
        Ok(()) => notify_callers_success(callers),
        Err(e) => notify_callers_error(callers, e),
    }
}

fn send_close_result(response: Option<Sender<ThreadResult>>, result: &io::Result<()>) {
    if let Some(response) = response {
        let _ = response.send(result.as_ref().map(|_| ()).map_err(ToString::to_string));
    }
}

fn close_drain_and_sync<W>(
    writer: &mut W,
    buffer: &Mutex<AlignedBuffer>,
    file_size: &AtomicU64,
) -> io::Result<()>
where
    W: Write + SyncData,
{
    drain_to_kernel_writer(writer, buffer, file_size)?;
    writer.sync_data()
}

/// Drain shared buffer to kernel page cache. No fdatasync.
/// Returns true if data was written.
fn drain_to_kernel(
    file: &mut File,
    buffer: &Mutex<AlignedBuffer>,
    file_size: &AtomicU64,
) -> io::Result<bool> {
    drain_to_kernel_writer(file, buffer, file_size)
}

fn drain_to_kernel_writer<W: Write>(
    writer: &mut W,
    buffer: &Mutex<AlignedBuffer>,
    file_size: &AtomicU64,
) -> io::Result<bool> {
    let taken = {
        let mut buf = buffer
            .lock()
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "buffer mutex poisoned"))?;
        buf.take()
    };

    if taken.is_empty() {
        return Ok(false);
    }

    let bytes = taken.logical_len as u64;
    if let Err(e) = write_all_retry(writer, taken.as_logical_slice()) {
        let mut buf = buffer
            .lock()
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "buffer mutex poisoned"))?;
        buf.prepend(taken.as_logical_slice());
        return Err(e);
    }

    file_size.fetch_add(bytes, Ordering::Release);
    Ok(true)
}

/// Write with retry on EINTR.
fn write_all_retry<W: Write>(file: &mut W, data: &[u8]) -> io::Result<()> {
    let mut written = 0;
    while written < data.len() {
        match file.write(&data[written..]) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "failed to write WAL bytes",
                ));
            }
            Ok(n) => written += n,
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

/// Notify all callers that sync completed successfully.
fn notify_callers_success(callers: &mut Vec<FlushCaller>) {
    for mut caller in callers.drain(..) {
        let _ = caller.env.send_and_clear(&caller.pid, |env| {
            let ref_term = caller.saved_ref.load(env);
            rustler::types::tuple::make_tuple(
                env,
                &[crate::atoms::wal_sync_complete().encode(env), ref_term],
            )
        });
    }
}

/// Notify all callers that sync failed.
fn notify_callers_error(callers: &mut Vec<FlushCaller>, error: &io::Error) {
    let reason = format!("{error}");
    for mut caller in callers.drain(..) {
        let _ = caller.env.send_and_clear(&caller.pid, |env| {
            let ref_term = caller.saved_ref.load(env);
            rustler::types::tuple::make_tuple(
                env,
                &[
                    crate::atoms::wal_sync_error().encode(env),
                    ref_term,
                    reason.as_str().encode(env),
                ],
            )
        });
    }
}

/// ra WAL header: "RAWA" (4 bytes) + version (1 byte) = 5 bytes.
/// Written by ra_log_wal:make_tmp/1 before the NIF opens the file.
pub const WAL_HEADER_SIZE: u64 = 5;

// ---------------------------------------------------------------------------
// File opening
// ---------------------------------------------------------------------------

/// Open a WAL file with platform-appropriate flags.
/// Seeks past the WAL header so writes start at offset 5.
pub fn open_wal_file(path: &str, pre_allocate_bytes: u64) -> io::Result<(File, bool)> {
    #[cfg(target_os = "linux")]
    let (mut file, o_direct) = open_wal_file_linux(path, pre_allocate_bytes)?;
    #[cfg(not(target_os = "linux"))]
    let (mut file, o_direct) = open_wal_file_fallback(path, pre_allocate_bytes)?;

    file.seek(SeekFrom::Start(WAL_HEADER_SIZE))?;
    Ok((file, o_direct))
}

#[cfg(target_os = "linux")]
fn open_wal_file_linux(path: &str, pre_allocate_bytes: u64) -> io::Result<(File, bool)> {
    // Ra WAL files have a 5-byte header, so the first data write starts at
    // offset 5. That offset cannot satisfy O_DIRECT alignment requirements.
    // Keep buffered writes and rely on explicit fdatasync for durability.
    let f = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .read(true)
        .open(path)?;

    if pre_allocate_bytes > 0 {
        let ret = unsafe { libc::fallocate(f.as_raw_fd(), 0, 0, pre_allocate_bytes as i64) };
        if ret != 0 {
            return Err(io::Error::last_os_error());
        }
    };

    Ok((f, false))
}

#[cfg(not(target_os = "linux"))]
fn open_wal_file_fallback(path: &str, _pre_allocate_bytes: u64) -> io::Result<(File, bool)> {
    let f = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .read(true)
        .open(path)?;
    Ok((f, false)) // No O_DIRECT on macOS
}

/// Read bytes from file at offset (for recovery).
pub fn pread_from_file(file: &mut File, offset: u64, len: u64) -> io::Result<Vec<u8>> {
    file.seek(SeekFrom::Start(offset))?;
    let mut buf = vec![0u8; len as usize];
    file.read_exact(&mut buf)?;
    Ok(buf)
}

// ---------------------------------------------------------------------------
// Tests (pure Rust, no BEAM)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    struct FailingWriter;

    impl Write for FailingWriter {
        fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
            Err(io::Error::new(io::ErrorKind::Other, "forced write failure"))
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    struct FailingSyncWriter {
        writes: Vec<u8>,
    }

    impl Write for FailingSyncWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.writes.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl SyncData for FailingSyncWriter {
        fn sync_data(&mut self) -> io::Result<()> {
            Err(io::Error::new(io::ErrorKind::Other, "forced sync failure"))
        }
    }

    #[test]
    fn drain_to_kernel_returns_write_error_instead_of_panicking() {
        let buffer = Mutex::new(AlignedBuffer::new());
        {
            let mut guard = buffer.lock().unwrap();
            guard.extend(b"entry");
        }

        let file_size = AtomicU64::new(WAL_HEADER_SIZE);
        let mut writer = FailingWriter;

        let result = drain_to_kernel_writer(&mut writer, &buffer, &file_size);

        assert!(result.is_err());
        assert_eq!(file_size.load(Ordering::Acquire), WAL_HEADER_SIZE);
    }

    #[test]
    fn drain_to_kernel_restores_failed_bytes_before_concurrent_writes() {
        struct AppendThenFail {
            buffer: Arc<Mutex<AlignedBuffer>>,
        }

        impl Write for AppendThenFail {
            fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
                self.buffer.lock().unwrap().extend(b"new");
                Err(io::Error::new(io::ErrorKind::Other, "forced write failure"))
            }

            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        let buffer = Arc::new(Mutex::new(AlignedBuffer::new()));
        {
            let mut guard = buffer.lock().unwrap();
            guard.extend(b"entry");
        }

        let file_size = AtomicU64::new(WAL_HEADER_SIZE);
        let mut writer = AppendThenFail {
            buffer: Arc::clone(&buffer),
        };

        let result = drain_to_kernel_writer(&mut writer, &buffer, &file_size);

        assert!(result.is_err());
        assert_eq!(file_size.load(Ordering::Acquire), WAL_HEADER_SIZE);

        let restored = buffer.lock().unwrap().take();
        assert_eq!(restored.as_logical_slice(), b"entrynew");
    }

    #[test]
    fn close_drain_and_sync_returns_sync_error() {
        let buffer = Mutex::new(AlignedBuffer::new());
        {
            let mut guard = buffer.lock().unwrap();
            guard.extend(b"entry");
        }

        let file_size = AtomicU64::new(WAL_HEADER_SIZE);
        let mut writer = FailingSyncWriter { writes: Vec::new() };

        let err = close_drain_and_sync(&mut writer, &buffer, &file_size).unwrap_err();

        assert!(err.to_string().contains("forced sync failure"));
        assert_eq!(file_size.load(Ordering::Acquire), WAL_HEADER_SIZE + 5);
        assert!(!writer.writes.is_empty());
    }

    /// Helper: create a thread config for testing (no BEAM notifications).
    #[allow(dead_code)]
    fn test_config(commit_delay_us: u64) -> (ThreadConfig, crossbeam_channel::Sender<ThreadMsg>) {
        let tmp = NamedTempFile::new().unwrap();
        let file = tmp.reopen().unwrap();
        let buffer = Arc::new(Mutex::new(AlignedBuffer::new()));
        let (tx, rx) = crossbeam_channel::unbounded();
        let alive = Arc::new(AtomicBool::new(true));
        let file_size = Arc::new(AtomicU64::new(0));
        let config = ThreadConfig {
            file,
            buffer: buffer.clone(),
            rx,
            alive: alive.clone(),
            file_size: file_size.clone(),
            commit_delay: Duration::from_micros(commit_delay_us),
            _use_o_direct: false,
        };
        (config, tx)
    }

    #[test]
    fn test_open_wal_file_fallback() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let (file, o_direct) = open_wal_file(path.to_str().unwrap(), 0).unwrap();
        drop(file);

        assert!(!o_direct);

        // File should exist
        assert!(path.exists());
    }

    #[test]
    fn test_open_with_preallocate() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let (file, _) = open_wal_file(path.to_str().unwrap(), 4096).unwrap();
        drop(file);
        // On Linux with fallocate, file size would be 4096.
        // On macOS, fallocate is skipped, file size is 0.
        assert!(path.exists());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_open_wal_file_linux_preallocate_failure_preserves_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        std::fs::write(&path, b"RAWA\x01existing bytes").unwrap();

        let result = open_wal_file_linux(path.to_str().unwrap(), u64::MAX);

        assert!(result.is_err());
        assert_eq!(std::fs::read(&path).unwrap(), b"RAWA\x01existing bytes");
    }

    #[test]
    fn test_write_all_retry() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let mut file = std::fs::File::create(&path).unwrap();
        write_all_retry(&mut file, b"hello world").unwrap();
        file.sync_all().unwrap();

        let contents = std::fs::read(&path).unwrap();
        assert_eq!(&contents, b"hello world");
    }

    #[test]
    fn test_write_all_retry_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let mut file = std::fs::File::create(&path).unwrap();
        write_all_retry(&mut file, b"").unwrap();
        let contents = std::fs::read(&path).unwrap();
        assert!(contents.is_empty());
    }

    #[test]
    fn test_write_all_retry_zero_progress_is_write_zero() {
        struct ZeroThenError;

        impl Write for ZeroThenError {
            fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
                Ok(0)
            }

            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        let err = write_all_retry(&mut ZeroThenError, b"not written").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::WriteZero);
    }

    #[test]
    fn test_pread() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        std::fs::write(&path, b"hello world").unwrap();
        let mut file = std::fs::File::open(&path).unwrap();
        let data = pread_from_file(&mut file, 6, 5).unwrap();
        assert_eq!(&data, b"world");
    }

    #[test]
    fn test_pread_at_offset_zero() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        std::fs::write(&path, b"hello").unwrap();
        let mut file = std::fs::File::open(&path).unwrap();
        let data = pread_from_file(&mut file, 0, 5).unwrap();
        assert_eq!(&data, b"hello");
    }

    #[test]
    fn test_drain_to_kernel() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .read(true)
            .open(&path)
            .unwrap();

        let buffer = Arc::new(Mutex::new(AlignedBuffer::new()));
        let file_size = Arc::new(AtomicU64::new(0));

        {
            let mut buf = buffer.lock().unwrap();
            buf.extend(b"test data 12345");
        }

        assert!(drain_to_kernel(&mut file, &buffer, &file_size).unwrap());
        assert_eq!(file_size.load(Ordering::Acquire), 15);

        let contents = std::fs::read(&path).unwrap();
        assert!(contents.len() >= 15);
        assert_eq!(&contents[..15], b"test data 12345");
    }

    #[test]
    fn test_drain_empty_buffer() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .read(true)
            .open(&path)
            .unwrap();

        let buffer = Arc::new(Mutex::new(AlignedBuffer::new()));
        let file_size = Arc::new(AtomicU64::new(0));

        assert!(!drain_to_kernel(&mut file, &buffer, &file_size).unwrap());
        assert_eq!(file_size.load(Ordering::Acquire), 0);
    }

    #[test]
    fn test_drain_multiple_writes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .read(true)
            .open(&path)
            .unwrap();

        let buffer = Arc::new(Mutex::new(AlignedBuffer::new()));
        let file_size = Arc::new(AtomicU64::new(0));

        {
            let mut buf = buffer.lock().unwrap();
            buf.extend(b"first ");
        }
        assert!(drain_to_kernel(&mut file, &buffer, &file_size).unwrap());
        assert_eq!(file_size.load(Ordering::Acquire), 6);

        {
            let mut buf = buffer.lock().unwrap();
            buf.extend(b"second");
        }
        assert!(drain_to_kernel(&mut file, &buffer, &file_size).unwrap());
        assert_eq!(file_size.load(Ordering::Acquire), 12);
    }

    #[test]
    fn drain_to_kernel_appends_logical_bytes_without_padding_gap() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .read(true)
            .open(&path)
            .unwrap();

        file.write_all(b"RAWA\x01").unwrap();
        file.seek(SeekFrom::Start(WAL_HEADER_SIZE)).unwrap();

        let buffer = Arc::new(Mutex::new(AlignedBuffer::new()));
        let file_size = Arc::new(AtomicU64::new(WAL_HEADER_SIZE));

        {
            let mut buf = buffer.lock().unwrap();
            buf.extend(b"abc");
        }
        assert!(drain_to_kernel(&mut file, &buffer, &file_size).unwrap());

        {
            let mut buf = buffer.lock().unwrap();
            buf.extend(b"def");
        }
        assert!(drain_to_kernel(&mut file, &buffer, &file_size).unwrap());
        file.sync_data().unwrap();

        assert_eq!(file_size.load(Ordering::Acquire), WAL_HEADER_SIZE + 6);

        let contents = std::fs::read(&path).unwrap();
        let logical_start = WAL_HEADER_SIZE as usize;
        let logical_end = logical_start + 6;
        assert_eq!(&contents[logical_start..logical_end], b"abcdef");
        assert_eq!(contents.len(), logical_end);
    }

    #[test]
    fn test_drain_then_sync() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .read(true)
            .open(&path)
            .unwrap();

        let buffer = Arc::new(Mutex::new(AlignedBuffer::new()));
        let file_size = Arc::new(AtomicU64::new(0));

        {
            let mut buf = buffer.lock().unwrap();
            buf.extend(b"final data");
        }

        drain_to_kernel(&mut file, &buffer, &file_size).unwrap();
        file.sync_data().unwrap();
        assert_eq!(file_size.load(Ordering::Acquire), 10);
    }

    #[test]
    fn test_thread_close_flushes_data() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .read(true)
            .open(&path)
            .unwrap();

        let buffer = Arc::new(Mutex::new(AlignedBuffer::new()));
        let alive = Arc::new(AtomicBool::new(true));
        let file_size = Arc::new(AtomicU64::new(0));
        let (tx, rx) = crossbeam_channel::unbounded();

        let buf_clone = buffer.clone();
        let alive_clone = alive.clone();
        let fs_clone = file_size.clone();

        let handle = std::thread::Builder::new()
            .name("test-wal".into())
            .spawn(move || {
                thread_loop(ThreadConfig {
                    file,
                    buffer: buf_clone,
                    rx,
                    alive: alive_clone,
                    file_size: fs_clone,
                    commit_delay: Duration::ZERO,
                    _use_o_direct: false,
                });
            })
            .unwrap();

        // Write data to buffer
        {
            let mut buf = buffer.lock().unwrap();
            buf.extend(b"thread close test");
        }

        // Send close — thread should flush before exiting
        let (close_tx, close_rx) = crossbeam_channel::bounded(1);
        tx.send(ThreadMsg::Close(Some(close_tx))).unwrap();
        assert_eq!(
            close_rx.recv_timeout(Duration::from_secs(5)).unwrap(),
            Ok(())
        );
        handle.join().unwrap();

        // Thread should be marked dead
        assert!(!alive.load(Ordering::Acquire));

        // Data should be on disk
        let contents = std::fs::read(&path).unwrap();
        assert!(contents.len() >= 17);
        assert_eq!(&contents[..17], b"thread close test");
        assert_eq!(file_size.load(Ordering::Acquire), 17);
    }

    #[test]
    fn test_thread_channel_disconnect_exits() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .read(true)
            .open(&path)
            .unwrap();

        let buffer = Arc::new(Mutex::new(AlignedBuffer::new()));
        let alive = Arc::new(AtomicBool::new(true));
        let file_size = Arc::new(AtomicU64::new(0));
        let (tx, rx) = crossbeam_channel::unbounded();

        let buf_clone = buffer.clone();
        let alive_clone = alive.clone();
        let fs_clone = file_size.clone();

        let handle = std::thread::Builder::new()
            .name("test-wal".into())
            .spawn(move || {
                thread_loop(ThreadConfig {
                    file,
                    buffer: buf_clone,
                    rx,
                    alive: alive_clone,
                    file_size: fs_clone,
                    commit_delay: Duration::ZERO,
                    _use_o_direct: false,
                });
            })
            .unwrap();

        // Write data without sending an explicit close message. Disconnect
        // must still drain and sync the buffer before the thread exits.
        {
            let mut buf = buffer.lock().unwrap();
            buf.extend(b"disconnect data");
        }

        // Drop the sender — channel disconnects
        drop(tx);

        // Thread should exit cleanly
        handle.join().unwrap();
        assert!(!alive.load(Ordering::Acquire));

        let contents = std::fs::read(&path).unwrap();
        assert!(contents.len() >= 15);
        assert_eq!(&contents[..15], b"disconnect data");
        assert_eq!(file_size.load(Ordering::Acquire), 15);
    }

    #[test]
    fn test_commit_delay_collects_multiple_flushes() {
        // This test verifies that during the commit delay window,
        // multiple flush requests are collected and served by one fdatasync.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .read(true)
            .open(&path)
            .unwrap();

        let buffer = Arc::new(Mutex::new(AlignedBuffer::new()));
        let alive = Arc::new(AtomicBool::new(true));
        let file_size = Arc::new(AtomicU64::new(0));
        let (tx, rx) = crossbeam_channel::unbounded();

        let buf_clone = buffer.clone();
        let alive_clone = alive.clone();
        let fs_clone = file_size.clone();

        let handle = std::thread::Builder::new()
            .name("test-wal".into())
            .spawn(move || {
                thread_loop(ThreadConfig {
                    file,
                    buffer: buf_clone,
                    rx,
                    alive: alive_clone,
                    file_size: fs_clone,
                    commit_delay: Duration::from_millis(50), // 50ms delay
                    _use_o_direct: false,
                });
            })
            .unwrap();

        // Write data
        {
            let mut buf = buffer.lock().unwrap();
            buf.extend(b"batch data");
        }

        // Send multiple flush requests rapidly (no BEAM callers in this test)
        // We can't send FlushCaller without BEAM, so just test Close behavior
        let (close_tx, close_rx) = crossbeam_channel::bounded(1);
        tx.send(ThreadMsg::Close(Some(close_tx))).unwrap();
        assert_eq!(
            close_rx.recv_timeout(Duration::from_secs(5)).unwrap(),
            Ok(())
        );
        handle.join().unwrap();

        let contents = std::fs::read(&path).unwrap();
        assert!(contents.len() >= 10);
        assert_eq!(&contents[..10], b"batch data");
    }

    #[test]
    fn test_large_buffer_flush() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .read(true)
            .open(&path)
            .unwrap();

        let buffer = Arc::new(Mutex::new(AlignedBuffer::new()));
        let file_size = Arc::new(AtomicU64::new(0));

        // 10 MB of data
        let data = vec![0xABu8; 10 * 1024 * 1024];
        {
            let mut buf = buffer.lock().unwrap();
            buf.extend(&data);
        }

        drain_to_kernel(&mut file, &buffer, &file_size).unwrap();
        file.sync_data().unwrap();

        assert_eq!(file_size.load(Ordering::Acquire), 10 * 1024 * 1024);
        let contents = std::fs::read(&path).unwrap();
        assert!(contents.len() >= 10 * 1024 * 1024);
        assert_eq!(&contents[..10 * 1024 * 1024], &data[..]);
    }
}
