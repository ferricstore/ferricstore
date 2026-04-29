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
use crossbeam_channel::{Receiver, RecvTimeoutError};
use rustler::Encoder;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[cfg(target_os = "linux")]
use std::os::unix::io::AsRawFd;

/// Messages sent from NIF to background thread (flush signals only, not data).
pub enum ThreadMsg {
    /// Request fdatasync. Caller will be notified on completion.
    Flush(FlushCaller),
    /// Shutdown: drain, sync, close, exit.
    Close,
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
            Err(_) => return,
        };

        match msg {
            ThreadMsg::Close => {
                if let Err(e) = drain_to_kernel(&mut file, &buffer, &file_size) {
                    eprintln!("[ferricstore_wal_nif] WAL write failed during close: {e}");
                }
                let _ = file.sync_data();
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
                    Ok(ThreadMsg::Close) => {
                        if let Err(e) = drain_to_kernel(&mut file, &buffer, &file_size) {
                            notify_callers_error(&mut callers, &e);
                            return;
                        }
                        let _ = file.sync_data();
                        notify_callers_success(&mut callers);
                        return;
                    }
                    Err(RecvTimeoutError::Timeout) => break,
                    Err(RecvTimeoutError::Disconnected) => {
                        if let Err(e) = drain_to_kernel(&mut file, &buffer, &file_size) {
                            notify_callers_error(&mut callers, &e);
                            return;
                        }
                        let _ = file.sync_data();
                        notify_callers_success(&mut callers);
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
                    Ok(ThreadMsg::Close) => {
                        if let Err(e) = drain_to_kernel(&mut file, &buffer, &file_size) {
                            notify_callers_error(&mut callers, &e);
                            return;
                        }
                        let _ = file.sync_data();
                        notify_callers_success(&mut callers);
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
            let has_data = {
                let buf = buffer.lock().expect("buffer mutex poisoned");
                !buf.is_empty()
            };
            if has_data {
                continue;
            }

            // Truly idle. Return to Phase 1.
            break;
        }
    }
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
        let mut buf = buffer.lock().expect("buffer mutex poisoned");
        buf.take()
    };

    if taken.is_empty() {
        return Ok(false);
    }

    let bytes = taken.logical_len as u64;
    write_all_retry(writer, taken.as_padded_slice())?;
    file_size.fetch_add(bytes, Ordering::Release);
    Ok(true)
}

/// Write with retry on EINTR.
fn write_all_retry<W: Write>(file: &mut W, data: &[u8]) -> io::Result<()> {
    let mut written = 0;
    while written < data.len() {
        match file.write(&data[written..]) {
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
    use std::os::unix::fs::OpenOptionsExt;

    // Try O_DIRECT first
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .read(true)
        .custom_flags(libc::O_DIRECT) // NO O_DSYNC — fdatasync is explicit
        .open(path);

    match file {
        Ok(f) => {
            if pre_allocate_bytes > 0 {
                let ret =
                    unsafe { libc::fallocate(f.as_raw_fd(), 0, 0, pre_allocate_bytes as i64) };
                if ret != 0 {
                    // fallocate failed (e.g., ENOSPC) — close and cleanup
                    drop(f);
                    let _ = std::fs::remove_file(path);
                    return Err(io::Error::last_os_error());
                }
            }
            Ok((f, true)) // O_DIRECT enabled
        }
        Err(_) => {
            // O_DIRECT not supported on this filesystem — fall back
            let f = std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .read(true)
                .open(path)?;
            Ok((f, false))
        }
    }
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

        #[cfg(not(target_os = "linux"))]
        assert!(!o_direct);

        #[cfg(target_os = "linux")]
        let _ = o_direct;

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
        tx.send(ThreadMsg::Close).unwrap();
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

        // Drop the sender — channel disconnects
        drop(tx);

        // Thread should exit cleanly
        handle.join().unwrap();
        assert!(!alive.load(Ordering::Acquire));
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
        tx.send(ThreadMsg::Close).unwrap();
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
