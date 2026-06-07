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
    pub target: FlushTarget,
    pub commit_delay: Duration,
}

pub enum FlushTarget {
    Beam {
        pid: rustler::LocalPid,
        env: rustler::OwnedEnv,
        saved_ref: rustler::env::SavedTerm,
    },
    #[cfg(test)]
    Test(Sender<u64>),
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
    _commit_delay: Duration,
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
                let commit_delay = caller.commit_delay;
                callers.push(caller);
                run_sync_cycle(
                    &mut file,
                    &buffer,
                    &file_size,
                    &rx,
                    &mut callers,
                    commit_delay,
                );
                continue;
            }
        }
    }
}

fn run_sync_cycle<W>(
    file: &mut W,
    buffer: &Arc<Mutex<AlignedBuffer>>,
    file_size: &Arc<AtomicU64>,
    rx: &Receiver<ThreadMsg>,
    callers: &mut Vec<FlushCaller>,
    commit_delay: Duration,
) where
    W: Write + SyncData,
{
    // =====================================================================
    // Phase 2: Drain buffer to kernel immediately (before sync decision)
    // =====================================================================
    if let Err(e) = drain_to_kernel(file, buffer, file_size) {
        notify_callers_error(callers, &e);
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
                    finish_close(file, buffer, file_size, callers, response);
                    return;
                }
                Err(RecvTimeoutError::Timeout) => break,
                Err(RecvTimeoutError::Disconnected) => {
                    finish_disconnect(file, buffer, file_size, callers);
                    return;
                }
            }
            if let Err(e) = drain_to_kernel(file, buffer, file_size) {
                notify_callers_error(callers, &e);
                return;
            }
        }
    }

    // =====================================================================
    // Phase 4: one Erlang-owned sync cycle
    // =====================================================================
    // Erlang owns sync_in_flight and the pending/syncing frontier. Do not
    // self-drive extra syncs here: Flush messages that arrive during fdatasync
    // must stay queued until Erlang observes this completion and explicitly
    // schedules the next sync cycle.
    if let Err(e) = drain_to_kernel(file, buffer, file_size) {
        notify_callers_error(callers, &e);
        return;
    }

    if !callers.is_empty() {
        let synced_position = file_size.load(Ordering::Acquire);
        match file.sync_data() {
            Ok(()) => {
                notify_callers_success(callers, synced_position);
            }
            Err(e) => {
                notify_callers_error(callers, &e);
            }
        }
    }
}

fn finish_close<W>(
    file: &mut W,
    buffer: &Mutex<AlignedBuffer>,
    file_size: &AtomicU64,
    callers: &mut Vec<FlushCaller>,
    response: Option<Sender<ThreadResult>>,
) where
    W: Write + SyncData,
{
    let result = close_drain_and_sync(file, buffer, file_size);
    notify_callers_for_close(callers, &result);
    send_close_result(response, &result);
}

fn finish_disconnect<W>(
    file: &mut W,
    buffer: &Mutex<AlignedBuffer>,
    file_size: &AtomicU64,
    callers: &mut Vec<FlushCaller>,
) where
    W: Write + SyncData,
{
    let result = close_drain_and_sync(file, buffer, file_size);
    notify_callers_for_close(callers, &result);

    if let Err(e) = result {
        eprintln!("[ferricstore_wal_nif] WAL drain/sync failed during disconnect: {e}");
    }
}

fn notify_callers_for_close(callers: &mut Vec<FlushCaller>, result: &io::Result<u64>) {
    match result {
        Ok(synced_position) => notify_callers_success(callers, *synced_position),
        Err(e) => notify_callers_error(callers, e),
    }
}

fn send_close_result(response: Option<Sender<ThreadResult>>, result: &io::Result<u64>) {
    if let Some(response) = response {
        let _ = response.send(result.as_ref().map(|_| ()).map_err(ToString::to_string));
    }
}

fn close_drain_and_sync<W>(
    writer: &mut W,
    buffer: &Mutex<AlignedBuffer>,
    file_size: &AtomicU64,
) -> io::Result<u64>
where
    W: Write + SyncData,
{
    drain_to_kernel_writer(writer, buffer, file_size)?;
    let synced_position = file_size.load(Ordering::Acquire);
    writer.sync_data()?;
    Ok(synced_position)
}

/// Drain shared buffer to kernel page cache. No fdatasync.
/// Returns true if data was written.
fn drain_to_kernel<W>(
    file: &mut W,
    buffer: &Mutex<AlignedBuffer>,
    file_size: &AtomicU64,
) -> io::Result<bool>
where
    W: Write,
{
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
fn notify_callers_success(callers: &mut Vec<FlushCaller>, synced_position: u64) {
    for caller in callers.drain(..) {
        match caller.target {
            FlushTarget::Beam {
                pid,
                mut env,
                saved_ref,
            } => {
                let _ = env.send_and_clear(&pid, |env| {
                    let ref_term = saved_ref.load(env);
                    rustler::types::tuple::make_tuple(
                        env,
                        &[
                            crate::atoms::wal_sync_complete().encode(env),
                            ref_term,
                            synced_position.encode(env),
                        ],
                    )
                });
            }
            #[cfg(test)]
            FlushTarget::Test(tx) => {
                let _ = tx.send(synced_position);
            }
        }
    }
}

/// Notify all callers that sync failed.
fn notify_callers_error(callers: &mut Vec<FlushCaller>, error: &io::Error) {
    let reason = format!("{error}");
    for caller in callers.drain(..) {
        match caller.target {
            FlushTarget::Beam {
                pid,
                mut env,
                saved_ref,
            } => {
                let _ = env.send_and_clear(&pid, |env| {
                    let ref_term = saved_ref.load(env);
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
            #[cfg(test)]
            FlushTarget::Test(_tx) => {}
        }
    }
}

/// WARaft segment logs start at byte 0.
pub const WAL_HEADER_SIZE: u64 = 0;

// ---------------------------------------------------------------------------
// File opening
// ---------------------------------------------------------------------------

/// Open an append log file with platform-appropriate flags.
pub fn open_wal_file(path: &str, pre_allocate_bytes: u64) -> io::Result<(File, bool)> {
    #[cfg(target_os = "linux")]
    let (mut file, o_direct) = open_wal_file_linux(path, pre_allocate_bytes)?;
    #[cfg(not(target_os = "linux"))]
    let (mut file, o_direct) = open_wal_file_fallback(path, pre_allocate_bytes)?;

    file.seek(SeekFrom::Start(WAL_HEADER_SIZE))?;
    Ok((file, o_direct))
}

/// Open a generic append log file and seek to the supplied logical end.
///
/// WARaft segment files are self-framed from byte 0. Segment preallocation is handled separately with
/// KEEP_SIZE so recovery never scans zero-filled preallocated trailers.
pub fn open_raw_append_file(path: &str, start_offset: u64) -> io::Result<(File, bool)> {
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .read(true)
        .open(path)?;

    file.seek(SeekFrom::Start(start_offset))?;
    Ok((file, false))
}

/// Reserve disk space without changing the logical file length.
///
/// Segment logs append framed records and recover by scanning until EOF. Extending
/// the visible file length during preallocation would leave zero-filled trailer
/// bytes that look like a torn/corrupt record. Linux has a keep-size fallocate
/// mode for this; other platforms keep the call as a safe no-op.
pub fn preallocate_keep_size_path(path: &str, bytes: u64) -> io::Result<()> {
    if bytes == 0 {
        return Ok(());
    }

    #[cfg(target_os = "linux")]
    {
        let f = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .read(true)
            .open(path)?;

        let ret = unsafe {
            libc::fallocate(
                f.as_raw_fd(),
                libc::FALLOC_FL_KEEP_SIZE,
                0,
                bytes as libc::off_t,
            )
        };

        if ret != 0 {
            return Err(io::Error::last_os_error());
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = path;
    }

    Ok(())
}

#[cfg(target_os = "linux")]
fn open_wal_file_linux(path: &str, pre_allocate_bytes: u64) -> io::Result<(File, bool)> {
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
    include!("sections/background_thread_tests.rs");
}
