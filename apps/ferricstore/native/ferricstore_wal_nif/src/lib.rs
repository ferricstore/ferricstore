// ferricstore_wal_nif -- Rust NIF append-log I/O layer for WARaft segments
//
// Sync/position submissions run on normal BEAM schedulers (<1μs each).
// WAL writes decode and copy caller-sized iodata on dirty CPU schedulers.
// Blocking I/O (write + fdatasync) runs on a dedicated background thread.
// Startup/recovery/shutdown filesystem calls use dirty I/O schedulers. Runtime
// sync submission stays non-blocking on normal schedulers while the dedicated
// background thread owns write() + fdatasync().
//
// Architecture:
//   NIF calls → Mutex<AlignedBuffer> (shared) → FlushRequest channel → Background thread
//   Background thread: adaptive commit_delay → write() → fdatasync() → notify caller

#![allow(clippy::needless_pass_by_value)] // Rustler NIF convention
#![allow(clippy::io_other_error)] // Keep compatibility with older Rust toolchains.

mod aligned_buffer;
mod background_thread;
mod wal_handle;

#[cfg(test)]
mod tests;

use rustler::{Atom, Binary, Encoder, Env, LocalPid, NifResult, OwnedBinary, ResourceArc, Term};
use std::time::Duration;
use wal_handle::WalHandle;

// WalHandle is registered as a NIF resource via `rustler::resource!` in
// the on_load callback below. The macro auto-implements `Resource`; no
// manual impl is needed (and would conflict with the macro).

mod atoms {
    rustler::atoms! {
        ok,
        error,
        wal_sync_complete,
        wal_sync_error,
        wal_thread_dead,
        backpressure,
        closed,
        timeout,
    }
}

// ---------------------------------------------------------------------------
// NIF Functions
// ---------------------------------------------------------------------------

/// Open a WAL file. Spawns background I/O thread.
/// commit_delay_us: maximum adaptive microseconds to wait before fdatasync (default 6000)
/// pre_allocate_bytes: fallocate size (default 256MB)
/// max_buffer_bytes: backpressure limit (default 64MB)
#[rustler::nif(schedule = "DirtyIo")]
fn open(
    path: String,
    commit_delay_us: u64,
    pre_allocate_bytes: u64,
    max_buffer_bytes: u64,
) -> NifResult<(Atom, ResourceArc<WalHandle>)> {
    match WalHandle::open(path, commit_delay_us, pre_allocate_bytes, max_buffer_bytes) {
        Ok(handle) => Ok((atoms::ok(), ResourceArc::new(handle))),
        Err(e) => Err(rustler::Error::Term(Box::new(format!("{e}")))),
    }
}

/// Open a generic append log file from byte 0.
#[rustler::nif(schedule = "DirtyIo")]
fn open_raw_append(
    path: String,
    commit_delay_us: u64,
    max_buffer_bytes: u64,
    start_offset: u64,
) -> NifResult<(Atom, ResourceArc<WalHandle>)> {
    match WalHandle::open_raw_append(path, commit_delay_us, max_buffer_bytes, start_offset) {
        Ok(handle) => Ok((atoms::ok(), ResourceArc::new(handle))),
        Err(e) => Err(rustler::Error::Term(Box::new(format!("{e}")))),
    }
}

/// Write pre-formatted iodata to the WAL buffer.
/// Copies bytes into the shared aligned buffer. Does NOT write to disk.
/// Returns :ok | {:error, :wal_thread_dead} | {:error, :backpressure}
#[rustler::nif(schedule = "DirtyCpu")]
fn write(handle: ResourceArc<WalHandle>, iodata: Term) -> NifResult<Atom> {
    handle.check_alive()?;

    let bytes = iodata.decode_as_binary()?;
    handle.buffer_write(bytes.as_slice())?;
    Ok(atoms::ok())
}

/// Request async fdatasync using the handle's default delay.
#[rustler::nif(schedule = "Normal")]
#[allow(unused_variables)]
fn sync(
    env: Env,
    handle: ResourceArc<WalHandle>,
    caller_pid: LocalPid,
    ref_term: Term<'_>,
) -> NifResult<Atom> {
    handle.check_alive()?;

    // Save the ref in an OwnedEnv so it survives past this NIF call
    let owned_env = rustler::OwnedEnv::new();
    let saved_ref = owned_env.save(ref_term);

    handle.request_sync(caller_pid, owned_env, saved_ref, handle.commit_delay())?;
    Ok(atoms::ok())
}

/// Request async fdatasync with a per-sync delay selected by the Erlang WAL.
/// Returns :ok immediately; the background thread replies after fdatasync.
#[rustler::nif(schedule = "Normal")]
#[allow(unused_variables)]
fn sync_with_delay(
    env: Env,
    handle: ResourceArc<WalHandle>,
    caller_pid: LocalPid,
    ref_term: Term<'_>,
    delay_us: u64,
) -> NifResult<Atom> {
    handle.check_alive()?;

    let owned_env = rustler::OwnedEnv::new();
    let saved_ref = owned_env.save(ref_term);

    handle.request_sync(
        caller_pid,
        owned_env,
        saved_ref,
        Duration::from_micros(delay_us),
    )?;
    Ok(atoms::ok())
}

/// Close the WAL file. Blocks until background thread drains, syncs, and exits.
#[rustler::nif(schedule = "DirtyIo")]
fn close<'a>(env: Env<'a>, handle: ResourceArc<WalHandle>) -> NifResult<Term<'a>> {
    match handle.close() {
        Ok(()) => Ok(atoms::ok().encode(env)),
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

/// Returns current logical file size in bytes. No syscall — reads atomic.
#[rustler::nif(schedule = "Normal")]
fn position(handle: ResourceArc<WalHandle>) -> NifResult<(Atom, u64)> {
    Ok((atoms::ok(), handle.file_size()))
}

/// Read bytes from WAL at offset. Used during recovery.
#[rustler::nif(schedule = "DirtyIo")]
fn pread<'a>(
    env: Env<'a>,
    handle: ResourceArc<WalHandle>,
    offset: u64,
    len: u64,
) -> NifResult<(Atom, Binary<'a>)> {
    let read_len = handle.pread_len(offset, len)?;
    let mut binary =
        OwnedBinary::new(read_len).ok_or(rustler::Error::Term(Box::new("alloc_failed")))?;
    handle.pread_into(offset, binary.as_mut_slice())?;
    Ok((atoms::ok(), binary.release(env)))
}

/// Reserve disk space for an external segmented log without changing file size.
#[rustler::nif(schedule = "DirtyIo")]
fn preallocate_keep_size<'a>(env: Env<'a>, path: String, bytes: u64) -> NifResult<Term<'a>> {
    match background_thread::preallocate_keep_size_path(&path, bytes) {
        Ok(()) => Ok(atoms::ok().encode(env)),
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// NIF Registration
// ---------------------------------------------------------------------------

#[allow(non_local_definitions)]
fn on_load(env: Env, _info: Term) -> bool {
    let _ = rustler::resource!(WalHandle, env);
    true
}

rustler::init!("ferricstore_wal_nif", load = on_load);
