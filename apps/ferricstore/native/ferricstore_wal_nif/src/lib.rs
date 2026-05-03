// ferricstore_wal_nif — Rust NIF WAL I/O layer for ra_log_wal
//
// Hot write/sync NIF functions run on normal BEAM schedulers (<1μs each).
// Blocking I/O (write + fdatasync) runs on a dedicated background thread.
// Startup/recovery/shutdown calls stay on Normal schedulers; long-running
// runtime I/O belongs on the background thread/Tokio path, not dirty schedulers.
//
// Architecture:
//   NIF calls → Mutex<AlignedBuffer> (shared) → FlushRequest channel → Background thread
//   Background thread: commit_delay → write() → fdatasync() → notify caller

#![allow(clippy::needless_pass_by_value)] // Rustler NIF convention

mod aligned_buffer;
mod background_thread;
mod wal_handle;

#[cfg(test)]
mod tests;

use rustler::{Atom, Binary, Encoder, Env, LocalPid, NifResult, OwnedBinary, ResourceArc, Term};
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
/// commit_delay_us: microseconds to wait before fdatasync (default 200)
/// pre_allocate_bytes: fallocate size (default 256MB)
/// max_buffer_bytes: backpressure limit (default 64MB)
#[rustler::nif(schedule = "Normal")]
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

/// Write pre-formatted iodata to the WAL buffer.
/// Copies bytes into the shared aligned buffer. Does NOT write to disk.
/// Returns :ok | {:error, :wal_thread_dead} | {:error, :backpressure}
#[rustler::nif(schedule = "Normal")]
fn write(handle: ResourceArc<WalHandle>, iodata: Term) -> NifResult<Atom> {
    handle.check_alive()?;

    // Collect iodata into bytes
    let bytes = iodata_to_bytes(iodata)?;

    handle.buffer_write(&bytes)?;
    Ok(atoms::ok())
}

/// Request async fdatasync. Background thread will flush buffer to disk,
/// fdatasync, and send {wal_sync_complete, Ref} to CallerPid.
/// Returns :ok immediately.
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

    handle.request_sync(caller_pid, owned_env, saved_ref)?;
    Ok(atoms::ok())
}

/// Close the WAL file. Blocks until background thread drains, syncs, and exits.
/// Timeout: 30 seconds.
#[rustler::nif(schedule = "Normal")]
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
#[rustler::nif(schedule = "Normal")]
fn pread<'a>(
    env: Env<'a>,
    handle: ResourceArc<WalHandle>,
    offset: u64,
    len: u64,
) -> NifResult<(Atom, Binary<'a>)> {
    let data = handle.pread(offset, len)?;
    let mut binary =
        OwnedBinary::new(data.len()).ok_or(rustler::Error::Term(Box::new("alloc_failed")))?;
    binary.as_mut_slice().copy_from_slice(&data);
    Ok((atoms::ok(), binary.release(env)))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Convert Erlang iodata (binary or iolist) to bytes.
///
/// Uses rustler's `decode_as_binary`, which calls `enif_inspect_iolist_as_binary`
/// under the hood. That function correctly handles every shape of valid iodata —
/// flat binaries, deeply nested lists, improper lists like `[H | <<tail>>]`, and
/// integer bytes. Our previous hand-rolled flattener returned `:badarg` on
/// improper lists, which crashed the ra WAL on every flush of a real batch.
fn iodata_to_bytes(term: Term) -> NifResult<Vec<u8>> {
    let bin = term.decode_as_binary()?;
    Ok(bin.as_slice().to_vec())
}

// ---------------------------------------------------------------------------
// NIF Registration
// ---------------------------------------------------------------------------

#[allow(non_local_definitions)]
fn on_load(env: Env, _info: Term) -> bool {
    let _ = rustler::resource!(WalHandle, env);
    true
}

rustler::init!("ferricstore_wal_nif", load = on_load);
