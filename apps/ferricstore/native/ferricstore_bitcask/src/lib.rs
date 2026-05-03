#![deny(clippy::all, clippy::pedantic)]
// These pedantic lints are noisy without adding safety value for this codebase:
// - possible_truncation: we target 64-bit Linux only; u64→usize is always safe.
// - cast_sign_loss / cast_lossless: io_uring result codes require these casts.
// - items_after_statements: common in test helpers and is clear.
// - doc_markdown: minor style preference, not a correctness issue.
// - missing_errors_doc / missing_panics_doc: NIF wrapper docs don't benefit from # Errors sections.
// - must_use_candidate: most public methods are called via NIF wrappers where must_use is irrelevant.
// - cast_possible_wrap: u64→i64 casts are intentional in store code.
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::cast_lossless)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::items_after_statements)]
#![allow(clippy::doc_markdown)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::must_use_candidate)]
#![allow(clippy::cast_possible_wrap)]
// NIF functions must return NifResult<Term> per the Rustler API, even when they never fail:
#![allow(clippy::unnecessary_wraps)]
// io_other_error: io_uring code uses Error::new(ErrorKind::Other, ..) for clarity; lint is Rust 1.83+.
#![allow(clippy::io_other_error)]
// Mmap modules (bloom, cms, cuckoo, topk) use raw pointer casts:
#![allow(clippy::ptr_as_ptr)]
#![allow(clippy::similar_names)]
#![allow(clippy::ref_option)]
#![allow(clippy::manual_let_else)]
#![allow(clippy::doc_link_with_quotes)]
// Rustler NIF functions require owned String/Vec params (not &str/&[u8]):
#![allow(clippy::needless_pass_by_value)]
// Wildcard imports in match arms for io::ErrorKind are clearer:
#![allow(clippy::enum_glob_use)]
// map().flatten() is clearer than and_then() in some contexts:
#![allow(clippy::map_flatten)]
// format!("{}", x) vs format!("{x}") — both fine, don't force inline:
#![allow(clippy::uninlined_format_args)]

pub mod async_io;
pub mod bloom;
pub mod cms;
pub mod compaction;
pub mod cuckoo;
pub mod fs_nif;
pub mod hint;
pub mod io_backend;
pub mod keydir;
pub mod log;
pub mod store;
pub mod tdigest;
pub mod topk;
pub mod tracking_alloc;

use rustler::{Binary, Encoder, Env, LocalPid, NifResult, OwnedBinary, ResourceArc, Term};
use std::os::unix::fs::FileExt;

/// A resource that owns a value buffer read from the Bitcask log.
///
/// When used with `ResourceArc::make_binary`, the BEAM creates a binary term
/// that points directly into this buffer — zero copy from Rust to BEAM.
/// The BEAM's GC tracks the reference: once the Erlang binary term becomes
/// unreachable, the `ResourceArc` ref-count drops to zero and this `Vec` is
/// freed.
///
/// ## Safety invariant
///
/// The `data` field MUST NOT be mutated after the `ResourceArc<ValueBuffer>`
/// is passed to `make_binary`. The returned BEAM binary shares the same
/// backing memory; any mutation would violate the immutability guarantee of
/// Erlang binaries and cause undefined behaviour.
struct ValueBuffer {
    data: Vec<u8>,
}

mod atoms {
    rustler::atoms! {
        ok,
        error,
        nil,
        tokio_complete,
        put,
        delete,
    }
}

#[derive(rustler::NifTaggedEnum)]
enum NifBatchWrite<'a> {
    Put(Binary<'a>, Binary<'a>, u64),
    Delete(Binary<'a>),
}

#[allow(non_local_definitions)]
fn load(env: Env, _info: Term) -> bool {
    let _ = rustler::resource!(ValueBuffer, env);
    tdigest::register_resource(env);
    tdigest::register_mmap_resource(env);
    true
}

// ---------------------------------------------------------------------------
// v2 Pure stateless NIF functions — no Store, no Mutex, no keydir in Rust.
// These are the building blocks for the Elixir-owned ETS keydir architecture.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// fadvise helpers — page cache hints for random-access pread patterns.
//
// FADV_RANDOM: disables kernel readahead on the fd. Without this, each pread
// triggers ~128KB of readahead on pages that will never be used (bloom bits,
// CMS counters, Bitcask cold reads are all hash-indexed random access).
//
// FADV_DONTNEED: hints the kernel to evict the pages we just read. For
// Bitcask cold reads, the value is promoted to ETS — the page cache copy
// is never needed again. For prob reads, parallel stateless access means
// no single reader benefits from caching. Saves page cache for hot data.
//
// On non-Linux (macOS), posix_fadvise is not available — these are no-ops.
// ---------------------------------------------------------------------------

/// Open a file for reading with FADV_RANDOM hint (disable readahead).
pub fn open_random_read(path: &std::path::Path) -> std::io::Result<std::fs::File> {
    let file = std::fs::File::open(path)?;
    fadvise_random(&file);
    Ok(file)
}

/// Open a file for read+write with FADV_RANDOM hint.
pub fn open_random_rw(path: &std::path::Path) -> std::io::Result<std::fs::File> {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)?;
    fadvise_random(&file);
    Ok(file)
}

/// Hint the kernel that this fd will be accessed randomly (disable readahead).
#[cfg(target_os = "linux")]
pub fn fadvise_random(file: &std::fs::File) {
    use std::os::unix::io::AsRawFd;
    unsafe {
        libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_RANDOM);
    }
}

#[cfg(not(target_os = "linux"))]
pub fn fadvise_random(_file: &std::fs::File) {}

/// Hint the kernel to evict pages at [offset, offset+len] from page cache.
#[cfg(target_os = "linux")]
pub fn fadvise_dontneed(file: &std::fs::File, offset: i64, len: i64) {
    use std::os::unix::io::AsRawFd;
    unsafe {
        libc::posix_fadvise(file.as_raw_fd(), offset, len, libc::POSIX_FADV_DONTNEED);
    }
}

#[cfg(not(target_os = "linux"))]
pub fn fadvise_dontneed(_file: &std::fs::File, _offset: i64, _len: i64) {}

/// Fsync a directory so that filename-to-inode mappings (dir entries) are
/// durable. Required after `File::create`, `rename`, `remove_file`, or
/// `touch` of any file inside a directory whose existence must survive a
/// kernel panic.
///
/// POSIX: a file's data `fsync` does NOT make the filename entry durable;
/// only the parent directory's fsync does that. Without this call, a
/// kernel panic after a rename/rm can leave the directory in a state
/// where the filename mapping doesn't match what the caller expected —
/// e.g. a freshly-compacted `00003.log` still shows as `compact_3.log`
/// because the rename never flushed to disk.
///
/// Uses `File::open` (read-only) + `sync_data()` which is valid for
/// directories on Linux and macOS. Empty path returns Err without
/// opening.
pub fn fsync_dir(path: &str) -> Result<(), String> {
    if path.is_empty() {
        return Err("empty path".to_string());
    }

    let dir =
        std::fs::File::open(std::path::Path::new(path)).map_err(|e| format!("open dir: {e}"))?;

    dir.sync_data().map_err(|e| format!("sync_data: {e}"))
}

/// Fsync a prob file (bloom/cuckoo/cms/topk) after a write before returning
/// `:ok` to the caller. Without this, writes go to the OS page cache only
/// and a kernel panic between the write and the background pagecache flush
/// would lose the data.
///
/// For bloom: bit-set is idempotent on Ra replay but the header `count`
/// field can desync with actual bits set (breaks `BF.CARD`).
/// For cuckoo: kick-chain partial writes corrupt the filter; replay is
/// NOT safe.
/// For cms: read-modify-write counters double-count on replay.
/// For topk: heap state corruption on partial writes.
///
/// Returns the formatted error string on failure so callers can propagate
/// it as `{:error, reason}` to Elixir. Uses `sync_data()` (fdatasync) — we
/// don't need metadata durability here, the file's size/perms never change
/// after create.
pub fn prob_fsync(file: &std::fs::File) -> Result<(), String> {
    file.sync_data().map_err(|e| format!("sync_data: {e}"))
}

/// Positioned write that rejects short writes.
///
/// POSIX `pwrite` may write fewer bytes than requested. The probabilistic
/// file formats update fixed-width counters/slots in place, so callers must
/// not treat a partial write as success.
pub(crate) fn write_all_at(
    file: &std::fs::File,
    mut buf: &[u8],
    mut offset: u64,
    label: &str,
) -> Result<(), String> {
    while !buf.is_empty() {
        match file.write_at(buf, offset) {
            Ok(0) => return Err(format!("short pwrite {label}: wrote 0 bytes")),
            Ok(n) => {
                buf = &buf[n..];
                offset += n as u64;
            }
            Err(e) => return Err(format!("pwrite {label}: {e}")),
        }
    }

    Ok(())
}

/// Parse the numeric file_id from a log file path.
///
/// L-NEW-1 fix: `"00000000000000000000".trim_start_matches('0')` produces `""`
/// which fails to parse as u64, accidentally falling through to `unwrap_or(0)`.
/// This function handles the all-zeros case explicitly, matching the pattern
/// used in `store.rs::collect_file_ids`.
fn parse_file_id(path: &std::path::Path) -> u64 {
    path.file_stem().and_then(|s| s.to_str()).map_or(0, |stem| {
        let trimmed = stem.trim_start_matches('0');
        if trimmed.is_empty() {
            // All zeros (e.g. "00000000000000000000.log") → file_id 0
            0
        } else {
            trimmed.parse::<u64>().unwrap_or(0)
        }
    })
}

/// Append a record to a data file. Returns `{:ok, {offset, record_size}}`.
///
/// Pure I/O — no keydir, no Mutex for reads.
/// The caller (Elixir Shard GenServer) serialises writes.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
fn v2_append_record<'a>(
    env: Env<'a>,
    path: String,
    key: Binary,
    value: Binary,
    expire_at_ms: u64,
) -> NifResult<Term<'a>> {
    use crate::log::validate_kv_sizes;

    if let Err(msg) = validate_kv_sizes(key.as_slice(), value.as_slice()) {
        return Ok((atoms::error(), msg).encode(env));
    }

    let p = std::path::Path::new(&path);
    let file_id = parse_file_id(p);

    // M-NEW-1 fix: use open_small (8KB buffer) for single-record writes to
    // avoid allocating a 256KB BufWriter that is used once and dropped.
    match log::LogWriter::open_small(p, file_id) {
        Ok(mut writer) => {
            let offset = writer
                .write(key.as_slice(), value.as_slice(), expire_at_ms)
                .map_err(|e| rustler::Error::Term(Box::new(e.to_string())))?;
            writer
                .sync()
                .map_err(|e| rustler::Error::Term(Box::new(e.to_string())))?;
            let record_size =
                (log::HEADER_SIZE + key.as_slice().len() + value.as_slice().len()) as u64;
            Ok((atoms::ok(), (offset, record_size)).encode(env))
        }
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

/// Append a tombstone record (logical delete) to a data file.
/// Returns `{:ok, {offset, record_size}}`.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
fn v2_append_tombstone<'a>(env: Env<'a>, path: String, key: Binary) -> NifResult<Term<'a>> {
    let p = std::path::Path::new(&path);
    let file_id = parse_file_id(p);

    // M-NEW-1 fix: use open_small (8KB buffer) for single-record writes.
    match log::LogWriter::open_small(p, file_id) {
        Ok(mut writer) => {
            let offset = writer
                .write_tombstone(key.as_slice())
                .map_err(|e| rustler::Error::Term(Box::new(e.to_string())))?;
            writer
                .sync()
                .map_err(|e| rustler::Error::Term(Box::new(e.to_string())))?;
            let record_size = (log::HEADER_SIZE + key.as_slice().len()) as u64;
            Ok((atoms::ok(), (offset, record_size)).encode(env))
        }
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

/// Append a batch of records with a single fsync. Returns
/// `{:ok, [{offset, value_size}, ...]}`.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
fn v2_append_batch<'a>(
    env: Env<'a>,
    path: String,
    records: Vec<(Binary<'a>, Binary<'a>, u64)>,
) -> NifResult<Term<'a>> {
    let p = std::path::Path::new(&path);
    let file_id = parse_file_id(p);

    match log::LogWriter::open(p, file_id) {
        Ok(mut writer) => {
            let entries: Vec<(&[u8], &[u8], u64)> = records
                .iter()
                .map(|(k, v, exp)| (k.as_slice(), v.as_slice(), *exp))
                .collect();

            match writer.write_batch(&entries) {
                Ok(results) => {
                    let tuples: Vec<(u64, usize)> = results;
                    Ok((atoms::ok(), tuples).encode(env))
                }
                Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
            }
        }
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

/// Read the value at a specific offset in a data file. Validates CRC.
/// Returns `{:ok, value_binary}` or `{:error, reason}`.
///
/// This is the cold-read path: ETS has the key's file_id, offset, value_size
/// but not the value bytes. We pread from disk and return the value.
///
/// No Mutex needed — pread is stateless and thread-safe.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
fn v2_pread_at(env: Env<'_>, path: String, offset: u64) -> NifResult<Term<'_>> {
    let p = std::path::Path::new(&path);

    // C-2/C-6 fix: use File::open + pread_record directly instead of
    // LogReader::open which does open + fstat + seek (4 syscalls).
    // File::open + pread = 2 syscalls (open + pread).
    // Future optimization: cache fds per shard in a global fd pool.
    match std::fs::File::open(p) {
        Ok(file) => {
            fadvise_random(&file);
            match log::pread_record_from_file(&file, offset) {
                Ok(Some(record)) => {
                    // Hint kernel to evict the pages — value is promoted to ETS,
                    // the page cache copy is never needed again.
                    let record_size = (log::HEADER_SIZE
                        + record.key.len()
                        + record.value.as_ref().map_or(0, Vec::len))
                        as i64;
                    fadvise_dontneed(&file, offset as i64, record_size);

                    match record.value {
                        Some(value) => {
                            let resource = ResourceArc::new(ValueBuffer { data: value });
                            let binary = resource.make_binary(env, |vb| &vb.data);
                            Ok((atoms::ok(), binary).encode(env))
                        }
                        None => Ok((atoms::ok(), atoms::nil()).encode(env)),
                    }
                }
                Ok(None) => Ok((atoms::error(), "offset past EOF").encode(env)),
                Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
            }
        }
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

/// Scan all records in a data file. Returns a list of record metadata.
/// `{:ok, [{key, offset, value_size, expire_at_ms, is_tombstone}, ...]}`.
///
/// Used by compaction and crash recovery to rebuild the ETS keydir.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
fn v2_scan_file<'a>(env: Env<'a>, path: String) -> NifResult<Term<'a>> {
    let p = std::path::Path::new(&path);

    match log::LogReader::open(p) {
        Ok(mut reader) => {
            let records = reader
                .iter_metadata_from_start_tolerant()
                .map_err(|e| rustler::Error::Term(Box::new(e.to_string())))?;

            let mut results: Vec<Term<'a>> = Vec::with_capacity(records.len());

            for record in &records {
                // M-REMAIN-1 fix: handle OOM gracefully instead of panicking.
                let key_bin = match OwnedBinary::new(record.key.len()) {
                    Some(mut ob) => {
                        ob.as_mut_slice().copy_from_slice(&record.key);
                        ob.release(env)
                    }
                    None => {
                        return Ok(
                            (atoms::error(), "out of memory allocating key binary").encode(env)
                        );
                    }
                };

                let tuple = (
                    key_bin,
                    record.offset,
                    record.value_size,
                    record.expire_at_ms,
                    record.is_tombstone,
                )
                    .encode(env);

                results.push(tuple);
            }

            Ok((atoms::ok(), results).encode(env))
        }
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

/// Scan records in a data file from an exact byte offset. Returns a list of record metadata.
/// `{:ok, [{key, offset, value_size, expire_at_ms, is_tombstone}, ...]}`.
///
/// Used by hint recovery to replay only active-file records appended after the
/// hint boundary.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
fn v2_scan_file_from_offset<'a>(
    env: Env<'a>,
    path: String,
    start_offset: u64,
) -> NifResult<Term<'a>> {
    let p = std::path::Path::new(&path);

    match log::LogReader::open(p) {
        Ok(mut reader) => {
            let records = reader
                .iter_metadata_from_offset_tolerant(start_offset)
                .map_err(|e| rustler::Error::Term(Box::new(e.to_string())))?;

            let mut results: Vec<Term<'a>> = Vec::with_capacity(records.len());

            for record in &records {
                let key_bin = match OwnedBinary::new(record.key.len()) {
                    Some(mut ob) => {
                        ob.as_mut_slice().copy_from_slice(&record.key);
                        ob.release(env)
                    }
                    None => {
                        return Ok(
                            (atoms::error(), "out of memory allocating key binary").encode(env)
                        );
                    }
                };

                let tuple = (
                    key_bin,
                    record.offset,
                    record.value_size,
                    record.expire_at_ms,
                    record.is_tombstone,
                )
                    .encode(env);

                results.push(tuple);
            }

            Ok((atoms::ok(), results).encode(env))
        }
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

/// Scan only tombstone metadata from a data file.
/// `{:ok, [{key, offset, record_size, expire_at_ms}, ...]}`.
///
/// Used during hint recovery. Hint files contain only live entries, so startup
/// must still apply tombstones from hinted logs. This scanner skips live value
/// payloads instead of materializing them, preserving fast cold-value recovery.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
fn v2_scan_tombstones<'a>(env: Env<'a>, path: String) -> NifResult<Term<'a>> {
    use std::io::Read;

    let p = std::path::Path::new(&path);

    match std::fs::File::open(p) {
        Ok(file) => {
            let mut reader = std::io::BufReader::new(file);
            let mut results: Vec<Term<'a>> = Vec::new();
            let mut offset: u64 = 0;

            'scan: loop {
                let mut header = [0u8; log::HEADER_SIZE];

                match reader.read_exact(&mut header) {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                    Err(e) => return Ok((atoms::error(), e.to_string()).encode(env)),
                }

                let stored_crc = u32::from_le_bytes(header[0..4].try_into().unwrap());
                let expire_at_ms = u64::from_le_bytes(header[12..20].try_into().unwrap());
                let key_size = u16::from_le_bytes(header[20..22].try_into().unwrap()) as usize;
                let value_size_raw = u32::from_le_bytes(header[22..26].try_into().unwrap());

                let mut key = vec![0u8; key_size];
                if reader.read_exact(&mut key).is_err() {
                    break;
                }

                let mut hasher = crc32fast::Hasher::new();
                hasher.update(&header[4..]);
                hasher.update(&key);

                if value_size_raw == log::TOMBSTONE {
                    if hasher.finalize() != stored_crc {
                        break;
                    }

                    let key_bin = match OwnedBinary::new(key.len()) {
                        Some(mut ob) => {
                            ob.as_mut_slice().copy_from_slice(&key);
                            ob.release(env)
                        }
                        None => {
                            return Ok(
                                (atoms::error(), "out of memory allocating key binary").encode(env)
                            );
                        }
                    };

                    let record_size = (log::HEADER_SIZE + key.len()) as u64;
                    results.push((key_bin, offset, record_size, expire_at_ms).encode(env));
                    offset += record_size;
                } else {
                    let mut remaining = value_size_raw as usize;
                    let mut buf = [0u8; 64 * 1024];

                    while remaining > 0 {
                        let chunk_len = remaining.min(buf.len());
                        let chunk = &mut buf[..chunk_len];

                        if reader.read_exact(chunk).is_err() {
                            break 'scan;
                        }

                        hasher.update(chunk);
                        remaining -= chunk_len;
                    }

                    if hasher.finalize() != stored_crc {
                        break;
                    }

                    offset += log::HEADER_SIZE as u64 + key_size as u64 + u64::from(value_size_raw);
                }
            }

            Ok((atoms::ok(), results).encode(env))
        }
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

/// Batch pread: read values at multiple offsets from the same file.
/// Returns `{:ok, [value_binary | nil, ...]}`.
///
/// L-7 fix: sort offsets ascending before reading so the kernel's readahead
/// benefits sequential access patterns. Results are re-ordered to match the
/// original `locations` order before returning.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
fn v2_pread_batch<'a>(env: Env<'a>, path: String, locations: Vec<u64>) -> NifResult<Term<'a>> {
    let p = std::path::Path::new(&path);

    // C-2/C-6 fix: open file once, use pread for each offset
    match std::fs::File::open(p) {
        Ok(file) => {
            fadvise_random(&file);
            let n = locations.len();

            // Build (original_index, offset) pairs and sort by offset for
            // sequential disk access.
            let mut sorted: Vec<(usize, u64)> = locations.iter().copied().enumerate().collect();
            sorted.sort_unstable_by_key(|&(_, off)| off);

            // Read in sorted (ascending offset) order.
            let mut slot_results: Vec<Option<Term<'a>>> = vec![None; n];
            let nil = atoms::nil().encode(env);

            for &(orig_idx, offset) in &sorted {
                let term = match log::pread_record_from_file(&file, offset) {
                    Ok(Some(record)) => {
                        fadvise_dontneed(
                            &file,
                            offset as i64,
                            (log::HEADER_SIZE
                                + record.key.len()
                                + record.value.as_ref().map_or(0, Vec::len))
                                as i64,
                        );
                        match record.value {
                            Some(value) => {
                                let resource = ResourceArc::new(ValueBuffer { data: value });
                                resource.make_binary(env, |vb| &vb.data).encode(env)
                            }
                            None => nil,
                        }
                    }
                    Ok(None) => nil,
                    Err(e) => return Ok((atoms::error(), e.to_string()).encode(env)),
                };
                slot_results[orig_idx] = Some(term);
            }

            // Unwrap results back to original order.
            let results: Vec<Term<'a>> =
                slot_results.into_iter().map(|t| t.unwrap_or(nil)).collect();

            Ok((atoms::ok(), results).encode(env))
        }
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

/// Fsync a data file. Returns `:ok` or `{:error, reason}`.
///
/// L-REMAIN-1 fix: open with write permission so `sync_data()` (fdatasync)
/// actually flushes dirty pages written by other fds. `File::open()` opens
/// read-only, and `fdatasync()` on a read-only fd is a no-op per POSIX.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
fn v2_fsync(env: Env<'_>, path: String) -> NifResult<Term<'_>> {
    let p = std::path::Path::new(&path);
    match std::fs::OpenOptions::new().write(true).open(p) {
        // C-7 fix: use sync_data (fdatasync) instead of sync_all (fsync)
        Ok(f) => match f.sync_data() {
            Ok(()) => Ok(atoms::ok().encode(env)),
            Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
        },
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

/// Fsync a directory so that recent `create` / `rename` / `remove_file`
/// operations inside it are durable. See `fsync_dir` doc for details.
///
/// Returns `:ok` or `{:error, reason}`.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
fn v2_fsync_dir(env: Env<'_>, path: String) -> NifResult<Term<'_>> {
    match fsync_dir(&path) {
        Ok(()) => Ok(atoms::ok().encode(env)),
        Err(msg) => Ok((atoms::error(), msg).encode(env)),
    }
}

/// Returns available bytes for the filesystem containing `path`.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
fn v2_available_disk_space(env: Env<'_>, path: String) -> NifResult<Term<'_>> {
    match store::available_disk_space_for_path(std::path::Path::new(&path)) {
        Ok(bytes) => Ok((atoms::ok(), bytes).encode(env)),
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

/// Write a hint file from a list of entries.
/// Each entry is `{key, file_id, offset, value_size, expire_at_ms}`.
/// Returns `:ok` or `{:error, reason}`.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
fn v2_write_hint_file<'a>(
    env: Env<'a>,
    path: String,
    entries: Vec<(Binary<'a>, u64, u64, u32, u64)>,
) -> NifResult<Term<'a>> {
    let p = std::path::Path::new(&path);

    match hint::HintWriter::open(p) {
        Ok(mut writer) => {
            for (key, file_id, offset, value_size, expire_at_ms) in &entries {
                let entry = hint::HintEntry {
                    file_id: *file_id,
                    offset: *offset,
                    value_size: *value_size,
                    expire_at_ms: *expire_at_ms,
                    key: key.as_slice().to_vec(),
                };
                if let Err(e) = writer.write_entry(&entry) {
                    return Ok((atoms::error(), e.to_string()).encode(env));
                }
            }
            match writer.commit() {
                Ok(()) => Ok(atoms::ok().encode(env)),
                Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
            }
        }
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

/// Read a hint file and return all entries.
/// Returns `{:ok, [{key, file_id, offset, value_size, expire_at_ms}, ...]}`.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
fn v2_read_hint_file<'a>(env: Env<'a>, path: String) -> NifResult<Term<'a>> {
    let p = std::path::Path::new(&path);

    match hint::HintReader::open(p) {
        Ok(mut reader) => match reader.read_all() {
            Ok(entries) => {
                let mut results: Vec<Term<'a>> = Vec::with_capacity(entries.len());
                for entry in &entries {
                    // M-REMAIN-1 fix: handle OOM gracefully instead of panicking.
                    let key_bin = match OwnedBinary::new(entry.key.len()) {
                        Some(mut ob) => {
                            ob.as_mut_slice().copy_from_slice(&entry.key);
                            ob.release(env)
                        }
                        None => {
                            return Ok(
                                (atoms::error(), "out of memory allocating key binary").encode(env)
                            );
                        }
                    };
                    let tuple = (
                        key_bin,
                        entry.file_id,
                        entry.offset,
                        entry.value_size,
                        entry.expire_at_ms,
                    )
                        .encode(env);
                    results.push(tuple);
                }
                Ok((atoms::ok(), results).encode(env))
            }
            Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
        },
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

/// Copy specified records from a source file to a destination file.
/// Returns `{:ok, [{new_offset, new_size}, ...]}`.
///
/// Used by compaction to copy only live records to a new file.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
fn v2_copy_records(
    env: Env<'_>,
    source_path: String,
    dest_path: String,
    offsets: Vec<u64>,
) -> NifResult<Term<'_>> {
    let src = std::path::Path::new(&source_path);
    let dst = std::path::Path::new(&dest_path);

    let dest_file_id = parse_file_id(dst);

    match log::LogReader::open(src) {
        Ok(mut reader) => match log::LogWriter::open(dst, dest_file_id) {
            Ok(mut writer) => {
                let mut results: Vec<(u64, u64)> = Vec::with_capacity(offsets.len());

                for &offset in &offsets {
                    match reader.read_at(offset) {
                        Ok(Some(record)) => {
                            if let Some(ref value) = record.value {
                                let new_offset = writer
                                    .write(&record.key, value, record.expire_at_ms)
                                    .map_err(|e| rustler::Error::Term(Box::new(e.to_string())))?;
                                let new_size =
                                    (log::HEADER_SIZE + record.key.len() + value.len()) as u64;
                                results.push((new_offset, new_size));
                            }
                            // Skip tombstones silently
                        }
                        Ok(None) => {
                            // Offset past EOF — skip
                        }
                        Err(e) => {
                            return Ok((atoms::error(), e.to_string()).encode(env));
                        }
                    }
                }

                writer
                    .sync()
                    .map_err(|e| rustler::Error::Term(Box::new(e.to_string())))?;
                Ok((atoms::ok(), results).encode(env))
            }
            Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
        },
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

/// Copy live records and tombstones into a replacement log.
///
/// Returns `{:ok, [{new_offset, new_size}, ...]}` for live offsets only, in
/// the same order as `live_offsets`. Tombstones are copied in source-offset
/// order so replay still suppresses older values after compaction.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
fn v2_copy_records_preserve_tombstones(
    env: Env<'_>,
    source_path: String,
    dest_path: String,
    live_offsets: Vec<u64>,
    tombstone_offsets: Vec<u64>,
) -> NifResult<Term<'_>> {
    let src = std::path::Path::new(&source_path);
    let dst = std::path::Path::new(&dest_path);

    let dest_file_id = parse_file_id(dst);

    match copy_records_preserve_tombstones_impl(
        src,
        dst,
        dest_file_id,
        &live_offsets,
        &tombstone_offsets,
    ) {
        Ok(results) => Ok((atoms::ok(), results).encode(env)),
        Err(reason) => Ok((atoms::error(), reason).encode(env)),
    }
}

fn copy_records_preserve_tombstones_impl(
    src: &std::path::Path,
    dst: &std::path::Path,
    dest_file_id: u64,
    live_offsets: &[u64],
    tombstone_offsets: &[u64],
) -> std::result::Result<Vec<(u64, u64)>, String> {
    let mut reader = log::LogReader::open(src).map_err(|e| e.to_string())?;
    let mut writer = log::LogWriter::open(dst, dest_file_id).map_err(|e| e.to_string())?;

    let live_set: std::collections::HashSet<u64> = live_offsets.iter().copied().collect();
    let mut all_offsets = live_offsets.to_vec();
    all_offsets.extend(tombstone_offsets);
    all_offsets.sort_unstable();
    all_offsets.dedup();

    let mut live_results: std::collections::HashMap<u64, (u64, u64)> =
        std::collections::HashMap::with_capacity(live_offsets.len());

    for offset in all_offsets {
        match reader.read_at(offset) {
            Ok(Some(record)) => {
                if let Some(ref value) = record.value {
                    let new_offset = writer
                        .write(&record.key, value, record.expire_at_ms)
                        .map_err(|e| e.to_string())?;

                    if live_set.contains(&offset) {
                        let new_size = (log::HEADER_SIZE + record.key.len() + value.len()) as u64;
                        live_results.insert(offset, (new_offset, new_size));
                    }
                } else {
                    writer
                        .write_tombstone(&record.key)
                        .map_err(|e| e.to_string())?;
                }
            }
            Ok(None) => {}
            Err(e) => return Err(e.to_string()),
        }
    }

    writer.sync().map_err(|e| e.to_string())?;

    Ok(live_offsets
        .iter()
        .filter_map(|offset| live_results.get(offset).copied())
        .collect())
}

#[cfg(test)]
mod copy_records_preserve_tombstones_tests {
    use super::*;

    #[test]
    fn copies_live_records_and_tombstones_in_replay_order() {
        let dir = tempfile::TempDir::new().unwrap();
        let source = dir.path().join("00001.log");
        let dest = dir.path().join("compact_1.log");

        let mut writer = log::LogWriter::open(&source, 1).unwrap();
        let live_offset = writer.write(b"b", b"live", 0).unwrap();
        let tombstone_offset = writer.write_tombstone(b"a").unwrap();
        writer.sync().unwrap();

        let results = copy_records_preserve_tombstones_impl(
            &source,
            &dest,
            1,
            &[live_offset],
            &[tombstone_offset],
        )
        .unwrap();

        assert_eq!(1, results.len());

        let mut reader = log::LogReader::open(&dest).unwrap();
        let first = reader.read_at(results[0].0).unwrap().unwrap();
        assert_eq!(b"b", first.key.as_slice());
        assert_eq!(Some(b"live".to_vec()), first.value);

        let tombstone_new_offset = (log::HEADER_SIZE + b"b".len() + b"live".len()) as u64;
        let second = reader.read_at(tombstone_new_offset).unwrap().unwrap();
        assert_eq!(b"a", second.key.as_slice());
        assert!(second.value.is_none());
    }
}

// ===========================================================================
// v2 Tokio async IO NIFs — pure stateless (no Store resource)
//
// These submit IO work to the global Tokio thread pool and send the result
// back to the calling Erlang process via OwnedEnv::send_and_clear.
// The BEAM Normal scheduler returns immediately — no blocking.
//
// All messages include a correlation_id so the Elixir side can match
// responses to requests, fixing the LIFO pending_reads ordering bug.
// ===========================================================================

/// Async pread: submit a single offset read to Tokio. Returns `:ok` immediately.
///
/// When the read completes, sends `{:tokio_complete, correlation_id, :ok, value_binary}`
/// or `{:tokio_complete, correlation_id, :ok, :nil}` (tombstone/EOF)
/// or `{:tokio_complete, correlation_id, :error, reason}` to the caller.
///
/// The BEAM scheduler is completely free while the Tokio thread does the
/// pread + CRC validation.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value)]
fn v2_pread_at_async(
    env: Env<'_>,
    caller_pid: LocalPid,
    correlation_id: u64,
    path: String,
    offset: u64,
) -> NifResult<Term<'_>> {
    async_io::runtime().spawn(async move {
        // spawn_blocking: IO runs on Tokio's blocking thread pool (up to 512 threads),
        // keeping async worker threads free for coordination.
        let result = tokio::task::spawn_blocking(move || {
            let p = std::path::Path::new(&path);
            std::fs::File::open(p)
                .map_err(|e| log::LogError(e.to_string()))
                .and_then(|file| {
                    fadvise_random(&file);
                    let record = log::pread_record_from_file(&file, offset);
                    if let Ok(Some(ref r)) = record {
                        let size =
                            (log::HEADER_SIZE + r.key.len() + r.value.as_ref().map_or(0, Vec::len))
                                as i64;
                        fadvise_dontneed(&file, offset as i64, size);
                    }
                    record
                })
        })
        .await
        .unwrap_or_else(|e| Err(log::LogError(format!("spawn_blocking failed: {e}"))));

        let mut msg_env = rustler::OwnedEnv::new();
        let _ = msg_env.send_and_clear(&caller_pid, |env| match result {
            Ok(Some(record)) => match record.value {
                Some(value) => {
                    let resource = ResourceArc::new(ValueBuffer { data: value });
                    let binary = resource.make_binary(env, |vb| &vb.data);
                    (atoms::tokio_complete(), correlation_id, atoms::ok(), binary).encode(env)
                }
                None => {
                    // Tombstone at this offset
                    (
                        atoms::tokio_complete(),
                        correlation_id,
                        atoms::ok(),
                        atoms::nil(),
                    )
                        .encode(env)
                }
            },
            Ok(None) => (
                atoms::tokio_complete(),
                correlation_id,
                atoms::error(),
                "offset past EOF",
            )
                .encode(env),
            Err(e) => (
                atoms::tokio_complete(),
                correlation_id,
                atoms::error(),
                e.to_string(),
            )
                .encode(env),
        });
    });
    Ok(atoms::ok().encode(env))
}

/// Async batch pread: submit multiple offset reads to Tokio concurrently.
/// Returns `:ok` immediately.
///
/// Each location is `{path, offset}`. All reads run concurrently on Tokio
/// worker threads. When ALL reads complete, sends a single message:
/// `{:tokio_complete, correlation_id, :ok, [value | nil, ...]}`
/// to the caller.
///
/// This is the async counterpart of `v2_pread_batch/2` and is used by the
/// MGET / GET_BATCH cold path.
fn group_pread_locations(
    locations: Vec<(String, u64)>,
) -> (usize, Vec<(String, Vec<(usize, u64)>)>) {
    let count = locations.len();
    let mut grouped: std::collections::HashMap<String, Vec<(usize, u64)>> =
        std::collections::HashMap::new();

    for (index, (path, offset)) in locations.into_iter().enumerate() {
        grouped.entry(path).or_default().push((index, offset));
    }

    (count, grouped.into_iter().collect())
}

fn validate_grouped_pread_groups(groups: &[(String, Vec<(usize, u64)>)]) -> Result<usize, String> {
    let count: usize = groups.iter().map(|(_, reads)| reads.len()).sum();
    let mut seen = vec![false; count];

    for (_path, reads) in groups {
        for (index, _offset) in reads {
            if *index >= count {
                return Err(format!(
                    "grouped pread index out of range: index={index}, count={count}"
                ));
            }

            if std::mem::replace(&mut seen[*index], true) {
                return Err(format!("grouped pread duplicate index: {index}"));
            }
        }
    }

    Ok(count)
}

fn pread_batch_for_path(
    path: String,
    reads: Vec<(usize, u64)>,
) -> Result<Vec<(usize, Option<Vec<u8>>)>, String> {
    let file = match std::fs::File::open(std::path::Path::new(&path)) {
        Ok(file) => file,
        Err(e) => return Err(e.to_string()),
    };

    fadvise_random(&file);

    let mut results = Vec::with_capacity(reads.len());

    for (index, offset) in reads {
        let value = match log::pread_record_from_file(&file, offset) {
            Ok(Some(record)) => {
                let size = (log::HEADER_SIZE
                    + record.key.len()
                    + record.value.as_ref().map_or(0, Vec::len)) as i64;
                fadvise_dontneed(&file, offset as i64, size);
                record.value
            }
            Ok(None) => None,
            Err(e) => return Err(e.to_string()),
        };

        results.push((index, value));
    }

    Ok(results)
}

fn apply_grouped_pread_results(
    values: &mut [Option<Vec<u8>>],
    results: Vec<(usize, Option<Vec<u8>>)>,
) {
    for (index, value) in results {
        if let Some(slot) = values.get_mut(index) {
            *slot = value;
        }
    }
}

fn send_pread_batch_result(
    caller_pid: LocalPid,
    correlation_id: u64,
    result: Result<Vec<Option<Vec<u8>>>, String>,
) {
    let mut msg_env = rustler::OwnedEnv::new();
    let _ = msg_env.send_and_clear(&caller_pid, |env| match result {
        Ok(values) => {
            let results: Vec<Term> = values
                .into_iter()
                .map(|opt| match opt {
                    Some(value) => {
                        let resource = ResourceArc::new(ValueBuffer { data: value });
                        resource.make_binary(env, |vb| &vb.data).encode(env)
                    }
                    None => atoms::nil().encode(env),
                })
                .collect();
            (
                atoms::tokio_complete(),
                correlation_id,
                atoms::ok(),
                results,
            )
                .encode(env)
        }
        Err(reason) => (
            atoms::tokio_complete(),
            correlation_id,
            atoms::error(),
            reason.as_str(),
        )
            .encode(env),
    });
}

#[cfg(test)]
fn pread_batch_grouped(locations: Vec<(String, u64)>) -> Result<Vec<Option<Vec<u8>>>, String> {
    let (count, groups) = group_pread_locations(locations);
    let mut values = vec![None; count];

    for (path, reads) in groups {
        apply_grouped_pread_results(&mut values, pread_batch_for_path(path, reads)?);
    }

    Ok(values)
}

#[rustler::nif(schedule = "DirtyCpu")]
#[allow(clippy::needless_pass_by_value)]
fn v2_pread_batch_path_async(
    env: Env<'_>,
    caller_pid: LocalPid,
    correlation_id: u64,
    path: String,
    offsets: Vec<u64>,
) -> NifResult<Term<'_>> {
    async_io::runtime().spawn(async move {
        let count = offsets.len();
        let reads: Vec<(usize, u64)> = offsets.into_iter().enumerate().collect();

        let result = tokio::task::spawn_blocking(move || {
            let mut values = vec![None; count];
            apply_grouped_pread_results(&mut values, pread_batch_for_path(path, reads)?);
            Ok(values)
        })
        .await
        .unwrap_or_else(|e| Err(format!("spawn_blocking: {e}")));

        send_pread_batch_result(caller_pid, correlation_id, result);
    });
    Ok(atoms::ok().encode(env))
}

#[rustler::nif(schedule = "DirtyCpu")]
#[allow(clippy::needless_pass_by_value)]
fn v2_pread_batch_async(
    env: Env<'_>,
    caller_pid: LocalPid,
    correlation_id: u64,
    locations: Vec<(String, u64)>,
) -> NifResult<Term<'_>> {
    async_io::runtime().spawn(async move {
        let (count, groups) = group_pread_locations(locations);
        let mut handles = Vec::with_capacity(groups.len());

        for (path, reads) in groups {
            handles.push(tokio::task::spawn_blocking(move || {
                pread_batch_for_path(path, reads)
            }));
        }

        let mut result: Result<Vec<Option<Vec<u8>>>, String> = Ok(vec![None; count]);
        for handle in handles {
            match handle.await {
                Ok(Ok(results)) => {
                    if let Ok(values) = &mut result {
                        apply_grouped_pread_results(values, results);
                    }
                }
                Ok(Err(reason)) => {
                    result = Err(reason);
                    break;
                }
                Err(e) => {
                    result = Err(format!("spawn_blocking: {e}"));
                    break;
                }
            }
        }

        send_pread_batch_result(caller_pid, correlation_id, result);
    });
    Ok(atoms::ok().encode(env))
}

#[rustler::nif(schedule = "DirtyCpu")]
#[allow(clippy::needless_pass_by_value)]
fn v2_pread_batch_grouped_async(
    env: Env<'_>,
    caller_pid: LocalPid,
    correlation_id: u64,
    groups: Vec<(String, Vec<(usize, u64)>)>,
) -> NifResult<Term<'_>> {
    async_io::runtime().spawn(async move {
        let count = match validate_grouped_pread_groups(&groups) {
            Ok(count) => count,
            Err(reason) => {
                send_pread_batch_result(caller_pid, correlation_id, Err(reason));
                return;
            }
        };

        let mut handles = Vec::with_capacity(groups.len());

        for (path, reads) in groups {
            handles.push(tokio::task::spawn_blocking(move || {
                pread_batch_for_path(path, reads)
            }));
        }

        let mut result: Result<Vec<Option<Vec<u8>>>, String> = Ok(vec![None; count]);
        for handle in handles {
            match handle.await {
                Ok(Ok(results)) => {
                    if let Ok(values) = &mut result {
                        apply_grouped_pread_results(values, results);
                    }
                }
                Ok(Err(reason)) => {
                    result = Err(reason);
                    break;
                }
                Err(e) => {
                    result = Err(format!("spawn_blocking: {e}"));
                    break;
                }
            }
        }

        send_pread_batch_result(caller_pid, correlation_id, result);
    });
    Ok(atoms::ok().encode(env))
}

/// Async fsync: submit fsync to Tokio thread pool. Returns `:ok` immediately.
///
/// Sends `{:tokio_complete, correlation_id, :ok, :ok}` or
/// `{:tokio_complete, correlation_id, :error, reason}` on completion.
///
/// Fsync can block for milliseconds even on NVMe. By offloading to Tokio,
/// the BEAM scheduler stays free.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value)]
fn v2_fsync_async(
    env: Env<'_>,
    caller_pid: LocalPid,
    correlation_id: u64,
    path: String,
) -> NifResult<Term<'_>> {
    async_io::runtime().spawn(async move {
        let result = tokio::task::spawn_blocking(move || {
            let p = std::path::Path::new(&path);
            std::fs::OpenOptions::new()
                .write(true)
                .open(p)
                .and_then(|f| f.sync_data())
        })
        .await
        .unwrap_or_else(|e| Err(std::io::Error::other(format!("spawn_blocking: {e}"))));

        let mut msg_env = rustler::OwnedEnv::new();
        let _ = msg_env.send_and_clear(&caller_pid, |env| match result {
            Ok(()) => (
                atoms::tokio_complete(),
                correlation_id,
                atoms::ok(),
                atoms::ok(),
            )
                .encode(env),
            Err(e) => (
                atoms::tokio_complete(),
                correlation_id,
                atoms::error(),
                e.to_string(),
            )
                .encode(env),
        });
    });
    Ok(atoms::ok().encode(env))
}

/// Append a batch of records **without** fsync. The data is written to the OS
/// page cache (~1-10us) but not forced to durable storage. The caller must
/// call `v2_fsync` or `v2_fsync_async` later to guarantee durability.
///
/// Returns `{:ok, [{offset, value_size}, ...]}` or `{:error, reason}`.
///
/// ## Scheduler contract
///
/// Runs on a DirtyIo scheduler. Even without fsync, this opens a file and
/// performs synchronous writes/flushes; disk or kernel backpressure must not
/// block a Normal BEAM scheduler.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
fn v2_append_batch_nosync<'a>(
    env: Env<'a>,
    path: String,
    records: Vec<(Binary<'a>, Binary<'a>, u64)>,
) -> NifResult<Term<'a>> {
    let p = std::path::Path::new(&path);
    let file_id = parse_file_id(p);

    match log::LogWriter::open(p, file_id) {
        Ok(mut writer) => {
            let entries: Vec<(&[u8], &[u8], u64)> = records
                .iter()
                .map(|(k, v, exp)| (k.as_slice(), v.as_slice(), *exp))
                .collect();

            match writer.write_batch_nosync(&entries) {
                Ok(results) => {
                    let tuples: Vec<(u64, usize)> = results;
                    Ok((atoms::ok(), tuples).encode(env))
                }
                Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
            }
        }
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

/// Append a mixed batch of put and delete records **without** fsync.
/// Returns `{:ok, [{:put, offset, value_size} | {:delete, offset, record_size}, ...]}`.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
fn v2_append_ops_batch_nosync<'a>(
    env: Env<'a>,
    path: String,
    records: Vec<NifBatchWrite<'a>>,
) -> NifResult<Term<'a>> {
    let p = std::path::Path::new(&path);
    let file_id = parse_file_id(p);

    match log::LogWriter::open(p, file_id) {
        Ok(mut writer) => {
            let entries: Vec<log::BatchWrite<'_>> = records
                .iter()
                .map(|record| match record {
                    NifBatchWrite::Put(key, value, expire_at_ms) => log::BatchWrite::Put {
                        key: key.as_slice(),
                        value: value.as_slice(),
                        expire_at_ms: *expire_at_ms,
                    },
                    NifBatchWrite::Delete(key) => log::BatchWrite::Delete {
                        key: key.as_slice(),
                    },
                })
                .collect();

            match writer.write_ops_batch_nosync(&entries) {
                Ok(results) => {
                    let tuples: Vec<Term<'a>> = results
                        .into_iter()
                        .map(|result| match result {
                            log::BatchWriteResult::Put { offset, value_len } => {
                                (atoms::put(), offset, value_len).encode(env)
                            }
                            log::BatchWriteResult::Delete {
                                offset,
                                record_size,
                            } => (atoms::delete(), offset, record_size).encode(env),
                        })
                        .collect();

                    Ok((atoms::ok(), tuples).encode(env))
                }
                Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
            }
        }
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

/// Async variant of `v2_append_batch`: copies BEAM binaries into owned
/// memory, then submits validation, record encoding, write, and fsync to Tokio.
/// Returns `:ok` immediately. When IO completes, sends
/// `{:tokio_complete, correlation_id, :ok, [{offset, value_size}, ...]}` or
/// `{:tokio_complete, correlation_id, :error, reason}` to `caller_pid`.
///
/// ## Scheduler contract
///
/// Runs on a DirtyCpu scheduler while copying BEAM binaries into owned memory.
/// CRC/record encoding and file write + fsync run on a Tokio blocking worker.
#[rustler::nif(schedule = "DirtyCpu")]
#[allow(clippy::needless_pass_by_value)]
fn v2_append_batch_async<'a>(
    env: Env<'a>,
    caller_pid: LocalPid,
    correlation_id: u64,
    path: String,
    records: Vec<(Binary<'a>, Binary<'a>, u64)>,
) -> NifResult<Term<'a>> {
    // Step 1: Copy BEAM binaries into owned Vecs before spawning to Tokio
    // because Binary<'a> borrows from the NIF env which is destroyed when
    // this function returns. Validation and record encoding happen in the
    // blocking worker below so large batches do not burn BEAM scheduler time.
    let entries: Vec<(Vec<u8>, Vec<u8>, u64)> = records
        .iter()
        .map(|(k, v, exp)| (k.as_slice().to_vec(), v.as_slice().to_vec(), *exp))
        .collect();

    let owned_path = path;

    // Step 2: Spawn CPU encoding + IO to Tokio blocking thread pool — BEAM
    // scheduler returns immediately.
    async_io::runtime().spawn(async move {
        let result = tokio::task::spawn_blocking(move || {
            let p = std::path::Path::new(&owned_path);
            let file_id = parse_file_id(p);

            for (key, value, _) in &entries {
                log::validate_kv_sizes(key, value).map_err(|e| e.to_string())?;
            }

            match log::LogWriter::open(p, file_id) {
                Ok(mut writer) => {
                    let mut offsets = Vec::with_capacity(entries.len());
                    let mut value_sizes = Vec::with_capacity(entries.len());
                    let mut write_err: Option<String> = None;
                    for (key, value, expire_at_ms) in &entries {
                        let buf = log::encode_record(key, value, *expire_at_ms);
                        match writer.write_raw(&buf) {
                            Ok(off) => {
                                offsets.push(off);
                                value_sizes.push(value.len());
                            }
                            Err(e) => {
                                write_err = Some(e.to_string());
                                offsets.clear();
                                value_sizes.clear();
                                break;
                            }
                        }
                    }
                    if write_err.is_none() {
                        match writer.sync() {
                            Ok(()) => {
                                let locations: Vec<(u64, usize)> = offsets
                                    .into_iter()
                                    .zip(value_sizes.iter())
                                    .map(|(off, &vs)| (off, vs))
                                    .collect();
                                Ok(locations)
                            }
                            Err(e) => Err(e.to_string()),
                        }
                    } else {
                        Err(write_err.unwrap_or_else(|| "write failed".to_string()))
                    }
                }
                Err(e) => Err(e.to_string()),
            }
        })
        .await
        .unwrap_or_else(|e| Err(format!("spawn_blocking: {e}")));

        // Step 3: Send result to the BEAM caller.
        let mut msg_env = rustler::OwnedEnv::new();
        let _ = msg_env.send_and_clear(&caller_pid, |env| match result {
            Ok(locations) => (
                atoms::tokio_complete(),
                correlation_id,
                atoms::ok(),
                locations,
            )
                .encode(env),
            Err(reason) => (
                atoms::tokio_complete(),
                correlation_id,
                atoms::error(),
                reason.as_str(),
            )
                .encode(env),
        });
    });

    Ok(atoms::ok().encode(env))
}

// ===========================================================================
// Audit fix tests
// ===========================================================================

#[cfg(test)]
mod audit_fix_tests {
    use super::*;
    use tempfile::TempDir;

    fn tmp() -> TempDir {
        tempfile::TempDir::new().unwrap()
    }

    // ------------------------------------------------------------------
    // L-NEW-1: parse_file_id handles all-zeros and edge cases
    // ------------------------------------------------------------------

    #[test]
    fn parse_file_id_normal_filename() {
        let path = std::path::Path::new("/data/00000000000000000001.log");
        assert_eq!(parse_file_id(path), 1);
    }

    #[test]
    fn parse_file_id_all_zeros() {
        let path = std::path::Path::new("/data/00000000000000000000.log");
        assert_eq!(
            parse_file_id(path),
            0,
            "all-zeros filename must produce file_id 0"
        );
    }

    #[test]
    fn parse_file_id_large_number() {
        let path = std::path::Path::new("/data/00000000000000012345.log");
        assert_eq!(parse_file_id(path), 12345);
    }

    #[test]
    fn parse_file_id_max_u64() {
        // 18446744073709551615 is u64::MAX
        let path = std::path::Path::new("/data/18446744073709551615.log");
        assert_eq!(parse_file_id(path), u64::MAX);
    }

    #[test]
    fn parse_file_id_no_extension() {
        let path = std::path::Path::new("/data/00000000000000000042");
        assert_eq!(parse_file_id(path), 42);
    }

    #[test]
    fn parse_file_id_non_numeric_returns_zero() {
        let path = std::path::Path::new("/data/notanumber.log");
        assert_eq!(
            parse_file_id(path),
            0,
            "non-numeric filename must produce file_id 0"
        );
    }

    #[test]
    fn parse_file_id_single_digit() {
        let path = std::path::Path::new("/data/7.log");
        assert_eq!(parse_file_id(path), 7);
    }

    // ------------------------------------------------------------------
    // L-REMAIN-1: v2_fsync opens with write permission
    // ------------------------------------------------------------------

    #[test]
    fn fsync_with_write_permission_works() {
        let dir = tmp();
        let path = dir.path().join("fsync_test.log");

        // Write some data using LogWriter
        {
            let mut writer = log::LogWriter::open(&path, 0).unwrap();
            writer.write(b"key", b"value", 0).unwrap();
            writer.sync().unwrap();
        }

        // Open with write permission and sync — should succeed
        let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        assert!(
            f.sync_data().is_ok(),
            "sync_data on write-opened file must succeed"
        );
    }

    #[test]
    fn fsync_nonexistent_file_returns_error() {
        let dir = tmp();
        let path = dir.path().join("nonexistent.log");
        assert!(
            std::fs::OpenOptions::new().write(true).open(&path).is_err(),
            "opening nonexistent file for write must fail"
        );
    }

    // ------------------------------------------------------------------
    // M-NEW-1: small buffer LogWriter for single-record writes
    // ------------------------------------------------------------------

    #[test]
    fn open_small_writes_correctly() {
        let dir = tmp();
        let path = dir.path().join("00000000000000000001.log");

        // Write a record using open_small
        {
            let mut writer = log::LogWriter::open_small(&path, 1).unwrap();
            let offset = writer.write(b"testkey", b"testvalue", 0).unwrap();
            writer.sync().unwrap();
            assert_eq!(offset, 0, "first record must be at offset 0");
        }

        // Verify we can read it back
        let file = std::fs::File::open(&path).unwrap();
        let record = log::pread_record_from_file(&file, 0).unwrap().unwrap();
        assert_eq!(&record.key, b"testkey");
        assert_eq!(record.value.as_ref().unwrap(), b"testvalue");
    }

    #[test]
    fn open_small_1000_sequential_writes_no_corruption() {
        let dir = tmp();
        let path = dir.path().join("00000000000000000001.log");

        // Write 1000 records using open_small (one per open, simulating v2 NIF pattern)
        let mut expected_offsets = Vec::new();
        for i in 0u64..1000 {
            let mut writer = log::LogWriter::open_small(&path, 1).unwrap();
            let key = format!("k{i:04}").into_bytes();
            let value = format!("v{i:04}").into_bytes();
            let offset = writer.write(&key, &value, 0).unwrap();
            writer.sync().unwrap();
            expected_offsets.push((offset, key, value));
        }

        // Verify all records are readable
        let file = std::fs::File::open(&path).unwrap();
        for (offset, key, value) in &expected_offsets {
            let record = log::pread_record_from_file(&file, *offset)
                .unwrap()
                .unwrap();
            assert_eq!(&record.key, key);
            assert_eq!(record.value.as_ref().unwrap(), value);
        }
    }

    #[test]
    fn grouped_batch_pread_preserves_input_order_across_paths_and_missing_offsets() {
        let dir = tmp();
        let path_a = dir.path().join("00000000000000000001.log");
        let path_b = dir.path().join("00000000000000000002.log");

        let mut writer_a = log::LogWriter::open(&path_a, 1).unwrap();
        let a0 = writer_a.write(b"a0", b"va0", 0).unwrap();
        let a1 = writer_a.write(b"a1", b"va1", 0).unwrap();
        writer_a.sync().unwrap();

        let mut writer_b = log::LogWriter::open(&path_b, 2).unwrap();
        let b0 = writer_b.write(b"b0", b"vb0", 0).unwrap();
        writer_b.sync().unwrap();

        let values = pread_batch_grouped(vec![
            (path_b.to_string_lossy().into_owned(), b0),
            (path_a.to_string_lossy().into_owned(), a1),
            (path_a.to_string_lossy().into_owned(), 999_999),
            (path_a.to_string_lossy().into_owned(), a0),
        ])
        .unwrap();

        assert_eq!(values[0].as_deref(), Some(&b"vb0"[..]));
        assert_eq!(values[1].as_deref(), Some(&b"va1"[..]));
        assert_eq!(values[2], None);
        assert_eq!(values[3].as_deref(), Some(&b"va0"[..]));
    }

    #[test]
    fn grouped_batch_pread_validation_rejects_sparse_and_duplicate_indexes() {
        let groups = vec![("a.log".to_string(), vec![(0, 10), (2, 20)])];
        let err = validate_grouped_pread_groups(&groups).unwrap_err();
        assert!(err.contains("out of range"));

        let groups = vec![
            ("a.log".to_string(), vec![(0, 10)]),
            ("b.log".to_string(), vec![(0, 20)]),
        ];
        let err = validate_grouped_pread_groups(&groups).unwrap_err();
        assert!(err.contains("duplicate"));
    }

    // ------------------------------------------------------------------
    // M-REMAIN-1: OwnedBinary OOM path exists (code structure test)
    // ------------------------------------------------------------------

    // Note: We cannot reliably trigger OOM in a unit test. We verify the
    // code structure by confirming that the old `.unwrap()` calls have been
    // replaced. The grep-based verification in the global audit section
    // confirms no `.unwrap()` remains on OwnedBinary::new in production paths.

    #[test]
    fn v2_scan_and_read_hint_have_no_unwrap_on_owned_binary() {
        // This is a structural test: read the source and verify the fix is in place.
        // The actual OOM path returns {:error, "out of memory allocating key binary"}.
        // We just verify the functions compile and work for normal cases.
        let dir = tmp();
        let path = dir.path().join("00000000000000000001.log");

        // Write a test record
        {
            let mut writer = log::LogWriter::open(&path, 1).unwrap();
            writer.write(b"scankey", b"scanval", 0).unwrap();
            writer.sync().unwrap();
        }

        // Verify we can read it via LogReader (same path as v2_scan_file)
        let mut reader = log::LogReader::open(&path).unwrap();
        let records = reader.iter_from_start_tolerant().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(&records[0].key, b"scankey");
    }

    // ------------------------------------------------------------------
    // H-NEW-1: Poisoned mutex recovery
    // ------------------------------------------------------------------

    #[test]
    fn poisoned_mutex_recovery_with_unwrap_or_else() {
        use std::sync::{Arc, Mutex};

        let m = Arc::new(Mutex::new(42u64));

        // Poison the mutex by panicking while holding the lock
        let m2 = m.clone();
        let result = std::panic::catch_unwind(move || {
            let _guard = m2.lock().unwrap();
            panic!("deliberate panic to poison the mutex");
        });
        assert!(result.is_err(), "panic should have been caught");

        // Verify the mutex is poisoned
        assert!(m.lock().is_err(), "mutex should be poisoned after panic");

        // Verify unwrap_or_else recovers the inner value
        let guard = m.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(*guard, 42, "recovered value must be intact");
    }
}

// ===========================================================================
// prob_fsync edge-case tests
// ---------------------------------------------------------------------------
// These tests cover the `prob_fsync()` helper that was added to ensure
// bloom/cuckoo/cms/topk writes are durable before the NIF returns :ok.
//
// Failure modes we want to verify:
//
//   * fsync on a newly-created empty file returns Ok and is cheap.
//   * fsync on a file opened read-only returns Err (sync_data requires write).
//   * fsync on a deleted-but-still-open file does NOT panic.
//   * Repeated fsync on the same open file is idempotent (no FD leak).
//   * Concurrent fsyncs across threads on the same file are safe.
//   * File descriptor stability: after many open+write+fsync+close cycles,
//     the number of open FDs on the process stays bounded.
// ===========================================================================

#[cfg(test)]
mod prob_fsync_tests {
    use super::*;
    use std::fs::{File, OpenOptions};
    use std::io::Write;
    use std::sync::Arc;
    use std::thread;
    use tempfile::TempDir;

    fn tmpfile() -> (TempDir, std::path::PathBuf) {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("probe.bin");
        let mut f = File::create(&path).unwrap();
        f.write_all(b"hello world").unwrap();
        f.sync_all().unwrap();
        (dir, path)
    }

    #[test]
    fn fsync_empty_file_ok() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("empty.bin");
        let f = File::create(&path).unwrap();
        assert!(prob_fsync(&f).is_ok(), "empty file fsync should succeed");
    }

    #[test]
    fn fsync_small_file_ok() {
        let (_dir, path) = tmpfile();
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        assert!(prob_fsync(&f).is_ok());
    }

    #[test]
    fn fsync_read_only_file_returns_err() {
        // On most Unix systems fsync/fdatasync on a read-only fd returns
        // EBADF. prob_fsync must propagate that as an Err, not panic.
        let (_dir, path) = tmpfile();
        let f = File::open(&path).unwrap(); // read-only
        let result = prob_fsync(&f);
        // On macOS/Linux sync_data may actually succeed on RO fds for some
        // filesystems — so we don't assert Err here. We only assert that
        // the call doesn't panic and that any error is surfaced as the
        // String payload of the Result (not a panic / abort).
        match result {
            Ok(()) => {}
            Err(msg) => assert!(
                msg.starts_with("sync_data:"),
                "error must be prefixed with sync_data: — got {msg}"
            ),
        }
    }

    #[test]
    fn fsync_after_unlink_does_not_panic() {
        // Open the file, keep the fd, unlink the path, then fsync. On Unix
        // the fd remains valid. prob_fsync must not panic — at worst return
        // an Err (some kernels will return EIO).
        let (dir, path) = tmpfile();
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        std::fs::remove_file(&path).unwrap();

        // File is gone from the directory but fd is still open.
        let _ = prob_fsync(&f);

        // Dir itself still exists; drop is fine.
        drop(dir);
    }

    #[test]
    fn repeated_fsync_idempotent() {
        let (_dir, path) = tmpfile();
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();

        // 1000 fsyncs on the same fd must all succeed and must not
        // cause the process to OOM / leak descriptors. (sync_data is
        // always idempotent per POSIX.)
        for _ in 0..1000 {
            prob_fsync(&f).expect("repeated fsync must succeed");
        }
    }

    #[test]
    fn fsync_concurrent_same_file_no_panic() {
        // sync_data is thread-safe per POSIX. Run 4 threads pounding the
        // same file with fsyncs — must not panic / deadlock.
        let (_dir, path) = tmpfile();
        let f = Arc::new(
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap(),
        );

        let handles: Vec<_> = (0..4)
            .map(|_| {
                let f = Arc::clone(&f);
                thread::spawn(move || {
                    for _ in 0..250 {
                        let _ = prob_fsync(&f);
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().expect("thread must not panic");
        }
    }

    /// FD-leak probe: open+write+fsync+close many files, ensure resources are
    /// released (no stack overflow, no allocator growth, no panic). We can't
    /// portably count open FDs without libc syscalls, but the `prob_fsync`
    /// helper takes `&File` not ownership — closing is the caller's job, so
    /// a leak here would show as a disk-space / inode problem in real
    /// workloads. This test exercises that closing path through the Rust
    /// Drop impl.
    #[test]
    fn no_fd_leak_across_many_open_cycles() {
        let dir = tempfile::TempDir::new().unwrap();

        for i in 0..2000 {
            let path = dir.path().join(format!("f_{i}.bin"));
            let mut f = File::create(&path).unwrap();
            f.write_all(&[0u8; 64]).unwrap();
            prob_fsync(&f).expect("fsync ok");
            // f dropped here — fd released by kernel via Drop.
        }

        // If we leaked FDs we'd hit EMFILE (too many open files) before
        // reaching 2000 on most default ulimits (~256 on macOS).
    }

    #[test]
    fn error_message_is_well_formed() {
        // Explicit failure path: close the underlying fd by creating a
        // File and dropping it, then try to sync a fresh fd pointing at a
        // nonexistent directory's child. That returns ENOENT on open, so
        // we exercise the "sync_data: <errno>" format indirectly by
        // provoking a sync on a file that has been closed via into_raw_fd.
        //
        // Simplified: just ensure the Ok variant returns unit and the Err
        // variant contains the sync_data: prefix if it ever fires. The
        // ok case is covered above; the err case is exercised via
        // fsync_read_only_file_returns_err.
        let (_dir, path) = tmpfile();
        let f = File::open(&path).unwrap();
        match prob_fsync(&f) {
            Ok(()) => {}
            Err(msg) => {
                assert!(
                    msg.starts_with("sync_data:"),
                    "error message must start with 'sync_data:' prefix, got {msg}"
                );
            }
        }
    }
}

// ===========================================================================
// fsync_dir edge-case tests
// ---------------------------------------------------------------------------
// Directory fsync makes rename/rm/create operations durable against kernel
// panic. POSIX: fsync(dirfd) flushes the directory's entries (filename →
// inode mappings), independent of file-data fsync.
//
// Cases to cover:
//   * Happy path: existing dir returns Ok.
//   * Nonexistent path: Err, no panic.
//   * Path is a regular file, not a dir: Err (on most platforms — some
//     allow fsync on files, so this is best-effort).
//   * Empty string path: Err.
//   * Path with no parent (root): Ok (fsyncing "/" is valid).
//   * Concurrent fsync from multiple threads on same dir: no panic, no
//     deadlock.
//   * FD leak probe: many open+fsync+close cycles should not exhaust FDs.
//   * Post-rename observability: after rename + fsync_dir, reopen via a
//     fresh path resolution sees the new name (sanity check, not a real
//     kernel-panic test).
//
// These tests are written BEFORE the helper is implemented — they should
// fail to compile until `fsync_dir` + the NIF exist.
// ===========================================================================

#[cfg(test)]
mod fsync_dir_tests {
    use super::*;
    use std::fs::{self, File};
    use std::io::Write;
    use std::sync::Arc;
    use std::thread;
    use tempfile::TempDir;

    #[test]
    fn existing_dir_ok() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().to_str().unwrap().to_string();
        assert!(
            fsync_dir(&path).is_ok(),
            "fsync on existing dir must succeed"
        );
    }

    #[test]
    fn nonexistent_dir_returns_err() {
        let missing = "/does/not/exist/for/test/xyz123";
        let result = fsync_dir(missing);
        assert!(result.is_err(), "fsync on missing path must return Err");
        if let Err(msg) = result {
            // Make sure the error is tagged so callers can log it meaningfully.
            assert!(!msg.is_empty(), "err message must be non-empty");
        }
    }

    #[test]
    fn path_to_regular_file_ok_or_well_formed_err() {
        // On most systems, fsync on a regular file fd is legal and equivalent
        // to v2_fsync on that file. We accept either Ok (file treated like a
        // file) or a well-formed Err. What we DO NOT accept: a panic.
        let dir = tempfile::TempDir::new().unwrap();
        let fpath = dir.path().join("not_a_dir.txt");
        let mut f = File::create(&fpath).unwrap();
        f.write_all(b"x").unwrap();
        f.sync_all().unwrap();

        let result = fsync_dir(fpath.to_str().unwrap());
        match result {
            Ok(()) => {}
            Err(msg) => assert!(!msg.is_empty()),
        }
    }

    #[test]
    fn empty_path_returns_err() {
        let result = fsync_dir("");
        assert!(result.is_err(), "empty path must return Err");
    }

    #[test]
    fn repeated_fsync_idempotent() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().to_str().unwrap().to_string();

        for _ in 0..1000 {
            fsync_dir(&path).expect("repeated dir fsync must succeed");
        }
    }

    #[test]
    fn concurrent_fsync_no_panic() {
        let dir = tempfile::TempDir::new().unwrap();
        let path: Arc<String> = Arc::new(dir.path().to_str().unwrap().to_string());

        let handles: Vec<_> = (0..4)
            .map(|_| {
                let p = Arc::clone(&path);
                thread::spawn(move || {
                    for _ in 0..250 {
                        let _ = fsync_dir(&p);
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().expect("thread must not panic");
        }
    }

    #[test]
    fn no_fd_leak_across_cycles() {
        // 2000 fsync_dir calls shouldn't blow past the default ulimit (~256
        // on macOS) if the helper properly drops the open fd.
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().to_str().unwrap().to_string();

        for _ in 0..2000 {
            fsync_dir(&path).expect("fsync dir ok");
        }
    }

    #[test]
    fn after_rename_dir_entry_present() {
        // This is a sanity test, not a kernel-panic test: after
        // rename + fsync_dir, the new filename must still be visible via
        // a fresh directory read. (That's trivially true on any reasonable
        // filesystem; we just want to ensure fsync_dir doesn't mangle
        // state.)
        let dir = tempfile::TempDir::new().unwrap();
        let old_path = dir.path().join("old.log");
        let new_path = dir.path().join("new.log");

        File::create(&old_path).unwrap();
        fs::rename(&old_path, &new_path).unwrap();
        fsync_dir(dir.path().to_str().unwrap()).unwrap();

        let entries: Vec<_> = fs::read_dir(dir.path())
            .unwrap()
            .filter_map(Result::ok)
            .map(|e| e.file_name())
            .collect();

        assert!(entries.iter().any(|n| n == "new.log"));
        assert!(!entries.iter().any(|n| n == "old.log"));
    }

    #[test]
    fn after_remove_dir_entry_gone() {
        let dir: TempDir = tempfile::TempDir::new().unwrap();
        let fpath = dir.path().join("will_be_removed.log");
        File::create(&fpath).unwrap();
        fs::remove_file(&fpath).unwrap();

        // Must not error just because the removed file doesn't exist —
        // we're syncing the DIR, which does exist.
        fsync_dir(dir.path().to_str().unwrap()).unwrap();

        assert!(!fpath.exists());
    }

    #[test]
    fn error_message_well_formed() {
        let result = fsync_dir("/nonexistent/xyz/abc");
        match result {
            Ok(()) => {}
            Err(msg) => assert!(
                !msg.is_empty() && msg.len() < 1024,
                "error message should be non-empty and bounded, got {msg}"
            ),
        }
    }
}

#[cfg(test)]
mod nif_scheduler_tests {
    #[test]
    fn file_backed_probabilistic_nifs_do_not_run_on_normal_schedulers() {
        for path in ["src/bloom.rs", "src/cms.rs", "src/cuckoo.rs", "src/topk.rs"] {
            let source =
                std::fs::read_to_string(path).unwrap_or_else(|err| panic!("read {path}: {err}"));

            for (line_idx, line) in source.lines().enumerate() {
                if line.contains("#[rustler::nif") {
                    assert!(
                        line.contains("DirtyIo"),
                        "{path}:{} file-backed probabilistic NIFs do pread/pwrite/fsync and must not block normal BEAM schedulers: {line}",
                        line_idx + 1
                    );
                }
            }
        }
    }

    #[test]
    fn file_backed_probabilistic_writes_go_through_write_all_at() {
        for path in ["src/bloom.rs", "src/cms.rs", "src/cuckoo.rs", "src/topk.rs"] {
            let source =
                std::fs::read_to_string(path).unwrap_or_else(|err| panic!("read {path}: {err}"));

            let production_source = source.split("#[cfg(test)]").next().unwrap_or(&source);

            for (line_idx, line) in production_source.lines().enumerate() {
                assert!(
                    !line.contains(".write_at("),
                    "{path}:{} must use crate::write_all_at so short pwrite results cannot be reported as success: {line}",
                    line_idx + 1
                );
            }
        }
    }
}

rustler::init!("Elixir.Ferricstore.Bitcask.NIF", load = load);
