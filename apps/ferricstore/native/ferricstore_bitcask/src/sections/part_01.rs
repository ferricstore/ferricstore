
use rustler::{Binary, Encoder, Env, LocalPid, NifResult, OwnedBinary, ResourceArc, Term};
use std::os::unix::fs::FileExt;
use std::sync::{Arc, Mutex, OnceLock};

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
        mismatch,
        miss,
        not_found,
        missing,
        busy,
        value,
        fallback,
    }
}

#[derive(rustler::NifTaggedEnum)]
enum NifBatchWrite<'a> {
    Put(Binary<'a>, Binary<'a>, u64),
    Delete(Binary<'a>),
}

#[derive(rustler::NifTaggedEnum)]
enum LmdbBatchWrite<'a> {
    Put(Binary<'a>, Binary<'a>),
    PutNew(Binary<'a>, Binary<'a>),
    Delete(Binary<'a>),
}

struct LmdbStore {
    env: heed::Env,
    db: heed::Database<heed::types::Bytes, heed::types::Bytes>,
}

static LMDB_STORES: OnceLock<Mutex<std::collections::HashMap<String, Arc<LmdbStore>>>> =
    OnceLock::new();

#[allow(non_local_definitions)]
fn load(env: Env, _info: Term) -> bool {
    let _ = rustler::resource!(ValueBuffer, env);
    flow_index::register_resource(env);
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
#[rustler::nif(schedule = "Normal")]
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
#[rustler::nif(schedule = "Normal")]
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
#[rustler::nif(schedule = "Normal")]
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
#[rustler::nif(schedule = "Normal")]
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

fn read_exact_at_for_ref(
    file: &std::fs::File,
    buf: &mut [u8],
    offset: u64,
) -> Result<bool, String> {
    let mut read_any = false;
    let mut total = 0;

    while total < buf.len() {
        let read_offset = offset
            .checked_add(total as u64)
            .ok_or_else(|| "file ref validation offset overflow".to_string())?;

        match file.read_at(&mut buf[total..], read_offset) {
            Ok(0) if !read_any => return Ok(false),
            Ok(0) => return Err("short read while validating file ref".to_string()),
            Ok(n) => {
                read_any = true;
                total += n;
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e.to_string()),
        }
    }

    Ok(true)
}

fn validate_value_ref_from_file(
    file: &std::fs::File,
    offset: u64,
    expected_key: &[u8],
    expected_value_size: u64,
) -> Result<Option<(u64, u64)>, String> {
    let mut header = [0u8; log::HEADER_SIZE];
    if !read_exact_at_for_ref(file, &mut header, offset)? {
        return Ok(None);
    }

    let stored_crc = u32::from_le_bytes(header[0..4].try_into().unwrap());
    let key_size = u16::from_le_bytes(header[20..22].try_into().unwrap()) as usize;
    let value_size_raw = u32::from_le_bytes(header[22..26].try_into().unwrap());

    if value_size_raw == log::TOMBSTONE || u64::from(value_size_raw) != expected_value_size {
        return Ok(None);
    }

    let mut key = vec![0u8; key_size];
    if key_size > 0 {
        let key_offset = offset
            .checked_add(log::HEADER_SIZE as u64)
            .ok_or_else(|| "file ref key offset overflow".to_string())?;
        read_exact_at_for_ref(file, &mut key, key_offset)?;
    }

    if key != expected_key {
        return Ok(None);
    }

    let value_offset = offset
        .checked_add(log::HEADER_SIZE as u64)
        .and_then(|off| off.checked_add(key_size as u64))
        .ok_or_else(|| "file ref value offset overflow".to_string())?;

    validate_file_ref_crc(
        file,
        value_offset,
        expected_value_size,
        &header,
        &key,
        stored_crc,
    )?;

    Ok(Some((value_offset, expected_value_size)))
}

fn validate_file_ref_crc(
    file: &std::fs::File,
    value_offset: u64,
    value_size: u64,
    header: &[u8; log::HEADER_SIZE],
    key: &[u8],
    stored_crc: u32,
) -> Result<(), String> {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&header[4..]);
    hasher.update(key);

    let mut remaining = value_size;
    let mut read_offset = value_offset;
    let mut buf = vec![0u8; 64 * 1024];

    while remaining > 0 {
        let read_size = usize::try_from(remaining.min(buf.len() as u64))
            .map_err(|_| "file ref value read size overflow".to_string())?;
        let chunk = &mut buf[..read_size];
        read_exact_at_for_ref(file, chunk, read_offset)?;
        hasher.update(chunk);
        read_offset = read_offset
            .checked_add(read_size as u64)
            .ok_or_else(|| "file ref value offset overflow".to_string())?;
        remaining -= read_size as u64;
    }

    let computed_crc = hasher.finalize();
    if computed_crc == stored_crc {
        Ok(())
    } else {
        Err(format!(
            "CRC mismatch: stored={stored_crc}, computed={computed_crc}"
        ))
    }
}
