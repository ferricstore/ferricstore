//! Bloom filter exposed as Rustler NIFs.
//!
//! Each Bloom filter is stored as a file on disk. The file layout is:
//!
//! ```text
//! [header: 32 bytes][bit array: ceil(num_bits / 8) bytes]
//! ```
//!
//! Header (32 bytes, little-endian):
//!   - bytes  0..7:  magic number (0x424C4F4F4D465F31 = "BLOOMF_1")
//!   - bytes  8..15: num_bits (u64)
//!   - bytes 16..19: num_hashes (u32)
//!   - bytes 20..23: reserved (u32, zero)
//!   - bytes 24..31: count (u64) — number of elements inserted
//!
//! ## Hash functions
//!
//! Uses the Kirsch-Mitzenmacker (2006) enhanced double-hashing technique:
//!   `h_i(x) = (h1(x) + i * h2(x)) mod m`
//!
//! where h1 and h2 are derived from xxh3 with two different seeds.

use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::Path;

use rustler::schedule::consume_timeslice;
use rustler::{Binary, Encoder, Env, LocalPid, NifResult, Term};

/// How often (in items) to call `consume_timeslice` and let the BEAM
/// decide whether we should yield. 64 matches the interval used in lib.rs.
const YIELD_CHECK_INTERVAL: usize = 64;

const MAGIC: u64 = 0x424C_4F4F_4D46_5F31; // "BLOOMF_1"
const HEADER_SIZE: usize = 32;
const MAX_NUM_HASHES: u32 = 1024;
const MAX_BLOOM_BYTES: u64 = 1 << 30;
const MAX_BLOOM_BITS: u64 = MAX_BLOOM_BYTES * 8;

// ---------------------------------------------------------------------------
// NIF atoms
// ---------------------------------------------------------------------------

mod atoms {
    rustler::atoms! {
        ok,
        error,
        enoent,
        tokio_complete,
    }
}

// ---------------------------------------------------------------------------
// Stateless file-based bloom filter NIFs (pread/pwrite, no mmap, no ResourceArc)
// ---------------------------------------------------------------------------

/// Compute hash positions for an element using Kirsch-Mitzenmacker double hashing
/// with xxh3. Standalone version of `BloomFilter::hash_positions` for stateless NIFs.
fn file_hash_positions(element: &[u8], num_bits: u64, num_hashes: u32) -> Vec<u64> {
    let h1 = xxhash_rust::xxh3::xxh3_64_with_seed(element, 0);
    let h2 = xxhash_rust::xxh3::xxh3_64_with_seed(element, 0x9E37_79B9_7F4A_7C15);
    (0..num_hashes as u64)
        .map(move |i| h1.wrapping_add(i.wrapping_mul(h2)) % num_bits)
        .collect()
}

fn bloom_file_size(num_bits: u64) -> Result<u64, String> {
    if num_bits > MAX_BLOOM_BITS {
        return Err(format!("bloom bit array exceeds {MAX_BLOOM_BYTES} bytes"));
    }
    let byte_count = num_bits.div_ceil(8);
    (HEADER_SIZE as u64)
        .checked_add(byte_count)
        .ok_or_else(|| "bloom file size overflow".into())
}

fn bloom_count_after_add(count: u64) -> Result<u64, String> {
    count
        .checked_add(1)
        .ok_or_else(|| "bloom count overflow".into())
}

fn bloom_read_exact_at(
    file: &File,
    buf: &mut [u8],
    offset: u64,
    label: &str,
) -> Result<(), String> {
    let mut read = 0;
    while read < buf.len() {
        let n = file
            .read_at(&mut buf[read..], offset + read as u64)
            .map_err(|e| format!("pread {label}: {e}"))?;
        if n == 0 {
            return Err(format!("truncated bloom file while reading {label}"));
        }
        read += n;
    }
    Ok(())
}

/// Read the bloom file header via pread. Returns `(num_bits, num_hashes, count)`.
fn file_read_header(file: &File) -> Result<(u64, u32, u64), String> {
    let mut header = [0u8; HEADER_SIZE];
    bloom_read_exact_at(file, &mut header, 0, "header")?;

    let magic = u64::from_le_bytes(header[0..8].try_into().unwrap());
    if magic != MAGIC {
        return Err("invalid bloom file magic".into());
    }

    let num_bits = u64::from_le_bytes(header[8..16].try_into().unwrap());
    let num_hashes = u32::from_le_bytes(header[16..20].try_into().unwrap());
    let count = u64::from_le_bytes(header[24..32].try_into().unwrap());

    if num_bits == 0 {
        return Err("num_bits must be > 0".into());
    }
    if header[20..24].iter().any(|byte| *byte != 0) {
        return Err("bloom reserved header bytes must be zero".into());
    }
    if count > num_bits {
        return Err("bloom count must not exceed num_bits".into());
    }
    let expected_size = bloom_file_size(num_bits)?;
    if num_hashes == 0 {
        return Err("num_hashes must be > 0".into());
    }
    if num_hashes > MAX_NUM_HASHES {
        return Err(format!("num_hashes must be <= {MAX_NUM_HASHES}"));
    }
    let actual_size = file
        .metadata()
        .map_err(|error| format!("read bloom file metadata: {error}"))?
        .len();
    if actual_size != expected_size {
        return Err(format!(
            "bloom file size mismatch: expected {expected_size}, got {actual_size}"
        ));
    }

    Ok((num_bits, num_hashes, count))
}

/// Map an IO error to either `:enoent` atom or a string reason.
fn map_io_error(e: &std::io::Error) -> FileError {
    if e.kind() == std::io::ErrorKind::NotFound {
        FileError::Enoent
    } else {
        FileError::Other(e.to_string())
    }
}

enum FileError {
    Enoent,
    Other(String),
}

fn encode_file_error(env: Env, fe: FileError) -> Term {
    match fe {
        FileError::Enoent => (atoms::error(), atoms::enoent()).encode(env),
        FileError::Other(s) => (atoms::error(), s).encode(env),
    }
}

/// Create a new bloom filter file at the given path.
/// Returns `{:ok, :ok}` or `{:error, reason}`.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn bloom_file_create(
    env: Env,
    path: String,
    num_bits: u64,
    num_hashes: u32,
) -> NifResult<Term> {
    if num_bits == 0 {
        return Ok((atoms::error(), "num_bits must be > 0").encode(env));
    }
    if num_hashes == 0 {
        return Ok((atoms::error(), "num_hashes must be > 0").encode(env));
    }
    if num_hashes > MAX_NUM_HASHES {
        return Ok((
            atoms::error(),
            format!("num_hashes must be <= {MAX_NUM_HASHES}"),
        )
            .encode(env));
    }
    let file_size = match bloom_file_size(num_bits) {
        Ok(size) => size,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    let p = Path::new(&path);

    // Ensure parent directory exists.
    if let Some(parent) = p.parent() {
        if let Err(e) = crate::fs_nif::create_dir_all_nofollow(parent) {
            return Ok((atoms::error(), format!("mkdir: {e}")).encode(env));
        }
    }

    // Write the file with header + zeroed bit array.
    let mut file = match crate::create_staged_locked_nofollow(p) {
        Ok(f) => f,
        Err(e) => return Ok((atoms::error(), format!("create: {e}")).encode(env)),
    };

    let mut header = [0u8; HEADER_SIZE];
    header[0..8].copy_from_slice(&MAGIC.to_le_bytes());
    header[8..16].copy_from_slice(&num_bits.to_le_bytes());
    header[16..20].copy_from_slice(&num_hashes.to_le_bytes());
    // bytes 20..24 reserved (zero)
    // bytes 24..32 count = 0

    if let Err(e) = file.write_all(&header) {
        return Ok((atoms::error(), format!("write header: {e}")).encode(env));
    }

    if let Err(e) = file.set_len(file_size) {
        return Ok((atoms::error(), format!("set file size: {e}")).encode(env));
    }

    if let Err(e) = file.publish() {
        return Ok((atoms::error(), format!("publish: {e}")).encode(env));
    }

    Ok((atoms::ok(), atoms::ok()).encode(env))
}

/// Add an element to a bloom filter file via pread/pwrite.
/// Returns `{:ok, 1}` if any bit was newly set, `{:ok, 0}` if all bits were already set.
/// Returns `{:error, :enoent}` if the file does not exist.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn bloom_file_add<'a>(env: Env<'a>, path: String, element: Binary<'a>) -> NifResult<Term<'a>> {
    let file = match crate::open_random_rw_locked(Path::new(&path)) {
        Ok(f) => f,
        Err(e) => return Ok(encode_file_error(env, map_io_error(&e))),
    };

    let (num_bits, num_hashes, count) = match file_read_header(&file) {
        Ok(h) => h,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    let positions = file_hash_positions(element.as_slice(), num_bits, num_hashes);
    let mut any_new = false;
    let mut new_count = None;

    for pos in positions {
        let byte_index = pos / 8;
        let bit_offset = (pos % 8) as u8;
        let file_offset = HEADER_SIZE as u64 + byte_index;

        let mut buf = [0u8; 1];
        if let Err(e) = bloom_read_exact_at(&file, &mut buf, file_offset, "bit") {
            return Ok((atoms::error(), e).encode(env));
        }

        let mask = 1u8 << bit_offset;
        if (buf[0] & mask) == 0 {
            if new_count.is_none() {
                match bloom_count_after_add(count) {
                    Ok(next) => new_count = Some(next),
                    Err(e) => return Ok((atoms::error(), e).encode(env)),
                }
            }
            buf[0] |= mask;
            if let Err(e) = crate::write_all_at(&file, &buf, file_offset, "bloom bit") {
                return Ok((atoms::error(), e).encode(env));
            }
            any_new = true;
        }
    }

    if let Some(new_count) = new_count {
        if let Err(e) = crate::write_all_at(&file, &new_count.to_le_bytes(), 24, "bloom count") {
            return Ok((atoms::error(), e).encode(env));
        }
    }

    // Durability: fsync before returning :ok. Without this, a kernel panic
    // after the write but before the background pagecache flush would lose
    // the bit. Ra replay is safe for bloom (bit-set is idempotent) but the
    // header `count` field can desync with the actual bits set, breaking
    // BF.CARD. See background fsync design notes.
    if let Err(e) = crate::prob_fsync(&file) {
        return Ok((atoms::error(), e).encode(env));
    }

    crate::fadvise_dontneed(&file, 0, 0);
    Ok((atoms::ok(), u32::from(any_new)).encode(env))
}

/// Add multiple elements to a bloom filter file via pread/pwrite.
/// Returns `{:ok, [0|1, ...]}` with one result per element.
/// Returns `{:error, :enoent}` if the file does not exist.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn bloom_file_madd<'a>(
    env: Env<'a>,
    path: String,
    elements: Vec<Binary<'a>>,
) -> NifResult<Term<'a>> {
    let file = match crate::open_random_rw_locked(Path::new(&path)) {
        Ok(f) => f,
        Err(e) => return Ok(encode_file_error(env, map_io_error(&e))),
    };

    let (num_bits, num_hashes, mut count) = match file_read_header(&file) {
        Ok(h) => h,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    let mut results: Vec<u32> = Vec::with_capacity(elements.len());
    let max_new_items = u64::try_from(elements.len()).unwrap_or(u64::MAX);
    let overflow_possible = count.checked_add(max_new_items).is_none();
    let mut pending_bits = if overflow_possible {
        Some(HashMap::new())
    } else {
        None
    };

    for (i, element) in elements.iter().enumerate() {
        let positions = file_hash_positions(element.as_slice(), num_bits, num_hashes);
        let mut any_new = false;

        for pos in positions {
            let byte_index = pos / 8;
            let bit_offset = (pos % 8) as u8;
            let file_offset = HEADER_SIZE as u64 + byte_index;

            let mut buf = [0u8; 1];
            if let Err(e) = bloom_read_exact_at(&file, &mut buf, file_offset, "bit") {
                return Ok((atoms::error(), e).encode(env));
            }

            let mask = 1u8 << bit_offset;
            if let Some(pending_bits) = pending_bits.as_mut() {
                let current = *pending_bits.entry(file_offset).or_insert(buf[0]);
                if (current & mask) == 0 {
                    pending_bits.insert(file_offset, current | mask);
                    any_new = true;
                }
            } else if (buf[0] & mask) == 0 {
                buf[0] |= mask;
                if let Err(e) = crate::write_all_at(&file, &buf, file_offset, "bloom bit") {
                    return Ok((atoms::error(), e).encode(env));
                }
                any_new = true;
            }
        }

        if any_new {
            count = match bloom_count_after_add(count) {
                Ok(next) => next,
                Err(e) => return Ok((atoms::error(), e).encode(env)),
            };
        }
        results.push(u32::from(any_new));

        if i % YIELD_CHECK_INTERVAL == 0 && i > 0 {
            let _ = consume_timeslice(env, 1);
        }
    }

    if let Some(pending_bits) = pending_bits {
        for (file_offset, byte) in pending_bits {
            if let Err(e) = crate::write_all_at(&file, &[byte], file_offset, "bloom bit") {
                return Ok((atoms::error(), e).encode(env));
            }
        }
    }

    // Write final count once after all additions.
    if let Err(e) = crate::write_all_at(&file, &count.to_le_bytes(), 24, "bloom count") {
        return Ok((atoms::error(), e).encode(env));
    }

    // Durability: one fsync per batch (amortized across all elements).
    if let Err(e) = crate::prob_fsync(&file) {
        return Ok((atoms::error(), e).encode(env));
    }

    crate::fadvise_dontneed(&file, 0, 0);
    Ok((atoms::ok(), results).encode(env))
}

/// Check if an element may exist in a bloom filter file via pread.
/// Returns `{:ok, 1}` if possibly present, `{:ok, 0}` if definitely not.
/// Returns `{:error, :enoent}` if the file does not exist.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn bloom_file_exists<'a>(
    env: Env<'a>,
    path: String,
    element: Binary<'a>,
) -> NifResult<Term<'a>> {
    let file = match crate::open_random_read_locked(Path::new(&path)) {
        Ok(f) => f,
        Err(e) => return Ok(encode_file_error(env, map_io_error(&e))),
    };

    let (num_bits, num_hashes, _count) = match file_read_header(&file) {
        Ok(h) => h,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    let positions = file_hash_positions(element.as_slice(), num_bits, num_hashes);

    for pos in positions {
        let byte_index = pos / 8;
        let bit_offset = (pos % 8) as u8;
        let file_offset = HEADER_SIZE as u64 + byte_index;

        let mut buf = [0u8; 1];
        if let Err(e) = bloom_read_exact_at(&file, &mut buf, file_offset, "bit") {
            return Ok((atoms::error(), e).encode(env));
        }

        if (buf[0] & (1u8 << bit_offset)) == 0 {
            crate::fadvise_dontneed(&file, 0, 0);
            return Ok((atoms::ok(), 0u32).encode(env));
        }
    }

    crate::fadvise_dontneed(&file, 0, 0);
    Ok((atoms::ok(), 1u32).encode(env))
}

/// Check if multiple elements may exist in a bloom filter file via pread.
/// Returns `{:ok, [0|1, ...]}` with one result per element.
/// Returns `{:error, :enoent}` if the file does not exist.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn bloom_file_mexists<'a>(
    env: Env<'a>,
    path: String,
    elements: Vec<Binary<'a>>,
) -> NifResult<Term<'a>> {
    let file = match crate::open_random_read_locked(Path::new(&path)) {
        Ok(f) => f,
        Err(e) => return Ok(encode_file_error(env, map_io_error(&e))),
    };

    let (num_bits, num_hashes, _count) = match file_read_header(&file) {
        Ok(h) => h,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    let mut results: Vec<u32> = Vec::with_capacity(elements.len());

    for (i, element) in elements.iter().enumerate() {
        let positions = file_hash_positions(element.as_slice(), num_bits, num_hashes);
        let mut found = true;

        for pos in positions {
            let byte_index = pos / 8;
            let bit_offset = (pos % 8) as u8;
            let file_offset = HEADER_SIZE as u64 + byte_index;

            let mut buf = [0u8; 1];
            if let Err(e) = bloom_read_exact_at(&file, &mut buf, file_offset, "bit") {
                return Ok((atoms::error(), e).encode(env));
            }

            if (buf[0] & (1u8 << bit_offset)) == 0 {
                found = false;
                break;
            }
        }

        results.push(u32::from(found));

        if i % YIELD_CHECK_INTERVAL == 0 && i > 0 {
            let _ = consume_timeslice(env, 1);
        }
    }

    crate::fadvise_dontneed(&file, 0, 0);
    Ok((atoms::ok(), results).encode(env))
}

/// Return the insertion count from a bloom filter file header.
/// Returns `{:ok, count}` or `{:error, :enoent}`.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn bloom_file_card(env: Env, path: String) -> NifResult<Term> {
    let file = match crate::open_random_read_locked(Path::new(&path)) {
        Ok(f) => f,
        Err(e) => return Ok(encode_file_error(env, map_io_error(&e))),
    };

    let (_num_bits, _num_hashes, count) = match file_read_header(&file) {
        Ok(h) => h,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    crate::fadvise_dontneed(&file, 0, 0);
    Ok((atoms::ok(), count).encode(env))
}

/// Return bloom filter info from a file header.
/// Returns `{:ok, {num_bits, count, num_hashes}}` or `{:error, :enoent}`.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn bloom_file_info(env: Env, path: String) -> NifResult<Term> {
    let file = match crate::open_random_read_locked(Path::new(&path)) {
        Ok(f) => f,
        Err(e) => return Ok(encode_file_error(env, map_io_error(&e))),
    };

    let (num_bits, num_hashes, count) = match file_read_header(&file) {
        Ok(h) => h,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    crate::fadvise_dontneed(&file, 0, 0);
    Ok((atoms::ok(), (num_bits, count, num_hashes as u64)).encode(env))
}

// ---------------------------------------------------------------------------
// Async variants of read NIFs — Tokio spawn_blocking, never block BEAM
// ---------------------------------------------------------------------------

/// Async bloom exists: spawns on Tokio, sends result to `caller_pid`.
#[rustler::nif(schedule = "DirtyCpu")]
#[allow(clippy::needless_pass_by_value)]
pub fn bloom_file_exists_async<'a>(
    env: Env<'a>,
    caller_pid: LocalPid,
    correlation_id: u64,
    path: String,
    element: Binary<'a>,
) -> NifResult<Term<'a>> {
    let element_owned = element.as_slice().to_vec();
    let blocking_task = match crate::async_io::try_spawn_blocking(move || {
        let file = crate::open_random_read_locked(std::path::Path::new(&path)).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                "enoent".to_string()
            } else {
                e.to_string()
            }
        })?;
        let (num_bits, num_hashes, _count) = file_read_header(&file).map_err(|e| e.clone())?;
        let positions = file_hash_positions(&element_owned, num_bits, num_hashes);
        for pos in positions {
            let byte_index = pos / 8;
            let bit_offset = (pos % 8) as u8;
            let file_offset = HEADER_SIZE as u64 + byte_index;
            let mut buf = [0u8; 1];
            bloom_read_exact_at(&file, &mut buf, file_offset, "bit")?;
            if (buf[0] & (1u8 << bit_offset)) == 0 {
                crate::fadvise_dontneed(&file, 0, 0);
                return Ok(0u32);
            }
        }
        crate::fadvise_dontneed(&file, 0, 0);
        Ok(1u32)
    }) {
        Ok(task) => task,
        Err(reason) => return Ok((atoms::error(), reason).encode(env)),
    };

    crate::async_io::runtime().spawn(async move {
        let result = blocking_task
            .await
            .unwrap_or_else(|e| Err(format!("spawn_blocking: {e}")));

        let mut msg_env = rustler::OwnedEnv::new();
        let _ = msg_env.send_and_clear(&caller_pid, |env| match result {
            Ok(val) => (atoms::tokio_complete(), correlation_id, atoms::ok(), val).encode(env),
            Err(reason) => (
                atoms::tokio_complete(),
                correlation_id,
                atoms::error(),
                reason,
            )
                .encode(env),
        });
    });
    Ok(atoms::ok().encode(env))
}

/// Async bloom mexists: spawns on Tokio, sends result to `caller_pid`.
#[rustler::nif(schedule = "DirtyCpu")]
#[allow(clippy::needless_pass_by_value)]
pub fn bloom_file_mexists_async<'a>(
    env: Env<'a>,
    caller_pid: LocalPid,
    correlation_id: u64,
    path: String,
    elements: Vec<Binary<'a>>,
) -> NifResult<Term<'a>> {
    let elements_owned: Vec<Vec<u8>> = elements.iter().map(|e| e.as_slice().to_vec()).collect();
    let blocking_task = match crate::async_io::try_spawn_blocking(move || {
        let file = crate::open_random_read_locked(std::path::Path::new(&path)).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                "enoent".to_string()
            } else {
                e.to_string()
            }
        })?;
        let (num_bits, num_hashes, _count) = file_read_header(&file).map_err(|e| e.clone())?;
        let mut results: Vec<u32> = Vec::with_capacity(elements_owned.len());
        for element in &elements_owned {
            let positions = file_hash_positions(element, num_bits, num_hashes);
            let mut found = true;
            for pos in positions {
                let byte_index = pos / 8;
                let bit_offset = (pos % 8) as u8;
                let file_offset = HEADER_SIZE as u64 + byte_index;
                let mut buf = [0u8; 1];
                bloom_read_exact_at(&file, &mut buf, file_offset, "bit")?;
                if (buf[0] & (1u8 << bit_offset)) == 0 {
                    found = false;
                    break;
                }
            }
            results.push(u32::from(found));
        }
        crate::fadvise_dontneed(&file, 0, 0);
        Ok(results)
    }) {
        Ok(task) => task,
        Err(reason) => return Ok((atoms::error(), reason).encode(env)),
    };

    crate::async_io::runtime().spawn(async move {
        let result = blocking_task
            .await
            .unwrap_or_else(|e| Err(format!("spawn_blocking: {e}")));

        let mut msg_env = rustler::OwnedEnv::new();
        let _ = msg_env.send_and_clear(&caller_pid, |env| match result {
            Ok(vals) => (atoms::tokio_complete(), correlation_id, atoms::ok(), vals).encode(env),
            Err(reason) => (
                atoms::tokio_complete(),
                correlation_id,
                atoms::error(),
                reason,
            )
                .encode(env),
        });
    });
    Ok(atoms::ok().encode(env))
}

/// Async bloom card: spawns on Tokio, sends result to `caller_pid`.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value)]
pub fn bloom_file_card_async(
    env: Env<'_>,
    caller_pid: LocalPid,
    correlation_id: u64,
    path: String,
) -> NifResult<Term<'_>> {
    let blocking_task = match crate::async_io::try_spawn_blocking(move || {
        let file = crate::open_random_read_locked(std::path::Path::new(&path)).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                "enoent".to_string()
            } else {
                e.to_string()
            }
        })?;
        let (_num_bits, _num_hashes, count) = file_read_header(&file).map_err(|e| e.clone())?;
        crate::fadvise_dontneed(&file, 0, 0);
        Ok(count)
    }) {
        Ok(task) => task,
        Err(reason) => return Ok((atoms::error(), reason).encode(env)),
    };

    crate::async_io::runtime().spawn(async move {
        let result = blocking_task
            .await
            .unwrap_or_else(|e| Err(format!("spawn_blocking: {e}")));

        let mut msg_env = rustler::OwnedEnv::new();
        let _ = msg_env.send_and_clear(&caller_pid, |env| match result {
            Ok(count) => (atoms::tokio_complete(), correlation_id, atoms::ok(), count).encode(env),
            Err(reason) => (
                atoms::tokio_complete(),
                correlation_id,
                atoms::error(),
                reason,
            )
                .encode(env),
        });
    });
    Ok(atoms::ok().encode(env))
}

/// Async bloom info: spawns on Tokio, sends result to `caller_pid`.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value)]
pub fn bloom_file_info_async(
    env: Env<'_>,
    caller_pid: LocalPid,
    correlation_id: u64,
    path: String,
) -> NifResult<Term<'_>> {
    let blocking_task = match crate::async_io::try_spawn_blocking(move || {
        let file = crate::open_random_read_locked(std::path::Path::new(&path)).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                "enoent".to_string()
            } else {
                e.to_string()
            }
        })?;
        let (num_bits, num_hashes, count) = file_read_header(&file).map_err(|e| e.clone())?;
        crate::fadvise_dontneed(&file, 0, 0);
        Ok((num_bits, count, num_hashes as u64))
    }) {
        Ok(task) => task,
        Err(reason) => return Ok((atoms::error(), reason).encode(env)),
    };

    crate::async_io::runtime().spawn(async move {
        let result = blocking_task
            .await
            .unwrap_or_else(|e| Err(format!("spawn_blocking: {e}")));

        let mut msg_env = rustler::OwnedEnv::new();
        let _ = msg_env.send_and_clear(&caller_pid, |env| match result {
            Ok((num_bits, count, num_hashes)) => (
                atoms::tokio_complete(),
                correlation_id,
                atoms::ok(),
                (num_bits, count, num_hashes),
            )
                .encode(env),
            Err(reason) => (
                atoms::tokio_complete(),
                correlation_id,
                atoms::error(),
                reason,
            )
                .encode(env),
        });
    });
    Ok(atoms::ok().encode(env))
}

// ---------------------------------------------------------------------------
// Rust unit tests (stateless file-based functions only)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    include!("sections/bloom_tests.rs");
}
