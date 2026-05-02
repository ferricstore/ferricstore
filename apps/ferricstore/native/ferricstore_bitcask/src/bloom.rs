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

use std::fs::{self, File};
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
    let byte_count = num_bits.div_ceil(8);
    (HEADER_SIZE as u64)
        .checked_add(byte_count)
        .ok_or_else(|| "bloom file size overflow".into())
}

/// Read the bloom file header via pread. Returns `(num_bits, num_hashes, count)`.
fn file_read_header(file: &File) -> Result<(u64, u32, u64), String> {
    let mut header = [0u8; HEADER_SIZE];
    file.read_at(&mut header, 0)
        .map_err(|e| format!("pread header: {e}"))?;

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
    if num_hashes == 0 {
        return Err("num_hashes must be > 0".into());
    }
    if num_hashes > MAX_NUM_HASHES {
        return Err(format!("num_hashes must be <= {MAX_NUM_HASHES}"));
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
        if let Err(e) = fs::create_dir_all(parent) {
            return Ok((atoms::error(), format!("mkdir: {e}")).encode(env));
        }
    }

    // Write the file with header + zeroed bit array.
    let mut file = match File::create(p) {
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

    if let Err(e) = file.sync_data() {
        return Ok((atoms::error(), format!("fdatasync: {e}")).encode(env));
    }

    Ok((atoms::ok(), atoms::ok()).encode(env))
}

/// Add an element to a bloom filter file via pread/pwrite.
/// Returns `{:ok, 1}` if any bit was newly set, `{:ok, 0}` if all bits were already set.
/// Returns `{:error, :enoent}` if the file does not exist.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn bloom_file_add<'a>(env: Env<'a>, path: String, element: Binary<'a>) -> NifResult<Term<'a>> {
    let file = match crate::open_random_rw(Path::new(&path)) {
        Ok(f) => f,
        Err(e) => return Ok(encode_file_error(env, map_io_error(&e))),
    };

    let (num_bits, num_hashes, count) = match file_read_header(&file) {
        Ok(h) => h,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    let positions = file_hash_positions(element.as_slice(), num_bits, num_hashes);
    let mut any_new = false;

    for pos in positions {
        let byte_index = pos / 8;
        let bit_offset = (pos % 8) as u8;
        let file_offset = HEADER_SIZE as u64 + byte_index;

        let mut buf = [0u8; 1];
        file.read_at(&mut buf, file_offset)
            .map_err(|e| rustler::Error::Term(Box::new(format!("pread bit: {e}"))))?;

        let mask = 1u8 << bit_offset;
        if (buf[0] & mask) == 0 {
            buf[0] |= mask;
            file.write_at(&buf, file_offset)
                .map_err(|e| rustler::Error::Term(Box::new(format!("pwrite bit: {e}"))))?;
            any_new = true;
        }
    }

    if any_new {
        let new_count = count + 1;
        file.write_at(&new_count.to_le_bytes(), 24)
            .map_err(|e| rustler::Error::Term(Box::new(format!("pwrite count: {e}"))))?;
    }

    // Durability: fsync before returning :ok. Without this, a kernel panic
    // after the write but before the background pagecache flush would lose
    // the bit. Ra replay is safe for bloom (bit-set is idempotent) but the
    // header `count` field can desync with the actual bits set, breaking
    // BF.CARD. See docs/bitcask-background-fsync.md.
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
    let file = match crate::open_random_rw(Path::new(&path)) {
        Ok(f) => f,
        Err(e) => return Ok(encode_file_error(env, map_io_error(&e))),
    };

    let (num_bits, num_hashes, mut count) = match file_read_header(&file) {
        Ok(h) => h,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    let mut results: Vec<u32> = Vec::with_capacity(elements.len());

    for (i, element) in elements.iter().enumerate() {
        let positions = file_hash_positions(element.as_slice(), num_bits, num_hashes);
        let mut any_new = false;

        for pos in positions {
            let byte_index = pos / 8;
            let bit_offset = (pos % 8) as u8;
            let file_offset = HEADER_SIZE as u64 + byte_index;

            let mut buf = [0u8; 1];
            file.read_at(&mut buf, file_offset)
                .map_err(|e| rustler::Error::Term(Box::new(format!("pread bit: {e}"))))?;

            let mask = 1u8 << bit_offset;
            if (buf[0] & mask) == 0 {
                buf[0] |= mask;
                file.write_at(&buf, file_offset)
                    .map_err(|e| rustler::Error::Term(Box::new(format!("pwrite bit: {e}"))))?;
                any_new = true;
            }
        }

        if any_new {
            count += 1;
        }
        results.push(u32::from(any_new));

        if i % YIELD_CHECK_INTERVAL == 0 && i > 0 {
            let _ = consume_timeslice(env, 1);
        }
    }

    // Write final count once after all additions.
    file.write_at(&count.to_le_bytes(), 24)
        .map_err(|e| rustler::Error::Term(Box::new(format!("pwrite count: {e}"))))?;

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
    let file = match crate::open_random_read(Path::new(&path)) {
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
        file.read_at(&mut buf, file_offset)
            .map_err(|e| rustler::Error::Term(Box::new(format!("pread bit: {e}"))))?;

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
    let file = match crate::open_random_read(Path::new(&path)) {
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
            file.read_at(&mut buf, file_offset)
                .map_err(|e| rustler::Error::Term(Box::new(format!("pread bit: {e}"))))?;

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
    let file = match crate::open_random_read(Path::new(&path)) {
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
    let file = match crate::open_random_read(Path::new(&path)) {
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
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
pub fn bloom_file_exists_async<'a>(
    env: Env<'a>,
    caller_pid: LocalPid,
    correlation_id: u64,
    path: String,
    element: Binary<'a>,
) -> NifResult<Term<'a>> {
    let element_owned = element.as_slice().to_vec();
    crate::async_io::runtime().spawn(async move {
        let result = tokio::task::spawn_blocking(move || {
            let file = crate::open_random_read(std::path::Path::new(&path)).map_err(|e| {
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
                file.read_at(&mut buf, file_offset)
                    .map_err(|e| e.to_string())?;
                if (buf[0] & (1u8 << bit_offset)) == 0 {
                    crate::fadvise_dontneed(&file, 0, 0);
                    return Ok(0u32);
                }
            }
            crate::fadvise_dontneed(&file, 0, 0);
            Ok(1u32)
        })
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
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
pub fn bloom_file_mexists_async<'a>(
    env: Env<'a>,
    caller_pid: LocalPid,
    correlation_id: u64,
    path: String,
    elements: Vec<Binary<'a>>,
) -> NifResult<Term<'a>> {
    let elements_owned: Vec<Vec<u8>> = elements.iter().map(|e| e.as_slice().to_vec()).collect();
    crate::async_io::runtime().spawn(async move {
        let result = tokio::task::spawn_blocking(move || {
            let file = crate::open_random_read(std::path::Path::new(&path)).map_err(|e| {
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
                    file.read_at(&mut buf, file_offset)
                        .map_err(|e| e.to_string())?;
                    if (buf[0] & (1u8 << bit_offset)) == 0 {
                        found = false;
                        break;
                    }
                }
                results.push(u32::from(found));
            }
            crate::fadvise_dontneed(&file, 0, 0);
            Ok(results)
        })
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
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
pub fn bloom_file_card_async(
    env: Env<'_>,
    caller_pid: LocalPid,
    correlation_id: u64,
    path: String,
) -> NifResult<Term<'_>> {
    crate::async_io::runtime().spawn(async move {
        let result = tokio::task::spawn_blocking(move || {
            let file = crate::open_random_read(std::path::Path::new(&path)).map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    "enoent".to_string()
                } else {
                    e.to_string()
                }
            })?;
            let (_num_bits, _num_hashes, count) = file_read_header(&file).map_err(|e| e.clone())?;
            crate::fadvise_dontneed(&file, 0, 0);
            Ok(count)
        })
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
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
pub fn bloom_file_info_async(
    env: Env<'_>,
    caller_pid: LocalPid,
    correlation_id: u64,
    path: String,
) -> NifResult<Term<'_>> {
    crate::async_io::runtime().spawn(async move {
        let result = tokio::task::spawn_blocking(move || {
            let file = crate::open_random_read(std::path::Path::new(&path)).map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    "enoent".to_string()
                } else {
                    e.to_string()
                }
            })?;
            let (num_bits, num_hashes, count) = file_read_header(&file).map_err(|e| e.clone())?;
            crate::fadvise_dontneed(&file, 0, 0);
            Ok((num_bits, count, num_hashes as u64))
        })
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
    use super::*;

    #[test]
    fn file_create_and_read_header() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bloom");

        // Create a bloom file manually
        let num_bits = 1000u64;
        let num_hashes = 7u32;
        let byte_count = num_bits.div_ceil(8) as usize;

        let mut file = File::create(&path).unwrap();
        let mut header = [0u8; HEADER_SIZE];
        header[0..8].copy_from_slice(&MAGIC.to_le_bytes());
        header[8..16].copy_from_slice(&num_bits.to_le_bytes());
        header[16..20].copy_from_slice(&num_hashes.to_le_bytes());
        file.write_all(&header).unwrap();
        file.write_all(&vec![0u8; byte_count]).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let file = File::open(&path).unwrap();
        let (bits, hashes, count) = file_read_header(&file).unwrap();
        assert_eq!(bits, 1000);
        assert_eq!(hashes, 7);
        assert_eq!(count, 0);
    }

    #[test]
    fn file_hash_positions_deterministic() {
        let pos1 = file_hash_positions(b"hello", 1000, 7);
        let pos2 = file_hash_positions(b"hello", 1000, 7);
        assert_eq!(pos1, pos2);
        assert_eq!(pos1.len(), 7);
        for &p in &pos1 {
            assert!(p < 1000);
        }
    }

    #[test]
    fn file_hash_positions_different_elements_differ() {
        let pos1 = file_hash_positions(b"hello", 100_000, 7);
        let pos2 = file_hash_positions(b"world", 100_000, 7);
        assert_ne!(pos1, pos2);
    }

    #[test]
    fn file_read_header_bad_magic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.bloom");
        std::fs::write(&path, [0xFF; 64]).unwrap();
        let file = File::open(&path).unwrap();
        assert!(file_read_header(&file).is_err());
    }

    #[test]
    fn map_io_error_enoent() {
        let e = std::io::Error::new(std::io::ErrorKind::NotFound, "not found");
        match map_io_error(&e) {
            FileError::Enoent => {}
            FileError::Other(_) => panic!("expected Enoent"),
        }
    }

    #[test]
    fn map_io_error_other() {
        let e = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied");
        match map_io_error(&e) {
            FileError::Other(s) => assert!(s.contains("denied")),
            FileError::Enoent => panic!("expected Other"),
        }
    }

    // -----------------------------------------------------------------------
    // Edge case tests
    // -----------------------------------------------------------------------

    /// Helper: create a valid bloom file and return the path string.
    fn create_bloom_file(
        dir: &std::path::Path,
        name: &str,
        num_bits: u64,
        num_hashes: u32,
    ) -> String {
        let path = dir.join(name);
        let byte_count = num_bits.div_ceil(8) as usize;
        let mut file = File::create(&path).unwrap();
        let mut header = [0u8; HEADER_SIZE];
        header[0..8].copy_from_slice(&MAGIC.to_le_bytes());
        header[8..16].copy_from_slice(&num_bits.to_le_bytes());
        header[16..20].copy_from_slice(&num_hashes.to_le_bytes());
        file.write_all(&header).unwrap();
        file.write_all(&vec![0u8; byte_count]).unwrap();
        file.sync_all().unwrap();
        path.to_str().unwrap().to_string()
    }

    #[test]
    fn empty_element_hashing_works() {
        // Zero-length binary should produce valid hash positions without panic.
        let positions = file_hash_positions(b"", 1000, 7);
        assert_eq!(positions.len(), 7);
        for &p in &positions {
            assert!(p < 1000);
        }
    }

    #[test]
    fn large_element_hashing_works() {
        // 1MB element should hash without panic.
        let big = vec![0xABu8; 1_000_000];
        let positions = file_hash_positions(&big, 10_000, 7);
        assert_eq!(positions.len(), 7);
        for &p in &positions {
            assert!(p < 10_000);
        }
    }

    #[test]
    fn truncated_header_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("truncated.bloom");
        // Write only 16 bytes (less than HEADER_SIZE=32)
        std::fs::write(&path, [0u8; 16]).unwrap();
        let file = File::open(&path).unwrap();
        assert!(file_read_header(&file).is_err());
    }

    #[test]
    fn wrong_magic_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wrong_magic.bloom");
        let mut data = [0u8; HEADER_SIZE + 8];
        // Write a different magic number
        data[0..8].copy_from_slice(&0xDEAD_BEEF_u64.to_le_bytes());
        std::fs::write(&path, data).unwrap();
        let file = File::open(&path).unwrap();
        let result = file_read_header(&file);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("magic"));
    }

    #[test]
    fn header_with_zero_num_bits_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("zero_bits.bloom");
        let mut data = [0u8; HEADER_SIZE];
        data[0..8].copy_from_slice(&MAGIC.to_le_bytes());
        data[8..16].copy_from_slice(&0u64.to_le_bytes());
        data[16..20].copy_from_slice(&1u32.to_le_bytes());
        std::fs::write(&path, data).unwrap();
        let file = File::open(&path).unwrap();

        let result = file_read_header(&file);

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("num_bits"));
    }

    #[test]
    fn header_with_zero_num_hashes_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("zero_hashes.bloom");
        let mut data = [0u8; HEADER_SIZE + 1];
        data[0..8].copy_from_slice(&MAGIC.to_le_bytes());
        data[8..16].copy_from_slice(&8u64.to_le_bytes());
        data[16..20].copy_from_slice(&0u32.to_le_bytes());
        std::fs::write(&path, data).unwrap();
        let file = File::open(&path).unwrap();

        let result = file_read_header(&file);

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("num_hashes"));
    }

    #[test]
    fn nonexistent_file_returns_io_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("does_not_exist.bloom");
        let result = crate::open_random_read(&path);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::NotFound);
    }

    #[test]
    fn minimum_size_bloom_filter() {
        // num_bits=1 -- smallest possible bloom filter
        let dir = tempfile::tempdir().unwrap();
        let path_str = create_bloom_file(dir.path(), "min.bloom", 1, 1);

        // Add and check an element
        let file = crate::open_random_rw(Path::new(&path_str)).unwrap();
        let positions = file_hash_positions(b"test", 1, 1);
        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0], 0);

        // Write the bit
        let file_offset = HEADER_SIZE as u64;
        let mut buf = [0u8; 1];
        file.read_at(&mut buf, file_offset).unwrap();
        assert_eq!(buf[0], 0);

        let mask = 1u8 << 0;
        buf[0] |= mask;
        file.write_at(&buf, file_offset).unwrap();

        // Read it back
        let mut buf2 = [0u8; 1];
        file.read_at(&mut buf2, file_offset).unwrap();
        assert_ne!(buf2[0] & mask, 0);
    }

    #[test]
    fn add_then_exists_roundtrip() {
        // Full roundtrip: create file, add elements, check exists, verify count
        let dir = tempfile::tempdir().unwrap();
        let path_str = create_bloom_file(dir.path(), "roundtrip.bloom", 10000, 7);

        // Add element "hello"
        let file = crate::open_random_rw(Path::new(&path_str)).unwrap();
        let (num_bits, num_hashes, count) = file_read_header(&file).unwrap();
        assert_eq!(count, 0);

        let positions = file_hash_positions(b"hello", num_bits, num_hashes);
        let mut any_new = false;
        for pos in &positions {
            let byte_index = pos / 8;
            let bit_offset = (pos % 8) as u8;
            let file_offset = HEADER_SIZE as u64 + byte_index;
            let mut buf = [0u8; 1];
            file.read_at(&mut buf, file_offset).unwrap();
            let mask = 1u8 << bit_offset;
            if (buf[0] & mask) == 0 {
                buf[0] |= mask;
                file.write_at(&buf, file_offset).unwrap();
                any_new = true;
            }
        }
        assert!(any_new);

        // Update count
        let new_count = 1u64;
        file.write_at(&new_count.to_le_bytes(), 24).unwrap();
        drop(file);

        // Verify exists
        let file = crate::open_random_read(Path::new(&path_str)).unwrap();
        let (num_bits, num_hashes, count) = file_read_header(&file).unwrap();
        assert_eq!(count, 1);

        let positions = file_hash_positions(b"hello", num_bits, num_hashes);
        for pos in &positions {
            let byte_index = pos / 8;
            let bit_offset = (pos % 8) as u8;
            let file_offset = HEADER_SIZE as u64 + byte_index;
            let mut buf = [0u8; 1];
            file.read_at(&mut buf, file_offset).unwrap();
            assert_ne!(buf[0] & (1u8 << bit_offset), 0, "bit {pos} should be set");
        }

        // Verify "world" is NOT in the filter (probabilistic, but with 10000 bits
        // and only 1 element, false positive is astronomically unlikely)
        let positions = file_hash_positions(b"world", num_bits, num_hashes);
        let mut found_all = true;
        for pos in &positions {
            let byte_index = pos / 8;
            let bit_offset = (pos % 8) as u8;
            let file_offset = HEADER_SIZE as u64 + byte_index;
            let mut buf = [0u8; 1];
            file.read_at(&mut buf, file_offset).unwrap();
            if (buf[0] & (1u8 << bit_offset)) == 0 {
                found_all = false;
                break;
            }
        }
        assert!(
            !found_all,
            "with only 1 element in 10000 bits, false positive should be extremely rare"
        );
    }

    #[test]
    fn hash_positions_with_num_bits_1_does_not_panic() {
        // All positions should be 0 when num_bits=1
        let positions = file_hash_positions(b"anything", 1, 10);
        assert_eq!(positions.len(), 10);
        for &p in &positions {
            assert_eq!(p, 0);
        }
    }

    #[test]
    fn null_bytes_in_element() {
        // Element containing null bytes should hash without panic
        let element = b"hello\x00world\x00";
        let positions = file_hash_positions(element, 1000, 7);
        assert_eq!(positions.len(), 7);
        for &p in &positions {
            assert!(p < 1000);
        }

        // Should differ from element without nulls
        let pos2 = file_hash_positions(b"helloworld", 1000, 7);
        assert_ne!(positions, pos2);
    }

    #[test]
    fn concurrent_reads_from_same_file() {
        let dir = tempfile::tempdir().unwrap();
        let path_str = create_bloom_file(dir.path(), "concurrent.bloom", 1000, 7);

        // Open two read handles concurrently
        let file1 = crate::open_random_read(Path::new(&path_str)).unwrap();
        let file2 = crate::open_random_read(Path::new(&path_str)).unwrap();

        let (bits1, hashes1, count1) = file_read_header(&file1).unwrap();
        let (bits2, hashes2, count2) = file_read_header(&file2).unwrap();

        assert_eq!(bits1, bits2);
        assert_eq!(hashes1, hashes2);
        assert_eq!(count1, count2);
    }
}
