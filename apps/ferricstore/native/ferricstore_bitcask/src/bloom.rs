//! Bloom filter exposed as Rustler NIFs.
//!
//! Each Bloom filter is stored as a file on disk. The file layout is:
//!
//! ```text
//! [header: 32 bytes][bit array: ceil(num_bits / 8) bytes][mutation token: 16 bytes]
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
const MAX_BLOOM_HASH_VISITS: usize = 131_072;

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
    bloom_mutation_token_offset(num_bits)?
        .checked_add(crate::prob_txn::TOKEN_SIZE as u64)
        .ok_or_else(|| "bloom file size overflow".into())
}

fn bloom_mutation_token_offset(num_bits: u64) -> Result<u64, String> {
    if num_bits > MAX_BLOOM_BITS {
        return Err(format!("bloom bit array exceeds {MAX_BLOOM_BYTES} bytes"));
    }
    let byte_count = num_bits.div_ceil(8);
    (HEADER_SIZE as u64)
        .checked_add(byte_count)
        .ok_or_else(|| "bloom mutation token offset overflow".into())
}

fn bloom_count_after_add(count: u64) -> Result<u64, String> {
    count
        .checked_add(1)
        .ok_or_else(|| "bloom count overflow".into())
}

fn validate_bloom_batch_work(item_count: usize, num_hashes: u32) -> Result<(), String> {
    let visits = item_count
        .checked_mul(num_hashes as usize)
        .ok_or_else(|| "bloom batch work overflow".to_string())?;
    if visits > MAX_BLOOM_HASH_VISITS {
        return Err(format!(
            "bloom batch work limit exceeded ({visits} hash visits, maximum {MAX_BLOOM_HASH_VISITS})"
        ));
    }
    Ok(())
}

fn bloom_load_touched_bytes(
    file: &File,
    offsets: &[u64],
    touched: &mut HashMap<u64, (u8, u8)>,
) -> Result<(), String> {
    let mut buffer = Vec::new();
    let mut run_start = 0;

    while run_start < offsets.len() {
        let mut run_end = run_start + 1;
        while run_end < offsets.len() && offsets[run_end] == offsets[run_end - 1] + 1 {
            run_end += 1;
        }

        buffer.resize(run_end - run_start, 0);
        bloom_read_exact_at(file, &mut buffer, offsets[run_start], "bit run")?;
        for (&offset, &byte) in offsets[run_start..run_end].iter().zip(&buffer) {
            touched.insert(offset, (byte, byte));
        }
        run_start = run_end;
    }

    Ok(())
}

fn bloom_store_touched_bytes(
    file: &File,
    offsets: &[u64],
    touched: &HashMap<u64, (u8, u8)>,
) -> Result<(), String> {
    let mut buffer = Vec::new();
    let mut run_start = 0;

    while run_start < offsets.len() {
        let mut run_end = run_start + 1;
        while run_end < offsets.len() && offsets[run_end] == offsets[run_end - 1] + 1 {
            run_end += 1;
        }

        let changed = offsets[run_start..run_end]
            .iter()
            .any(|offset| touched[offset].0 != touched[offset].1);
        if changed {
            buffer.clear();
            buffer.extend(
                offsets[run_start..run_end]
                    .iter()
                    .map(|offset| touched[offset].1),
            );
            crate::write_all_at(file, &buffer, offsets[run_start], "bloom bit run")?;
        }
        run_start = run_end;
    }

    Ok(())
}

fn bloom_encode_results(results: &[u32]) -> Result<Vec<u8>, String> {
    let mut encoded = Vec::new();
    encoded
        .try_reserve_exact(results.len())
        .map_err(|_| "bloom mutation result allocation failed".to_string())?;
    for result in results {
        match result {
            0 => encoded.push(0),
            1 => encoded.push(1),
            _ => return Err("invalid bloom mutation result".into()),
        }
    }
    Ok(encoded)
}

fn bloom_decode_results(encoded: &[u8], expected_count: usize) -> Result<Vec<u32>, String> {
    if encoded.len() != expected_count {
        return Err("bloom mutation receipt result length mismatch".into());
    }
    let mut results = Vec::new();
    results
        .try_reserve_exact(expected_count)
        .map_err(|_| "bloom mutation result allocation failed".to_string())?;
    for result in encoded {
        match result {
            0 => results.push(0),
            1 => results.push(1),
            _ => return Err("invalid bloom mutation receipt result".into()),
        }
    }
    Ok(results)
}

fn bloom_stale_results(count: usize) -> Result<Vec<u32>, String> {
    let mut results = Vec::new();
    results
        .try_reserve_exact(count)
        .map_err(|_| "bloom mutation result allocation failed".to_string())?;
    results.resize(count, 0);
    Ok(results)
}

fn bloom_stage_transactional_madd(
    env: Env<'_>,
    file: &File,
    num_bits: u64,
    num_hashes: u32,
    mut count: u64,
    elements: &[&[u8]],
) -> Result<(Vec<u32>, Vec<crate::prob_txn::AfterImage>), String> {
    validate_bloom_batch_work(elements.len(), num_hashes)?;

    let mut element_masks = Vec::new();
    element_masks
        .try_reserve_exact(elements.len())
        .map_err(|_| "bloom batch allocation failed".to_string())?;
    let mut touched = HashMap::new();
    touched
        .try_reserve(MAX_BLOOM_HASH_VISITS.min(elements.len() * num_hashes as usize))
        .map_err(|_| "bloom batch allocation failed".to_string())?;

    for element in elements {
        let positions = file_hash_positions(element, num_bits, num_hashes);
        let mut masks = Vec::new();
        masks
            .try_reserve_exact(positions.len())
            .map_err(|_| "bloom batch allocation failed".to_string())?;
        for position in positions {
            let offset = HEADER_SIZE as u64 + position / 8;
            let mask = 1_u8 << (position % 8) as u8;
            touched.entry(offset).or_insert((0, 0));
            masks.push((offset, mask));
        }
        element_masks.push(masks);
    }

    let mut offsets = touched.keys().copied().collect::<Vec<_>>();
    offsets.sort_unstable();
    bloom_load_touched_bytes(file, &offsets, &mut touched)?;

    let mut results = Vec::new();
    results
        .try_reserve_exact(elements.len())
        .map_err(|_| "bloom batch allocation failed".to_string())?;
    for (index, masks) in element_masks.iter().enumerate() {
        let mut any_new = false;
        for (offset, mask) in masks {
            let (_original, current) = touched.get_mut(offset).unwrap();
            if (*current & mask) == 0 {
                *current |= mask;
                any_new = true;
            }
        }
        if any_new {
            count = bloom_count_after_add(count)?;
        }
        results.push(u32::from(any_new));
        if index % YIELD_CHECK_INTERVAL == 0 && index > 0 {
            let _ = consume_timeslice(env, 1);
        }
    }

    let mut images = Vec::new();
    images
        .try_reserve_exact(offsets.len().saturating_add(1))
        .map_err(|_| "bloom mutation journal allocation failed".to_string())?;
    let mut run_start = 0;
    while run_start < offsets.len() {
        let mut run_end = run_start + 1;
        while run_end < offsets.len() && offsets[run_end] == offsets[run_end - 1] + 1 {
            run_end += 1;
        }

        let run = &offsets[run_start..run_end];
        if run
            .iter()
            .any(|offset| touched[offset].0 != touched[offset].1)
        {
            let mut bytes = Vec::new();
            bytes
                .try_reserve_exact(run.len())
                .map_err(|_| "bloom mutation journal allocation failed".to_string())?;
            bytes.extend(run.iter().map(|offset| touched[offset].1));
            images.push(crate::prob_txn::AfterImage::new(run[0], bytes));
        }
        run_start = run_end;
    }
    if results.contains(&1) {
        images.push(crate::prob_txn::AfterImage::new(
            24,
            count.to_le_bytes().to_vec(),
        ));
    }
    Ok((results, images))
}

fn bloom_transactional_madd(
    env: Env<'_>,
    file: &File,
    receipt_path: &Path,
    num_bits: u64,
    num_hashes: u32,
    elements: &[&[u8]],
    token: crate::prob_txn::MutationToken,
) -> Result<Vec<u32>, String> {
    let token_offset = bloom_mutation_token_offset(num_bits)?;
    let file_size = bloom_file_size(num_bits)?;
    match crate::prob_txn::begin(file, receipt_path, token, token_offset, file_size)? {
        crate::prob_txn::MutationDecision::Replay(result) => {
            bloom_decode_results(&result, elements.len())
        }
        crate::prob_txn::MutationDecision::Stale => bloom_stale_results(elements.len()),
        crate::prob_txn::MutationDecision::Apply => {
            let (current_bits, current_hashes, count) = file_read_header(file)?;
            if current_bits != num_bits || current_hashes != num_hashes {
                return Err("bloom layout changed while mutation lock was held".into());
            }
            let (results, images) =
                bloom_stage_transactional_madd(env, file, num_bits, num_hashes, count, elements)?;
            let encoded_result = bloom_encode_results(&results)?;
            crate::prob_txn::commit(
                file,
                receipt_path,
                token,
                token_offset,
                file_size,
                images,
                encoded_result,
            )?;
            Ok(results)
        }
    }
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

pub(crate) fn recover_sidecar(path: &Path) -> Result<(), String> {
    let file = crate::open_random_rw_locked(path)
        .map_err(|error| format!("open bloom sidecar for recovery: {error}"))?;
    let (num_bits, _num_hashes, _count) = file_read_header(&file)?;
    crate::prob_txn::recover(
        &file,
        path,
        bloom_mutation_token_offset(num_bits)?,
        bloom_file_size(num_bits)?,
    )
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

/// Add one element using a deterministic Raft mutation token.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn bloom_file_add_at<'a>(
    env: Env<'a>,
    path: String,
    receipt_path: String,
    element: Binary<'a>,
    mutation_index: u64,
    mutation_ordinal: u64,
) -> NifResult<Term<'a>> {
    let token = crate::prob_txn::MutationToken::new(mutation_index, mutation_ordinal);
    if token == crate::prob_txn::MutationToken::ZERO {
        return Ok((atoms::error(), "bloom mutation token must be non-zero").encode(env));
    }
    let path = Path::new(&path);
    let file = match crate::open_random_rw_locked(path) {
        Ok(file) => file,
        Err(error) => return Ok(encode_file_error(env, map_io_error(&error))),
    };
    let (num_bits, num_hashes, _) = match file_read_header(&file) {
        Ok(header) => header,
        Err(error) => return Ok((atoms::error(), error).encode(env)),
    };
    let elements = [element.as_slice()];
    match bloom_transactional_madd(
        env,
        &file,
        Path::new(&receipt_path),
        num_bits,
        num_hashes,
        &elements,
        token,
    ) {
        Ok(results) => {
            crate::fadvise_dontneed(&file, 0, 0);
            Ok((atoms::ok(), results[0]).encode(env))
        }
        Err(error) => Ok((atoms::error(), error).encode(env)),
    }
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
    if let Err(e) = validate_bloom_batch_work(elements.len(), num_hashes) {
        return Ok((atoms::error(), e).encode(env));
    }

    let mut element_masks = Vec::new();
    if element_masks.try_reserve_exact(elements.len()).is_err() {
        return Ok((atoms::error(), "bloom batch allocation failed").encode(env));
    }
    let mut touched = HashMap::new();
    if touched
        .try_reserve(MAX_BLOOM_HASH_VISITS.min(elements.len() * num_hashes as usize))
        .is_err()
    {
        return Ok((atoms::error(), "bloom batch allocation failed").encode(env));
    }

    for element in &elements {
        let positions = file_hash_positions(element.as_slice(), num_bits, num_hashes);
        let mut masks = Vec::new();
        if masks.try_reserve_exact(positions.len()).is_err() {
            return Ok((atoms::error(), "bloom batch allocation failed").encode(env));
        }
        for pos in positions {
            let offset = HEADER_SIZE as u64 + pos / 8;
            let mask = 1u8 << (pos % 8) as u8;
            touched.entry(offset).or_insert((0, 0));
            masks.push((offset, mask));
        }
        element_masks.push(masks);
    }

    let mut offsets: Vec<u64> = touched.keys().copied().collect();
    offsets.sort_unstable();
    if let Err(e) = bloom_load_touched_bytes(&file, &offsets, &mut touched) {
        return Ok((atoms::error(), e).encode(env));
    }

    let mut results: Vec<u32> = Vec::new();
    if results.try_reserve_exact(elements.len()).is_err() {
        return Ok((atoms::error(), "bloom batch allocation failed").encode(env));
    }

    for (i, masks) in element_masks.iter().enumerate() {
        let mut any_new = false;

        for (offset, mask) in masks {
            let (_original, current) = touched.get_mut(offset).unwrap();
            if (*current & mask) == 0 {
                *current |= mask;
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

    if results.iter().all(|result| *result == 0) {
        crate::fadvise_dontneed(&file, 0, 0);
        return Ok((atoms::ok(), results).encode(env));
    }

    if let Err(e) = bloom_store_touched_bytes(&file, &offsets, &touched) {
        return Ok((atoms::error(), e).encode(env));
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

/// Add multiple elements using a deterministic Raft mutation token.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn bloom_file_madd_at<'a>(
    env: Env<'a>,
    path: String,
    receipt_path: String,
    elements: Vec<Binary<'a>>,
    mutation_index: u64,
    mutation_ordinal: u64,
) -> NifResult<Term<'a>> {
    let token = crate::prob_txn::MutationToken::new(mutation_index, mutation_ordinal);
    if token == crate::prob_txn::MutationToken::ZERO {
        return Ok((atoms::error(), "bloom mutation token must be non-zero").encode(env));
    }
    let path = Path::new(&path);
    let file = match crate::open_random_rw_locked(path) {
        Ok(file) => file,
        Err(error) => return Ok(encode_file_error(env, map_io_error(&error))),
    };
    let (num_bits, num_hashes, _) = match file_read_header(&file) {
        Ok(header) => header,
        Err(error) => return Ok((atoms::error(), error).encode(env)),
    };
    let mut borrowed_elements = Vec::new();
    if borrowed_elements.try_reserve_exact(elements.len()).is_err() {
        return Ok((atoms::error(), "bloom mutation input allocation failed").encode(env));
    }
    for element in &elements {
        borrowed_elements.push(element.as_slice());
    }
    match bloom_transactional_madd(
        env,
        &file,
        Path::new(&receipt_path),
        num_bits,
        num_hashes,
        &borrowed_elements,
        token,
    ) {
        Ok(results) => {
            crate::fadvise_dontneed(&file, 0, 0);
            Ok((atoms::ok(), results).encode(env))
        }
        Err(error) => Ok((atoms::error(), error).encode(env)),
    }
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
    if let Err(e) = validate_bloom_batch_work(elements.len(), num_hashes) {
        return Ok((atoms::error(), e).encode(env));
    }

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
    let input_bytes = match crate::async_io::checked_input_bytes([element.len()]) {
        Ok(bytes) => bytes,
        Err(reason) => return Ok((atoms::error(), reason).encode(env)),
    };
    let blocking_task = match crate::async_io::try_spawn_blocking_with_input(
        input_bytes,
        || element.as_slice().to_vec(),
        move |element_owned| {
            let file =
                crate::open_random_read_locked(std::path::Path::new(&path)).map_err(|e| {
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
        },
    ) {
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
    let input_bytes =
        match crate::async_io::checked_input_bytes(elements.iter().map(|element| element.len())) {
            Ok(bytes) => bytes,
            Err(reason) => return Ok((atoms::error(), reason).encode(env)),
        };
    let blocking_task = match crate::async_io::try_spawn_blocking_with_input(
        input_bytes,
        || {
            elements
                .iter()
                .map(|element| element.as_slice().to_vec())
                .collect::<Vec<_>>()
        },
        move |elements_owned| {
            let file =
                crate::open_random_read_locked(std::path::Path::new(&path)).map_err(|e| {
                    if e.kind() == std::io::ErrorKind::NotFound {
                        "enoent".to_string()
                    } else {
                        e.to_string()
                    }
                })?;
            let (num_bits, num_hashes, _count) = file_read_header(&file).map_err(|e| e.clone())?;
            validate_bloom_batch_work(elements_owned.len(), num_hashes)?;
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
        },
    ) {
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
