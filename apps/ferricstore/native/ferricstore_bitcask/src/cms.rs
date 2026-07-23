//! Count-Min Sketch (CMS) — stateless pread/pwrite file NIFs.
//!
//! The sketch is a `depth x width` matrix of `i64` counters stored in
//! row-major order in a file.
//!
//! ## File layout
//!
//! ```text
//! [magic: 8B][width: u64 LE][depth: u64 LE][count: u64 LE]
//! [counters: i64 LE * width * depth][mutation token: 16B]
//! ```
//!
//! Header size: 32 bytes. Magic: `CMS_FIL1` (0x434D535F46494C31).

use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
#[cfg(unix)]
use std::os::unix::fs::FileExt;
use std::path::Path;

use rustler::schedule::consume_timeslice;
use rustler::{Encoder, Env, LocalPid, NifResult, Term};

/// How often (in items) to call `consume_timeslice` and let the BEAM
/// decide whether we should yield. 64 matches the interval used in lib.rs.
const YIELD_CHECK_INTERVAL: usize = 64;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Magic number for mmap CMS files.
const MMAP_MAGIC: u64 = 0x434D_535F_4649_4C31; // "CMS_FIL1"
/// Header size for mmap files (magic + width + depth + count = 4 * 8 = 32).
const MMAP_HEADER_SIZE: usize = 32;
const MUTATION_TOKEN_SIZE: u64 = crate::prob_txn::TOKEN_SIZE as u64;
const MAX_CMS_DEPTH: u64 = 1024;
const MAX_CMS_COUNTERS: u64 = 16_777_216;
const MAX_CMS_MERGE_SOURCES: usize = 128;
const MAX_CMS_MERGE_COUNTER_VISITS: u64 = MAX_CMS_COUNTERS;
const CMS_MERGE_CHUNK_COUNTERS: usize = 8_192;

mod atoms {
    rustler::atoms! {
        ok,
        error,
        enoent,
        tokio_complete,
    }
}

// ---------------------------------------------------------------------------
// Standalone hash functions
// ---------------------------------------------------------------------------

/// Standalone FNV-1a 64-bit hash.
fn fnv1a_standalone(data: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &byte in data {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    hash
}

/// Standalone FNV-1a with salt prefix.
fn fnv1a_salted_standalone(data: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &byte in b"__cms_salt__" {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    for &byte in data {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    hash
}

/// Compute `depth` bucket indices for the given element, matching the
/// double-hashing scheme.
fn hash_indices_standalone(element: &[u8], width: u64, depth: u64) -> Vec<u64> {
    let h1 = fnv1a_standalone(element);
    let h2 = fnv1a_salted_standalone(element);
    (0..depth)
        .map(|i| {
            let combined = h1.wrapping_add(i.wrapping_mul(h2));
            combined % width
        })
        .collect()
}

// ---------------------------------------------------------------------------
// File helpers
// ---------------------------------------------------------------------------

fn cms_counter_bytes(width: u64, depth: u64) -> Result<u64, String> {
    let counters = width
        .checked_mul(depth)
        .ok_or_else(|| "CMS counter region size overflow".to_string())?;
    if counters > MAX_CMS_COUNTERS {
        return Err(format!(
            "CMS counter region exceeds {MAX_CMS_COUNTERS} counters"
        ));
    }
    counters
        .checked_mul(8)
        .ok_or_else(|| "CMS counter region size overflow".into())
}

fn cms_mutation_token_offset(width: u64, depth: u64) -> Result<u64, String> {
    (MMAP_HEADER_SIZE as u64)
        .checked_add(cms_counter_bytes(width, depth)?)
        .ok_or_else(|| "CMS mutation token offset overflow".into())
}

fn cms_file_size(width: u64, depth: u64) -> Result<u64, String> {
    cms_mutation_token_offset(width, depth)?
        .checked_add(MUTATION_TOKEN_SIZE)
        .ok_or_else(|| "CMS file size overflow".into())
}

fn cms_next_total_count(total_count: u64, count: i64) -> Result<u64, String> {
    if count >= 0 {
        total_count
            .checked_add(count as u64)
            .ok_or_else(|| "CMS total count overflow".into())
    } else {
        Ok(total_count.saturating_sub(count.unsigned_abs()))
    }
}

fn cms_next_merge_total_count(
    total_count: u64,
    src_count: u64,
    weight: i64,
) -> Result<u64, String> {
    let delta = i128::from(src_count)
        .checked_mul(i128::from(weight))
        .ok_or_else(|| "CMS total count overflow".to_string())?;
    let next = i128::from(total_count)
        .checked_add(delta)
        .ok_or_else(|| "CMS total count overflow".to_string())?;
    if next < 0 {
        return Err("CMS total count cannot be negative".to_string());
    }

    u64::try_from(next).map_err(|_| "CMS total count overflow".to_string())
}

fn cms_finalize_merge_counter(acc: i128) -> Result<i64, String> {
    if acc < 0 {
        return Err("CMS counter cannot be negative".to_string());
    }

    i64::try_from(acc).map_err(|_| "CMS counter overflow".to_string())
}

fn cms_read_exact_at(file: &File, buf: &mut [u8], offset: u64, label: &str) -> Result<(), String> {
    let mut read = 0;
    while read < buf.len() {
        let n = file
            .read_at(&mut buf[read..], offset + read as u64)
            .map_err(|e| format!("read {label}: {e}"))?;
        if n == 0 {
            return Err(format!("truncated CMS file while reading {label}"));
        }
        read += n;
    }
    Ok(())
}

struct CmsIncrementPlan {
    counts: Vec<i64>,
    updates: Vec<(u64, i64)>,
    total_count: u64,
}

fn cms_stage_increments<'a>(
    file: &File,
    width: u64,
    depth: u64,
    mut total_count: u64,
    items: impl IntoIterator<Item = (&'a [u8], i64)>,
) -> Result<CmsIncrementPlan, String> {
    if width == 0 || depth == 0 {
        return Err("width and depth must be > 0".into());
    }

    let mut staged = HashMap::<u64, i64>::new();
    let mut counts = Vec::new();
    let mut buf = [0u8; 8];

    for (element, count) in items {
        let indices = hash_indices_standalone(element, width, depth);
        let mut min_val = i64::MAX;

        for (row, col) in indices.into_iter().enumerate() {
            let offset = MMAP_HEADER_SIZE as u64 + (row as u64 * width + col) * 8;
            let current = if let Some(value) = staged.get(&offset) {
                *value
            } else {
                cms_read_exact_at(file, &mut buf, offset, "counter")?;
                i64::from_le_bytes(buf)
            };
            let next = current
                .checked_add(count)
                .ok_or_else(|| format!("CMS counter overflow: {current} + {count}"))?;
            staged.insert(offset, next);
            min_val = min_val.min(next);
        }

        total_count = cms_next_total_count(total_count, count)?;
        counts.push(min_val);
    }

    let mut updates = staged.into_iter().collect::<Vec<_>>();
    updates.sort_unstable_by_key(|(offset, _value)| *offset);
    Ok(CmsIncrementPlan {
        counts,
        updates,
        total_count,
    })
}

fn cms_query_counts<'a>(
    file: &File,
    width: u64,
    depth: u64,
    elements: impl IntoIterator<Item = &'a [u8]>,
) -> Result<Vec<i64>, String> {
    let mut counts = Vec::new();
    let mut buf = [0_u8; 8];

    for element in elements {
        let indices = hash_indices_standalone(element, width, depth);
        let mut min_val = i64::MAX;
        for (row, col) in indices.into_iter().enumerate() {
            let offset = MMAP_HEADER_SIZE as u64 + (row as u64 * width + col) * 8;
            cms_read_exact_at(file, &mut buf, offset, "counter")?;
            min_val = min_val.min(i64::from_le_bytes(buf));
        }
        counts.push(min_val);
    }

    Ok(counts)
}

fn cms_encode_counts(counts: &[i64]) -> Result<Vec<u8>, String> {
    let encoded_len = counts
        .len()
        .checked_mul(8)
        .ok_or_else(|| "CMS mutation result size overflow".to_string())?;
    let mut encoded = Vec::new();
    encoded
        .try_reserve_exact(encoded_len)
        .map_err(|_| "CMS mutation result allocation failed".to_string())?;
    for count in counts {
        encoded.extend_from_slice(&count.to_le_bytes());
    }
    Ok(encoded)
}

fn cms_decode_counts(encoded: &[u8], expected_count: usize) -> Result<Vec<i64>, String> {
    let expected_len = expected_count
        .checked_mul(8)
        .ok_or_else(|| "CMS mutation result size overflow".to_string())?;
    if encoded.len() != expected_len {
        return Err("CMS mutation receipt result length mismatch".into());
    }

    let mut counts = Vec::new();
    counts
        .try_reserve_exact(expected_count)
        .map_err(|_| "CMS mutation result allocation failed".to_string())?;
    for bytes in encoded.chunks_exact(8) {
        counts.push(i64::from_le_bytes(bytes.try_into().unwrap()));
    }
    Ok(counts)
}

fn cms_transactional_incrby(
    file: &File,
    receipt_path: &Path,
    width: u64,
    depth: u64,
    items: &[(&[u8], i64)],
    token: crate::prob_txn::MutationToken,
) -> Result<Vec<i64>, String> {
    let token_offset = cms_mutation_token_offset(width, depth)?;
    let expected_file_size = cms_file_size(width, depth)?;

    match crate::prob_txn::begin(file, receipt_path, token, token_offset, expected_file_size)? {
        crate::prob_txn::MutationDecision::Replay(result) => {
            cms_decode_counts(&result, items.len())
        }

        crate::prob_txn::MutationDecision::Stale => cms_query_counts(
            file,
            width,
            depth,
            items.iter().map(|(element, _)| *element),
        ),

        crate::prob_txn::MutationDecision::Apply => {
            // Recovery in begin() may have advanced the durable total count.
            let (_, _, total_count) = cms_file_read_header(file)?;
            let plan = cms_stage_increments(
                file,
                width,
                depth,
                total_count,
                items.iter().map(|(element, count)| (*element, *count)),
            )?;

            let mut images = Vec::new();
            images
                .try_reserve_exact(plan.updates.len() + 1)
                .map_err(|_| "CMS mutation journal allocation failed".to_string())?;
            for (offset, next_value) in &plan.updates {
                images.push(crate::prob_txn::AfterImage::new(
                    *offset,
                    next_value.to_le_bytes().to_vec(),
                ));
            }
            images.push(crate::prob_txn::AfterImage::new(
                24,
                plan.total_count.to_le_bytes().to_vec(),
            ));

            let result = cms_encode_counts(&plan.counts)?;
            crate::prob_txn::commit(
                file,
                receipt_path,
                token,
                token_offset,
                expected_file_size,
                images,
                result,
            )?;
            Ok(plan.counts)
        }
    }
}

/// Read the CMS file header (width, depth, count) via pread.
/// Returns `(width, depth, count)` or an error string.
fn cms_file_read_header(file: &File) -> Result<(u64, u64, u64), String> {
    let mut header = [0u8; MMAP_HEADER_SIZE];
    cms_read_exact_at(file, &mut header, 0, "header")?;

    let magic = u64::from_le_bytes(header[0..8].try_into().unwrap());
    if magic != MMAP_MAGIC {
        return Err("invalid CMS file magic".into());
    }

    let width = u64::from_le_bytes(header[8..16].try_into().unwrap());
    let depth = u64::from_le_bytes(header[16..24].try_into().unwrap());
    let count = u64::from_le_bytes(header[24..32].try_into().unwrap());

    if width == 0 || depth == 0 {
        return Err("width and depth must be > 0".into());
    }
    if depth > MAX_CMS_DEPTH {
        return Err(format!("depth must be <= {MAX_CMS_DEPTH}"));
    }
    let expected_size = cms_file_size(width, depth)?;
    let actual_size = file
        .metadata()
        .map_err(|error| format!("read CMS file metadata: {error}"))?
        .len();
    if actual_size != expected_size {
        return Err(format!(
            "CMS file size mismatch: expected {expected_size}, got {actual_size}"
        ));
    }

    Ok((width, depth, count))
}

pub(crate) fn recover_sidecar(path: &Path) -> Result<(), String> {
    let file = crate::open_random_rw_locked(path)
        .map_err(|error| format!("open CMS sidecar for recovery: {error}"))?;
    let (width, depth, _count) = cms_file_read_header(&file)?;
    crate::prob_txn::recover(
        &file,
        path,
        cms_mutation_token_offset(width, depth)?,
        cms_file_size(width, depth)?,
    )
}

/// Map an `io::Error` to either the `:enoent` atom or a string, for
/// consistent error tuples across the stateless CMS file NIFs.
fn map_io_error(e: &std::io::Error) -> CmsFileError {
    if e.kind() == std::io::ErrorKind::NotFound {
        CmsFileError::Enoent
    } else {
        CmsFileError::Other(e.to_string())
    }
}

enum CmsFileError {
    Enoent,
    Other(String),
}

impl CmsFileError {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        match self {
            CmsFileError::Enoent => (atoms::error(), atoms::enoent()).encode(env),
            CmsFileError::Other(msg) => (atoms::error(), msg.as_str()).encode(env),
        }
    }
}

// ---------------------------------------------------------------------------
// Stateless pread/pwrite file NIF functions
// ---------------------------------------------------------------------------

/// Create a new CMS file at `path` with the given dimensions.
///
/// Returns `{:ok, :ok}` or `{:error, reason}`.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn cms_file_create(env: Env, path: String, width: u64, depth: u64) -> NifResult<Term> {
    if width == 0 {
        return Ok((atoms::error(), "width must be > 0").encode(env));
    }
    if depth == 0 {
        return Ok((atoms::error(), "depth must be > 0").encode(env));
    }
    if depth > MAX_CMS_DEPTH {
        return Ok((atoms::error(), format!("depth must be <= {MAX_CMS_DEPTH}")).encode(env));
    }
    let file_size = match cms_file_size(width, depth) {
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

    let mut file = match crate::create_staged_locked_nofollow(p) {
        Ok(f) => f,
        Err(e) => return Ok((atoms::error(), format!("create: {e}")).encode(env)),
    };

    let mut header = [0u8; MMAP_HEADER_SIZE];
    header[0..8].copy_from_slice(&MMAP_MAGIC.to_le_bytes());
    header[8..16].copy_from_slice(&width.to_le_bytes());
    header[16..24].copy_from_slice(&depth.to_le_bytes());
    // count = 0 at bytes 24..32

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

/// Increment elements in a CMS file via pread/pwrite.
///
/// `items` is a list of `{element_binary, count_integer}` tuples.
///
/// Returns `{:ok, [min_count, ...]}` or `{:error, reason}`.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn cms_file_incrby<'a>(
    env: Env<'a>,
    path: String,
    items: Vec<(rustler::Binary<'a>, i64)>,
) -> NifResult<Term<'a>> {
    let file = match crate::open_random_rw_locked(Path::new(&path)) {
        Ok(f) => f,
        Err(e) => return Ok(map_io_error(&e).encode(env)),
    };

    let (width, depth, total_count) = match cms_file_read_header(&file) {
        Ok(h) => h,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    let plan = match cms_stage_increments(
        &file,
        width,
        depth,
        total_count,
        items
            .iter()
            .map(|(element, count)| (element.as_slice(), *count)),
    ) {
        Ok(plan) => plan,
        Err(error) => return Ok((atoms::error(), error).encode(env)),
    };

    for (idx, (offset, next_val)) in plan.updates.iter().enumerate() {
        if let Err(e) = crate::write_all_at(&file, &next_val.to_le_bytes(), *offset, "cms counter")
        {
            return Ok((atoms::error(), e).encode(env));
        }
        if idx % YIELD_CHECK_INTERVAL == 0 && idx > 0 {
            let _ = consume_timeslice(env, 1);
        }
    }

    // Update total count in header
    if let Err(e) = crate::write_all_at(&file, &plan.total_count.to_le_bytes(), 24, "cms count") {
        return Ok((atoms::error(), e).encode(env));
    }

    // Durability: fsync before returning the computed counts. CMS is a
    // read-modify-write on counters; replay after a partial-write crash
    // double-counts and produces cross-replica divergence. See
    // background fsync design notes.
    if let Err(e) = crate::prob_fsync(&file) {
        return Ok((atoms::error(), e).encode(env));
    }

    crate::fadvise_dontneed(&file, 0, 0);
    Ok((atoms::ok(), plan.counts).encode(env))
}

/// Increment a CMS using a deterministic Raft mutation token.
///
/// A committed token is replay-safe and older tokens never rewrite newer data.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn cms_file_incrby_at<'a>(
    env: Env<'a>,
    path: String,
    receipt_path: String,
    items: Vec<(rustler::Binary<'a>, i64)>,
    mutation_index: u64,
    mutation_ordinal: u64,
) -> NifResult<Term<'a>> {
    let token = crate::prob_txn::MutationToken::new(mutation_index, mutation_ordinal);
    if token == crate::prob_txn::MutationToken::ZERO {
        return Ok((atoms::error(), "CMS mutation token must be non-zero").encode(env));
    }

    let path = Path::new(&path);
    let receipt_path = Path::new(&receipt_path);
    let file = match crate::open_random_rw_locked(path) {
        Ok(file) => file,
        Err(error) => return Ok(map_io_error(&error).encode(env)),
    };
    let (width, depth, _) = match cms_file_read_header(&file) {
        Ok(header) => header,
        Err(error) => return Ok((atoms::error(), error).encode(env)),
    };

    let mut borrowed_items = Vec::new();
    if borrowed_items.try_reserve_exact(items.len()).is_err() {
        return Ok((atoms::error(), "CMS mutation input allocation failed").encode(env));
    }
    for (element, count) in &items {
        borrowed_items.push((element.as_slice(), *count));
    }

    match cms_transactional_incrby(&file, receipt_path, width, depth, &borrowed_items, token) {
        Ok(counts) => {
            crate::fadvise_dontneed(&file, 0, 0);
            Ok((atoms::ok(), counts).encode(env))
        }
        Err(error) => Ok((atoms::error(), error).encode(env)),
    }
}

/// Query elements in a CMS file via pread.
///
/// `elements` is a list of binaries.
///
/// Returns `{:ok, [count, ...]}` or `{:error, reason}`.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn cms_file_query<'a>(
    env: Env<'a>,
    path: String,
    elements: Vec<rustler::Binary<'a>>,
) -> NifResult<Term<'a>> {
    let file = match crate::open_random_read_locked(Path::new(&path)) {
        Ok(f) => f,
        Err(e) => return Ok(map_io_error(&e).encode(env)),
    };

    let (width, depth, _count) = match cms_file_read_header(&file) {
        Ok(h) => h,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    let mut counts: Vec<i64> = Vec::with_capacity(elements.len());
    let mut buf = [0u8; 8];

    for (idx, element) in elements.iter().enumerate() {
        let indices = hash_indices_standalone(element.as_slice(), width, depth);
        let mut min_val = i64::MAX;

        for (row, &col) in indices.iter().enumerate() {
            let offset = MMAP_HEADER_SIZE as u64 + (row as u64 * width + col) * 8;

            if let Err(e) = cms_read_exact_at(&file, &mut buf, offset, "counter") {
                return Ok((atoms::error(), e).encode(env));
            }
            let val = i64::from_le_bytes(buf);
            min_val = min_val.min(val);
        }

        counts.push(min_val);

        if idx % YIELD_CHECK_INTERVAL == 0 && idx > 0 {
            let _ = consume_timeslice(env, 1);
        }
    }

    crate::fadvise_dontneed(&file, 0, 0);
    Ok((atoms::ok(), counts).encode(env))
}

/// Return CMS file info: `{:ok, {width, depth, count}}` or `{:error, reason}`.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn cms_file_info(env: Env, path: String) -> NifResult<Term> {
    let file = match crate::open_random_read_locked(Path::new(&path)) {
        Ok(f) => f,
        Err(e) => return Ok(map_io_error(&e).encode(env)),
    };

    let (width, depth, count) = match cms_file_read_header(&file) {
        Ok(h) => h,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    crate::fadvise_dontneed(&file, 0, 0);
    Ok((atoms::ok(), (width, depth, count)).encode(env))
}

struct CmsMergePlan {
    images: Vec<crate::prob_txn::AfterImage>,
}

fn cms_stage_merge(
    env: Env<'_>,
    dst_width: u64,
    dst_depth: u64,
    source_files: &[File],
    weights: &[i64],
) -> Result<CmsMergePlan, String> {
    if source_files.len() != weights.len() {
        return Err("src_paths and weights must have the same length".into());
    }

    let merge_counter_visits = dst_width
        .checked_mul(dst_depth)
        .and_then(|counter_count| counter_count.checked_mul(source_files.len() as u64))
        .ok_or_else(|| "CMS merge work overflow".to_string())?;
    if merge_counter_visits > MAX_CMS_MERGE_COUNTER_VISITS {
        return Err("CMS merge work limit exceeded".into());
    }

    let mut source_counts = Vec::new();
    source_counts
        .try_reserve_exact(source_files.len())
        .map_err(|_| "CMS merge source metadata allocation failed".to_string())?;
    for source_file in source_files {
        let (source_width, source_depth, source_count) = cms_file_read_header(source_file)?;
        if source_width != dst_width || source_depth != dst_depth {
            return Err("width/depth mismatch".into());
        }
        source_counts.push(source_count);
    }

    if source_files.is_empty() {
        return Ok(CmsMergePlan { images: Vec::new() });
    }

    let mut next_dst_count = 0;
    for (source_count, weight) in source_counts.iter().zip(weights) {
        next_dst_count = cms_next_merge_total_count(next_dst_count, *source_count, *weight)?;
    }

    let total_counters = usize::try_from(dst_width * dst_depth)
        .map_err(|_| "CMS counter count exceeds platform size".to_string())?;
    let chunk_count = total_counters.div_ceil(CMS_MERGE_CHUNK_COUNTERS);
    let mut images = Vec::new();
    images
        .try_reserve_exact(chunk_count + 1)
        .map_err(|_| "CMS merge journal allocation failed".to_string())?;

    let chunk_bytes = CMS_MERGE_CHUNK_COUNTERS * 8;
    let mut source_chunk = Vec::new();
    let mut accumulators = Vec::new();
    source_chunk
        .try_reserve_exact(chunk_bytes)
        .map_err(|_| "CMS merge allocation failed".to_string())?;
    accumulators
        .try_reserve_exact(CMS_MERGE_CHUNK_COUNTERS)
        .map_err(|_| "CMS merge allocation failed".to_string())?;

    for chunk_start in (0..total_counters).step_by(CMS_MERGE_CHUNK_COUNTERS) {
        let counter_count = (total_counters - chunk_start).min(CMS_MERGE_CHUNK_COUNTERS);
        let byte_count = counter_count * 8;
        let offset = MMAP_HEADER_SIZE as u64 + chunk_start as u64 * 8;

        accumulators.clear();
        accumulators.resize(counter_count, 0_i128);
        for (source_file, weight) in source_files.iter().zip(weights) {
            source_chunk.resize(byte_count, 0);
            cms_read_exact_at(source_file, &mut source_chunk, offset, "source chunk")?;
            for (accumulator, bytes) in accumulators.iter_mut().zip(source_chunk.chunks_exact(8)) {
                let source_value = i64::from_le_bytes(bytes.try_into().unwrap());
                let delta = i128::from(source_value) * i128::from(*weight);
                *accumulator = accumulator
                    .checked_add(delta)
                    .ok_or_else(|| "CMS counter overflow".to_string())?;
            }
        }

        let mut output = Vec::new();
        output
            .try_reserve_exact(byte_count)
            .map_err(|_| "CMS merge output allocation failed".to_string())?;
        for accumulator in &accumulators {
            output.extend_from_slice(&cms_finalize_merge_counter(*accumulator)?.to_le_bytes());
        }
        images.push(crate::prob_txn::AfterImage::new(offset, output));
        let _ = consume_timeslice(env, 1);
    }

    images.push(crate::prob_txn::AfterImage::new(
        24,
        next_dst_count.to_le_bytes().to_vec(),
    ));
    Ok(CmsMergePlan { images })
}

fn cms_apply_merge_plan(
    env: Env<'_>,
    destination: &File,
    plan: &CmsMergePlan,
) -> Result<(), String> {
    for (index, image) in plan.images.iter().enumerate() {
        crate::write_all_at(
            destination,
            &image.bytes,
            image.offset,
            "cms merge after-image",
        )?;
        if index % YIELD_CHECK_INTERVAL == 0 && index > 0 {
            let _ = consume_timeslice(env, 1);
        }
    }
    crate::prob_fsync(destination)
}

/// Merge source CMS files (with weights) into a destination file via pread/pwrite.
///
/// For each counter position, replace the destination with the weighted sum
/// of the source counters. Updates the destination count to the same sum.
///
/// Returns `:ok` or `{:error, reason}`.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn cms_file_merge(
    env: Env<'_>,
    dst_path: String,
    src_paths: Vec<String>,
    weights: Vec<i64>,
) -> NifResult<Term<'_>> {
    if src_paths.len() != weights.len() {
        return Ok((
            atoms::error(),
            "src_paths and weights must have the same length",
        )
            .encode(env));
    }
    if src_paths.len() > MAX_CMS_MERGE_SOURCES {
        return Ok((
            atoms::error(),
            format!("CMS merge accepts at most {MAX_CMS_MERGE_SOURCES} sources"),
        )
            .encode(env));
    }

    let source_paths = src_paths
        .iter()
        .map(|path| Path::new(path.as_str()))
        .collect::<Vec<_>>();
    let merge_files = match crate::open_random_merge_locked(Path::new(&dst_path), &source_paths) {
        Ok(f) => f,
        Err(e) => return Ok(map_io_error(&e).encode(env)),
    };
    let dst_file = &merge_files.destination;

    let (dst_width, dst_depth, _) = match cms_file_read_header(dst_file) {
        Ok(h) => h,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };
    let plan = match cms_stage_merge(env, dst_width, dst_depth, &merge_files.sources, &weights) {
        Ok(plan) => plan,
        Err(error) => return Ok((atoms::error(), error).encode(env)),
    };
    if !plan.images.is_empty() {
        if let Err(error) = cms_apply_merge_plan(env, dst_file, &plan) {
            return Ok((atoms::error(), error).encode(env));
        }
    }

    crate::fadvise_dontneed(dst_file, 0, 0);
    for src_file in &merge_files.sources {
        crate::fadvise_dontneed(src_file, 0, 0);
    }

    Ok(atoms::ok().encode(env))
}

/// Merge CMS sources using a deterministic Raft mutation token.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn cms_file_merge_at(
    env: Env<'_>,
    dst_path: String,
    receipt_path: String,
    src_paths: Vec<String>,
    weights: Vec<i64>,
    mutation_index: u64,
    mutation_ordinal: u64,
) -> NifResult<Term<'_>> {
    if src_paths.len() != weights.len() {
        return Ok((
            atoms::error(),
            "src_paths and weights must have the same length",
        )
            .encode(env));
    }
    if src_paths.len() > MAX_CMS_MERGE_SOURCES {
        return Ok((
            atoms::error(),
            format!("CMS merge accepts at most {MAX_CMS_MERGE_SOURCES} sources"),
        )
            .encode(env));
    }

    let token = crate::prob_txn::MutationToken::new(mutation_index, mutation_ordinal);
    if token == crate::prob_txn::MutationToken::ZERO {
        return Ok((atoms::error(), "CMS mutation token must be non-zero").encode(env));
    }
    let destination_path = Path::new(&dst_path);
    let receipt_path = Path::new(&receipt_path);

    // A replay must not depend on source files that may have been removed by
    // later commands. Check the destination token before opening the lock set.
    {
        let destination = match crate::open_random_rw_locked(destination_path) {
            Ok(file) => file,
            Err(error) => return Ok(map_io_error(&error).encode(env)),
        };
        let (width, depth, _) = match cms_file_read_header(&destination) {
            Ok(header) => header,
            Err(error) => return Ok((atoms::error(), error).encode(env)),
        };
        let token_offset = match cms_mutation_token_offset(width, depth) {
            Ok(offset) => offset,
            Err(error) => return Ok((atoms::error(), error).encode(env)),
        };
        let file_size = match cms_file_size(width, depth) {
            Ok(size) => size,
            Err(error) => return Ok((atoms::error(), error).encode(env)),
        };
        match crate::prob_txn::begin(&destination, receipt_path, token, token_offset, file_size) {
            Ok(
                crate::prob_txn::MutationDecision::Replay(_)
                | crate::prob_txn::MutationDecision::Stale,
            ) => return Ok(atoms::ok().encode(env)),
            Ok(crate::prob_txn::MutationDecision::Apply) => {}
            Err(error) => return Ok((atoms::error(), error).encode(env)),
        }
    }

    let source_paths = src_paths
        .iter()
        .map(|path| Path::new(path.as_str()))
        .collect::<Vec<_>>();
    let merge_files = match crate::open_random_merge_locked(destination_path, &source_paths) {
        Ok(files) => files,
        Err(error) => return Ok(map_io_error(&error).encode(env)),
    };
    let destination = &merge_files.destination;
    let (width, depth, _) = match cms_file_read_header(destination) {
        Ok(header) => header,
        Err(error) => return Ok((atoms::error(), error).encode(env)),
    };
    let token_offset = match cms_mutation_token_offset(width, depth) {
        Ok(offset) => offset,
        Err(error) => return Ok((atoms::error(), error).encode(env)),
    };
    let file_size = match cms_file_size(width, depth) {
        Ok(size) => size,
        Err(error) => return Ok((atoms::error(), error).encode(env)),
    };
    match crate::prob_txn::begin(destination, receipt_path, token, token_offset, file_size) {
        Ok(
            crate::prob_txn::MutationDecision::Replay(_) | crate::prob_txn::MutationDecision::Stale,
        ) => return Ok(atoms::ok().encode(env)),
        Ok(crate::prob_txn::MutationDecision::Apply) => {}
        Err(error) => return Ok((atoms::error(), error).encode(env)),
    }

    let plan = match cms_stage_merge(env, width, depth, &merge_files.sources, &weights) {
        Ok(plan) => plan,
        Err(error) => return Ok((atoms::error(), error).encode(env)),
    };
    if plan.images.is_empty() {
        return Ok(atoms::ok().encode(env));
    }
    if let Err(error) = crate::prob_txn::commit(
        destination,
        receipt_path,
        token,
        token_offset,
        file_size,
        plan.images,
        Vec::new(),
    ) {
        return Ok((atoms::error(), error).encode(env));
    }

    crate::fadvise_dontneed(destination, 0, 0);
    for source in &merge_files.sources {
        crate::fadvise_dontneed(source, 0, 0);
    }
    Ok(atoms::ok().encode(env))
}

// ---------------------------------------------------------------------------
// Async variants of read NIFs — Tokio spawn_blocking, never block BEAM
// ---------------------------------------------------------------------------

/// Async CMS query: spawns on Tokio, sends result to `caller_pid`.
#[rustler::nif(schedule = "DirtyCpu")]
#[allow(clippy::needless_pass_by_value)]
pub fn cms_file_query_async<'a>(
    env: Env<'a>,
    caller_pid: LocalPid,
    correlation_id: u64,
    path: String,
    elements: Vec<rustler::Binary<'a>>,
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
            let (width, depth, _count) = cms_file_read_header(&file).map_err(|e| e.clone())?;
            let mut counts: Vec<i64> = Vec::with_capacity(elements_owned.len());
            let mut buf = [0u8; 8];
            for element in &elements_owned {
                let indices = hash_indices_standalone(element, width, depth);
                let mut min_val = i64::MAX;
                for (row, &col) in indices.iter().enumerate() {
                    let offset = MMAP_HEADER_SIZE as u64 + (row as u64 * width + col) * 8;
                    cms_read_exact_at(&file, &mut buf, offset, "counter")?;
                    let val = i64::from_le_bytes(buf);
                    min_val = min_val.min(val);
                }
                counts.push(min_val);
            }
            crate::fadvise_dontneed(&file, 0, 0);
            Ok(counts)
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
            Ok(counts) => {
                (atoms::tokio_complete(), correlation_id, atoms::ok(), counts).encode(env)
            }
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

/// Async CMS info: spawns on Tokio, sends result to `caller_pid`.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value)]
pub fn cms_file_info_async(
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
        let (width, depth, count) = cms_file_read_header(&file).map_err(|e| e.clone())?;
        crate::fadvise_dontneed(&file, 0, 0);
        Ok((width, depth, count))
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
            Ok((width, depth, count)) => (
                atoms::tokio_complete(),
                correlation_id,
                atoms::ok(),
                (width, depth, count),
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
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    include!("sections/cms_tests.rs");
}
