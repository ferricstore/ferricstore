//! Count-Min Sketch (CMS) — stateless pread/pwrite file NIFs.
//!
//! The sketch is a `depth x width` matrix of `i64` counters stored in
//! row-major order in a file.
//!
//! ## File layout
//!
//! ```text
//! [magic: 8B][width: u64 LE][depth: u64 LE][count: u64 LE][counters: i64 LE * width * depth]
//! ```
//!
//! Header size: 32 bytes. Magic: `CMS_FIL1` (0x434D535F46494C31).

use std::fs::{self, File};
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
const MAX_CMS_DEPTH: u64 = 1024;
const MAX_CMS_COUNTERS: u64 = 16_777_216;

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

fn cms_file_size(width: u64, depth: u64) -> Result<u64, String> {
    (MMAP_HEADER_SIZE as u64)
        .checked_add(cms_counter_bytes(width, depth)?)
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
    let delta = (src_count as u128)
        .checked_mul(weight.unsigned_abs() as u128)
        .ok_or_else(|| "CMS total count overflow".to_string())?;

    if weight >= 0 {
        let next = (total_count as u128)
            .checked_add(delta)
            .ok_or_else(|| "CMS total count overflow".to_string())?;
        u64::try_from(next).map_err(|_| "CMS total count overflow".to_string())
    } else if delta >= total_count as u128 {
        Ok(0)
    } else {
        Ok((total_count as u128 - delta) as u64)
    }
}

fn cms_finalize_merge_counter(acc: i128) -> Result<i64, String> {
    if acc < 0 {
        Ok(0)
    } else {
        i64::try_from(acc).map_err(|_| "CMS counter overflow".to_string())
    }
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
    let _ = cms_counter_bytes(width, depth)?;

    Ok((width, depth, count))
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
#[rustler::nif(schedule = "Normal")]
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
        if let Err(e) = fs::create_dir_all(parent) {
            return Ok((atoms::error(), format!("mkdir: {e}")).encode(env));
        }
    }

    let mut file = match File::create(p) {
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

    if let Err(e) = file.sync_data() {
        return Ok((atoms::error(), format!("fdatasync: {e}")).encode(env));
    }

    Ok((atoms::ok(), atoms::ok()).encode(env))
}

/// Increment elements in a CMS file via pread/pwrite.
///
/// `items` is a list of `{element_binary, count_integer}` tuples.
///
/// Returns `{:ok, [min_count, ...]}` or `{:error, reason}`.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn cms_file_incrby<'a>(
    env: Env<'a>,
    path: String,
    items: Vec<(rustler::Binary<'a>, i64)>,
) -> NifResult<Term<'a>> {
    let file = match crate::open_random_rw(Path::new(&path)) {
        Ok(f) => f,
        Err(e) => return Ok(map_io_error(&e).encode(env)),
    };

    let (width, depth, mut total_count) = match cms_file_read_header(&file) {
        Ok(h) => h,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    let mut counts: Vec<i64> = Vec::with_capacity(items.len());
    let mut buf = [0u8; 8];

    for (idx, (element, count)) in items.iter().enumerate() {
        let indices = hash_indices_standalone(element.as_slice(), width, depth);
        let mut min_val = i64::MAX;
        let mut updates: Vec<(u64, i64)> = Vec::with_capacity(indices.len());

        for (row, &col) in indices.iter().enumerate() {
            let offset = MMAP_HEADER_SIZE as u64 + (row as u64 * width + col) * 8;

            if let Err(e) = cms_read_exact_at(&file, &mut buf, offset, "counter") {
                return Ok((atoms::error(), e).encode(env));
            }
            let val = i64::from_le_bytes(buf);

            let next_val = match val.checked_add(*count) {
                Some(next_val) => next_val,
                None => {
                    return Ok((
                        atoms::error(),
                        format!("CMS counter overflow: {val} + {count}"),
                    )
                        .encode(env));
                }
            };

            min_val = min_val.min(next_val);
            updates.push((offset, next_val));
        }

        let next_total_count = match cms_next_total_count(total_count, *count) {
            Ok(next_total_count) => next_total_count,
            Err(e) => return Ok((atoms::error(), e).encode(env)),
        };

        for (offset, next_val) in updates {
            if let Err(e) =
                crate::write_all_at(&file, &next_val.to_le_bytes(), offset, "cms counter")
            {
                return Ok((atoms::error(), e).encode(env));
            }
        }

        total_count = next_total_count;
        counts.push(min_val);

        if idx % YIELD_CHECK_INTERVAL == 0 && idx > 0 {
            let _ = consume_timeslice(env, 1);
        }
    }

    // Update total count in header
    if let Err(e) = crate::write_all_at(&file, &total_count.to_le_bytes(), 24, "cms count") {
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
    Ok((atoms::ok(), counts).encode(env))
}

/// Query elements in a CMS file via pread.
///
/// `elements` is a list of binaries.
///
/// Returns `{:ok, [count, ...]}` or `{:error, reason}`.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn cms_file_query<'a>(
    env: Env<'a>,
    path: String,
    elements: Vec<rustler::Binary<'a>>,
) -> NifResult<Term<'a>> {
    let file = match crate::open_random_read(Path::new(&path)) {
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
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn cms_file_info(env: Env, path: String) -> NifResult<Term> {
    let file = match crate::open_random_read(Path::new(&path)) {
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

/// Merge source CMS files (with weights) into a destination file via pread/pwrite.
///
/// For each counter position: read dst counter, add weighted src counters,
/// clamp negatives to 0, write back. Updates dst count.
///
/// Returns `:ok` or `{:error, reason}`.
#[rustler::nif(schedule = "Normal")]
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

    // Open destination read+write
    let dst_file = match crate::open_random_rw(Path::new(&dst_path)) {
        Ok(f) => f,
        Err(e) => return Ok(map_io_error(&e).encode(env)),
    };

    let (dst_width, dst_depth, dst_count) = match cms_file_read_header(&dst_file) {
        Ok(h) => h,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    // Open each source read-only and validate dimensions
    let mut src_files: Vec<(File, u64)> = Vec::with_capacity(src_paths.len());
    for src_path in &src_paths {
        let src_file = match crate::open_random_read(Path::new(src_path)) {
            Ok(f) => f,
            Err(e) => return Ok(map_io_error(&e).encode(env)),
        };

        let (src_width, src_depth, src_count) = match cms_file_read_header(&src_file) {
            Ok(h) => h,
            Err(e) => return Ok((atoms::error(), e).encode(env)),
        };

        if src_width != dst_width || src_depth != dst_depth {
            return Ok((atoms::error(), "width/depth mismatch").encode(env));
        }

        src_files.push((src_file, src_count));
    }

    if src_files.is_empty() {
        return Ok(atoms::ok().encode(env));
    }

    let mut next_dst_count = dst_count;
    for (j, (_, src_count)) in src_files.iter().enumerate() {
        next_dst_count = match cms_next_merge_total_count(next_dst_count, *src_count, weights[j]) {
            Ok(next_dst_count) => next_dst_count,
            Err(e) => return Ok((atoms::error(), e).encode(env)),
        };
    }

    let total_counters = dst_width * dst_depth;
    let mut dst_buf = [0u8; 8];
    let mut src_buf = [0u8; 8];
    let mut merged_counters = Vec::with_capacity(total_counters as usize);

    for i in 0..total_counters {
        let offset = MMAP_HEADER_SIZE as u64 + i * 8;

        // Read dst counter
        if let Err(e) = cms_read_exact_at(&dst_file, &mut dst_buf, offset, "destination counter") {
            return Ok((atoms::error(), e).encode(env));
        }
        let mut val = i128::from(i64::from_le_bytes(dst_buf));

        // Add weighted src counters
        for (j, (src_file, _)) in src_files.iter().enumerate() {
            if let Err(e) = cms_read_exact_at(src_file, &mut src_buf, offset, "source counter") {
                return Ok((atoms::error(), e).encode(env));
            }
            let src_val = i64::from_le_bytes(src_buf);
            let delta = i128::from(src_val) * i128::from(weights[j]);
            val = match val.checked_add(delta) {
                Some(next) => next,
                None => return Ok((atoms::error(), "CMS counter overflow").encode(env)),
            };
        }

        let val = match cms_finalize_merge_counter(val) {
            Ok(val) => val,
            Err(e) => return Ok((atoms::error(), e).encode(env)),
        };
        merged_counters.push(val);

        if (i as usize) % YIELD_CHECK_INTERVAL == 0 && i > 0 {
            let _ = consume_timeslice(env, 1);
        }
    }

    for (i, val) in merged_counters.iter().enumerate() {
        let offset = MMAP_HEADER_SIZE as u64 + (i as u64) * 8;
        if let Err(e) = crate::write_all_at(&dst_file, &val.to_le_bytes(), offset, "cms counter") {
            return Ok((atoms::error(), e).encode(env));
        }

        if i % YIELD_CHECK_INTERVAL == 0 && i > 0 {
            let _ = consume_timeslice(env, 1);
        }
    }

    if let Err(e) = crate::write_all_at(&dst_file, &next_dst_count.to_le_bytes(), 24, "cms count") {
        return Ok((atoms::error(), e).encode(env));
    }

    // Durability: fsync the destination before returning. Sources are
    // read-only during merge so they don't need fsync.
    if let Err(e) = crate::prob_fsync(&dst_file) {
        return Ok((atoms::error(), e).encode(env));
    }

    crate::fadvise_dontneed(&dst_file, 0, 0);
    for (src_file, _) in &src_files {
        crate::fadvise_dontneed(src_file, 0, 0);
    }

    Ok(atoms::ok().encode(env))
}

// ---------------------------------------------------------------------------
// Async variants of read NIFs — Tokio spawn_blocking, never block BEAM
// ---------------------------------------------------------------------------

/// Async CMS query: spawns on Tokio, sends result to `caller_pid`.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value)]
pub fn cms_file_query_async<'a>(
    env: Env<'a>,
    caller_pid: LocalPid,
    correlation_id: u64,
    path: String,
    elements: Vec<rustler::Binary<'a>>,
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
        })
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
    crate::async_io::runtime().spawn(async move {
        let result = tokio::task::spawn_blocking(move || {
            let file = crate::open_random_read(std::path::Path::new(&path)).map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    "enoent".to_string()
                } else {
                    e.to_string()
                }
            })?;
            let (width, depth, count) = cms_file_read_header(&file).map_err(|e| e.clone())?;
            crate::fadvise_dontneed(&file, 0, 0);
            Ok((width, depth, count))
        })
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
