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
    // docs/bitcask-background-fsync.md.
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
    use super::*;

    // -----------------------------------------------------------------------
    // Stateless pread/pwrite file tests
    // -----------------------------------------------------------------------

    #[test]
    fn file_create_and_info() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("create_info.cms");
        let path_str = path.to_str().unwrap().to_string();

        {
            let p = Path::new(&path_str);
            if let Some(parent) = p.parent() {
                fs::create_dir_all(parent).unwrap();
            }
            let counter_bytes = 100usize * 7 * 8;
            let mut file = File::create(p).unwrap();
            let mut header = [0u8; MMAP_HEADER_SIZE];
            header[0..8].copy_from_slice(&MMAP_MAGIC.to_le_bytes());
            header[8..16].copy_from_slice(&100u64.to_le_bytes());
            header[16..24].copy_from_slice(&7u64.to_le_bytes());
            file.write_all(&header).unwrap();
            file.write_all(&vec![0u8; counter_bytes]).unwrap();
            file.sync_all().unwrap();
        }

        let file = File::open(&path_str).unwrap();
        let (w, d, c) = cms_file_read_header(&file).unwrap();
        assert_eq!(w, 100);
        assert_eq!(d, 7);
        assert_eq!(c, 0);
    }

    #[test]
    fn file_query_unseen_returns_zero() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("unseen.cms");
        let width = 100u64;
        let depth = 5u64;

        {
            let counter_bytes = (width as usize) * (depth as usize) * 8;
            let mut file = File::create(&path).unwrap();
            let mut header = [0u8; MMAP_HEADER_SIZE];
            header[0..8].copy_from_slice(&MMAP_MAGIC.to_le_bytes());
            header[8..16].copy_from_slice(&width.to_le_bytes());
            header[16..24].copy_from_slice(&depth.to_le_bytes());
            file.write_all(&header).unwrap();
            file.write_all(&vec![0u8; counter_bytes]).unwrap();
            file.sync_all().unwrap();
        }

        let file = File::open(&path).unwrap();
        let (w, d, _) = cms_file_read_header(&file).unwrap();
        let indices = hash_indices_standalone(b"never_seen", w, d);
        let mut buf = [0u8; 8];
        let mut min_val = i64::MAX;
        for (row, &col) in indices.iter().enumerate() {
            let offset = MMAP_HEADER_SIZE as u64 + (row as u64 * w + col) * 8;
            file.read_at(&mut buf, offset).unwrap();
            min_val = min_val.min(i64::from_le_bytes(buf));
        }
        assert_eq!(min_val, 0);
    }

    #[test]
    fn file_read_header_nonexistent_returns_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nonexistent.cms");
        let result = File::open(&path);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::NotFound);
    }

    #[test]
    fn standalone_hash_deterministic() {
        let element = b"test_element";
        let h1 = fnv1a_standalone(element);
        let h2 = fnv1a_standalone(element);
        assert_eq!(h1, h2, "fnv1a must be deterministic");

        let s1 = fnv1a_salted_standalone(element);
        let s2 = fnv1a_salted_standalone(element);
        assert_eq!(s1, s2, "fnv1a_salted must be deterministic");

        // salted and unsalted must differ
        assert_ne!(h1, s1, "salted and unsalted hashes must differ");
    }

    #[test]
    fn hash_indices_within_bounds() {
        let width = 100u64;
        let depth = 7u64;
        let indices = hash_indices_standalone(b"test", width, depth);
        assert_eq!(indices.len(), depth as usize);
        for &idx in &indices {
            assert!(idx < width, "index {idx} >= width {width}");
        }
    }

    // -----------------------------------------------------------------------
    // Edge case tests
    // -----------------------------------------------------------------------

    /// Helper: create a valid CMS file, returning the path string.
    fn create_cms_file(dir: &std::path::Path, name: &str, width: u64, depth: u64) -> String {
        let path = dir.join(name);
        let counter_bytes = (width as usize) * (depth as usize) * 8;
        let mut file = File::create(&path).unwrap();
        let mut header = [0u8; MMAP_HEADER_SIZE];
        header[0..8].copy_from_slice(&MMAP_MAGIC.to_le_bytes());
        header[8..16].copy_from_slice(&width.to_le_bytes());
        header[16..24].copy_from_slice(&depth.to_le_bytes());
        file.write_all(&header).unwrap();
        file.write_all(&vec![0u8; counter_bytes]).unwrap();
        file.sync_all().unwrap();
        path.to_str().unwrap().to_string()
    }

    #[test]
    fn empty_element_hashing() {
        // Zero-length element should hash without panic
        let h1 = fnv1a_standalone(b"");
        let h2 = fnv1a_salted_standalone(b"");
        // Should produce valid hashes (not necessarily different for empty input)
        assert!(h1 != 0 || h2 != 0, "at least one hash should be non-zero");

        let indices = hash_indices_standalone(b"", 100, 5);
        assert_eq!(indices.len(), 5);
        for &idx in &indices {
            assert!(idx < 100);
        }
    }

    #[test]
    fn large_element_hashing() {
        // 1MB element
        let big = vec![0xCDu8; 1_000_000];
        let indices = hash_indices_standalone(&big, 1000, 7);
        assert_eq!(indices.len(), 7);
        for &idx in &indices {
            assert!(idx < 1000);
        }
    }

    #[test]
    fn truncated_header_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("truncated.cms");
        std::fs::write(&path, [0u8; 16]).unwrap();
        let file = File::open(&path).unwrap();
        assert!(cms_file_read_header(&file).is_err());
    }

    #[test]
    fn read_exact_at_rejects_short_counter() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("short_counter.cms");
        std::fs::write(&path, [0u8; 4]).unwrap();
        let file = File::open(&path).unwrap();
        let mut buf = [0u8; 8];

        let err = cms_read_exact_at(&file, &mut buf, 0, "counter").unwrap_err();

        assert!(err.contains("truncated"));
    }

    #[test]
    fn wrong_magic_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad_magic.cms");
        let mut data = [0u8; MMAP_HEADER_SIZE + 64];
        data[0..8].copy_from_slice(&0xBAAD_F00D_u64.to_le_bytes());
        data[8..16].copy_from_slice(&10u64.to_le_bytes());
        data[16..24].copy_from_slice(&5u64.to_le_bytes());
        std::fs::write(&path, data).unwrap();
        let file = File::open(&path).unwrap();
        let result = cms_file_read_header(&file);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("magic"));
    }

    #[test]
    fn header_with_zero_width_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("zero_width.cms");
        let mut header = [0u8; MMAP_HEADER_SIZE];
        header[0..8].copy_from_slice(&MMAP_MAGIC.to_le_bytes());
        header[8..16].copy_from_slice(&0u64.to_le_bytes()); // width=0
        header[16..24].copy_from_slice(&5u64.to_le_bytes());
        std::fs::write(&path, header).unwrap();
        let file = File::open(&path).unwrap();
        let result = cms_file_read_header(&file);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("width and depth must be > 0"));
    }

    #[test]
    fn header_with_oversized_counter_region_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("oversized.cms");
        let mut header = [0u8; MMAP_HEADER_SIZE];
        header[0..8].copy_from_slice(&MMAP_MAGIC.to_le_bytes());
        header[8..16].copy_from_slice(&u64::MAX.to_le_bytes());
        header[16..24].copy_from_slice(&2u64.to_le_bytes());
        std::fs::write(&path, header).unwrap();
        let file = File::open(&path).unwrap();

        let result = cms_file_read_header(&file);

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("counter"));
    }

    #[test]
    fn minimum_size_cms() {
        // width=1, depth=1 -- smallest possible CMS
        let dir = tempfile::tempdir().unwrap();
        let path_str = create_cms_file(dir.path(), "min.cms", 1, 1);
        let file = File::open(&path_str).unwrap();
        let (w, d, c) = cms_file_read_header(&file).unwrap();
        assert_eq!(w, 1);
        assert_eq!(d, 1);
        assert_eq!(c, 0);
    }

    #[test]
    fn incrby_roundtrip() {
        // Create, increment, then query
        let dir = tempfile::tempdir().unwrap();
        let path_str = create_cms_file(dir.path(), "incr.cms", 100, 5);

        let file = crate::open_random_rw(Path::new(&path_str)).unwrap();
        let (width, depth, _) = cms_file_read_header(&file).unwrap();

        // Increment "hello" by 3
        let indices = hash_indices_standalone(b"hello", width, depth);
        let mut buf = [0u8; 8];
        for (row, &col) in indices.iter().enumerate() {
            let offset = MMAP_HEADER_SIZE as u64 + (row as u64 * width + col) * 8;
            file.read_at(&mut buf, offset).unwrap();
            let mut val = i64::from_le_bytes(buf);
            val += 3;
            file.write_at(&val.to_le_bytes(), offset).unwrap();
        }
        drop(file);

        // Query
        let file = crate::open_random_read(Path::new(&path_str)).unwrap();
        let (width, depth, _) = cms_file_read_header(&file).unwrap();
        let indices = hash_indices_standalone(b"hello", width, depth);
        let mut min_val = i64::MAX;
        for (row, &col) in indices.iter().enumerate() {
            let offset = MMAP_HEADER_SIZE as u64 + (row as u64 * width + col) * 8;
            file.read_at(&mut buf, offset).unwrap();
            let val = i64::from_le_bytes(buf);
            min_val = min_val.min(val);
        }
        assert_eq!(min_val, 3);
    }

    #[test]
    fn merge_additive() {
        // Create two CMS files, increment differently, merge, verify additive result.
        let dir = tempfile::tempdir().unwrap();
        let dst_str = create_cms_file(dir.path(), "dst.cms", 50, 3);
        let src_str = create_cms_file(dir.path(), "src.cms", 50, 3);

        // Increment "foo" by 5 in dst
        {
            let file = crate::open_random_rw(Path::new(&dst_str)).unwrap();
            let (width, depth, _) = cms_file_read_header(&file).unwrap();
            let indices = hash_indices_standalone(b"foo", width, depth);
            for (row, &col) in indices.iter().enumerate() {
                let offset = MMAP_HEADER_SIZE as u64 + (row as u64 * width + col) * 8;
                file.write_at(&5i64.to_le_bytes(), offset).unwrap();
            }
            // Set count=5
            file.write_at(&5u64.to_le_bytes(), 24).unwrap();
        }

        // Increment "foo" by 3 in src
        {
            let file = crate::open_random_rw(Path::new(&src_str)).unwrap();
            let (width, depth, _) = cms_file_read_header(&file).unwrap();
            let indices = hash_indices_standalone(b"foo", width, depth);
            for (row, &col) in indices.iter().enumerate() {
                let offset = MMAP_HEADER_SIZE as u64 + (row as u64 * width + col) * 8;
                file.write_at(&3i64.to_le_bytes(), offset).unwrap();
            }
            file.write_at(&3u64.to_le_bytes(), 24).unwrap();
        }

        // Merge: dst += src * 1
        {
            let dst_file = crate::open_random_rw(Path::new(&dst_str)).unwrap();
            let src_file = crate::open_random_read(Path::new(&src_str)).unwrap();
            let (width, depth, _) = cms_file_read_header(&dst_file).unwrap();
            let total_counters = width * depth;
            let mut dst_buf = [0u8; 8];
            let mut src_buf = [0u8; 8];
            for i in 0..total_counters {
                let offset = MMAP_HEADER_SIZE as u64 + i * 8;
                dst_file.read_at(&mut dst_buf, offset).unwrap();
                src_file.read_at(&mut src_buf, offset).unwrap();
                let val = i64::from_le_bytes(dst_buf) + i64::from_le_bytes(src_buf);
                dst_file.write_at(&val.to_le_bytes(), offset).unwrap();
            }
        }

        // Verify: "foo" should have count 8 (5+3)
        let file = crate::open_random_read(Path::new(&dst_str)).unwrap();
        let (width, depth, _) = cms_file_read_header(&file).unwrap();
        let indices = hash_indices_standalone(b"foo", width, depth);
        let mut min_val = i64::MAX;
        let mut buf = [0u8; 8];
        for (row, &col) in indices.iter().enumerate() {
            let offset = MMAP_HEADER_SIZE as u64 + (row as u64 * width + col) * 8;
            file.read_at(&mut buf, offset).unwrap();
            min_val = min_val.min(i64::from_le_bytes(buf));
        }
        assert_eq!(min_val, 8);
    }

    #[test]
    fn nonexistent_file_returns_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nope.cms");
        let result = crate::open_random_read(&path);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::NotFound);
    }

    #[test]
    fn null_bytes_in_element() {
        let element = b"foo\x00bar\x00baz";
        let indices = hash_indices_standalone(element, 100, 5);
        assert_eq!(indices.len(), 5);
        for &idx in &indices {
            assert!(idx < 100);
        }
        // Should differ from element without nulls
        let indices2 = hash_indices_standalone(b"foobarbaz", 100, 5);
        assert_ne!(indices, indices2);
    }

    // -----------------------------------------------------------------------
    // Incrby / query file I/O tests
    // -----------------------------------------------------------------------

    /// Helper: do pread/pwrite incrby for a single element, return min count.
    fn file_incrby_one(path: &str, element: &[u8], count: i64) -> i64 {
        let file = crate::open_random_rw(Path::new(path)).unwrap();
        let (width, depth, mut total_count) = cms_file_read_header(&file).unwrap();
        let indices = hash_indices_standalone(element, width, depth);
        let mut buf = [0u8; 8];
        let mut min_val = i64::MAX;
        for (row, &col) in indices.iter().enumerate() {
            let offset = MMAP_HEADER_SIZE as u64 + (row as u64 * width + col) * 8;
            file.read_at(&mut buf, offset).unwrap();
            let mut val = i64::from_le_bytes(buf);
            val += count;
            file.write_at(&val.to_le_bytes(), offset).unwrap();
            min_val = min_val.min(val);
        }
        total_count = total_count.wrapping_add(count as u64);
        file.write_at(&total_count.to_le_bytes(), 24).unwrap();
        min_val
    }

    /// Helper: query a single element via pread, return min count.
    fn file_query_one(path: &str, element: &[u8]) -> i64 {
        let file = crate::open_random_read(Path::new(path)).unwrap();
        let (width, depth, _) = cms_file_read_header(&file).unwrap();
        let indices = hash_indices_standalone(element, width, depth);
        let mut buf = [0u8; 8];
        let mut min_val = i64::MAX;
        for (row, &col) in indices.iter().enumerate() {
            let offset = MMAP_HEADER_SIZE as u64 + (row as u64 * width + col) * 8;
            file.read_at(&mut buf, offset).unwrap();
            min_val = min_val.min(i64::from_le_bytes(buf));
        }
        min_val
    }

    #[test]
    fn incrby_by_one() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_cms_file(dir.path(), "incr1.cms", 100, 5);
        let min = file_incrby_one(&path, b"elem", 1);
        assert_eq!(min, 1);
    }

    #[test]
    fn incrby_by_ten() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_cms_file(dir.path(), "incr10.cms", 100, 5);
        let min = file_incrby_one(&path, b"elem", 10);
        assert_eq!(min, 10);
    }

    #[test]
    fn incrby_negative() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_cms_file(dir.path(), "incr_neg.cms", 100, 5);
        // Increment up first, then decrement
        file_incrby_one(&path, b"x", 5);
        let min = file_incrby_one(&path, b"x", -3);
        assert_eq!(min, 2);
    }

    #[test]
    fn incrby_accumulates() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_cms_file(dir.path(), "accum.cms", 100, 5);
        file_incrby_one(&path, b"item", 3);
        file_incrby_one(&path, b"item", 7);
        file_incrby_one(&path, b"item", 2);
        let count = file_query_one(&path, b"item");
        assert_eq!(count, 12);
    }

    #[test]
    fn query_multiple_independent_elements() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_cms_file(dir.path(), "multi.cms", 200, 7);
        file_incrby_one(&path, b"alpha", 10);
        file_incrby_one(&path, b"beta", 20);
        file_incrby_one(&path, b"gamma", 5);

        let a = file_query_one(&path, b"alpha");
        let b = file_query_one(&path, b"beta");
        let g = file_query_one(&path, b"gamma");
        let unseen = file_query_one(&path, b"delta");

        // CMS can overcount due to collisions, but never undercount
        assert!(a >= 10, "alpha: expected >=10, got {a}");
        assert!(b >= 20, "beta: expected >=20, got {b}");
        assert!(g >= 5, "gamma: expected >=5, got {g}");
        assert_eq!(unseen, 0);
    }

    #[test]
    fn merge_two_files_additive() {
        let dir = tempfile::tempdir().unwrap();
        let dst = create_cms_file(dir.path(), "merge_dst.cms", 50, 3);
        let src = create_cms_file(dir.path(), "merge_src.cms", 50, 3);

        file_incrby_one(&dst, b"key", 10);
        file_incrby_one(&src, b"key", 7);

        // Merge src into dst with weight 1
        {
            let dst_file = crate::open_random_rw(Path::new(&dst)).unwrap();
            let src_file = crate::open_random_read(Path::new(&src)).unwrap();
            let (width, depth, _) = cms_file_read_header(&dst_file).unwrap();
            let total = width * depth;
            let mut db = [0u8; 8];
            let mut sb = [0u8; 8];
            for i in 0..total {
                let offset = MMAP_HEADER_SIZE as u64 + i * 8;
                dst_file.read_at(&mut db, offset).unwrap();
                src_file.read_at(&mut sb, offset).unwrap();
                let val = i64::from_le_bytes(db) + i64::from_le_bytes(sb);
                dst_file.write_at(&val.to_le_bytes(), offset).unwrap();
            }
        }

        let count = file_query_one(&dst, b"key");
        assert_eq!(count, 17);
    }

    #[test]
    fn merge_with_weight() {
        let dir = tempfile::tempdir().unwrap();
        let dst = create_cms_file(dir.path(), "mw_dst.cms", 50, 3);
        let src = create_cms_file(dir.path(), "mw_src.cms", 50, 3);

        file_incrby_one(&dst, b"w", 4);
        file_incrby_one(&src, b"w", 3);

        // Merge src into dst with weight 2: dst += src * 2
        {
            let dst_file = crate::open_random_rw(Path::new(&dst)).unwrap();
            let src_file = crate::open_random_read(Path::new(&src)).unwrap();
            let (width, depth, _) = cms_file_read_header(&dst_file).unwrap();
            let total = width * depth;
            let mut db = [0u8; 8];
            let mut sb = [0u8; 8];
            let weight: i64 = 2;
            for i in 0..total {
                let offset = MMAP_HEADER_SIZE as u64 + i * 8;
                dst_file.read_at(&mut db, offset).unwrap();
                src_file.read_at(&mut sb, offset).unwrap();
                let val = i64::from_le_bytes(db) + i64::from_le_bytes(sb) * weight;
                dst_file.write_at(&val.to_le_bytes(), offset).unwrap();
            }
        }

        // 4 + 3*2 = 10
        let count = file_query_one(&dst, b"w");
        assert_eq!(count, 10);
    }

    #[test]
    fn merge_total_count_rejects_positive_overflow() {
        let err = cms_next_merge_total_count(u64::MAX, 1, 1).unwrap_err();

        assert!(err.contains("overflow"));
    }

    #[test]
    fn merge_total_count_clamps_negative_delta_to_zero() {
        assert_eq!(cms_next_merge_total_count(5, 10, -1).unwrap(), 0);
    }

    #[test]
    fn merge_counter_rejects_positive_overflow() {
        let err = cms_finalize_merge_counter(i128::from(i64::MAX) + 1).unwrap_err();

        assert!(err.contains("overflow"));
    }

    #[test]
    fn counter_large_values() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_cms_file(dir.path(), "large.cms", 100, 5);
        // Increment near i64 max / 2 twice — should not panic
        let big = i64::MAX / 2;
        let min1 = file_incrby_one(&path, b"big", big);
        assert_eq!(min1, big);
        // Second increment wraps — just verify no panic
        let min2 = file_incrby_one(&path, b"big", big);
        assert_eq!(min2, big.wrapping_add(big));
    }

    #[test]
    fn concurrent_reads_during_incrby() {
        use std::sync::Arc;
        use std::thread;

        let dir = tempfile::tempdir().unwrap();
        let path = create_cms_file(dir.path(), "conc.cms", 200, 5);
        let path_arc = Arc::new(path);

        // Writer thread
        let pw = Arc::clone(&path_arc);
        let writer = thread::spawn(move || {
            for _ in 0..100 {
                file_incrby_one(&pw, b"concurrent", 1);
            }
        });

        // Reader threads
        let mut readers = Vec::new();
        for _ in 0..4 {
            let pr = Arc::clone(&path_arc);
            readers.push(thread::spawn(move || {
                for _ in 0..50 {
                    let count = file_query_one(&pr, b"concurrent");
                    // count must be non-negative (no corruption)
                    assert!(count >= 0, "negative count during concurrent read: {count}");
                }
            }));
        }

        writer.join().unwrap();
        for r in readers {
            r.join().unwrap();
        }

        // Final count must be exactly 100
        let final_count = file_query_one(&path_arc, b"concurrent");
        assert_eq!(final_count, 100);
    }

    #[test]
    fn total_count_in_header_tracks_increments() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_cms_file(dir.path(), "hdr_count.cms", 100, 5);
        file_incrby_one(&path, b"a", 3);
        file_incrby_one(&path, b"b", 7);

        let file = crate::open_random_read(Path::new(&path)).unwrap();
        let (_, _, total_count) = cms_file_read_header(&file).unwrap();
        assert_eq!(total_count, 10);
    }
}
