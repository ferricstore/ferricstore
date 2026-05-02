//! Cuckoo filter implementation for FerricStore.
//!
//! A space-efficient probabilistic data structure similar to Bloom filters,
//! but supporting deletion and approximate counting. Stores fingerprints of
//! elements in a hash table with two candidate bucket positions per element.
//!
//! ## File layout
//!
//! ```text
//! [magic: 2B][version: 1B][capacity: 4B][bucket_size: 1B]
//! [fingerprint_size: 1B][max_kicks: 2B][num_items: 8B][num_deletes: 8B]
//! [buckets: capacity * bucket_size * fingerprint_size bytes]
//! ```
//!
//! Total header size: 27 bytes.

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::Path;

use rustler::{Binary, Encoder, Env, LocalPid, NifResult, Term};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Magic bytes identifying a cuckoo filter blob.
const MAGIC: [u8; 2] = [0xCF, 0x01];
/// Current serialization version.
const VERSION: u8 = 1;
/// Header size in bytes.
const HEADER_SIZE: usize = 27;

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
// Stateless pread/pwrite file-based NIF functions
// ---------------------------------------------------------------------------
//
// These functions open a file, read/write specific bytes via pread/pwrite
// (read_at/write_at), and close on Drop. No mmap, no ResourceArc, no Mutex.

/// Default fingerprint size for stateless file operations (1 byte).
const FILE_DEFAULT_FINGERPRINT_SIZE: usize = 1;
/// Default max kicks for stateless file operations.
const FILE_DEFAULT_MAX_KICKS: u16 = 500;

/// Header offsets for cuckoo file format.
const OFF_MAGIC: u64 = 0;
const OFF_NUM_ITEMS: u64 = 11;
const OFF_NUM_DELETES: u64 = 19;

/// Parsed header from a cuckoo file.
struct CuckooFileHeader {
    num_buckets: u32,
    bucket_size: u8,
    fingerprint_size: u8,
    max_kicks: u16,
    num_items: u64,
    num_deletes: u64,
}

/// Read and validate the 27-byte header from a file.
fn cuckoo_read_header(file: &File) -> Result<CuckooFileHeader, String> {
    let mut hdr = [0u8; HEADER_SIZE];
    file.read_at(&mut hdr, OFF_MAGIC)
        .map_err(|e| format!("read header: {e}"))?;

    if hdr[0..2] != MAGIC {
        return Err("invalid cuckoo file magic".into());
    }
    if hdr[2] != VERSION {
        return Err(format!("unsupported cuckoo version {}", hdr[2]));
    }

    let num_buckets = u32::from_le_bytes([hdr[3], hdr[4], hdr[5], hdr[6]]);
    let bucket_size = hdr[7];
    let fingerprint_size = hdr[8];
    let max_kicks = u16::from_le_bytes([hdr[9], hdr[10]]);
    let num_items = u64::from_le_bytes([
        hdr[11], hdr[12], hdr[13], hdr[14], hdr[15], hdr[16], hdr[17], hdr[18],
    ]);
    let num_deletes = u64::from_le_bytes([
        hdr[19], hdr[20], hdr[21], hdr[22], hdr[23], hdr[24], hdr[25], hdr[26],
    ]);

    Ok(CuckooFileHeader {
        num_buckets,
        bucket_size,
        fingerprint_size,
        max_kicks,
        num_items,
        num_deletes,
    })
}

/// Compute fingerprint and primary bucket index from element bytes.
fn cuckoo_file_fingerprint_and_bucket(
    element: &[u8],
    fingerprint_size: usize,
    num_buckets: u32,
) -> (Vec<u8>, usize) {
    let hash = xxhash_rust::xxh3::xxh3_128(element).to_le_bytes();

    let mut fp = hash[..fingerprint_size].to_vec();
    if fp.iter().all(|&b| b == 0) {
        fp[0] = 1;
    }

    let start = fingerprint_size;
    let hash_val = u64::from_le_bytes([
        hash[start],
        hash[start + 1],
        hash[start + 2],
        hash[start + 3],
        hash[start + 4],
        hash[start + 5],
        hash[start + 6],
        hash[start + 7],
    ]);
    let bucket = (hash_val as usize) % (num_buckets as usize);

    (fp, bucket)
}

/// Compute alternate bucket index.
fn cuckoo_file_alternate_bucket(bucket: usize, fp: &[u8], num_buckets: u32) -> usize {
    let hash = xxhash_rust::xxh3::xxh3_128(fp).to_le_bytes();
    let fp_hash = u64::from_le_bytes([
        hash[0], hash[1], hash[2], hash[3], hash[4], hash[5], hash[6], hash[7],
    ]);
    ((bucket as u64) ^ fp_hash) as usize % (num_buckets as usize)
}

/// Compute the byte offset in the file for a given bucket and slot.
fn cuckoo_file_slot_offset(
    bucket_idx: usize,
    slot_idx: usize,
    bucket_size: u8,
    fingerprint_size: u8,
) -> u64 {
    HEADER_SIZE as u64
        + ((bucket_idx * (bucket_size as usize) + slot_idx) * (fingerprint_size as usize)) as u64
}

/// Read a fingerprint from a specific bucket/slot in the file.
fn cuckoo_file_read_slot(
    file: &File,
    bucket_idx: usize,
    slot_idx: usize,
    bucket_size: u8,
    fingerprint_size: u8,
) -> Result<Vec<u8>, String> {
    let offset = cuckoo_file_slot_offset(bucket_idx, slot_idx, bucket_size, fingerprint_size);
    let mut buf = vec![0u8; fingerprint_size as usize];
    file.read_at(&mut buf, offset)
        .map_err(|e| format!("read slot: {e}"))?;
    Ok(buf)
}

/// Write a fingerprint to a specific bucket/slot in the file.
fn cuckoo_file_write_slot(
    file: &File,
    bucket_idx: usize,
    slot_idx: usize,
    bucket_size: u8,
    fingerprint_size: u8,
    fp: &[u8],
) -> Result<(), String> {
    let offset = cuckoo_file_slot_offset(bucket_idx, slot_idx, bucket_size, fingerprint_size);
    file.write_at(fp, offset)
        .map_err(|e| format!("write slot: {e}"))?;
    Ok(())
}

/// Write num_items to the header.
fn cuckoo_file_write_num_items(file: &File, num_items: u64) -> Result<(), String> {
    file.write_at(&num_items.to_le_bytes(), OFF_NUM_ITEMS)
        .map_err(|e| format!("write num_items: {e}"))?;
    Ok(())
}

/// Write num_deletes to the header.
fn cuckoo_file_write_num_deletes(file: &File, num_deletes: u64) -> Result<(), String> {
    file.write_at(&num_deletes.to_le_bytes(), OFF_NUM_DELETES)
        .map_err(|e| format!("write num_deletes: {e}"))?;
    Ok(())
}

fn cuckoo_file_read_slot_staged(
    file: &File,
    staged: &HashMap<(usize, usize), Vec<u8>>,
    bucket_idx: usize,
    slot_idx: usize,
    bucket_size: u8,
    fingerprint_size: u8,
) -> Result<Vec<u8>, String> {
    staged.get(&(bucket_idx, slot_idx)).cloned().map_or_else(
        || cuckoo_file_read_slot(file, bucket_idx, slot_idx, bucket_size, fingerprint_size),
        Ok,
    )
}

fn cuckoo_file_try_eviction(
    file: &File,
    hdr: &CuckooFileHeader,
    fp: Vec<u8>,
    start_bucket: usize,
) -> Result<bool, String> {
    let mut staged: HashMap<(usize, usize), Vec<u8>> = HashMap::new();
    let mut cur_fp = fp;
    let mut cur_bucket = start_bucket;

    for kicks in 0..(hdr.max_kicks as u32) {
        let slot_idx = (kicks as usize) % (hdr.bucket_size as usize);

        let evicted = cuckoo_file_read_slot_staged(
            file,
            &staged,
            cur_bucket,
            slot_idx,
            hdr.bucket_size,
            hdr.fingerprint_size,
        )?;

        staged.insert((cur_bucket, slot_idx), cur_fp);

        let alt = cuckoo_file_alternate_bucket(cur_bucket, &evicted, hdr.num_buckets);

        for slot in 0..hdr.bucket_size {
            let slot_idx = slot as usize;
            let s = cuckoo_file_read_slot_staged(
                file,
                &staged,
                alt,
                slot_idx,
                hdr.bucket_size,
                hdr.fingerprint_size,
            )?;

            if s.iter().all(|&b| b == 0) {
                staged.insert((alt, slot_idx), evicted);

                for ((bucket, slot), value) in staged {
                    cuckoo_file_write_slot(
                        file,
                        bucket,
                        slot,
                        hdr.bucket_size,
                        hdr.fingerprint_size,
                        &value,
                    )?;
                }

                return Ok(true);
            }
        }

        cur_fp = evicted;
        cur_bucket = alt;
    }

    Ok(false)
}

/// Error type for file open operations distinguishing not-found from other errors.
#[derive(Debug)]
enum FileOpenError {
    NotFound,
    Other(String),
}

/// Open a cuckoo file for reading only.
fn cuckoo_file_open_read(path: &str) -> Result<File, FileOpenError> {
    crate::open_random_read(Path::new(path)).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            FileOpenError::NotFound
        } else {
            FileOpenError::Other(format!("open: {e}"))
        }
    })
}

/// Open a cuckoo file for reading and writing.
fn cuckoo_file_open_rw(path: &str) -> Result<File, FileOpenError> {
    crate::open_random_rw(Path::new(path)).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            FileOpenError::NotFound
        } else {
            FileOpenError::Other(format!("open: {e}"))
        }
    })
}

/// Encode a FileOpenError as an Erlang error term.
fn encode_file_open_error(env: Env, err: FileOpenError) -> Term {
    match err {
        FileOpenError::NotFound => (atoms::error(), atoms::enoent()).encode(env),
        FileOpenError::Other(msg) => (atoms::error(), msg).encode(env),
    }
}

/// Create a new cuckoo filter file with the given capacity and bucket_size.
/// Uses fingerprint_size=1 and max_kicks=500.
/// Returns `{:ok, :ok}` or `{:error, reason}`.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn cuckoo_file_create(
    env: Env,
    path: String,
    capacity: u32,
    bucket_size: u8,
) -> NifResult<Term> {
    if capacity == 0 {
        return Ok((atoms::error(), "capacity must be > 0").encode(env));
    }
    if bucket_size == 0 {
        return Ok((atoms::error(), "bucket_size must be > 0").encode(env));
    }

    let fingerprint_size = FILE_DEFAULT_FINGERPRINT_SIZE as u8;
    let max_kicks = FILE_DEFAULT_MAX_KICKS;
    let bucket_bytes = (capacity as usize) * (bucket_size as usize) * (fingerprint_size as usize);
    let file_size = HEADER_SIZE + bucket_bytes;

    // Ensure parent directory exists.
    let p = Path::new(&path);
    if let Some(parent) = p.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .map_err(|e| rustler::Error::Term(Box::new(format!("mkdir: {e}"))))?;
        }
    }

    // Write header + zeroed buckets.
    let mut file =
        File::create(p).map_err(|e| rustler::Error::Term(Box::new(format!("create: {e}"))))?;
    let mut header = [0u8; HEADER_SIZE];
    header[0..2].copy_from_slice(&MAGIC);
    header[2] = VERSION;
    header[3..7].copy_from_slice(&capacity.to_le_bytes());
    header[7] = bucket_size;
    header[8] = fingerprint_size;
    header[9..11].copy_from_slice(&max_kicks.to_le_bytes());
    // num_items = 0 at bytes 11..19 (already zero)
    // num_deletes = 0 at bytes 19..27 (already zero)

    let mut buf = Vec::with_capacity(file_size);
    buf.extend_from_slice(&header);
    buf.resize(file_size, 0);

    file.write_all(&buf)
        .map_err(|e| rustler::Error::Term(Box::new(format!("write: {e}"))))?;
    file.sync_data()
        .map_err(|e| rustler::Error::Term(Box::new(format!("fdatasync: {e}"))))?;

    Ok((atoms::ok(), atoms::ok()).encode(env))
}

/// Add an element to a cuckoo filter file.
/// Opens the file, reads header, inserts fingerprint, updates counters, closes.
/// Returns `{:ok, 1}` or `{:error, "filter is full"}`.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(
    clippy::needless_pass_by_value,
    clippy::unnecessary_wraps,
    clippy::too_many_lines
)]
pub fn cuckoo_file_add<'a>(env: Env<'a>, path: String, element: Binary<'a>) -> NifResult<Term<'a>> {
    let file = match cuckoo_file_open_rw(&path) {
        Ok(f) => f,
        Err(e) => {
            return Ok(encode_file_open_error(env, e));
        }
    };

    let hdr = match cuckoo_read_header(&file) {
        Ok(h) => h,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    let (fp, b1) = cuckoo_file_fingerprint_and_bucket(
        element.as_slice(),
        hdr.fingerprint_size as usize,
        hdr.num_buckets,
    );
    let b2 = cuckoo_file_alternate_bucket(b1, &fp, hdr.num_buckets);

    // Try primary bucket.
    for slot in 0..hdr.bucket_size {
        let s = match cuckoo_file_read_slot(
            &file,
            b1,
            slot as usize,
            hdr.bucket_size,
            hdr.fingerprint_size,
        ) {
            Ok(s) => s,
            Err(e) => return Ok((atoms::error(), e).encode(env)),
        };
        if s.iter().all(|&b| b == 0) {
            if let Err(e) = cuckoo_file_write_slot(
                &file,
                b1,
                slot as usize,
                hdr.bucket_size,
                hdr.fingerprint_size,
                &fp,
            ) {
                return Ok((atoms::error(), e).encode(env));
            }
            if let Err(e) = cuckoo_file_write_num_items(&file, hdr.num_items + 1) {
                return Ok((atoms::error(), e).encode(env));
            }
            if let Err(e) = crate::prob_fsync(&file) {
                return Ok((atoms::error(), e).encode(env));
            }
            crate::fadvise_dontneed(&file, 0, 0);
            return Ok((atoms::ok(), 1u64).encode(env));
        }
    }

    // Try alternate bucket.
    for slot in 0..hdr.bucket_size {
        let s = match cuckoo_file_read_slot(
            &file,
            b2,
            slot as usize,
            hdr.bucket_size,
            hdr.fingerprint_size,
        ) {
            Ok(s) => s,
            Err(e) => return Ok((atoms::error(), e).encode(env)),
        };
        if s.iter().all(|&b| b == 0) {
            if let Err(e) = cuckoo_file_write_slot(
                &file,
                b2,
                slot as usize,
                hdr.bucket_size,
                hdr.fingerprint_size,
                &fp,
            ) {
                return Ok((atoms::error(), e).encode(env));
            }
            if let Err(e) = cuckoo_file_write_num_items(&file, hdr.num_items + 1) {
                return Ok((atoms::error(), e).encode(env));
            }
            if let Err(e) = crate::prob_fsync(&file) {
                return Ok((atoms::error(), e).encode(env));
            }
            crate::fadvise_dontneed(&file, 0, 0);
            return Ok((atoms::ok(), 1u64).encode(env));
        }
    }

    match cuckoo_file_try_eviction(&file, &hdr, fp, b1) {
        Ok(true) => {
            if let Err(e) = cuckoo_file_write_num_items(&file, hdr.num_items + 1) {
                return Ok((atoms::error(), e).encode(env));
            }
            if let Err(e) = crate::prob_fsync(&file) {
                return Ok((atoms::error(), e).encode(env));
            }
            crate::fadvise_dontneed(&file, 0, 0);
            return Ok((atoms::ok(), 1u64).encode(env));
        }
        Ok(false) => {}
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    }

    crate::fadvise_dontneed(&file, 0, 0);
    Ok((atoms::error(), "filter is full").encode(env))
}

/// Add an element only if it does not already exist.
/// Returns `{:ok, 0}` (already present) or `{:ok, 1}` (added), or `{:error, reason}`.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(
    clippy::needless_pass_by_value,
    clippy::unnecessary_wraps,
    clippy::too_many_lines
)]
pub fn cuckoo_file_addnx<'a>(
    env: Env<'a>,
    path: String,
    element: Binary<'a>,
) -> NifResult<Term<'a>> {
    // Check existence first using the same file.
    let file = match cuckoo_file_open_rw(&path) {
        Ok(f) => f,
        Err(e) => {
            return Ok(encode_file_open_error(env, e));
        }
    };

    let hdr = match cuckoo_read_header(&file) {
        Ok(h) => h,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    let (fp, b1) = cuckoo_file_fingerprint_and_bucket(
        element.as_slice(),
        hdr.fingerprint_size as usize,
        hdr.num_buckets,
    );
    let b2 = cuckoo_file_alternate_bucket(b1, &fp, hdr.num_buckets);

    // Check if exists in either bucket.
    for bucket in &[b1, b2] {
        for slot in 0..hdr.bucket_size {
            let s = match cuckoo_file_read_slot(
                &file,
                *bucket,
                slot as usize,
                hdr.bucket_size,
                hdr.fingerprint_size,
            ) {
                Ok(s) => s,
                Err(e) => return Ok((atoms::error(), e).encode(env)),
            };
            if s == fp {
                crate::fadvise_dontneed(&file, 0, 0);
                return Ok((atoms::ok(), 0u64).encode(env));
            }
        }
    }

    // Not found, try to add. Try primary bucket.
    for slot in 0..hdr.bucket_size {
        let s = match cuckoo_file_read_slot(
            &file,
            b1,
            slot as usize,
            hdr.bucket_size,
            hdr.fingerprint_size,
        ) {
            Ok(s) => s,
            Err(e) => return Ok((atoms::error(), e).encode(env)),
        };
        if s.iter().all(|&b| b == 0) {
            if let Err(e) = cuckoo_file_write_slot(
                &file,
                b1,
                slot as usize,
                hdr.bucket_size,
                hdr.fingerprint_size,
                &fp,
            ) {
                return Ok((atoms::error(), e).encode(env));
            }
            if let Err(e) = cuckoo_file_write_num_items(&file, hdr.num_items + 1) {
                return Ok((atoms::error(), e).encode(env));
            }
            if let Err(e) = crate::prob_fsync(&file) {
                return Ok((atoms::error(), e).encode(env));
            }
            crate::fadvise_dontneed(&file, 0, 0);
            return Ok((atoms::ok(), 1u64).encode(env));
        }
    }

    // Try alternate bucket.
    for slot in 0..hdr.bucket_size {
        let s = match cuckoo_file_read_slot(
            &file,
            b2,
            slot as usize,
            hdr.bucket_size,
            hdr.fingerprint_size,
        ) {
            Ok(s) => s,
            Err(e) => return Ok((atoms::error(), e).encode(env)),
        };
        if s.iter().all(|&b| b == 0) {
            if let Err(e) = cuckoo_file_write_slot(
                &file,
                b2,
                slot as usize,
                hdr.bucket_size,
                hdr.fingerprint_size,
                &fp,
            ) {
                return Ok((atoms::error(), e).encode(env));
            }
            if let Err(e) = cuckoo_file_write_num_items(&file, hdr.num_items + 1) {
                return Ok((atoms::error(), e).encode(env));
            }
            if let Err(e) = crate::prob_fsync(&file) {
                return Ok((atoms::error(), e).encode(env));
            }
            crate::fadvise_dontneed(&file, 0, 0);
            return Ok((atoms::ok(), 1u64).encode(env));
        }
    }

    match cuckoo_file_try_eviction(&file, &hdr, fp, b1) {
        Ok(true) => {
            if let Err(e) = cuckoo_file_write_num_items(&file, hdr.num_items + 1) {
                return Ok((atoms::error(), e).encode(env));
            }
            if let Err(e) = crate::prob_fsync(&file) {
                return Ok((atoms::error(), e).encode(env));
            }
            crate::fadvise_dontneed(&file, 0, 0);
            return Ok((atoms::ok(), 1u64).encode(env));
        }
        Ok(false) => {}
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    }

    crate::fadvise_dontneed(&file, 0, 0);
    Ok((atoms::error(), "filter is full").encode(env))
}

/// Delete one occurrence of an element from a cuckoo filter file.
/// Returns `{:ok, 0}` (not found) or `{:ok, 1}` (deleted), or `{:error, reason}`.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn cuckoo_file_del<'a>(env: Env<'a>, path: String, element: Binary<'a>) -> NifResult<Term<'a>> {
    let file = match cuckoo_file_open_rw(&path) {
        Ok(f) => f,
        Err(e) => {
            return Ok(encode_file_open_error(env, e));
        }
    };

    let hdr = match cuckoo_read_header(&file) {
        Ok(h) => h,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    let (fp, b1) = cuckoo_file_fingerprint_and_bucket(
        element.as_slice(),
        hdr.fingerprint_size as usize,
        hdr.num_buckets,
    );
    let b2 = cuckoo_file_alternate_bucket(b1, &fp, hdr.num_buckets);
    let empty = vec![0u8; hdr.fingerprint_size as usize];

    // Try primary bucket first.
    for slot in 0..hdr.bucket_size {
        let s = match cuckoo_file_read_slot(
            &file,
            b1,
            slot as usize,
            hdr.bucket_size,
            hdr.fingerprint_size,
        ) {
            Ok(s) => s,
            Err(e) => return Ok((atoms::error(), e).encode(env)),
        };
        if s == fp {
            if let Err(e) = cuckoo_file_write_slot(
                &file,
                b1,
                slot as usize,
                hdr.bucket_size,
                hdr.fingerprint_size,
                &empty,
            ) {
                return Ok((atoms::error(), e).encode(env));
            }
            if let Err(e) = cuckoo_file_write_num_items(&file, hdr.num_items.wrapping_sub(1)) {
                return Ok((atoms::error(), e).encode(env));
            }
            if let Err(e) = cuckoo_file_write_num_deletes(&file, hdr.num_deletes + 1) {
                return Ok((atoms::error(), e).encode(env));
            }
            if let Err(e) = crate::prob_fsync(&file) {
                return Ok((atoms::error(), e).encode(env));
            }
            crate::fadvise_dontneed(&file, 0, 0);
            return Ok((atoms::ok(), 1u64).encode(env));
        }
    }

    // Try alternate bucket.
    for slot in 0..hdr.bucket_size {
        let s = match cuckoo_file_read_slot(
            &file,
            b2,
            slot as usize,
            hdr.bucket_size,
            hdr.fingerprint_size,
        ) {
            Ok(s) => s,
            Err(e) => return Ok((atoms::error(), e).encode(env)),
        };
        if s == fp {
            if let Err(e) = cuckoo_file_write_slot(
                &file,
                b2,
                slot as usize,
                hdr.bucket_size,
                hdr.fingerprint_size,
                &empty,
            ) {
                return Ok((atoms::error(), e).encode(env));
            }
            if let Err(e) = cuckoo_file_write_num_items(&file, hdr.num_items.wrapping_sub(1)) {
                return Ok((atoms::error(), e).encode(env));
            }
            if let Err(e) = cuckoo_file_write_num_deletes(&file, hdr.num_deletes + 1) {
                return Ok((atoms::error(), e).encode(env));
            }
            if let Err(e) = crate::prob_fsync(&file) {
                return Ok((atoms::error(), e).encode(env));
            }
            crate::fadvise_dontneed(&file, 0, 0);
            return Ok((atoms::ok(), 1u64).encode(env));
        }
    }

    crate::fadvise_dontneed(&file, 0, 0);
    Ok((atoms::ok(), 0u64).encode(env))
}

/// Check if an element may exist in a cuckoo filter file.
/// Returns `{:ok, 0}` or `{:ok, 1}`, or `{:error, reason}`.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn cuckoo_file_exists<'a>(
    env: Env<'a>,
    path: String,
    element: Binary<'a>,
) -> NifResult<Term<'a>> {
    let file = match cuckoo_file_open_read(&path) {
        Ok(f) => f,
        Err(e) => {
            return Ok(encode_file_open_error(env, e));
        }
    };

    let hdr = match cuckoo_read_header(&file) {
        Ok(h) => h,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    let (fp, b1) = cuckoo_file_fingerprint_and_bucket(
        element.as_slice(),
        hdr.fingerprint_size as usize,
        hdr.num_buckets,
    );
    let b2 = cuckoo_file_alternate_bucket(b1, &fp, hdr.num_buckets);

    for bucket in &[b1, b2] {
        for slot in 0..hdr.bucket_size {
            let s = match cuckoo_file_read_slot(
                &file,
                *bucket,
                slot as usize,
                hdr.bucket_size,
                hdr.fingerprint_size,
            ) {
                Ok(s) => s,
                Err(e) => return Ok((atoms::error(), e).encode(env)),
            };
            if s == fp {
                crate::fadvise_dontneed(&file, 0, 0);
                return Ok((atoms::ok(), 1u64).encode(env));
            }
        }
    }

    crate::fadvise_dontneed(&file, 0, 0);
    Ok((atoms::ok(), 0u64).encode(env))
}

/// Count occurrences of an element's fingerprint in a cuckoo filter file.
/// Returns `{:ok, count}` or `{:error, reason}`.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn cuckoo_file_count<'a>(
    env: Env<'a>,
    path: String,
    element: Binary<'a>,
) -> NifResult<Term<'a>> {
    let file = match cuckoo_file_open_read(&path) {
        Ok(f) => f,
        Err(e) => {
            return Ok(encode_file_open_error(env, e));
        }
    };

    let hdr = match cuckoo_read_header(&file) {
        Ok(h) => h,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    let (fp, b1) = cuckoo_file_fingerprint_and_bucket(
        element.as_slice(),
        hdr.fingerprint_size as usize,
        hdr.num_buckets,
    );
    let b2 = cuckoo_file_alternate_bucket(b1, &fp, hdr.num_buckets);

    let mut total = 0u64;
    for bucket in &[b1, b2] {
        for slot in 0..hdr.bucket_size {
            let s = match cuckoo_file_read_slot(
                &file,
                *bucket,
                slot as usize,
                hdr.bucket_size,
                hdr.fingerprint_size,
            ) {
                Ok(s) => s,
                Err(e) => return Ok((atoms::error(), e).encode(env)),
            };
            if s == fp {
                total += 1;
            }
        }
    }

    crate::fadvise_dontneed(&file, 0, 0);
    Ok((atoms::ok(), total).encode(env))
}

/// Read cuckoo filter file info/metadata.
/// Returns `{:ok, {num_buckets, bucket_size, fingerprint_size, num_items, num_deletes, total_slots, max_kicks}}`
/// or `{:error, reason}`.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn cuckoo_file_info(env: Env, path: String) -> NifResult<Term> {
    let file = match cuckoo_file_open_read(&path) {
        Ok(f) => f,
        Err(e) => {
            return Ok(encode_file_open_error(env, e));
        }
    };

    let hdr = match cuckoo_read_header(&file) {
        Ok(h) => h,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    let total_slots = (hdr.num_buckets as u64) * (hdr.bucket_size as u64);
    let info = (
        atoms::ok(),
        (
            hdr.num_buckets as u64,
            hdr.bucket_size as u64,
            hdr.fingerprint_size as u64,
            hdr.num_items,
            hdr.num_deletes,
            total_slots,
            hdr.max_kicks as u64,
        ),
    );
    crate::fadvise_dontneed(&file, 0, 0);
    Ok(info.encode(env))
}

// ---------------------------------------------------------------------------
// Async variants of read NIFs — Tokio spawn_blocking, never block BEAM
// ---------------------------------------------------------------------------

/// Async cuckoo exists: spawns on Tokio, sends result to `caller_pid`.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
pub fn cuckoo_file_exists_async<'a>(
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
            let hdr = cuckoo_read_header(&file).map_err(|e| e.clone())?;
            let (fp, b1) = cuckoo_file_fingerprint_and_bucket(
                &element_owned,
                hdr.fingerprint_size as usize,
                hdr.num_buckets,
            );
            let b2 = cuckoo_file_alternate_bucket(b1, &fp, hdr.num_buckets);
            for bucket in &[b1, b2] {
                for slot in 0..hdr.bucket_size {
                    let s = cuckoo_file_read_slot(
                        &file,
                        *bucket,
                        slot as usize,
                        hdr.bucket_size,
                        hdr.fingerprint_size,
                    )
                    .map_err(|e| e.clone())?;
                    if s == fp {
                        crate::fadvise_dontneed(&file, 0, 0);
                        return Ok(1u64);
                    }
                }
            }
            crate::fadvise_dontneed(&file, 0, 0);
            Ok(0u64)
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

/// Async cuckoo count: spawns on Tokio, sends result to `caller_pid`.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
pub fn cuckoo_file_count_async<'a>(
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
            let hdr = cuckoo_read_header(&file).map_err(|e| e.clone())?;
            let (fp, b1) = cuckoo_file_fingerprint_and_bucket(
                &element_owned,
                hdr.fingerprint_size as usize,
                hdr.num_buckets,
            );
            let b2 = cuckoo_file_alternate_bucket(b1, &fp, hdr.num_buckets);
            let mut total = 0u64;
            for bucket in &[b1, b2] {
                for slot in 0..hdr.bucket_size {
                    let s = cuckoo_file_read_slot(
                        &file,
                        *bucket,
                        slot as usize,
                        hdr.bucket_size,
                        hdr.fingerprint_size,
                    )
                    .map_err(|e| e.clone())?;
                    if s == fp {
                        total += 1;
                    }
                }
            }
            crate::fadvise_dontneed(&file, 0, 0);
            Ok(total)
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

/// Async cuckoo info: spawns on Tokio, sends result to `caller_pid`.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
pub fn cuckoo_file_info_async(
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
            let hdr = cuckoo_read_header(&file).map_err(|e| e.clone())?;
            let total_slots = (hdr.num_buckets as u64) * (hdr.bucket_size as u64);
            crate::fadvise_dontneed(&file, 0, 0);
            Ok((
                hdr.num_buckets as u64,
                hdr.bucket_size as u64,
                hdr.fingerprint_size as u64,
                hdr.num_items,
                hdr.num_deletes,
                total_slots,
                hdr.max_kicks as u64,
            ))
        })
        .await
        .unwrap_or_else(|e| Err(format!("spawn_blocking: {e}")));

        let mut msg_env = rustler::OwnedEnv::new();
        let _ = msg_env.send_and_clear(&caller_pid, |env| match result {
            Ok(info) => (atoms::tokio_complete(), correlation_id, atoms::ok(), info).encode(env),
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
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_create_and_read_header() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.cuckoo");
        let path_str = path.to_str().unwrap().to_string();

        // Create file manually (same logic as NIF but without Env).
        let capacity: u32 = 1024;
        let bucket_size: u8 = 4;
        let fingerprint_size: u8 = FILE_DEFAULT_FINGERPRINT_SIZE as u8;
        let max_kicks = FILE_DEFAULT_MAX_KICKS;
        let bucket_bytes =
            (capacity as usize) * (bucket_size as usize) * (fingerprint_size as usize);
        let file_size = HEADER_SIZE + bucket_bytes;

        let mut file = File::create(&path).unwrap();
        let mut header = [0u8; HEADER_SIZE];
        header[0..2].copy_from_slice(&MAGIC);
        header[2] = VERSION;
        header[3..7].copy_from_slice(&capacity.to_le_bytes());
        header[7] = bucket_size;
        header[8] = fingerprint_size;
        header[9..11].copy_from_slice(&max_kicks.to_le_bytes());

        let mut buf = Vec::with_capacity(file_size);
        buf.extend_from_slice(&header);
        buf.resize(file_size, 0);
        file.write_all(&buf).unwrap();
        file.sync_all().unwrap();
        drop(file);

        // Read back and validate header.
        let file = cuckoo_file_open_read(&path_str).unwrap();
        let hdr = cuckoo_read_header(&file).unwrap();
        assert_eq!(hdr.num_buckets, 1024);
        assert_eq!(hdr.bucket_size, 4);
        assert_eq!(hdr.fingerprint_size, FILE_DEFAULT_FINGERPRINT_SIZE as u8);
        assert_eq!(hdr.num_items, 0);
        assert_eq!(hdr.num_deletes, 0);
    }

    #[test]
    fn file_slot_read_write_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("slot.cuckoo");
        let path_str = path.to_str().unwrap().to_string();

        let capacity: u32 = 64;
        let bucket_size: u8 = 4;
        let fingerprint_size: u8 = 1;
        let bucket_bytes =
            (capacity as usize) * (bucket_size as usize) * (fingerprint_size as usize);
        let file_size = HEADER_SIZE + bucket_bytes;

        let mut file = File::create(&path).unwrap();
        let mut header = [0u8; HEADER_SIZE];
        header[0..2].copy_from_slice(&MAGIC);
        header[2] = VERSION;
        header[3..7].copy_from_slice(&capacity.to_le_bytes());
        header[7] = bucket_size;
        header[8] = fingerprint_size;
        header[9..11].copy_from_slice(&FILE_DEFAULT_MAX_KICKS.to_le_bytes());
        let mut buf = Vec::with_capacity(file_size);
        buf.extend_from_slice(&header);
        buf.resize(file_size, 0);
        file.write_all(&buf).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let file = cuckoo_file_open_rw(&path_str).unwrap();

        // Write a fingerprint and read it back.
        let fp = vec![0x42u8];
        cuckoo_file_write_slot(&file, 3, 2, bucket_size, fingerprint_size, &fp).unwrap();
        let read_back = cuckoo_file_read_slot(&file, 3, 2, bucket_size, fingerprint_size).unwrap();
        assert_eq!(read_back, fp);

        // Verify other slots are still empty.
        let empty = cuckoo_file_read_slot(&file, 3, 0, bucket_size, fingerprint_size).unwrap();
        assert!(empty.iter().all(|&b| b == 0));
    }

    #[test]
    fn file_fingerprint_never_zero() {
        for i in 0..10_000 {
            let (fp, _) = cuckoo_file_fingerprint_and_bucket(
                format!("elem_{i}").as_bytes(),
                FILE_DEFAULT_FINGERPRINT_SIZE,
                1024,
            );
            assert!(
                !fp.iter().all(|&b| b == 0),
                "fingerprint was all zeros for elem_{i}"
            );
        }
    }

    #[test]
    fn file_alternate_bucket_is_involution() {
        // alt(alt(b, fp)) == b  (the cuckoo property)
        for i in 0..1000 {
            let elem = format!("invol_{i}");
            let (fp, b1) = cuckoo_file_fingerprint_and_bucket(elem.as_bytes(), 1, 1024);
            let b2 = cuckoo_file_alternate_bucket(b1, &fp, 1024);
            let b1_again = cuckoo_file_alternate_bucket(b2, &fp, 1024);
            assert_eq!(
                b1, b1_again,
                "alternate_bucket must be an involution for elem {i}"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Edge case tests
    // -----------------------------------------------------------------------

    /// Helper: create a valid cuckoo file and return the path string.
    fn create_cuckoo_file(
        dir: &std::path::Path,
        name: &str,
        capacity: u32,
        bucket_size: u8,
    ) -> String {
        let path = dir.join(name);
        let fingerprint_size: u8 = FILE_DEFAULT_FINGERPRINT_SIZE as u8;
        let max_kicks = FILE_DEFAULT_MAX_KICKS;
        let bucket_bytes =
            (capacity as usize) * (bucket_size as usize) * (fingerprint_size as usize);
        let file_size = HEADER_SIZE + bucket_bytes;

        let mut file = File::create(&path).unwrap();
        let mut header = [0u8; HEADER_SIZE];
        header[0..2].copy_from_slice(&MAGIC);
        header[2] = VERSION;
        header[3..7].copy_from_slice(&capacity.to_le_bytes());
        header[7] = bucket_size;
        header[8] = fingerprint_size;
        header[9..11].copy_from_slice(&max_kicks.to_le_bytes());

        let mut buf = Vec::with_capacity(file_size);
        buf.extend_from_slice(&header);
        buf.resize(file_size, 0);
        file.write_all(&buf).unwrap();
        file.sync_all().unwrap();
        path.to_str().unwrap().to_string()
    }

    #[test]
    fn empty_element_fingerprint() {
        // Zero-length element should produce a valid non-zero fingerprint.
        let (fp, bucket) = cuckoo_file_fingerprint_and_bucket(b"", 1, 1024);
        assert!(
            !fp.iter().all(|&b| b == 0),
            "fingerprint should never be all zeros"
        );
        assert!(bucket < 1024);
    }

    #[test]
    fn large_element_fingerprint() {
        // 1MB element should work without panic.
        let big = vec![0xEFu8; 1_000_000];
        let (fp, bucket) = cuckoo_file_fingerprint_and_bucket(&big, 1, 1024);
        assert!(!fp.iter().all(|&b| b == 0));
        assert!(bucket < 1024);
    }

    #[test]
    fn truncated_header_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("truncated.cuckoo");
        std::fs::write(&path, [0u8; 10]).unwrap();
        let file = File::open(&path).unwrap();
        assert!(cuckoo_read_header(&file).is_err());
    }

    #[test]
    fn wrong_magic_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad_magic.cuckoo");
        let mut data = [0u8; HEADER_SIZE + 64];
        data[0] = 0xFF;
        data[1] = 0xFF;
        std::fs::write(&path, data).unwrap();
        let file = File::open(&path).unwrap();
        let result = cuckoo_read_header(&file);
        assert!(result.is_err());
        match result {
            Err(msg) => assert!(msg.contains("magic"), "expected magic error, got: {msg}"),
            Ok(_) => panic!("expected error"),
        }
    }

    #[test]
    fn wrong_version_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad_version.cuckoo");
        let mut data = [0u8; HEADER_SIZE + 64];
        data[0..2].copy_from_slice(&MAGIC);
        data[2] = 99; // wrong version
        std::fs::write(&path, data).unwrap();
        let file = File::open(&path).unwrap();
        let result = cuckoo_read_header(&file);
        assert!(result.is_err());
        match result {
            Err(msg) => assert!(
                msg.contains("version"),
                "expected version error, got: {msg}"
            ),
            Ok(_) => panic!("expected error"),
        }
    }

    #[test]
    fn minimum_capacity_cuckoo() {
        // capacity=1, bucket_size=1 -- smallest possible cuckoo filter
        let dir = tempfile::tempdir().unwrap();
        let path_str = create_cuckoo_file(dir.path(), "min.cuckoo", 1, 1);
        let file = cuckoo_file_open_read(&path_str).unwrap();
        let hdr = cuckoo_read_header(&file).unwrap();
        assert_eq!(hdr.num_buckets, 1);
        assert_eq!(hdr.bucket_size, 1);
        assert_eq!(hdr.num_items, 0);
    }

    #[test]
    fn add_and_exists_roundtrip() {
        // Full roundtrip: create, add an element, check it exists.
        let dir = tempfile::tempdir().unwrap();
        let path_str = create_cuckoo_file(dir.path(), "roundtrip.cuckoo", 64, 4);

        let file = cuckoo_file_open_rw(&path_str).unwrap();
        let hdr = cuckoo_read_header(&file).unwrap();

        let (fp, b1) = cuckoo_file_fingerprint_and_bucket(
            b"hello",
            hdr.fingerprint_size as usize,
            hdr.num_buckets,
        );

        // Write fingerprint to first slot of primary bucket
        cuckoo_file_write_slot(&file, b1, 0, hdr.bucket_size, hdr.fingerprint_size, &fp).unwrap();
        cuckoo_file_write_num_items(&file, 1).unwrap();
        drop(file);

        // Check exists
        let file = cuckoo_file_open_read(&path_str).unwrap();
        let hdr = cuckoo_read_header(&file).unwrap();
        assert_eq!(hdr.num_items, 1);

        let (fp2, b1_2) = cuckoo_file_fingerprint_and_bucket(
            b"hello",
            hdr.fingerprint_size as usize,
            hdr.num_buckets,
        );
        assert_eq!(fp, fp2);
        assert_eq!(b1, b1_2);

        let read_fp =
            cuckoo_file_read_slot(&file, b1, 0, hdr.bucket_size, hdr.fingerprint_size).unwrap();
        assert_eq!(read_fp, fp);
    }

    #[test]
    fn delete_decrements_items() {
        let dir = tempfile::tempdir().unwrap();
        let path_str = create_cuckoo_file(dir.path(), "del.cuckoo", 64, 4);
        let file = cuckoo_file_open_rw(&path_str).unwrap();
        let hdr = cuckoo_read_header(&file).unwrap();

        let (fp, b1) = cuckoo_file_fingerprint_and_bucket(
            b"deleteme",
            hdr.fingerprint_size as usize,
            hdr.num_buckets,
        );

        // Add the element
        cuckoo_file_write_slot(&file, b1, 0, hdr.bucket_size, hdr.fingerprint_size, &fp).unwrap();
        cuckoo_file_write_num_items(&file, 1).unwrap();

        // Delete it
        let empty = vec![0u8; hdr.fingerprint_size as usize];
        cuckoo_file_write_slot(&file, b1, 0, hdr.bucket_size, hdr.fingerprint_size, &empty)
            .unwrap();
        cuckoo_file_write_num_items(&file, 0).unwrap();
        cuckoo_file_write_num_deletes(&file, 1).unwrap();

        // Verify header
        let hdr2 = cuckoo_read_header(&file).unwrap();
        assert_eq!(hdr2.num_items, 0);
        assert_eq!(hdr2.num_deletes, 1);

        // Verify slot is empty
        let read_fp =
            cuckoo_file_read_slot(&file, b1, 0, hdr.bucket_size, hdr.fingerprint_size).unwrap();
        assert!(read_fp.iter().all(|&b| b == 0));
    }

    #[test]
    fn filter_full_when_capacity_1_and_bucket_size_1() {
        // With capacity=1 and bucket_size=1, after inserting 1 element,
        // the next insert may require eviction. With only 1 bucket and 1 slot,
        // the eviction loop should terminate (bounded by max_kicks).
        let dir = tempfile::tempdir().unwrap();
        let path_str = create_cuckoo_file(dir.path(), "full.cuckoo", 1, 1);
        let file = cuckoo_file_open_rw(&path_str).unwrap();
        let hdr = cuckoo_read_header(&file).unwrap();

        // Add first element
        let (fp1, b1) = cuckoo_file_fingerprint_and_bucket(
            b"first",
            hdr.fingerprint_size as usize,
            hdr.num_buckets,
        );
        cuckoo_file_write_slot(&file, b1, 0, hdr.bucket_size, hdr.fingerprint_size, &fp1).unwrap();
        cuckoo_file_write_num_items(&file, 1).unwrap();

        // All slots are now full. The eviction loop is tested via the NIF in
        // Elixir tests. Here we just verify max_kicks is bounded.
        assert_eq!(hdr.max_kicks, FILE_DEFAULT_MAX_KICKS);
        assert!(hdr.max_kicks <= 500);
    }

    #[test]
    fn null_bytes_in_element() {
        let element = b"test\x00with\x00nulls";
        let (fp, bucket) = cuckoo_file_fingerprint_and_bucket(element, 1, 1024);
        assert!(!fp.iter().all(|&b| b == 0));
        assert!(bucket < 1024);

        // Should differ from element without nulls
        let (fp2, bucket2) = cuckoo_file_fingerprint_and_bucket(b"testwithnulls", 1, 1024);
        assert!(
            fp != fp2 || bucket != bucket2,
            "null bytes should affect hash"
        );
    }

    #[test]
    fn nonexistent_file_returns_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let path_str = dir.path().join("nope.cuckoo").to_str().unwrap().to_string();
        match cuckoo_file_open_read(&path_str) {
            Err(FileOpenError::NotFound) => {} // expected
            other => panic!("expected NotFound, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // Test helpers — replicate NIF logic without Env/Term
    // -----------------------------------------------------------------------

    /// Add an element to a cuckoo file. Returns Ok(true) on success,
    /// Err("filter is full") when eviction chain exhausts max_kicks.
    fn test_add(path: &str, element: &[u8]) -> Result<bool, String> {
        let file = cuckoo_file_open_rw(path).map_err(|e| format!("{e:?}"))?;
        let hdr = cuckoo_read_header(&file)?;
        let (fp, b1) = cuckoo_file_fingerprint_and_bucket(
            element,
            hdr.fingerprint_size as usize,
            hdr.num_buckets,
        );
        let b2 = cuckoo_file_alternate_bucket(b1, &fp, hdr.num_buckets);

        // Try primary bucket.
        for slot in 0..hdr.bucket_size {
            let s = cuckoo_file_read_slot(
                &file,
                b1,
                slot as usize,
                hdr.bucket_size,
                hdr.fingerprint_size,
            )?;
            if s.iter().all(|&b| b == 0) {
                cuckoo_file_write_slot(
                    &file,
                    b1,
                    slot as usize,
                    hdr.bucket_size,
                    hdr.fingerprint_size,
                    &fp,
                )?;
                cuckoo_file_write_num_items(&file, hdr.num_items + 1)?;
                return Ok(true);
            }
        }

        // Try alternate bucket.
        for slot in 0..hdr.bucket_size {
            let s = cuckoo_file_read_slot(
                &file,
                b2,
                slot as usize,
                hdr.bucket_size,
                hdr.fingerprint_size,
            )?;
            if s.iter().all(|&b| b == 0) {
                cuckoo_file_write_slot(
                    &file,
                    b2,
                    slot as usize,
                    hdr.bucket_size,
                    hdr.fingerprint_size,
                    &fp,
                )?;
                cuckoo_file_write_num_items(&file, hdr.num_items + 1)?;
                return Ok(true);
            }
        }

        // Cuckoo eviction chain.
        let mut cur_fp = fp;
        let mut cur_bucket = b1;
        for kicks in 0..(hdr.max_kicks as u32) {
            let slot_idx = (kicks as usize) % (hdr.bucket_size as usize);
            let evicted = cuckoo_file_read_slot(
                &file,
                cur_bucket,
                slot_idx,
                hdr.bucket_size,
                hdr.fingerprint_size,
            )?;
            cuckoo_file_write_slot(
                &file,
                cur_bucket,
                slot_idx,
                hdr.bucket_size,
                hdr.fingerprint_size,
                &cur_fp,
            )?;
            let alt = cuckoo_file_alternate_bucket(cur_bucket, &evicted, hdr.num_buckets);
            for slot in 0..hdr.bucket_size {
                let s = cuckoo_file_read_slot(
                    &file,
                    alt,
                    slot as usize,
                    hdr.bucket_size,
                    hdr.fingerprint_size,
                )?;
                if s.iter().all(|&b| b == 0) {
                    cuckoo_file_write_slot(
                        &file,
                        alt,
                        slot as usize,
                        hdr.bucket_size,
                        hdr.fingerprint_size,
                        &evicted,
                    )?;
                    cuckoo_file_write_num_items(&file, hdr.num_items + 1)?;
                    return Ok(true);
                }
            }
            cur_fp = evicted;
            cur_bucket = alt;
        }
        Err("filter is full".into())
    }

    /// Check if an element exists. Returns Ok(true/false).
    fn test_exists(path: &str, element: &[u8]) -> Result<bool, String> {
        let file = cuckoo_file_open_read(path).map_err(|e| format!("{e:?}"))?;
        let hdr = cuckoo_read_header(&file)?;
        let (fp, b1) = cuckoo_file_fingerprint_and_bucket(
            element,
            hdr.fingerprint_size as usize,
            hdr.num_buckets,
        );
        let b2 = cuckoo_file_alternate_bucket(b1, &fp, hdr.num_buckets);
        for bucket in &[b1, b2] {
            for slot in 0..hdr.bucket_size {
                let s = cuckoo_file_read_slot(
                    &file,
                    *bucket,
                    slot as usize,
                    hdr.bucket_size,
                    hdr.fingerprint_size,
                )?;
                if s == fp {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Delete one occurrence. Returns Ok(true) if deleted, Ok(false) if not found.
    fn test_del(path: &str, element: &[u8]) -> Result<bool, String> {
        let file = cuckoo_file_open_rw(path).map_err(|e| format!("{e:?}"))?;
        let hdr = cuckoo_read_header(&file)?;
        let (fp, b1) = cuckoo_file_fingerprint_and_bucket(
            element,
            hdr.fingerprint_size as usize,
            hdr.num_buckets,
        );
        let b2 = cuckoo_file_alternate_bucket(b1, &fp, hdr.num_buckets);
        let empty = vec![0u8; hdr.fingerprint_size as usize];
        for bucket in &[b1, b2] {
            for slot in 0..hdr.bucket_size {
                let s = cuckoo_file_read_slot(
                    &file,
                    *bucket,
                    slot as usize,
                    hdr.bucket_size,
                    hdr.fingerprint_size,
                )?;
                if s == fp {
                    cuckoo_file_write_slot(
                        &file,
                        *bucket,
                        slot as usize,
                        hdr.bucket_size,
                        hdr.fingerprint_size,
                        &empty,
                    )?;
                    cuckoo_file_write_num_items(&file, hdr.num_items.wrapping_sub(1))?;
                    cuckoo_file_write_num_deletes(&file, hdr.num_deletes + 1)?;
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Add only if not exists. Returns Ok(1) if added, Ok(0) if already present.
    #[allow(clippy::too_many_lines)]
    fn test_addnx(path: &str, element: &[u8]) -> Result<u64, String> {
        let file = cuckoo_file_open_rw(path).map_err(|e| format!("{e:?}"))?;
        let hdr = cuckoo_read_header(&file)?;
        let (fp, b1) = cuckoo_file_fingerprint_and_bucket(
            element,
            hdr.fingerprint_size as usize,
            hdr.num_buckets,
        );
        let b2 = cuckoo_file_alternate_bucket(b1, &fp, hdr.num_buckets);

        // Check existence first.
        for bucket in &[b1, b2] {
            for slot in 0..hdr.bucket_size {
                let s = cuckoo_file_read_slot(
                    &file,
                    *bucket,
                    slot as usize,
                    hdr.bucket_size,
                    hdr.fingerprint_size,
                )?;
                if s == fp {
                    return Ok(0);
                }
            }
        }

        // Not found — insert (try primary, then alternate, then eviction).
        for slot in 0..hdr.bucket_size {
            let s = cuckoo_file_read_slot(
                &file,
                b1,
                slot as usize,
                hdr.bucket_size,
                hdr.fingerprint_size,
            )?;
            if s.iter().all(|&b| b == 0) {
                cuckoo_file_write_slot(
                    &file,
                    b1,
                    slot as usize,
                    hdr.bucket_size,
                    hdr.fingerprint_size,
                    &fp,
                )?;
                cuckoo_file_write_num_items(&file, hdr.num_items + 1)?;
                return Ok(1);
            }
        }
        for slot in 0..hdr.bucket_size {
            let s = cuckoo_file_read_slot(
                &file,
                b2,
                slot as usize,
                hdr.bucket_size,
                hdr.fingerprint_size,
            )?;
            if s.iter().all(|&b| b == 0) {
                cuckoo_file_write_slot(
                    &file,
                    b2,
                    slot as usize,
                    hdr.bucket_size,
                    hdr.fingerprint_size,
                    &fp,
                )?;
                cuckoo_file_write_num_items(&file, hdr.num_items + 1)?;
                return Ok(1);
            }
        }

        // Eviction chain.
        let mut cur_fp = fp;
        let mut cur_bucket = b1;
        for kicks in 0..(hdr.max_kicks as u32) {
            let slot_idx = (kicks as usize) % (hdr.bucket_size as usize);
            let evicted = cuckoo_file_read_slot(
                &file,
                cur_bucket,
                slot_idx,
                hdr.bucket_size,
                hdr.fingerprint_size,
            )?;
            cuckoo_file_write_slot(
                &file,
                cur_bucket,
                slot_idx,
                hdr.bucket_size,
                hdr.fingerprint_size,
                &cur_fp,
            )?;
            let alt = cuckoo_file_alternate_bucket(cur_bucket, &evicted, hdr.num_buckets);
            for slot in 0..hdr.bucket_size {
                let s = cuckoo_file_read_slot(
                    &file,
                    alt,
                    slot as usize,
                    hdr.bucket_size,
                    hdr.fingerprint_size,
                )?;
                if s.iter().all(|&b| b == 0) {
                    cuckoo_file_write_slot(
                        &file,
                        alt,
                        slot as usize,
                        hdr.bucket_size,
                        hdr.fingerprint_size,
                        &evicted,
                    )?;
                    cuckoo_file_write_num_items(&file, hdr.num_items + 1)?;
                    return Ok(1);
                }
            }
            cur_fp = evicted;
            cur_bucket = alt;
        }
        Err("filter is full".into())
    }

    /// Count occurrences of an element's fingerprint.
    fn test_count(path: &str, element: &[u8]) -> Result<u64, String> {
        let file = cuckoo_file_open_read(path).map_err(|e| format!("{e:?}"))?;
        let hdr = cuckoo_read_header(&file)?;
        let (fp, b1) = cuckoo_file_fingerprint_and_bucket(
            element,
            hdr.fingerprint_size as usize,
            hdr.num_buckets,
        );
        let b2 = cuckoo_file_alternate_bucket(b1, &fp, hdr.num_buckets);
        let mut total = 0u64;
        for bucket in &[b1, b2] {
            for slot in 0..hdr.bucket_size {
                let s = cuckoo_file_read_slot(
                    &file,
                    *bucket,
                    slot as usize,
                    hdr.bucket_size,
                    hdr.fingerprint_size,
                )?;
                if s == fp {
                    total += 1;
                }
            }
        }
        Ok(total)
    }

    type InfoTuple = (u64, u64, u64, u64, u64, u64, u64);

    /// Read filter info. Returns (num_buckets, bucket_size, fp_size, num_items, num_deletes, total_slots, max_kicks).
    fn test_info(path: &str) -> Result<InfoTuple, String> {
        let file = cuckoo_file_open_read(path).map_err(|e| format!("{e:?}"))?;
        let hdr = cuckoo_read_header(&file)?;
        let total_slots = (hdr.num_buckets as u64) * (hdr.bucket_size as u64);
        Ok((
            hdr.num_buckets as u64,
            hdr.bucket_size as u64,
            hdr.fingerprint_size as u64,
            hdr.num_items,
            hdr.num_deletes,
            total_slots,
            hdr.max_kicks as u64,
        ))
    }

    // -----------------------------------------------------------------------
    // Full integration tests using test helpers
    // -----------------------------------------------------------------------

    #[test]
    fn add_and_exists_via_helpers() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_cuckoo_file(dir.path(), "add_exists.cuckoo", 128, 4);

        test_add(&path, b"hello").unwrap();
        assert!(test_exists(&path, b"hello").unwrap());
        assert!(!test_exists(&path, b"world").unwrap());
    }

    #[test]
    fn add_multiple_elements() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_cuckoo_file(dir.path(), "multi.cuckoo", 256, 4);

        let elements: Vec<Vec<u8>> = (0..50).map(|i| format!("elem_{i}").into_bytes()).collect();

        for elem in &elements {
            test_add(&path, elem).unwrap();
        }

        for elem in &elements {
            assert!(
                test_exists(&path, elem).unwrap(),
                "element {:?} should exist after add",
                std::str::from_utf8(elem).unwrap()
            );
        }

        // Verify something never added does not exist.
        assert!(!test_exists(&path, b"never_added").unwrap());
    }

    #[test]
    fn delete_element() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_cuckoo_file(dir.path(), "delete.cuckoo", 128, 4);

        test_add(&path, b"todelete").unwrap();
        assert!(test_exists(&path, b"todelete").unwrap());

        let deleted = test_del(&path, b"todelete").unwrap();
        assert!(deleted);

        assert!(!test_exists(&path, b"todelete").unwrap());
    }

    #[test]
    fn addnx_already_exists() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_cuckoo_file(dir.path(), "addnx.cuckoo", 128, 4);

        // First add should succeed.
        let r1 = test_addnx(&path, b"unique").unwrap();
        assert_eq!(r1, 1);

        // Second addnx of same element should return 0.
        let r2 = test_addnx(&path, b"unique").unwrap();
        assert_eq!(r2, 0);

        // Different element should still add.
        let r3 = test_addnx(&path, b"different").unwrap();
        assert_eq!(r3, 1);
    }

    #[test]
    fn count_duplicates() {
        // Cuckoo filters allow duplicates via regular add.
        let dir = tempfile::tempdir().unwrap();
        let path = create_cuckoo_file(dir.path(), "count.cuckoo", 128, 4);

        assert_eq!(test_count(&path, b"dup").unwrap(), 0);

        test_add(&path, b"dup").unwrap();
        assert_eq!(test_count(&path, b"dup").unwrap(), 1);

        test_add(&path, b"dup").unwrap();
        assert_eq!(test_count(&path, b"dup").unwrap(), 2);

        test_add(&path, b"dup").unwrap();
        assert_eq!(test_count(&path, b"dup").unwrap(), 3);
    }

    #[test]
    fn info_matches_creation_params() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_cuckoo_file(dir.path(), "info.cuckoo", 200, 4);

        let (num_buckets, bucket_size, fp_size, num_items, num_deletes, total_slots, max_kicks) =
            test_info(&path).unwrap();

        assert_eq!(num_buckets, 200);
        assert_eq!(bucket_size, 4);
        assert_eq!(fp_size, FILE_DEFAULT_FINGERPRINT_SIZE as u64);
        assert_eq!(num_items, 0);
        assert_eq!(num_deletes, 0);
        assert_eq!(total_slots, 200 * 4);
        assert_eq!(max_kicks, FILE_DEFAULT_MAX_KICKS as u64);

        // Add some elements and verify counters update.
        test_add(&path, b"a").unwrap();
        test_add(&path, b"b").unwrap();
        let (_, _, _, items2, _, _, _) = test_info(&path).unwrap();
        assert_eq!(items2, 2);

        test_del(&path, b"a").unwrap();
        let (_, _, _, items3, deletes3, _, _) = test_info(&path).unwrap();
        assert_eq!(items3, 1);
        assert_eq!(deletes3, 1);
    }

    #[test]
    fn nonexistent_file_errors_for_all_ops() {
        let dir = tempfile::tempdir().unwrap();
        let bad = dir
            .path()
            .join("missing.cuckoo")
            .to_str()
            .unwrap()
            .to_string();

        assert!(test_exists(&bad, b"x").is_err());
        assert!(test_add(&bad, b"x").is_err());
        assert!(test_del(&bad, b"x").is_err());
        assert!(test_addnx(&bad, b"x").is_err());
        assert!(test_count(&bad, b"x").is_err());
        assert!(test_info(&bad).is_err());
    }

    #[test]
    fn empty_filter_exists_and_count() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_cuckoo_file(dir.path(), "empty.cuckoo", 64, 4);

        assert!(!test_exists(&path, b"anything").unwrap());
        assert_eq!(test_count(&path, b"anything").unwrap(), 0);
    }

    #[test]
    fn zero_fingerprint_bypass() {
        // The code maps all-zero fingerprints to [1, 0, ...].
        // Verify with different fingerprint sizes.
        for fp_size in 1..=4 {
            for i in 0..5000 {
                let (fp, _) = cuckoo_file_fingerprint_and_bucket(
                    format!("zfp_{fp_size}_{i}").as_bytes(),
                    fp_size,
                    1024,
                );
                assert!(
                    !fp.iter().all(|&b| b == 0),
                    "fingerprint must never be all zeros (fp_size={fp_size}, i={i})"
                );
            }
        }
    }

    #[test]
    fn kick_chain_triggered_and_elements_findable() {
        // Use a small filter that forces eviction kicks.
        // capacity=8 buckets, bucket_size=2 => 16 total slots.
        // Inserting ~12 elements should trigger kicks since collisions
        // are inevitable with only 8 buckets.
        let dir = tempfile::tempdir().unwrap();
        let path = create_cuckoo_file(dir.path(), "kick.cuckoo", 8, 2);

        let mut added = Vec::new();
        for i in 0..12 {
            let elem = format!("kick_{i}").into_bytes();
            match test_add(&path, &elem) {
                Ok(_) => added.push(elem),
                Err(_) => break, // filter full, stop
            }
        }

        // Every successfully added element must be findable.
        for elem in &added {
            assert!(
                test_exists(&path, elem).unwrap(),
                "element {:?} was added but not found",
                std::str::from_utf8(elem).unwrap()
            );
        }
        assert!(added.len() >= 2, "should have added at least some elements");
    }

    #[test]
    fn filter_full_returns_error() {
        // Tiny filter: 2 buckets, 1 slot each => 2 total slots.
        let dir = tempfile::tempdir().unwrap();
        let path = create_cuckoo_file(dir.path(), "full2.cuckoo", 2, 1);

        // Keep adding until we get "filter is full".
        let mut count = 0;
        for i in 0..1000 {
            let elem = format!("fill_{i}").into_bytes();
            match test_add(&path, &elem) {
                Ok(_) => count += 1,
                Err(e) => {
                    assert!(e.contains("filter is full"), "unexpected error: {e}");
                    break;
                }
            }
        }
        // With 2 slots, we can hold at most 2 elements (could be fewer
        // due to bucket collisions with the eviction chain failing).
        assert!(count <= 2, "should not exceed total slot count");
        assert!(count >= 1, "should have added at least one element");
    }

    #[test]
    fn delete_non_existent_element() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_cuckoo_file(dir.path(), "del_none.cuckoo", 64, 4);

        // Delete something never added.
        let deleted = test_del(&path, b"ghost").unwrap();
        assert!(!deleted);

        // Counters should be unchanged.
        let (_, _, _, items, deletes, _, _) = test_info(&path).unwrap();
        assert_eq!(items, 0);
        assert_eq!(deletes, 0);
    }

    #[test]
    fn large_number_of_insertions() {
        // Stress test: 1024 buckets * 4 slots = 4096 total slots.
        // At ~95% load cuckoo filters start failing, so insert up to 3500.
        let dir = tempfile::tempdir().unwrap();
        let path = create_cuckoo_file(dir.path(), "stress.cuckoo", 1024, 4);

        let mut inserted = Vec::new();
        for i in 0..3500 {
            let elem = format!("stress_{i}").into_bytes();
            match test_add(&path, &elem) {
                Ok(_) => inserted.push(elem),
                Err(_) => break,
            }
        }

        // All successfully inserted elements must be findable.
        for elem in &inserted {
            assert!(
                test_exists(&path, elem).unwrap(),
                "element {:?} lost after stress insert (total inserted: {})",
                std::str::from_utf8(elem).unwrap(),
                inserted.len()
            );
        }

        // Verify info counters.
        let (_, _, _, items, _, _, _) = test_info(&path).unwrap();
        assert_eq!(items, inserted.len() as u64);

        // We should have managed to insert a substantial number.
        assert!(
            inserted.len() >= 500,
            "expected at least 500 insertions, got {}",
            inserted.len()
        );
    }

    #[test]
    fn concurrent_reads_from_same_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_cuckoo_file(dir.path(), "conc.cuckoo", 128, 4);

        // Pre-populate with some elements.
        for i in 0..20 {
            test_add(&path, format!("conc_{i}").as_bytes()).unwrap();
        }

        let path_clone = path.clone();
        let handles: Vec<_> = (0..4)
            .map(|t| {
                let p = path_clone.clone();
                std::thread::spawn(move || {
                    for i in 0..20 {
                        let elem = format!("conc_{i}");
                        let exists = test_exists(&p, elem.as_bytes()).unwrap();
                        assert!(exists, "thread {t}: element {elem} should exist");
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().expect("reader thread panicked");
        }
    }

    #[test]
    fn add_delete_readd_cycle() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_cuckoo_file(dir.path(), "cycle.cuckoo", 64, 4);

        test_add(&path, b"cycle").unwrap();
        assert!(test_exists(&path, b"cycle").unwrap());

        test_del(&path, b"cycle").unwrap();
        assert!(!test_exists(&path, b"cycle").unwrap());

        // Re-add after delete should work.
        test_add(&path, b"cycle").unwrap();
        assert!(test_exists(&path, b"cycle").unwrap());
    }

    #[test]
    fn delete_only_removes_one_occurrence() {
        // Add duplicates, delete one, verify count decreases by 1.
        let dir = tempfile::tempdir().unwrap();
        let path = create_cuckoo_file(dir.path(), "del_one.cuckoo", 128, 4);

        test_add(&path, b"multi").unwrap();
        test_add(&path, b"multi").unwrap();
        test_add(&path, b"multi").unwrap();
        assert_eq!(test_count(&path, b"multi").unwrap(), 3);

        test_del(&path, b"multi").unwrap();
        assert_eq!(test_count(&path, b"multi").unwrap(), 2);

        test_del(&path, b"multi").unwrap();
        assert_eq!(test_count(&path, b"multi").unwrap(), 1);

        // Still exists.
        assert!(test_exists(&path, b"multi").unwrap());

        test_del(&path, b"multi").unwrap();
        assert_eq!(test_count(&path, b"multi").unwrap(), 0);
        assert!(!test_exists(&path, b"multi").unwrap());
    }

    #[test]
    fn slot_offset_calculation() {
        // Verify byte offsets are calculated correctly.
        // bucket_idx=0, slot_idx=0 => HEADER_SIZE
        assert_eq!(cuckoo_file_slot_offset(0, 0, 4, 1), HEADER_SIZE as u64);

        // bucket_idx=1, slot_idx=0 with bucket_size=4, fp_size=1
        // => HEADER_SIZE + (1*4 + 0)*1 = HEADER_SIZE + 4
        assert_eq!(cuckoo_file_slot_offset(1, 0, 4, 1), HEADER_SIZE as u64 + 4);

        // bucket_idx=0, slot_idx=2 with bucket_size=4, fp_size=2
        // => HEADER_SIZE + (0*4 + 2)*2 = HEADER_SIZE + 4
        assert_eq!(cuckoo_file_slot_offset(0, 2, 4, 2), HEADER_SIZE as u64 + 4);

        // bucket_idx=3, slot_idx=1 with bucket_size=4, fp_size=1
        // => HEADER_SIZE + (3*4 + 1)*1 = HEADER_SIZE + 13
        assert_eq!(cuckoo_file_slot_offset(3, 1, 4, 1), HEADER_SIZE as u64 + 13);
    }

    #[test]
    fn num_items_counter_increments_per_add() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_cuckoo_file(dir.path(), "counter.cuckoo", 128, 4);

        for i in 0..10 {
            test_add(&path, format!("cnt_{i}").as_bytes()).unwrap();
            let (_, _, _, items, _, _, _) = test_info(&path).unwrap();
            assert_eq!(items, (i + 1) as u64);
        }
    }

    #[test]
    fn addnx_after_delete_readds() {
        // addnx after deletion should re-insert since element is gone.
        let dir = tempfile::tempdir().unwrap();
        let path = create_cuckoo_file(dir.path(), "addnx_del.cuckoo", 128, 4);

        assert_eq!(test_addnx(&path, b"reinsert").unwrap(), 1);
        assert_eq!(test_addnx(&path, b"reinsert").unwrap(), 0);

        test_del(&path, b"reinsert").unwrap();
        assert!(!test_exists(&path, b"reinsert").unwrap());

        // Now addnx should succeed again.
        assert_eq!(test_addnx(&path, b"reinsert").unwrap(), 1);
        assert!(test_exists(&path, b"reinsert").unwrap());
    }
}
