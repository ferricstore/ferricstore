
use std::collections::HashMap;
use std::fs::File;
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
/// Fingerprint bytes plus the alternate-bucket hash must fit in xxh3_128 output.
const MAX_FINGERPRINT_SIZE: u8 = 8;
const MAX_CUCKOO_BUCKET_BYTES: u64 = 1 << 30;

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

fn cuckoo_bucket_bytes(
    num_buckets: u32,
    bucket_size: u8,
    fingerprint_size: u8,
) -> Result<u64, String> {
    let bytes = u64::from(num_buckets)
        .checked_mul(u64::from(bucket_size))
        .and_then(|slots| slots.checked_mul(u64::from(fingerprint_size)))
        .ok_or_else(|| "cuckoo bucket region size overflow".to_string())?;
    if bytes > MAX_CUCKOO_BUCKET_BYTES {
        return Err(format!(
            "cuckoo bucket region exceeds {MAX_CUCKOO_BUCKET_BYTES} bytes"
        ));
    }
    Ok(bytes)
}

fn cuckoo_file_size(
    num_buckets: u32,
    bucket_size: u8,
    fingerprint_size: u8,
) -> Result<u64, String> {
    (HEADER_SIZE as u64)
        .checked_add(cuckoo_bucket_bytes(
            num_buckets,
            bucket_size,
            fingerprint_size,
        )?)
        .ok_or_else(|| "cuckoo file size overflow".into())
}

fn cuckoo_num_items_after_insert(num_items: u64) -> Result<u64, String> {
    num_items
        .checked_add(1)
        .ok_or_else(|| "cuckoo num_items overflow".into())
}

fn cuckoo_num_items_after_delete(num_items: u64) -> Result<u64, String> {
    num_items
        .checked_sub(1)
        .ok_or_else(|| "cuckoo num_items underflow".into())
}

fn cuckoo_num_deletes_after_delete(num_deletes: u64) -> Result<u64, String> {
    num_deletes
        .checked_add(1)
        .ok_or_else(|| "cuckoo num_deletes overflow".into())
}

fn cuckoo_read_exact_at(
    file: &File,
    buf: &mut [u8],
    offset: u64,
    label: &str,
) -> Result<(), String> {
    let mut read = 0;
    while read < buf.len() {
        let n = file
            .read_at(&mut buf[read..], offset + read as u64)
            .map_err(|e| format!("read {label}: {e}"))?;
        if n == 0 {
            return Err(format!("truncated cuckoo file while reading {label}"));
        }
        read += n;
    }
    Ok(())
}

/// Read and validate the 27-byte header from a file.
fn cuckoo_read_header(file: &File) -> Result<CuckooFileHeader, String> {
    let mut hdr = [0u8; HEADER_SIZE];
    cuckoo_read_exact_at(file, &mut hdr, OFF_MAGIC, "header")?;

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

    if num_buckets == 0 {
        return Err("num_buckets must be > 0".into());
    }
    if bucket_size == 0 {
        return Err("bucket_size must be > 0".into());
    }
    if fingerprint_size == 0 || fingerprint_size > MAX_FINGERPRINT_SIZE {
        return Err(format!(
            "fingerprint_size must be between 1 and {MAX_FINGERPRINT_SIZE}"
        ));
    }
    if max_kicks == 0 {
        return Err("max_kicks must be > 0".into());
    }
    let total_slots = u64::from(num_buckets) * u64::from(bucket_size);
    if num_items > total_slots {
        return Err("cuckoo num_items must not exceed total slots".into());
    }

    let expected_size = cuckoo_file_size(num_buckets, bucket_size, fingerprint_size)?;
    let actual_size = file
        .metadata()
        .map_err(|error| format!("read cuckoo file metadata: {error}"))?
        .len();
    if actual_size != expected_size {
        return Err(format!(
            "cuckoo file size mismatch: expected {expected_size}, got {actual_size}"
        ));
    }

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
    let modulus = u64::from(num_buckets);

    // XOR followed by `% num_buckets` is only involutive when the bucket
    // count is a power of two. Capacities are intentionally arbitrary, so use
    // modular reflection: h - (h - bucket) == bucket (mod N).
    ((fp_hash % modulus + modulus - bucket as u64) % modulus) as usize
}

fn cuckoo_file_candidate_buckets(
    primary: usize,
    alternate: usize,
) -> impl Iterator<Item = usize> {
    std::iter::once(primary).chain((alternate != primary).then_some(alternate))
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
    cuckoo_read_exact_at(file, &mut buf, offset, "slot")?;
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
    crate::write_all_at(file, fp, offset, "cuckoo slot")
}

/// Write num_items to the header.
fn cuckoo_file_write_num_items(file: &File, num_items: u64) -> Result<(), String> {
    crate::write_all_at(
        file,
        &num_items.to_le_bytes(),
        OFF_NUM_ITEMS,
        "cuckoo num_items",
    )
}

/// Write num_deletes to the header.
fn cuckoo_file_write_num_deletes(file: &File, num_deletes: u64) -> Result<(), String> {
    crate::write_all_at(
        file,
        &num_deletes.to_le_bytes(),
        OFF_NUM_DELETES,
        "cuckoo num_deletes",
    )
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
fn cuckoo_file_open_read(path: &str) -> Result<crate::LockedFile, FileOpenError> {
    crate::open_random_read_locked(Path::new(path)).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            FileOpenError::NotFound
        } else {
            FileOpenError::Other(format!("open: {e}"))
        }
    })
}

/// Open a cuckoo file for reading and writing.
fn cuckoo_file_open_rw(path: &str) -> Result<crate::LockedFile, FileOpenError> {
    crate::open_random_rw_locked(Path::new(path)).map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            FileOpenError::NotFound
        } else {
            FileOpenError::Other(format!("open: {e}"))
        }
    })
}

fn cuckoo_file_exists_in_open_file(
    file: &File,
    hdr: &CuckooFileHeader,
    element: &[u8],
) -> Result<u64, String> {
    let (fp, b1) =
        cuckoo_file_fingerprint_and_bucket(element, hdr.fingerprint_size as usize, hdr.num_buckets);
    let b2 = cuckoo_file_alternate_bucket(b1, &fp, hdr.num_buckets);

    for bucket in cuckoo_file_candidate_buckets(b1, b2) {
        for slot in 0..hdr.bucket_size {
            let s = cuckoo_file_read_slot(
                file,
                bucket,
                slot as usize,
                hdr.bucket_size,
                hdr.fingerprint_size,
            )?;
            if s == fp {
                return Ok(1);
            }
        }
    }

    Ok(0)
}

/// Encode a FileOpenError as an Erlang error term.
fn encode_file_open_error(env: Env, err: FileOpenError) -> Term {
    match err {
        FileOpenError::NotFound => (atoms::error(), atoms::enoent()).encode(env),
        FileOpenError::Other(msg) => (atoms::error(), msg).encode(env),
    }
}
