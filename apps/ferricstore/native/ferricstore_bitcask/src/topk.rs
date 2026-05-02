//! Stateless file-backed Top-K data structure (v2).
//!
//! Uses pread/pwrite on a fixed-layout file: header + CMS counters + min-heap.
//! No mmap, no ResourceArc — fully stateless NIF functions.

use std::collections::HashSet;
use std::fs::{self, File};
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::Path;

use rustler::{Binary, Encoder, Env, LocalPid, NifResult, OwnedBinary, Term};

// ---------------------------------------------------------------------------
// Constants (shared file format with the old mmap implementation)
// ---------------------------------------------------------------------------

const TOPK_MAGIC: u64 = 0x544F_504B_4D4D_5031; // "TOPKMMP1"
const TOPK_HEADER_SIZE: usize = 64;
const HEAP_ENTRY_SIZE: usize = 264; // 8 (count) + 4 (len) + 252 (element)
const MAX_ELEMENT_LEN: usize = 252;
const MAX_TOPK_K: usize = 100_000;
const MAX_TOPK_CMS_COUNTERS: usize = 1_048_576;

struct TopKLayout {
    heap_offset: usize,
    file_size: u64,
}

// ---------------------------------------------------------------------------
// Hash function
// ---------------------------------------------------------------------------

/// FNV-1a hash with a configurable offset basis for double hashing.
fn fnv1a(data: &[u8], offset_basis: u64) -> u64 {
    let mut hash = offset_basis;
    for &byte in data {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    hash
}

// ---------------------------------------------------------------------------
// NIF atoms
// ---------------------------------------------------------------------------

mod atoms {
    rustler::atoms! {
        ok,
        error,
        nil,
        enoent,
        tokio_complete,
    }
}

// ---------------------------------------------------------------------------
// Heap offset helper (replaces MmapTopK::heap_offset)
// ---------------------------------------------------------------------------

fn topk_layout(k: usize, width: usize, depth: usize) -> Result<TopKLayout, String> {
    let cms_entries = width
        .checked_mul(depth)
        .ok_or_else(|| "TopK CMS counter count overflow".to_string())?;
    if cms_entries > MAX_TOPK_CMS_COUNTERS {
        return Err(format!(
            "TopK CMS counter count exceeds {MAX_TOPK_CMS_COUNTERS}"
        ));
    }
    if k > MAX_TOPK_K {
        return Err(format!("k must be <= {MAX_TOPK_K}"));
    }
    let cms_bytes = cms_entries
        .checked_mul(8)
        .ok_or_else(|| "TopK CMS byte size overflow".to_string())?;
    let heap_offset = TOPK_HEADER_SIZE
        .checked_add(cms_bytes)
        .ok_or_else(|| "TopK heap offset overflow".to_string())?;
    let heap_bytes = k
        .checked_mul(HEAP_ENTRY_SIZE)
        .ok_or_else(|| "TopK heap byte size overflow".to_string())?;
    let file_size = heap_offset
        .checked_add(heap_bytes)
        .ok_or_else(|| "TopK file size overflow".to_string())?;

    Ok(TopKLayout {
        heap_offset,
        file_size: u64::try_from(file_size)
            .map_err(|_| "TopK file size exceeds u64".to_string())?,
    })
}

fn heap_offset(width: usize, depth: usize) -> usize {
    topk_layout(0, width, depth)
        .map(|layout| layout.heap_offset)
        .unwrap_or(usize::MAX)
}

// ===========================================================================
// v2 Stateless file-based TopK NIF functions (pread/pwrite)
// ===========================================================================
//
// These functions open the file, read/write specific regions via pread/pwrite,
// and close the fd on Drop. No mmap, no resource handle — fully stateless.
//
// File layout (little-endian):
//
// ```text
// [header: 64 bytes]
// [CMS counters: width * depth * 8 bytes (i64 each)]
// [heap entries: k * HEAP_ENTRY_SIZE bytes]
// ```
//
// Header (64 bytes):
//   bytes  0..7:  magic (0x544F504B_4D4D5031 = "TOPKMMP1")
//   bytes  8..11: k (u32)
//   bytes 12..15: width (u32)
//   bytes 16..19: depth (u32)
//   bytes 20..27: decay (f64)
//   bytes 28..31: heap_len (u32) — number of items currently in heap
//   bytes 32..63: reserved (zero)
//
// Each heap entry (HEAP_ENTRY_SIZE = 264 bytes):
//   bytes 0..7:   count (i64)
//   bytes 8..11:  element_len (u32)
//   bytes 12..263: element bytes (max 252 bytes, zero-padded)

/// Helper: read the mmap-format header from a file, returning
/// (k, width, depth, decay, heap_len) or an error string.
fn v2_read_header(file: &File) -> Result<(usize, usize, usize, f64, usize), String> {
    let mut hdr = [0u8; TOPK_HEADER_SIZE];
    file.read_at(&mut hdr, 0)
        .map_err(|e| format!("read header: {e}"))?;

    let magic = u64::from_le_bytes(hdr[0..8].try_into().unwrap());
    if magic != TOPK_MAGIC {
        return Err("invalid topk file magic".into());
    }

    let k = u32::from_le_bytes(hdr[8..12].try_into().unwrap()) as usize;
    let width = u32::from_le_bytes(hdr[12..16].try_into().unwrap()) as usize;
    let depth = u32::from_le_bytes(hdr[16..20].try_into().unwrap()) as usize;
    let decay = f64::from_le_bytes(hdr[20..28].try_into().unwrap());
    let heap_len = u32::from_le_bytes(hdr[28..32].try_into().unwrap()) as usize;

    if k == 0 {
        return Err("k must be > 0".into());
    }
    if width == 0 {
        return Err("width must be > 0".into());
    }
    if depth == 0 {
        return Err("depth must be > 0".into());
    }
    if !(0.0..=1.0).contains(&decay) {
        return Err("decay must be between 0 and 1".into());
    }
    if heap_len > k {
        return Err("heap_len must be <= k".into());
    }

    let _ = topk_layout(k, width, depth)?;

    Ok((k, width, depth, decay, heap_len))
}

/// Helper: read all CMS counters from the file into a Vec<i64>.
fn v2_read_cms(file: &File, width: usize, depth: usize) -> Result<Vec<i64>, String> {
    let cms_size = width * depth;
    let byte_len = cms_size * 8;
    let mut buf = vec![0u8; byte_len];
    file.read_at(&mut buf, TOPK_HEADER_SIZE as u64)
        .map_err(|e| format!("read cms: {e}"))?;

    let mut counters = Vec::with_capacity(cms_size);
    for i in 0..cms_size {
        let off = i * 8;
        counters.push(i64::from_le_bytes(buf[off..off + 8].try_into().unwrap()));
    }
    Ok(counters)
}

/// Helper: write all CMS counters back to the file.
fn v2_write_cms(file: &File, counters: &[i64]) -> Result<(), String> {
    let byte_len = counters.len() * 8;
    let mut buf = vec![0u8; byte_len];
    for (i, &val) in counters.iter().enumerate() {
        buf[i * 8..(i + 1) * 8].copy_from_slice(&val.to_le_bytes());
    }
    file.write_at(&buf, TOPK_HEADER_SIZE as u64)
        .map_err(|e| format!("write cms: {e}"))?;
    Ok(())
}

/// A heap entry read from file.
struct V2HeapEntry {
    element: String,
    count: i64,
}

/// Helper: read all heap entries from the file.
fn v2_read_heap(
    file: &File,
    width: usize,
    depth: usize,
    heap_len: usize,
    k: usize,
) -> Result<Vec<V2HeapEntry>, String> {
    let heap_base = heap_offset(width, depth) as u64;
    let read_count = heap_len.min(k);
    let byte_len = read_count * HEAP_ENTRY_SIZE;
    let mut buf = vec![0u8; byte_len];
    if byte_len > 0 {
        file.read_at(&mut buf, heap_base)
            .map_err(|e| format!("read heap: {e}"))?;
    }

    let mut entries = Vec::with_capacity(read_count);
    for i in 0..read_count {
        let base = i * HEAP_ENTRY_SIZE;
        let count = i64::from_le_bytes(buf[base..base + 8].try_into().unwrap());
        let elem_len = u32::from_le_bytes(buf[base + 8..base + 12].try_into().unwrap()) as usize;
        let clamped = elem_len.min(MAX_ELEMENT_LEN);
        let element = String::from_utf8_lossy(&buf[base + 12..base + 12 + clamped]).to_string();
        entries.push(V2HeapEntry { element, count });
    }
    Ok(entries)
}

/// Helper: write all heap entries + update heap_len in header.
fn v2_write_heap(
    file: &File,
    width: usize,
    depth: usize,
    entries: &[V2HeapEntry],
) -> Result<(), String> {
    let heap_base = heap_offset(width, depth) as u64;

    // Write heap entries
    let byte_len = entries.len() * HEAP_ENTRY_SIZE;
    let mut buf = vec![0u8; byte_len];
    for (i, entry) in entries.iter().enumerate() {
        let base = i * HEAP_ENTRY_SIZE;
        buf[base..base + 8].copy_from_slice(&entry.count.to_le_bytes());
        let elem_bytes = entry.element.as_bytes();
        let len = elem_bytes.len().min(MAX_ELEMENT_LEN);
        buf[base + 8..base + 12].copy_from_slice(&(len as u32).to_le_bytes());
        buf[base + 12..base + 12 + len].copy_from_slice(&elem_bytes[..len]);
        // rest is already zeroed
    }
    if byte_len > 0 {
        file.write_at(&buf, heap_base)
            .map_err(|e| format!("write heap: {e}"))?;
    }

    // Update heap_len in header at offset 28
    file.write_at(&(entries.len() as u32).to_le_bytes(), 28)
        .map_err(|e| format!("write heap_len: {e}"))?;

    Ok(())
}

/// Helper: CMS increment using in-memory counters array. Returns min estimate.
fn v2_cms_increment(
    counters: &mut [i64],
    width: usize,
    depth: usize,
    element: &[u8],
    count: i64,
) -> i64 {
    let h1 = fnv1a(element, 0x811c_9dc5);
    let h2 = fnv1a(element, 0x050c_5d1f);
    let mut min_count = i64::MAX;
    for i in 0..depth {
        let h = h1.wrapping_add((i as u64).wrapping_mul(h2));
        let col = (h % width as u64) as usize;
        let idx = i * width + col;
        counters[idx] += count;
        min_count = min_count.min(counters[idx]);
    }
    min_count
}

/// Helper: CMS estimate (read-only) using in-memory counters array.
fn v2_cms_estimate(counters: &[i64], width: usize, depth: usize, element: &[u8]) -> i64 {
    let h1 = fnv1a(element, 0x811c_9dc5);
    let h2 = fnv1a(element, 0x050c_5d1f);
    let mut min_count = i64::MAX;
    for i in 0..depth {
        let h = h1.wrapping_add((i as u64).wrapping_mul(h2));
        let col = (h % width as u64) as usize;
        let idx = i * width + col;
        min_count = min_count.min(counters[idx]);
    }
    min_count
}

/// Helper: add an element to the in-memory heap entries with CMS increment.
/// Returns the evicted element name if an eviction occurred.
fn v2_heap_add(
    entries: &mut Vec<V2HeapEntry>,
    fingerprints: &mut HashSet<String>,
    k: usize,
    element: &str,
    estimated: i64,
) -> Option<String> {
    // Already tracked? Update count in-place.
    if fingerprints.contains(element) {
        for entry in entries.iter_mut() {
            if entry.element == element {
                entry.count = estimated;
                break;
            }
        }
        return None;
    }

    // Heap has room
    if entries.len() < k {
        entries.push(V2HeapEntry {
            element: element.to_string(),
            count: estimated,
        });
        fingerprints.insert(element.to_string());
        return None;
    }

    // Heap full: find min and check if new element beats it
    let mut min_idx = 0;
    let mut min_count = entries[0].count;
    for (i, entry) in entries.iter().enumerate().skip(1) {
        if entry.count < min_count {
            min_count = entry.count;
            min_idx = i;
        }
    }

    if estimated > min_count {
        let evicted = entries[min_idx].element.clone();
        fingerprints.remove(&evicted);
        entries[min_idx] = V2HeapEntry {
            element: element.to_string(),
            count: estimated,
        };
        fingerprints.insert(element.to_string());
        Some(evicted)
    } else {
        None
    }
}

/// Create a new TopK file at the given path.
/// Returns `{:ok, :ok}` or `{:error, reason}`.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn topk_file_create_v2(
    env: Env,
    path: String,
    k: u32,
    width: u32,
    depth: u32,
    decay: f64,
) -> NifResult<Term> {
    if k == 0 {
        return Ok((atoms::error(), "k must be > 0").encode(env));
    }
    if width == 0 {
        return Ok((atoms::error(), "width must be > 0").encode(env));
    }
    if depth == 0 {
        return Ok((atoms::error(), "depth must be > 0").encode(env));
    }
    if !(0.0..=1.0).contains(&decay) {
        return Ok((atoms::error(), "decay must be between 0 and 1").encode(env));
    }
    let layout = match topk_layout(k as usize, width as usize, depth as usize) {
        Ok(layout) => layout,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    let p = Path::new(&path);
    if let Some(parent) = p.parent() {
        if let Err(e) = fs::create_dir_all(parent) {
            return Ok((atoms::error(), format!("mkdir: {e}")).encode(env));
        }
    }

    let mut file = match File::create(p) {
        Ok(f) => f,
        Err(e) => return Ok((atoms::error(), format!("create: {e}")).encode(env)),
    };

    let mut header = [0u8; TOPK_HEADER_SIZE];
    header[0..8].copy_from_slice(&TOPK_MAGIC.to_le_bytes());
    header[8..12].copy_from_slice(&k.to_le_bytes());
    header[12..16].copy_from_slice(&width.to_le_bytes());
    header[16..20].copy_from_slice(&depth.to_le_bytes());
    header[20..28].copy_from_slice(&decay.to_le_bytes());
    // heap_len = 0, reserved = 0 (already zeroed)

    if let Err(e) = file.write_all(&header) {
        return Ok((atoms::error(), format!("write header: {e}")).encode(env));
    }

    if let Err(e) = file.set_len(layout.file_size) {
        return Ok((atoms::error(), format!("set file size: {e}")).encode(env));
    }
    if let Err(e) = file.sync_data() {
        return Ok((atoms::error(), format!("fdatasync: {e}")).encode(env));
    }

    Ok((atoms::ok(), atoms::ok()).encode(env))
}

/// Add elements (each with increment 1) to a file-backed TopK.
/// Returns a list: nil for no eviction, or the evicted element binary.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn topk_file_add_v2<'a>(
    env: Env<'a>,
    path: String,
    elements: Vec<Binary<'a>>,
) -> NifResult<Term<'a>> {
    let file = match crate::open_random_rw(Path::new(&path)) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok((atoms::error(), atoms::enoent()).encode(env));
        }
        Err(e) => return Ok((atoms::error(), format!("open: {e}")).encode(env)),
    };

    let (k, width, depth, _decay, heap_len) = match v2_read_header(&file) {
        Ok(h) => h,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    let mut counters = match v2_read_cms(&file, width, depth) {
        Ok(c) => c,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    let mut heap_entries = match v2_read_heap(&file, width, depth, heap_len, k) {
        Ok(h) => h,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    let mut fingerprints: HashSet<String> =
        heap_entries.iter().map(|e| e.element.clone()).collect();

    let mut results: Vec<Term<'a>> = Vec::with_capacity(elements.len());
    for elem_bin in &elements {
        let elem_bytes = elem_bin.as_slice();
        let elem_str = String::from_utf8_lossy(elem_bytes);
        let estimated = v2_cms_increment(&mut counters, width, depth, elem_bytes, 1);
        match v2_heap_add(
            &mut heap_entries,
            &mut fingerprints,
            k,
            &elem_str,
            estimated,
        ) {
            Some(evicted) => {
                let evicted_bytes = evicted.as_bytes();
                match OwnedBinary::new(evicted_bytes.len()) {
                    Some(mut ob) => {
                        ob.as_mut_slice().copy_from_slice(evicted_bytes);
                        results.push(Binary::from_owned(ob, env).encode(env));
                    }
                    None => {
                        results.push(atoms::nil().encode(env));
                    }
                }
            }
            None => {
                results.push(atoms::nil().encode(env));
            }
        }
    }

    // Write back modified data
    if let Err(e) = v2_write_cms(&file, &counters) {
        return Ok((atoms::error(), e).encode(env));
    }
    if let Err(e) = v2_write_heap(&file, width, depth, &heap_entries) {
        return Ok((atoms::error(), e).encode(env));
    }
    // Durability: fsync before returning. TopK is NOT idempotent under
    // Raft replay (heap state + decay + counter RMW), so relying on
    // pagecache flush + replay corrupts state on kernel panic. See
    // docs/bitcask-background-fsync.md.
    if let Err(e) = crate::prob_fsync(&file) {
        return Ok((atoms::error(), e).encode(env));
    }

    crate::fadvise_dontneed(&file, 0, 0);
    Ok(results.encode(env))
}

/// Increment elements by specified amounts in a file-backed TopK.
/// `pairs` is a list of `{element_binary, increment}` tuples.
/// Returns a list: nil for no eviction, or the evicted element binary.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn topk_file_incrby_v2<'a>(
    env: Env<'a>,
    path: String,
    pairs: Vec<(Binary<'a>, i64)>,
) -> NifResult<Term<'a>> {
    let file = match crate::open_random_rw(Path::new(&path)) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok((atoms::error(), atoms::enoent()).encode(env));
        }
        Err(e) => return Ok((atoms::error(), format!("open: {e}")).encode(env)),
    };

    let (k, width, depth, _decay, heap_len) = match v2_read_header(&file) {
        Ok(h) => h,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    let mut counters = match v2_read_cms(&file, width, depth) {
        Ok(c) => c,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    let mut heap_entries = match v2_read_heap(&file, width, depth, heap_len, k) {
        Ok(h) => h,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    let mut fingerprints: HashSet<String> =
        heap_entries.iter().map(|e| e.element.clone()).collect();

    let mut results: Vec<Term<'a>> = Vec::with_capacity(pairs.len());
    for (elem_bin, count) in &pairs {
        let elem_bytes = elem_bin.as_slice();
        let elem_str = String::from_utf8_lossy(elem_bytes);
        let estimated = v2_cms_increment(&mut counters, width, depth, elem_bytes, *count);
        match v2_heap_add(
            &mut heap_entries,
            &mut fingerprints,
            k,
            &elem_str,
            estimated,
        ) {
            Some(evicted) => {
                let evicted_bytes = evicted.as_bytes();
                match OwnedBinary::new(evicted_bytes.len()) {
                    Some(mut ob) => {
                        ob.as_mut_slice().copy_from_slice(evicted_bytes);
                        results.push(Binary::from_owned(ob, env).encode(env));
                    }
                    None => {
                        results.push(atoms::nil().encode(env));
                    }
                }
            }
            None => {
                results.push(atoms::nil().encode(env));
            }
        }
    }

    // Write back modified data
    if let Err(e) = v2_write_cms(&file, &counters) {
        return Ok((atoms::error(), e).encode(env));
    }
    if let Err(e) = v2_write_heap(&file, width, depth, &heap_entries) {
        return Ok((atoms::error(), e).encode(env));
    }
    // Durability: fsync — see comment in topk_file_add_v2.
    if let Err(e) = crate::prob_fsync(&file) {
        return Ok((atoms::error(), e).encode(env));
    }

    crate::fadvise_dontneed(&file, 0, 0);
    Ok(results.encode(env))
}

/// Query whether elements are in the top-K heap of a file-backed TopK.
/// Returns a list of 0 (not in top-K) or 1 (in top-K).
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn topk_file_query_v2<'a>(
    env: Env<'a>,
    path: String,
    elements: Vec<Binary<'a>>,
) -> NifResult<Term<'a>> {
    let p = Path::new(&path);

    let file = match crate::open_random_read(p) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok((atoms::error(), atoms::enoent()).encode(env));
        }
        Err(e) => return Ok((atoms::error(), format!("open: {e}")).encode(env)),
    };

    let (k, width, depth, _decay, heap_len) = match v2_read_header(&file) {
        Ok(h) => h,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    let heap_entries = match v2_read_heap(&file, width, depth, heap_len, k) {
        Ok(h) => h,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    let fingerprints: HashSet<String> = heap_entries.iter().map(|e| e.element.clone()).collect();

    let results: Vec<i32> = elements
        .iter()
        .map(|elem_bin| {
            let elem_str = String::from_utf8_lossy(elem_bin.as_slice());
            i32::from(fingerprints.contains(elem_str.as_ref()))
        })
        .collect();

    crate::fadvise_dontneed(&file, 0, 0);
    Ok(results.encode(env))
}

/// List all elements in the top-K heap, sorted by count descending.
/// Returns a list of element binaries.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn topk_file_list_v2(env: Env<'_>, path: String) -> NifResult<Term<'_>> {
    let p = Path::new(&path);

    let file = match crate::open_random_read(p) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok((atoms::error(), atoms::enoent()).encode(env));
        }
        Err(e) => return Ok((atoms::error(), format!("open: {e}")).encode(env)),
    };

    let (k, width, depth, _decay, heap_len) = match v2_read_header(&file) {
        Ok(h) => h,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    let mut heap_entries = match v2_read_heap(&file, width, depth, heap_len, k) {
        Ok(h) => h,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    // Sort by count descending, then element ascending for ties
    heap_entries.sort_by(|a, b| {
        b.count
            .cmp(&a.count)
            .then_with(|| a.element.cmp(&b.element))
    });

    let mut result_terms: Vec<Term<'_>> = Vec::with_capacity(heap_entries.len());
    for entry in &heap_entries {
        let elem_bytes = entry.element.as_bytes();
        match OwnedBinary::new(elem_bytes.len()) {
            Some(mut ob) => {
                ob.as_mut_slice().copy_from_slice(elem_bytes);
                result_terms.push(Binary::from_owned(ob, env).encode(env));
            }
            None => {
                return Ok((atoms::error(), "out of memory").encode(env));
            }
        }
    }

    crate::fadvise_dontneed(&file, 0, 0);
    Ok(result_terms.encode(env))
}

/// Return CMS count estimates for the given elements from a file-backed TopK.
/// Returns a list of i64 estimates.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn topk_file_count_v2<'a>(
    env: Env<'a>,
    path: String,
    elements: Vec<Binary<'a>>,
) -> NifResult<Term<'a>> {
    let p = Path::new(&path);

    let file = match crate::open_random_read(p) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok((atoms::error(), atoms::enoent()).encode(env));
        }
        Err(e) => return Ok((atoms::error(), format!("open: {e}")).encode(env)),
    };

    let (_k, width, depth, _decay, _heap_len) = match v2_read_header(&file) {
        Ok(h) => h,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    let counters = match v2_read_cms(&file, width, depth) {
        Ok(c) => c,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    let results: Vec<i64> = elements
        .iter()
        .map(|elem_bin| v2_cms_estimate(&counters, width, depth, elem_bin.as_slice()))
        .collect();

    crate::fadvise_dontneed(&file, 0, 0);
    Ok(results.encode(env))
}

/// Return metadata from a file-backed TopK: `{k, width, depth, decay}`.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn topk_file_info_v2(env: Env, path: String) -> NifResult<Term> {
    let p = Path::new(&path);

    let file = match crate::open_random_read(p) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok((atoms::error(), atoms::enoent()).encode(env));
        }
        Err(e) => return Ok((atoms::error(), format!("open: {e}")).encode(env)),
    };

    let (k, width, depth, decay, _heap_len) = match v2_read_header(&file) {
        Ok(h) => h,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    crate::fadvise_dontneed(&file, 0, 0);
    Ok((k, width, depth, decay).encode(env))
}

// ---------------------------------------------------------------------------
// Async variants of read NIFs — Tokio spawn_blocking, never block BEAM
// ---------------------------------------------------------------------------

/// Async topk query: spawns on Tokio, sends result to `caller_pid`.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
pub fn topk_file_query_v2_async<'a>(
    env: Env<'a>,
    caller_pid: LocalPid,
    correlation_id: u64,
    path: String,
    elements: Vec<Binary<'a>>,
) -> NifResult<Term<'a>> {
    let elements_owned: Vec<Vec<u8>> = elements.iter().map(|e| e.as_slice().to_vec()).collect();
    crate::async_io::runtime().spawn(async move {
        let result = tokio::task::spawn_blocking(move || {
            let p = std::path::Path::new(&path);
            let file = crate::open_random_read(p).map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    "enoent".to_string()
                } else {
                    e.to_string()
                }
            })?;
            let (k, width, depth, _decay, heap_len) =
                v2_read_header(&file).map_err(|e| e.clone())?;
            let heap_entries =
                v2_read_heap(&file, width, depth, heap_len, k).map_err(|e| e.clone())?;
            let fingerprints: std::collections::HashSet<String> =
                heap_entries.iter().map(|e| e.element.clone()).collect();
            let results: Vec<i32> = elements_owned
                .iter()
                .map(|elem| {
                    let elem_str = String::from_utf8_lossy(elem);
                    i32::from(fingerprints.contains(elem_str.as_ref()))
                })
                .collect();
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

/// Async topk list: spawns on Tokio, sends result to `caller_pid`.
/// Returns element names as a list of byte vectors (encoded as binaries).
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
pub fn topk_file_list_v2_async(
    env: Env<'_>,
    caller_pid: LocalPid,
    correlation_id: u64,
    path: String,
) -> NifResult<Term<'_>> {
    crate::async_io::runtime().spawn(async move {
        let result = tokio::task::spawn_blocking(move || {
            let p = std::path::Path::new(&path);
            let file = crate::open_random_read(p).map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    "enoent".to_string()
                } else {
                    e.to_string()
                }
            })?;
            let (k, width, depth, _decay, heap_len) =
                v2_read_header(&file).map_err(|e| e.clone())?;
            let mut heap_entries =
                v2_read_heap(&file, width, depth, heap_len, k).map_err(|e| e.clone())?;
            heap_entries.sort_by(|a, b| {
                b.count
                    .cmp(&a.count)
                    .then_with(|| a.element.cmp(&b.element))
            });
            let items: Vec<Vec<u8>> = heap_entries
                .iter()
                .map(|e| e.element.as_bytes().to_vec())
                .collect();
            crate::fadvise_dontneed(&file, 0, 0);
            Ok(items)
        })
        .await
        .unwrap_or_else(|e| Err(format!("spawn_blocking: {e}")));

        let mut msg_env = rustler::OwnedEnv::new();
        let _ = msg_env.send_and_clear(&caller_pid, |env| match result {
            Ok(items) => {
                let terms: Vec<rustler::Term<'_>> = items
                    .iter()
                    .map(|item| match OwnedBinary::new(item.len()) {
                        Some(mut ob) => {
                            ob.as_mut_slice().copy_from_slice(item);
                            rustler::Binary::from_owned(ob, env).encode(env)
                        }
                        None => atoms::error().encode(env),
                    })
                    .collect();
                (atoms::tokio_complete(), correlation_id, atoms::ok(), terms).encode(env)
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

/// Async topk count: spawns on Tokio, sends CMS estimates to `caller_pid`.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
pub fn topk_file_count_v2_async<'a>(
    env: Env<'a>,
    caller_pid: LocalPid,
    correlation_id: u64,
    path: String,
    elements: Vec<Binary<'a>>,
) -> NifResult<Term<'a>> {
    let elements_owned: Vec<Vec<u8>> = elements.iter().map(|e| e.as_slice().to_vec()).collect();
    crate::async_io::runtime().spawn(async move {
        let result = tokio::task::spawn_blocking(move || {
            let p = std::path::Path::new(&path);
            let file = crate::open_random_read(p).map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    "enoent".to_string()
                } else {
                    e.to_string()
                }
            })?;
            let (_k, width, depth, _decay, _heap_len) =
                v2_read_header(&file).map_err(|e| e.clone())?;
            let counters = v2_read_cms(&file, width, depth).map_err(|e| e.clone())?;
            let results: Vec<i64> = elements_owned
                .iter()
                .map(|elem| v2_cms_estimate(&counters, width, depth, elem))
                .collect();
            crate::fadvise_dontneed(&file, 0, 0);
            Ok(results)
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

/// Async topk info: spawns on Tokio, sends metadata to `caller_pid`.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
pub fn topk_file_info_v2_async(
    env: Env<'_>,
    caller_pid: LocalPid,
    correlation_id: u64,
    path: String,
) -> NifResult<Term<'_>> {
    crate::async_io::runtime().spawn(async move {
        let result = tokio::task::spawn_blocking(move || {
            let p = std::path::Path::new(&path);
            let file = crate::open_random_read(p).map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    "enoent".to_string()
                } else {
                    e.to_string()
                }
            })?;
            let (k, width, depth, decay, _heap_len) =
                v2_read_header(&file).map_err(|e| e.clone())?;
            crate::fadvise_dontneed(&file, 0, 0);
            Ok((k, width, depth, decay))
        })
        .await
        .unwrap_or_else(|e| Err(format!("spawn_blocking: {e}")));

        let mut msg_env = rustler::OwnedEnv::new();
        let _ = msg_env.send_and_clear(&caller_pid, |env| match result {
            Ok((k, width, depth, decay)) => (
                atoms::tokio_complete(),
                correlation_id,
                atoms::ok(),
                (k, width, depth, decay),
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
// Rust-only unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cms_fnv1a_deterministic() {
        let h1 = fnv1a(b"hello", 0x811c_9dc5);
        let h2 = fnv1a(b"hello", 0x811c_9dc5);
        assert_eq!(h1, h2);

        let h3 = fnv1a(b"world", 0x811c_9dc5);
        assert_ne!(h1, h3);
    }

    // -----------------------------------------------------------------------
    // Edge case tests
    // -----------------------------------------------------------------------

    /// Helper: create a valid TopK file and return the path string.
    fn create_topk_file(
        dir: &std::path::Path,
        name: &str,
        k: u32,
        width: u32,
        depth: u32,
        decay: f64,
    ) -> String {
        let path = dir.join(name);
        let file_size = TOPK_HEADER_SIZE
            + (width as usize * depth as usize * 8)
            + (k as usize * HEAP_ENTRY_SIZE);

        let mut file = File::create(&path).unwrap();
        let mut header = [0u8; TOPK_HEADER_SIZE];
        header[0..8].copy_from_slice(&TOPK_MAGIC.to_le_bytes());
        header[8..12].copy_from_slice(&k.to_le_bytes());
        header[12..16].copy_from_slice(&width.to_le_bytes());
        header[16..20].copy_from_slice(&depth.to_le_bytes());
        header[20..28].copy_from_slice(&decay.to_le_bytes());
        // heap_len=0, reserved=0 (already zeroed)

        file.write_all(&header).unwrap();
        let zeros = vec![0u8; file_size - TOPK_HEADER_SIZE];
        file.write_all(&zeros).unwrap();
        file.sync_all().unwrap();
        path.to_str().unwrap().to_string()
    }

    #[test]
    fn empty_element_fnv1a() {
        let h = fnv1a(b"", 0x811c_9dc5);
        // Should be the offset basis XOR'd with nothing = offset basis
        assert_eq!(h, 0x811c_9dc5);
    }

    #[test]
    fn large_element_fnv1a() {
        let big = vec![0xAAu8; 1_000_000];
        let h = fnv1a(&big, 0x811c_9dc5);
        // Just verify it doesn't panic and returns a non-zero value.
        assert_ne!(h, 0);
    }

    #[test]
    fn truncated_header_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("truncated.topk");
        std::fs::write(&path, [0u8; 32]).unwrap();
        let file = File::open(&path).unwrap();
        assert!(v2_read_header(&file).is_err());
    }

    #[test]
    fn wrong_magic_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad_magic.topk");
        let mut data = [0u8; TOPK_HEADER_SIZE + 64];
        data[0..8].copy_from_slice(&0xDEAD_BEEF_u64.to_le_bytes());
        std::fs::write(&path, data).unwrap();
        let file = File::open(&path).unwrap();
        let result = v2_read_header(&file);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("magic"));
    }

    #[test]
    fn invalid_header_dimensions_return_error() {
        for (name, k, width, depth, decay) in [
            ("zero_k.topk", 0u32, 8u32, 3u32, 0.9f64),
            ("zero_width.topk", 5u32, 0u32, 3u32, 0.9f64),
            ("zero_depth.topk", 5u32, 8u32, 0u32, 0.9f64),
            ("oversized_cms.topk", 5u32, u32::MAX, u32::MAX, 0.9f64),
            ("invalid_decay.topk", 5u32, 8u32, 3u32, f64::NAN),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join(name);
            let mut data = [0u8; TOPK_HEADER_SIZE];
            data[0..8].copy_from_slice(&TOPK_MAGIC.to_le_bytes());
            data[8..12].copy_from_slice(&k.to_le_bytes());
            data[12..16].copy_from_slice(&width.to_le_bytes());
            data[16..20].copy_from_slice(&depth.to_le_bytes());
            data[20..28].copy_from_slice(&decay.to_le_bytes());
            std::fs::write(&path, data).unwrap();
            let file = File::open(&path).unwrap();

            let result = v2_read_header(&file);

            assert!(result.is_err(), "{name} should be rejected");
        }
    }

    #[test]
    fn valid_header_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path_str = create_topk_file(dir.path(), "header.topk", 5, 8, 7, 0.9);
        let file = File::open(&path_str).unwrap();
        let (k, width, depth, decay, heap_len) = v2_read_header(&file).unwrap();
        assert_eq!(k, 5);
        assert_eq!(width, 8);
        assert_eq!(depth, 7);
        assert!((decay - 0.9).abs() < 1e-10);
        assert_eq!(heap_len, 0);
    }

    #[test]
    fn cms_increment_and_estimate() {
        let width = 10;
        let depth = 3;
        let mut counters = vec![0i64; width * depth];

        // Increment "apple" by 5
        let est = v2_cms_increment(&mut counters, width, depth, b"apple", 5);
        assert_eq!(est, 5);

        // Increment "apple" by 3 more
        let est2 = v2_cms_increment(&mut counters, width, depth, b"apple", 3);
        assert_eq!(est2, 8);

        // Estimate should match
        let est3 = v2_cms_estimate(&counters, width, depth, b"apple");
        assert_eq!(est3, 8);

        // Unseen element should be 0
        let est4 = v2_cms_estimate(&counters, width, depth, b"banana");
        assert_eq!(est4, 0);
    }

    #[test]
    fn heap_add_and_eviction() {
        let k = 3;
        let mut entries: Vec<V2HeapEntry> = Vec::new();
        let mut fingerprints: HashSet<String> = HashSet::new();

        // Add 3 elements (heap has room, no eviction)
        assert_eq!(
            v2_heap_add(&mut entries, &mut fingerprints, k, "a", 10),
            None
        );
        assert_eq!(
            v2_heap_add(&mut entries, &mut fingerprints, k, "b", 20),
            None
        );
        assert_eq!(
            v2_heap_add(&mut entries, &mut fingerprints, k, "c", 30),
            None
        );
        assert_eq!(entries.len(), 3);

        // Add a 4th element with higher count => evicts "a" (count=10, the min)
        let evicted = v2_heap_add(&mut entries, &mut fingerprints, k, "d", 40);
        assert_eq!(evicted, Some("a".to_string()));
        assert_eq!(entries.len(), 3);
        assert!(!fingerprints.contains("a"));
        assert!(fingerprints.contains("d"));
    }

    #[test]
    fn heap_add_no_eviction_when_new_is_too_small() {
        let k = 2;
        let mut entries: Vec<V2HeapEntry> = Vec::new();
        let mut fingerprints: HashSet<String> = HashSet::new();

        v2_heap_add(&mut entries, &mut fingerprints, k, "a", 100);
        v2_heap_add(&mut entries, &mut fingerprints, k, "b", 200);

        // New element with count=50 is less than min(100), no eviction
        let evicted = v2_heap_add(&mut entries, &mut fingerprints, k, "c", 50);
        assert_eq!(evicted, None);
        assert_eq!(entries.len(), 2);
        assert!(!fingerprints.contains("c"));
    }

    #[test]
    fn heap_add_update_existing() {
        let k = 3;
        let mut entries: Vec<V2HeapEntry> = Vec::new();
        let mut fingerprints: HashSet<String> = HashSet::new();

        v2_heap_add(&mut entries, &mut fingerprints, k, "a", 10);
        v2_heap_add(&mut entries, &mut fingerprints, k, "b", 20);

        // Update "a" with new count
        let evicted = v2_heap_add(&mut entries, &mut fingerprints, k, "a", 50);
        assert_eq!(evicted, None); // no eviction, just update
        assert_eq!(entries.len(), 2);

        // Find "a" and verify count was updated
        let a_entry = entries.iter().find(|e| e.element == "a").unwrap();
        assert_eq!(a_entry.count, 50);
    }

    #[test]
    fn heap_read_write_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path_str = create_topk_file(dir.path(), "heap_rw.topk", 5, 8, 3, 0.9);
        let file = crate::open_random_rw(std::path::Path::new(&path_str)).unwrap();
        let (_, width, depth, _, _) = v2_read_header(&file).unwrap();

        // Write some heap entries
        let entries = vec![
            V2HeapEntry {
                element: "alpha".to_string(),
                count: 100,
            },
            V2HeapEntry {
                element: "beta".to_string(),
                count: 50,
            },
            V2HeapEntry {
                element: "gamma".to_string(),
                count: 25,
            },
        ];
        v2_write_heap(&file, width, depth, &entries).unwrap();

        // Read them back
        let read_entries = v2_read_heap(&file, width, depth, 3, 5).unwrap();
        assert_eq!(read_entries.len(), 3);
        assert_eq!(read_entries[0].element, "alpha");
        assert_eq!(read_entries[0].count, 100);
        assert_eq!(read_entries[1].element, "beta");
        assert_eq!(read_entries[1].count, 50);
        assert_eq!(read_entries[2].element, "gamma");
        assert_eq!(read_entries[2].count, 25);
    }

    #[test]
    fn empty_heap_read() {
        let dir = tempfile::tempdir().unwrap();
        let path_str = create_topk_file(dir.path(), "empty_heap.topk", 5, 8, 3, 0.9);
        let file = crate::open_random_read(std::path::Path::new(&path_str)).unwrap();
        let (k, width, depth, _, heap_len) = v2_read_header(&file).unwrap();
        assert_eq!(heap_len, 0);

        let entries = v2_read_heap(&file, width, depth, heap_len, k).unwrap();
        assert_eq!(entries.len(), 0);
    }

    #[test]
    fn cms_write_read_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path_str = create_topk_file(dir.path(), "cms_rw.topk", 3, 10, 5, 0.9);
        let file = crate::open_random_rw(std::path::Path::new(&path_str)).unwrap();
        let (_, width, depth, _, _) = v2_read_header(&file).unwrap();

        let mut counters = vec![0i64; width * depth];
        v2_cms_increment(&mut counters, width, depth, b"test", 42);

        v2_write_cms(&file, &counters).unwrap();

        let read_counters = v2_read_cms(&file, width, depth).unwrap();
        assert_eq!(counters, read_counters);

        let est = v2_cms_estimate(&read_counters, width, depth, b"test");
        assert_eq!(est, 42);
    }

    #[test]
    fn nonexistent_file_returns_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nope.topk");
        let result = crate::open_random_read(&path);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::NotFound);
    }

    #[test]
    fn element_max_length_clamp() {
        // Elements longer than MAX_ELEMENT_LEN (252) should be clamped
        let long_element = "x".repeat(300);
        let dir = tempfile::tempdir().unwrap();
        let path_str = create_topk_file(dir.path(), "long.topk", 3, 8, 3, 0.9);
        let file = crate::open_random_rw(std::path::Path::new(&path_str)).unwrap();
        let (_, width, depth, _, _) = v2_read_header(&file).unwrap();

        let entries = vec![V2HeapEntry {
            element: long_element.clone(),
            count: 10,
        }];
        v2_write_heap(&file, width, depth, &entries).unwrap();

        let read_entries = v2_read_heap(&file, width, depth, 1, 3).unwrap();
        assert_eq!(read_entries.len(), 1);
        // Should be clamped to MAX_ELEMENT_LEN
        assert_eq!(read_entries[0].element.len(), MAX_ELEMENT_LEN);
        assert_eq!(read_entries[0].count, 10);
    }

    // -----------------------------------------------------------------------
    // End-to-end file I/O tests (heap + CMS coordination)
    // -----------------------------------------------------------------------

    /// Helper: add elements to a TopK file using the same logic as the NIF,
    /// returning evicted element names (None = no eviction).
    fn topk_add_elements(path: &str, elements: &[&str]) -> Vec<Option<String>> {
        let file = crate::open_random_rw(std::path::Path::new(path)).unwrap();
        let (k, width, depth, _decay, heap_len) = v2_read_header(&file).unwrap();
        let mut counters = v2_read_cms(&file, width, depth).unwrap();
        let mut heap_entries = v2_read_heap(&file, width, depth, heap_len, k).unwrap();
        let mut fingerprints: HashSet<String> =
            heap_entries.iter().map(|e| e.element.clone()).collect();

        let mut results = Vec::new();
        for &elem in elements {
            let estimated = v2_cms_increment(&mut counters, width, depth, elem.as_bytes(), 1);
            let evicted = v2_heap_add(&mut heap_entries, &mut fingerprints, k, elem, estimated);
            results.push(evicted);
        }

        v2_write_cms(&file, &counters).unwrap();
        v2_write_heap(&file, width, depth, &heap_entries).unwrap();
        results
    }

    /// Helper: incrby a single element, return evicted name if any.
    fn topk_incrby_one(path: &str, element: &str, count: i64) -> Option<String> {
        let file = crate::open_random_rw(std::path::Path::new(path)).unwrap();
        let (k, width, depth, _decay, heap_len) = v2_read_header(&file).unwrap();
        let mut counters = v2_read_cms(&file, width, depth).unwrap();
        let mut heap_entries = v2_read_heap(&file, width, depth, heap_len, k).unwrap();
        let mut fingerprints: HashSet<String> =
            heap_entries.iter().map(|e| e.element.clone()).collect();

        let estimated = v2_cms_increment(&mut counters, width, depth, element.as_bytes(), count);
        let evicted = v2_heap_add(&mut heap_entries, &mut fingerprints, k, element, estimated);

        v2_write_cms(&file, &counters).unwrap();
        v2_write_heap(&file, width, depth, &heap_entries).unwrap();
        evicted
    }

    /// Helper: query whether an element is in the heap.
    fn topk_query(path: &str, element: &str) -> bool {
        let file = crate::open_random_read(std::path::Path::new(path)).unwrap();
        let (k, width, depth, _decay, heap_len) = v2_read_header(&file).unwrap();
        let heap_entries = v2_read_heap(&file, width, depth, heap_len, k).unwrap();
        heap_entries.iter().any(|e| e.element == element)
    }

    /// Helper: get CMS count estimate for an element.
    fn topk_count(path: &str, element: &str) -> i64 {
        let file = crate::open_random_read(std::path::Path::new(path)).unwrap();
        let (_k, width, depth, _decay, _heap_len) = v2_read_header(&file).unwrap();
        let counters = v2_read_cms(&file, width, depth).unwrap();
        v2_cms_estimate(&counters, width, depth, element.as_bytes())
    }

    /// Helper: list heap entries sorted descending by count.
    fn topk_list(path: &str) -> Vec<(String, i64)> {
        let file = crate::open_random_read(std::path::Path::new(path)).unwrap();
        let (k, width, depth, _decay, heap_len) = v2_read_header(&file).unwrap();
        let mut entries = v2_read_heap(&file, width, depth, heap_len, k).unwrap();
        entries.sort_by(|a, b| {
            b.count
                .cmp(&a.count)
                .then_with(|| a.element.cmp(&b.element))
        });
        entries.into_iter().map(|e| (e.element, e.count)).collect()
    }

    #[test]
    fn create_add_and_query() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_topk_file(dir.path(), "add_query.topk", 5, 64, 5, 0.9);
        topk_add_elements(&path, &["hello"]);
        assert!(topk_query(&path, "hello"));
        assert!(!topk_query(&path, "world"));
    }

    #[test]
    fn topk_eviction() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_topk_file(dir.path(), "evict.topk", 3, 64, 5, 0.9);

        // Add 3 elements — fills the heap
        topk_add_elements(&path, &["a", "b", "c"]);
        assert!(topk_query(&path, "a"));
        assert!(topk_query(&path, "b"));
        assert!(topk_query(&path, "c"));

        // Bump "d" so its CMS estimate exceeds the heap minimum (all at 1).
        // Increment "d" by 10 so it clearly beats the min.
        topk_incrby_one(&path, "d", 10);
        assert!(
            topk_query(&path, "d"),
            "d should have evicted a min element"
        );

        // One of a/b/c got evicted
        let in_heap: Vec<bool> = ["a", "b", "c"]
            .iter()
            .map(|e| topk_query(&path, e))
            .collect();
        let still_in = in_heap.iter().filter(|&&v| v).count();
        assert_eq!(still_in, 2, "exactly one of a/b/c should be evicted");
    }

    #[test]
    fn add_same_element_accumulates_count() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_topk_file(dir.path(), "accum.topk", 5, 64, 5, 0.9);

        topk_add_elements(&path, &["x", "x", "x", "x", "x"]);

        let count = topk_count(&path, "x");
        assert_eq!(count, 5);
        assert!(topk_query(&path, "x"));
    }

    #[test]
    fn query_nonexistent_returns_zero_count() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_topk_file(dir.path(), "noexist.topk", 5, 64, 5, 0.9);

        let count = topk_count(&path, "never_added");
        assert_eq!(count, 0);
        assert!(!topk_query(&path, "never_added"));
    }

    #[test]
    fn list_returns_sorted_descending() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_topk_file(dir.path(), "list.topk", 5, 64, 5, 0.9);

        topk_incrby_one(&path, "low", 1);
        topk_incrby_one(&path, "mid", 5);
        topk_incrby_one(&path, "high", 10);

        let list = topk_list(&path);
        assert_eq!(list.len(), 3);
        assert_eq!(list[0].0, "high");
        assert_eq!(list[0].1, 10);
        assert_eq!(list[1].0, "mid");
        assert_eq!(list[1].1, 5);
        assert_eq!(list[2].0, "low");
        assert_eq!(list[2].1, 1);
    }

    #[test]
    fn info_metadata_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_topk_file(dir.path(), "info.topk", 10, 128, 7, 0.85);
        let file = crate::open_random_read(std::path::Path::new(&path)).unwrap();
        let (k, width, depth, decay, heap_len) = v2_read_header(&file).unwrap();
        assert_eq!(k, 10);
        assert_eq!(width, 128);
        assert_eq!(depth, 7);
        assert!((decay - 0.85).abs() < 1e-10);
        assert_eq!(heap_len, 0);
    }

    #[test]
    fn large_k_stress() {
        let dir = tempfile::tempdir().unwrap();
        let k = 100u32;
        let path = create_topk_file(dir.path(), "stress.topk", k, 256, 5, 0.9);

        // Add 200 distinct elements
        for i in 0..200u32 {
            let name = format!("elem_{i:04}");
            topk_incrby_one(&path, &name, (i + 1) as i64);
        }

        let list = topk_list(&path);
        assert_eq!(list.len(), k as usize);

        // All entries in the list should have count >= 100 (bottom half evicted).
        // CMS can overcount due to hash collisions, so use >= not >.
        for (name, count) in &list {
            assert!(
                *count >= 100,
                "element {name} with count {count} should have been evicted"
            );
        }

        // The top element's CMS estimate should be >= 200 (may overcount
        // due to collisions, so we only check the lower bound).
        assert!(
            list[0].1 >= 200,
            "top element count {} should be >= 200",
            list[0].1
        );
    }

    #[test]
    fn incrby_with_large_increment() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_topk_file(dir.path(), "big_incr.topk", 5, 64, 5, 0.9);
        topk_incrby_one(&path, "big", 1_000_000);
        let count = topk_count(&path, "big");
        assert_eq!(count, 1_000_000);
    }

    #[test]
    fn eviction_preserves_higher_counts() {
        let dir = tempfile::tempdir().unwrap();
        let path = create_topk_file(dir.path(), "pres.topk", 2, 64, 5, 0.9);

        topk_incrby_one(&path, "keep_a", 100);
        topk_incrby_one(&path, "keep_b", 50);

        // This has a lower count than both, should NOT evict
        topk_incrby_one(&path, "loser", 1);
        assert!(!topk_query(&path, "loser"));
        assert!(topk_query(&path, "keep_a"));
        assert!(topk_query(&path, "keep_b"));
    }
}
