//! Stateless file-backed Top-K data structure (v2).
//!
//! Uses pread/pwrite on a fixed-layout file: header + CMS counters + min-heap.
//! No mmap, no ResourceArc — fully stateless NIF functions.

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::Path;

use rustler::{Binary, Encoder, Env, LocalPid, NifResult, OwnedBinary, Term};

// ---------------------------------------------------------------------------
// Constants (shared file format with the old mmap implementation)
// ---------------------------------------------------------------------------

const TOPK_MAGIC: [u8; 8] = *b"TOPKFS01";
const TOPK_HEADER_SIZE: usize = 64;
const HEAP_ENTRY_SIZE: usize = 264; // 8 (count) + 4 (len) + 252 (element)
const MAX_ELEMENT_LEN: usize = 252;
const MAX_TOPK_K: usize = 100_000;
const MAX_TOPK_CMS_COUNTERS: usize = 1_048_576;

struct TopKLayout {
    heap_offset: usize,
    token_offset: u64,
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
    let token_offset = heap_offset
        .checked_add(heap_bytes)
        .ok_or_else(|| "TopK file size overflow".to_string())?;
    let file_size = token_offset
        .checked_add(crate::prob_txn::TOKEN_SIZE)
        .ok_or_else(|| "TopK mutation footer size overflow".to_string())?;

    Ok(TopKLayout {
        heap_offset,
        token_offset: u64::try_from(token_offset)
            .map_err(|_| "TopK mutation footer offset exceeds u64".to_string())?,
        file_size: u64::try_from(file_size)
            .map_err(|_| "TopK file size exceeds u64".to_string())?,
    })
}

fn heap_offset(width: usize, depth: usize) -> usize {
    topk_layout(0, width, depth)
        .map(|layout| layout.heap_offset)
        .unwrap_or(usize::MAX)
}

fn topk_read_exact_at(file: &File, buf: &mut [u8], offset: u64, label: &str) -> Result<(), String> {
    let mut read = 0;
    while read < buf.len() {
        let n = file
            .read_at(&mut buf[read..], offset + read as u64)
            .map_err(|e| format!("read {label}: {e}"))?;
        if n == 0 {
            return Err(format!("truncated topk file while reading {label}"));
        }
        read += n;
    }
    Ok(())
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
// [mutation token: 16 bytes]
// ```
//
// Header (64 bytes):
//   bytes  0..7:  magic ("TOPKFS01")
//   bytes  8..11: k (u32)
//   bytes 12..15: width (u32)
//   bytes 16..19: depth (u32)
//   bytes 20..23: heap_len (u32) — number of items currently in heap
//   bytes 24..63: reserved (zero)
//
// Each heap entry (HEAP_ENTRY_SIZE = 264 bytes):
//   bytes 0..7:   count (i64)
//   bytes 8..11:  element_len (u32)
//   bytes 12..263: element bytes (max 252 bytes, zero-padded)

/// Helper: read the fixed-format header from a file, returning
/// (k, width, depth, heap_len) or an error string.
fn v2_read_header(file: &File) -> Result<(usize, usize, usize, usize), String> {
    let mut hdr = [0u8; TOPK_HEADER_SIZE];
    topk_read_exact_at(file, &mut hdr, 0, "header")?;

    if hdr[0..8] != TOPK_MAGIC {
        return Err("invalid topk file magic".into());
    }

    let k = u32::from_le_bytes(hdr[8..12].try_into().unwrap()) as usize;
    let width = u32::from_le_bytes(hdr[12..16].try_into().unwrap()) as usize;
    let depth = u32::from_le_bytes(hdr[16..20].try_into().unwrap()) as usize;
    let heap_len = u32::from_le_bytes(hdr[20..24].try_into().unwrap()) as usize;

    if k == 0 {
        return Err("k must be > 0".into());
    }
    if width == 0 {
        return Err("width must be > 0".into());
    }
    if depth == 0 {
        return Err("depth must be > 0".into());
    }
    if heap_len > k {
        return Err("heap_len must be <= k".into());
    }
    if hdr[24..].iter().any(|byte| *byte != 0) {
        return Err("topk reserved header bytes must be zero".into());
    }

    let layout = topk_layout(k, width, depth)?;
    let actual_size = file
        .metadata()
        .map_err(|error| format!("read TopK file metadata: {error}"))?
        .len();
    if actual_size != layout.file_size {
        return Err(format!(
            "TopK file size mismatch: expected {}, got {actual_size}",
            layout.file_size
        ));
    }

    Ok((k, width, depth, heap_len))
}

/// Helper: read all CMS counters from the file into a Vec<i64>.
fn v2_read_cms(file: &File, width: usize, depth: usize) -> Result<Vec<i64>, String> {
    let cms_size = width * depth;
    let byte_len = cms_size * 8;
    let mut buf = vec![0u8; byte_len];
    topk_read_exact_at(file, &mut buf, TOPK_HEADER_SIZE as u64, "cms")?;

    let mut counters = Vec::with_capacity(cms_size);
    for i in 0..cms_size {
        let off = i * 8;
        counters.push(i64::from_le_bytes(buf[off..off + 8].try_into().unwrap()));
    }
    Ok(counters)
}

fn v2_encode_cms(counters: &[i64]) -> Result<Vec<u8>, String> {
    let byte_len = counters
        .len()
        .checked_mul(8)
        .ok_or_else(|| "TopK CMS encoding size overflow".to_string())?;
    let mut buf = Vec::new();
    buf.try_reserve_exact(byte_len)
        .map_err(|_| "TopK CMS encoding allocation failed".to_string())?;
    buf.resize(byte_len, 0);
    for (i, &val) in counters.iter().enumerate() {
        buf[i * 8..(i + 1) * 8].copy_from_slice(&val.to_le_bytes());
    }
    Ok(buf)
}

/// Helper: write all CMS counters back to the file.
#[cfg(test)]
fn v2_write_cms(file: &File, counters: &[i64]) -> Result<(), String> {
    let encoded = v2_encode_cms(counters)?;
    crate::write_all_at(file, &encoded, TOPK_HEADER_SIZE as u64, "topk cms")
}

/// A heap entry read from file.
struct V2HeapEntry {
    element: Vec<u8>,
    count: i64,
}

struct V2IndexedHeap {
    entries: Vec<V2HeapEntry>,
    positions: HashMap<Vec<u8>, usize>,
    capacity: usize,
}

impl V2IndexedHeap {
    fn new(
        mut entries: Vec<V2HeapEntry>,
        capacity: usize,
        incoming_count: usize,
    ) -> Result<Self, String> {
        if entries.len() > capacity {
            return Err("TopK heap length exceeds k".into());
        }

        let additional = incoming_count.min(capacity - entries.len());
        entries
            .try_reserve_exact(additional)
            .map_err(|_| "out of memory while reserving TopK heap".to_string())?;

        let indexed_count = entries
            .len()
            .checked_add(additional)
            .ok_or_else(|| "TopK heap index size overflow".to_string())?;
        let mut positions = HashMap::new();
        positions
            .try_reserve(indexed_count)
            .map_err(|_| "out of memory while reserving TopK heap index".to_string())?;

        for (index, entry) in entries.iter().enumerate() {
            if positions.insert(entry.element.clone(), index).is_some() {
                return Err("TopK heap contains a duplicate element".into());
            }
        }

        let mut heap = Self {
            entries,
            positions,
            capacity,
        };
        for index in (0..heap.entries.len() / 2).rev() {
            heap.sift_down(index);
        }
        Ok(heap)
    }

    fn entry_precedes(left: &V2HeapEntry, right: &V2HeapEntry) -> bool {
        left.count < right.count
            || (left.count == right.count && left.element.as_slice() < right.element.as_slice())
    }

    fn swap(&mut self, left: usize, right: usize) {
        if left == right {
            return;
        }

        let Self {
            entries, positions, ..
        } = self;
        entries.swap(left, right);
        *positions
            .get_mut(entries[left].element.as_slice())
            .expect("TopK heap index must contain the swapped element") = left;
        *positions
            .get_mut(entries[right].element.as_slice())
            .expect("TopK heap index must contain the swapped element") = right;
    }

    fn sift_up(&mut self, mut index: usize) {
        while index > 0 {
            let parent = (index - 1) / 2;
            if !Self::entry_precedes(&self.entries[index], &self.entries[parent]) {
                break;
            }
            self.swap(index, parent);
            index = parent;
        }
    }

    fn sift_down(&mut self, mut index: usize) {
        loop {
            let left = index * 2 + 1;
            if left >= self.entries.len() {
                break;
            }

            let right = left + 1;
            let child = if right < self.entries.len()
                && Self::entry_precedes(&self.entries[right], &self.entries[left])
            {
                right
            } else {
                left
            };
            if !Self::entry_precedes(&self.entries[child], &self.entries[index]) {
                break;
            }
            self.swap(index, child);
            index = child;
        }
    }

    fn add(&mut self, element: &[u8], estimated: i64) -> Option<Vec<u8>> {
        if let Some(index) = self.positions.get(element).copied() {
            let previous = self.entries[index].count;
            self.entries[index].count = estimated;
            if estimated < previous {
                self.sift_up(index);
            } else if estimated > previous {
                self.sift_down(index);
            }
            return None;
        }

        if self.entries.len() < self.capacity {
            let index = self.entries.len();
            let indexed_element = element.to_vec();
            self.entries.push(V2HeapEntry {
                element: indexed_element.clone(),
                count: estimated,
            });
            self.positions.insert(indexed_element, index);
            self.sift_up(index);
            return None;
        }

        if estimated <= self.entries[0].count {
            return None;
        }

        let indexed_element = element.to_vec();
        let evicted = std::mem::replace(
            &mut self.entries[0],
            V2HeapEntry {
                element: indexed_element.clone(),
                count: estimated,
            },
        )
        .element;
        self.positions.remove(evicted.as_slice());
        self.positions.insert(indexed_element, 0);
        self.sift_down(0);
        Some(evicted)
    }
}

fn v2_query_fingerprints(entries: &[V2HeapEntry]) -> HashSet<&[u8]> {
    entries
        .iter()
        .map(|entry| entry.element.as_slice())
        .collect()
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
        topk_read_exact_at(file, &mut buf, heap_base, "heap")?;
    }

    let mut entries = Vec::with_capacity(read_count);
    for i in 0..read_count {
        let base = i * HEAP_ENTRY_SIZE;
        let count = i64::from_le_bytes(buf[base..base + 8].try_into().unwrap());
        let elem_len = u32::from_le_bytes(buf[base + 8..base + 12].try_into().unwrap()) as usize;
        if elem_len > MAX_ELEMENT_LEN {
            return Err(format!(
                "TopK heap element length {elem_len} exceeds {MAX_ELEMENT_LEN}"
            ));
        }
        let element = buf[base + 12..base + 12 + elem_len].to_vec();
        entries.push(V2HeapEntry { element, count });
    }

    let mut seen = HashSet::with_capacity(entries.len());
    for entry in &entries {
        if !seen.insert(entry.element.as_slice()) {
            return Err("TopK heap contains a duplicate element".into());
        }
    }

    Ok(entries)
}

fn v2_encode_heap(entries: &[V2HeapEntry]) -> Result<Vec<u8>, String> {
    let byte_len = entries
        .len()
        .checked_mul(HEAP_ENTRY_SIZE)
        .ok_or_else(|| "TopK heap encoding size overflow".to_string())?;
    let mut buf = Vec::new();
    buf.try_reserve_exact(byte_len)
        .map_err(|_| "TopK heap encoding allocation failed".to_string())?;
    buf.resize(byte_len, 0);
    for (i, entry) in entries.iter().enumerate() {
        let base = i * HEAP_ENTRY_SIZE;
        buf[base..base + 8].copy_from_slice(&entry.count.to_le_bytes());
        let elem_bytes = entry.element.as_slice();
        let len = elem_bytes.len();
        if len > MAX_ELEMENT_LEN {
            return Err(format!(
                "TopK heap element length {len} exceeds {MAX_ELEMENT_LEN}"
            ));
        }
        buf[base + 8..base + 12].copy_from_slice(&(len as u32).to_le_bytes());
        buf[base + 12..base + 12 + len].copy_from_slice(&elem_bytes[..len]);
    }
    Ok(buf)
}

/// Helper: write all heap entries + update heap_len in header.
#[cfg(test)]
fn v2_write_heap(
    file: &File,
    width: usize,
    depth: usize,
    entries: &[V2HeapEntry],
) -> Result<(), String> {
    let heap_base = heap_offset(width, depth) as u64;
    let encoded = v2_encode_heap(entries)?;
    if !encoded.is_empty() {
        crate::write_all_at(file, &encoded, heap_base, "topk heap")?;
    }

    // Update heap_len in header at offset 20.
    crate::write_all_at(
        file,
        &(entries.len() as u32).to_le_bytes(),
        20,
        "topk heap_len",
    )
}

/// Helper: CMS increment using in-memory counters array. Returns min estimate.
fn v2_cms_increment(
    counters: &mut [i64],
    width: usize,
    depth: usize,
    element: &[u8],
    count: i64,
) -> Result<i64, String> {
    let h1 = fnv1a(element, 0x811c_9dc5);
    let h2 = fnv1a(element, 0x050c_5d1f);
    let mut min_count = i64::MAX;
    let mut updates = Vec::with_capacity(depth);

    for i in 0..depth {
        let h = h1.wrapping_add((i as u64).wrapping_mul(h2));
        let col = (h % width as u64) as usize;
        let idx = i * width + col;
        let next = counters[idx]
            .checked_add(count)
            .ok_or_else(|| format!("TopK CMS counter overflow: {} + {}", counters[idx], count))?;
        updates.push((idx, next));
        min_count = min_count.min(next);
    }

    for (idx, next) in updates {
        counters[idx] = next;
    }

    Ok(min_count)
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

struct V2MutationPlan {
    images: Vec<crate::prob_txn::AfterImage>,
    results: Vec<Option<Vec<u8>>>,
}

fn v2_stage_mutation(file: &File, updates: &[(&[u8], i64)]) -> Result<V2MutationPlan, String> {
    let (k, width, depth, heap_len) = v2_read_header(file)?;
    let layout = topk_layout(k, width, depth)?;
    let mut counters = v2_read_cms(file, width, depth)?;
    let heap_entries = v2_read_heap(file, width, depth, heap_len, k)?;
    let mut heap = V2IndexedHeap::new(heap_entries, k, updates.len())?;

    let mut results = Vec::new();
    results
        .try_reserve_exact(updates.len())
        .map_err(|_| "TopK mutation result allocation failed".to_string())?;
    for (element, increment) in updates {
        let estimated = v2_cms_increment(&mut counters, width, depth, element, *increment)?;
        results.push(heap.add(element, estimated));
    }

    let mut images = Vec::new();
    images
        .try_reserve_exact(3)
        .map_err(|_| "TopK mutation journal allocation failed".to_string())?;
    images.push(crate::prob_txn::AfterImage::new(
        TOPK_HEADER_SIZE as u64,
        v2_encode_cms(&counters)?,
    ));
    let heap_bytes = v2_encode_heap(&heap.entries)?;
    if !heap_bytes.is_empty() {
        images.push(crate::prob_txn::AfterImage::new(
            u64::try_from(layout.heap_offset)
                .map_err(|_| "TopK heap offset exceeds u64".to_string())?,
            heap_bytes,
        ));
    }
    let heap_len = u32::try_from(heap.entries.len())
        .map_err(|_| "TopK heap length exceeds u32".to_string())?;
    images.push(crate::prob_txn::AfterImage::new(
        20,
        heap_len.to_le_bytes().to_vec(),
    ));

    Ok(V2MutationPlan { images, results })
}

fn v2_encode_mutation_results(results: &[Option<Vec<u8>>]) -> Result<Vec<u8>, String> {
    let count = u32::try_from(results.len())
        .map_err(|_| "TopK mutation result count exceeds u32".to_string())?;
    let payload_size = results.iter().try_fold(4_usize, |size, result| {
        size.checked_add(4)
            .and_then(|next| {
                result
                    .as_ref()
                    .map_or(Some(next), |value| next.checked_add(value.len()))
            })
            .ok_or_else(|| "TopK mutation result size overflow".to_string())
    })?;
    let mut encoded = Vec::new();
    encoded
        .try_reserve_exact(payload_size)
        .map_err(|_| "TopK mutation result encoding allocation failed".to_string())?;
    encoded.extend_from_slice(&count.to_le_bytes());
    for result in results {
        match result {
            None => encoded.extend_from_slice(&u32::MAX.to_le_bytes()),
            Some(value) => {
                let length = u32::try_from(value.len())
                    .map_err(|_| "TopK mutation result element exceeds u32".to_string())?;
                encoded.extend_from_slice(&length.to_le_bytes());
                encoded.extend_from_slice(value);
            }
        }
    }
    Ok(encoded)
}

fn v2_decode_mutation_results(
    encoded: &[u8],
    expected_count: usize,
) -> Result<Vec<Option<Vec<u8>>>, String> {
    let count_bytes = encoded
        .get(0..4)
        .ok_or_else(|| "truncated TopK mutation result".to_string())?;
    let count = u32::from_le_bytes(count_bytes.try_into().unwrap()) as usize;
    if count != expected_count {
        return Err("TopK mutation result cardinality mismatch".into());
    }
    let mut results = Vec::new();
    results
        .try_reserve_exact(count)
        .map_err(|_| "TopK mutation result allocation failed".to_string())?;
    let mut cursor = 4_usize;
    for _ in 0..count {
        let length_end = cursor
            .checked_add(4)
            .ok_or_else(|| "TopK mutation result offset overflow".to_string())?;
        let length_bytes = encoded
            .get(cursor..length_end)
            .ok_or_else(|| "truncated TopK mutation result".to_string())?;
        let length = u32::from_le_bytes(length_bytes.try_into().unwrap());
        cursor = length_end;
        if length == u32::MAX {
            results.push(None);
            continue;
        }
        let length = length as usize;
        if length > MAX_ELEMENT_LEN {
            return Err("TopK mutation result element exceeds maximum length".into());
        }
        let value_end = cursor
            .checked_add(length)
            .ok_or_else(|| "TopK mutation result length overflow".to_string())?;
        let value = encoded
            .get(cursor..value_end)
            .ok_or_else(|| "truncated TopK mutation result".to_string())?;
        results.push(Some(value.to_vec()));
        cursor = value_end;
    }
    if cursor != encoded.len() {
        return Err("TopK mutation result contains trailing bytes".into());
    }
    Ok(results)
}

fn v2_stale_mutation_results(count: usize) -> Result<Vec<Option<Vec<u8>>>, String> {
    let mut results = Vec::new();
    results
        .try_reserve_exact(count)
        .map_err(|_| "TopK stale result allocation failed".to_string())?;
    results.resize_with(count, || None);
    Ok(results)
}

fn v2_apply_mutation_plan(file: &File, plan: &V2MutationPlan) -> Result<(), String> {
    for image in &plan.images {
        crate::write_all_at(
            file,
            &image.bytes,
            image.offset,
            "TopK mutation after-image",
        )?;
    }
    crate::prob_fsync(file)
}

fn v2_transactional_mutation(
    file: &File,
    receipt_path: &Path,
    updates: &[(&[u8], i64)],
    token: crate::prob_txn::MutationToken,
) -> Result<Vec<Option<Vec<u8>>>, String> {
    let (k, width, depth, _) = v2_read_header(file)?;
    let layout = topk_layout(k, width, depth)?;
    match crate::prob_txn::begin(
        file,
        receipt_path,
        token,
        layout.token_offset,
        layout.file_size,
    )? {
        crate::prob_txn::MutationDecision::Replay(result) => {
            v2_decode_mutation_results(&result, updates.len())
        }
        crate::prob_txn::MutationDecision::Stale => v2_stale_mutation_results(updates.len()),
        crate::prob_txn::MutationDecision::Apply => {
            let plan = v2_stage_mutation(file, updates)?;
            let encoded_result = v2_encode_mutation_results(&plan.results)?;
            crate::prob_txn::commit(
                file,
                receipt_path,
                token,
                layout.token_offset,
                layout.file_size,
                plan.images,
                encoded_result,
            )?;
            Ok(plan.results)
        }
    }
}

fn v2_encode_result_terms<'a>(
    env: Env<'a>,
    results: Vec<Option<Vec<u8>>>,
) -> Result<Term<'a>, String> {
    let mut terms = Vec::new();
    terms
        .try_reserve_exact(results.len())
        .map_err(|_| "TopK result term allocation failed".to_string())?;
    for result in results {
        match result {
            None => terms.push(atoms::nil().encode(env)),
            Some(value) => {
                let mut binary = OwnedBinary::new(value.len())
                    .ok_or_else(|| "TopK result binary allocation failed".to_string())?;
                binary.as_mut_slice().copy_from_slice(&value);
                terms.push(Binary::from_owned(binary, env).encode(env));
            }
        }
    }
    Ok(terms.encode(env))
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
    let layout = match topk_layout(k as usize, width as usize, depth as usize) {
        Ok(layout) => layout,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    let p = Path::new(&path);
    if let Some(parent) = p.parent() {
        if let Err(e) = crate::fs_nif::create_dir_all_nofollow(parent) {
            return Ok((atoms::error(), format!("mkdir: {e}")).encode(env));
        }
    }

    let mut file = match crate::create_staged_locked_nofollow(p) {
        Ok(f) => f,
        Err(e) => return Ok((atoms::error(), format!("create: {e}")).encode(env)),
    };

    let mut header = [0u8; TOPK_HEADER_SIZE];
    header[0..8].copy_from_slice(&TOPK_MAGIC);
    header[8..12].copy_from_slice(&k.to_le_bytes());
    header[12..16].copy_from_slice(&width.to_le_bytes());
    header[16..20].copy_from_slice(&depth.to_le_bytes());
    // heap_len = 0 and reserved = 0 (already zeroed)

    if let Err(e) = file.write_all(&header) {
        return Ok((atoms::error(), format!("write header: {e}")).encode(env));
    }

    if let Err(e) = file.set_len(layout.file_size) {
        return Ok((atoms::error(), format!("set file size: {e}")).encode(env));
    }
    if let Err(e) = file.publish() {
        return Ok((atoms::error(), format!("publish: {e}")).encode(env));
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
    if let Some(len) = elements
        .iter()
        .map(|element| element.as_slice().len())
        .find(|len| *len > MAX_ELEMENT_LEN)
    {
        return Ok((
            atoms::error(),
            format!("TopK element length {len} exceeds {MAX_ELEMENT_LEN}"),
        )
            .encode(env));
    }

    let file = match crate::open_random_rw_locked(Path::new(&path)) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok((atoms::error(), atoms::enoent()).encode(env));
        }
        Err(e) => return Ok((atoms::error(), format!("open: {e}")).encode(env)),
    };

    let mut updates = Vec::new();
    if updates.try_reserve_exact(elements.len()).is_err() {
        return Ok((atoms::error(), "TopK mutation input allocation failed").encode(env));
    }
    for element in &elements {
        updates.push((element.as_slice(), 1));
    }
    let plan = match v2_stage_mutation(&file, &updates) {
        Ok(plan) => plan,
        Err(error) => return Ok((atoms::error(), error).encode(env)),
    };
    if let Err(error) = v2_apply_mutation_plan(&file, &plan) {
        return Ok((atoms::error(), error).encode(env));
    }

    crate::fadvise_dontneed(&file, 0, 0);
    match v2_encode_result_terms(env, plan.results) {
        Ok(term) => Ok(term),
        Err(error) => Ok((atoms::error(), error).encode(env)),
    }
}

/// Add elements using a deterministic Raft mutation token.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn topk_file_add_v2_at<'a>(
    env: Env<'a>,
    path: String,
    receipt_path: String,
    elements: Vec<Binary<'a>>,
    mutation_index: u64,
    mutation_ordinal: u64,
) -> NifResult<Term<'a>> {
    if let Some(len) = elements
        .iter()
        .map(|element| element.as_slice().len())
        .find(|len| *len > MAX_ELEMENT_LEN)
    {
        return Ok((
            atoms::error(),
            format!("TopK element length {len} exceeds {MAX_ELEMENT_LEN}"),
        )
            .encode(env));
    }
    let token = crate::prob_txn::MutationToken::new(mutation_index, mutation_ordinal);
    if token == crate::prob_txn::MutationToken::ZERO {
        return Ok((atoms::error(), "TopK mutation token must be non-zero").encode(env));
    }
    let file = match crate::open_random_rw_locked(Path::new(&path)) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok((atoms::error(), atoms::enoent()).encode(env));
        }
        Err(error) => return Ok((atoms::error(), format!("open: {error}")).encode(env)),
    };
    let mut updates = Vec::new();
    if updates.try_reserve_exact(elements.len()).is_err() {
        return Ok((atoms::error(), "TopK mutation input allocation failed").encode(env));
    }
    for element in &elements {
        updates.push((element.as_slice(), 1));
    }
    let results = match v2_transactional_mutation(&file, Path::new(&receipt_path), &updates, token)
    {
        Ok(results) => results,
        Err(error) => return Ok((atoms::error(), error).encode(env)),
    };
    crate::fadvise_dontneed(&file, 0, 0);
    match v2_encode_result_terms(env, results) {
        Ok(term) => Ok(term),
        Err(error) => Ok((atoms::error(), error).encode(env)),
    }
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
    if pairs.iter().any(|(_element, count)| *count <= 0) {
        return Ok((atoms::error(), "TopK increment must be positive").encode(env));
    }

    if let Some(len) = pairs
        .iter()
        .map(|(element, _count)| element.as_slice().len())
        .find(|len| *len > MAX_ELEMENT_LEN)
    {
        return Ok((
            atoms::error(),
            format!("TopK element length {len} exceeds {MAX_ELEMENT_LEN}"),
        )
            .encode(env));
    }

    let file = match crate::open_random_rw_locked(Path::new(&path)) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok((atoms::error(), atoms::enoent()).encode(env));
        }
        Err(e) => return Ok((atoms::error(), format!("open: {e}")).encode(env)),
    };

    let mut updates = Vec::new();
    if updates.try_reserve_exact(pairs.len()).is_err() {
        return Ok((atoms::error(), "TopK mutation input allocation failed").encode(env));
    }
    for (element, count) in &pairs {
        updates.push((element.as_slice(), *count));
    }
    let plan = match v2_stage_mutation(&file, &updates) {
        Ok(plan) => plan,
        Err(error) => return Ok((atoms::error(), error).encode(env)),
    };
    if let Err(error) = v2_apply_mutation_plan(&file, &plan) {
        return Ok((atoms::error(), error).encode(env));
    }

    crate::fadvise_dontneed(&file, 0, 0);
    match v2_encode_result_terms(env, plan.results) {
        Ok(term) => Ok(term),
        Err(error) => Ok((atoms::error(), error).encode(env)),
    }
}

/// Increment elements using a deterministic Raft mutation token.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn topk_file_incrby_v2_at<'a>(
    env: Env<'a>,
    path: String,
    receipt_path: String,
    pairs: Vec<(Binary<'a>, i64)>,
    mutation_index: u64,
    mutation_ordinal: u64,
) -> NifResult<Term<'a>> {
    if pairs.iter().any(|(_element, count)| *count <= 0) {
        return Ok((atoms::error(), "TopK increment must be positive").encode(env));
    }
    if let Some(len) = pairs
        .iter()
        .map(|(element, _count)| element.as_slice().len())
        .find(|len| *len > MAX_ELEMENT_LEN)
    {
        return Ok((
            atoms::error(),
            format!("TopK element length {len} exceeds {MAX_ELEMENT_LEN}"),
        )
            .encode(env));
    }
    let token = crate::prob_txn::MutationToken::new(mutation_index, mutation_ordinal);
    if token == crate::prob_txn::MutationToken::ZERO {
        return Ok((atoms::error(), "TopK mutation token must be non-zero").encode(env));
    }
    let file = match crate::open_random_rw_locked(Path::new(&path)) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok((atoms::error(), atoms::enoent()).encode(env));
        }
        Err(error) => return Ok((atoms::error(), format!("open: {error}")).encode(env)),
    };
    let mut updates = Vec::new();
    if updates.try_reserve_exact(pairs.len()).is_err() {
        return Ok((atoms::error(), "TopK mutation input allocation failed").encode(env));
    }
    for (element, count) in &pairs {
        updates.push((element.as_slice(), *count));
    }
    let results = match v2_transactional_mutation(&file, Path::new(&receipt_path), &updates, token)
    {
        Ok(results) => results,
        Err(error) => return Ok((atoms::error(), error).encode(env)),
    };
    crate::fadvise_dontneed(&file, 0, 0);
    match v2_encode_result_terms(env, results) {
        Ok(term) => Ok(term),
        Err(error) => Ok((atoms::error(), error).encode(env)),
    }
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

    let file = match crate::open_random_read_locked(p) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok((atoms::error(), atoms::enoent()).encode(env));
        }
        Err(e) => return Ok((atoms::error(), format!("open: {e}")).encode(env)),
    };

    let (k, width, depth, heap_len) = match v2_read_header(&file) {
        Ok(h) => h,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    let heap_entries = match v2_read_heap(&file, width, depth, heap_len, k) {
        Ok(h) => h,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    let fingerprints = v2_query_fingerprints(&heap_entries);

    let results: Vec<i32> = elements
        .iter()
        .map(|elem_bin| i32::from(fingerprints.contains(elem_bin.as_slice())))
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

    let file = match crate::open_random_read_locked(p) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok((atoms::error(), atoms::enoent()).encode(env));
        }
        Err(e) => return Ok((atoms::error(), format!("open: {e}")).encode(env)),
    };

    let (k, width, depth, heap_len) = match v2_read_header(&file) {
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
        let elem_bytes = entry.element.as_slice();
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

fn v2_list_with_counts(file: &File) -> Result<Vec<(Vec<u8>, i64)>, String> {
    let (k, width, depth, heap_len) = v2_read_header(file)?;
    let mut heap_entries = v2_read_heap(file, width, depth, heap_len, k)?;
    let counters = v2_read_cms(file, width, depth)?;

    heap_entries.sort_by(|a, b| {
        b.count
            .cmp(&a.count)
            .then_with(|| a.element.cmp(&b.element))
    });

    let mut result = Vec::new();
    result
        .try_reserve_exact(heap_entries.len())
        .map_err(|_| "TopK list allocation failed".to_string())?;
    for entry in heap_entries {
        let count = v2_cms_estimate(&counters, width, depth, &entry.element);
        result.push((entry.element, count));
    }
    Ok(result)
}

/// List elements and their CMS estimates from one locked file snapshot.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn topk_file_list_with_count(env: Env<'_>, path: String) -> NifResult<Term<'_>> {
    let file = match crate::open_random_read_locked(Path::new(&path)) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok((atoms::error(), atoms::enoent()).encode(env));
        }
        Err(e) => return Ok((atoms::error(), format!("open: {e}")).encode(env)),
    };

    let entries = match v2_list_with_counts(&file) {
        Ok(entries) => entries,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };
    let mut terms = Vec::new();
    if terms.try_reserve_exact(entries.len() * 2).is_err() {
        return Ok((atoms::error(), "TopK list allocation failed").encode(env));
    }
    for (element, count) in entries {
        let Some(mut binary) = OwnedBinary::new(element.len()) else {
            return Ok((atoms::error(), "out of memory").encode(env));
        };
        binary.as_mut_slice().copy_from_slice(&element);
        terms.push(Binary::from_owned(binary, env).encode(env));
        terms.push(count.encode(env));
    }

    crate::fadvise_dontneed(&file, 0, 0);
    Ok(terms.encode(env))
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

    let file = match crate::open_random_read_locked(p) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok((atoms::error(), atoms::enoent()).encode(env));
        }
        Err(e) => return Ok((atoms::error(), format!("open: {e}")).encode(env)),
    };

    let (_k, width, depth, _heap_len) = match v2_read_header(&file) {
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

/// Return metadata from a file-backed TopK: `{k, width, depth}`.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn topk_file_info_v2(env: Env, path: String) -> NifResult<Term> {
    let p = Path::new(&path);

    let file = match crate::open_random_read_locked(p) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok((atoms::error(), atoms::enoent()).encode(env));
        }
        Err(e) => return Ok((atoms::error(), format!("open: {e}")).encode(env)),
    };

    let (k, width, depth, _heap_len) = match v2_read_header(&file) {
        Ok(h) => h,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    crate::fadvise_dontneed(&file, 0, 0);
    Ok((k, width, depth).encode(env))
}

// ---------------------------------------------------------------------------
// Async variants of read NIFs — Tokio spawn_blocking, never block BEAM
// ---------------------------------------------------------------------------

/// Async topk query: spawns on Tokio, sends result to `caller_pid`.
#[rustler::nif(schedule = "DirtyCpu")]
#[allow(clippy::needless_pass_by_value)]
pub fn topk_file_query_v2_async<'a>(
    env: Env<'a>,
    caller_pid: LocalPid,
    correlation_id: u64,
    path: String,
    elements: Vec<Binary<'a>>,
) -> NifResult<Term<'a>> {
    let elements_owned: Vec<Vec<u8>> = elements.iter().map(|e| e.as_slice().to_vec()).collect();
    let blocking_task = match crate::async_io::try_spawn_blocking(move || {
        let p = std::path::Path::new(&path);
        let file = crate::open_random_read_locked(p).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                "enoent".to_string()
            } else {
                e.to_string()
            }
        })?;
        let (k, width, depth, heap_len) = v2_read_header(&file)?;
        let heap_entries = v2_read_heap(&file, width, depth, heap_len, k).map_err(|e| e.clone())?;
        let fingerprints = v2_query_fingerprints(&heap_entries);
        let results: Vec<i32> = elements_owned
            .iter()
            .map(|elem| i32::from(fingerprints.contains(elem.as_slice())))
            .collect();
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

/// Async topk list: spawns on Tokio, sends result to `caller_pid`.
/// Returns element names as a list of byte vectors (encoded as binaries).
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value)]
pub fn topk_file_list_v2_async(
    env: Env<'_>,
    caller_pid: LocalPid,
    correlation_id: u64,
    path: String,
) -> NifResult<Term<'_>> {
    let blocking_task = match crate::async_io::try_spawn_blocking(move || {
        let p = std::path::Path::new(&path);
        let file = crate::open_random_read_locked(p).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                "enoent".to_string()
            } else {
                e.to_string()
            }
        })?;
        let (k, width, depth, heap_len) = v2_read_header(&file)?;
        let mut heap_entries =
            v2_read_heap(&file, width, depth, heap_len, k).map_err(|e| e.clone())?;
        heap_entries.sort_by(|a, b| {
            b.count
                .cmp(&a.count)
                .then_with(|| a.element.cmp(&b.element))
        });
        let items: Vec<Vec<u8>> = heap_entries.iter().map(|e| e.element.clone()).collect();
        crate::fadvise_dontneed(&file, 0, 0);
        Ok(items)
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

/// Async TopK list-with-count from one locked file snapshot.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value)]
pub fn topk_file_list_with_count_async(
    env: Env<'_>,
    caller_pid: LocalPid,
    correlation_id: u64,
    path: String,
) -> NifResult<Term<'_>> {
    let blocking_task = match crate::async_io::try_spawn_blocking(move || {
        let file = crate::open_random_read_locked(Path::new(&path)).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                "enoent".to_string()
            } else {
                e.to_string()
            }
        })?;
        let result = v2_list_with_counts(&file);
        crate::fadvise_dontneed(&file, 0, 0);
        result
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
            Ok(entries) => {
                let mut terms = Vec::new();
                if terms.try_reserve_exact(entries.len() * 2).is_err() {
                    return (
                        atoms::tokio_complete(),
                        correlation_id,
                        atoms::error(),
                        "TopK list allocation failed",
                    )
                        .encode(env);
                }

                for (element, count) in entries {
                    let Some(mut binary) = OwnedBinary::new(element.len()) else {
                        return (
                            atoms::tokio_complete(),
                            correlation_id,
                            atoms::error(),
                            "out of memory",
                        )
                            .encode(env);
                    };
                    binary.as_mut_slice().copy_from_slice(&element);
                    terms.push(Binary::from_owned(binary, env).encode(env));
                    terms.push(count.encode(env));
                }

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
#[rustler::nif(schedule = "DirtyCpu")]
#[allow(clippy::needless_pass_by_value)]
pub fn topk_file_count_v2_async<'a>(
    env: Env<'a>,
    caller_pid: LocalPid,
    correlation_id: u64,
    path: String,
    elements: Vec<Binary<'a>>,
) -> NifResult<Term<'a>> {
    let elements_owned: Vec<Vec<u8>> = elements.iter().map(|e| e.as_slice().to_vec()).collect();
    let blocking_task = match crate::async_io::try_spawn_blocking(move || {
        let p = std::path::Path::new(&path);
        let file = crate::open_random_read_locked(p).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                "enoent".to_string()
            } else {
                e.to_string()
            }
        })?;
        let (_k, width, depth, _heap_len) = v2_read_header(&file)?;
        let counters = v2_read_cms(&file, width, depth).map_err(|e| e.clone())?;
        let results: Vec<i64> = elements_owned
            .iter()
            .map(|elem| v2_cms_estimate(&counters, width, depth, elem))
            .collect();
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
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value)]
pub fn topk_file_info_v2_async(
    env: Env<'_>,
    caller_pid: LocalPid,
    correlation_id: u64,
    path: String,
) -> NifResult<Term<'_>> {
    let blocking_task = match crate::async_io::try_spawn_blocking(move || {
        let p = std::path::Path::new(&path);
        let file = crate::open_random_read_locked(p).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                "enoent".to_string()
            } else {
                e.to_string()
            }
        })?;
        let (k, width, depth, _heap_len) = v2_read_header(&file)?;
        crate::fadvise_dontneed(&file, 0, 0);
        Ok((k, width, depth))
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
            Ok((k, width, depth)) => (
                atoms::tokio_complete(),
                correlation_id,
                atoms::ok(),
                (k, width, depth),
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

pub(crate) fn recover_sidecar(path: &Path) -> Result<(), String> {
    let file = crate::open_random_rw_locked(path)
        .map_err(|error| format!("open TopK sidecar for recovery: {error}"))?;
    let (k, width, depth, _heap_len) = v2_read_header(&file)?;
    let layout = topk_layout(k, width, depth)?;
    crate::prob_txn::recover(&file, path, layout.token_offset, layout.file_size)
}

// ---------------------------------------------------------------------------
// Rust-only unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    include!("sections/topk_tests.rs");
}
