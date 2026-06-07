#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value)]
fn v2_validate_value_ref<'a>(
    env: Env<'a>,
    path: String,
    offset: u64,
    expected_key: Binary,
    expected_value_size: u64,
) -> NifResult<Term<'a>> {
    match std::fs::File::open(std::path::Path::new(&path)) {
        Ok(file) => match validate_value_ref_from_file(
            &file,
            offset,
            expected_key.as_slice(),
            expected_value_size,
        ) {
            Ok(Some((value_offset, value_size))) => {
                Ok((atoms::ok(), (value_offset, value_size)).encode(env))
            }
            Ok(None) => Ok(atoms::mismatch().encode(env)),
            Err(e) => Ok((atoms::error(), e).encode(env)),
        },
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

/// Scan all records in a data file. Returns a list of record metadata.
/// `{:ok, [{key, offset, value_size, expire_at_ms, is_tombstone}, ...]}`.
///
/// Used by compaction and crash recovery to rebuild the ETS keydir.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value)]
fn v2_scan_file<'a>(env: Env<'a>, path: String) -> NifResult<Term<'a>> {
    let p = std::path::Path::new(&path);

    match log::LogReader::open(p) {
        Ok(mut reader) => {
            let records = reader
                .iter_metadata_from_start_tolerant()
                .map_err(|e| rustler::Error::Term(Box::new(e.to_string())))?;
            match encode_scan_records(env, &records) {
                Ok(results) => Ok((atoms::ok(), results).encode(env)),
                Err(e) => Ok((atoms::error(), e).encode(env)),
            }
        }
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

/// Scan a bounded page of records in a data file from an exact byte offset.
/// `{:ok, records, next_offset, done?}`.
///
/// `done?` has the same tolerant crash-recovery meaning as `v2_scan_file/1`:
/// true means EOF or a truncated/corrupt tail was reached. This is the startup
/// recovery hot path because it avoids returning millions of metadata tuples in
/// a single BEAM NIF result.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value)]
fn v2_scan_file_page<'a>(
    env: Env<'a>,
    path: String,
    start_offset: u64,
    limit: usize,
) -> NifResult<Term<'a>> {
    if limit == 0 {
        return Ok((atoms::error(), "limit must be positive").encode(env));
    }

    let p = std::path::Path::new(&path);

    match log::LogReader::open(p) {
        Ok(mut reader) => match reader.iter_metadata_page_from_offset_tolerant(start_offset, limit)
        {
            Ok((records, next_offset, done)) => match encode_scan_records(env, &records) {
                Ok(results) => Ok((atoms::ok(), results, next_offset, done).encode(env)),
                Err(e) => Ok((atoms::error(), e).encode(env)),
            },
            Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
        },
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

/// Scan records in a data file from an exact byte offset. Returns a list of record metadata.
/// `{:ok, [{key, offset, value_size, expire_at_ms, is_tombstone}, ...]}`.
///
/// Used by hint recovery to replay only active-file records appended after the
/// hint boundary.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value)]
fn v2_scan_file_from_offset<'a>(
    env: Env<'a>,
    path: String,
    start_offset: u64,
) -> NifResult<Term<'a>> {
    let p = std::path::Path::new(&path);

    match log::LogReader::open(p) {
        Ok(mut reader) => {
            let records = reader
                .iter_metadata_from_offset_tolerant(start_offset)
                .map_err(|e| rustler::Error::Term(Box::new(e.to_string())))?;

            match encode_scan_records(env, &records) {
                Ok(results) => Ok((atoms::ok(), results).encode(env)),
                Err(e) => Ok((atoms::error(), e).encode(env)),
            }
        }
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

fn encode_scan_records<'a>(
    env: Env<'a>,
    records: &[log::RecordMetadata],
) -> Result<Vec<Term<'a>>, &'static str> {
    let mut results: Vec<Term<'a>> = Vec::with_capacity(records.len());

    for record in records {
        let key_bin = match OwnedBinary::new(record.key.len()) {
            Some(mut ob) => {
                ob.as_mut_slice().copy_from_slice(&record.key);
                ob.release(env)
            }
            None => {
                return Err("out of memory allocating key binary");
            }
        };

        results.push(
            (
                key_bin,
                record.offset,
                record.value_size,
                record.expire_at_ms,
                record.is_tombstone,
            )
                .encode(env),
        );
    }

    Ok(results)
}

/// Scan only tombstone metadata from a data file.
/// `{:ok, [{key, offset, record_size, expire_at_ms}, ...]}`.
///
/// Used during hint recovery. Hint files contain only live entries, so startup
/// must still apply tombstones from hinted logs. This scanner skips live value
/// payloads instead of materializing them, preserving fast cold-value recovery.
#[derive(Debug, PartialEq, Eq)]
struct TombstoneScanRecord {
    key: Vec<u8>,
    offset: u64,
    record_size: u64,
    expire_at_ms: u64,
}

const SCAN_VALUE_HASH_CHUNK_SIZE: usize = 64 * 1024;

fn read_value_into_crc<R: std::io::Read>(
    reader: &mut R,
    hasher: &mut crc32fast::Hasher,
    mut remaining: u64,
    context: &str,
) -> Result<(), String> {
    let mut buf = [0u8; SCAN_VALUE_HASH_CHUNK_SIZE];

    while remaining > 0 {
        let read_len = remaining.min(buf.len() as u64) as usize;
        reader
            .read_exact(&mut buf[..read_len])
            .map_err(|e| format!("{context}: unexpected EOF in value: {e}"))?;
        hasher.update(&buf[..read_len]);
        remaining -= read_len as u64;
    }

    Ok(())
}

fn scan_tombstones_from_path(path: &std::path::Path) -> Result<Vec<TombstoneScanRecord>, String> {
    use std::io::Read;

    let file = std::fs::File::open(path).map_err(|e| e.to_string())?;
    let file_len = file.metadata().map_err(|e| e.to_string())?.len();
    let mut reader = std::io::BufReader::new(file);
    let mut results = Vec::new();
    let mut offset: u64 = 0;

    while offset < file_len {
        let mut header = [0u8; log::HEADER_SIZE];
        reader.read_exact(&mut header).map_err(|e| {
            format!("tombstone scan {path:?}:{offset}: unexpected EOF in header: {e}")
        })?;

        let stored_crc = u32::from_le_bytes(header[0..4].try_into().unwrap());
        let expire_at_ms = u64::from_le_bytes(header[12..20].try_into().unwrap());
        let key_size = u16::from_le_bytes(header[20..22].try_into().unwrap()) as usize;
        let value_size_raw = u32::from_le_bytes(header[22..26].try_into().unwrap());

        let mut key = vec![0u8; key_size];
        reader
            .read_exact(&mut key)
            .map_err(|e| format!("tombstone scan {path:?}:{offset}: failed to read key: {e}"))?;

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&header[4..]);
        hasher.update(&key);

        if value_size_raw == log::TOMBSTONE {
            let actual_crc = hasher.finalize();

            if actual_crc != stored_crc {
                return Err(format!(
                    "tombstone scan {path:?}:{offset}: CRC mismatch stored={stored_crc} actual={actual_crc}"
                ));
            }

            let record_size = (log::HEADER_SIZE + key.len()) as u64;
            results.push(TombstoneScanRecord {
                key,
                offset,
                record_size,
                expire_at_ms,
            });
            offset += record_size;
        } else {
            let record_size = log::HEADER_SIZE as u64 + key_size as u64 + u64::from(value_size_raw);
            let next_offset = offset
                .checked_add(record_size)
                .ok_or_else(|| format!("tombstone scan {path:?}:{offset}: record size overflow"))?;

            if next_offset > file_len {
                return Err(format!(
                    "tombstone scan {path:?}:{offset}: unexpected EOF in value"
                ));
            }

            read_value_into_crc(
                &mut reader,
                &mut hasher,
                u64::from(value_size_raw),
                &format!("tombstone scan {path:?}:{offset}"),
            )?;

            let actual_crc = hasher.finalize();

            if actual_crc != stored_crc {
                return Err(format!(
                    "tombstone scan {path:?}:{offset}: CRC mismatch stored={stored_crc} actual={actual_crc}"
                ));
            }

            offset = next_offset;
        }
    }

    Ok(results)
}

#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value)]
fn v2_scan_tombstones<'a>(env: Env<'a>, path: String) -> NifResult<Term<'a>> {
    let p = std::path::Path::new(&path);

    match scan_tombstones_from_path(p) {
        Ok(records) => {
            let mut results: Vec<Term<'a>> = Vec::new();

            for record in records {
                let key_bin = match OwnedBinary::new(record.key.len()) {
                    Some(mut ob) => {
                        ob.as_mut_slice().copy_from_slice(&record.key);
                        ob.release(env)
                    }
                    None => {
                        return Ok(
                            (atoms::error(), "out of memory allocating key binary").encode(env)
                        );
                    }
                };

                results.push(
                    (
                        key_bin,
                        record.offset,
                        record.record_size,
                        record.expire_at_ms,
                    )
                        .encode(env),
                );
            }

            Ok((atoms::ok(), results).encode(env))
        }
        Err(e) => Ok((atoms::error(), e).encode(env)),
    }
}

/// Scan the newest state for a bounded set of keys in one file.
/// `{:ok, [{key, expire_at_ms, is_tombstone}, ...]}`.
///
/// Used by compaction tombstone-dependency checks. It reads headers and keys
/// and seeks over live values instead of hashing payload bytes; the caller only
/// needs to know whether a lower file contains a live, expired, or tombstone
/// state for each masked key.
#[derive(Debug, PartialEq, Eq)]
struct KeyStateScanRecord {
    key: Vec<u8>,
    expire_at_ms: u64,
    is_tombstone: bool,
}

fn scan_key_states_from_path(
    path: &std::path::Path,
    masked_keys: &[Vec<u8>],
) -> Result<Vec<KeyStateScanRecord>, String> {
    use std::collections::{HashMap, HashSet};
    use std::io::Read;

    if masked_keys.is_empty() {
        return Ok(Vec::new());
    }

    let targets: HashSet<Vec<u8>> = masked_keys.iter().cloned().collect();
    let file = std::fs::File::open(path).map_err(|e| e.to_string())?;
    let file_len = file.metadata().map_err(|e| e.to_string())?.len();
    let mut reader = std::io::BufReader::new(file);
    let mut states: HashMap<Vec<u8>, (u64, bool)> = HashMap::new();
    let mut offset: u64 = 0;

    while offset < file_len {
        let mut header = [0u8; log::HEADER_SIZE];

        match reader.read_exact(&mut header) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Err(format!(
                    "key-state scan {path:?}:{offset}: unexpected EOF in header"
                ));
            }
            Err(e) => return Err(format!("key-state scan {path:?}:{offset}: {e}")),
        }

        let stored_crc = u32::from_le_bytes(header[0..4].try_into().unwrap());
        let expire_at_ms = u64::from_le_bytes(header[12..20].try_into().unwrap());
        let key_size = u16::from_le_bytes(header[20..22].try_into().unwrap()) as usize;
        let value_size_raw = u32::from_le_bytes(header[22..26].try_into().unwrap());
        let is_tombstone = value_size_raw == log::TOMBSTONE;

        let mut key = vec![0u8; key_size];
        match reader.read_exact(&mut key) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Err(format!(
                    "key-state scan {path:?}:{offset}: unexpected EOF in key"
                ));
            }
            Err(e) => return Err(format!("key-state scan {path:?}:{offset}: {e}")),
        }

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&header[4..]);
        hasher.update(&key);

        let value_size = if is_tombstone {
            0_u64
        } else {
            u64::from(value_size_raw)
        };

        let record_size = log::HEADER_SIZE as u64 + key_size as u64 + value_size;
        let next_offset = offset
            .checked_add(record_size)
            .ok_or_else(|| format!("key-state scan {path:?}:{offset}: record offset overflow"))?;

        if next_offset > file_len {
            return Err(format!(
                "key-state scan {path:?}:{offset}: unexpected EOF in value"
            ));
        }

        read_value_into_crc(
            &mut reader,
            &mut hasher,
            value_size,
            &format!("key-state scan {path:?}:{offset}"),
        )?;

        let computed_crc = hasher.finalize();

        if computed_crc != stored_crc {
            return Err(format!(
                "key-state scan {path:?}:{offset}: CRC mismatch stored={stored_crc} actual={computed_crc}"
            ));
        }

        if targets.contains(&key) {
            states.insert(key.clone(), (expire_at_ms, is_tombstone));
        }

        offset = next_offset;
    }

    Ok(states
        .into_iter()
        .map(|(key, (expire_at_ms, is_tombstone))| KeyStateScanRecord {
            key,
            expire_at_ms,
            is_tombstone,
        })
        .collect())
}

#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value)]
fn v2_scan_key_states<'a>(
    env: Env<'a>,
    path: String,
    masked_keys: Vec<Binary>,
) -> NifResult<Term<'a>> {
    let p = std::path::Path::new(&path);
    let keys: Vec<Vec<u8>> = masked_keys
        .iter()
        .map(|key| key.as_slice().to_vec())
        .collect();

    match scan_key_states_from_path(p, &keys) {
        Ok(records) => {
            let mut results: Vec<Term<'a>> = Vec::with_capacity(records.len());

            for record in records {
                let key_bin = match OwnedBinary::new(record.key.len()) {
                    Some(mut ob) => {
                        ob.as_mut_slice().copy_from_slice(&record.key);
                        ob.release(env)
                    }
                    None => {
                        return Ok(
                            (atoms::error(), "out of memory allocating key binary").encode(env)
                        );
                    }
                };

                results.push((key_bin, record.expire_at_ms, record.is_tombstone).encode(env));
            }

            Ok((atoms::ok(), results).encode(env))
        }
        Err(e) => Ok((atoms::error(), e).encode(env)),
    }
}

/// Batch pread: read values at multiple offsets from the same file.
/// Returns `{:ok, [value_binary | nil, ...]}`.
///
/// L-7 fix: sort offsets ascending before reading so the kernel's readahead
/// benefits sequential access patterns. Results are re-ordered to match the
/// original `locations` order before returning.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value)]
fn v2_pread_batch<'a>(env: Env<'a>, path: String, locations: Vec<u64>) -> NifResult<Term<'a>> {
    let p = std::path::Path::new(&path);

    // C-2/C-6 fix: open file once, use pread for each offset
    match std::fs::File::open(p) {
        Ok(file) => {
            fadvise_random(&file);
            let n = locations.len();

            // Build (original_index, offset) pairs and sort by offset for
            // sequential disk access.
            let mut sorted: Vec<(usize, u64)> = locations.iter().copied().enumerate().collect();
            sorted.sort_unstable_by_key(|&(_, off)| off);

            // Read in sorted (ascending offset) order.
            let mut slot_results: Vec<Option<Term<'a>>> = vec![None; n];
            let nil = atoms::nil().encode(env);

            for &(orig_idx, offset) in &sorted {
                let term = match log::pread_record_from_file(&file, offset) {
                    Ok(Some(record)) => {
                        fadvise_dontneed(
                            &file,
                            offset as i64,
                            (log::HEADER_SIZE
                                + record.key.len()
                                + record.value.as_ref().map_or(0, Vec::len))
                                as i64,
                        );
                        match record.value {
                            Some(value) => {
                                let resource = ResourceArc::new(ValueBuffer { data: value });
                                resource.make_binary(env, |vb| &vb.data).encode(env)
                            }
                            None => nil,
                        }
                    }
                    Ok(None) => nil,
                    Err(e) => return Ok((atoms::error(), e.to_string()).encode(env)),
                };
                slot_results[orig_idx] = Some(term);
            }

            // Unwrap results back to original order.
            let results: Vec<Term<'a>> =
                slot_results.into_iter().map(|t| t.unwrap_or(nil)).collect();

            Ok((atoms::ok(), results).encode(env))
        }
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

/// Fsync a data file. Returns `:ok` or `{:error, reason}`.
///
/// L-REMAIN-1 fix: open with write permission so `sync_data()` (fdatasync)
/// actually flushes dirty pages written by other fds. `File::open()` opens
/// read-only, and `fdatasync()` on a read-only fd is a no-op per POSIX.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value)]
fn v2_fsync(env: Env<'_>, path: String) -> NifResult<Term<'_>> {
    let p = std::path::Path::new(&path);
    match std::fs::OpenOptions::new().write(true).open(p) {
        // C-7 fix: use sync_data (fdatasync) instead of sync_all (fsync)
        Ok(f) => match f.sync_data() {
            Ok(()) => Ok(atoms::ok().encode(env)),
            Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
        },
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

/// Fsync a directory so that recent `create` / `rename` / `remove_file`
/// operations inside it are durable. See `fsync_dir` doc for details.
///
/// Returns `:ok` or `{:error, reason}`.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value)]
fn v2_fsync_dir(env: Env<'_>, path: String) -> NifResult<Term<'_>> {
    match fsync_dir(&path) {
        Ok(()) => Ok(atoms::ok().encode(env)),
        Err(msg) => Ok((atoms::error(), msg).encode(env)),
    }
}

/// Returns available bytes for the filesystem containing `path`.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value)]
fn v2_available_disk_space(env: Env<'_>, path: String) -> NifResult<Term<'_>> {
    match store::available_disk_space_for_path(std::path::Path::new(&path)) {
        Ok(bytes) => Ok((atoms::ok(), bytes).encode(env)),
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

/// Write a hint file from a list of entries.
/// Each entry is `{key, file_id, offset, value_size, expire_at_ms}`.
/// Returns `:ok` or `{:error, reason}`.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value)]
fn v2_write_hint_file<'a>(
    env: Env<'a>,
    path: String,
    entries: Vec<(Binary<'a>, u64, u64, u32, u64)>,
) -> NifResult<Term<'a>> {
    let p = std::path::Path::new(&path);

    match hint::HintWriter::open(p) {
        Ok(mut writer) => {
            for (key, file_id, offset, value_size, expire_at_ms) in &entries {
                let entry = hint::HintEntry {
                    file_id: *file_id,
                    offset: *offset,
                    value_size: *value_size,
                    expire_at_ms: *expire_at_ms,
                    key: key.as_slice().to_vec(),
                };
                if let Err(e) = writer.write_entry(&entry) {
                    return Ok((atoms::error(), e.to_string()).encode(env));
                }
            }
            match writer.commit() {
                Ok(()) => Ok(atoms::ok().encode(env)),
                Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
            }
        }
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

/// Read a hint file and return all entries.
/// Returns `{:ok, [{key, file_id, offset, value_size, expire_at_ms}, ...]}`.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value)]
fn v2_read_hint_file<'a>(env: Env<'a>, path: String) -> NifResult<Term<'a>> {
    let p = std::path::Path::new(&path);

    match hint::HintReader::open(p) {
        Ok(mut reader) => match reader.read_all() {
            Ok(entries) => {
                let mut results: Vec<Term<'a>> = Vec::with_capacity(entries.len());
                for entry in &entries {
                    // M-REMAIN-1 fix: handle OOM gracefully instead of panicking.
                    let key_bin = match OwnedBinary::new(entry.key.len()) {
                        Some(mut ob) => {
                            ob.as_mut_slice().copy_from_slice(&entry.key);
                            ob.release(env)
                        }
                        None => {
                            return Ok(
                                (atoms::error(), "out of memory allocating key binary").encode(env)
                            );
                        }
                    };
                    let tuple = (
                        key_bin,
                        entry.file_id,
                        entry.offset,
                        entry.value_size,
                        entry.expire_at_ms,
                    )
                        .encode(env);
                    results.push(tuple);
                }
                Ok((atoms::ok(), results).encode(env))
            }
            Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
        },
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

/// Copy specified records from a source file to a destination file.
/// Returns `{:ok, [{new_offset, new_size}, ...]}`.
///
/// Used by compaction to copy only live records to a new file.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value)]
fn v2_copy_records(
    env: Env<'_>,
    source_path: String,
    dest_path: String,
    offsets: Vec<u64>,
) -> NifResult<Term<'_>> {
    let src = std::path::Path::new(&source_path);
    let dst = std::path::Path::new(&dest_path);

    let dest_file_id = parse_file_id(dst);

    match std::fs::File::open(src) {
        Ok(file) => match log::LogWriter::open(dst, dest_file_id) {
            Ok(mut writer) => {
                let mut results: Vec<(u64, u64)> = Vec::with_capacity(offsets.len());

                for &offset in &offsets {
                    match log::copy_record_raw_from_file(&file, &mut writer, offset, false) {
                        Ok(Some(copied)) => results.push((copied.offset, copied.record_size)),
                        Ok(None) => {
                            // Offset past EOF or tombstone -- skip.
                        }
                        Err(e) => {
                            return Ok((atoms::error(), e.to_string()).encode(env));
                        }
                    }
                }

                writer
                    .sync()
                    .map_err(|e| rustler::Error::Term(Box::new(e.to_string())))?;
                Ok((atoms::ok(), results).encode(env))
            }
            Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
        },
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

/// Copy live records and tombstones into a replacement log.
///
/// Returns `{:ok, [{new_offset, new_size}, ...]}` for live offsets only, in
/// the same order as `live_offsets`. Tombstones are copied in source-offset
/// order so replay still suppresses older values after compaction.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value)]
fn v2_copy_records_preserve_tombstones(
    env: Env<'_>,
    source_path: String,
    dest_path: String,
    live_offsets: Vec<u64>,
    tombstone_offsets: Vec<u64>,
) -> NifResult<Term<'_>> {
    let src = std::path::Path::new(&source_path);
    let dst = std::path::Path::new(&dest_path);

    let dest_file_id = parse_file_id(dst);

    match copy_records_preserve_tombstones_impl(
        src,
        dst,
        dest_file_id,
        &live_offsets,
        &tombstone_offsets,
    ) {
        Ok(results) => Ok((atoms::ok(), results).encode(env)),
        Err(reason) => Ok((atoms::error(), reason).encode(env)),
    }
}

fn copy_records_preserve_tombstones_impl(
    src: &std::path::Path,
    dst: &std::path::Path,
    dest_file_id: u64,
    live_offsets: &[u64],
    tombstone_offsets: &[u64],
) -> std::result::Result<Vec<(u64, u64)>, String> {
    let file = std::fs::File::open(src).map_err(|e| e.to_string())?;
    let mut writer = log::LogWriter::open(dst, dest_file_id).map_err(|e| e.to_string())?;

    let live_set: std::collections::HashSet<u64> = live_offsets.iter().copied().collect();
    let mut all_offsets = live_offsets.to_vec();
    all_offsets.extend(tombstone_offsets);
    all_offsets.sort_unstable();
    all_offsets.dedup();

    let mut live_results: std::collections::HashMap<u64, (u64, u64)> =
        std::collections::HashMap::with_capacity(live_offsets.len());

    for offset in all_offsets {
        match log::copy_record_raw_from_file(&file, &mut writer, offset, true) {
            Ok(Some(copied)) => {
                if live_set.contains(&offset) && !copied.is_tombstone {
                    live_results.insert(offset, (copied.offset, copied.record_size));
                }
            }
            Ok(None) => {}
            Err(e) => return Err(e.to_string()),
        }
    }

    writer.sync().map_err(|e| e.to_string())?;

    Ok(live_offsets
        .iter()
        .filter_map(|offset| live_results.get(offset).copied())
        .collect())
}
