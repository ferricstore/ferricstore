const MAX_SCAN_FILE_PAGE_RECORDS: usize = 65_536;

fn available_disk_space_for_path(path: &std::path::Path) -> Result<u64, String> {
    #[cfg(unix)]
    {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let path = CString::new(path.as_os_str().as_bytes()).map_err(|error| error.to_string())?;
        let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
        let result = unsafe { libc::statvfs(path.as_ptr(), stat.as_mut_ptr()) };

        if result != 0 {
            return Err(format!(
                "statvfs failed: {}",
                std::io::Error::last_os_error()
            ));
        }

        let stat = unsafe { stat.assume_init() };
        (stat.f_bavail as u64)
            .checked_mul(stat.f_frsize)
            .ok_or_else(|| "available disk space overflow".to_owned())
    }

    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(u64::MAX)
    }
}

fn validate_scan_file_page_limit(limit: usize) -> Result<usize, String> {
    if limit == 0 {
        return Err("limit must be positive".to_owned());
    }

    if limit > MAX_SCAN_FILE_PAGE_RECORDS {
        return Err(format!(
            "limit exceeds maximum {MAX_SCAN_FILE_PAGE_RECORDS}"
        ));
    }

    Ok(limit)
}

#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
fn v2_validate_value_ref<'a>(
    env: Env<'a>,
    path: String,
    offset: u64,
    expected_key: Binary,
    expected_value_size: u64,
) -> NifResult<Term<'a>> {
    match open_random_read(std::path::Path::new(&path)) {
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

/// Scan a bounded page of records in a data file from an exact byte offset.
/// `{:ok, records, next_offset, done?}`.
///
/// `done?` is true when EOF or a truncated tail was reached. This is the
/// startup recovery path because it avoids returning millions of metadata
/// tuples in a single BEAM NIF result.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
fn v2_scan_file_page<'a>(
    env: Env<'a>,
    path: String,
    start_offset: u64,
    limit: usize,
) -> NifResult<Term<'a>> {
    let limit = match validate_scan_file_page_limit(limit) {
        Ok(limit) => limit,
        Err(error) => return Ok((atoms::error(), error).encode(env)),
    };

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
const MAX_TOMBSTONE_SCAN_PAGE_RECORDS: usize = 65_536;

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

fn scan_tombstones_from_reader<R: std::io::Read + std::io::Seek>(
    reader: &mut R,
    path: &std::path::Path,
    file_len: u64,
    start_offset: u64,
    max_records: usize,
) -> Result<(Vec<TombstoneScanRecord>, u64, bool), String> {
    use std::io::SeekFrom;

    reader
        .seek(SeekFrom::Start(start_offset))
        .map_err(|e| format!("tombstone scan {path:?}:{start_offset}: failed to seek: {e}"))?;

    let mut results = Vec::new();
    let mut offset = start_offset;
    let mut scanned_records = 0usize;

    while offset < file_len && scanned_records < max_records {
        let mut header = [0u8; log::HEADER_SIZE];
        reader.read_exact(&mut header).map_err(|e| {
            format!("tombstone scan {path:?}:{offset}: unexpected EOF in header: {e}")
        })?;

        let stored_crc = u32::from_le_bytes(header[0..4].try_into().unwrap());
        let expire_at_ms = u64::from_le_bytes(header[12..20].try_into().unwrap());
        let key_size = u16::from_le_bytes(header[20..22].try_into().unwrap()) as usize;
        let value_size_raw = u32::from_le_bytes(header[22..26].try_into().unwrap());
        let is_tombstone = value_size_raw == log::TOMBSTONE;
        let value_size = log::decoded_value_size(value_size_raw, is_tombstone)
            .map_err(|e| format!("tombstone scan {path:?}:{offset}: {e}"))?;

        let mut key = vec![0u8; key_size];
        reader
            .read_exact(&mut key)
            .map_err(|e| format!("tombstone scan {path:?}:{offset}: failed to read key: {e}"))?;

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&header[4..]);
        hasher.update(&key);

        if is_tombstone {
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
            offset = offset
                .checked_add(record_size)
                .ok_or_else(|| format!("tombstone scan {path:?}:{offset}: offset overflow"))?;
        } else {
            let record_size = (log::HEADER_SIZE as u64)
                .checked_add(key_size as u64)
                .and_then(|size| size.checked_add(value_size as u64))
                .ok_or_else(|| format!("tombstone scan {path:?}:{offset}: record size overflow"))?;
            let next_offset = offset
                .checked_add(record_size)
                .ok_or_else(|| format!("tombstone scan {path:?}:{offset}: record size overflow"))?;

            if next_offset > file_len {
                return Err(format!(
                    "tombstone scan {path:?}:{offset}: unexpected EOF in value"
                ));
            }

            read_value_into_crc(
                reader,
                &mut hasher,
                value_size as u64,
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

        scanned_records += 1;
    }

    Ok((results, offset, offset == file_len))
}

fn open_tombstone_scan(
    path: &std::path::Path,
    start_offset: u64,
    max_records: usize,
) -> Result<(Vec<TombstoneScanRecord>, u64, bool), String> {
    let file = open_random_read(path).map_err(|e| e.to_string())?;
    let file_len = file.metadata().map_err(|e| e.to_string())?.len();

    if start_offset > file_len {
        return Err(format!(
            "start_offset {start_offset} exceeds file length {file_len}"
        ));
    }

    let mut reader = std::io::BufReader::new(file);
    scan_tombstones_from_reader(&mut reader, path, file_len, start_offset, max_records)
}

#[cfg(test)]
fn scan_tombstones_from_path(path: &std::path::Path) -> Result<Vec<TombstoneScanRecord>, String> {
    let (records, _next_offset, done) = open_tombstone_scan(path, 0, usize::MAX)?;

    if !done {
        return Err(format!("tombstone scan {path:?}: did not reach EOF"));
    }

    Ok(records)
}

fn scan_tombstones_page_from_path(
    path: &std::path::Path,
    start_offset: u64,
    max_records: usize,
) -> Result<(Vec<TombstoneScanRecord>, u64, bool), String> {
    if max_records == 0 {
        return Err("max_records must be positive".to_owned());
    }

    if max_records > MAX_TOMBSTONE_SCAN_PAGE_RECORDS {
        return Err(format!(
            "max_records exceeds maximum {MAX_TOMBSTONE_SCAN_PAGE_RECORDS}"
        ));
    }

    open_tombstone_scan(path, start_offset, max_records)
}

fn encode_tombstone_scan_records<'a>(
    env: Env<'a>,
    records: &[TombstoneScanRecord],
) -> Result<Vec<Term<'a>>, &'static str> {
    let mut results = Vec::new();
    results
        .try_reserve_exact(records.len())
        .map_err(|_| "out of memory allocating tombstone scan results")?;

    for record in records {
        let key_bin = match OwnedBinary::new(record.key.len()) {
            Some(mut ob) => {
                ob.as_mut_slice().copy_from_slice(&record.key);
                ob.release(env)
            }
            None => return Err("out of memory allocating key binary"),
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

    Ok(results)
}

/// Strictly scan at most `max_records` physical records for tombstones.
/// Returns `{:ok, tombstones, next_offset, done?}` where the cursor always
/// points to the next physical record and `done?` is true only at captured EOF.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
fn v2_scan_tombstones_page<'a>(
    env: Env<'a>,
    path: String,
    start_offset: u64,
    max_records: usize,
) -> NifResult<Term<'a>> {
    let p = std::path::Path::new(&path);

    match scan_tombstones_page_from_path(p, start_offset, max_records) {
        Ok((records, next_offset, done)) => match encode_tombstone_scan_records(env, &records) {
            Ok(results) => Ok((atoms::ok(), results, next_offset, done).encode(env)),
            Err(e) => Ok((atoms::error(), e).encode(env)),
        },
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
    let file = open_random_read(path).map_err(|e| e.to_string())?;
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
        let value_size = log::decoded_value_size(value_size_raw, is_tombstone)
            .map_err(|e| format!("key-state scan {path:?}:{offset}: {e}"))?
            as u64;

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

#[rustler::nif(schedule = "DirtyIo")]
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
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
fn v2_pread_batch<'a>(env: Env<'a>, path: String, locations: Vec<u64>) -> NifResult<Term<'a>> {
    let p = std::path::Path::new(&path);

    // C-2/C-6 fix: open file once, use pread for each offset
    match open_random_read(p) {
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
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
fn v2_fsync(env: Env<'_>, path: String) -> NifResult<Term<'_>> {
    let p = std::path::Path::new(&path);
    match open_write_nofollow(p) {
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
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
fn v2_fsync_dir(env: Env<'_>, path: String) -> NifResult<Term<'_>> {
    match fsync_dir(&path) {
        Ok(()) => Ok(atoms::ok().encode(env)),
        Err(msg) => Ok((atoms::error(), msg).encode(env)),
    }
}

/// Returns available bytes for the filesystem containing `path`.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
fn v2_available_disk_space(env: Env<'_>, path: String) -> NifResult<Term<'_>> {
    match available_disk_space_for_path(std::path::Path::new(&path)) {
        Ok(bytes) => Ok((atoms::ok(), bytes).encode(env)),
        Err(error) => Ok((atoms::error(), error).encode(env)),
    }
}

/// Write a hint file from a list of entries.
/// Each entry is `{key, file_id, offset, value_size, expire_at_ms}`.
/// Returns `:ok` or `{:error, reason}`.
#[rustler::nif(schedule = "DirtyIo")]
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

/// Read a bounded page of hint entries from an exact byte offset.
/// Returns `{:ok, entries, next_offset, done}`.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
fn v2_read_hint_file_page<'a>(
    env: Env<'a>,
    path: String,
    start_offset: u64,
    max_entries: usize,
    max_bytes: usize,
) -> NifResult<Term<'a>> {
    let p = std::path::Path::new(&path);

    match hint::HintReader::open(p) {
        Ok(mut reader) => match reader.read_page(start_offset, max_entries, max_bytes) {
            Ok((entries, next_offset, done)) => {
                let mut results: Vec<Term<'a>> = Vec::with_capacity(entries.len());

                for entry in &entries {
                    let key_bin = match OwnedBinary::new(entry.key.len()) {
                        Some(mut owned) => {
                            owned.as_mut_slice().copy_from_slice(&entry.key);
                            owned.release(env)
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
                            entry.file_id,
                            entry.offset,
                            entry.value_size,
                            entry.expire_at_ms,
                        )
                            .encode(env),
                    );
                }

                Ok((atoms::ok(), results, next_offset, done).encode(env))
            }
            Err(error) => Ok((atoms::error(), error.to_string()).encode(env)),
        },
        Err(error) => Ok((atoms::error(), error.to_string()).encode(env)),
    }
}

/// Build a hint for a sealed log without materializing values or a BEAM list.
/// Tombstones remain in the log and are replayed separately during recovery.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
fn v2_build_hint_file_from_log<'a>(
    env: Env<'a>,
    log_path: String,
    hint_path: String,
    file_id: u64,
) -> NifResult<Term<'a>> {
    let result = (|| -> std::result::Result<(u64, u64), String> {
        let mut reader = log::LogReader::open(std::path::Path::new(&log_path))
            .map_err(|error| error.to_string())?;
        let mut writer = hint::HintWriter::open(std::path::Path::new(&hint_path))
            .map_err(|error| error.to_string())?;
        let mut offset = 0u64;
        let mut entry_count = 0u64;

        loop {
            if let Some(record) = reader
                .read_next_metadata(offset)
                .map_err(|error| error.to_string())?
            {
                offset = offset
                    .checked_add(record.record_size)
                    .ok_or_else(|| "hint builder offset overflow".to_string())?;

                if !record.is_tombstone {
                    writer
                        .write_entry(&hint::HintEntry {
                            file_id,
                            offset: record.offset,
                            value_size: record.value_size,
                            expire_at_ms: record.expire_at_ms,
                            key: record.key,
                        })
                        .map_err(|error| error.to_string())?;
                    entry_count = entry_count.saturating_add(1);
                }
            } else {
                writer.commit().map_err(|error| error.to_string())?;
                return Ok((entry_count, offset));
            }
        }
    })();

    match result {
        Ok((entry_count, end_offset)) => {
            Ok((atoms::ok(), entry_count, end_offset).encode(env))
        }
        Err(error) => Ok((atoms::error(), error).encode(env)),
    }
}

/// Copy specified records from a source file to a destination file.
/// Returns `{:ok, [{new_offset, new_size}, ...]}`.
///
/// Used by compaction to copy only live records to a new file.
#[rustler::nif(schedule = "DirtyIo")]
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

    match copy_live_records_impl(src, dst, dest_file_id, &offsets) {
        Ok(results) => Ok((atoms::ok(), results).encode(env)),
        Err(reason) => Ok((atoms::error(), reason).encode(env)),
    }
}

fn copy_live_records_impl(
    src: &std::path::Path,
    dst: &std::path::Path,
    dest_file_id: u64,
    offsets: &[u64],
) -> std::result::Result<Vec<(u64, u64)>, String> {
    let unique_offsets: std::collections::HashSet<u64> = offsets.iter().copied().collect();
    if unique_offsets.len() != offsets.len() {
        return Err("duplicate live offset in compaction request".to_owned());
    }

    let file = open_random_read(src).map_err(|error| error.to_string())?;
    let mut writer = log::LogWriter::open(dst, dest_file_id).map_err(|error| error.to_string())?;
    let mut results = Vec::new();
    results
        .try_reserve_exact(offsets.len())
        .map_err(|_| "out of memory allocating compaction copy results".to_owned())?;

    for &offset in offsets {
        let copied = log::copy_live_record_raw_from_file(&file, &mut writer, offset)
            .map_err(|error| error.to_string())?;
        results.push((copied.offset, copied.record_size));
    }

    writer.sync().map_err(|error| error.to_string())?;
    Ok(results)
}

/// Copy live records and tombstones into a replacement log.
///
/// Returns `{:ok, [{new_offset, new_size}, ...]}` for live offsets only, in
/// the same order as `live_offsets`. Tombstones are copied in source-offset
/// order so replay still suppresses older values after compaction.
#[rustler::nif(schedule = "DirtyIo")]
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
    let live_set: std::collections::HashSet<u64> = live_offsets.iter().copied().collect();
    let tombstone_set: std::collections::HashSet<u64> =
        tombstone_offsets.iter().copied().collect();

    if live_set.len() != live_offsets.len() {
        return Err("duplicate live offset in compaction request".to_owned());
    }

    if tombstone_set.len() != tombstone_offsets.len() {
        return Err("duplicate tombstone offset in compaction request".to_owned());
    }

    if let Some(conflicting_offset) = live_set.intersection(&tombstone_set).next() {
        return Err(format!(
            "offset {conflicting_offset} requested as both live and tombstone"
        ));
    }

    let file = open_random_read(src).map_err(|e| e.to_string())?;
    let mut writer = log::LogWriter::open(dst, dest_file_id).map_err(|e| e.to_string())?;

    let mut all_offsets = live_offsets.to_vec();
    all_offsets.extend(tombstone_offsets);
    all_offsets.sort_unstable();
    all_offsets.dedup();

    let mut live_results: std::collections::HashMap<u64, (u64, u64)> =
        std::collections::HashMap::with_capacity(live_offsets.len());

    for offset in all_offsets {
        match log::copy_record_raw_from_file(&file, &mut writer, offset, true) {
            Ok(Some(copied)) => {
                if live_set.contains(&offset) {
                    if copied.is_tombstone {
                        return Err(format!(
                            "requested live offset {offset} contained a tombstone; expected live record"
                        ));
                    }
                    live_results.insert(offset, (copied.offset, copied.record_size));
                } else if !copied.is_tombstone {
                    return Err(format!(
                        "requested tombstone offset {offset} contained a live record; expected tombstone"
                    ));
                }
            }
            Ok(None) if live_set.contains(&offset) => {
                return Err(format!("requested live offset {offset} was not found"));
            }
            Ok(None) => {
                return Err(format!(
                    "requested tombstone offset {offset} was not found"
                ));
            }
            Err(e) => return Err(e.to_string()),
        }
    }

    writer.sync().map_err(|e| e.to_string())?;

    live_offsets
        .iter()
        .map(|offset| {
            live_results.get(offset).copied().ok_or_else(|| {
                format!("requested live offset {offset} produced no copy result")
            })
        })
        .collect()
}
