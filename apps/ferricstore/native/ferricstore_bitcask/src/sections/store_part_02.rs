/// Read-modify-write operation variants.
pub enum RmwOp {
    SetRange(u64, Vec<u8>),
    SetBit(u64, u8),
    Append(Vec<u8>),
    IncrBy(i64),
    IncrByFloat(f64),
}

fn format_float(val: f64) -> String {
    let s = format!("{val:.17}");
    if let Some(dot_pos) = s.find('.') {
        let trimmed = s.trim_end_matches('0');
        if trimmed.len() == dot_pos + 1 {
            trimmed[..dot_pos].to_string()
        } else {
            trimmed.to_string()
        }
    } else {
        s
    }
}

pub fn available_disk_space_for_path(path: &Path) -> Result<u64> {
    #[cfg(unix)]
    {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;
        let c_path =
            CString::new(path.as_os_str().as_bytes()).map_err(|e| StoreError(e.to_string()))?;
        #[allow(unsafe_code)]
        unsafe {
            let mut stat: libc::statvfs = std::mem::zeroed();
            let ret = libc::statvfs(c_path.as_ptr(), &mut stat);
            if ret != 0 {
                return Err(StoreError(format!(
                    "statvfs failed: {}",
                    std::io::Error::last_os_error()
                )));
            }
            #[allow(clippy::unnecessary_cast)]
            Ok(stat.f_bavail as u64 * stat.f_frsize as u64)
        }
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(u64::MAX)
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

fn log_path(data_dir: &Path, file_id: u64) -> PathBuf {
    // v1 uses 20-char zero-padded names, v2 uses 5-char zero-padded names.
    // Try the v2 short name first; fall back to v1 long name if it doesn't exist.
    let short = data_dir.join(format!("{file_id:05}.log"));
    if short.exists() {
        short
    } else {
        data_dir.join(format!("{file_id:020}.log"))
    }
}

fn hint_path(data_dir: &Path, file_id: u64) -> PathBuf {
    let short = data_dir.join(format!("{file_id:05}.hint"));
    if short.exists() {
        short
    } else {
        data_dir.join(format!("{file_id:020}.hint"))
    }
}

/// Scan `data_dir` for `*.log` files and return their numeric IDs.
fn collect_file_ids(data_dir: &Path) -> Result<Vec<u64>> {
    let mut ids = Vec::new();
    for entry in fs::read_dir(data_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(stem) = name.strip_suffix(".log") {
            let trimmed = stem.trim_start_matches('0');
            if trimmed.is_empty() {
                // All zeros (e.g. "00000", "00000000000000000000") => file_id 0
                ids.push(0);
            } else if let Ok(id) = trimmed.parse::<u64>() {
                ids.push(id);
            }
        }
    }
    Ok(ids)
}

/// Replay a raw log file from a given offset into the keydir.
/// Used after loading a hint file to pick up any writes appended after the hint was generated.
/// Replay a log file from a given offset. Returns the byte offset just past
/// the last valid record.
fn replay_log_from(
    log_path: &Path,
    file_id: u64,
    start_offset: u64,
    keydir: &mut KeyDir,
) -> Result<u64> {
    let mut reader = LogReader::open(log_path).map_err(|e| StoreError(e.to_string()))?;
    reader
        .seek_to(start_offset)
        .map_err(|e| StoreError(e.to_string()))?;
    let mut offset = start_offset;

    // Use tolerant iteration: stop silently at truncated/corrupt tail records.
    while let Ok(Some(record)) = reader.read_next() {
        let record_len =
            (HEADER_SIZE + record.key.len() + record.value.as_ref().map_or(0, Vec::len)) as u64;

        if let Some(value) = record.value {
            keydir.put(
                record.key.clone(),
                KeyEntry {
                    file_id,
                    offset,
                    #[allow(clippy::cast_possible_truncation)]
                    value_size: value.len() as u32,
                    expire_at_ms: record.expire_at_ms,
                    ref_bit: false,
                },
            );
        } else {
            keydir.delete(&record.key);
        }

        offset += record_len;
    }

    Ok(offset)
}

/// Replay a raw log file into the keydir (used when no hint file exists).
/// Replay a log file into the keydir. Returns the byte offset just past
/// the last valid record (i.e. the point where a new writer should start
/// appending). Any garbage or torn bytes beyond this offset are ignored.
fn replay_log(log_path: &Path, file_id: u64, keydir: &mut KeyDir) -> Result<u64> {
    let mut reader = LogReader::open(log_path).map_err(|e| StoreError(e.to_string()))?;
    let mut offset: u64 = 0;

    let records = reader
        .iter_from_start_tolerant()
        .map_err(|e| StoreError(e.to_string()))?;
    for record in records {
        let record_len =
            (HEADER_SIZE + record.key.len() + record.value.as_ref().map_or(0, Vec::len)) as u64;

        if let Some(value) = record.value {
            keydir.put(
                record.key.clone(),
                KeyEntry {
                    file_id,
                    offset,
                    #[allow(clippy::cast_possible_truncation)]
                    value_size: value.len() as u32,
                    expire_at_ms: record.expire_at_ms,
                    ref_bit: false,
                },
            );
        } else {
            keydir.delete(&record.key);
        }

        offset += record_len;
    }

    Ok(offset)
}

fn now_ms() -> u64 {
    #[allow(clippy::cast_possible_truncation)]
    // millis won't exceed u64::MAX until year 584 million
    let ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    ms
}

