//! Compaction — merges old log files into a single file, removes stale and
//! deleted entries, and writes a hint file for fast startup.
//!
//! After compaction the caller should:
//!   1. Replace the keydir entries for the compacted file IDs with the new ones.
//!   2. Delete the old data + hint files.
//!   3. Point the `Store` at the new compacted file.

use std::fs;
use std::path::{Path, PathBuf};

use crate::hint::{HintEntry, HintWriter};
use crate::keydir::KeyDir;
use crate::log::{LogReader, LogWriter};

#[derive(Debug)]
pub struct CompactionError(pub String);

impl std::fmt::Display for CompactionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CompactionError: {}", self.0)
    }
}

impl std::error::Error for CompactionError {}

impl From<crate::log::LogError> for CompactionError {
    fn from(e: crate::log::LogError) -> Self {
        CompactionError(e.to_string())
    }
}

impl From<crate::hint::HintError> for CompactionError {
    fn from(e: crate::hint::HintError) -> Self {
        CompactionError(e.to_string())
    }
}

impl From<std::io::Error> for CompactionError {
    fn from(e: std::io::Error) -> Self {
        CompactionError(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, CompactionError>;

/// Result of a compaction run.
#[derive(Debug)]
pub struct CompactionOutput {
    /// ID of the newly written merged data file.
    pub new_file_id: u64,
    /// Absolute path of the new data file.
    pub new_log_path: PathBuf,
    /// Absolute path of the new hint file.
    pub new_hint_path: PathBuf,
    /// Number of live records written to the new file.
    pub records_written: usize,
    /// Number of stale / deleted records dropped.
    pub records_dropped: usize,
}

/// Merge all log files listed in `file_ids` into a single new file.
///
/// Only records whose key is still live in `keydir` (same `file_id` + offset)
/// are copied. Expired and deleted keys are dropped.
///
/// The caller is responsible for updating the `Store`'s keydir and deleting
/// the old files after this returns successfully.
///
/// # Errors
///
/// Returns a `CompactionError` if any log file cannot be read, the new merged
/// data file or hint file cannot be written, or an I/O error occurs during
/// sync/flush.
pub fn compact(
    data_dir: &Path,
    file_ids: &[u64],
    keydir: &KeyDir,
    new_file_id: u64,
    now_ms: u64,
) -> Result<CompactionOutput> {
    // Issue 4.3: new_file_id must be strictly greater than every input file ID
    // so that startup replay processes old files before the compacted output,
    // preventing stale entries from overwriting compacted keydir entries.
    if let Some(&max_input_id) = file_ids.iter().max() {
        if new_file_id <= max_input_id {
            return Err(CompactionError(format!(
                "new_file_id ({new_file_id}) must be greater than all input file IDs (max: {max_input_id})",
            )));
        }
    }

    let new_log_path = log_path(data_dir, new_file_id);
    let new_hint_path = hint_path(data_dir, new_file_id);

    let mut writer = LogWriter::open(&new_log_path, new_file_id)?;
    let mut hint_writer = HintWriter::open(&new_hint_path)?;

    let mut records_written = 0usize;
    let mut records_dropped = 0usize;

    for &fid in file_ids {
        let source_log = log_path(data_dir, fid);
        if !source_log.exists() {
            continue;
        }

        let mut reader = LogReader::open(&source_log)?;

        // Scan metadata first so stale or expired cold values are never
        // materialized. Only offsets that are still live are read back below.
        let records = reader.iter_metadata_from_start_tolerant()?;

        for record in records {
            let is_live = keydir
                .get(&record.key)
                .is_some_and(|e| e.file_id == fid && e.offset == record.offset);

            let is_expired = record.expire_at_ms != 0 && record.expire_at_ms <= now_ms;

            if !is_live || is_expired || record.is_tombstone {
                records_dropped += 1;
                continue;
            }

            match reader.read_at(record.offset)? {
                Some(full_record) => {
                    if full_record.key != record.key {
                        return Err(CompactionError(format!(
                            "record key changed while compacting offset {}",
                            record.offset
                        )));
                    }

                    if let Some(ref value) = full_record.value {
                        let new_offset =
                            writer.write(&full_record.key, value, full_record.expire_at_ms)?;
                        hint_writer.write_entry(&HintEntry {
                            file_id: new_file_id,
                            offset: new_offset,
                            value_size: record.value_size,
                            expire_at_ms: full_record.expire_at_ms,
                            key: full_record.key,
                        })?;
                        records_written += 1;
                    } else {
                        records_dropped += 1;
                    }
                }
                None => {
                    return Err(CompactionError(format!(
                        "live record offset {} disappeared during compaction",
                        record.offset
                    )));
                }
            }
        }
    }

    writer.sync()?;
    hint_writer.commit()?;

    Ok(CompactionOutput {
        new_file_id,
        new_log_path,
        new_hint_path,
        records_written,
        records_dropped,
    })
}

/// Delete the old data and hint files after a successful compaction.
///
/// # Errors
///
/// Returns a `CompactionError` if any file cannot be removed due to an I/O error.
pub fn remove_old_files(data_dir: &Path, file_ids: &[u64]) -> Result<()> {
    for &fid in file_ids {
        let lp = log_path(data_dir, fid);
        let hp = hint_path(data_dir, fid);
        if lp.exists() {
            fs::remove_file(&lp)?;
        }
        if hp.exists() {
            fs::remove_file(&hp)?;
        }
    }
    Ok(())
}

fn log_path(data_dir: &Path, file_id: u64) -> PathBuf {
    data_dir.join(format!("{file_id:020}.log"))
}

fn hint_path(data_dir: &Path, file_id: u64) -> PathBuf {
    data_dir.join(format!("{file_id:020}.hint"))
}

#[cfg(test)]
mod tests {
    include!("sections/compaction_tests.rs");
}
