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
    use super::*;
    use crate::hint::HintReader;
    use crate::keydir::KeyEntry;
    use crate::log::LogWriter;
    use tempfile::TempDir;

    fn tmp() -> TempDir {
        tempfile::TempDir::new().unwrap()
    }

    fn make_log(dir: &Path, file_id: u64, writes: &[(&[u8], &[u8])]) -> u64 {
        let path = log_path(dir, file_id);
        let mut w = LogWriter::open(&path, file_id).unwrap();
        let mut last_offset = 0u64;
        for (k, v) in writes {
            last_offset = w.write(k, v, 0).unwrap();
        }
        w.sync().unwrap();
        last_offset
    }

    #[test]
    fn compact_single_file_no_stale() {
        let dir = tmp();
        let last_offset = make_log(dir.path(), 1, &[(b"a", b"1"), (b"b", b"2")]);

        // Build a keydir pointing to the latest records
        use crate::log::HEADER_SIZE;
        let off_a = 0u64;
        let off_b = (HEADER_SIZE + 1 + 1) as u64; // after first record

        let mut kd = KeyDir::new();
        kd.put(
            b"a".to_vec(),
            KeyEntry {
                file_id: 1,
                offset: off_a,
                value_size: 1,
                expire_at_ms: 0,
                ref_bit: false,
            },
        );
        kd.put(
            b"b".to_vec(),
            KeyEntry {
                file_id: 1,
                offset: off_b,
                value_size: 1,
                expire_at_ms: 0,
                ref_bit: false,
            },
        );

        let out = compact(dir.path(), &[1], &kd, 99, 0).unwrap();

        assert_eq!(out.records_written, 2);
        assert_eq!(out.records_dropped, 0);
        assert!(out.new_log_path.exists());
        assert!(out.new_hint_path.exists());

        // Verify hint file has 2 entries
        let mut hr = HintReader::open(&out.new_hint_path).unwrap();
        let hints = hr.read_all().unwrap();
        assert_eq!(hints.len(), 2);
        let _ = last_offset; // suppress unused warning
    }

    #[test]
    fn compact_drops_overwritten_entries() {
        let dir = tmp();
        // Write "k" twice — only the second write is live
        use crate::log::HEADER_SIZE;
        let off_v1 = 0u64;
        let off_v2 = (HEADER_SIZE + 1 + 2) as u64; // after "k"/"v1" record

        make_log(dir.path(), 1, &[(b"k", b"v1"), (b"k", b"v2")]);

        let mut kd = KeyDir::new();
        kd.put(
            b"k".to_vec(),
            KeyEntry {
                file_id: 1,
                offset: off_v2,
                value_size: 2,
                expire_at_ms: 0,
                ref_bit: false,
            },
        );
        let _ = off_v1;

        let out = compact(dir.path(), &[1], &kd, 99, 0).unwrap();

        assert_eq!(out.records_written, 1, "only latest write survives");
        assert_eq!(out.records_dropped, 1, "stale write is dropped");
    }

    #[test]
    fn compact_drops_deleted_entries() {
        let dir = tmp();
        // Write "k", then delete it — keydir has no entry for "k"
        let path = log_path(dir.path(), 1);
        let mut w = LogWriter::open(&path, 1).unwrap();
        w.write(b"k", b"v", 0).unwrap();
        w.write_tombstone(b"k").unwrap();
        w.sync().unwrap();

        let kd = KeyDir::new(); // empty — key is deleted

        let out = compact(dir.path(), &[1], &kd, 99, 0).unwrap();

        assert_eq!(out.records_written, 0);
        assert_eq!(out.records_dropped, 2); // both the write and the tombstone
    }

    #[test]
    fn compact_drops_expired_entries() {
        let dir = tmp();
        let path = log_path(dir.path(), 1);
        let mut w = LogWriter::open(&path, 1).unwrap();
        w.write(b"ttl", b"val", 1000).unwrap(); // expires at ms=1000
        w.sync().unwrap();

        use crate::log::HEADER_SIZE;
        let mut kd = KeyDir::new();
        kd.put(
            b"ttl".to_vec(),
            KeyEntry {
                file_id: 1,
                offset: 0,
                value_size: 3,
                expire_at_ms: 1000,
                ref_bit: false,
            },
        );

        let out = compact(dir.path(), &[1], &kd, 99, 5000).unwrap(); // now_ms=5000 > 1000
        let _ = HEADER_SIZE;

        assert_eq!(out.records_written, 0);
        assert_eq!(out.records_dropped, 1);
    }

    #[test]
    fn compact_merges_multiple_files() {
        let dir = tmp();
        use crate::log::HEADER_SIZE;

        // file 1: "a"="1"
        make_log(dir.path(), 1, &[(b"a", b"1")]);
        // file 2: "b"="2"
        make_log(dir.path(), 2, &[(b"b", b"2")]);

        let off_a = 0u64;
        let off_b = 0u64;

        let mut kd = KeyDir::new();
        kd.put(
            b"a".to_vec(),
            KeyEntry {
                file_id: 1,
                offset: off_a,
                value_size: 1,
                expire_at_ms: 0,
                ref_bit: false,
            },
        );
        kd.put(
            b"b".to_vec(),
            KeyEntry {
                file_id: 2,
                offset: off_b,
                value_size: 1,
                expire_at_ms: 0,
                ref_bit: false,
            },
        );
        let _ = HEADER_SIZE;

        let out = compact(dir.path(), &[1, 2], &kd, 99, 0).unwrap();

        assert_eq!(out.records_written, 2);
        assert_eq!(out.records_dropped, 0);
    }

    #[test]
    fn remove_old_files_cleans_up() {
        let dir = tmp();
        make_log(dir.path(), 1, &[(b"k", b"v")]);
        let hint = hint_path(dir.path(), 1);
        // create a dummy hint file
        std::fs::write(&hint, b"dummy").unwrap();

        assert!(log_path(dir.path(), 1).exists());
        assert!(hint.exists());

        remove_old_files(dir.path(), &[1]).unwrap();

        assert!(!log_path(dir.path(), 1).exists());
        assert!(!hint.exists());
    }

    // ------------------------------------------------------------------
    // Edge cases
    // ------------------------------------------------------------------

    #[test]
    fn compact_empty_file_list_produces_empty_output() {
        let dir = tmp();
        let kd = KeyDir::new();
        let out = compact(dir.path(), &[], &kd, 99, 0).unwrap();
        assert_eq!(out.records_written, 0);
        assert_eq!(out.records_dropped, 0);
        // Output files are still created (empty)
        assert!(out.new_log_path.exists());
        assert!(out.new_hint_path.exists());
    }

    #[test]
    fn compact_missing_file_id_is_skipped_gracefully() {
        let dir = tmp();
        // file_id 42 does not exist on disk
        let kd = KeyDir::new();
        // Should not error — just skip the missing file
        let out = compact(dir.path(), &[42], &kd, 99, 0).unwrap();
        assert_eq!(out.records_written, 0);
        assert_eq!(out.records_dropped, 0);
    }

    #[test]
    fn compact_all_entries_expired_produces_empty_output() {
        let dir = tmp();
        // Write 5 entries all expiring at ms=100
        let path = log_path(dir.path(), 1);
        let mut w = LogWriter::open(&path, 1).unwrap();
        let mut offsets = Vec::new();
        for i in 0u8..5 {
            offsets.push(w.write(&[i], &[i], 100).unwrap());
        }
        w.sync().unwrap();

        let mut kd = KeyDir::new();
        for (i, &offset) in offsets.iter().enumerate() {
            kd.put(
                vec![i as u8],
                KeyEntry {
                    file_id: 1,
                    offset,
                    value_size: 1,
                    expire_at_ms: 100,
                    ref_bit: false,
                },
            );
        }

        // now_ms=9999 — all expired
        let out = compact(dir.path(), &[1], &kd, 99, 9999).unwrap();
        assert_eq!(out.records_written, 0);
        assert_eq!(out.records_dropped, 5);
    }

    #[test]
    fn compact_not_yet_expired_entries_are_kept() {
        let dir = tmp();
        let path = log_path(dir.path(), 1);
        let mut w = LogWriter::open(&path, 1).unwrap();
        let offset = w.write(b"k", b"v", 9_999_999_999).unwrap(); // far future
        w.sync().unwrap();

        let mut kd = KeyDir::new();
        kd.put(
            b"k".to_vec(),
            KeyEntry {
                file_id: 1,
                offset,
                value_size: 1,
                expire_at_ms: 9_999_999_999,
                ref_bit: false,
            },
        );

        let out = compact(dir.path(), &[1], &kd, 99, 0).unwrap(); // now_ms=0
        assert_eq!(out.records_written, 1);
        assert_eq!(out.records_dropped, 0);
    }

    #[test]
    fn compact_output_file_id_must_not_collide_with_inputs() {
        let dir = tmp();
        make_log(dir.path(), 1, &[(b"k", b"v")]);

        let mut kd = KeyDir::new();
        kd.put(
            b"k".to_vec(),
            KeyEntry {
                file_id: 1,
                offset: 0,
                value_size: 1,
                expire_at_ms: 0,
                ref_bit: false,
            },
        );

        // new_file_id=100 must be different from input file_id=1
        let out = compact(dir.path(), &[1], &kd, 100, 0).unwrap();
        assert_eq!(out.new_file_id, 100);
        assert!(out.new_log_path.exists());
        // Input file must still exist (caller is responsible for deletion)
        assert!(log_path(dir.path(), 1).exists());
    }

    #[test]
    fn remove_old_files_missing_files_is_ok() {
        let dir = tmp();
        // file_ids 5 and 6 don't exist — should not error
        remove_old_files(dir.path(), &[5, 6]).unwrap();
    }

    #[test]
    fn compact_compacted_output_roundtrips_through_hint() {
        let dir = tmp();
        make_log(dir.path(), 1, &[(b"x", b"y")]);

        let mut kd = KeyDir::new();
        kd.put(
            b"x".to_vec(),
            KeyEntry {
                file_id: 1,
                offset: 0,
                value_size: 1,
                expire_at_ms: 0,
                ref_bit: false,
            },
        );

        let out = compact(dir.path(), &[1], &kd, 99, 0).unwrap();

        // Reload keydir from the hint file produced by compaction
        let mut new_kd = KeyDir::new();
        let mut reader = crate::hint::HintReader::open(&out.new_hint_path).unwrap();
        reader.load_into(&mut new_kd).unwrap();

        let entry = new_kd.get(b"x").unwrap();
        assert_eq!(entry.file_id, 99);

        // Read the value from the new log file
        let mut log_reader = crate::log::LogReader::open(&out.new_log_path).unwrap();
        let record = log_reader.read_at(entry.offset).unwrap().unwrap();
        assert_eq!(record.value, Some(b"y".to_vec()));
    }

    #[test]
    fn compact_keeps_entries_with_future_expiry() {
        let dir = tmp();
        let path = log_path(dir.path(), 1);
        let mut w = LogWriter::open(&path, 1).unwrap();
        let offset = w.write(b"k", b"v", u64::MAX).unwrap(); // far future
        w.sync().unwrap();

        let mut kd = KeyDir::new();
        kd.put(
            b"k".to_vec(),
            KeyEntry {
                file_id: 1,
                offset,
                value_size: 1,
                expire_at_ms: u64::MAX,
                ref_bit: false,
            },
        );

        // now_ms=0, far below u64::MAX — entry must survive
        let out = compact(dir.path(), &[1], &kd, 99, 0).unwrap();
        assert_eq!(out.records_written, 1);
        assert_eq!(out.records_dropped, 0);
    }

    #[test]
    fn compact_with_100_live_entries_writes_all() {
        let dir = tmp();
        let path = log_path(dir.path(), 1);
        let mut w = LogWriter::open(&path, 1).unwrap();
        let mut offsets = Vec::with_capacity(100);
        for i in 0u8..100 {
            offsets.push(w.write(&[i], &[i], 0).unwrap());
        }
        w.sync().unwrap();

        let mut kd = KeyDir::new();
        for (i, &offset) in offsets.iter().enumerate() {
            kd.put(
                vec![i as u8],
                KeyEntry {
                    file_id: 1,
                    offset,
                    value_size: 1,
                    expire_at_ms: 0,
                    ref_bit: false,
                },
            );
        }

        let out = compact(dir.path(), &[1], &kd, 99, 0).unwrap();
        assert_eq!(out.records_written, 100);

        // Verify hint file loads back to 100 entries
        let mut new_kd = KeyDir::new();
        let mut hr = HintReader::open(&out.new_hint_path).unwrap();
        hr.load_into(&mut new_kd).unwrap();
        assert_eq!(new_kd.len(), 100);
    }

    #[test]
    fn compact_preserves_expire_at_ms_in_output_hint() {
        let dir = tmp();
        let path = log_path(dir.path(), 1);
        let mut w = LogWriter::open(&path, 1).unwrap();
        let offset = w.write(b"ttl_key", b"val", 99_999).unwrap();
        w.sync().unwrap();

        let mut kd = KeyDir::new();
        kd.put(
            b"ttl_key".to_vec(),
            KeyEntry {
                file_id: 1,
                offset,
                value_size: 3,
                expire_at_ms: 99_999,
                ref_bit: false,
            },
        );

        // now_ms=0 so the entry is not expired
        let out = compact(dir.path(), &[1], &kd, 99, 0).unwrap();
        assert_eq!(out.records_written, 1);

        let mut hr = HintReader::open(&out.new_hint_path).unwrap();
        let hints = hr.read_all().unwrap();
        assert_eq!(hints.len(), 1);
        assert_eq!(hints[0].expire_at_ms, 99_999);
    }

    #[test]
    fn compact_drops_stale_entry_when_keydir_points_to_newer_offset() {
        let dir = tmp();
        // Write key "k" twice — two different offsets in the same file
        use crate::log::HEADER_SIZE;
        let off_v1 = 0u64;
        let off_v2 = (HEADER_SIZE + 1 + 2) as u64; // after "k"/"v1" record (key=1, value=2)

        let path = log_path(dir.path(), 1);
        let mut w = LogWriter::open(&path, 1).unwrap();
        w.write(b"k", b"v1", 0).unwrap();
        w.write(b"k", b"v2", 0).unwrap();
        w.sync().unwrap();

        // Keydir points to the second (newer) write
        let mut kd = KeyDir::new();
        kd.put(
            b"k".to_vec(),
            KeyEntry {
                file_id: 1,
                offset: off_v2,
                value_size: 2,
                expire_at_ms: 0,
                ref_bit: false,
            },
        );
        let _ = off_v1;

        let out = compact(dir.path(), &[1], &kd, 99, 0).unwrap();
        assert_eq!(
            out.records_written, 1,
            "only the live (newer) record survives"
        );
        assert_eq!(out.records_dropped, 1, "stale first record is dropped");
    }

    // ------------------------------------------------------------------
    // Issue 4.1: Compaction must tolerate a corrupt/truncated tail record
    // ------------------------------------------------------------------

    /// Appending garbage bytes to the end of a log file (simulating a crash
    /// mid-write) must not cause compaction to fail.  The valid records
    /// before the corrupt tail must be preserved.
    #[test]
    fn compact_tolerates_corrupt_tail_in_input_file() {
        use std::io::Write as _;

        let dir = tmp();

        // Write 3 live records to file 1.
        let path = log_path(dir.path(), 1);
        let mut w = LogWriter::open(&path, 1).unwrap();
        let off_k1 = w.write(b"k1", b"v1", 0).unwrap();
        let off_k2 = w.write(b"k2", b"v2", 0).unwrap();
        let off_k3 = w.write(b"k3", b"v3", 0).unwrap();
        w.sync().unwrap();
        drop(w);

        // Append 10 bytes of garbage — simulates a partial tail record from a crash.
        {
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            f.write_all(&[0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55])
                .unwrap();
            f.sync_all().unwrap();
        }

        // Build keydir pointing to all three live records.
        let mut kd = KeyDir::new();
        for (key, off) in [
            (b"k1".as_slice(), off_k1),
            (b"k2".as_slice(), off_k2),
            (b"k3".as_slice(), off_k3),
        ] {
            kd.put(
                key.to_vec(),
                KeyEntry {
                    file_id: 1,
                    offset: off,
                    value_size: 2,
                    expire_at_ms: 0,
                    ref_bit: false,
                },
            );
        }

        // Compaction must succeed despite the garbage tail.
        let out = compact(dir.path(), &[1], &kd, 100, 0)
            .expect("compact must not fail on a corrupt tail record");

        assert_eq!(out.records_written, 3, "all 3 live records must be written");
        assert_eq!(out.records_dropped, 0);

        // All keys must be readable from the new log file.
        let mut reader = crate::log::LogReader::open(&out.new_log_path).unwrap();
        let records = reader.iter_from_start().unwrap();
        assert_eq!(records.len(), 3);
        let keys: Vec<&[u8]> = records.iter().map(|r| r.key.as_slice()).collect();
        assert!(keys.contains(&b"k1".as_slice()));
        assert!(keys.contains(&b"k2".as_slice()));
        assert!(keys.contains(&b"k3".as_slice()));
    }

    // ------------------------------------------------------------------
    // Issue 4.3: Output file ID must be strictly greater than all input IDs
    // ------------------------------------------------------------------

    /// Calling `compact()` with `new_file_id` <= `max(file_ids)` must return Err.
    #[test]
    fn compact_rejects_output_id_not_greater_than_input_ids() {
        let dir = tmp();
        make_log(dir.path(), 1, &[(b"a", b"1")]);
        make_log(dir.path(), 2, &[(b"b", b"2")]);
        make_log(dir.path(), 3, &[(b"c", b"3")]);

        let kd = KeyDir::new();
        // Keydir contents do not matter for this check — the guard fires first.

        // new_file_id=2 is less than max input id 3 — must error.
        let result = compact(dir.path(), &[1, 2, 3], &kd, 2, 0);
        assert!(
            result.is_err(),
            "compact must reject new_file_id (2) <= max input id (3)"
        );
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("new_file_id") && msg.contains('2') && msg.contains('3'),
            "error message must mention new_file_id and max input id; got: {msg}"
        );

        // new_file_id=3 equals max input id — also must error.
        let result_eq = compact(dir.path(), &[1, 2, 3], &kd, 3, 0);
        assert!(
            result_eq.is_err(),
            "compact must reject new_file_id (3) == max input id (3)"
        );
    }

    /// Calling `compact()` with `new_file_id` strictly greater than all input IDs
    /// must succeed.
    #[test]
    fn compact_accepts_output_id_greater_than_all_inputs() {
        let dir = tmp();
        use crate::log::HEADER_SIZE;

        make_log(dir.path(), 1, &[(b"a", b"1")]);
        make_log(dir.path(), 2, &[(b"b", b"2")]);
        make_log(dir.path(), 3, &[(b"c", b"3")]);

        let mut kd = KeyDir::new();
        kd.put(
            b"a".to_vec(),
            KeyEntry {
                file_id: 1,
                offset: 0,
                value_size: 1,
                expire_at_ms: 0,
                ref_bit: false,
            },
        );
        kd.put(
            b"b".to_vec(),
            KeyEntry {
                file_id: 2,
                offset: 0,
                value_size: 1,
                expire_at_ms: 0,
                ref_bit: false,
            },
        );
        kd.put(
            b"c".to_vec(),
            KeyEntry {
                file_id: 3,
                offset: 0,
                value_size: 1,
                expire_at_ms: 0,
                ref_bit: false,
            },
        );
        let _ = HEADER_SIZE;

        // new_file_id=100 is well above max input id 3 — must succeed.
        let out = compact(dir.path(), &[1, 2, 3], &kd, 100, 0)
            .expect("compact must succeed when new_file_id > all input ids");

        assert_eq!(out.new_file_id, 100);
        assert_eq!(out.records_written, 3);
    }

    /// Edge case: `new_file_id` == `max_input_id` must fail;
    /// `new_file_id` == `max_input_id` + 1 must succeed.
    #[test]
    fn compact_output_id_must_exceed_max_input_id() {
        let dir = tmp();
        make_log(dir.path(), 5, &[(b"k", b"v")]);

        let mut kd = KeyDir::new();
        kd.put(
            b"k".to_vec(),
            KeyEntry {
                file_id: 5,
                offset: 0,
                value_size: 1,
                expire_at_ms: 0,
                ref_bit: false,
            },
        );

        // Equal — must fail.
        assert!(
            compact(dir.path(), &[5], &kd, 5, 0).is_err(),
            "new_file_id == max_input_id must fail"
        );

        // One above — must succeed.
        let out = compact(dir.path(), &[5], &kd, 6, 0)
            .expect("new_file_id == max_input_id + 1 must succeed");
        assert_eq!(out.new_file_id, 6);
        assert_eq!(out.records_written, 1);
    }

    // ------------------------------------------------------------------
    // Additional edge cases from coverage requirements
    // ------------------------------------------------------------------

    /// Write a key 20 times, compact — compacted file has only 1 entry (latest value).
    #[test]
    fn compact_single_key_overwritten_many_times() {
        let dir = tmp();
        let path = log_path(dir.path(), 1);
        let mut w = LogWriter::open(&path, 1).unwrap();
        let mut last_offset = 0u64;
        for i in 0u8..20 {
            last_offset = w.write(b"hotkey", &[i], 0).unwrap();
        }
        w.sync().unwrap();

        // Keydir points only to the last write.
        let mut kd = KeyDir::new();
        kd.put(
            b"hotkey".to_vec(),
            KeyEntry {
                file_id: 1,
                offset: last_offset,
                value_size: 1,
                expire_at_ms: 0,
                ref_bit: false,
            },
        );

        let out = compact(dir.path(), &[1], &kd, 99, 0).unwrap();
        assert_eq!(out.records_written, 1, "only the latest write must survive");
        assert_eq!(out.records_dropped, 19, "19 stale writes must be dropped");

        // Verify the compacted log has exactly 1 record with the latest value.
        let mut reader = crate::log::LogReader::open(&out.new_log_path).unwrap();
        let records = reader.iter_from_start().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key, b"hotkey");
        assert_eq!(records[0].value, Some(vec![19u8])); // last iteration value
    }

    /// Put 10 keys then delete 5 of them. Compact — compacted file has exactly 5 entries.
    #[test]
    fn compact_drops_tombstoned_keys() {
        let dir = tmp();
        let path = log_path(dir.path(), 1);
        let mut w = LogWriter::open(&path, 1).unwrap();
        let mut live_offsets: Vec<(Vec<u8>, u64)> = Vec::new();

        for i in 0u8..10 {
            let key = vec![i];
            let offset = w.write(&key, &[i], 0).unwrap();
            live_offsets.push((key, offset));
        }
        // Write tombstones for keys 0..5.
        for i in 0u8..5 {
            w.write_tombstone(&[i]).unwrap();
        }
        w.sync().unwrap();

        // Keydir has only keys 5..10.
        let mut kd = KeyDir::new();
        for (key, offset) in live_offsets.iter().skip(5) {
            kd.put(
                key.clone(),
                KeyEntry {
                    file_id: 1,
                    offset: *offset,
                    value_size: 1,
                    expire_at_ms: 0,
                    ref_bit: false,
                },
            );
        }

        let out = compact(dir.path(), &[1], &kd, 99, 0).unwrap();
        assert_eq!(
            out.records_written, 5,
            "5 live keys must survive compaction"
        );

        let mut hr = HintReader::open(&out.new_hint_path).unwrap();
        let hints = hr.read_all().unwrap();
        assert_eq!(hints.len(), 5, "hint file must have exactly 5 entries");
    }

    /// The compacted output file must be smaller than the sum of the input files
    /// when there is significant write amplification (many overwrites).
    #[test]
    fn compact_output_file_is_smaller_than_inputs() {
        let dir = tmp();
        let path = log_path(dir.path(), 1);
        let mut w = LogWriter::open(&path, 1).unwrap();

        // Write each of 10 keys 50 times — only the last write per key is live.
        let mut last_offsets: std::collections::HashMap<Vec<u8>, u64> =
            std::collections::HashMap::new();
        for _round in 0u8..50 {
            for k in 0u8..10 {
                let offset = w.write(&[k], &[k; 100], 0).unwrap(); // 100-byte values
                last_offsets.insert(vec![k], offset);
            }
        }
        w.sync().unwrap();

        let input_size = std::fs::metadata(&path).unwrap().len();

        let mut kd = KeyDir::new();
        for (key, offset) in &last_offsets {
            kd.put(
                key.clone(),
                KeyEntry {
                    file_id: 1,
                    offset: *offset,
                    value_size: 100,
                    expire_at_ms: 0,
                    ref_bit: false,
                },
            );
        }

        let out = compact(dir.path(), &[1], &kd, 99, 0).unwrap();
        let output_size = std::fs::metadata(&out.new_log_path).unwrap().len();

        assert_eq!(out.records_written, 10, "10 live keys must survive");
        assert!(
            output_size < input_size,
            "compacted output ({output_size} bytes) must be smaller than input ({input_size} bytes)"
        );
    }

    #[test]
    fn compact_uses_metadata_scan_before_copying_live_values() {
        let source = include_str!("compaction.rs");
        let compact_start = source.find("pub fn compact(").unwrap();
        let compact_tail = &source[compact_start..];
        let compact_end = compact_tail.find("pub fn remove_old_files").unwrap();
        let compact_body = &compact_tail[..compact_end];

        assert!(
            compact_body.contains("iter_metadata_from_start_tolerant"),
            "compact must scan metadata first so stale or expired large values are not materialized"
        );
        assert!(
            !compact_body.contains("iter_from_start_tolerant()?"),
            "compact must not load every source record value before deciding whether it is live"
        );
    }

    /// `remove_old_files` removes both .log and .hint files for the given file IDs.
    #[test]
    fn remove_old_files_deletes_data_and_hint_files() {
        let dir = tmp();

        // Create log and hint files for IDs 10 and 20.
        for &fid in &[10u64, 20u64] {
            make_log(dir.path(), fid, &[(b"k", b"v")]);
            let hp = hint_path(dir.path(), fid);
            std::fs::write(&hp, b"fake hint").unwrap();
        }

        assert!(log_path(dir.path(), 10).exists());
        assert!(hint_path(dir.path(), 10).exists());
        assert!(log_path(dir.path(), 20).exists());
        assert!(hint_path(dir.path(), 20).exists());

        remove_old_files(dir.path(), &[10, 20]).unwrap();

        assert!(
            !log_path(dir.path(), 10).exists(),
            ".log for fid 10 must be deleted"
        );
        assert!(
            !hint_path(dir.path(), 10).exists(),
            ".hint for fid 10 must be deleted"
        );
        assert!(
            !log_path(dir.path(), 20).exists(),
            ".log for fid 20 must be deleted"
        );
        assert!(
            !hint_path(dir.path(), 20).exists(),
            ".hint for fid 20 must be deleted"
        );
    }
}
