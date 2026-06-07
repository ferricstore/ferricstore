//! Hint files accelerate startup by storing the keydir index without values.
//!
//! A hint file mirrors the corresponding data file but omits value bytes.
//! Each entry is prefixed with a CRC32 checksum that covers all remaining
//! fields, preventing a corrupt hint file from silently inserting bad disk
//! pointers into the keydir.
//!
//! ```text
//! [ crc32: u32 | file_id: u64 | offset: u64 | value_size: u32 | expire_at_ms: u64 | key_size: u16 | key: [u8] ]
//! ```
//!
//! The CRC covers: `file_id || offset || value_size || expire_at_ms || key_size || key`
//! (everything after the `crc32` field).
//!
//! Reading hint files rebuilds the full keydir in milliseconds on startup —
//! no need to scan the entire value log.

use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::keydir::{KeyDir, KeyEntry};

/// CRC32 field size prepended to every hint entry.
const CRC_SIZE: usize = 4;

/// Fixed-size body header per hint record (after the CRC, before the key bytes).
/// `file_id`(8) + `offset`(8) + `value_size`(4) + `expire_at_ms`(8) + `key_size`(2) = 30
const HINT_BODY_HEADER_SIZE: usize = 30;

/// Total header bytes per hint record (before the key bytes):
/// `crc32`(4) + `file_id`(8) + `offset`(8) + `value_size`(4) + `expire_at_ms`(8) + `key_size`(2) = 34
pub const HINT_HEADER_SIZE: usize = CRC_SIZE + HINT_BODY_HEADER_SIZE;

#[derive(Debug)]
pub struct HintError(pub String);

impl std::fmt::Display for HintError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HintError: {}", self.0)
    }
}

impl std::error::Error for HintError {}

impl From<io::Error> for HintError {
    fn from(e: io::Error) -> Self {
        HintError(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, HintError>;

/// A single entry read from a hint file.
#[derive(Debug, PartialEq, Eq)]
pub struct HintEntry {
    pub file_id: u64,
    pub offset: u64,
    pub value_size: u32,
    pub expire_at_ms: u64,
    pub key: Vec<u8>,
}

/// Writes a hint file alongside a data file using an atomic write-then-rename
/// strategy.
///
/// All content is written to a `.hint.tmp` temporary file. Only when
/// [`HintWriter::commit`] is called are the contents flushed, synced to disk,
/// and the temporary file atomically renamed to the final `.hint` path.
///
/// If `commit` is never called (e.g. the process panics or an error occurs),
/// the [`Drop`] implementation removes the `.hint.tmp` file so no partial
/// content is left on disk.
pub struct HintWriter {
    writer: BufWriter<File>,
    tmp_path: PathBuf,
    final_path: PathBuf,
}

impl HintWriter {
    /// Open a new hint writer targeting `path`.
    ///
    /// Content is written to `<path>.tmp` and only moved to `path` on
    /// [`commit`](HintWriter::commit).
    ///
    /// # Errors
    ///
    /// Returns a `HintError` if the temporary file cannot be created or opened.
    pub fn open(path: &Path) -> Result<Self> {
        let tmp_path = path.with_extension("hint.tmp");
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp_path)?;
        Ok(Self {
            writer: BufWriter::new(file),
            tmp_path,
            final_path: path.to_path_buf(),
        })
    }

    /// # Errors
    ///
    /// Returns a `HintError` if the entry cannot be written to disk.
    pub fn write_entry(&mut self, entry: &HintEntry) -> Result<()> {
        #[allow(clippy::cast_possible_truncation)]
        let key_size = entry.key.len() as u16;

        // Build the body first so we can compute the CRC over it.
        let mut body = Vec::with_capacity(HINT_BODY_HEADER_SIZE + entry.key.len());
        body.extend_from_slice(&entry.file_id.to_le_bytes());
        body.extend_from_slice(&entry.offset.to_le_bytes());
        body.extend_from_slice(&entry.value_size.to_le_bytes());
        body.extend_from_slice(&entry.expire_at_ms.to_le_bytes());
        body.extend_from_slice(&key_size.to_le_bytes());
        body.extend_from_slice(&entry.key);

        let crc = crc32(&body);
        self.writer.write_all(&crc.to_le_bytes())?;
        self.writer.write_all(&body)?;
        Ok(())
    }

    /// Flush all buffered data, sync to disk, and atomically rename the
    /// temporary file to the final hint path.
    ///
    /// After a successful `commit` the `.hint.tmp` file no longer exists (it
    /// has been renamed to `.hint`). The subsequent `drop` will attempt to
    /// remove the `.hint.tmp` path and fail silently, which is correct.
    ///
    /// # Errors
    ///
    /// Returns a `HintError` if flushing, syncing, or renaming fails. On
    /// error the temporary file is left in place; the [`Drop`] impl will
    /// remove it when the writer is dropped.
    pub fn commit(mut self) -> Result<()> {
        // Drain the BufWriter into the OS, then fdatasync the file data.
        // We use `sync_data()` (fdatasync) instead of `sync_all()` (fsync)
        // because hint files never change size after this call — all
        // body bytes were written up-front — and mtime/atime metadata is
        // not a durability concern for recovery.
        //
        // The rename below makes the final filename visible but the
        // rename's directory entry is not durable until the parent
        // directory is fsynced. The caller (Elixir site) is responsible
        // for `v2_fsync_dir(shard_data_path)` after this returns —
        // rotation's dir-fsync step already covers that.
        self.writer.flush()?;
        self.writer.get_ref().sync_data()?;
        std::fs::rename(&self.tmp_path, &self.final_path)?;
        Ok(())
    }
}

impl Drop for HintWriter {
    /// Best-effort cleanup of the temporary file if [`commit`](HintWriter::commit)
    /// was never called (crash / error path). The `remove_file` call is allowed
    /// to fail silently: after a successful `commit` the file has already been
    /// renamed away, so `remove_file` will return `NotFound`, which is harmless.
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.tmp_path);
    }
}

/// Reads hint entries and reconstructs the keydir.
pub struct HintReader {
    file: File,
}

impl HintReader {
    /// # Errors
    ///
    /// Returns a `HintError` if the file cannot be opened.
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        Ok(Self { file })
    }

    /// Load all entries from the hint file into `keydir`.
    /// Tombstones (`value_size` == 0) are skipped -- they represent deleted keys.
    ///
    /// # Errors
    ///
    /// Returns a `HintError` if the hint file cannot be read or is malformed.
    pub fn load_into(&mut self, keydir: &mut KeyDir) -> Result<()> {
        self.file.seek(SeekFrom::Start(0))?;
        while let Some(entry) = read_hint_entry(&mut self.file)? {
            if entry.value_size == 0 {
                keydir.delete(&entry.key);
            } else {
                keydir.put(
                    entry.key.clone(),
                    KeyEntry {
                        file_id: entry.file_id,
                        offset: entry.offset,
                        value_size: entry.value_size,
                        expire_at_ms: entry.expire_at_ms,
                        ref_bit: false,
                    },
                );
            }
        }
        Ok(())
    }

    /// Collect all hint entries (used in tests and compaction).
    ///
    /// # Errors
    ///
    /// Returns a `HintError` if the hint file cannot be read or is malformed.
    pub fn read_all(&mut self) -> Result<Vec<HintEntry>> {
        self.file.seek(SeekFrom::Start(0))?;
        let mut entries = Vec::new();
        while let Some(entry) = read_hint_entry(&mut self.file)? {
            entries.push(entry);
        }
        Ok(entries)
    }
}

fn read_hint_entry(reader: &mut impl Read) -> Result<Option<HintEntry>> {
    // Read the CRC32 (4 bytes). A clean EOF here means the file ended normally.
    let mut crc_buf = [0u8; CRC_SIZE];
    match reader.read_exact(&mut crc_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let stored_crc = u32::from_le_bytes(crc_buf);

    // Read the fixed-size body header (30 bytes).
    let mut header = [0u8; HINT_BODY_HEADER_SIZE];
    reader.read_exact(&mut header).map_err(HintError::from)?;

    let file_id = u64::from_le_bytes(
        header[0..8]
            .try_into()
            .map_err(|_| HintError("slice conversion failed".to_string()))?,
    );
    let offset = u64::from_le_bytes(
        header[8..16]
            .try_into()
            .map_err(|_| HintError("slice conversion failed".to_string()))?,
    );
    let value_size = u32::from_le_bytes(
        header[16..20]
            .try_into()
            .map_err(|_| HintError("slice conversion failed".to_string()))?,
    );
    let expire_at_ms = u64::from_le_bytes(
        header[20..28]
            .try_into()
            .map_err(|_| HintError("slice conversion failed".to_string()))?,
    );
    let key_size = u16::from_le_bytes(
        header[28..30]
            .try_into()
            .map_err(|_| HintError("slice conversion failed".to_string()))?,
    ) as usize;

    let mut key = vec![0u8; key_size];
    reader.read_exact(&mut key).map_err(HintError::from)?;

    // Validate CRC over body (header + key).
    let mut body = Vec::with_capacity(HINT_BODY_HEADER_SIZE + key.len());
    body.extend_from_slice(&header);
    body.extend_from_slice(&key);
    let computed_crc = crc32(&body);

    if computed_crc != stored_crc {
        return Err(HintError(format!(
            "hint entry CRC mismatch: stored={stored_crc:#010x}, computed={computed_crc:#010x}"
        )));
    }

    Ok(Some(HintEntry {
        file_id,
        offset,
        value_size,
        expire_at_ms,
        key,
    }))
}

/// CRC-32/ISO-HDLC — delegates to `crc32fast::hash` for hardware-accelerated
/// CRC computation (SSE 4.2 / ARMv8 CRC instructions when available).
///
/// H-REMAIN-1 fix: replaced byte-at-a-time hand-rolled CRC32 with
/// `crc32fast::hash()`. Both use the same CRC-32/ISO-HDLC polynomial
/// (0xEDB88320), so existing hint files remain compatible with no
/// migration needed.
fn crc32(data: &[u8]) -> u32 {
    crc32fast::hash(data)
}

#[cfg(test)]
mod tests {
    include!("sections/hint_tests.rs");
}
