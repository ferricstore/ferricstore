//! Crash-safe after-image transactions for probabilistic sidecar mutations.

use std::cmp::Ordering;
use std::ffi::OsString;
use std::fs::File;
use std::io::{Read, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use rustler::{Encoder, Env, NifResult, Term};

const MAGIC: &[u8; 8] = b"FPTXN001";
const VERSION: u32 = 1;
const FIXED_HEADER_SIZE: usize = 56;
const CHECKSUM_SIZE: usize = 4;
pub const TOKEN_SIZE: usize = 16;
const MAX_TRANSACTION_BYTES: usize = 512 * 1024 * 1024;
const MAX_AFTER_IMAGES: usize = 20_000_000;
const COMPACT_RECEIPT_THRESHOLD_BYTES: usize = 1024 * 1024;

mod atoms {
    rustler::atoms! {
        ok,
        error,
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct MutationToken {
    pub index: u64,
    pub ordinal: u64,
}

impl MutationToken {
    pub const ZERO: Self = Self {
        index: 0,
        ordinal: 0,
    };

    #[must_use]
    pub const fn new(index: u64, ordinal: u64) -> Self {
        Self { index, ordinal }
    }

    #[must_use]
    pub fn encode(self) -> [u8; TOKEN_SIZE] {
        let mut encoded = [0_u8; TOKEN_SIZE];
        encoded[0..8].copy_from_slice(&self.index.to_le_bytes());
        encoded[8..16].copy_from_slice(&self.ordinal.to_le_bytes());
        encoded
    }

    fn decode(encoded: &[u8]) -> Result<Self, String> {
        if encoded.len() != TOKEN_SIZE {
            return Err("invalid probabilistic mutation token length".into());
        }

        Ok(Self {
            index: u64::from_le_bytes(encoded[0..8].try_into().unwrap()),
            ordinal: u64::from_le_bytes(encoded[8..16].try_into().unwrap()),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AfterImage {
    pub offset: u64,
    pub bytes: Vec<u8>,
}

impl AfterImage {
    #[must_use]
    pub const fn new(offset: u64, bytes: Vec<u8>) -> Self {
        Self { offset, bytes }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum MutationDecision {
    Apply,
    Replay(Vec<u8>),
    Stale,
}

#[derive(Debug)]
struct Transaction {
    token: MutationToken,
    token_offset: u64,
    expected_file_size: u64,
    images: Vec<AfterImage>,
    result: Vec<u8>,
}

#[must_use]
pub fn transaction_path(target: &Path) -> PathBuf {
    let mut file_name = target
        .file_name()
        .map_or_else(OsString::new, std::ffi::OsStr::to_os_string);
    file_name.push(".mutation");
    target.with_file_name(file_name)
}

#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn prob_file_recover(env: Env<'_>, path: String, extension: String) -> NifResult<Term<'_>> {
    let path = Path::new(&path);
    let result = match extension.as_str() {
        "bloom" => crate::bloom::recover_sidecar(path),
        "cms" => crate::cms::recover_sidecar(path),
        "cuckoo" => crate::cuckoo::recover_sidecar(path),
        "topk" => crate::topk::recover_sidecar(path),
        _ => Err("unsupported probabilistic sidecar extension".into()),
    };

    match result {
        Ok(()) => Ok(atoms::ok().encode(env)),
        Err(error) => Ok((atoms::error(), error).encode(env)),
    }
}

pub fn begin(
    file: &File,
    target: &Path,
    token: MutationToken,
    token_offset: u64,
    expected_file_size: u64,
) -> Result<MutationDecision, String> {
    recover(file, target, token_offset, expected_file_size)?;
    let current = read_token(file, token_offset)?;

    match token.cmp(&current) {
        Ordering::Greater => Ok(MutationDecision::Apply),
        Ordering::Less => Ok(MutationDecision::Stale),
        Ordering::Equal => {
            let transaction = read_transaction_if_present(target)?;
            match transaction {
                Some((transaction, _pending)) if transaction.token == token => {
                    Ok(MutationDecision::Replay(transaction.result))
                }
                _missing_or_older_receipt => Ok(MutationDecision::Stale),
            }
        }
    }
}

pub fn commit(
    file: &File,
    target: &Path,
    token: MutationToken,
    token_offset: u64,
    expected_file_size: u64,
    mut images: Vec<AfterImage>,
    result: Vec<u8>,
) -> Result<(), String> {
    validate_token_location(token_offset, expected_file_size)?;
    validate_images(&images, expected_file_size)?;

    images.push(AfterImage::new(token_offset, token.encode().to_vec()));
    let transaction = Transaction {
        token,
        token_offset,
        expected_file_size,
        images,
        result,
    };

    write_transaction(target, &transaction)?;
    apply_transaction(file, &transaction)?;
    compact_receipt_if_large(target, &transaction);
    Ok(())
}

pub fn recover(
    file: &File,
    target: &Path,
    token_offset: u64,
    expected_file_size: u64,
) -> Result<(), String> {
    validate_token_location(token_offset, expected_file_size)?;
    let Some((transaction, pending)) = read_transaction_if_present(target)? else {
        return Ok(());
    };

    if transaction.token_offset != token_offset
        || transaction.expected_file_size != expected_file_size
    {
        return Err("probabilistic mutation receipt does not match target layout".into());
    }

    let current = read_token(file, token_offset)?;
    if current < transaction.token {
        apply_transaction(file, &transaction)?;
    }
    if pending {
        promote_pending_transaction(target);
    }
    compact_receipt_if_large(target, &transaction);
    Ok(())
}

fn compact_receipt_if_large(target: &Path, transaction: &Transaction) {
    let Ok((_image_count, _result_len, encoded_size)) = validate_transaction_encoding(transaction)
    else {
        return;
    };
    if encoded_size < COMPACT_RECEIPT_THRESHOLD_BYTES || transaction.images.is_empty() {
        return;
    }

    let compact = Transaction {
        token: transaction.token,
        token_offset: transaction.token_offset,
        expected_file_size: transaction.expected_file_size,
        images: Vec::new(),
        result: transaction.result.clone(),
    };
    // The target and full recovery receipt are already durable. Compaction is
    // best-effort so an allocation or metadata-space failure cannot turn a
    // committed mutation into an apply failure.
    let _ = write_transaction(target, &compact);
}

fn apply_transaction(file: &File, transaction: &Transaction) -> Result<(), String> {
    for image in &transaction.images {
        crate::write_all_at(
            file,
            &image.bytes,
            image.offset,
            "probabilistic mutation after-image",
        )?;
    }
    crate::prob_fsync(file)
}

fn read_token(file: &File, token_offset: u64) -> Result<MutationToken, String> {
    let mut encoded = [0_u8; TOKEN_SIZE];
    read_exact_at(
        file,
        &mut encoded,
        token_offset,
        "probabilistic mutation token",
    )?;
    MutationToken::decode(&encoded)
}

fn read_exact_at(file: &File, buffer: &mut [u8], offset: u64, label: &str) -> Result<(), String> {
    let mut read = 0;
    while read < buffer.len() {
        let count = file
            .read_at(&mut buffer[read..], offset + read as u64)
            .map_err(|error| format!("pread {label}: {error}"))?;
        if count == 0 {
            return Err(format!("truncated file while reading {label}"));
        }
        read += count;
    }
    Ok(())
}

fn write_transaction(target: &Path, transaction: &Transaction) -> Result<(), String> {
    let (image_count, result_len, _encoded_size) = validate_transaction_encoding(transaction)?;
    let receipt_path = transaction_path(target);
    let mut staged = crate::create_staged_locked_nofollow(&receipt_path)
        .map_err(|error| format!("create probabilistic mutation receipt: {error}"))?;
    let mut checksum = crc32fast::Hasher::new();
    let writer: &mut File = &mut staged;

    write_checksummed(writer, &mut checksum, MAGIC)?;
    write_checksummed(writer, &mut checksum, &VERSION.to_le_bytes())?;
    write_checksummed(writer, &mut checksum, &0_u32.to_le_bytes())?;
    write_checksummed(
        writer,
        &mut checksum,
        &transaction.token.index.to_le_bytes(),
    )?;
    write_checksummed(
        writer,
        &mut checksum,
        &transaction.token.ordinal.to_le_bytes(),
    )?;
    write_checksummed(
        writer,
        &mut checksum,
        &transaction.token_offset.to_le_bytes(),
    )?;
    write_checksummed(
        writer,
        &mut checksum,
        &transaction.expected_file_size.to_le_bytes(),
    )?;
    write_checksummed(writer, &mut checksum, &image_count.to_le_bytes())?;
    write_checksummed(writer, &mut checksum, &result_len.to_le_bytes())?;

    for image in &transaction.images {
        let image_len = u64::try_from(image.bytes.len())
            .map_err(|_| "probabilistic mutation after-image exceeds platform size".to_string())?;
        write_checksummed(writer, &mut checksum, &image.offset.to_le_bytes())?;
        write_checksummed(writer, &mut checksum, &image_len.to_le_bytes())?;
        write_checksummed(writer, &mut checksum, &image.bytes)?;
    }
    write_checksummed(writer, &mut checksum, &transaction.result)?;
    writer
        .write_all(&checksum.finalize().to_le_bytes())
        .map_err(|error| format!("write probabilistic mutation receipt checksum: {error}"))?;
    staged
        .publish()
        .map_err(|error| format!("publish probabilistic mutation receipt: {error}"))
}

fn write_checksummed(
    writer: &mut impl Write,
    checksum: &mut crc32fast::Hasher,
    bytes: &[u8],
) -> Result<(), String> {
    writer
        .write_all(bytes)
        .map_err(|error| format!("write probabilistic mutation receipt: {error}"))?;
    checksum.update(bytes);
    Ok(())
}

fn pending_transaction_path(target: &Path) -> Option<PathBuf> {
    let file_name = target.file_name()?.to_str()?;
    if file_name.ends_with(".pending-create") {
        return None;
    }
    Some(target.with_file_name(format!("{file_name}.pending-create.mutation")))
}

fn promote_pending_transaction(target: &Path) {
    let Some(pending_path) = pending_transaction_path(target) else {
        return;
    };
    let _ = crate::path_open::rename_nofollow(&pending_path, &transaction_path(target));
}

fn read_transaction_if_present(target: &Path) -> Result<Option<(Transaction, bool)>, String> {
    let path = transaction_path(target);
    if let Some(transaction) = read_transaction_at_path(&path)? {
        return Ok(Some((transaction, false)));
    }
    let Some(pending_path) = pending_transaction_path(target) else {
        return Ok(None);
    };
    read_transaction_at_path(&pending_path)
        .map(|transaction| transaction.map(|value| (value, true)))
}

fn read_transaction_at_path(path: &Path) -> Result<Option<Transaction>, String> {
    let mut file = match crate::open_random_read_locked(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(format!("open probabilistic mutation receipt: {error}")),
    };

    let size = file
        .metadata()
        .map_err(|error| format!("stat probabilistic mutation receipt: {error}"))?
        .len();
    let size = usize::try_from(size)
        .map_err(|_| "probabilistic mutation receipt exceeds platform size".to_string())?;
    if !(FIXED_HEADER_SIZE + CHECKSUM_SIZE..=MAX_TRANSACTION_BYTES).contains(&size) {
        return Err("invalid probabilistic mutation receipt size".into());
    }

    let mut encoded = Vec::new();
    encoded
        .try_reserve_exact(size)
        .map_err(|_| "probabilistic mutation receipt allocation failed".to_string())?;
    file.read_to_end(&mut encoded)
        .map_err(|error| format!("read probabilistic mutation receipt: {error}"))?;
    decode_transaction(&encoded).map(Some)
}

fn validate_transaction_encoding(transaction: &Transaction) -> Result<(u32, u32, usize), String> {
    validate_token_location(transaction.token_offset, transaction.expected_file_size)?;
    validate_images(&transaction.images, transaction.expected_file_size)?;

    let image_count = u32::try_from(transaction.images.len())
        .map_err(|_| "too many probabilistic mutation after-images".to_string())?;
    let result_len = u32::try_from(transaction.result.len())
        .map_err(|_| "probabilistic mutation result is too large".to_string())?;

    let mut total_size = FIXED_HEADER_SIZE + transaction.result.len() + CHECKSUM_SIZE;
    for image in &transaction.images {
        total_size = total_size
            .checked_add(16)
            .and_then(|size| size.checked_add(image.bytes.len()))
            .ok_or_else(|| "probabilistic mutation receipt size overflow".to_string())?;
    }
    if total_size > MAX_TRANSACTION_BYTES {
        return Err("probabilistic mutation receipt exceeds size limit".into());
    }
    Ok((image_count, result_len, total_size))
}

fn decode_transaction(encoded: &[u8]) -> Result<Transaction, String> {
    if encoded.len() < FIXED_HEADER_SIZE + CHECKSUM_SIZE || encoded.len() > MAX_TRANSACTION_BYTES {
        return Err("invalid probabilistic mutation receipt size".into());
    }
    let content_len = encoded.len() - CHECKSUM_SIZE;
    let expected_checksum = u32::from_le_bytes(encoded[content_len..].try_into().unwrap());
    if crc32fast::hash(&encoded[..content_len]) != expected_checksum {
        return Err("probabilistic mutation receipt checksum mismatch".into());
    }
    if &encoded[0..8] != MAGIC {
        return Err("invalid probabilistic mutation receipt magic".into());
    }
    if read_u32(encoded, 8)? != VERSION || read_u32(encoded, 12)? != 0 {
        return Err("unsupported probabilistic mutation receipt version".into());
    }

    let token = MutationToken::new(read_u64(encoded, 16)?, read_u64(encoded, 24)?);
    let token_offset = read_u64(encoded, 32)?;
    let expected_file_size = read_u64(encoded, 40)?;
    let image_count = read_u32(encoded, 48)? as usize;
    let result_len = read_u32(encoded, 52)? as usize;
    if image_count > MAX_AFTER_IMAGES {
        return Err("too many probabilistic mutation after-images".into());
    }
    validate_token_location(token_offset, expected_file_size)?;

    let mut cursor = FIXED_HEADER_SIZE;
    let mut images = Vec::new();
    images
        .try_reserve_exact(image_count)
        .map_err(|_| "probabilistic mutation after-image allocation failed".to_string())?;
    for _ in 0..image_count {
        let offset = read_u64_at_cursor(encoded, &mut cursor, content_len)?;
        let length = read_u64_at_cursor(encoded, &mut cursor, content_len)?;
        let length = usize::try_from(length)
            .map_err(|_| "probabilistic mutation after-image exceeds platform size".to_string())?;
        let end = cursor
            .checked_add(length)
            .ok_or_else(|| "probabilistic mutation after-image length overflow".to_string())?;
        if end > content_len {
            return Err("truncated probabilistic mutation after-image".into());
        }
        images.push(AfterImage::new(offset, encoded[cursor..end].to_vec()));
        cursor = end;
    }

    let result_end = cursor
        .checked_add(result_len)
        .ok_or_else(|| "probabilistic mutation result length overflow".to_string())?;
    if result_end != content_len {
        return Err("invalid probabilistic mutation receipt payload length".into());
    }
    let result = encoded[cursor..result_end].to_vec();
    validate_images(&images, expected_file_size)?;

    Ok(Transaction {
        token,
        token_offset,
        expected_file_size,
        images,
        result,
    })
}

fn validate_token_location(token_offset: u64, expected_file_size: u64) -> Result<(), String> {
    match token_offset.checked_add(TOKEN_SIZE as u64) {
        Some(end) if end == expected_file_size => Ok(()),
        _ => Err("probabilistic mutation token is not the file footer".into()),
    }
}

fn validate_images(images: &[AfterImage], expected_file_size: u64) -> Result<(), String> {
    if images.len() > MAX_AFTER_IMAGES {
        return Err("too many probabilistic mutation after-images".into());
    }
    for image in images {
        let length = u64::try_from(image.bytes.len())
            .map_err(|_| "probabilistic mutation after-image exceeds platform size".to_string())?;
        let end = image
            .offset
            .checked_add(length)
            .ok_or_else(|| "probabilistic mutation after-image range overflow".to_string())?;
        if end > expected_file_size {
            return Err("probabilistic mutation after-image exceeds target file".into());
        }
    }
    Ok(())
}

fn read_u32(encoded: &[u8], offset: usize) -> Result<u32, String> {
    let end = offset
        .checked_add(4)
        .ok_or_else(|| "probabilistic mutation receipt offset overflow".to_string())?;
    let bytes = encoded
        .get(offset..end)
        .ok_or_else(|| "truncated probabilistic mutation receipt header".to_string())?;
    Ok(u32::from_le_bytes(bytes.try_into().unwrap()))
}

fn read_u64(encoded: &[u8], offset: usize) -> Result<u64, String> {
    let end = offset
        .checked_add(8)
        .ok_or_else(|| "probabilistic mutation receipt offset overflow".to_string())?;
    let bytes = encoded
        .get(offset..end)
        .ok_or_else(|| "truncated probabilistic mutation receipt header".to_string())?;
    Ok(u64::from_le_bytes(bytes.try_into().unwrap()))
}

fn read_u64_at_cursor(
    encoded: &[u8],
    cursor: &mut usize,
    content_len: usize,
) -> Result<u64, String> {
    let end = cursor
        .checked_add(8)
        .ok_or_else(|| "probabilistic mutation receipt cursor overflow".to_string())?;
    if end > content_len {
        return Err("truncated probabilistic mutation receipt".into());
    }
    let value = u64::from_le_bytes(encoded[*cursor..end].try_into().unwrap());
    *cursor = end;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;

    fn target_file(dir: &Path) -> (PathBuf, File) {
        let path = dir.join("target.cms");
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        file.set_len(64).unwrap();
        (path, file)
    }

    #[test]
    fn commit_is_replay_safe_for_the_same_token() {
        let dir = tempfile::tempdir().unwrap();
        let (path, file) = target_file(dir.path());
        let token = MutationToken::new(7, 1);
        let result = vec![3, 4, 5];

        assert_eq!(
            begin(&file, &path, token, 48, 64).unwrap(),
            MutationDecision::Apply
        );
        commit(
            &file,
            &path,
            token,
            48,
            64,
            vec![AfterImage::new(8, vec![9, 8, 7])],
            result.clone(),
        )
        .unwrap();

        assert_eq!(
            begin(&file, &path, token, 48, 64).unwrap(),
            MutationDecision::Replay(result)
        );
        let mut bytes = [0_u8; 3];
        read_exact_at(&file, &mut bytes, 8, "test bytes").unwrap();
        assert_eq!(bytes, [9, 8, 7]);
        assert_eq!(read_token(&file, 48).unwrap(), token);
    }

    #[test]
    fn commit_compacts_large_after_images_after_the_target_is_durable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("large-target.cms");
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let image = vec![7_u8; 2 * 1024 * 1024];
        let token_offset = image.len() as u64;
        let expected_file_size = token_offset + TOKEN_SIZE as u64;
        file.set_len(expected_file_size).unwrap();
        let token = MutationToken::new(9, 1);
        let result = vec![4, 2];

        commit(
            &file,
            &path,
            token,
            token_offset,
            expected_file_size,
            vec![AfterImage::new(0, image)],
            result.clone(),
        )
        .unwrap();

        let receipt_size = std::fs::metadata(transaction_path(&path)).unwrap().len();
        assert_eq!(
            receipt_size,
            (FIXED_HEADER_SIZE + result.len() + CHECKSUM_SIZE) as u64
        );
        assert_eq!(
            begin(&file, &path, token, token_offset, expected_file_size).unwrap(),
            MutationDecision::Replay(result)
        );
    }

    #[test]
    fn recovery_finishes_a_partially_written_after_image_once() {
        let dir = tempfile::tempdir().unwrap();
        let (path, file) = target_file(dir.path());
        let token = MutationToken::new(11, 2);
        let transaction = Transaction {
            token,
            token_offset: 48,
            expected_file_size: 64,
            images: vec![
                AfterImage::new(4, vec![1, 2, 3, 4]),
                AfterImage::new(20, vec![5, 6, 7, 8]),
                AfterImage::new(48, token.encode().to_vec()),
            ],
            result: vec![42],
        };
        write_transaction(&path, &transaction).unwrap();
        crate::write_all_at(&file, &[1, 2, 3, 4], 4, "partial test image").unwrap();

        recover(&file, &path, 48, 64).unwrap();
        recover(&file, &path, 48, 64).unwrap();

        let mut first = [0_u8; 4];
        let mut second = [0_u8; 4];
        read_exact_at(&file, &mut first, 4, "first test image").unwrap();
        read_exact_at(&file, &mut second, 20, "second test image").unwrap();
        assert_eq!(first, [1, 2, 3, 4]);
        assert_eq!(second, [5, 6, 7, 8]);
        assert_eq!(read_token(&file, 48).unwrap(), token);
    }

    #[test]
    fn corrupt_receipt_never_changes_the_target() {
        let dir = tempfile::tempdir().unwrap();
        let (path, file) = target_file(dir.path());
        std::fs::write(transaction_path(&path), b"not a valid receipt").unwrap();

        assert!(recover(&file, &path, 48, 64).is_err());
        assert_eq!(read_token(&file, 48).unwrap(), MutationToken::ZERO);
    }
}
