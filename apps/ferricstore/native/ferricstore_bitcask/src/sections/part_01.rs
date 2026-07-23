
use rustler::{Binary, Encoder, Env, LocalPid, NifResult, OwnedBinary, ResourceArc, Term};
use std::os::unix::fs::FileExt;
use std::sync::{Arc, Mutex, OnceLock, RwLock, Weak};

/// A resource that owns a value buffer read from the Bitcask log.
///
/// When used with `ResourceArc::make_binary`, the BEAM creates a binary term
/// that points directly into this buffer — zero copy from Rust to BEAM.
/// The BEAM's GC tracks the reference: once the Erlang binary term becomes
/// unreachable, the `ResourceArc` ref-count drops to zero and this `Vec` is
/// freed.
///
/// ## Safety invariant
///
/// The `data` field MUST NOT be mutated after the `ResourceArc<ValueBuffer>`
/// is passed to `make_binary`. The returned BEAM binary shares the same
/// backing memory; any mutation would violate the immutability guarantee of
/// Erlang binaries and cause undefined behaviour.
struct ValueBuffer {
    data: Vec<u8>,
}

mod atoms {
    rustler::atoms! {
        ok,
        error,
        nil,
        tokio_complete,
        put,
        delete,
        mismatch,
        miss,
        not_found,
        missing,
        busy,
        value,
        fallback,
        compare_failed,
        range_entry_too_large,
        prefix_merge_byte_budget_exceeded,
        batch_value_budget_exceeded,
        batch_key_budget_exceeded,
        invalid_composite_entry,
    }
}

#[derive(rustler::NifTaggedEnum)]
enum NifBatchWrite<'a> {
    Put(Binary<'a>, Binary<'a>, u64),
    Delete(Binary<'a>),
}

#[derive(rustler::NifTaggedEnum)]
enum LmdbBatchWrite<'a> {
    Put(Binary<'a>, Binary<'a>),
    PutNew(Binary<'a>, Binary<'a>),
    Delete(Binary<'a>),
    Compare(Binary<'a>, Binary<'a>),
    CompareMissing(Binary<'a>),
}

struct LmdbStore {
    env: heed::Env,
    db: heed::Database<heed::types::Bytes, heed::types::Bytes>,
    map_size: usize,
}

type LmdbStoreCell = OnceLock<Result<Arc<LmdbStore>, String>>;

static LMDB_STORES: OnceLock<Mutex<std::collections::HashMap<String, Arc<LmdbStoreCell>>>> =
    OnceLock::new();

struct LmdbValidatedPath {
    cache_key: String,
    store: Weak<LmdbStore>,
}

static LMDB_VALIDATED_PATHS: OnceLock<
    RwLock<std::collections::HashMap<String, LmdbValidatedPath>>,
> = OnceLock::new();

#[allow(non_local_definitions)]
fn load(env: Env, _info: Term) -> bool {
    if let Err(error) = async_io::initialize() {
        eprintln!("ferricstore NIF failed to initialise async runtime: {error}");
        return false;
    }

    let _ = rustler::resource!(ValueBuffer, env);
    flow_index::register_resource(env);
    tdigest::register_resource(env);
    tdigest::register_mmap_resource(env);
    true
}

// ---------------------------------------------------------------------------
// v2 Pure stateless NIF functions — no Store, no Mutex, no keydir in Rust.
// These are the building blocks for the Elixir-owned ETS keydir architecture.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// fadvise helpers — page cache hints for random-access pread patterns.
//
// FADV_RANDOM: disables kernel readahead on the fd. Without this, each pread
// triggers ~128KB of readahead on pages that will never be used (bloom bits,
// CMS counters, Bitcask cold reads are all hash-indexed random access).
//
// FADV_DONTNEED is reserved for explicit Bitcask record ranges after a cold
// value is promoted to ETS. A zero length would mean "to end of file" on
// Linux, so whole-file requests are ignored to preserve shared warm pages.
//
// On non-Linux (macOS), posix_fadvise is not available — these are no-ops.
// ---------------------------------------------------------------------------

/// Open a file for reading with FADV_RANDOM hint (disable readahead).
pub fn open_random_read(path: &std::path::Path) -> std::io::Result<std::fs::File> {
    #[cfg(unix)]
    let file = crate::path_open::open_file_nofollow(path, libc::O_RDONLY, 0)?;

    #[cfg(not(unix))]
    let file = {
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    reject_final_component_symlink(path)?;
        options.open(path)?
    };

    ensure_regular_file(&file, "random-access read target")?;
    fadvise_random(&file);
    Ok(file)
}

/// Open a file for read+write with FADV_RANDOM hint.
pub fn open_random_rw(path: &std::path::Path) -> std::io::Result<std::fs::File> {
    #[cfg(unix)]
    let file = crate::path_open::open_file_nofollow(path, libc::O_RDWR, 0)?;

    #[cfg(not(unix))]
    let file = {
    let mut options = std::fs::OpenOptions::new();
    options.read(true).write(true);
    reject_final_component_symlink(path)?;
        options.open(path)?
    };

    ensure_regular_file(&file, "random-access read-write target")?;
    fadvise_random(&file);
    Ok(file)
}

fn ensure_regular_file(file: &std::fs::File, description: &str) -> std::io::Result<()> {
    if file.metadata()?.file_type().is_file() {
        Ok(())
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("{description} is not a regular file"),
        ))
    }
}

#[cfg(unix)]
#[derive(Debug)]
pub(crate) struct FileLock(std::os::fd::RawFd);

#[cfg(unix)]
impl Drop for FileLock {
    fn drop(&mut self) {
        let _ = unsafe { libc::flock(self.0, libc::LOCK_UN) };
    }
}

#[cfg(not(unix))]
#[derive(Debug)]
pub(crate) struct FileLock;

#[cfg(unix)]
fn lock_file(file: &std::fs::File, operation: libc::c_int) -> std::io::Result<FileLock> {
    use std::os::fd::AsRawFd;

    let fd = file.as_raw_fd();
    loop {
        if unsafe { libc::flock(fd, operation) } == 0 {
            return Ok(FileLock(fd));
        }

        let error = std::io::Error::last_os_error();
        if error.kind() != std::io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

#[cfg(unix)]
pub(crate) fn lock_file_exclusive(file: &std::fs::File) -> std::io::Result<FileLock> {
    lock_file(file, libc::LOCK_EX)
}

#[cfg(unix)]
pub(crate) fn lock_file_shared(file: &std::fs::File) -> std::io::Result<FileLock> {
    lock_file(file, libc::LOCK_SH)
}

#[cfg(not(unix))]
pub(crate) fn lock_file_exclusive(_file: &std::fs::File) -> std::io::Result<FileLock> {
    Ok(FileLock)
}

#[cfg(not(unix))]
pub(crate) fn lock_file_shared(_file: &std::fs::File) -> std::io::Result<FileLock> {
    Ok(FileLock)
}

/// An open sidecar descriptor whose advisory lock is held for its lifetime.
#[derive(Debug)]
pub struct LockedFile {
    // Unlock while `file` is still open. Rust drops fields in declaration order.
    _lock: Option<FileLock>,
    file: std::fs::File,
}

impl std::ops::Deref for LockedFile {
    type Target = std::fs::File;

    fn deref(&self) -> &Self::Target {
        &self.file
    }
}

impl std::ops::DerefMut for LockedFile {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.file
    }
}

/// Open a sidecar for a consistent read. Concurrent readers share the lock;
/// creators and mutators wait until every reader has finished.
pub fn open_random_read_locked(path: &std::path::Path) -> std::io::Result<LockedFile> {
    let file = open_random_read(path)?;
    let lock = lock_file_shared(&file)?;
    Ok(LockedFile {
        _lock: Some(lock),
        file,
    })
}

/// Open a sidecar for a serialized read-modify-write operation.
pub fn open_random_rw_locked(path: &std::path::Path) -> std::io::Result<LockedFile> {
    let file = open_random_rw(path)?;
    let lock = lock_file_exclusive(&file)?;
    Ok(LockedFile {
        _lock: Some(lock),
        file,
    })
}

#[cfg(unix)]
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct FileIdentity {
    device: u64,
    inode: u64,
}

#[cfg(unix)]
fn file_identity(file: &std::fs::File) -> std::io::Result<FileIdentity> {
    use std::os::unix::fs::MetadataExt;

    let metadata = file.metadata()?;
    Ok(FileIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    })
}

#[cfg(not(unix))]
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct FileIdentity(u64);

#[cfg(not(unix))]
fn file_identity(_file: &std::fs::File) -> std::io::Result<FileIdentity> {
    static NEXT_IDENTITY: std::sync::atomic::AtomicU64 =
        std::sync::atomic::AtomicU64::new(0);
    Ok(FileIdentity(NEXT_IDENTITY.fetch_add(
        1,
        std::sync::atomic::Ordering::Relaxed,
    )))
}

/// A destination and its merge sources protected as one deadlock-free lock set.
pub(crate) struct MergeLockedFiles {
    // Release locks while every descriptor is still open.
    _locks: Vec<FileLock>,
    pub(crate) destination: std::fs::File,
    pub(crate) sources: Vec<std::fs::File>,
}

#[derive(Clone, Copy)]
enum MergeLockTarget {
    Destination,
    Source(usize),
}

/// Open every file in a read-modify-write merge and acquire unique inode locks
/// in one global order. Opposing merges therefore cannot form an A->B/B->A
/// lock cycle, and destination hard-link aliases reuse its exclusive lock.
pub(crate) fn open_random_merge_locked(
    destination_path: &std::path::Path,
    source_paths: &[&std::path::Path],
) -> std::io::Result<MergeLockedFiles> {
    let destination = open_random_rw(destination_path)?;
    let mut sources = Vec::new();
    sources.try_reserve_exact(source_paths.len()).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::OutOfMemory,
            "merge source descriptor allocation failed",
        )
    })?;
    for path in source_paths {
        sources.push(open_random_read(path)?);
    }

    let mut targets = std::collections::BTreeMap::new();
    for (index, source) in sources.iter().enumerate() {
        targets
            .entry(file_identity(source)?)
            .or_insert(MergeLockTarget::Source(index));
    }
    targets.insert(
        file_identity(&destination)?,
        MergeLockTarget::Destination,
    );

    let mut locks = Vec::new();
    locks.try_reserve_exact(targets.len()).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::OutOfMemory,
            "merge lock allocation failed",
        )
    })?;
    for target in targets.into_values() {
        let lock = match target {
            MergeLockTarget::Destination => lock_file_exclusive(&destination)?,
            MergeLockTarget::Source(index) => lock_file_shared(&sources[index])?,
        };
        locks.push(lock);
    }

    Ok(MergeLockedFiles {
        _locks: locks,
        destination,
        sources,
    })
}

static SIDECAR_TEMP_SEQUENCE: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

pub struct StagedFile {
    file: LockedFile,
    temp_path: std::path::PathBuf,
    destination_path: std::path::PathBuf,
    published: bool,
}

impl std::ops::Deref for StagedFile {
    type Target = std::fs::File;

    fn deref(&self) -> &Self::Target {
        &self.file
    }
}

impl std::ops::DerefMut for StagedFile {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.file
    }
}

impl StagedFile {
    /// Sync a complete sidecar and publish it with one same-directory rename.
    pub fn publish(mut self) -> std::io::Result<()> {
        self.file.sync_all()?;
        #[cfg(unix)]
        crate::path_open::rename_nofollow(&self.temp_path, &self.destination_path)?;
        #[cfg(not(unix))]
        std::fs::rename(&self.temp_path, &self.destination_path)?;
        self.published = true;

        let parent = self
            .destination_path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| std::path::Path::new("."));
        sync_sidecar_directory(parent)
    }
}

impl Drop for StagedFile {
    fn drop(&mut self) {
        if !self.published {
            #[cfg(unix)]
            let _ = crate::path_open::remove_file_nofollow(&self.temp_path);
            #[cfg(not(unix))]
            let _ = std::fs::remove_file(&self.temp_path);
        }
    }
}

/// Create an exclusively locked same-directory temporary sidecar. The final
/// path remains absent (or retains its prior complete inode) until `publish`.
pub fn create_staged_locked_nofollow(path: &std::path::Path) -> std::io::Result<StagedFile> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| std::path::Path::new("."));
    let file_name = path.file_name().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "sidecar path has no file name")
    })?;

    for _ in 0..128 {
        let sequence = SIDECAR_TEMP_SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let temp_path = parent.join(format!(
            ".{}.ferric-sidecar-{}-{sequence}",
            file_name.to_string_lossy(),
            std::process::id()
        ));
        #[cfg(unix)]
        let file_result = crate::path_open::open_file_nofollow(
            &temp_path,
            libc::O_RDWR | libc::O_CREAT | libc::O_EXCL,
            0o600,
        );

        #[cfg(not(unix))]
        let file_result = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&temp_path);

        match file_result {
            Ok(file) => {
                let lock = lock_file_exclusive(&file)?;
                return Ok(StagedFile {
                    file: LockedFile {
                        _lock: Some(lock),
                        file,
                    },
                    temp_path,
                    destination_path: path.to_path_buf(),
                    published: false,
                });
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }

    Err(std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        "could not allocate sidecar staging file",
    ))
}

#[cfg(unix)]
fn sync_sidecar_directory(path: &std::path::Path) -> std::io::Result<()> {
    crate::path_open::open_directory_nofollow(path)?.sync_all()
}

#[cfg(not(unix))]
fn sync_sidecar_directory(path: &std::path::Path) -> std::io::Result<()> {
    std::fs::File::open(path)?.sync_all()
}

/// Open or create an append-only log without following a final-component symlink.
pub(crate) fn open_append_nofollow(
    path: &std::path::Path,
) -> std::io::Result<std::fs::File> {
    #[cfg(unix)]
    let file = crate::path_open::open_file_nofollow(
        path,
        libc::O_WRONLY | libc::O_CREAT | libc::O_APPEND,
        0o600,
    )?;

    #[cfg(not(unix))]
    let file = {
    let mut options = std::fs::OpenOptions::new();
    options.create(true).append(true);
    reject_final_component_symlink(path)?;
        options.open(path)?
    };

    ensure_private_storage_file(&file, "append target")?;

    Ok(file)
}

/// Open or create a random-access log without following a final-component symlink.
#[cfg(target_os = "linux")]
pub(crate) fn open_rw_create_nofollow(
    path: &std::path::Path,
) -> std::io::Result<std::fs::File> {
    let file = crate::path_open::open_file_nofollow(
        path,
        libc::O_RDWR | libc::O_CREAT,
        0o600,
    )?;
    ensure_private_storage_file(&file, "random-access log target")?;
    Ok(file)
}

/// Open an existing file for write-only maintenance without following a symlink.
pub(crate) fn open_write_nofollow(
    path: &std::path::Path,
) -> std::io::Result<std::fs::File> {
    #[cfg(unix)]
    let file = crate::path_open::open_file_nofollow(path, libc::O_WRONLY, 0)?;

    #[cfg(not(unix))]
    let file = {
    let mut options = std::fs::OpenOptions::new();
    options.write(true);
    reject_final_component_symlink(path)?;
        options.open(path)?
    };

    ensure_regular_file(&file, "maintenance write target")?;
    Ok(file)
}

/// Create or truncate a regular file without following a final-component
/// symlink. Probabilistic sidecar creation uses this before publishing metadata.
pub fn create_truncate_nofollow(path: &std::path::Path) -> std::io::Result<std::fs::File> {
    #[cfg(unix)]
    let file = crate::path_open::open_file_nofollow(
        path,
        libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC,
        0o600,
    )?;

    #[cfg(not(unix))]
    let file = {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    reject_final_component_symlink(path)?;
        options.open(path)?
    };

    ensure_private_storage_file(&file, "truncate target")?;
    Ok(file)
}

#[cfg(unix)]
fn ensure_private_storage_file(file: &std::fs::File, description: &str) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let metadata = file.metadata()?;
    if !metadata.file_type().is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("{description} is not a regular file"),
        ));
    }
    if private_storage_mode_needs_repair(metadata.permissions().mode()) {
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn ensure_private_storage_file(file: &std::fs::File, description: &str) -> std::io::Result<()> {
    ensure_regular_file(file, description)
}

#[cfg(unix)]
fn private_storage_mode_needs_repair(mode: u32) -> bool {
    mode & 0o7777 != 0o600
}

#[cfg(not(unix))]
fn reject_final_component_symlink(path: &std::path::Path) -> std::io::Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "final path component is a symlink",
        )),
        Ok(_metadata) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

/// Hint the kernel that this fd will be accessed randomly (disable readahead).
#[cfg(target_os = "linux")]
pub fn fadvise_random(file: &std::fs::File) {
    use std::os::unix::io::AsRawFd;
    unsafe {
        libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_RANDOM);
    }
}

#[cfg(not(target_os = "linux"))]
pub fn fadvise_random(_file: &std::fs::File) {}

/// Hint the kernel to evict pages at [offset, offset+len] from page cache.
#[cfg(target_os = "linux")]
#[inline]
pub fn fadvise_dontneed(file: &std::fs::File, offset: i64, len: i64) {
    use std::os::unix::io::AsRawFd;
    let Some((offset, len)) = bounded_dontneed_range(offset, len) else {
        return;
    };
    unsafe {
        libc::posix_fadvise(file.as_raw_fd(), offset, len, libc::POSIX_FADV_DONTNEED);
    }
}

#[cfg(not(target_os = "linux"))]
pub fn fadvise_dontneed(_file: &std::fs::File, _offset: i64, _len: i64) {}

#[cfg(any(target_os = "linux", test))]
fn bounded_dontneed_range(offset: i64, len: i64) -> Option<(i64, i64)> {
    (offset >= 0 && len > 0).then_some((offset, len))
}

/// Fsync a directory so that filename-to-inode mappings (dir entries) are
/// durable. Required after `File::create`, `rename`, `remove_file`, or
/// `touch` of any file inside a directory whose existence must survive a
/// kernel panic.
///
/// POSIX: a file's data `fsync` does NOT make the filename entry durable;
/// only the parent directory's fsync does that. Without this call, a
/// kernel panic after a rename/rm can leave the directory in a state
/// where the filename mapping doesn't match what the caller expected —
/// e.g. a freshly-compacted `00003.log` still shows as `compact_3.log`
/// because the rename never flushed to disk.
///
/// Uses `File::open` (read-only) + `sync_data()` which is valid for
/// directories on Linux and macOS. Empty path returns Err without
/// opening.
pub fn fsync_dir(path: &str) -> Result<(), String> {
    if path.is_empty() {
        return Err("empty path".to_string());
    }

    let path = std::path::Path::new(path);

    #[cfg(unix)]
    let dir =
        crate::path_open::open_directory_nofollow(path).map_err(|e| format!("open dir: {e}"))?;

    #[cfg(not(unix))]
    let dir = {
        reject_final_component_symlink(path).map_err(|e| format!("open dir: {e}"))?;
        let dir = std::fs::File::open(path).map_err(|e| format!("open dir: {e}"))?;
        if !dir
            .metadata()
            .map_err(|e| format!("stat dir: {e}"))?
            .is_dir()
        {
            return Err("open dir: path is not a directory".to_string());
        }
        dir
    };

    dir.sync_data().map_err(|e| format!("sync_data: {e}"))
}

/// Fsync a prob file (bloom/cuckoo/cms/topk) after a write before returning
/// `:ok` to the caller. Without this, writes go to the OS page cache only
/// and a kernel panic between the write and the background pagecache flush
/// would lose the data.
///
/// For bloom: bit-set is idempotent on Ra replay but the header `count`
/// field can desync with actual bits set (breaks `BF.CARD`).
/// For cuckoo: kick-chain partial writes corrupt the filter; replay is
/// NOT safe.
/// For cms: read-modify-write counters double-count on replay.
/// For topk: heap state corruption on partial writes.
///
/// Returns the formatted error string on failure so callers can propagate
/// it as `{:error, reason}` to Elixir. Uses `sync_data()` (fdatasync) — we
/// don't need metadata durability here, the file's size/perms never change
/// after create.
pub fn prob_fsync(file: &std::fs::File) -> Result<(), String> {
    file.sync_data().map_err(|e| format!("sync_data: {e}"))
}

/// Positioned write that rejects short writes.
///
/// POSIX `pwrite` may write fewer bytes than requested. The probabilistic
/// file formats update fixed-width counters/slots in place, so callers must
/// not treat a partial write as success.
pub(crate) fn write_all_at(
    file: &std::fs::File,
    mut buf: &[u8],
    mut offset: u64,
    label: &str,
) -> Result<(), String> {
    while !buf.is_empty() {
        match file.write_at(buf, offset) {
            Ok(0) => return Err(format!("short pwrite {label}: wrote 0 bytes")),
            Ok(n) => {
                buf = &buf[n..];
                offset += n as u64;
            }
            Err(e) => return Err(format!("pwrite {label}: {e}")),
        }
    }

    Ok(())
}

/// Parse the numeric file_id from a log file path.
///
/// L-NEW-1 fix: `"00000000000000000000".trim_start_matches('0')` produces `""`
/// which fails to parse as u64, accidentally falling through to `unwrap_or(0)`.
/// This function handles the all-zeros case explicitly, matching the pattern
/// used in `store.rs::collect_file_ids`.
fn parse_file_id(path: &std::path::Path) -> u64 {
    path.file_stem().and_then(|s| s.to_str()).map_or(0, |stem| {
        let trimmed = stem.trim_start_matches('0');
        if trimmed.is_empty() {
            // All zeros (e.g. "00000000000000000000.log") → file_id 0
            0
        } else {
            trimmed.parse::<u64>().unwrap_or(0)
        }
    })
}

/// Append a record to a data file. Returns `{:ok, {offset, record_size}}`.
///
/// Pure I/O — no keydir, no Mutex for reads.
/// The caller (Elixir Shard GenServer) serialises writes.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
fn v2_append_record<'a>(
    env: Env<'a>,
    path: String,
    key: Binary,
    value: Binary,
    expire_at_ms: u64,
) -> NifResult<Term<'a>> {
    use crate::log::validate_kv_sizes;

    if let Err(msg) = validate_kv_sizes(key.as_slice(), value.as_slice()) {
        return Ok((atoms::error(), msg).encode(env));
    }

    let p = std::path::Path::new(&path);
    let file_id = parse_file_id(p);

    // M-NEW-1 fix: use open_small (8KB buffer) for single-record writes to
    // avoid allocating a 256KB BufWriter that is used once and dropped.
    match log::LogWriter::open_small(p, file_id) {
        Ok(mut writer) => {
            let offset = writer
                .write_sync(key.as_slice(), value.as_slice(), expire_at_ms)
                .map_err(|e| rustler::Error::Term(Box::new(e.to_string())))?;
            let record_size =
                (log::HEADER_SIZE + key.as_slice().len() + value.as_slice().len()) as u64;
            Ok((atoms::ok(), (offset, record_size)).encode(env))
        }
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

/// Append a tombstone record (logical delete) to a data file.
/// Returns `{:ok, {offset, record_size}}`.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
fn v2_append_tombstone<'a>(env: Env<'a>, path: String, key: Binary) -> NifResult<Term<'a>> {
    let p = std::path::Path::new(&path);
    let file_id = parse_file_id(p);

    // M-NEW-1 fix: use open_small (8KB buffer) for single-record writes.
    match log::LogWriter::open_small(p, file_id) {
        Ok(mut writer) => {
            let offset = writer
                .write_tombstone_sync(key.as_slice())
                .map_err(|e| rustler::Error::Term(Box::new(e.to_string())))?;
            let record_size = (log::HEADER_SIZE + key.as_slice().len()) as u64;
            Ok((atoms::ok(), (offset, record_size)).encode(env))
        }
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

/// Append a batch of records with a single fsync. Returns
/// `{:ok, [{offset, value_size}, ...]}`.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
fn v2_append_batch<'a>(
    env: Env<'a>,
    path: String,
    records: Vec<(Binary<'a>, Binary<'a>, u64)>,
) -> NifResult<Term<'a>> {
    let p = std::path::Path::new(&path);
    let file_id = parse_file_id(p);

    match log::LogWriter::open(p, file_id) {
        Ok(mut writer) => {
            let entries: Vec<(&[u8], &[u8], u64)> = records
                .iter()
                .map(|(k, v, exp)| (k.as_slice(), v.as_slice(), *exp))
                .collect();

            match writer.write_batch(&entries) {
                Ok(results) => {
                    let tuples: Vec<(u64, usize)> = results;
                    Ok((atoms::ok(), tuples).encode(env))
                }
                Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
            }
        }
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

/// Read the value at a specific offset in a data file. Validates CRC.
/// Returns `{:ok, value_binary}` or `{:error, reason}`.
///
/// This is the cold-read path: ETS has the key's file_id, offset, value_size
/// but not the value bytes. We pread from disk and return the value.
///
/// No Mutex needed — pread is stateless and thread-safe.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
fn v2_pread_at(env: Env<'_>, path: String, offset: u64) -> NifResult<Term<'_>> {
    let p = std::path::Path::new(&path);

    // C-2/C-6 fix: use File::open + pread_record directly instead of
    // LogReader::open which does open + fstat + seek (4 syscalls).
    // File::open + pread = 2 syscalls (open + pread).
    // Future optimization: cache fds per shard in a global fd pool.
    match open_random_read(p) {
        Ok(file) => {
            fadvise_random(&file);
            match log::pread_record_from_file(&file, offset) {
                Ok(Some(record)) => {
                    // Hint kernel to evict the pages — value is promoted to ETS,
                    // the page cache copy is never needed again.
                    let record_size = (log::HEADER_SIZE
                        + record.key.len()
                        + record.value.as_ref().map_or(0, Vec::len))
                        as i64;
                    fadvise_dontneed(&file, offset as i64, record_size);

                    match record.value {
                        Some(value) => {
                            let resource = ResourceArc::new(ValueBuffer { data: value });
                            let binary = resource.make_binary(env, |vb| &vb.data);
                            Ok((atoms::ok(), binary).encode(env))
                        }
                        None => Ok((atoms::ok(), atoms::nil()).encode(env)),
                    }
                }
                Ok(None) => Ok((atoms::error(), "offset past EOF").encode(env)),
                Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
            }
        }
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

fn read_exact_at_for_ref(
    file: &std::fs::File,
    buf: &mut [u8],
    offset: u64,
) -> Result<bool, String> {
    let mut read_any = false;
    let mut total = 0;

    while total < buf.len() {
        let read_offset = offset
            .checked_add(total as u64)
            .ok_or_else(|| "file ref validation offset overflow".to_string())?;

        match file.read_at(&mut buf[total..], read_offset) {
            Ok(0) if !read_any => return Ok(false),
            Ok(0) => return Err("short read while validating file ref".to_string()),
            Ok(n) => {
                read_any = true;
                total += n;
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e.to_string()),
        }
    }

    Ok(true)
}

fn validate_value_ref_from_file(
    file: &std::fs::File,
    offset: u64,
    expected_key: &[u8],
    expected_value_size: u64,
) -> Result<Option<(u64, u64)>, String> {
    let mut header = [0u8; log::HEADER_SIZE];
    if !read_exact_at_for_ref(file, &mut header, offset)? {
        return Ok(None);
    }

    let stored_crc = u32::from_le_bytes(header[0..4].try_into().unwrap());
    let key_size = u16::from_le_bytes(header[20..22].try_into().unwrap()) as usize;
    let value_size_raw = u32::from_le_bytes(header[22..26].try_into().unwrap());

    if value_size_raw == log::TOMBSTONE || u64::from(value_size_raw) != expected_value_size {
        return Ok(None);
    }

    let mut key = vec![0u8; key_size];
    if key_size > 0 {
        let key_offset = offset
            .checked_add(log::HEADER_SIZE as u64)
            .ok_or_else(|| "file ref key offset overflow".to_string())?;
        if !read_exact_at_for_ref(file, &mut key, key_offset)? {
            return Err("short read while validating file ref key".to_string());
        }
    }

    if key != expected_key {
        return Ok(None);
    }

    let value_offset = offset
        .checked_add(log::HEADER_SIZE as u64)
        .and_then(|off| off.checked_add(key_size as u64))
        .ok_or_else(|| "file ref value offset overflow".to_string())?;

    validate_file_ref_crc(
        file,
        value_offset,
        expected_value_size,
        &header,
        &key,
        stored_crc,
    )?;

    Ok(Some((value_offset, expected_value_size)))
}

fn validate_file_ref_crc(
    file: &std::fs::File,
    value_offset: u64,
    value_size: u64,
    header: &[u8; log::HEADER_SIZE],
    key: &[u8],
    stored_crc: u32,
) -> Result<(), String> {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&header[4..]);
    hasher.update(key);

    let mut remaining = value_size;
    let mut read_offset = value_offset;
    let mut buf = vec![0u8; 64 * 1024];

    while remaining > 0 {
        let read_size = usize::try_from(remaining.min(buf.len() as u64))
            .map_err(|_| "file ref value read size overflow".to_string())?;
        let chunk = &mut buf[..read_size];
        if !read_exact_at_for_ref(file, chunk, read_offset)? {
            return Err("short read while validating file ref value".to_string());
        }
        hasher.update(chunk);
        read_offset = read_offset
            .checked_add(read_size as u64)
            .ok_or_else(|| "file ref value offset overflow".to_string())?;
        remaining -= read_size as u64;
    }

    let computed_crc = hasher.finalize();
    if computed_crc == stored_crc {
        Ok(())
    } else {
        Err(format!(
            "CRC mismatch: stored={stored_crc}, computed={computed_crc}"
        ))
    }
}
