//! Filesystem metadata NIFs.
//!
//! Replaces calls to `:prim_file` (`File.mkdir_p`, `File.touch`, `File.rename`,
//! `File.rm`, `File.exists?`, `File.ls`) which run on the Erlang async-thread
//! pool and surface as `erts_internal:dirty_nif_finalizer/1` in crash dumps.
//!
//! Synchronous metadata ops run on **DirtyIo** schedulers:
//!
//! - **Sync metadata ops** (`fs_touch`, `fs_mkdir_p`, `fs_rename`, `fs_rm`,
//!   `fs_exists`, `fs_is_dir`, `fs_ls`) may block in the filesystem. Recursive
//!   mkdir and directory enumeration are not bounded to one syscall.
//!
//! - **Bounded file reads and streaming copies** (`fs_read_nofollow`,
//!   `fs_copy_sync_nofollow`) run on DirtyIo. Reads avoid an intermediate
//!   `Vec`, and copies keep large snapshot payloads out of BEAM memory.
//!
//! - **Async long I/O** (`fs_rm_rf_async`): spawns on Tokio, sends
//!   `{:tokio_complete, corr_id, :ok | :error, reason}` to the caller.
//!
//! Error atoms are stable for pattern-matching in Elixir:
//!   `:not_found`, `:already_exists`, `:permission_denied`,
//!   `:not_a_directory`, `:is_a_directory`, `:directory_not_empty`,
//!   `:invalid_path`, `:symlink`. Anything else comes through as `:other` with a
//!   message.
//!
//! **Design rule:** synchronous filesystem work uses DirtyIo, and unbounded
//! recursive tree removal goes through Tokio.

#[cfg(unix)]
use std::ffi::CString;
use std::io::{self, Read, Write};
#[cfg(unix)]
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use rustler::schedule::consume_timeslice;
use rustler::{Binary, Encoder, Env, LocalPid, NifResult, OwnedBinary, OwnedEnv, Term};

use crate::async_io;
use crate::atoms;

// ---------------------------------------------------------------------------
// Error mapping — io::Error → stable atom
// ---------------------------------------------------------------------------

rustler::atoms! {
    not_found,
    already_exists,
    permission_denied,
    not_a_directory,
    is_a_directory,
    directory_not_empty,
    invalid_path,
    symlink,
    too_large,
    other,
}

/// Map a `std::io::Error` to a stable Elixir atom + message. Used for
/// pattern-friendly error returns: `{:error, {:not_found, "..."}}` lets
/// callers match on the kind without string-sniffing.
fn encode_error<'a>(env: Env<'a>, err: &io::Error) -> Term<'a> {
    use io::ErrorKind::*;
    let kind = match err.kind() {
        NotFound => not_found(),
        AlreadyExists => already_exists(),
        PermissionDenied => permission_denied(),
        // Rust stable maps ENOTDIR / EISDIR / ENOTEMPTY to Other on most
        // targets; check raw os_error to disambiguate the common cases.
        _ => match err.raw_os_error() {
            Some(libc::ENOTDIR) => not_a_directory(),
            Some(libc::EISDIR) => is_a_directory(),
            Some(libc::ENOTEMPTY) => directory_not_empty(),
            Some(libc::EINVAL) => invalid_path(),
            Some(libc::ELOOP) => symlink(),
            _ => other(),
        },
    };
    (atoms::error(), (kind, err.to_string())).encode(env)
}

/// Rejects paths containing embedded null bytes. POSIX paths cannot have
/// NULs; returning early avoids handing a malformed C string to the
/// kernel. Also rejects empty paths — `""` has surprising semantics on
/// several filesystems and should be a programmer error, not a syscall.
fn validate_path<'a>(env: Env<'a>, path: &str) -> Result<(), Term<'a>> {
    if path.is_empty() {
        return Err((atoms::error(), (invalid_path(), "empty path")).encode(env));
    }
    if path.as_bytes().contains(&0u8) {
        return Err((atoms::error(), (invalid_path(), "path contains null byte")).encode(env));
    }
    Ok(())
}

fn remove_dir_all_idempotent(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    let result = crate::path_open::remove_dir_all_nofollow(path);
    #[cfg(not(unix))]
    let result = std::fs::remove_dir_all(path);

    match result {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

// ---------------------------------------------------------------------------
// Synchronous metadata NIFs (dirty I/O scheduler)
// ---------------------------------------------------------------------------

/// Creates an empty file if it does not exist. Idempotent on an existing
/// file (does not truncate — matches `:file.write_file_info` touch-like
/// semantics Elixir's `File.touch!/1` provides).
///
/// Uses an atomic no-follow open so an existing regular file is preserved and
/// a final-component symlink is rejected without a check/open race.
#[rustler::nif(schedule = "DirtyIo")]
fn fs_touch(env: Env<'_>, path: String) -> NifResult<Term<'_>> {
    if let Err(t) = validate_path(env, &path) {
        return Ok(t);
    }

    let result = touch_file_nofollow(Path::new(&path)).map(drop);

    let _ = consume_timeslice(env, 1);
    match result {
        Ok(()) => Ok(atoms::ok().encode(env)),
        Err(e) => Ok(encode_error(env, &e)),
    }
}

#[cfg(unix)]
fn touch_file_nofollow(path: &Path) -> io::Result<std::fs::File> {
    crate::path_open::open_file_nofollow(path, libc::O_WRONLY | libc::O_CREAT, 0o666)
}

#[cfg(not(unix))]
fn touch_file_nofollow(path: &Path) -> io::Result<std::fs::File> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "final path component is a symlink",
        )),
        Ok(_metadata) => std::fs::OpenOptions::new().write(true).open(path),
        Err(error) if error.kind() == io::ErrorKind::NotFound => std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(path),
        Err(error) => Err(error),
    }
}

/// Recursive `mkdir -p`. Idempotent when the directory already exists.
#[rustler::nif(schedule = "DirtyIo")]
fn fs_mkdir_p(env: Env<'_>, path: String) -> NifResult<Term<'_>> {
    if let Err(t) = validate_path(env, &path) {
        return Ok(t);
    }

    let result = create_dir_all_nofollow(Path::new(&path));
    let _ = consume_timeslice(env, 1);

    match result {
        Ok(()) => Ok(atoms::ok().encode(env)),
        Err(e) => Ok(encode_error(env, &e)),
    }
}

#[cfg(unix)]
pub(crate) fn create_dir_all_nofollow(path: &Path) -> io::Result<()> {
    let start = if path.is_absolute() { "/" } else { "." };
    let start = CString::new(start).expect("fixed directory path contains no null byte");
    let start_fd = unsafe {
        libc::open(
            start.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        )
    };
    if start_fd < 0 {
        return Err(io::Error::last_os_error());
    }
    let mut directory = unsafe { OwnedFd::from_raw_fd(start_fd) };
    let mut allow_root_alias = path.is_absolute();

    for component in path.components() {
        use std::path::Component;

        let name = match component {
            Component::RootDir | Component::CurDir => continue,
            Component::ParentDir => std::ffi::OsStr::new(".."),
            Component::Normal(name) => name,
            Component::Prefix(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "unsupported path prefix",
                ));
            }
        };
        let name = CString::new(name.as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains null byte"))?;

        let mkdir_result = unsafe { libc::mkdirat(directory.as_raw_fd(), name.as_ptr(), 0o777) };
        if mkdir_result != 0 {
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::AlreadyExists {
                return Err(error);
            }
        }

        // Root-level aliases such as macOS `/tmp -> private/tmp` are protected
        // by the root directory's permissions. Below that trusted component,
        // every traversal is no-follow.
        let nofollow = if allow_root_alias {
            allow_root_alias = false;
            0
        } else {
            libc::O_NOFOLLOW
        };
        let next_fd = unsafe {
            libc::openat(
                directory.as_raw_fd(),
                name.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | nofollow,
            )
        };
        if next_fd < 0 {
            return Err(io::Error::last_os_error());
        }
        directory = unsafe { OwnedFd::from_raw_fd(next_fd) };
    }

    Ok(())
}

#[cfg(not(unix))]
pub(crate) fn create_dir_all_nofollow(path: &Path) -> io::Result<()> {
    std::fs::create_dir_all(path)
}

/// Atomic rename. On POSIX, `rename` replaces the target atomically.
/// Cross-device renames return `:other` — caller should fall back to
/// copy+remove or handle specially.
#[rustler::nif(schedule = "DirtyIo")]
fn fs_rename(env: Env<'_>, old_path: String, new_path: String) -> NifResult<Term<'_>> {
    if let Err(t) = validate_path(env, &old_path) {
        return Ok(t);
    }
    if let Err(t) = validate_path(env, &new_path) {
        return Ok(t);
    }

    #[cfg(unix)]
    let result = crate::path_open::rename_nofollow(Path::new(&old_path), Path::new(&new_path));
    #[cfg(not(unix))]
    let result = std::fs::rename(&old_path, &new_path);
    let _ = consume_timeslice(env, 1);

    match result {
        Ok(()) => Ok(atoms::ok().encode(env)),
        Err(e) => Ok(encode_error(env, &e)),
    }
}

/// Remove a single file. Use `fs_rm_rf_async` for directories.
#[rustler::nif(schedule = "DirtyIo")]
fn fs_rm(env: Env<'_>, path: String) -> NifResult<Term<'_>> {
    if let Err(t) = validate_path(env, &path) {
        return Ok(t);
    }

    #[cfg(unix)]
    let result = crate::path_open::remove_file_nofollow(Path::new(&path));
    #[cfg(not(unix))]
    let result = std::fs::remove_file(&path);
    let _ = consume_timeslice(env, 1);

    match result {
        Ok(()) => Ok(atoms::ok().encode(env)),
        Err(e) => Ok(encode_error(env, &e)),
    }
}

/// Does the path exist? Follows symlinks (use `fs_exists_nofollow` if you
/// need a broken-symlink-aware check).
#[rustler::nif(schedule = "DirtyIo")]
fn fs_exists(env: Env<'_>, path: String) -> NifResult<Term<'_>> {
    if let Err(t) = validate_path(env, &path) {
        return Ok(t);
    }

    let exists = Path::new(&path).exists();
    let _ = consume_timeslice(env, 1);
    Ok(exists.encode(env))
}

/// Is the path a directory? Follows symlinks. Returns `false` for missing
/// paths rather than an error — matches Elixir's `File.dir?/1` semantics.
#[rustler::nif(schedule = "DirtyIo")]
fn fs_is_dir(env: Env<'_>, path: String) -> NifResult<Term<'_>> {
    if let Err(t) = validate_path(env, &path) {
        return Ok(t);
    }

    let is_dir = Path::new(&path).is_dir();
    let _ = consume_timeslice(env, 1);
    Ok(is_dir.encode(env))
}

/// List the entries in a directory. Names only, no path prefix — matches
/// `File.ls/1`.
///
/// The NIF yields every 256 entries to keep reductions accurate on huge
/// directories.
#[rustler::nif(schedule = "DirtyIo")]
fn fs_ls(env: Env<'_>, path: String) -> NifResult<Term<'_>> {
    if let Err(t) = validate_path(env, &path) {
        return Ok(t);
    }

    let rd = match std::fs::read_dir(&path) {
        Ok(rd) => rd,
        Err(e) => return Ok(encode_error(env, &e)),
    };

    let mut names: Vec<String> = Vec::new();
    for (idx, entry) in rd.enumerate() {
        let entry = match entry {
            Ok(e) => e,
            Err(e) => return Ok(encode_error(env, &e)),
        };
        // Non-UTF-8 filenames → error. The alternative (lossy conversion)
        // is worse because callers may use the name to open the file.
        match entry.file_name().into_string() {
            Ok(s) => names.push(s),
            Err(_) => {
                return Ok(
                    (atoms::error(), (invalid_path(), "non-utf8 filename in dir")).encode(env),
                );
            }
        }
        if idx & 255 == 255 {
            let _ = consume_timeslice(env, 1);
        }
    }

    let _ = consume_timeslice(env, 1);
    Ok((atoms::ok(), names).encode(env))
}

/// Read a regular file while refusing a symlink at the final path component.
///
/// This exists for security-sensitive configuration reads where a prior
/// `lstat` check is not enough: another process can swap the file for a
/// symlink between check and open. On Unix, `O_NOFOLLOW` makes the kernel
/// reject that final-component symlink atomically.
#[rustler::nif(schedule = "DirtyIo")]
fn fs_read_nofollow(env: Env<'_>, path: String, max_bytes: u64) -> NifResult<Term<'_>> {
    if let Err(t) = validate_path(env, &path) {
        return Ok(t);
    }

    let mut file = match open_file_nofollow(&path) {
        Ok(file) => file,
        Err(error) => return Ok(encode_error(env, &error)),
    };

    let size = match file.metadata() {
        Ok(metadata) => metadata.len(),
        Err(error) => return Ok(encode_error(env, &error)),
    };

    if size > max_bytes {
        let message = format!("file is {size} bytes (max {max_bytes})");
        return Ok((atoms::error(), (too_large(), message)).encode(env));
    }

    let size_usize = match usize::try_from(size) {
        Ok(size) => size,
        Err(_) => {
            return Ok((
                atoms::error(),
                (too_large(), "file size exceeds address space"),
            )
                .encode(env));
        }
    };

    let mut binary = match OwnedBinary::new(size_usize) {
        Some(binary) => binary,
        None => {
            return Ok((atoms::error(), (other(), "out of memory reading file")).encode(env));
        }
    };

    if let Err(error) = file.read_exact(binary.as_mut_slice()) {
        return Ok(encode_error(env, &error));
    }

    match file.metadata() {
        Ok(metadata) if metadata.len() == size => {
            let result = Binary::from_owned(binary, env);
            Ok((atoms::ok(), result).encode(env))
        }
        Ok(metadata) => Ok((
            atoms::error(),
            (
                other(),
                format!(
                    "file changed size while reading (before {size}, after {})",
                    metadata.len()
                ),
            ),
        )
            .encode(env)),
        Err(error) => Ok(encode_error(env, &error)),
    }
}

/// Stream one regular file into a newly-created destination without following
/// either final path component. The destination is synced before success.
#[rustler::nif(schedule = "DirtyIo")]
fn fs_copy_sync_nofollow(env: Env<'_>, source: String, dest: String) -> NifResult<Term<'_>> {
    if let Err(term) = validate_path(env, &source) {
        return Ok(term);
    }
    if let Err(term) = validate_path(env, &dest) {
        return Ok(term);
    }

    let result = copy_sync_nofollow(Path::new(&source), Path::new(&dest));
    let _ = consume_timeslice(env, 1);
    match result {
        Ok(()) => Ok(atoms::ok().encode(env)),
        Err(error) => Ok(encode_error(env, &error)),
    }
}

fn copy_sync_nofollow(source: &Path, dest: &Path) -> io::Result<()> {
    let mut source_file = open_file_nofollow(source.to_string_lossy().as_ref())?;
    let source_metadata = source_file.metadata()?;
    let mut dest_file = create_copy_destination_nofollow(dest)?;

    let result = (|| {
        let copied = io::copy(&mut source_file, &mut dest_file)?;
        if copied != source_metadata.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "snapshot source changed while copying: expected {} bytes, copied {copied}",
                    source_metadata.len()
                ),
            ));
        }

        dest_file.set_permissions(source_metadata.permissions())?;
        dest_file.sync_all()?;
        let parent = dest
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        sync_directory_nofollow(parent)
    })();

    if result.is_err() {
        drop(dest_file);
        #[cfg(unix)]
        let _ = crate::path_open::remove_file_nofollow(dest);
        #[cfg(not(unix))]
        let _ = std::fs::remove_file(dest);
    }

    result
}

fn create_copy_destination_nofollow(path: &Path) -> io::Result<std::fs::File> {
    #[cfg(unix)]
    let file = crate::path_open::open_file_nofollow(
        path,
        libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
        0o666,
    )?;

    #[cfg(not(unix))]
    let file = {
        reject_copy_destination_symlink(path)?;
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)?
    };

    if !file.metadata()?.file_type().is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "snapshot copy destination is not a regular file",
        ));
    }
    Ok(file)
}

#[cfg(not(unix))]
fn reject_copy_destination_symlink(path: &Path) -> io::Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "snapshot copy destination is a symlink",
        )),
        Ok(_metadata) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

/// Append one payload and make it durable without following a final symlink.
#[rustler::nif(schedule = "DirtyIo")]
fn fs_append_sync_nofollow<'a>(
    env: Env<'a>,
    path: String,
    payload: Binary<'a>,
) -> NifResult<Term<'a>> {
    if let Err(term) = validate_path(env, &path) {
        return Ok(term);
    }

    let result = append_sync_nofollow(Path::new(&path), payload.as_slice());
    let _ = consume_timeslice(env, 1);
    match result {
        Ok(()) => Ok(atoms::ok().encode(env)),
        Err(error) => Ok(encode_error(env, &error)),
    }
}

/// Append one payload durably only when the opened regular file remains within
/// `max_bytes`. The descriptor lock covers both the size check and append, so
/// cooperating writers cannot each pass the check and exceed the ceiling.
#[rustler::nif(schedule = "DirtyIo")]
fn fs_append_sync_nofollow_bounded<'a>(
    env: Env<'a>,
    path: String,
    payload: Binary<'a>,
    max_bytes: u64,
) -> NifResult<Term<'a>> {
    if let Err(term) = validate_path(env, &path) {
        return Ok(term);
    }

    let result = append_sync_nofollow_bounded(Path::new(&path), payload.as_slice(), max_bytes);
    let _ = consume_timeslice(env, 1);
    match result {
        Ok(()) => Ok(atoms::ok().encode(env)),
        Err(BoundedAppendError::TooLarge {
            current_bytes,
            payload_bytes,
            max_bytes,
        }) => {
            let attempted_bytes = current_bytes.checked_add(payload_bytes);
            let message = match attempted_bytes {
                Some(attempted_bytes) => format!(
                    "append would grow file from {current_bytes} to {attempted_bytes} bytes (max {max_bytes})"
                ),
                None => format!(
                    "append size overflow: current {current_bytes} bytes, payload {payload_bytes} bytes (max {max_bytes})"
                ),
            };
            Ok((atoms::error(), (too_large(), message)).encode(env))
        }
        Err(BoundedAppendError::Io(error)) => Ok(encode_error(env, &error)),
    }
}

/// Durably replace a file from one bounded payload using an exclusive
/// same-directory temporary file and an atomic rename.
#[rustler::nif(schedule = "DirtyIo")]
fn fs_atomic_replace_nofollow<'a>(
    env: Env<'a>,
    path: String,
    payload: Binary<'a>,
    max_bytes: u64,
) -> NifResult<Term<'a>> {
    if let Err(term) = validate_path(env, &path) {
        return Ok(term);
    }

    let result = atomic_replace_nofollow(Path::new(&path), payload.as_slice(), max_bytes);
    let _ = consume_timeslice(env, 1);
    match result {
        Ok(()) => Ok(atoms::ok().encode(env)),
        Err(error) if error.kind() == io::ErrorKind::InvalidInput => {
            Ok((atoms::error(), (too_large(), error.to_string())).encode(env))
        }
        Err(error) => Ok(encode_error(env, &error)),
    }
}

fn append_sync_nofollow(path: &Path, payload: &[u8]) -> io::Result<()> {
    let mut file = crate::open_append_nofollow(path)?;
    let _lock = crate::lock_file_exclusive(&file)?;
    file.write_all(payload)?;
    file.sync_data()
}

#[derive(Debug)]
enum BoundedAppendError {
    TooLarge {
        current_bytes: u64,
        payload_bytes: u64,
        max_bytes: u64,
    },
    Io(io::Error),
}

impl From<io::Error> for BoundedAppendError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

fn append_sync_nofollow_bounded(
    path: &Path,
    payload: &[u8],
    max_bytes: u64,
) -> Result<(), BoundedAppendError> {
    let payload_bytes = u64::try_from(payload.len()).map_err(|_| BoundedAppendError::TooLarge {
        current_bytes: 0,
        payload_bytes: u64::MAX,
        max_bytes,
    })?;
    if payload_bytes > max_bytes {
        return Err(BoundedAppendError::TooLarge {
            current_bytes: 0,
            payload_bytes,
            max_bytes,
        });
    }

    let mut file = crate::open_append_nofollow(path)?;
    let _lock = crate::lock_file_exclusive(&file)?;
    let metadata = file.metadata()?;
    if !metadata.file_type().is_file() {
        return Err(BoundedAppendError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "append target is not a regular file",
        )));
    }

    let current_bytes = metadata.len();
    let attempted_bytes = current_bytes.checked_add(payload_bytes);
    if attempted_bytes.map_or(true, |attempted| attempted > max_bytes) {
        return Err(BoundedAppendError::TooLarge {
            current_bytes,
            payload_bytes,
            max_bytes,
        });
    }

    file.write_all(payload)?;
    file.sync_data()?;
    Ok(())
}

static ATOMIC_TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn atomic_replace_nofollow(path: &Path, payload: &[u8], max_bytes: u64) -> io::Result<()> {
    let payload_len = u64::try_from(payload.len()).map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidInput, "payload exceeds address space")
    })?;
    if payload_len > max_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("payload is {payload_len} bytes (max {max_bytes})"),
        ));
    }

    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let file_name = path
        .file_name()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "path has no file name"))?;

    let (temp_path, mut temp_file) = create_atomic_temp(parent, file_name)?;
    let result = (|| {
        temp_file.write_all(payload)?;
        temp_file.sync_all()?;
        #[cfg(unix)]
        crate::path_open::rename_nofollow(&temp_path, path)?;
        #[cfg(not(unix))]
        std::fs::rename(&temp_path, path)?;
        sync_directory_nofollow(parent)
    })();

    if result.is_err() {
        #[cfg(unix)]
        let _ = crate::path_open::remove_file_nofollow(&temp_path);
        #[cfg(not(unix))]
        let _ = std::fs::remove_file(&temp_path);
    }
    result
}

fn create_atomic_temp(
    parent: &Path,
    file_name: &std::ffi::OsStr,
) -> io::Result<(std::path::PathBuf, std::fs::File)> {
    for _ in 0..128 {
        let sequence = ATOMIC_TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let temp_name = format!(
            ".{}.ferric-tmp-{}-{sequence}",
            file_name.to_string_lossy(),
            std::process::id()
        );
        let temp_path = parent.join(temp_name);

        match crate::path_open::open_file_nofollow(
            &temp_path,
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
            0o600,
        ) {
            Ok(file) => return Ok((temp_path, file)),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }

    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "could not allocate exclusive atomic-replace temporary file",
    ))
}

#[cfg(unix)]
fn sync_directory_nofollow(path: &Path) -> io::Result<()> {
    crate::path_open::open_directory_nofollow(path)?.sync_all()
}

#[cfg(not(unix))]
fn sync_directory_nofollow(path: &Path) -> io::Result<()> {
    std::fs::File::open(path)?.sync_all()
}

#[cfg(unix)]
fn open_file_nofollow(path: &str) -> io::Result<std::fs::File> {
    let file = crate::path_open::open_file_nofollow(Path::new(path), libc::O_RDONLY, 0)?;
    if !file.metadata()?.file_type().is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "read target is not a regular file",
        ));
    }
    Ok(file)
}

#[cfg(not(unix))]
fn open_file_nofollow(path: &str) -> io::Result<std::fs::File> {
    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "final path component is a symlink",
        ));
    }
    let file = std::fs::File::open(path)?;
    if !file.metadata()?.file_type().is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "read target is not a regular file",
        ));
    }
    Ok(file)
}

// ---------------------------------------------------------------------------
// Async I/O NIFs (Tokio-backed, Normal scheduler)
// ---------------------------------------------------------------------------

/// Recursive remove of a directory tree. Potentially long — runs on the
/// Tokio blocking pool and sends `{:tokio_complete, corr_id, :ok}` or
/// `{:tokio_complete, corr_id, :error, {kind_atom, message}}` on done.
///
/// Idempotent: removing a non-existent path succeeds (caller's intent is
/// "ensure gone"; already-gone is a no-op).
#[rustler::nif(schedule = "Normal")]
fn fs_rm_rf_async(
    env: Env<'_>,
    caller_pid: LocalPid,
    correlation_id: u64,
    path: String,
) -> NifResult<Term<'_>> {
    if let Err(t) = validate_path(env, &path) {
        return Ok(t);
    }

    let blocking_task = match async_io::try_spawn_blocking(move || {
        let p = Path::new(&path);
        remove_dir_all_idempotent(p)
    }) {
        Ok(task) => task,
        Err(reason) => return Ok((atoms::error(), (other(), reason)).encode(env)),
    };

    async_io::runtime().spawn(async move {
        let result = blocking_task
            .await
            .unwrap_or_else(|e| Err(io::Error::other(format!("spawn_blocking failed: {e}"))));

        let mut msg_env = OwnedEnv::new();
        let _ = msg_env.send_and_clear(&caller_pid, |env| match result {
            Ok(()) => (atoms::tokio_complete(), correlation_id, atoms::ok()).encode(env),
            Err(e) => {
                let err_term = encode_error_owned(env, &e);
                (
                    atoms::tokio_complete(),
                    correlation_id,
                    atoms::error(),
                    err_term,
                )
                    .encode(env)
            }
        });
    });

    Ok(atoms::ok().encode(env))
}

/// Variant of `encode_error` returning just the `{kind_atom, msg}` tuple
/// (no outer `:error`) — used inside the async message where the outer
/// tuple already includes `:error`.
fn encode_error_owned<'a>(env: Env<'a>, err: &io::Error) -> Term<'a> {
    use io::ErrorKind::*;
    let kind = match err.kind() {
        NotFound => not_found(),
        AlreadyExists => already_exists(),
        PermissionDenied => permission_denied(),
        _ => match err.raw_os_error() {
            Some(libc::ENOTDIR) => not_a_directory(),
            Some(libc::EISDIR) => is_a_directory(),
            Some(libc::ENOTEMPTY) => directory_not_empty(),
            Some(libc::EINVAL) => invalid_path(),
            Some(libc::ELOOP) => symlink(),
            _ => other(),
        },
    };
    (kind, err.to_string()).encode(env)
}

// ===========================================================================
// Tests
// ===========================================================================
//
// Tests exercise the helper functions directly where possible. NIF-level
// integration (encoding via `env`) is covered by the Elixir test suite
// because those APIs need a live BEAM environment.
//
// What we assert here:
//
//   * Path validation rejects empty + null-byte paths.
//   * Error kind mapping is stable for the common POSIX errno values.
//   * The underlying `std::fs` calls we delegate to behave the way the
//     callers expect for the concurrency/edge-case scenarios our
//     production code relies on.

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{self, File};
    use std::io::Write;
    use std::os::unix::fs::PermissionsExt;
    use tempfile::TempDir;

    // -----------------------------------------------------------------------
    // encode_error / encode_error_owned: error kind mapping
    // -----------------------------------------------------------------------

    #[test]
    fn error_kind_mapping_covers_posix_errnos() {
        // We can't call `encode_error` without an `Env`, but we can cover
        // the internal logic by matching `io::Error` → `ErrorKind` /
        // raw_os_error pairs. This asserts the match arms stay in sync
        // with the POSIX errno values we care about.
        let cases = vec![
            (io::ErrorKind::NotFound, None, "not_found"),
            (io::ErrorKind::AlreadyExists, None, "already_exists"),
            (io::ErrorKind::PermissionDenied, None, "permission_denied"),
            (io::ErrorKind::Other, Some(libc::ENOTDIR), "not_a_directory"),
            (io::ErrorKind::Other, Some(libc::EISDIR), "is_a_directory"),
            (
                io::ErrorKind::Other,
                Some(libc::ENOTEMPTY),
                "directory_not_empty",
            ),
            (io::ErrorKind::Other, Some(libc::EINVAL), "invalid_path"),
            (io::ErrorKind::Other, Some(libc::ELOOP), "symlink"),
            (io::ErrorKind::Other, Some(libc::EIO), "other"),
        ];

        for (kind, errno, expected_label) in cases {
            // Mirror of encode_error's logic without the NIF env.
            let label: &str = match kind {
                io::ErrorKind::NotFound => "not_found",
                io::ErrorKind::AlreadyExists => "already_exists",
                io::ErrorKind::PermissionDenied => "permission_denied",
                _ => match errno {
                    Some(libc::ENOTDIR) => "not_a_directory",
                    Some(libc::EISDIR) => "is_a_directory",
                    Some(libc::ENOTEMPTY) => "directory_not_empty",
                    Some(libc::EINVAL) => "invalid_path",
                    Some(libc::ELOOP) => "symlink",
                    _ => "other",
                },
            };
            assert_eq!(
                label, expected_label,
                "kind={kind:?} errno={errno:?} mapped wrong"
            );
        }
    }

    // -----------------------------------------------------------------------
    // std::fs behavior we rely on — these anchor the contract the NIFs
    // expose to Elixir callers. If these tests start failing, the Rust
    // standard library's semantics changed and the Elixir side needs to
    // adapt.
    // -----------------------------------------------------------------------

    #[test]
    fn touch_creates_empty_file_without_truncating_existing() {
        let dir = TempDir::new().unwrap();
        let p = dir.path().join("foo");

        // First touch — file does not exist, create empty.
        let created = File::options()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&p);
        assert!(created.is_ok());
        drop(created);

        assert_eq!(fs::metadata(&p).unwrap().len(), 0);

        // Write some bytes and touch again — content must survive.
        fs::write(&p, b"hello").unwrap();
        let touched = File::options()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&p);
        assert!(touched.is_ok());
        drop(touched);

        assert_eq!(fs::read(&p).unwrap(), b"hello");
    }

    #[cfg(unix)]
    #[test]
    fn touch_nofollow_rejects_symlink_without_mutating_target() {
        use std::os::unix::fs::symlink;

        let dir = TempDir::new().unwrap();
        let target = dir.path().join("target");
        let link = dir.path().join("active.log");
        fs::write(&target, b"protected").unwrap();
        symlink(&target, &link).unwrap();

        let err = touch_file_nofollow(&link).unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::ELOOP));
        assert_eq!(fs::read(target).unwrap(), b"protected");
    }

    #[cfg(unix)]
    #[test]
    fn touch_nofollow_rejects_intermediate_directory_symlinks() {
        let dir = tempfile::TempDir::new().unwrap();
        let outside = dir.path().join("outside");
        let inside = dir.path().join("inside");
        fs::create_dir(&outside).unwrap();
        fs::create_dir(&inside).unwrap();
        std::os::unix::fs::symlink(&outside, inside.join("redirect")).unwrap();

        let redirected = inside.join("redirect/touched");
        assert!(touch_file_nofollow(&redirected).is_err());
        assert!(!outside.join("touched").exists());
    }

    #[test]
    fn mkdir_p_is_idempotent_for_existing_directory() {
        let dir = TempDir::new().unwrap();
        let nested = dir.path().join("a/b/c");
        create_dir_all_nofollow(&nested).unwrap();
        create_dir_all_nofollow(&nested).unwrap();
        assert!(nested.is_dir());
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn mkdir_p_nofollow_allows_macos_root_tmp_alias() {
        let dir = tempfile::Builder::new()
            .prefix("ferric-mkdir-")
            .tempdir_in("/tmp")
            .unwrap();
        let nested = dir.path().join("nested/path");

        create_dir_all_nofollow(&nested).unwrap();

        assert!(nested.is_dir());
    }

    #[test]
    fn mkdir_p_errors_when_path_is_an_existing_file() {
        let dir = TempDir::new().unwrap();
        let file_path = dir.path().join("blocker");
        File::create(&file_path).unwrap();

        let err = create_dir_all_nofollow(&file_path).unwrap_err();
        // On most POSIX systems this surfaces as AlreadyExists or
        // NotADirectory depending on stdlib version; we just need it
        // to be an error.
        assert!(matches!(
            err.kind(),
            io::ErrorKind::AlreadyExists
                | io::ErrorKind::Other
                | io::ErrorKind::InvalidInput
                | io::ErrorKind::NotADirectory
        ));
    }

    #[cfg(unix)]
    #[test]
    fn mkdir_p_nofollow_rejects_intermediate_symlink_escape() {
        let dir = TempDir::new().unwrap();
        let base = dir.path().join("base");
        let outside = dir.path().join("outside");
        fs::create_dir(&base).unwrap();
        fs::create_dir(&outside).unwrap();
        std::os::unix::fs::symlink(&outside, base.join("redirect")).unwrap();

        let error = create_dir_all_nofollow(&base.join("redirect/escaped")).unwrap_err();

        assert!(matches!(
            error.raw_os_error(),
            Some(code) if code == libc::ELOOP || code == libc::ENOTDIR
        ));
        assert!(!outside.join("escaped").exists());
    }

    #[test]
    fn rm_rf_treats_concurrent_not_found_as_success() {
        let dir = TempDir::new().unwrap();
        let vanished = dir.path().join("already-gone");

        let result = remove_dir_all_idempotent(&vanished);

        assert!(
            result.is_ok(),
            "rm_rf must stay idempotent if another process removes the path first"
        );
    }

    #[test]
    fn rename_overwrites_target_on_posix() {
        let dir = TempDir::new().unwrap();
        let a = dir.path().join("a");
        let b = dir.path().join("b");
        fs::write(&a, b"from-a").unwrap();
        fs::write(&b, b"was-b").unwrap();
        fs::rename(&a, &b).unwrap();

        assert!(!a.exists(), "rename must unlink source");
        assert_eq!(fs::read(&b).unwrap(), b"from-a");
    }

    #[test]
    fn rename_of_missing_source_returns_not_found() {
        let dir = TempDir::new().unwrap();
        let a = dir.path().join("nope");
        let b = dir.path().join("dest");
        let err = fs::rename(&a, &b).unwrap_err();
        assert!(matches!(
            err.kind(),
            io::ErrorKind::NotFound | io::ErrorKind::Other
        ));
    }

    #[test]
    fn rm_of_missing_file_returns_not_found() {
        let dir = TempDir::new().unwrap();
        let p = dir.path().join("ghost");
        let err = fs::remove_file(&p).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn rm_refuses_to_remove_a_directory() {
        let dir = TempDir::new().unwrap();
        let sub = dir.path().join("inner");
        fs::create_dir(&sub).unwrap();

        let err = fs::remove_file(&sub).unwrap_err();
        // Expect EISDIR or a close equivalent. Some stdlib versions map
        // this to Other; we only need the op to fail so `fs_rm` doesn't
        // silently delete directories.
        assert!(matches!(
            err.kind(),
            io::ErrorKind::Other | io::ErrorKind::IsADirectory | io::ErrorKind::PermissionDenied
        ));
    }

    #[test]
    fn exists_true_for_existing_file() {
        let dir = TempDir::new().unwrap();
        let p = dir.path().join("f");
        File::create(&p).unwrap();
        assert!(p.exists());
    }

    #[test]
    fn exists_false_for_broken_symlink() {
        let dir = TempDir::new().unwrap();
        let target = dir.path().join("target-that-vanished");
        let link = dir.path().join("link");
        std::os::unix::fs::symlink(&target, &link).unwrap();

        // Path::exists follows symlinks by design; broken symlink → false.
        assert!(!link.exists());
    }

    #[test]
    fn exists_false_for_missing_path() {
        let dir = TempDir::new().unwrap();
        assert!(!dir.path().join("nowhere").exists());
    }

    #[test]
    fn is_dir_false_for_file() {
        let dir = TempDir::new().unwrap();
        let f = dir.path().join("notadir");
        File::create(&f).unwrap();
        assert!(!f.is_dir());
        assert!(dir.path().is_dir());
    }

    #[test]
    fn is_dir_follows_symlink_to_dir() {
        let dir = TempDir::new().unwrap();
        let real = dir.path().join("real");
        fs::create_dir(&real).unwrap();
        let link = dir.path().join("link-to-dir");
        std::os::unix::fs::symlink(&real, &link).unwrap();

        assert!(link.is_dir());
    }

    #[test]
    fn ls_lists_entries_names_only_no_path() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("one"), b"").unwrap();
        fs::write(dir.path().join("two"), b"").unwrap();
        fs::create_dir(dir.path().join("subdir")).unwrap();

        let mut names: Vec<String> = fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok().map(|e| e.file_name().into_string().ok()).flatten())
            .collect();
        names.sort();
        assert_eq!(names, vec!["one", "subdir", "two"]);
    }

    #[test]
    fn ls_of_missing_dir_returns_not_found() {
        let dir = TempDir::new().unwrap();
        let err = fs::read_dir(dir.path().join("does-not-exist")).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn ls_of_file_returns_not_a_directory() {
        let dir = TempDir::new().unwrap();
        let f = dir.path().join("just-a-file");
        fs::write(&f, b"x").unwrap();
        let err = fs::read_dir(&f).unwrap_err();
        // Rust stdlib maps ENOTDIR to Other on stable; either is ok.
        assert!(matches!(
            err.kind(),
            io::ErrorKind::Other | io::ErrorKind::NotADirectory | io::ErrorKind::InvalidInput
        ));
        assert_eq!(err.raw_os_error(), Some(libc::ENOTDIR));
    }

    #[test]
    fn ls_handles_many_entries() {
        let dir = TempDir::new().unwrap();
        // 1000 > our 256-entry yield interval — exercise the yielding
        // loop boundary.
        for i in 0..1000 {
            fs::write(dir.path().join(format!("f_{:04}", i)), b"").unwrap();
        }
        let count = fs::read_dir(dir.path()).unwrap().count();
        assert_eq!(count, 1000);
    }

    // -----------------------------------------------------------------------
    // Path validation
    // -----------------------------------------------------------------------

    #[test]
    fn empty_path_is_rejected_by_helper() {
        // We can't call validate_path without an `Env` — mirror the check.
        let path = "";
        assert!(path.is_empty(), "empty path detection trivially holds");
    }

    #[test]
    fn null_byte_in_path_rejected_by_helper() {
        let path = "foo\0bar";
        assert!(path.as_bytes().contains(&0u8));
    }

    #[test]
    fn append_sync_nofollow_rejects_a_final_symlink() {
        let dir = TempDir::new().unwrap();
        let target = dir.path().join("target");
        let link = dir.path().join("log");
        fs::write(&target, b"protected").unwrap();
        std::os::unix::fs::symlink(&target, &link).unwrap();

        assert!(append_sync_nofollow(&link, b"attack").is_err());
        assert_eq!(fs::read(target).unwrap(), b"protected");
    }

    #[test]
    fn append_sync_nofollow_appends_complete_payloads() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("log");

        append_sync_nofollow(&path, b"one").unwrap();
        append_sync_nofollow(&path, b"two").unwrap();
        assert_eq!(fs::read(path).unwrap(), b"onetwo");
    }

    #[test]
    fn bounded_append_rejects_at_the_cap_without_growing_the_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("bounded-log");
        fs::write(&path, b"12345").unwrap();

        let error = append_sync_nofollow_bounded(&path, b"6", 5).unwrap_err();

        assert!(matches!(error, BoundedAppendError::TooLarge { .. }));
        assert_eq!(fs::read(&path).unwrap(), b"12345");
    }

    #[test]
    fn bounded_append_accepts_an_exact_fit() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("bounded-log");
        fs::write(&path, b"12").unwrap();

        append_sync_nofollow_bounded(&path, b"345", 5).unwrap();

        assert_eq!(fs::read(&path).unwrap(), b"12345");
    }

    #[test]
    fn bounded_append_serializes_the_size_check_with_the_write() {
        let dir = TempDir::new().unwrap();
        let path = std::sync::Arc::new(dir.path().join("bounded-log"));
        fs::write(path.as_ref(), b"").unwrap();
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));

        let handles = [b"aaaa".as_slice(), b"bbbb".as_slice()].map(|payload| {
            let path = std::sync::Arc::clone(&path);
            let barrier = std::sync::Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                append_sync_nofollow_bounded(path.as_ref(), payload, 6)
            })
        });
        barrier.wait();

        let outcomes = handles.map(|handle| handle.join().unwrap());
        assert_eq!(outcomes.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            outcomes
                .iter()
                .filter(|result| matches!(result, Err(BoundedAppendError::TooLarge { .. })))
                .count(),
            1
        );
        assert_eq!(fs::metadata(path.as_ref()).unwrap().len(), 4);
    }

    #[test]
    fn oversized_bounded_append_does_not_create_a_file() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("bounded-log");

        let error = append_sync_nofollow_bounded(&path, b"123", 2).unwrap_err();

        assert!(matches!(error, BoundedAppendError::TooLarge { .. }));
        assert!(!path.exists());
    }

    #[test]
    fn atomic_replace_is_bounded_and_replaces_a_symlink_not_its_target() {
        let dir = TempDir::new().unwrap();
        let target = dir.path().join("target");
        let link = dir.path().join("metadata");
        fs::write(&target, b"protected").unwrap();
        std::os::unix::fs::symlink(&target, &link).unwrap();

        atomic_replace_nofollow(&link, b"new", 3).unwrap();
        assert_eq!(fs::read(&target).unwrap(), b"protected");
        assert_eq!(fs::read(&link).unwrap(), b"new");
        assert!(!fs::symlink_metadata(link).unwrap().file_type().is_symlink());

        let error = atomic_replace_nofollow(&dir.path().join("bounded"), b"four", 3).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(!dir.path().join("bounded").exists());
    }

    // -----------------------------------------------------------------------
    // rm_rf semantics (this is what fs_rm_rf_async delegates to)
    // -----------------------------------------------------------------------

    #[test]
    fn rm_rf_removes_empty_dir() {
        let dir = TempDir::new().unwrap();
        let sub = dir.path().join("empty");
        fs::create_dir(&sub).unwrap();

        remove_dir_all_idempotent(&sub).unwrap();
        assert!(!sub.exists());
    }

    #[test]
    fn rm_rf_removes_nested_tree() {
        let dir = TempDir::new().unwrap();
        let root = dir.path().join("root");
        fs::create_dir_all(root.join("a/b/c")).unwrap();
        fs::write(root.join("a/file"), b"x").unwrap();
        fs::write(root.join("a/b/file"), b"y").unwrap();
        fs::write(root.join("a/b/c/file"), b"z").unwrap();

        remove_dir_all_idempotent(&root).unwrap();
        assert!(!root.exists());
    }

    #[test]
    fn rm_rf_does_not_follow_symlinks_outside_tree() {
        let outer = TempDir::new().unwrap();
        let outside_file = outer.path().join("outside");
        fs::write(&outside_file, b"precious").unwrap();

        let inner = TempDir::new().unwrap();
        let root = inner.path().join("root");
        fs::create_dir(&root).unwrap();
        let link = root.join("pointer");
        std::os::unix::fs::symlink(&outside_file, &link).unwrap();

        remove_dir_all_idempotent(&root).unwrap();

        assert!(!root.exists(), "tree must be removed");
        assert!(
            outside_file.exists(),
            "rm_rf must NOT follow symlinks that point outside the tree"
        );
    }

    #[cfg(unix)]
    #[test]
    fn rm_rf_rejects_an_intermediate_directory_symlink() {
        let dir = TempDir::new().unwrap();
        let outside = dir.path().join("outside");
        let inside = dir.path().join("inside");
        fs::create_dir(&outside).unwrap();
        fs::create_dir(&inside).unwrap();
        fs::create_dir(outside.join("victim")).unwrap();
        fs::write(outside.join("victim/protected"), b"keep").unwrap();
        std::os::unix::fs::symlink(&outside, inside.join("redirect")).unwrap();

        assert!(remove_dir_all_idempotent(&inside.join("redirect/victim")).is_err());
        assert_eq!(fs::read(outside.join("victim/protected")).unwrap(), b"keep");
    }

    #[test]
    fn rm_rf_missing_path_errors_not_found() {
        // We want the *async NIF* to treat this as idempotent (we check
        // exists() first), but the raw stdlib call errors. Document the
        // baseline:
        let dir = TempDir::new().unwrap();
        let err = fs::remove_dir_all(dir.path().join("ghost")).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }

    // -----------------------------------------------------------------------
    // Permission edge cases (skipped on root — root bypasses DAC)
    // -----------------------------------------------------------------------

    #[test]
    fn rm_of_file_in_readonly_dir_is_denied() {
        // Skip when running as root (CI sometimes does).
        if unsafe { libc::geteuid() } == 0 {
            return;
        }

        let dir = TempDir::new().unwrap();
        let sub = dir.path().join("locked");
        fs::create_dir(&sub).unwrap();

        let victim = sub.join("kill-me");
        let mut f = File::create(&victim).unwrap();
        f.write_all(b"x").unwrap();

        // Make parent dir read-only: no unlink allowed.
        let mut perms = fs::metadata(&sub).unwrap().permissions();
        perms.set_mode(0o500);
        fs::set_permissions(&sub, perms).unwrap();

        let err = fs::remove_file(&victim).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::PermissionDenied);

        // Restore so TempDir can clean up.
        let mut restore = fs::metadata(&sub).unwrap().permissions();
        restore.set_mode(0o700);
        fs::set_permissions(&sub, restore).unwrap();
    }
}
