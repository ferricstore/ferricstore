//! Descriptor-relative Unix path opens that reject symlink traversal.

#![cfg(unix)]

use std::ffi::{CStr, CString, OsStr};
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::path::{Component, Path};

fn component_name(name: &OsStr) -> io::Result<CString> {
    CString::new(name.as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains null byte"))
}

fn open_directory_at(
    parent_fd: libc::c_int,
    name: &OsStr,
    allow_symlink: bool,
) -> io::Result<OwnedFd> {
    let name = component_name(name)?;
    let nofollow = if allow_symlink { 0 } else { libc::O_NOFOLLOW };
    let fd = unsafe {
        libc::openat(
            parent_fd,
            name.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NONBLOCK | nofollow,
        )
    };

    if fd < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(unsafe { OwnedFd::from_raw_fd(fd) })
    }
}

fn starting_directory(path: &Path) -> io::Result<OwnedFd> {
    let start = if path.is_absolute() { c"/" } else { c"." };
    let fd = unsafe {
        libc::open(
            start.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        )
    };

    if fd < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(unsafe { OwnedFd::from_raw_fd(fd) })
    }
}

#[cfg(target_os = "linux")]
fn openat2_nofollow(
    path: &Path,
    flags: libc::c_int,
    mode: libc::mode_t,
) -> io::Result<Option<std::fs::File>> {
    let path = component_name(path.as_os_str())?;
    // `open_how` is non-exhaustive on Linux libc targets, so initialize the
    // kernel ABI struct to zero before setting the fields this syscall uses.
    let mut how = unsafe { std::mem::zeroed::<libc::open_how>() };
    how.flags = flags as u64;
    how.mode = u64::from(mode);
    how.resolve = libc::RESOLVE_NO_SYMLINKS;
    let fd = unsafe {
        libc::syscall(
            libc::SYS_openat2,
            libc::AT_FDCWD,
            path.as_ptr(),
            std::ptr::addr_of!(how),
            std::mem::size_of::<libc::open_how>(),
        )
    };

    if fd >= 0 {
        return Ok(Some(unsafe {
            std::fs::File::from_raw_fd(fd as libc::c_int)
        }));
    }

    let error = io::Error::last_os_error();
    match error.raw_os_error() {
        // Old kernels and restrictive container profiles fall back to the
        // portable descriptor-walk below.
        Some(code) if matches!(code, libc::ENOSYS | libc::EINVAL | libc::EPERM) => Ok(None),
        _other => Err(error),
    }
}

pub(crate) fn open_directory_nofollow(path: &Path) -> io::Result<std::fs::File> {
    #[cfg(target_os = "linux")]
    if let Some(directory) = openat2_nofollow(
        path,
        libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
        0,
    )? {
        return Ok(directory);
    }

    let mut directory = starting_directory(path)?;
    let mut allow_root_alias = path.is_absolute();

    for component in path.components() {
        let name = match component {
            Component::RootDir | Component::CurDir => continue,
            Component::ParentDir => OsStr::new(".."),
            Component::Normal(name) => name,
            Component::Prefix(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "unsupported path prefix",
                ));
            }
        };

        // macOS commonly exposes root-owned aliases such as `/tmp` and `/var`.
        // Only that first absolute component may be followed. Every component
        // below it is resolved relative to an already-open directory fd with
        // O_NOFOLLOW, so a writable-tree symlink cannot redirect the open.
        directory = open_directory_at(directory.as_raw_fd(), name, allow_root_alias)?;
        allow_root_alias = false;
    }

    Ok(std::fs::File::from(directory))
}

fn open_parent_and_name(path: &Path) -> io::Result<(std::fs::File, CString)> {
    let file_name = path
        .file_name()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "path has no file name"))?;
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));

    Ok((open_directory_nofollow(parent)?, component_name(file_name)?))
}

pub(crate) fn open_file_nofollow(
    path: &Path,
    flags: libc::c_int,
    mode: libc::mode_t,
) -> io::Result<std::fs::File> {
    let flags = flags | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK;

    #[cfg(target_os = "linux")]
    if let Some(file) = openat2_nofollow(path, flags, mode)? {
        return Ok(file);
    }

    let (directory, file_name) = open_parent_and_name(path)?;

    let fd = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            file_name.as_ptr(),
            flags,
            libc::c_uint::from(mode),
        )
    };

    if fd < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(unsafe { std::fs::File::from_raw_fd(fd) })
    }
}

pub(crate) fn rename_nofollow(source: &Path, destination: &Path) -> io::Result<()> {
    let (source_directory, source_name) = open_parent_and_name(source)?;
    let (destination_directory, destination_name) = open_parent_and_name(destination)?;
    let result = unsafe {
        libc::renameat(
            source_directory.as_raw_fd(),
            source_name.as_ptr(),
            destination_directory.as_raw_fd(),
            destination_name.as_ptr(),
        )
    };

    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

pub(crate) fn remove_file_nofollow(path: &Path) -> io::Result<()> {
    let (directory, file_name) = open_parent_and_name(path)?;
    let result = unsafe { libc::unlinkat(directory.as_raw_fd(), file_name.as_ptr(), 0) };

    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

struct DirectoryStream(*mut libc::DIR);

impl Drop for DirectoryStream {
    fn drop(&mut self) {
        let _ = unsafe { libc::closedir(self.0) };
    }
}

fn remove_directory_contents(directory: &std::fs::File) -> io::Result<()> {
    let duplicate_fd = unsafe { libc::fcntl(directory.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 0) };
    if duplicate_fd < 0 {
        return Err(io::Error::last_os_error());
    }

    let stream = unsafe { libc::fdopendir(duplicate_fd) };
    if stream.is_null() {
        let error = io::Error::last_os_error();
        let _ = unsafe { libc::close(duplicate_fd) };
        return Err(error);
    }
    let stream = DirectoryStream(stream);

    loop {
        let entry = unsafe { libc::readdir(stream.0) };
        if entry.is_null() {
            break;
        }

        let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) };
        if name.to_bytes() == b"." || name.to_bytes() == b".." {
            continue;
        }

        let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
        let stat_result = unsafe {
            libc::fstatat(
                directory.as_raw_fd(),
                name.as_ptr(),
                stat.as_mut_ptr(),
                libc::AT_SYMLINK_NOFOLLOW,
            )
        };
        if stat_result != 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::NotFound {
                continue;
            }
            return Err(error);
        }
        let stat = unsafe { stat.assume_init() };

        if stat.st_mode & libc::S_IFMT == libc::S_IFDIR {
            let child_fd = unsafe {
                libc::openat(
                    directory.as_raw_fd(),
                    name.as_ptr(),
                    libc::O_RDONLY
                        | libc::O_DIRECTORY
                        | libc::O_CLOEXEC
                        | libc::O_NOFOLLOW
                        | libc::O_NONBLOCK,
                )
            };
            if child_fd < 0 {
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::NotFound {
                    continue;
                }
                return Err(error);
            }

            let child = unsafe { std::fs::File::from_raw_fd(child_fd) };
            remove_directory_contents(&child)?;

            if unsafe { libc::unlinkat(directory.as_raw_fd(), name.as_ptr(), libc::AT_REMOVEDIR) }
                != 0
            {
                let error = io::Error::last_os_error();
                if error.kind() != io::ErrorKind::NotFound {
                    return Err(error);
                }
            }
        } else if unsafe { libc::unlinkat(directory.as_raw_fd(), name.as_ptr(), 0) } != 0 {
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::NotFound {
                return Err(error);
            }
        }
    }

    Ok(())
}

pub(crate) fn remove_dir_all_nofollow(path: &Path) -> io::Result<()> {
    let (parent, name) = open_parent_and_name(path)?;
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    if unsafe {
        libc::fstatat(
            parent.as_raw_fd(),
            name.as_ptr(),
            stat.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    } != 0
    {
        return Err(io::Error::last_os_error());
    }
    let stat = unsafe { stat.assume_init() };

    if stat.st_mode & libc::S_IFMT != libc::S_IFDIR {
        return remove_file_nofollow(path);
    }

    let directory_fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY
                | libc::O_DIRECTORY
                | libc::O_CLOEXEC
                | libc::O_NOFOLLOW
                | libc::O_NONBLOCK,
        )
    };
    if directory_fd < 0 {
        return Err(io::Error::last_os_error());
    }

    let directory = unsafe { std::fs::File::from_raw_fd(directory_fd) };
    remove_directory_contents(&directory)?;

    if unsafe { libc::unlinkat(parent.as_raw_fd(), name.as_ptr(), libc::AT_REMOVEDIR) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}
