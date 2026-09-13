use rustler::{Encoder, Env, NifResult, Term};

#[derive(Debug, PartialEq, Eq)]
struct DiskCapacity {
    total: u64,
    used: u64,
    available: u64,
}

fn capacity_from_blocks(
    blocks: u128,
    free: u128,
    available: u128,
    size: u128,
) -> Result<DiskCapacity, String> {
    let bytes = |count: u128| {
        count
            .checked_mul(size)
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or_else(|| "disk capacity overflow".to_owned())
    };
    // Reserved blocks are free but not available to unprivileged users.
    // Keep used = total - free, matching df rather than counting reserves as used.
    Ok(DiskCapacity {
        total: bytes(blocks)?,
        used: bytes(blocks.saturating_sub(free))?,
        available: bytes(available)?,
    })
}

#[cfg(unix)]
fn read_disk_capacity(path: &str) -> Result<DiskCapacity, String> {
    let stat = statvfs_for_path(std::path::Path::new(path))?;
    capacity_from_blocks(
        u128::from(stat.f_blocks),
        u128::from(stat.f_bfree),
        u128::from(stat.f_bavail),
        u128::from(stat.f_frsize),
    )
}

#[cfg(unix)]
pub(crate) fn statvfs_for_path(path: &std::path::Path) -> Result<libc::statvfs, String> {
    use std::os::unix::ffi::OsStrExt;

    let path =
        std::ffi::CString::new(path.as_os_str().as_bytes()).map_err(|error| error.to_string())?;
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // statvfs initializes the complete structure on success; no pointers escape.
    if unsafe { libc::statvfs(path.as_ptr(), stat.as_mut_ptr()) } != 0 {
        return Err(format!(
            "statvfs failed: {}",
            std::io::Error::last_os_error()
        ));
    }
    Ok(unsafe { stat.assume_init() })
}

#[cfg(not(unix))]
fn read_disk_capacity(_path: &str) -> Result<DiskCapacity, String> {
    Err("disk capacity is unsupported on this platform".to_owned())
}

#[rustler::nif(schedule = "DirtyIo")]
fn disk_capacity(env: Env<'_>, path: String) -> NifResult<Term<'_>> {
    match read_disk_capacity(&path) {
        Ok(capacity) => Ok((
            crate::atoms::ok(),
            capacity.total,
            capacity.used,
            capacity.available,
        )
            .encode(env)),
        Err(error) => Ok((crate::atoms::error(), error).encode(env)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserved_blocks_are_not_used_blocks() {
        assert_eq!(
            capacity_from_blocks(100, 30, 20, 4096).unwrap(),
            DiskCapacity {
                total: 409_600,
                used: 286_720,
                available: 81_920,
            }
        );
    }

    #[test]
    fn zero_and_unusual_filesystem_counters_do_not_underflow() {
        assert_eq!(
            capacity_from_blocks(0, 0, 0, 4096).unwrap(),
            DiskCapacity {
                total: 0,
                used: 0,
                available: 0
            }
        );
        assert_eq!(capacity_from_blocks(1, 2, 1, 512).unwrap().used, 0);
    }

    #[test]
    fn overflow_is_an_error_not_a_healthy_measurement() {
        assert!(capacity_from_blocks(u128::from(u64::MAX), 0, 0, 4096).is_err());
        assert!(capacity_from_blocks(1, 0, u128::from(u64::MAX), 4096).is_err());
    }

    #[test]
    fn reads_real_capacity_and_rejects_invalid_paths() {
        let dir = tempfile::tempdir().unwrap();
        let capacity = read_disk_capacity(dir.path().to_str().unwrap()).unwrap();
        assert!(capacity.total > 0);
        assert!(capacity.available > 0);
        assert!(read_disk_capacity(dir.path().join("missing").to_str().unwrap()).is_err());
        assert!(read_disk_capacity("\0").is_err());
    }
}
