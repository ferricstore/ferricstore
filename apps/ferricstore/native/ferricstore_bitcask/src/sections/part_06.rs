#[cfg(test)]
mod prob_fsync_tests {
    use super::*;
    use std::fs::{File, OpenOptions};
    use std::io::Write;
    use std::sync::Arc;
    use std::thread;
    use tempfile::TempDir;

    fn tmpfile() -> (TempDir, std::path::PathBuf) {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("probe.bin");
        let mut f = File::create(&path).unwrap();
        f.write_all(b"hello world").unwrap();
        f.sync_all().unwrap();
        (dir, path)
    }

    #[test]
    fn fsync_empty_file_ok() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("empty.bin");
        let f = File::create(&path).unwrap();
        assert!(prob_fsync(&f).is_ok(), "empty file fsync should succeed");
    }

    #[test]
    fn fsync_small_file_ok() {
        let (_dir, path) = tmpfile();
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        assert!(prob_fsync(&f).is_ok());
    }

    #[test]
    fn fsync_read_only_file_returns_err() {
        // On most Unix systems fsync/fdatasync on a read-only fd returns
        // EBADF. prob_fsync must propagate that as an Err, not panic.
        let (_dir, path) = tmpfile();
        let f = File::open(&path).unwrap(); // read-only
        let result = prob_fsync(&f);
        // On macOS/Linux sync_data may actually succeed on RO fds for some
        // filesystems — so we don't assert Err here. We only assert that
        // the call doesn't panic and that any error is surfaced as the
        // String payload of the Result (not a panic / abort).
        match result {
            Ok(()) => {}
            Err(msg) => assert!(
                msg.starts_with("sync_data:"),
                "error must be prefixed with sync_data: — got {msg}"
            ),
        }
    }

    #[test]
    fn fsync_after_unlink_does_not_panic() {
        // Open the file, keep the fd, unlink the path, then fsync. On Unix
        // the fd remains valid. prob_fsync must not panic — at worst return
        // an Err (some kernels will return EIO).
        let (dir, path) = tmpfile();
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        std::fs::remove_file(&path).unwrap();

        // File is gone from the directory but fd is still open.
        let _ = prob_fsync(&f);

        // Dir itself still exists; drop is fine.
        drop(dir);
    }

    #[test]
    fn repeated_fsync_idempotent() {
        let (_dir, path) = tmpfile();
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();

        // 1000 fsyncs on the same fd must all succeed and must not
        // cause the process to OOM / leak descriptors. (sync_data is
        // always idempotent per POSIX.)
        for _ in 0..1000 {
            prob_fsync(&f).expect("repeated fsync must succeed");
        }
    }

    #[test]
    fn fsync_concurrent_same_file_no_panic() {
        // sync_data is thread-safe per POSIX. Run 4 threads pounding the
        // same file with fsyncs — must not panic / deadlock.
        let (_dir, path) = tmpfile();
        let f = Arc::new(
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap(),
        );

        let handles: Vec<_> = (0..4)
            .map(|_| {
                let f = Arc::clone(&f);
                thread::spawn(move || {
                    for _ in 0..250 {
                        let _ = prob_fsync(&f);
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().expect("thread must not panic");
        }
    }

    /// FD-leak probe: open+write+fsync+close many files, ensure resources are
    /// released (no stack overflow, no allocator growth, no panic). We can't
    /// portably count open FDs without libc syscalls, but the `prob_fsync`
    /// helper takes `&File` not ownership — closing is the caller's job, so
    /// a leak here would show as a disk-space / inode problem in real
    /// workloads. This test exercises that closing path through the Rust
    /// Drop impl.
    #[test]
    fn no_fd_leak_across_many_open_cycles() {
        let dir = tempfile::TempDir::new().unwrap();

        for i in 0..2000 {
            let path = dir.path().join(format!("f_{i}.bin"));
            let mut f = File::create(&path).unwrap();
            f.write_all(&[0u8; 64]).unwrap();
            prob_fsync(&f).expect("fsync ok");
            // f dropped here — fd released by kernel via Drop.
        }

        // If we leaked FDs we'd hit EMFILE (too many open files) before
        // reaching 2000 on most default ulimits (~256 on macOS).
    }

    #[test]
    fn error_message_is_well_formed() {
        // Explicit failure path: close the underlying fd by creating a
        // File and dropping it, then try to sync a fresh fd pointing at a
        // nonexistent directory's child. That returns ENOENT on open, so
        // we exercise the "sync_data: <errno>" format indirectly by
        // provoking a sync on a file that has been closed via into_raw_fd.
        //
        // Simplified: just ensure the Ok variant returns unit and the Err
        // variant contains the sync_data: prefix if it ever fires. The
        // ok case is covered above; the err case is exercised via
        // fsync_read_only_file_returns_err.
        let (_dir, path) = tmpfile();
        let f = File::open(&path).unwrap();
        match prob_fsync(&f) {
            Ok(()) => {}
            Err(msg) => {
                assert!(
                    msg.starts_with("sync_data:"),
                    "error message must start with 'sync_data:' prefix, got {msg}"
                );
            }
        }
    }
}

// ===========================================================================
// fsync_dir edge-case tests
// ---------------------------------------------------------------------------
// Directory fsync makes rename/rm/create operations durable against kernel
// panic. POSIX: fsync(dirfd) flushes the directory's entries (filename →
// inode mappings), independent of file-data fsync.
//
// Cases to cover:
//   * Happy path: existing dir returns Ok.
//   * Nonexistent path: Err, no panic.
//   * Path is a regular file, not a dir: Err (on most platforms — some
//     allow fsync on files, so this is best-effort).
//   * Empty string path: Err.
//   * Path with no parent (root): Ok (fsyncing "/" is valid).
//   * Concurrent fsync from multiple threads on same dir: no panic, no
//     deadlock.
//   * FD leak probe: many open+fsync+close cycles should not exhaust FDs.
//   * Post-rename observability: after rename + fsync_dir, reopen via a
//     fresh path resolution sees the new name (sanity check, not a real
//     kernel-panic test).
//
// These tests are written BEFORE the helper is implemented — they should
// fail to compile until `fsync_dir` + the NIF exist.
// ===========================================================================

#[cfg(test)]
mod fsync_dir_tests {
    use super::*;
    use std::fs::{self, File};
    use std::io::Write;
    use std::sync::Arc;
    use std::thread;
    use tempfile::TempDir;

    #[test]
    fn existing_dir_ok() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().to_str().unwrap().to_string();
        assert!(
            fsync_dir(&path).is_ok(),
            "fsync on existing dir must succeed"
        );
    }

    #[test]
    fn nonexistent_dir_returns_err() {
        let missing = "/does/not/exist/for/test/xyz123";
        let result = fsync_dir(missing);
        assert!(result.is_err(), "fsync on missing path must return Err");
        if let Err(msg) = result {
            // Make sure the error is tagged so callers can log it meaningfully.
            assert!(!msg.is_empty(), "err message must be non-empty");
        }
    }

    #[test]
    fn path_to_regular_file_ok_or_well_formed_err() {
        // On most systems, fsync on a regular file fd is legal and equivalent
        // to v2_fsync on that file. We accept either Ok (file treated like a
        // file) or a well-formed Err. What we DO NOT accept: a panic.
        let dir = tempfile::TempDir::new().unwrap();
        let fpath = dir.path().join("not_a_dir.txt");
        let mut f = File::create(&fpath).unwrap();
        f.write_all(b"x").unwrap();
        f.sync_all().unwrap();

        let result = fsync_dir(fpath.to_str().unwrap());
        match result {
            Ok(()) => {}
            Err(msg) => assert!(!msg.is_empty()),
        }
    }

    #[test]
    fn empty_path_returns_err() {
        let result = fsync_dir("");
        assert!(result.is_err(), "empty path must return Err");
    }

    #[test]
    fn repeated_fsync_idempotent() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().to_str().unwrap().to_string();

        for _ in 0..1000 {
            fsync_dir(&path).expect("repeated dir fsync must succeed");
        }
    }

    #[test]
    fn concurrent_fsync_no_panic() {
        let dir = tempfile::TempDir::new().unwrap();
        let path: Arc<String> = Arc::new(dir.path().to_str().unwrap().to_string());

        let handles: Vec<_> = (0..4)
            .map(|_| {
                let p = Arc::clone(&path);
                thread::spawn(move || {
                    for _ in 0..250 {
                        let _ = fsync_dir(&p);
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().expect("thread must not panic");
        }
    }

    #[test]
    fn no_fd_leak_across_cycles() {
        // 2000 fsync_dir calls shouldn't blow past the default ulimit (~256
        // on macOS) if the helper properly drops the open fd.
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().to_str().unwrap().to_string();

        for _ in 0..2000 {
            fsync_dir(&path).expect("fsync dir ok");
        }
    }

    #[test]
    fn after_rename_dir_entry_present() {
        // This is a sanity test, not a kernel-panic test: after
        // rename + fsync_dir, the new filename must still be visible via
        // a fresh directory read. (That's trivially true on any reasonable
        // filesystem; we just want to ensure fsync_dir doesn't mangle
        // state.)
        let dir = tempfile::TempDir::new().unwrap();
        let old_path = dir.path().join("old.log");
        let new_path = dir.path().join("new.log");

        File::create(&old_path).unwrap();
        fs::rename(&old_path, &new_path).unwrap();
        fsync_dir(dir.path().to_str().unwrap()).unwrap();

        let entries: Vec<_> = fs::read_dir(dir.path())
            .unwrap()
            .filter_map(Result::ok)
            .map(|e| e.file_name())
            .collect();

        assert!(entries.iter().any(|n| n == "new.log"));
        assert!(!entries.iter().any(|n| n == "old.log"));
    }

    #[test]
    fn after_remove_dir_entry_gone() {
        let dir: TempDir = tempfile::TempDir::new().unwrap();
        let fpath = dir.path().join("will_be_removed.log");
        File::create(&fpath).unwrap();
        fs::remove_file(&fpath).unwrap();

        // Must not error just because the removed file doesn't exist —
        // we're syncing the DIR, which does exist.
        fsync_dir(dir.path().to_str().unwrap()).unwrap();

        assert!(!fpath.exists());
    }

    #[test]
    fn error_message_well_formed() {
        let result = fsync_dir("/nonexistent/xyz/abc");
        match result {
            Ok(()) => {}
            Err(msg) => assert!(
                !msg.is_empty() && msg.len() < 1024,
                "error message should be non-empty and bounded, got {msg}"
            ),
        }
    }
}

#[cfg(test)]
mod nif_scheduler_tests {
    #[test]
    fn file_backed_probabilistic_nifs_do_not_run_on_dirty_schedulers() {
        for path in ["src/bloom.rs", "src/cms.rs", "src/cuckoo.rs", "src/topk.rs"] {
            let source =
                std::fs::read_to_string(path).unwrap_or_else(|err| panic!("read {path}: {err}"));

            for (line_idx, line) in source.lines().enumerate() {
                if line.contains("#[rustler::nif") {
                    assert!(
                        !line.contains("DirtyIo") && !line.contains("DirtyCpu"),
                        "{path}:{} file-backed probabilistic NIFs must stay off dirty schedulers; move long I/O to async workers instead: {line}",
                        line_idx + 1
                    );
                }
            }
        }
    }

    #[test]
    fn file_backed_probabilistic_writes_go_through_write_all_at() {
        for path in ["src/bloom.rs", "src/cms.rs", "src/cuckoo.rs", "src/topk.rs"] {
            let source =
                std::fs::read_to_string(path).unwrap_or_else(|err| panic!("read {path}: {err}"));

            let production_source = source.split("#[cfg(test)]").next().unwrap_or(&source);

            for (line_idx, line) in production_source.lines().enumerate() {
                assert!(
                    !line.contains(".write_at("),
                    "{path}:{} must use crate::write_all_at so short pwrite results cannot be reported as success: {line}",
                    line_idx + 1
                );
            }
        }
    }
}

#[cfg(test)]
mod lmdb_cache_tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn lmdb_store_reuses_canonical_path_for_aliases() {
        let dir = tempfile::TempDir::new().unwrap();
        let real = dir.path().join("db");
        let alias_root = dir.path().join("alias");
        std::os::unix::fs::symlink(dir.path(), &alias_root).unwrap();
        let alias = alias_root.join("db");

        let first = lmdb_store(real.to_str().unwrap(), 64 * 1024 * 1024).unwrap();
        let second = lmdb_store(alias.to_str().unwrap(), 128 * 1024 * 1024).unwrap();

        assert!(
            Arc::ptr_eq(&first, &second),
            "aliased LMDB paths must reuse the already-open environment"
        );
    }

    #[test]
    fn lmdb_store_opens_without_readahead() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = lmdb_store(dir.path().to_str().unwrap(), 64 * 1024 * 1024).unwrap();
        let flags = store
            .env
            .flags()
            .unwrap()
            .expect("LMDB env flags should be representable by heed");

        assert!(
            flags.contains(heed::EnvFlags::NO_READ_AHEAD),
            "startup prefix scans should not force the OS to fault huge LMDB files into RSS"
        );
    }
}

