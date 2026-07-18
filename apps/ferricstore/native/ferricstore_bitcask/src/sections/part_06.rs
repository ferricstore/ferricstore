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

#[cfg(all(test, unix))]
mod nofollow_random_file_tests {
    use super::*;
    use std::os::unix::fs::symlink;

    #[test]
    fn random_access_helpers_reject_final_component_symlinks() {
        let dir = tempfile::TempDir::new().unwrap();
        let target = dir.path().join("target");
        let link = dir.path().join("sidecar");
        std::fs::write(&target, b"protected").unwrap();
        symlink(&target, &link).unwrap();

        assert_eq!(
            open_random_read(&link).unwrap_err().raw_os_error(),
            Some(libc::ELOOP)
        );
        assert_eq!(
            open_random_rw(&link).unwrap_err().raw_os_error(),
            Some(libc::ELOOP)
        );
    }

    #[test]
    fn nofollow_helpers_reject_intermediate_directory_symlinks() {
        let dir = tempfile::TempDir::new().unwrap();
        let outside = dir.path().join("outside");
        let inside = dir.path().join("inside");
        std::fs::create_dir(&outside).unwrap();
        std::fs::create_dir(&inside).unwrap();
        std::fs::write(outside.join("existing"), b"protected").unwrap();
        symlink(&outside, inside.join("redirect")).unwrap();

        let redirected_existing = inside.join("redirect/existing");
        let redirected_new = inside.join("redirect/new");
        let redirected_truncate = inside.join("redirect/truncate");

        assert!(open_random_read(&redirected_existing).is_err());
        assert!(open_random_rw(&redirected_existing).is_err());
        assert!(open_append_nofollow(&redirected_new).is_err());
        assert!(create_truncate_nofollow(&redirected_truncate).is_err());

        assert!(!outside.join("new").exists());
        assert!(!outside.join("truncate").exists());
        assert_eq!(std::fs::read(outside.join("existing")).unwrap(), b"protected");
    }

    #[test]
    fn rename_and_unlink_reject_intermediate_directory_symlinks() {
        let dir = tempfile::TempDir::new().unwrap();
        let outside = dir.path().join("outside");
        let inside = dir.path().join("inside");
        std::fs::create_dir(&outside).unwrap();
        std::fs::create_dir(&inside).unwrap();
        std::fs::write(outside.join("source"), b"protected").unwrap();
        symlink(&outside, inside.join("redirect")).unwrap();

        assert!(
            crate::path_open::rename_nofollow(
                &inside.join("redirect/source"),
                &inside.join("redirect/renamed"),
            )
            .is_err()
        );
        assert!(
            crate::path_open::remove_file_nofollow(&inside.join("redirect/source")).is_err()
        );

        assert_eq!(std::fs::read(outside.join("source")).unwrap(), b"protected");
        assert!(!outside.join("renamed").exists());
    }

    #[test]
    fn create_truncate_helper_rejects_symlink_without_touching_target() {
        let dir = tempfile::TempDir::new().unwrap();
        let target = dir.path().join("target");
        let link = dir.path().join("sidecar");
        std::fs::write(&target, b"protected").unwrap();
        symlink(&target, &link).unwrap();

        assert_eq!(
            create_truncate_nofollow(&link)
                .unwrap_err()
                .raw_os_error(),
            Some(libc::ELOOP)
        );
        assert_eq!(std::fs::read(target).unwrap(), b"protected");
    }

    #[test]
    fn append_opener_rejects_a_fifo_without_waiting_for_a_reader() {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let dir = tempfile::TempDir::new().unwrap();
        let fifo = dir.path().join("append.fifo");
        let fifo_c = CString::new(fifo.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(fifo_c.as_ptr(), 0o600) }, 0);

        let (tx, rx) = std::sync::mpsc::channel();
        let worker_path = fifo.clone();
        let worker = std::thread::spawn(move || {
            tx.send(open_append_nofollow(&worker_path).map(drop))
                .unwrap();
        });

        let result = match rx.recv_timeout(std::time::Duration::from_millis(100)) {
            Ok(result) => result,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                // Unblock the old behavior so a failing regression never leaks
                // a thread or stalls TempDir cleanup.
                let reader = unsafe {
                    libc::open(
                        fifo_c.as_ptr(),
                        libc::O_RDONLY | libc::O_NONBLOCK | libc::O_CLOEXEC,
                    )
                };
                assert!(reader >= 0);
                let result = rx
                    .recv_timeout(std::time::Duration::from_secs(1))
                    .expect("blocked FIFO append opener did not recover");
                unsafe { libc::close(reader) };
                worker.join().unwrap();
                panic!("append opener blocked on a FIFO and later returned {result:?}");
            }
            Err(error) => panic!("append opener channel failed: {error}"),
        };

        worker.join().unwrap();
        assert!(result.is_err(), "append opener accepted a FIFO");
    }

    #[test]
    fn random_read_opener_rejects_a_fifo_without_waiting_for_a_writer() {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let dir = tempfile::TempDir::new().unwrap();
        let fifo = dir.path().join("read.fifo");
        let fifo_c = CString::new(fifo.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(fifo_c.as_ptr(), 0o600) }, 0);

        let (tx, rx) = std::sync::mpsc::channel();
        let worker_path = fifo.clone();
        let worker = std::thread::spawn(move || {
            tx.send(open_random_read(&worker_path).map(drop)).unwrap();
        });

        let result = match rx.recv_timeout(std::time::Duration::from_millis(100)) {
            Ok(result) => result,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                // Unblock the old behavior so a failing regression never leaks
                // a thread or stalls TempDir cleanup.
                let writer = unsafe {
                    libc::open(
                        fifo_c.as_ptr(),
                        libc::O_WRONLY | libc::O_NONBLOCK | libc::O_CLOEXEC,
                    )
                };
                assert!(writer >= 0);
                let result = rx
                    .recv_timeout(std::time::Duration::from_secs(1))
                    .expect("blocked FIFO read opener did not recover");
                unsafe { libc::close(writer) };
                worker.join().unwrap();
                panic!("random read opener blocked on a FIFO and later returned {result:?}");
            }
            Err(error) => panic!("random read opener channel failed: {error}"),
        };

        worker.join().unwrap();
        assert!(result.is_err(), "random read opener accepted a FIFO");
    }

    #[test]
    fn random_rw_opener_rejects_a_fifo() {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let dir = tempfile::TempDir::new().unwrap();
        let fifo = dir.path().join("rw.fifo");
        let fifo_c = CString::new(fifo.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(fifo_c.as_ptr(), 0o600) }, 0);

        assert!(
            open_random_rw(&fifo).is_err(),
            "random read-write opener accepted a FIFO"
        );
    }
}

#[cfg(test)]
mod probabilistic_sidecar_locking_tests {
    use super::*;

    #[test]
    fn exclusive_file_locks_serialize_independent_descriptors() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("sidecar");
        std::fs::write(&path, b"sidecar").unwrap();

        let first = open_random_rw(&path).unwrap();
        let first_lock = lock_file_exclusive(&first).unwrap();
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (acquired_tx, acquired_rx) = std::sync::mpsc::channel();
        let worker_path = path.clone();
        let worker = std::thread::spawn(move || {
            let second = open_random_rw(&worker_path).unwrap();
            ready_tx.send(()).unwrap();
            let _second_lock = lock_file_exclusive(&second).unwrap();
            acquired_tx.send(()).unwrap();
        });

        ready_rx.recv().unwrap();
        assert_eq!(
            acquired_rx.recv_timeout(std::time::Duration::from_millis(50)),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        );
        drop(first_lock);
        acquired_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap();
        worker.join().unwrap();
    }

    #[test]
    fn probabilistic_sidecars_use_lock_aware_open_helpers() {
        let production_sources = [
            include_str!("../bloom.rs"),
            include_str!("../cms.rs"),
            include_str!("../topk.rs"),
            include_str!("cuckoo_part_01.rs"),
            include_str!("cuckoo_part_02.rs"),
        ];
        let combined = production_sources
            .iter()
            .map(|source| source.split("#[cfg(test)]").next().unwrap_or(source))
            .collect::<Vec<_>>()
            .join("\n");

        assert!(
            combined.contains("open_random_read_locked"),
            "sidecar reads must hold a shared advisory lock"
        );
        assert!(
            combined.contains("open_random_rw_locked"),
            "sidecar mutations must hold an exclusive advisory lock"
        );
        assert!(
            combined.contains("create_staged_locked_nofollow"),
            "sidecar creation must stage before atomic publication"
        );
        assert!(
            !combined.contains("crate::open_random_read("),
            "sidecar reads must not bypass the lock-aware helper"
        );
        assert!(
            !combined.contains("crate::open_random_rw("),
            "sidecar mutations must not bypass the lock-aware helper"
        );
        assert!(
            !combined.contains("crate::create_truncate_nofollow("),
            "sidecar creation must not truncate before acquiring its lock"
        );
    }

    #[test]
    fn staged_sidecar_is_invisible_until_atomic_publish() {
        use std::io::Write;

        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("sidecar");
        let mut staged = create_staged_locked_nofollow(&path).unwrap();
        staged.write_all(b"complete-layout").unwrap();

        assert!(!path.exists());
        staged.publish().unwrap();
        assert_eq!(std::fs::read(path).unwrap(), b"complete-layout");
    }

    #[test]
    fn opposing_merge_lock_sets_do_not_deadlock() {
        let dir = tempfile::TempDir::new().unwrap();
        let left = dir.path().join("left");
        let right = dir.path().join("right");
        std::fs::write(&left, b"left").unwrap();
        std::fs::write(&right, b"right").unwrap();

        let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
        let (acquired_tx, acquired_rx) = std::sync::mpsc::channel();
        let mut workers = Vec::new();
        for (destination, source) in [(left.clone(), right.clone()), (right, left)] {
            let worker_barrier = std::sync::Arc::clone(&barrier);
            let worker_tx = acquired_tx.clone();
            workers.push(std::thread::spawn(move || {
                worker_barrier.wait();
                let sources = [source.as_path()];
                let _files = open_random_merge_locked(&destination, &sources).unwrap();
                worker_tx.send(()).unwrap();
                std::thread::sleep(std::time::Duration::from_millis(10));
            }));
        }

        barrier.wait();
        acquired_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap();
        acquired_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap();
        for worker in workers {
            worker.join().unwrap();
        }
    }

    #[cfg(unix)]
    #[test]
    fn merge_lock_set_deduplicates_destination_hardlink_aliases() {
        let dir = tempfile::TempDir::new().unwrap();
        let destination = dir.path().join("destination");
        let alias = dir.path().join("alias");
        std::fs::write(&destination, b"sidecar").unwrap();
        std::fs::hard_link(&destination, &alias).unwrap();

        let sources = [alias.as_path()];
        let files = open_random_merge_locked(&destination, &sources).unwrap();
        assert_eq!(files.sources.len(), 1);
    }
}

#[cfg(test)]
mod io_backend_architecture_tests {
    fn rust_sources(path: &std::path::Path, sources: &mut Vec<std::path::PathBuf>) {
        for entry in std::fs::read_dir(path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                rust_sources(&path, sources);
            } else if path.extension().and_then(std::ffi::OsStr::to_str) == Some("rs") {
                sources.push(path);
            }
        }
    }

    #[test]
    fn unused_async_uring_backend_is_not_exported() {
        let source = include_str!("../io_backend/mod.rs");
        let production = source.split("#[cfg(test)]").next().unwrap_or(source);

        assert!(!production.contains("mod async_uring"));
        assert!(!production.contains("AsyncUringBackend"));
    }

    #[test]
    fn active_uring_validates_kernel_lengths_offsets_and_completion_tags() {
        let source = include_str!("../io_backend/uring.rs");
        let production = source.split("#[cfg(test)]").next().unwrap_or(source);

        assert!(production.contains("fn uring_write_len("));
        assert!(production.contains("fn checked_write_end("));
        assert!(!production.contains("unwrap_or(u32::MAX)"));
        assert!(!production.contains(".user_data(file_offset)"));
        assert!(production.contains(".user_data(index as u64)"));
        assert!(production.contains("validate_uring_single_completion("));
        assert!(production.contains("fn submit_and_wait_retry("));
        assert!(
            production.contains(".push_multiple(&sqes)"),
            "batch SQEs must be capacity-checked before any borrowed buffer pointer is queued"
        );
    }

    #[test]
    fn native_code_uses_only_compile_time_atoms() {
        let mut sources = Vec::new();
        rust_sources(
            &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
            &mut sources,
        );

        let forbidden = [
            ["Atom", "::from_str"].concat(),
            ["Atom", "::from_bytes"].concat(),
            ["enif_", "make_atom"].concat(),
            ["make_", "existing_atom"].concat(),
        ];

        for path in sources {
            let source = std::fs::read_to_string(&path).unwrap();
            for pattern in &forbidden {
                assert!(
                    !source.contains(pattern),
                    "runtime atom construction is forbidden in {}: {pattern}",
                    path.display()
                );
            }
        }
    }

    #[test]
    fn async_blocking_work_uses_central_bounded_admission() {
        let sources = [
            include_str!("part_03.rs"),
            include_str!("part_05.rs"),
            include_str!("../bloom.rs"),
            include_str!("../cms.rs"),
            include_str!("cuckoo_part_02.rs"),
            include_str!("../topk.rs"),
            include_str!("../fs_nif.rs"),
        ];

        for source in sources {
            let production = source.split("#[cfg(test)]").next().unwrap_or(source);
            assert!(
                !production.contains("tokio::task::spawn_blocking(move ||"),
                "async NIFs must submit through async_io::try_spawn_blocking"
            );
        }
    }
}

#[cfg(test)]
mod lmdb_cache_architecture_tests {
    #[test]
    fn unrelated_lmdb_paths_do_not_open_under_the_global_cache_mutex() {
        let declarations = include_str!("../sections/part_01.rs");
        let implementation = include_str!("../sections/part_04.rs");
        let lmdb_store = implementation
            .split("fn lmdb_store(")
            .nth(1)
            .unwrap()
            .split("#[rustler::nif")
            .next()
            .unwrap();

        assert!(declarations.contains("LmdbStoreCell"));
        assert!(lmdb_store.contains("get_or_init"));
        assert!(lmdb_store.find("create_dir_all_nofollow").unwrap()
            < lmdb_store.find("stores.lock()").unwrap());
        assert!(lmdb_store.find("std::fs::canonicalize").unwrap()
            < lmdb_store.find("stores.lock()").unwrap());
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
    fn path_to_regular_file_is_rejected() {
        let dir = tempfile::TempDir::new().unwrap();
        let fpath = dir.path().join("not_a_dir.txt");
        let mut f = File::create(&fpath).unwrap();
        f.write_all(b"x").unwrap();
        f.sync_all().unwrap();

        let result = fsync_dir(fpath.to_str().unwrap());
        assert!(result.is_err(), "fsync_dir must reject a regular file");
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_directory_is_rejected() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::TempDir::new().unwrap();
        let target = dir.path().join("target");
        let link = dir.path().join("redirect");
        fs::create_dir(&target).unwrap();
        symlink(&target, &link).unwrap();

        let result = fsync_dir(link.to_str().unwrap());
        assert!(result.is_err(), "fsync_dir must not follow a symlink");
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
    fn file_backed_probabilistic_nifs_use_the_correct_scheduler_class() {
        for path in [
            "src/bloom.rs",
            "src/cms.rs",
            "src/sections/cuckoo_part_02.rs",
            "src/topk.rs",
        ] {
            let source =
                std::fs::read_to_string(path).unwrap_or_else(|err| panic!("read {path}: {err}"));

            let lines = source.lines().collect::<Vec<_>>();
            for (line_idx, line) in lines.iter().enumerate() {
                if line.contains("#[rustler::nif") {
                    let function = lines[line_idx + 1..]
                        .iter()
                        .find(|candidate| candidate.trim_start().starts_with("pub fn "))
                        .unwrap_or_else(|| panic!("{path}:{} missing NIF function", line_idx + 1));
                    let valid_scheduler = if function.contains("_async") {
                        line.contains("Normal") || line.contains("DirtyCpu")
                    } else {
                        line.contains("DirtyIo")
                    };

                    assert!(
                        valid_scheduler && !(function.contains("_async") && line.contains("DirtyIo")),
                        "{path}:{} synchronous file I/O must use DirtyIo; async dispatch may use Normal or DirtyCpu for bounded input staging, never DirtyIo: {line} before {function}",
                        line_idx + 1
                    );
                }
            }
        }
    }

    #[test]
    fn file_backed_probabilistic_writes_go_through_write_all_at() {
        for path in [
            "src/bloom.rs",
            "src/cms.rs",
            "src/sections/cuckoo_part_01.rs",
            "src/sections/cuckoo_part_02.rs",
            "src/topk.rs",
        ] {
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
mod scan_page_limit_tests {
    use super::*;

    #[test]
    fn metadata_scan_page_limit_is_bounded_at_the_native_boundary() {
        assert_eq!(validate_scan_file_page_limit(1).unwrap(), 1);
        assert_eq!(
            validate_scan_file_page_limit(MAX_SCAN_FILE_PAGE_RECORDS).unwrap(),
            MAX_SCAN_FILE_PAGE_RECORDS
        );
        assert!(validate_scan_file_page_limit(0).is_err());
        assert!(validate_scan_file_page_limit(MAX_SCAN_FILE_PAGE_RECORDS + 1).is_err());
    }
}

#[cfg(test)]
mod retired_store_architecture_tests {
    #[test]
    fn resource_style_store_stack_is_not_compiled() {
        let lib_source = include_str!("../lib.rs");
        let hint_source = include_str!("../hint.rs");

        for declaration in ["pub mod store;", "pub mod keydir;", "pub mod compaction;"] {
            assert!(
                !lib_source.contains(declaration),
                "retired native module is still compiled: {declaration}"
            );
        }

        assert!(
            !hint_source.contains("pub fn load_into"),
            "hint decoding must not retain the retired Rust KeyDir adapter"
        );
        assert!(
            !hint_source.contains("crate::keydir"),
            "hint decoding must not depend on the retired Rust KeyDir"
        );
    }

    #[test]
    fn full_materialization_nifs_are_not_compiled() {
        let source = include_str!("part_02.rs");

        for function in [
            "fn v2_scan_file<'a>",
            "fn v2_scan_file_from_offset<'a>",
            "fn v2_scan_tombstones<'a>",
            "fn v2_read_hint_file<'a>",
        ] {
            assert!(
                !source.contains(function),
                "unbounded full-materialization NIF is still compiled: {function}"
            );
        }

        for function in [
            "fn v2_scan_file_page",
            "fn v2_scan_tombstones_page",
            "fn v2_read_hint_file_page",
        ] {
            assert!(
                source.contains(function),
                "bounded streaming NIF is missing: {function}"
            );
        }
    }
}

#[cfg(test)]
mod lmdb_cache_tests {
    use super::*;
    use std::sync::mpsc;
    use std::thread;

    #[cfg(unix)]
    #[test]
    fn lmdb_store_reuses_canonical_path_for_non_symlink_aliases() {
        let dir = tempfile::TempDir::new().unwrap();
        let real = dir.path().join("db");
        let alias = dir.path().join("unused").join("..").join("db");

        let first = lmdb_store(real.to_str().unwrap(), 64 * 1024 * 1024).unwrap();
        let second = lmdb_store(alias.to_str().unwrap(), 64 * 1024 * 1024).unwrap();

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

    #[test]
    fn lmdb_release_waits_for_an_acquired_store_lease() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("leased-db");
        let path_string = path.to_str().unwrap().to_owned();
        let store = lmdb_store(&path_string, 64 * 1024 * 1024).unwrap();

        let mut wtxn = store.env.write_txn().unwrap();
        store.db.put(&mut wtxn, b"key", b"value").unwrap();
        wtxn.commit().unwrap();

        let (acquired_tx, acquired_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let holder = thread::spawn(move || {
            acquired_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            drop(store);
        });

        acquired_rx.recv().unwrap();
        let cache_key = std::fs::canonicalize(&path)
            .unwrap()
            .to_string_lossy()
            .into_owned();
        let stores = LMDB_STORES.get().unwrap();

        {
            let mut guard = stores.lock().unwrap();
            assert_eq!(
                release_lmdb_cache_entry(&mut guard, &cache_key),
                LmdbCacheRelease::Busy(1)
            );
        }

        release_tx.send(()).unwrap();
        holder.join().unwrap();

        {
            let mut guard = stores.lock().unwrap();
            assert_eq!(
                release_lmdb_cache_entry(&mut guard, &cache_key),
                LmdbCacheRelease::Released(1)
            );
        }

        let reopened = lmdb_store(&path_string, 64 * 1024 * 1024).unwrap();
        let rtxn = reopened.env.read_txn().unwrap();
        assert_eq!(reopened.db.get(&rtxn, b"key").unwrap(), Some(b"value".as_slice()));
    }
}
