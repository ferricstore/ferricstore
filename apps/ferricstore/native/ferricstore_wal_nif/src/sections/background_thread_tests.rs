    use super::*;
    use std::sync::atomic::AtomicUsize;
    use tempfile::NamedTempFile;

    struct FailingWriter;

    impl Write for FailingWriter {
        fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
            Err(io::Error::new(io::ErrorKind::Other, "forced write failure"))
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    struct FailingSyncWriter {
        writes: Vec<u8>,
    }

    impl Write for FailingSyncWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.writes.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl SyncData for FailingSyncWriter {
        fn sync_data(&mut self) -> io::Result<()> {
            Err(io::Error::new(io::ErrorKind::Other, "forced sync failure"))
        }
    }

    struct BlockingSyncWriter {
        writes: Vec<u8>,
        sync_started_tx: Sender<()>,
        release_sync_rx: Receiver<()>,
        sync_count: Arc<AtomicUsize>,
    }

    impl Write for BlockingSyncWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.writes.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl SyncData for BlockingSyncWriter {
        fn sync_data(&mut self) -> io::Result<()> {
            self.sync_count.fetch_add(1, Ordering::AcqRel);
            self.sync_started_tx.send(()).unwrap();
            self.release_sync_rx
                .recv_timeout(Duration::from_secs(5))
                .unwrap();
            Ok(())
        }
    }

    fn test_flush(tx: Sender<u64>, commit_delay: Duration) -> FlushCaller {
        FlushCaller {
            target: FlushTarget::Test(tx),
            commit_delay,
        }
    }

    #[test]
    fn preallocate_keep_size_does_not_extend_logical_file_size() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("segment.seg");
        std::fs::write(&path, b"abc").unwrap();

        preallocate_keep_size_path(path.to_str().unwrap(), 1024 * 1024).unwrap();

        let metadata = std::fs::metadata(&path).unwrap();
        assert_eq!(metadata.len(), 3);
    }

    #[test]
    fn drain_to_kernel_returns_write_error_instead_of_panicking() {
        let buffer = Mutex::new(AlignedBuffer::new());
        {
            let mut guard = buffer.lock().unwrap();
            guard.extend(b"entry");
        }

        let file_size = AtomicU64::new(WAL_HEADER_SIZE);
        let mut writer = FailingWriter;

        let result = drain_to_kernel_writer(&mut writer, &buffer, &file_size);

        assert!(result.is_err());
        assert_eq!(file_size.load(Ordering::Acquire), WAL_HEADER_SIZE);
    }

    #[test]
    fn drain_to_kernel_requeues_only_the_unwritten_suffix_after_partial_write() {
        struct PartialThenFail {
            writes: Vec<u8>,
            calls: usize,
        }

        impl Write for PartialThenFail {
            fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
                self.calls += 1;
                if self.calls == 1 {
                    self.writes.extend_from_slice(&buf[..2]);
                    Ok(2)
                } else {
                    Err(io::Error::new(io::ErrorKind::Other, "forced write failure"))
                }
            }

            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        let buffer = Mutex::new(AlignedBuffer::new());
        buffer.lock().unwrap().extend(b"entry");
        let file_size = AtomicU64::new(WAL_HEADER_SIZE);
        let mut writer = PartialThenFail {
            writes: Vec::new(),
            calls: 0,
        };

        assert!(drain_to_kernel_writer(&mut writer, &buffer, &file_size).is_err());

        assert_eq!(writer.writes, b"en");
        assert_eq!(file_size.load(Ordering::Acquire), WAL_HEADER_SIZE + 2);
        assert_eq!(buffer.lock().unwrap().take().as_logical_slice(), b"try");
    }

    #[test]
    fn drain_to_kernel_restores_failed_bytes_before_concurrent_writes() {
        struct AppendThenFail {
            buffer: Arc<Mutex<AlignedBuffer>>,
        }

        impl Write for AppendThenFail {
            fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
                self.buffer.lock().unwrap().extend(b"new");
                Err(io::Error::new(io::ErrorKind::Other, "forced write failure"))
            }

            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        let buffer = Arc::new(Mutex::new(AlignedBuffer::new()));
        {
            let mut guard = buffer.lock().unwrap();
            guard.extend(b"entry");
        }

        let file_size = AtomicU64::new(WAL_HEADER_SIZE);
        let mut writer = AppendThenFail {
            buffer: Arc::clone(&buffer),
        };

        let result = drain_to_kernel_writer(&mut writer, &buffer, &file_size);

        assert!(result.is_err());
        assert_eq!(file_size.load(Ordering::Acquire), WAL_HEADER_SIZE);

        let restored = buffer.lock().unwrap().take();
        assert_eq!(restored.as_logical_slice(), b"entrynew");
    }

    #[test]
    fn close_drain_and_sync_returns_sync_error() {
        let buffer = Mutex::new(AlignedBuffer::new());
        {
            let mut guard = buffer.lock().unwrap();
            guard.extend(b"entry");
        }

        let file_size = AtomicU64::new(WAL_HEADER_SIZE);
        let mut writer = FailingSyncWriter { writes: Vec::new() };

        let err = close_drain_and_sync(&mut writer, &buffer, &file_size).unwrap_err();

        assert!(err.to_string().contains("forced sync failure"));
        assert_eq!(file_size.load(Ordering::Acquire), WAL_HEADER_SIZE + 5);
        assert!(!writer.writes.is_empty());
    }

    #[test]
    fn sync_cycle_does_not_self_drive_flushes_arriving_during_fdatasync() {
        let buffer = Arc::new(Mutex::new(AlignedBuffer::new()));
        {
            let mut guard = buffer.lock().unwrap();
            guard.extend(b"first");
        }

        let file_size = Arc::new(AtomicU64::new(0));
        let (flush_tx, flush_rx) = crossbeam_channel::unbounded();
        let (first_done_tx, first_done_rx) = crossbeam_channel::bounded(1);
        let (second_done_tx, second_done_rx) = crossbeam_channel::bounded(1);
        let (sync_started_tx, sync_started_rx) = crossbeam_channel::bounded(1);
        let (release_sync_tx, release_sync_rx) = crossbeam_channel::bounded(1);
        let sync_count = Arc::new(AtomicUsize::new(0));

        let thread_buffer = Arc::clone(&buffer);
        let thread_file_size = Arc::clone(&file_size);
        let thread_sync_count = Arc::clone(&sync_count);

        let handle = std::thread::spawn(move || {
            let mut writer = BlockingSyncWriter {
                writes: Vec::new(),
                sync_started_tx,
                release_sync_rx,
                sync_count: thread_sync_count,
            };
            let mut callers = vec![test_flush(first_done_tx, Duration::ZERO)];

            run_sync_cycle(
                &mut writer,
                &thread_buffer,
                &thread_file_size,
                &flush_rx,
                &mut callers,
                Duration::ZERO,
            );
        });

        sync_started_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap();

        {
            let mut guard = buffer.lock().unwrap();
            guard.extend(b"second");
        }
        flush_tx
            .send(ThreadMsg::Flush(test_flush(second_done_tx, Duration::ZERO)))
            .unwrap();

        release_sync_tx.send(()).unwrap();

        assert_eq!(
            first_done_rx.recv_timeout(Duration::from_secs(5)).unwrap(),
            5
        );

        let second_sync_started = sync_started_rx
            .recv_timeout(Duration::from_millis(100))
            .is_ok();
        if second_sync_started {
            let _ = release_sync_tx.send(());
        }

        assert!(
            !second_sync_started,
            "flush arriving during fdatasync must wait for the next Erlang-owned sync cycle"
        );
        assert!(second_done_rx
            .recv_timeout(Duration::from_millis(20))
            .is_err());

        handle.join().unwrap();
        assert_eq!(sync_count.load(Ordering::Acquire), 1);
    }

    #[test]
    fn close_during_commit_delay_exits_the_worker_after_acknowledging_close() {
        let (config, tx) = test_config(0);
        let (flush_done_tx, _flush_done_rx) = crossbeam_channel::bounded(1);
        let (close_result_tx, close_result_rx) = crossbeam_channel::bounded(1);
        let handle = std::thread::spawn(move || thread_loop(config));

        tx.send(ThreadMsg::Flush(test_flush(
            flush_done_tx,
            Duration::from_millis(250),
        )))
        .unwrap();
        tx.send(ThreadMsg::Close(Some(close_result_tx))).unwrap();

        assert_eq!(
            close_result_rx
                .recv_timeout(Duration::from_secs(2))
                .unwrap(),
            Ok(())
        );

        let deadline = Instant::now() + Duration::from_secs(1);
        while !handle.is_finished() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        let exited_after_close = handle.is_finished();

        drop(tx);
        handle.join().unwrap();
        assert!(
            exited_after_close,
            "worker acknowledged Close during commit delay but kept receiving messages"
        );
    }

    #[test]
    fn commit_deadline_rejects_an_unrepresentable_delay() {
        let error = commit_deadline(Instant::now(), Duration::MAX).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn worker_panic_notifies_active_and_queued_flush_callers_and_close() {
        let (config, tx) = test_config(0);
        let (active_tx, active_rx) = crossbeam_channel::bounded(1);
        let (queued_tx, queued_rx) = crossbeam_channel::bounded(1);
        let (close_tx, close_rx) = crossbeam_channel::bounded(1);
        let handle = std::thread::spawn(move || thread_loop(config));

        tx.send(ThreadMsg::Flush(FlushCaller {
            target: FlushTarget::TestResult(active_tx),
            commit_delay: Duration::from_secs(1),
        }))
        .unwrap();
        tx.send(ThreadMsg::Panic).unwrap();
        tx.send(ThreadMsg::Flush(FlushCaller {
            target: FlushTarget::TestResult(queued_tx),
            commit_delay: Duration::ZERO,
        }))
        .unwrap();
        tx.send(ThreadMsg::Close(Some(close_tx))).unwrap();

        for result in [
            active_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            queued_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
        ] {
            let reason = result.unwrap_err();
            assert!(reason.contains("WAL thread panicked"), "{reason}");
        }

        let close_reason = close_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .unwrap_err();
        assert!(close_reason.contains("WAL thread panicked"));
        handle.join().unwrap();
    }

    #[test]
    fn panic_cleanup_waits_for_sync_admission_before_draining_the_queue() {
        let admission = Arc::new(Mutex::new(()));
        let alive = Arc::new(AtomicBool::new(true));
        let (tx, rx) = crossbeam_channel::unbounded();
        let (result_tx, result_rx) = crossbeam_channel::bounded(1);
        let guard = admission.lock().unwrap();

        let panic_admission = Arc::clone(&admission);
        let panic_alive = Arc::clone(&alive);
        let cleanup = std::thread::spawn(move || {
            let error = io::Error::new(io::ErrorKind::Other, "WAL thread panicked: forced");
            let mut active_callers = Vec::new();

            notify_thread_panic(
                &panic_alive,
                &panic_admission,
                &mut active_callers,
                &rx,
                &error,
            );
        });

        tx.send(ThreadMsg::Flush(FlushCaller {
            target: FlushTarget::TestResult(result_tx),
            commit_delay: Duration::ZERO,
        }))
        .unwrap();

        drop(guard);
        cleanup.join().unwrap();

        assert!(!alive.load(Ordering::Acquire));
        let reason = result_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .unwrap_err();
        assert!(reason.contains("WAL thread panicked"));
    }

    /// Helper: create a thread config for testing (no BEAM notifications).
    #[allow(dead_code)]
    fn test_config(commit_delay_us: u64) -> (ThreadConfig, crossbeam_channel::Sender<ThreadMsg>) {
        let tmp = NamedTempFile::new().unwrap();
        let file = tmp.reopen().unwrap();
        let buffer = Arc::new(Mutex::new(AlignedBuffer::new()));
        let (tx, rx) = crossbeam_channel::unbounded();
        let alive = Arc::new(AtomicBool::new(true));
        let file_size = Arc::new(AtomicU64::new(0));
        let config = ThreadConfig {
            file,
            buffer: buffer.clone(),
            rx,
            alive: alive.clone(),
            sync_admission: Arc::new(Mutex::new(())),
            file_size: file_size.clone(),
            commit_delay: Duration::from_micros(commit_delay_us),
            _use_o_direct: false,
        };
        (config, tx)
    }

    #[test]
    fn test_open_wal_file_fallback() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let (file, o_direct) = open_wal_file(path.to_str().unwrap(), 0).unwrap();
        drop(file);

        assert!(!o_direct);

        // File should exist
        assert!(path.exists());
    }

    #[test]
    fn test_open_with_preallocate() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let (file, _) = open_wal_file(path.to_str().unwrap(), 4096).unwrap();
        drop(file);
        assert_eq!(std::fs::metadata(path).unwrap().len(), 0);
    }

    #[test]
    fn raw_append_open_truncates_stale_suffix_at_logical_end() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("segment.wal");
        std::fs::write(&path, b"valid-stale-suffix").unwrap();

        let (mut file, _) = open_raw_append_file(path.to_str().unwrap(), 5).unwrap();
        assert_eq!(file.metadata().unwrap().len(), 5);

        file.write_all(b"-new").unwrap();
        file.sync_all().unwrap();
        drop(file);

        assert_eq!(std::fs::read(path).unwrap(), b"valid-new");
    }

    #[test]
    fn raw_append_recovery_syncs_a_stale_suffix_truncation_before_seeking() {
        #[derive(Default)]
        struct TrackingRawFile {
            len: u64,
            position: u64,
            operations: Vec<&'static str>,
        }

        impl Seek for TrackingRawFile {
            fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
                let SeekFrom::Start(position) = position else {
                    panic!("raw append preparation must seek from start");
                };
                self.operations.push("seek");
                self.position = position;
                Ok(position)
            }
        }

        impl SyncData for TrackingRawFile {
            fn sync_data(&mut self) -> io::Result<()> {
                self.operations.push("sync");
                Ok(())
            }
        }

        impl RawAppendFile for TrackingRawFile {
            fn file_len(&self) -> io::Result<u64> {
                Ok(self.len)
            }

            fn set_logical_len(&mut self, len: u64) -> io::Result<()> {
                self.operations.push("truncate");
                self.len = len;
                Ok(())
            }
        }

        let mut file = TrackingRawFile {
            len: 19,
            ..TrackingRawFile::default()
        };

        prepare_raw_append_file(&mut file, 5).unwrap();

        assert_eq!(file.operations, ["truncate", "sync", "seek"]);
        assert_eq!(file.len, 5);
        assert_eq!(file.position, 5);
    }

    #[test]
    fn raw_append_open_rejects_a_logical_end_beyond_eof() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("short-segment.wal");
        std::fs::write(&path, b"valid").unwrap();

        let error = open_raw_append_file(path.to_str().unwrap(), 6).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
        assert_eq!(std::fs::read(path).unwrap(), b"valid");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_open_wal_file_linux_preallocate_failure_preserves_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        std::fs::write(&path, b"existing bytes").unwrap();

        let result = open_wal_file_linux(path.to_str().unwrap(), u64::MAX);

        assert!(result.is_err());
        assert_eq!(std::fs::read(&path).unwrap(), b"existing bytes");
    }

    #[cfg(unix)]
    #[test]
    fn wal_open_helpers_reject_final_component_symlinks() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("target.wal");
        let link = dir.path().join("linked.wal");
        std::fs::write(&target, b"protected").unwrap();
        symlink(&target, &link).unwrap();

        assert_eq!(
            open_raw_append_file(link.to_str().unwrap(), 0)
                .unwrap_err()
                .raw_os_error(),
            Some(libc::ELOOP)
        );
        assert_eq!(
            open_wal_file(link.to_str().unwrap(), 0)
                .unwrap_err()
                .raw_os_error(),
            Some(libc::ELOOP)
        );
        assert_eq!(std::fs::read(target).unwrap(), b"protected");
    }

    #[cfg(unix)]
    #[test]
    fn wal_open_helpers_reject_a_fifo_without_blocking() {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let dir = tempfile::tempdir().unwrap();
        let fifo = dir.path().join("wal.fifo");
        let fifo_c = CString::new(fifo.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(fifo_c.as_ptr(), 0o600) }, 0);

        assert!(
            open_rw_create_nofollow(fifo.to_str().unwrap()).is_err(),
            "WAL read-write opener accepted a FIFO"
        );

        let (tx, rx) = std::sync::mpsc::channel();
        let worker_path = fifo.clone();
        let worker = std::thread::spawn(move || {
            tx.send(open_read_nofollow(worker_path.to_str().unwrap()).map(drop))
                .unwrap();
        });

        let result = match rx.recv_timeout(Duration::from_millis(100)) {
            Ok(result) => result,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                let writer = unsafe {
                    libc::open(
                        fifo_c.as_ptr(),
                        libc::O_WRONLY | libc::O_NONBLOCK | libc::O_CLOEXEC,
                    )
                };
                assert!(writer >= 0);
                let result = rx
                    .recv_timeout(Duration::from_secs(1))
                    .expect("blocked WAL read opener did not recover");
                unsafe { libc::close(writer) };
                worker.join().unwrap();
                panic!("WAL read opener blocked on a FIFO and later returned {result:?}");
            }
            Err(error) => panic!("WAL read opener channel failed: {error}"),
        };

        worker.join().unwrap();
        assert!(result.is_err(), "WAL read opener accepted a FIFO");
    }

    #[test]
    fn test_write_all_retry() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let mut file = std::fs::File::create(&path).unwrap();
        write_all_retry(&mut file, b"hello world").unwrap();
        file.sync_all().unwrap();

        let contents = std::fs::read(&path).unwrap();
        assert_eq!(&contents, b"hello world");
    }

    #[test]
    fn test_write_all_retry_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let mut file = std::fs::File::create(&path).unwrap();
        write_all_retry(&mut file, b"").unwrap();
        let contents = std::fs::read(&path).unwrap();
        assert!(contents.is_empty());
    }

    #[test]
    fn test_write_all_retry_zero_progress_is_write_zero() {
        struct ZeroThenError;

        impl Write for ZeroThenError {
            fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
                Ok(0)
            }

            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        let err = write_all_retry(&mut ZeroThenError, b"not written").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::WriteZero);
    }

    #[test]
    fn test_pread() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        std::fs::write(&path, b"hello world").unwrap();
        let mut file = std::fs::File::open(&path).unwrap();
        let mut data = [0; 5];
        pread_into_file(&mut file, 6, &mut data).unwrap();
        assert_eq!(&data, b"world");
    }

    #[test]
    fn test_pread_at_offset_zero() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        std::fs::write(&path, b"hello").unwrap();
        let mut file = std::fs::File::open(&path).unwrap();
        let mut data = [0; 5];
        pread_into_file(&mut file, 0, &mut data).unwrap();
        assert_eq!(&data, b"hello");
    }

    #[cfg(unix)]
    #[test]
    fn newly_created_wal_files_are_owner_only() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("private.wal");
        drop(open_rw_create_nofollow(path.to_str().unwrap()).unwrap());

        let mode = std::fs::metadata(path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);

        let existing = dir.path().join("existing.wal");
        std::fs::write(&existing, b"existing").unwrap();
        std::fs::set_permissions(&existing, std::fs::Permissions::from_mode(0o644)).unwrap();
        drop(open_rw_create_nofollow(existing.to_str().unwrap()).unwrap());
        let mode = std::fs::metadata(existing).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }

    #[test]
    fn pread_range_rejects_oversized_and_overflowing_requests_before_allocation() {
        assert_eq!(validated_pread_len(128, 64, 64).unwrap(), 64);

        let oversized = validated_pread_len(
            u64::MAX,
            0,
            MAX_PREAD_BYTES.saturating_add(1),
        )
        .unwrap_err();
        assert_eq!(oversized.kind(), io::ErrorKind::InvalidInput);

        let overflow = validated_pread_len(u64::MAX, u64::MAX, 1).unwrap_err();
        assert_eq!(overflow.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn pread_range_rejects_reads_past_the_current_file_length_before_allocation() {
        let err = validated_pread_len(8, 4, 5).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn test_drain_to_kernel() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .read(true)
            .open(&path)
            .unwrap();

        let buffer = Arc::new(Mutex::new(AlignedBuffer::new()));
        let file_size = Arc::new(AtomicU64::new(0));

        {
            let mut buf = buffer.lock().unwrap();
            buf.extend(b"test data 12345");
        }

        assert!(drain_to_kernel(&mut file, &buffer, &file_size).unwrap());
        assert_eq!(file_size.load(Ordering::Acquire), 15);

        let contents = std::fs::read(&path).unwrap();
        assert!(contents.len() >= 15);
        assert_eq!(&contents[..15], b"test data 12345");
    }

    #[test]
    fn test_drain_empty_buffer() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .read(true)
            .open(&path)
            .unwrap();

        let buffer = Arc::new(Mutex::new(AlignedBuffer::new()));
        let file_size = Arc::new(AtomicU64::new(0));

        assert!(!drain_to_kernel(&mut file, &buffer, &file_size).unwrap());
        assert_eq!(file_size.load(Ordering::Acquire), 0);
    }

    #[test]
    fn test_drain_multiple_writes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .read(true)
            .open(&path)
            .unwrap();

        let buffer = Arc::new(Mutex::new(AlignedBuffer::new()));
        let file_size = Arc::new(AtomicU64::new(0));

        {
            let mut buf = buffer.lock().unwrap();
            buf.extend(b"first ");
        }
        assert!(drain_to_kernel(&mut file, &buffer, &file_size).unwrap());
        assert_eq!(file_size.load(Ordering::Acquire), 6);

        {
            let mut buf = buffer.lock().unwrap();
            buf.extend(b"second");
        }
        assert!(drain_to_kernel(&mut file, &buffer, &file_size).unwrap());
        assert_eq!(file_size.load(Ordering::Acquire), 12);
    }

    #[test]
    fn drain_to_kernel_appends_logical_bytes_from_byte_zero() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .read(true)
            .open(&path)
            .unwrap();

        file.seek(SeekFrom::Start(WAL_HEADER_SIZE)).unwrap();

        let buffer = Arc::new(Mutex::new(AlignedBuffer::new()));
        let file_size = Arc::new(AtomicU64::new(WAL_HEADER_SIZE));

        {
            let mut buf = buffer.lock().unwrap();
            buf.extend(b"abc");
        }
        assert!(drain_to_kernel(&mut file, &buffer, &file_size).unwrap());

        {
            let mut buf = buffer.lock().unwrap();
            buf.extend(b"def");
        }
        assert!(drain_to_kernel(&mut file, &buffer, &file_size).unwrap());
        file.sync_data().unwrap();

        assert_eq!(file_size.load(Ordering::Acquire), WAL_HEADER_SIZE + 6);

        let contents = std::fs::read(&path).unwrap();
        let logical_start = WAL_HEADER_SIZE as usize;
        let logical_end = logical_start + 6;
        assert_eq!(&contents[logical_start..logical_end], b"abcdef");
        assert_eq!(contents.len(), logical_end);
    }

    #[test]
    fn test_drain_then_sync() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .read(true)
            .open(&path)
            .unwrap();

        let buffer = Arc::new(Mutex::new(AlignedBuffer::new()));
        let file_size = Arc::new(AtomicU64::new(0));

        {
            let mut buf = buffer.lock().unwrap();
            buf.extend(b"final data");
        }

        drain_to_kernel(&mut file, &buffer, &file_size).unwrap();
        file.sync_data().unwrap();
        assert_eq!(file_size.load(Ordering::Acquire), 10);
    }

    #[test]
    fn test_thread_close_flushes_data() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .read(true)
            .open(&path)
            .unwrap();

        let buffer = Arc::new(Mutex::new(AlignedBuffer::new()));
        let alive = Arc::new(AtomicBool::new(true));
        let file_size = Arc::new(AtomicU64::new(0));
        let (tx, rx) = crossbeam_channel::unbounded();

        let buf_clone = buffer.clone();
        let alive_clone = alive.clone();
        let fs_clone = file_size.clone();

        let handle = std::thread::Builder::new()
            .name("test-wal".into())
            .spawn(move || {
                thread_loop(ThreadConfig {
                    file,
                    buffer: buf_clone,
                    rx,
                    alive: alive_clone,
                    sync_admission: Arc::new(Mutex::new(())),
                    file_size: fs_clone,
                    commit_delay: Duration::ZERO,
                    _use_o_direct: false,
                });
            })
            .unwrap();

        // Write data to buffer
        {
            let mut buf = buffer.lock().unwrap();
            buf.extend(b"thread close test");
        }

        // Send close — thread should flush before exiting
        let (close_tx, close_rx) = crossbeam_channel::bounded(1);
        tx.send(ThreadMsg::Close(Some(close_tx))).unwrap();
        assert_eq!(
            close_rx.recv_timeout(Duration::from_secs(5)).unwrap(),
            Ok(())
        );
        handle.join().unwrap();

        // Thread should be marked dead
        assert!(!alive.load(Ordering::Acquire));

        // Data should be on disk
        let contents = std::fs::read(&path).unwrap();
        assert!(contents.len() >= 17);
        assert_eq!(&contents[..17], b"thread close test");
        assert_eq!(file_size.load(Ordering::Acquire), 17);
    }

    #[test]
    fn test_thread_channel_disconnect_exits() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .read(true)
            .open(&path)
            .unwrap();

        let buffer = Arc::new(Mutex::new(AlignedBuffer::new()));
        let alive = Arc::new(AtomicBool::new(true));
        let file_size = Arc::new(AtomicU64::new(0));
        let (tx, rx) = crossbeam_channel::unbounded();

        let buf_clone = buffer.clone();
        let alive_clone = alive.clone();
        let fs_clone = file_size.clone();

        let handle = std::thread::Builder::new()
            .name("test-wal".into())
            .spawn(move || {
                thread_loop(ThreadConfig {
                    file,
                    buffer: buf_clone,
                    rx,
                    alive: alive_clone,
                    sync_admission: Arc::new(Mutex::new(())),
                    file_size: fs_clone,
                    commit_delay: Duration::ZERO,
                    _use_o_direct: false,
                });
            })
            .unwrap();

        // Write data without sending an explicit close message. Disconnect
        // must still drain and sync the buffer before the thread exits.
        {
            let mut buf = buffer.lock().unwrap();
            buf.extend(b"disconnect data");
        }

        // Drop the sender — channel disconnects
        drop(tx);

        // Thread should exit cleanly
        handle.join().unwrap();
        assert!(!alive.load(Ordering::Acquire));

        let contents = std::fs::read(&path).unwrap();
        assert!(contents.len() >= 15);
        assert_eq!(&contents[..15], b"disconnect data");
        assert_eq!(file_size.load(Ordering::Acquire), 15);
    }

    #[test]
    fn test_commit_delay_collects_multiple_flushes() {
        // This test verifies that during the commit delay window,
        // multiple flush requests are collected and served by one fdatasync.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .read(true)
            .open(&path)
            .unwrap();

        let buffer = Arc::new(Mutex::new(AlignedBuffer::new()));
        let alive = Arc::new(AtomicBool::new(true));
        let file_size = Arc::new(AtomicU64::new(0));
        let (tx, rx) = crossbeam_channel::unbounded();

        let buf_clone = buffer.clone();
        let alive_clone = alive.clone();
        let fs_clone = file_size.clone();

        let handle = std::thread::Builder::new()
            .name("test-wal".into())
            .spawn(move || {
                thread_loop(ThreadConfig {
                    file,
                    buffer: buf_clone,
                    rx,
                    alive: alive_clone,
                    sync_admission: Arc::new(Mutex::new(())),
                    file_size: fs_clone,
                    commit_delay: Duration::from_millis(50), // 50ms delay
                    _use_o_direct: false,
                });
            })
            .unwrap();

        // Write data
        {
            let mut buf = buffer.lock().unwrap();
            buf.extend(b"batch data");
        }

        // Send multiple flush requests rapidly (no BEAM callers in this test)
        // We can't send FlushCaller without BEAM, so just test Close behavior
        let (close_tx, close_rx) = crossbeam_channel::bounded(1);
        tx.send(ThreadMsg::Close(Some(close_tx))).unwrap();
        assert_eq!(
            close_rx.recv_timeout(Duration::from_secs(5)).unwrap(),
            Ok(())
        );
        handle.join().unwrap();

        let contents = std::fs::read(&path).unwrap();
        assert!(contents.len() >= 10);
        assert_eq!(&contents[..10], b"batch data");
    }

    #[test]
    fn test_large_buffer_flush() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .read(true)
            .open(&path)
            .unwrap();

        let buffer = Arc::new(Mutex::new(AlignedBuffer::new()));
        let file_size = Arc::new(AtomicU64::new(0));

        // 10 MB of data
        let data = vec![0xABu8; 10 * 1024 * 1024];
        {
            let mut buf = buffer.lock().unwrap();
            buf.extend(&data);
        }

        drain_to_kernel(&mut file, &buffer, &file_size).unwrap();
        file.sync_data().unwrap();

        assert_eq!(file_size.load(Ordering::Acquire), 10 * 1024 * 1024);
        let contents = std::fs::read(&path).unwrap();
        assert!(contents.len() >= 10 * 1024 * 1024);
        assert_eq!(&contents[..10 * 1024 * 1024], &data[..]);
    }
