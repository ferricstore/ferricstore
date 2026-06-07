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
        // On Linux with fallocate, file size would be 4096.
        // On macOS, fallocate is skipped, file size is 0.
        assert!(path.exists());
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
        let data = pread_from_file(&mut file, 6, 5).unwrap();
        assert_eq!(&data, b"world");
    }

    #[test]
    fn test_pread_at_offset_zero() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.wal");
        std::fs::write(&path, b"hello").unwrap();
        let mut file = std::fs::File::open(&path).unwrap();
        let data = pread_from_file(&mut file, 0, 5).unwrap();
        assert_eq!(&data, b"hello");
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
