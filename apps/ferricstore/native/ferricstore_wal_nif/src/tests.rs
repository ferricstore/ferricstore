// Integration-level tests for the WAL NIF.
// These test the full stack: WalHandle → AlignedBuffer → BackgroundThread → Disk
// without BEAM (pure Rust).

#[cfg(test)]
mod integration {
    use crate::background_thread::WAL_HEADER_SIZE;
    use crate::wal_handle::WalHandle;
    use std::sync::Arc;
    use std::time::Duration;

    const HDR: u64 = WAL_HEADER_SIZE;
    const HDR_USIZE: usize = WAL_HEADER_SIZE as usize;

    // -----------------------------------------------------------------------
    // Basic operations
    // -----------------------------------------------------------------------

    #[test]
    fn test_full_lifecycle() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir
            .path()
            .join("lifecycle.wal")
            .to_str()
            .unwrap()
            .to_string();

        let handle = WalHandle::open(path.clone(), 0, 0, 64 * 1024 * 1024).unwrap();
        assert!(handle.check_alive().is_ok());
        assert_eq!(handle.file_size(), HDR);

        handle.buffer_write(b"entry1").unwrap();
        handle.buffer_write(b"entry2").unwrap();
        handle.close().unwrap();

        assert_eq!(handle.file_size(), HDR + 12);
        assert!(handle.check_alive().is_err());

        let contents = std::fs::read(&path).unwrap();
        assert_eq!(&contents[HDR_USIZE..HDR_USIZE + 12], b"entry1entry2");
    }

    #[test]
    fn test_open_nonexistent_directory() {
        let result = WalHandle::open(
            "/nonexistent/path/test.wal".to_string(),
            0,
            0,
            64 * 1024 * 1024,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_open_readonly_directory() {
        // On most systems /proc or /sys are read-only
        #[cfg(target_os = "linux")]
        {
            let result = WalHandle::open("/proc/test.wal".to_string(), 0, 0, 64 * 1024 * 1024);
            assert!(result.is_err());
        }
    }

    #[test]
    fn test_double_close() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir
            .path()
            .join("double_close.wal")
            .to_str()
            .unwrap()
            .to_string();

        let handle = WalHandle::open(path, 0, 0, 64 * 1024 * 1024).unwrap();
        handle.close().unwrap();
        // Second close should not panic — thread already dead
        let result = handle.close();
        // May succeed (no-op) or error, but must not panic
        let _ = result;
    }

    #[test]
    fn test_close_without_writes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir
            .path()
            .join("no_writes.wal")
            .to_str()
            .unwrap()
            .to_string();

        let handle = WalHandle::open(path, 0, 0, 64 * 1024 * 1024).unwrap();
        handle.close().unwrap();
        assert_eq!(handle.file_size(), HDR);
    }

    // -----------------------------------------------------------------------
    // Stress / Load tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_high_throughput_writes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir
            .path()
            .join("throughput.wal")
            .to_str()
            .unwrap()
            .to_string();

        let handle = WalHandle::open(path, 0, 0, 256 * 1024 * 1024).unwrap();

        // 100K small writes
        let data = b"KV_ENTRY_256B_PADDING_XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";
        for _ in 0..100_000 {
            handle.buffer_write(data).unwrap();
        }

        handle.close().unwrap();
        assert_eq!(handle.file_size(), HDR + 100_000 * data.len() as u64);
    }

    #[test]
    fn test_concurrent_writers_stress() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir
            .path()
            .join("concurrent.wal")
            .to_str()
            .unwrap()
            .to_string();

        let handle = Arc::new(WalHandle::open(path, 0, 0, 256 * 1024 * 1024).unwrap());

        let mut threads = Vec::new();
        for thread_id in 0..20 {
            let h = handle.clone();
            threads.push(std::thread::spawn(move || {
                let entry = format!("T{thread_id:02}:ENTRY_DATA_HERE\n");
                for _ in 0..5000 {
                    h.buffer_write(entry.as_bytes()).unwrap();
                }
            }));
        }

        for t in threads {
            t.join().unwrap();
        }

        handle.close().unwrap();
        // 20 threads × 5000 writes = 100K entries
        assert!(handle.file_size() > 0);
    }

    #[test]
    fn test_writer_contention_under_load() {
        // Many writers competing for the mutex simultaneously
        let dir = tempfile::tempdir().unwrap();
        let path = dir
            .path()
            .join("contention.wal")
            .to_str()
            .unwrap()
            .to_string();

        let handle = Arc::new(WalHandle::open(path, 0, 0, 512 * 1024 * 1024).unwrap());

        let mut threads = Vec::new();
        // 50 threads, each writing 1000 × 1KB entries
        for thread_id in 0..50 {
            let h = handle.clone();
            threads.push(std::thread::spawn(move || {
                let entry = vec![thread_id as u8; 1024];
                for _ in 0..1000 {
                    h.buffer_write(&entry).unwrap();
                }
            }));
        }

        for t in threads {
            t.join().unwrap();
        }

        handle.close().unwrap();
        assert_eq!(handle.file_size(), HDR + 50 * 1000 * 1024);
    }

    #[test]
    fn test_rapid_open_close_cycles() {
        let dir = tempfile::tempdir().unwrap();

        for i in 0..50 {
            let path = dir
                .path()
                .join(format!("cycle_{i}.wal"))
                .to_str()
                .unwrap()
                .to_string();
            let handle = WalHandle::open(path, 0, 0, 64 * 1024 * 1024).unwrap();
            handle.buffer_write(b"data").unwrap();
            handle.close().unwrap();
        }
    }

    // -----------------------------------------------------------------------
    // Edge cases — sizes
    // -----------------------------------------------------------------------

    #[test]
    fn test_empty_write() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.wal").to_str().unwrap().to_string();

        let handle = WalHandle::open(path, 0, 0, 64 * 1024 * 1024).unwrap();
        handle.buffer_write(b"").unwrap();
        handle.close().unwrap();
        assert_eq!(handle.file_size(), HDR);
    }

    #[test]
    fn test_single_byte_write() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("single.wal").to_str().unwrap().to_string();

        let handle = WalHandle::open(path, 0, 0, 64 * 1024 * 1024).unwrap();
        handle.buffer_write(b"X").unwrap();
        handle.close().unwrap();
        assert_eq!(handle.file_size(), HDR + 1);
    }

    #[test]
    fn test_large_single_write() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("large.wal").to_str().unwrap().to_string();

        let handle = WalHandle::open(path, 0, 0, 256 * 1024 * 1024).unwrap();

        // 10 MB single write
        let data = vec![0x42u8; 10 * 1024 * 1024];
        handle.buffer_write(&data).unwrap();
        handle.close().unwrap();

        assert_eq!(handle.file_size(), HDR + 10 * 1024 * 1024);
    }

    #[test]
    fn test_many_tiny_writes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tiny.wal").to_str().unwrap().to_string();

        let handle = WalHandle::open(path, 0, 0, 64 * 1024 * 1024).unwrap();

        // 50K writes of 1 byte each
        for i in 0..50_000u32 {
            handle.buffer_write(&[(i % 256) as u8]).unwrap();
        }

        handle.close().unwrap();
        assert_eq!(handle.file_size(), HDR + 50_000);
    }

    #[test]
    fn test_exactly_alignment_boundary_writes() {
        // Write data that's exactly 4096 bytes (alignment boundary)
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("aligned.wal").to_str().unwrap().to_string();

        let handle = WalHandle::open(path, 0, 0, 64 * 1024 * 1024).unwrap();
        let data = vec![0xAA; 4096];
        handle.buffer_write(&data).unwrap();
        handle.close().unwrap();
        assert_eq!(handle.file_size(), HDR + 4096);
    }

    #[test]
    fn test_one_byte_over_alignment() {
        // 4097 bytes — forces padding to 8192
        let dir = tempfile::tempdir().unwrap();
        let path = dir
            .path()
            .join("over_align.wal")
            .to_str()
            .unwrap()
            .to_string();

        let handle = WalHandle::open(path.clone(), 0, 0, 64 * 1024 * 1024).unwrap();
        let data = vec![0xBB; 4097];
        handle.buffer_write(&data).unwrap();
        handle.close().unwrap();
        assert_eq!(handle.file_size(), HDR + 4097);

        let on_disk = std::fs::read(&path).unwrap();
        assert!(on_disk.len() >= HDR_USIZE + 4097);
        assert_eq!(&on_disk[HDR_USIZE..HDR_USIZE + 4097], &data[..]);
    }

    #[test]
    fn test_one_byte_under_alignment() {
        // 4095 bytes — still pads to 4096
        let dir = tempfile::tempdir().unwrap();
        let path = dir
            .path()
            .join("under_align.wal")
            .to_str()
            .unwrap()
            .to_string();

        let handle = WalHandle::open(path, 0, 0, 64 * 1024 * 1024).unwrap();
        let data = vec![0xCC; 4095];
        handle.buffer_write(&data).unwrap();
        handle.close().unwrap();
        assert_eq!(handle.file_size(), HDR + 4095);
    }

    // -----------------------------------------------------------------------
    // Backpressure
    // -----------------------------------------------------------------------

    #[test]
    fn test_backpressure_blocks_new_writes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bp.wal").to_str().unwrap().to_string();

        // 1KB max buffer
        let handle = WalHandle::open(path, 0, 0, 1024).unwrap();

        // Fill buffer to limit
        handle.buffer_write(&[0u8; 900]).unwrap();

        // Next write exceeds limit
        let result = handle.buffer_write(&[0u8; 200]);
        assert!(result.is_err());

        handle.close().unwrap();
    }

    #[test]
    fn test_backpressure_exact_boundary() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir
            .path()
            .join("bp_exact.wal")
            .to_str()
            .unwrap()
            .to_string();

        let handle = WalHandle::open(path, 0, 0, 100).unwrap();

        // Fill to exactly max
        let data = vec![0u8; 100];
        handle.buffer_write(&data).unwrap();

        // One more byte should fail
        let result = handle.buffer_write(b"x");
        assert!(result.is_err());

        handle.close().unwrap();
    }

    #[test]
    fn test_backpressure_allows_single_oversized_write_on_empty_buffer() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir
            .path()
            .join("bp_single_oversized.wal")
            .to_str()
            .unwrap()
            .to_string();

        let handle = WalHandle::open(path, 0, 0, 100).unwrap();
        let data = vec![0u8; 101];

        handle.buffer_write(&data).unwrap();
        assert!(handle.buffer_write(b"x").is_err());

        handle.close().unwrap();
    }

    #[test]
    fn test_backpressure_zero_byte_write_at_limit() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bp_zero.wal").to_str().unwrap().to_string();

        let handle = WalHandle::open(path, 0, 0, 100).unwrap();
        handle.buffer_write(&[0u8; 100]).unwrap();

        // Empty write at limit should succeed (adds 0 bytes)
        handle.buffer_write(b"").unwrap();

        handle.close().unwrap();
    }

    #[test]
    fn test_backpressure_concurrent_writers_hit_limit() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir
            .path()
            .join("bp_concurrent.wal")
            .to_str()
            .unwrap()
            .to_string();

        // Small buffer — concurrent writers will fight for space
        let handle = Arc::new(WalHandle::open(path, 0, 0, 4096).unwrap());

        let mut threads = Vec::new();
        let errors = Arc::new(std::sync::atomic::AtomicU64::new(0));

        for _ in 0..10 {
            let h = handle.clone();
            let err = errors.clone();
            threads.push(std::thread::spawn(move || {
                for _ in 0..100 {
                    let data = vec![0u8; 100];
                    if h.buffer_write(&data).is_err() {
                        err.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
            }));
        }

        for t in threads {
            t.join().unwrap();
        }

        // Some writes should have been rejected due to backpressure
        assert!(
            errors.load(std::sync::atomic::Ordering::Relaxed) > 0,
            "expected some backpressure errors with small buffer and many writers"
        );

        handle.close().unwrap();
    }

    // -----------------------------------------------------------------------
    // Recovery (pread)
    // -----------------------------------------------------------------------

    #[test]
    fn test_pread_various_offsets() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pread.wal").to_str().unwrap().to_string();

        let handle = WalHandle::open(path, 0, 0, 64 * 1024 * 1024).unwrap();
        handle.buffer_write(b"0123456789ABCDEF").unwrap();
        handle.close().unwrap();

        assert_eq!(&handle.pread(HDR, 4).unwrap(), b"0123");
        assert_eq!(&handle.pread(HDR + 4, 4).unwrap(), b"4567");
        assert_eq!(&handle.pread(HDR + 10, 6).unwrap(), b"ABCDEF");
        assert_eq!(&handle.pread(HDR, 16).unwrap(), b"0123456789ABCDEF");
    }

    #[test]
    fn test_pread_single_byte() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir
            .path()
            .join("pread_one.wal")
            .to_str()
            .unwrap()
            .to_string();

        let handle = WalHandle::open(path, 0, 0, 64 * 1024 * 1024).unwrap();
        handle.buffer_write(b"ABCDE").unwrap();
        handle.close().unwrap();

        assert_eq!(&handle.pread(HDR + 2, 1).unwrap(), b"C");
    }

    #[test]
    fn test_pread_at_end() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir
            .path()
            .join("pread_end.wal")
            .to_str()
            .unwrap()
            .to_string();

        let handle = WalHandle::open(path, 0, 0, 64 * 1024 * 1024).unwrap();
        handle.buffer_write(b"12345").unwrap();
        handle.close().unwrap();

        assert_eq!(&handle.pread(HDR + 4, 1).unwrap(), b"5");
    }

    #[test]
    fn test_pread_beyond_logical_size() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir
            .path()
            .join("pread_beyond.wal")
            .to_str()
            .unwrap()
            .to_string();

        let handle = WalHandle::open(path, 0, 0, 64 * 1024 * 1024).unwrap();
        handle.buffer_write(b"short").unwrap();
        handle.close().unwrap();

        let result = handle.pread(HDR, 100);
        let _ = result; // no crash or UB
    }

    #[test]
    fn test_pread_zero_length() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir
            .path()
            .join("pread_zero.wal")
            .to_str()
            .unwrap()
            .to_string();

        let handle = WalHandle::open(path, 0, 0, 64 * 1024 * 1024).unwrap();
        handle.buffer_write(b"data").unwrap();
        handle.close().unwrap();

        let result = handle.pread(HDR, 0).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_pread_large_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir
            .path()
            .join("pread_large.wal")
            .to_str()
            .unwrap()
            .to_string();

        let handle = WalHandle::open(path, 0, 0, 64 * 1024 * 1024).unwrap();
        let data = vec![0x42u8; 1024 * 1024]; // 1MB
        handle.buffer_write(&data).unwrap();
        handle.close().unwrap();

        let result = handle.pread(HDR, 1024 * 1024).unwrap();
        assert_eq!(result.len(), 1024 * 1024);
        assert_eq!(&result[..], &data[..]);
    }

    // -----------------------------------------------------------------------
    // Thread death detection
    // -----------------------------------------------------------------------

    #[test]
    fn test_operations_after_thread_death() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dead.wal").to_str().unwrap().to_string();

        let handle = WalHandle::open(path, 0, 0, 64 * 1024 * 1024).unwrap();
        handle.close().unwrap(); // kills thread

        // All operations should return error
        assert!(handle.check_alive().is_err());
        assert!(handle.buffer_write(b"should fail").is_err());
    }

    #[test]
    fn test_concurrent_close_attempts() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir
            .path()
            .join("multi_close.wal")
            .to_str()
            .unwrap()
            .to_string();

        let handle = Arc::new(WalHandle::open(path, 0, 0, 64 * 1024 * 1024).unwrap());

        let mut threads = Vec::new();
        for _ in 0..5 {
            let h = handle.clone();
            threads.push(std::thread::spawn(move || {
                let _ = h.close(); // multiple threads closing — should not panic
            }));
        }

        for t in threads {
            t.join().unwrap();
        }

        assert!(handle.check_alive().is_err());
    }

    // -----------------------------------------------------------------------
    // Data integrity
    // -----------------------------------------------------------------------

    #[test]
    fn test_write_read_integrity() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir
            .path()
            .join("integrity.wal")
            .to_str()
            .unwrap()
            .to_string();

        let handle = WalHandle::open(path.clone(), 0, 0, 64 * 1024 * 1024).unwrap();

        // Write known pattern
        for i in 0u32..1000 {
            let entry = format!("ENTRY_{i:05}_DATA\n");
            handle.buffer_write(entry.as_bytes()).unwrap();
        }
        handle.close().unwrap();

        let contents = std::fs::read(&path).unwrap();
        for i in 0u32..1000 {
            let expected = format!("ENTRY_{i:05}_DATA\n");
            let offset = HDR_USIZE + i as usize * expected.len();
            assert_eq!(
                &contents[offset..offset + expected.len()],
                expected.as_bytes(),
                "mismatch at entry {i}"
            );
        }
    }

    #[test]
    fn test_binary_data_integrity() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("binary.wal").to_str().unwrap().to_string();

        let handle = WalHandle::open(path.clone(), 0, 0, 64 * 1024 * 1024).unwrap();

        // Write all byte values (including null bytes, 0xFF, etc.)
        let mut data = Vec::new();
        for _ in 0..100 {
            for b in 0..=255u8 {
                data.push(b);
            }
        }
        handle.buffer_write(&data).unwrap();
        handle.close().unwrap();

        assert_eq!(handle.file_size(), HDR + 25600);
        let contents = std::fs::read(&path).unwrap();
        assert_eq!(&contents[HDR_USIZE..HDR_USIZE + 25600], &data[..]);
    }

    #[test]
    fn test_interleaved_write_patterns() {
        // Different sized writes interleaved — tests buffer coalescing
        let dir = tempfile::tempdir().unwrap();
        let path = dir
            .path()
            .join("interleaved.wal")
            .to_str()
            .unwrap()
            .to_string();

        let handle = WalHandle::open(path.clone(), 0, 0, 64 * 1024 * 1024).unwrap();

        let mut expected = Vec::new();
        for i in 0..1000 {
            let size = match i % 5 {
                0 => 1,     // tiny
                1 => 64,    // small
                2 => 256,   // medium
                3 => 4096,  // alignment boundary
                _ => 10000, // large
            };
            let data = vec![(i % 256) as u8; size];
            handle.buffer_write(&data).unwrap();
            expected.extend_from_slice(&data);
        }

        handle.close().unwrap();
        assert_eq!(handle.file_size(), HDR + expected.len() as u64);

        let contents = std::fs::read(&path).unwrap();
        assert_eq!(
            &contents[HDR_USIZE..HDR_USIZE + expected.len()],
            &expected[..]
        );
    }

    // -----------------------------------------------------------------------
    // Commit delay behavior
    // -----------------------------------------------------------------------

    #[test]
    fn test_commit_delay_does_not_lose_data() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("delay.wal").to_str().unwrap().to_string();

        // Long commit delay — data should still be flushed on close
        let handle = WalHandle::open(path.clone(), 500_000, 0, 64 * 1024 * 1024).unwrap(); // 500ms delay

        for i in 0..100 {
            let data = format!("entry_{i}\n");
            handle.buffer_write(data.as_bytes()).unwrap();
        }

        handle.close().unwrap();

        let contents = std::fs::read(&path).unwrap();
        assert_eq!(&contents[HDR_USIZE..HDR_USIZE + 8], b"entry_0\n");
        assert!(handle.file_size() > 0);
    }

    #[test]
    fn test_zero_delay_immediate_flush() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir
            .path()
            .join("no_delay.wal")
            .to_str()
            .unwrap()
            .to_string();

        let handle = WalHandle::open(path, 0, 0, 64 * 1024 * 1024).unwrap();
        handle.buffer_write(b"immediate").unwrap();
        handle.close().unwrap();
        assert_eq!(handle.file_size(), HDR + 9);
    }

    // -----------------------------------------------------------------------
    // File system edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_write_to_same_path_after_close() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("reopen.wal").to_str().unwrap().to_string();

        // First open + write + close
        let handle1 = WalHandle::open(path.clone(), 0, 0, 64 * 1024 * 1024).unwrap();
        handle1.buffer_write(b"first").unwrap();
        handle1.close().unwrap();

        // Second open overwrites (O_TRUNC behavior depends on implementation)
        let handle2 = WalHandle::open(path.clone(), 0, 0, 64 * 1024 * 1024).unwrap();
        handle2.buffer_write(b"second").unwrap();
        handle2.close().unwrap();

        // File should contain data (exact content depends on open flags)
        let contents = std::fs::read(&path).unwrap();
        assert!(contents.len() >= 6);
    }

    #[test]
    fn test_path_with_unicode() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir
            .path()
            .join("wal_ünïcödé_テスト.wal")
            .to_str()
            .unwrap()
            .to_string();

        let handle = WalHandle::open(path, 0, 0, 64 * 1024 * 1024).unwrap();
        handle.buffer_write(b"unicode path test").unwrap();
        handle.close().unwrap();
        assert_eq!(handle.file_size(), HDR + 17);
    }

    #[test]
    fn test_path_with_spaces() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir
            .path()
            .join("wal file with spaces.wal")
            .to_str()
            .unwrap()
            .to_string();

        let handle = WalHandle::open(path, 0, 0, 64 * 1024 * 1024).unwrap();
        handle.buffer_write(b"spaces").unwrap();
        handle.close().unwrap();
        assert_eq!(handle.file_size(), HDR + 6);
    }

    // -----------------------------------------------------------------------
    // Memory / resource leak tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_drop_without_close() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("drop.wal").to_str().unwrap().to_string();

        {
            let handle = WalHandle::open(path.clone(), 0, 0, 64 * 1024 * 1024).unwrap();
            handle.buffer_write(b"dropped without close").unwrap();
            // handle dropped here — Drop impl should signal thread to exit
        }

        // Give the thread time to clean up
        std::thread::sleep(Duration::from_millis(100));

        // File should exist (thread may or may not have flushed)
        assert!(std::path::Path::new(&path).exists());
    }

    #[test]
    fn test_many_handles_no_leak() {
        // Open and close many handles — should not leak threads or fds
        let dir = tempfile::tempdir().unwrap();

        for i in 0..100 {
            let path = dir
                .path()
                .join(format!("leak_{i}.wal"))
                .to_str()
                .unwrap()
                .to_string();
            let handle = WalHandle::open(path, 0, 0, 64 * 1024 * 1024).unwrap();
            handle.buffer_write(b"x").unwrap();
            handle.close().unwrap();
        }
        // If threads leaked, we'd have 100 zombie threads eating memory
    }

    #[test]
    fn test_arc_drop_last_reference() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir
            .path()
            .join("arc_drop.wal")
            .to_str()
            .unwrap()
            .to_string();

        let handle = Arc::new(WalHandle::open(path, 0, 0, 64 * 1024 * 1024).unwrap());
        let h2 = handle.clone();
        let h3 = handle.clone();

        h2.buffer_write(b"from h2").unwrap();
        h3.buffer_write(b"from h3").unwrap();

        drop(h2);
        drop(h3);
        // handle is last reference — drop should clean up
        handle.close().unwrap();
    }

    // -----------------------------------------------------------------------
    // Timing / ordering
    // -----------------------------------------------------------------------

    #[test]
    fn test_writes_are_ordered() {
        // Sequential writes from one thread must appear in order on disk
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ordered.wal").to_str().unwrap().to_string();

        let handle = WalHandle::open(path.clone(), 0, 0, 64 * 1024 * 1024).unwrap();

        for i in 0u64..10000 {
            let entry = i.to_le_bytes();
            handle.buffer_write(&entry).unwrap();
        }

        handle.close().unwrap();
        assert_eq!(handle.file_size(), HDR + 80000);

        let contents = std::fs::read(&path).unwrap();
        for i in 0u64..10000 {
            let offset = HDR_USIZE + (i * 8) as usize;
            let val = u64::from_le_bytes(contents[offset..offset + 8].try_into().unwrap());
            assert_eq!(val, i, "entry {i} out of order");
        }
    }

    #[test]
    fn test_close_is_durable() {
        // After close returns, all data must be on disk (not just in page cache)
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("durable.wal").to_str().unwrap().to_string();

        let handle = WalHandle::open(path.clone(), 0, 0, 64 * 1024 * 1024).unwrap();
        handle.buffer_write(b"durable data").unwrap();
        handle.close().unwrap();

        let contents = std::fs::read(&path).unwrap();
        assert_eq!(&contents[HDR_USIZE..HDR_USIZE + 12], b"durable data");
    }
}
