use super::*;

#[test]
fn file_create_and_read_header() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.bloom");

    // Create a bloom file manually
    let num_bits = 1000u64;
    let num_hashes = 7u32;
    let byte_count = num_bits.div_ceil(8) as usize;

    let mut file = File::create(&path).unwrap();
    let mut header = [0u8; HEADER_SIZE];
    header[0..8].copy_from_slice(&MAGIC.to_le_bytes());
    header[8..16].copy_from_slice(&num_bits.to_le_bytes());
    header[16..20].copy_from_slice(&num_hashes.to_le_bytes());
    file.write_all(&header).unwrap();
    file.write_all(&vec![0u8; byte_count]).unwrap();
    file.write_all(&[0_u8; crate::prob_txn::TOKEN_SIZE])
        .unwrap();
    file.sync_all().unwrap();
    drop(file);

    let file = File::open(&path).unwrap();
    let (bits, hashes, count) = file_read_header(&file).unwrap();
    assert_eq!(bits, 1000);
    assert_eq!(hashes, 7);
    assert_eq!(count, 0);
}

#[test]
fn file_hash_positions_deterministic() {
    let pos1 = file_hash_positions(b"hello", 1000, 7);
    let pos2 = file_hash_positions(b"hello", 1000, 7);
    assert_eq!(pos1, pos2);
    assert_eq!(pos1.len(), 7);
    for &p in &pos1 {
        assert!(p < 1000);
    }
}

#[test]
fn file_hash_positions_different_elements_differ() {
    let pos1 = file_hash_positions(b"hello", 100_000, 7);
    let pos2 = file_hash_positions(b"world", 100_000, 7);
    assert_ne!(pos1, pos2);
}

#[test]
fn count_after_add_rejects_overflow() {
    assert_eq!(bloom_count_after_add(0).unwrap(), 1);
    assert_eq!(bloom_count_after_add(u64::MAX - 1).unwrap(), u64::MAX);
    assert!(bloom_count_after_add(u64::MAX).is_err());
}

#[test]
fn file_read_header_bad_magic() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bad.bloom");
    std::fs::write(&path, [0xFF; 64]).unwrap();
    let file = File::open(&path).unwrap();
    assert!(file_read_header(&file).is_err());
}

#[test]
fn map_io_error_enoent() {
    let e = std::io::Error::new(std::io::ErrorKind::NotFound, "not found");
    match map_io_error(&e) {
        FileError::Enoent => {}
        FileError::Other(_) => panic!("expected Enoent"),
    }
}

#[test]
fn map_io_error_other() {
    let e = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied");
    match map_io_error(&e) {
        FileError::Other(s) => assert!(s.contains("denied")),
        FileError::Enoent => panic!("expected Other"),
    }
}

// -----------------------------------------------------------------------
// Edge case tests
// -----------------------------------------------------------------------

/// Helper: create a valid bloom file and return the path string.
fn create_bloom_file(dir: &std::path::Path, name: &str, num_bits: u64, num_hashes: u32) -> String {
    let path = dir.join(name);
    let byte_count = num_bits.div_ceil(8) as usize;
    let mut file = File::create(&path).unwrap();
    let mut header = [0u8; HEADER_SIZE];
    header[0..8].copy_from_slice(&MAGIC.to_le_bytes());
    header[8..16].copy_from_slice(&num_bits.to_le_bytes());
    header[16..20].copy_from_slice(&num_hashes.to_le_bytes());
    file.write_all(&header).unwrap();
    file.write_all(&vec![0u8; byte_count]).unwrap();
    file.write_all(&[0_u8; crate::prob_txn::TOKEN_SIZE])
        .unwrap();
    file.sync_all().unwrap();
    path.to_str().unwrap().to_string()
}

#[test]
fn empty_element_hashing_works() {
    // Zero-length binary should produce valid hash positions without panic.
    let positions = file_hash_positions(b"", 1000, 7);
    assert_eq!(positions.len(), 7);
    for &p in &positions {
        assert!(p < 1000);
    }
}

#[test]
fn large_element_hashing_works() {
    // 1MB element should hash without panic.
    let big = vec![0xABu8; 1_000_000];
    let positions = file_hash_positions(&big, 10_000, 7);
    assert_eq!(positions.len(), 7);
    for &p in &positions {
        assert!(p < 10_000);
    }
}

#[test]
fn truncated_header_returns_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("truncated.bloom");
    // Write only 16 bytes (less than HEADER_SIZE=32)
    std::fs::write(&path, [0u8; 16]).unwrap();
    let file = File::open(&path).unwrap();
    assert!(file_read_header(&file).is_err());
}

#[test]
fn complete_header_rejects_a_truncated_bit_region() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("missing_bits.bloom");
    let mut header = [0u8; HEADER_SIZE];
    header[0..8].copy_from_slice(&MAGIC.to_le_bytes());
    header[8..16].copy_from_slice(&64u64.to_le_bytes());
    header[16..20].copy_from_slice(&1u32.to_le_bytes());
    std::fs::write(&path, header).unwrap();

    assert!(file_read_header(&File::open(path).unwrap()).is_err());
}

#[test]
fn header_rejects_nonzero_reserved_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("reserved.bloom");
    let mut data = [0u8; HEADER_SIZE + 1];
    data[0..8].copy_from_slice(&MAGIC.to_le_bytes());
    data[8..16].copy_from_slice(&8u64.to_le_bytes());
    data[16..20].copy_from_slice(&1u32.to_le_bytes());
    data[20] = 1;
    std::fs::write(&path, data).unwrap();

    let error = file_read_header(&File::open(path).unwrap()).unwrap_err();

    assert!(error.contains("reserved"));
}

#[test]
fn header_rejects_count_larger_than_the_bit_array() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("impossible_count.bloom");
    let mut data = [0u8; HEADER_SIZE + 1];
    data[0..8].copy_from_slice(&MAGIC.to_le_bytes());
    data[8..16].copy_from_slice(&8u64.to_le_bytes());
    data[16..20].copy_from_slice(&1u32.to_le_bytes());
    data[24..32].copy_from_slice(&9u64.to_le_bytes());
    std::fs::write(&path, data).unwrap();

    let error = file_read_header(&File::open(path).unwrap()).unwrap_err();

    assert!(error.contains("count"));
}

#[test]
fn read_exact_at_rejects_short_bitset() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("short_bitset.bloom");
    std::fs::write(&path, [0u8; 0]).unwrap();
    let file = File::open(&path).unwrap();
    let mut buf = [0u8; 1];

    let err = bloom_read_exact_at(&file, &mut buf, 0, "bit").unwrap_err();

    assert!(err.contains("truncated"));
}

#[test]
fn wrong_magic_returns_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wrong_magic.bloom");
    let mut data = [0u8; HEADER_SIZE + 8];
    // Write a different magic number
    data[0..8].copy_from_slice(&0xDEAD_BEEF_u64.to_le_bytes());
    std::fs::write(&path, data).unwrap();
    let file = File::open(&path).unwrap();
    let result = file_read_header(&file);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("magic"));
}

#[test]
fn header_with_zero_num_bits_returns_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("zero_bits.bloom");
    let mut data = [0u8; HEADER_SIZE];
    data[0..8].copy_from_slice(&MAGIC.to_le_bytes());
    data[8..16].copy_from_slice(&0u64.to_le_bytes());
    data[16..20].copy_from_slice(&1u32.to_le_bytes());
    std::fs::write(&path, data).unwrap();
    let file = File::open(&path).unwrap();

    let result = file_read_header(&file);

    assert!(result.is_err());
    assert!(result.unwrap_err().contains("num_bits"));
}

#[test]
fn bloom_size_rejects_bit_arrays_above_the_native_resource_ceiling() {
    assert!(bloom_file_size(MAX_BLOOM_BITS).is_ok());

    let error = bloom_file_size(MAX_BLOOM_BITS + 1).unwrap_err();

    assert!(error.contains("exceeds"));
}

#[test]
fn header_rejects_bit_arrays_above_the_native_resource_ceiling() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("oversized_bits.bloom");
    let mut data = [0u8; HEADER_SIZE];
    data[0..8].copy_from_slice(&MAGIC.to_le_bytes());
    data[8..16].copy_from_slice(&(MAX_BLOOM_BITS + 1).to_le_bytes());
    data[16..20].copy_from_slice(&1u32.to_le_bytes());
    std::fs::write(&path, data).unwrap();
    let file = File::open(&path).unwrap();

    let error = file_read_header(&file).unwrap_err();

    assert!(error.contains("exceeds"));
}

#[test]
fn header_with_zero_num_hashes_returns_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("zero_hashes.bloom");
    let mut data = [0u8; HEADER_SIZE + 1];
    data[0..8].copy_from_slice(&MAGIC.to_le_bytes());
    data[8..16].copy_from_slice(&8u64.to_le_bytes());
    data[16..20].copy_from_slice(&0u32.to_le_bytes());
    std::fs::write(&path, data).unwrap();
    let file = File::open(&path).unwrap();

    let result = file_read_header(&file);

    assert!(result.is_err());
    assert!(result.unwrap_err().contains("num_hashes"));
}

#[test]
fn nonexistent_file_returns_io_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("does_not_exist.bloom");
    let result = crate::open_random_read(&path);
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::NotFound);
}

#[test]
fn minimum_size_bloom_filter() {
    // num_bits=1 -- smallest possible bloom filter
    let dir = tempfile::tempdir().unwrap();
    let path_str = create_bloom_file(dir.path(), "min.bloom", 1, 1);

    // Add and check an element
    let file = crate::open_random_rw(Path::new(&path_str)).unwrap();
    let positions = file_hash_positions(b"test", 1, 1);
    assert_eq!(positions.len(), 1);
    assert_eq!(positions[0], 0);

    // Write the bit
    let file_offset = HEADER_SIZE as u64;
    let mut buf = [0u8; 1];
    file.read_at(&mut buf, file_offset).unwrap();
    assert_eq!(buf[0], 0);

    let mask = 1u8 << 0;
    buf[0] |= mask;
    file.write_at(&buf, file_offset).unwrap();

    // Read it back
    let mut buf2 = [0u8; 1];
    file.read_at(&mut buf2, file_offset).unwrap();
    assert_ne!(buf2[0] & mask, 0);
}

#[test]
fn add_then_exists_roundtrip() {
    // Full roundtrip: create file, add elements, check exists, verify count
    let dir = tempfile::tempdir().unwrap();
    let path_str = create_bloom_file(dir.path(), "roundtrip.bloom", 10000, 7);

    // Add element "hello"
    let file = crate::open_random_rw(Path::new(&path_str)).unwrap();
    let (num_bits, num_hashes, count) = file_read_header(&file).unwrap();
    assert_eq!(count, 0);

    let positions = file_hash_positions(b"hello", num_bits, num_hashes);
    let mut any_new = false;
    for pos in &positions {
        let byte_index = pos / 8;
        let bit_offset = (pos % 8) as u8;
        let file_offset = HEADER_SIZE as u64 + byte_index;
        let mut buf = [0u8; 1];
        file.read_at(&mut buf, file_offset).unwrap();
        let mask = 1u8 << bit_offset;
        if (buf[0] & mask) == 0 {
            buf[0] |= mask;
            file.write_at(&buf, file_offset).unwrap();
            any_new = true;
        }
    }
    assert!(any_new);

    // Update count
    let new_count = 1u64;
    file.write_at(&new_count.to_le_bytes(), 24).unwrap();
    drop(file);

    // Verify exists
    let file = crate::open_random_read(Path::new(&path_str)).unwrap();
    let (num_bits, num_hashes, count) = file_read_header(&file).unwrap();
    assert_eq!(count, 1);

    let positions = file_hash_positions(b"hello", num_bits, num_hashes);
    for pos in &positions {
        let byte_index = pos / 8;
        let bit_offset = (pos % 8) as u8;
        let file_offset = HEADER_SIZE as u64 + byte_index;
        let mut buf = [0u8; 1];
        file.read_at(&mut buf, file_offset).unwrap();
        assert_ne!(buf[0] & (1u8 << bit_offset), 0, "bit {pos} should be set");
    }

    // Verify "world" is NOT in the filter (probabilistic, but with 10000 bits
    // and only 1 element, false positive is astronomically unlikely)
    let positions = file_hash_positions(b"world", num_bits, num_hashes);
    let mut found_all = true;
    for pos in &positions {
        let byte_index = pos / 8;
        let bit_offset = (pos % 8) as u8;
        let file_offset = HEADER_SIZE as u64 + byte_index;
        let mut buf = [0u8; 1];
        file.read_at(&mut buf, file_offset).unwrap();
        if (buf[0] & (1u8 << bit_offset)) == 0 {
            found_all = false;
            break;
        }
    }
    assert!(
        !found_all,
        "with only 1 element in 10000 bits, false positive should be extremely rare"
    );
}

#[test]
fn hash_positions_with_num_bits_1_does_not_panic() {
    // All positions should be 0 when num_bits=1
    let positions = file_hash_positions(b"anything", 1, 10);
    assert_eq!(positions.len(), 10);
    for &p in &positions {
        assert_eq!(p, 0);
    }
}

#[test]
fn null_bytes_in_element() {
    // Element containing null bytes should hash without panic
    let element = b"hello\x00world\x00";
    let positions = file_hash_positions(element, 1000, 7);
    assert_eq!(positions.len(), 7);
    for &p in &positions {
        assert!(p < 1000);
    }

    // Should differ from element without nulls
    let pos2 = file_hash_positions(b"helloworld", 1000, 7);
    assert_ne!(positions, pos2);
}

#[test]
fn concurrent_reads_from_same_file() {
    let dir = tempfile::tempdir().unwrap();
    let path_str = create_bloom_file(dir.path(), "concurrent.bloom", 1000, 7);

    // Open two read handles concurrently
    let file1 = crate::open_random_read(Path::new(&path_str)).unwrap();
    let file2 = crate::open_random_read(Path::new(&path_str)).unwrap();

    let (bits1, hashes1, count1) = file_read_header(&file1).unwrap();
    let (bits2, hashes2, count2) = file_read_header(&file2).unwrap();

    assert_eq!(bits1, bits2);
    assert_eq!(hashes1, hashes2);
    assert_eq!(count1, count2);
}
