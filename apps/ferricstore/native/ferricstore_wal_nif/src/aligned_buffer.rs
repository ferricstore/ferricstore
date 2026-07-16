// Aligned buffer for O_DIRECT writes.
//
// O_DIRECT on Linux requires:
// - Buffer address aligned to filesystem block size (4096)
// - Write size aligned to 512 bytes (sector size)
// - File offset aligned to 512 bytes
//
// This buffer handles alignment transparently. On macOS (no O_DIRECT),
// alignment is unnecessary but harmless.

use std::alloc::{self, Layout};
use std::ptr;

const ALIGNMENT: usize = 4096;
const INITIAL_CAPACITY: usize = 256 * 1024; // 256 KB

/// A byte buffer with 4096-byte aligned memory.
pub struct AlignedBuffer {
    ptr: *mut u8,
    len: usize,
    capacity: usize,
}

// Safety: AlignedBuffer owns its memory and only accessed under Mutex
unsafe impl Send for AlignedBuffer {}
unsafe impl Sync for AlignedBuffer {}

impl AlignedBuffer {
    /// Create a new empty aligned buffer with default capacity.
    pub fn new() -> Self {
        Self::with_capacity(INITIAL_CAPACITY)
    }

    /// Create a new empty aligned buffer with specified capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        let capacity = capacity.max(ALIGNMENT); // minimum one alignment unit
        let capacity = round_up(capacity, ALIGNMENT);
        let ptr = alloc_aligned(capacity);
        AlignedBuffer {
            ptr,
            len: 0,
            capacity,
        }
    }

    /// Append bytes to the buffer, growing if needed.
    pub fn extend(&mut self, data: &[u8]) {
        let required = self.len + data.len();
        if required > self.capacity {
            self.grow(required);
        }
        unsafe {
            ptr::copy_nonoverlapping(data.as_ptr(), self.ptr.add(self.len), data.len());
        }
        self.len += data.len();
    }

    /// Prepend bytes before the current buffer contents.
    ///
    /// Used when a background WAL drain fails after taking bytes out of the
    /// shared buffer. New writes may have arrived while the kernel write was in
    /// flight, so the failed bytes must be restored before them to preserve WAL
    /// order on retry.
    pub fn prepend(&mut self, data: &[u8]) {
        if data.is_empty() {
            return;
        }

        let required = self.len + data.len();
        if required > self.capacity {
            self.grow(required);
        }

        unsafe {
            ptr::copy(self.ptr, self.ptr.add(data.len()), self.len);
            ptr::copy_nonoverlapping(data.as_ptr(), self.ptr, data.len());
        }
        self.len = required;
    }

    /// Take ownership of the buffer contents, replacing with a fresh empty buffer.
    /// Returns the data pointer, logical length, and padded length (for O_DIRECT).
    /// Caller is responsible for freeing the returned pointer via `free_aligned`.
    pub fn take(&mut self) -> TakenBuffer {
        if self.len == 0 {
            return TakenBuffer::empty();
        }

        let old_ptr = self.ptr;
        let old_len = self.len;
        let padded_len = round_up(old_len, ALIGNMENT);

        // Zero-fill the padding region so recovery sees clean zeros
        if padded_len > old_len {
            unsafe {
                ptr::write_bytes(old_ptr.add(old_len), 0, padded_len - old_len);
            }
        }

        // Allocate a fresh buffer for new writes
        self.ptr = alloc_aligned(self.capacity);
        self.len = 0;

        TakenBuffer {
            ptr: old_ptr,
            logical_len: old_len,
            padded_len,
            allocation_size: self.capacity,
        }
    }

    /// Current logical data length (not padded).
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the buffer is empty.
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Clear the buffer without deallocating.
    #[allow(dead_code)]
    pub fn clear(&mut self) {
        self.len = 0;
    }

    fn grow(&mut self, min_capacity: usize) {
        let new_capacity = round_up(min_capacity.max(self.capacity * 2), ALIGNMENT);
        let new_ptr = alloc_aligned(new_capacity);
        if self.len > 0 {
            unsafe {
                ptr::copy_nonoverlapping(self.ptr, new_ptr, self.len);
            }
        }
        free_aligned(self.ptr, self.capacity);
        self.ptr = new_ptr;
        self.capacity = new_capacity;
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        if self.capacity > 0 {
            free_aligned(self.ptr, self.capacity);
        }
    }
}

/// Owned buffer data taken from AlignedBuffer. Must be freed explicitly.
pub struct TakenBuffer {
    ptr: *mut u8,
    /// Actual data length.
    pub logical_len: usize,
    /// Length padded to ALIGNMENT for O_DIRECT writes.
    pub padded_len: usize,
    /// Exact layout size used to allocate `ptr`.
    allocation_size: usize,
}

unsafe impl Send for TakenBuffer {}

impl TakenBuffer {
    fn empty() -> Self {
        TakenBuffer {
            ptr: ptr::null_mut(),
            logical_len: 0,
            padded_len: 0,
            allocation_size: 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.logical_len == 0
    }

    /// Get the padded data as a slice for writing.
    /// The slice length is `padded_len` (aligned), but only `logical_len` bytes are real data.
    #[allow(dead_code)]
    pub fn as_padded_slice(&self) -> &[u8] {
        if self.ptr.is_null() {
            return &[];
        }
        unsafe { std::slice::from_raw_parts(self.ptr, self.padded_len) }
    }

    /// Get only the bytes that belong to the logical WAL stream.
    pub fn as_logical_slice(&self) -> &[u8] {
        if self.ptr.is_null() {
            return &[];
        }
        unsafe { std::slice::from_raw_parts(self.ptr, self.logical_len) }
    }
}

impl Drop for TakenBuffer {
    fn drop(&mut self) {
        if !self.ptr.is_null() && self.allocation_size > 0 {
            free_aligned(self.ptr, self.allocation_size);
        }
    }
}

fn round_up(value: usize, alignment: usize) -> usize {
    (value + alignment - 1) & !(alignment - 1)
}

fn alloc_aligned(size: usize) -> *mut u8 {
    let layout = Layout::from_size_align(size, ALIGNMENT).expect("invalid layout");
    let ptr = unsafe { alloc::alloc_zeroed(layout) };
    if ptr.is_null() {
        alloc::handle_alloc_error(layout);
    }
    ptr
}

fn free_aligned(ptr: *mut u8, size: usize) {
    let layout = Layout::from_size_align(size, ALIGNMENT).expect("invalid layout");
    unsafe {
        alloc::dealloc(ptr, layout);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_buffer() {
        let buf = AlignedBuffer::new();
        assert!(buf.is_empty());
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn test_extend_and_len() {
        let mut buf = AlignedBuffer::new();
        buf.extend(b"hello");
        assert_eq!(buf.len(), 5);
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_extend_multiple() {
        let mut buf = AlignedBuffer::new();
        buf.extend(b"hello ");
        buf.extend(b"world");
        assert_eq!(buf.len(), 11);
    }

    #[test]
    fn test_prepend_preserves_existing_order() {
        let mut buf = AlignedBuffer::new();
        buf.extend(b"new");
        buf.prepend(b"entry");

        let taken = buf.take();

        assert_eq!(taken.as_logical_slice(), b"entrynew");
    }

    #[test]
    fn test_take_returns_data() {
        let mut buf = AlignedBuffer::new();
        buf.extend(b"test data");
        let taken = buf.take();
        assert_eq!(taken.logical_len, 9);
        assert!(taken.padded_len >= 9);
        assert_eq!(taken.allocation_size, INITIAL_CAPACITY);
        assert!(taken.padded_len < taken.allocation_size);
        assert_eq!(taken.padded_len % ALIGNMENT, 0);
        assert_eq!(&taken.as_padded_slice()[..9], b"test data");
        // Buffer should be empty after take
        assert!(buf.is_empty());
    }

    #[test]
    fn test_take_empty() {
        let mut buf = AlignedBuffer::new();
        let taken = buf.take();
        assert!(taken.is_empty());
        assert_eq!(taken.as_padded_slice().len(), 0);
    }

    #[test]
    fn test_take_padding_is_zeros() {
        let mut buf = AlignedBuffer::new();
        buf.extend(b"x"); // 1 byte
        let taken = buf.take();
        assert_eq!(taken.padded_len, ALIGNMENT);
        let slice = taken.as_padded_slice();
        assert_eq!(slice[0], b'x');
        // All padding bytes should be zero
        for &b in &slice[1..] {
            assert_eq!(b, 0, "padding byte should be zero");
        }
    }

    #[test]
    fn test_alignment() {
        let buf = AlignedBuffer::new();
        // ptr should be aligned to ALIGNMENT
        assert_eq!(buf.ptr as usize % ALIGNMENT, 0);
    }

    #[test]
    fn test_taken_buffer_alignment() {
        let mut buf = AlignedBuffer::new();
        buf.extend(b"some data here");
        let taken = buf.take();
        assert_eq!(taken.ptr as usize % ALIGNMENT, 0);
    }

    #[test]
    fn test_grow() {
        let mut buf = AlignedBuffer::with_capacity(ALIGNMENT); // minimal capacity
                                                               // Write more than capacity
        let data = vec![0xABu8; ALIGNMENT * 3];
        buf.extend(&data);
        assert_eq!(buf.len(), ALIGNMENT * 3);
        // Verify data integrity after grow
        let taken = buf.take();
        assert_eq!(&taken.as_padded_slice()[..ALIGNMENT * 3], &data[..]);
    }

    #[test]
    fn test_clear() {
        let mut buf = AlignedBuffer::new();
        buf.extend(b"some data");
        buf.clear();
        assert!(buf.is_empty());
        assert_eq!(buf.len(), 0);
    }

    #[test]
    fn test_reuse_after_take() {
        let mut buf = AlignedBuffer::new();
        buf.extend(b"first");
        let _taken1 = buf.take();

        buf.extend(b"second");
        let taken2 = buf.take();
        assert_eq!(taken2.logical_len, 6);
        assert_eq!(&taken2.as_padded_slice()[..6], b"second");
    }

    #[test]
    fn test_large_write() {
        let mut buf = AlignedBuffer::new();
        let data = vec![0x42u8; 1024 * 1024]; // 1 MB
        buf.extend(&data);
        assert_eq!(buf.len(), 1024 * 1024);
        let taken = buf.take();
        assert_eq!(taken.logical_len, 1024 * 1024);
        assert_eq!(taken.padded_len % ALIGNMENT, 0);
    }

    #[test]
    fn test_exact_alignment_write() {
        let mut buf = AlignedBuffer::new();
        let data = vec![0x42u8; ALIGNMENT]; // exactly one alignment unit
        buf.extend(&data);
        let taken = buf.take();
        assert_eq!(taken.logical_len, ALIGNMENT);
        assert_eq!(taken.padded_len, ALIGNMENT); // no extra padding needed
    }

    #[test]
    fn test_many_small_extends() {
        let mut buf = AlignedBuffer::new();
        for i in 0..10000 {
            buf.extend(&[i as u8]);
        }
        assert_eq!(buf.len(), 10000);
        let taken = buf.take();
        for i in 0..10000 {
            assert_eq!(taken.as_padded_slice()[i], i as u8);
        }
    }

    #[test]
    fn test_round_up() {
        assert_eq!(round_up(0, 4096), 0);
        assert_eq!(round_up(1, 4096), 4096);
        assert_eq!(round_up(4096, 4096), 4096);
        assert_eq!(round_up(4097, 4096), 8192);
        assert_eq!(round_up(512, 512), 512);
        assert_eq!(round_up(513, 512), 1024);
    }
}
