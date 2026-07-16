//! Append-only log file for Bitcask.
//!
//! On-disk record format (little-endian):
//!
//! ```text
//! [ crc32: u32 | timestamp_ms: u64 | expire_at_ms: u64 | key_size: u16 | value_size: u32 | key: [u8] | value: [u8] ]
//! ```
//!
//! A tombstone record has `value_size = u32::MAX` and no value bytes. Zero is
//! reserved for a live empty value, so the two states remain unambiguous.
//!
//! The CRC32 covers everything after the checksum field:
//!   `timestamp_ms || expire_at_ms || key_size || value_size || key || value`
include!("sections/log_part_01.rs");
include!("sections/log_part_02.rs");
include!("sections/log_part_03.rs");

#[cfg(test)]
mod tests {
    include!("sections/log_tests_part_01.rs");
    include!("sections/log_tests_part_02.rs");
    include!("sections/log_tests_part_03.rs");
}
