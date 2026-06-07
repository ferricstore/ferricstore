//! `Store` — the public Bitcask API.
//!
//! Ties together the keydir (in-memory index), log writer (disk appends),
//! and hint files (fast startup). All writes go through the log first, then
//! update the keydir. Reads consult the keydir then do a single pread.
include!("sections/store_part_01.rs");
include!("sections/store_part_02.rs");

#[cfg(test)]
mod tests {
    include!("sections/store_tests_part_01.rs");
    include!("sections/store_tests_part_02.rs");
    include!("sections/store_tests_part_03.rs");
}
