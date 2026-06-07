//! Cuckoo filter implementation for FerricStore.
//!
//! A space-efficient probabilistic data structure similar to Bloom filters,
//! but supporting deletion and approximate counting. Stores fingerprints of
//! elements in a hash table with two candidate bucket positions per element.
//!
//! ## File layout
//!
//! ```text
//! [magic: 2B][version: 1B][capacity: 4B][bucket_size: 1B]
//! [fingerprint_size: 1B][max_kicks: 2B][num_items: 8B][num_deletes: 8B]
//! [buckets: capacity * bucket_size * fingerprint_size bytes]
//! ```
//!
//! Total header size: 27 bytes.
include!("sections/cuckoo_part_01.rs");
include!("sections/cuckoo_part_02.rs");

#[cfg(test)]
mod tests {
    include!("sections/cuckoo_tests_part_01.rs");
    include!("sections/cuckoo_tests_part_02.rs");
}
