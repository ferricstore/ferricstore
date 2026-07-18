//! Cuckoo filter implementation for FerricStore.
//!
//! A space-efficient probabilistic data structure similar to Bloom filters,
//! but supporting deletion and approximate counting. Stores fingerprints of
//! elements in a hash table with two candidate bucket positions per element.
//!
//! ## File layout
//!
//! ```text
//! [magic: 2B][version: 1B][num_buckets: 4B][bucket_size: 1B]
//! [fingerprint_size: 1B][max_kicks: 2B][num_items: 8B][num_deletes: 8B]
//! [buckets: num_buckets * bucket_size * fingerprint_size bytes]
//! [mutation token: 16B]
//! ```
//!
//! Total header size: 27 bytes.
include!("sections/cuckoo_part_01.rs");
include!("sections/cuckoo_part_02.rs");

pub(crate) fn recover_sidecar(path: &std::path::Path) -> Result<(), String> {
    let file = crate::open_random_rw_locked(path)
        .map_err(|error| format!("open cuckoo sidecar for recovery: {error}"))?;
    let header = cuckoo_read_header(&file)?;
    crate::prob_txn::recover(
        &file,
        path,
        cuckoo_mutation_token_offset(
            header.num_buckets,
            header.bucket_size,
            header.fingerprint_size,
        )?,
        cuckoo_file_size(
            header.num_buckets,
            header.bucket_size,
            header.fingerprint_size,
        )?,
    )
}

#[cfg(test)]
mod tests {
    include!("sections/cuckoo_tests_part_01.rs");
    include!("sections/cuckoo_tests_part_02.rs");
}
