fn cuckoo_file_mutate_at(
    path: &str,
    receipt_path: &str,
    element: &[u8],
    mutation_index: u64,
    mutation_ordinal: u64,
    mutation: CuckooMutation,
) -> Result<u64, FileOpenError> {
    let token = crate::prob_txn::MutationToken::new(mutation_index, mutation_ordinal);
    if token == crate::prob_txn::MutationToken::ZERO {
        return Err(FileOpenError::Other(
            "cuckoo mutation token must be non-zero".into(),
        ));
    }
    let file = cuckoo_file_open_rw(path)?;
    let header = cuckoo_read_header(&file).map_err(FileOpenError::Other)?;
    let result = cuckoo_transactional_mutation(
        &file,
        Path::new(receipt_path),
        &header,
        element,
        mutation,
        token,
    )
    .map_err(FileOpenError::Other)?;
    crate::fadvise_dontneed(&file, 0, 0);
    Ok(result)
}

/// Create a new cuckoo filter with at least `capacity` item slots.
/// Uses fingerprint_size=1 and max_kicks=500.
/// Returns `{:ok, :ok}` or `{:error, reason}`.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn cuckoo_file_create(
    env: Env,
    path: String,
    capacity: u32,
    bucket_size: u8,
) -> NifResult<Term> {
    let fingerprint_size = FILE_DEFAULT_FINGERPRINT_SIZE as u8;
    let max_kicks = FILE_DEFAULT_MAX_KICKS;
    let num_buckets = match cuckoo_bucket_count(capacity, bucket_size) {
        Ok(count) => count,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };
    let file_size = match cuckoo_file_size(num_buckets, bucket_size, fingerprint_size) {
        Ok(size) => size,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    // Ensure parent directory exists.
    let p = Path::new(&path);
    if let Some(parent) = p.parent() {
        if !parent.as_os_str().is_empty() {
            crate::fs_nif::create_dir_all_nofollow(parent)
                .map_err(|e| rustler::Error::Term(Box::new(format!("mkdir: {e}"))))?;
        }
    }

    // Write header + zeroed buckets.
    let mut file = crate::create_staged_locked_nofollow(p)
        .map_err(|e| rustler::Error::Term(Box::new(format!("create: {e}"))))?;
    let mut header = [0u8; HEADER_SIZE];
    header[0..2].copy_from_slice(&MAGIC);
    header[2] = VERSION;
    header[3..7].copy_from_slice(&num_buckets.to_le_bytes());
    header[7] = bucket_size;
    header[8] = fingerprint_size;
    header[9..11].copy_from_slice(&max_kicks.to_le_bytes());
    // num_items = 0 at bytes 11..19 (already zero)
    // num_deletes = 0 at bytes 19..27 (already zero)

    file.write_all(&header)
        .map_err(|e| rustler::Error::Term(Box::new(format!("write header: {e}"))))?;
    file.set_len(file_size)
        .map_err(|e| rustler::Error::Term(Box::new(format!("set file size: {e}"))))?;
    file.publish()
        .map_err(|e| rustler::Error::Term(Box::new(format!("publish: {e}"))))?;

    Ok((atoms::ok(), atoms::ok()).encode(env))
}

/// Add an element to a cuckoo filter file.
/// Opens the file, reads header, inserts fingerprint, updates counters, closes.
/// Returns `{:ok, 1}` or `{:error, "filter is full"}`.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(
    clippy::needless_pass_by_value,
    clippy::unnecessary_wraps,
    clippy::too_many_lines
)]
pub fn cuckoo_file_add<'a>(env: Env<'a>, path: String, element: Binary<'a>) -> NifResult<Term<'a>> {
    let file = match cuckoo_file_open_rw(&path) {
        Ok(f) => f,
        Err(e) => {
            return Ok(encode_file_open_error(env, e));
        }
    };

    let hdr = match cuckoo_read_header(&file) {
        Ok(h) => h,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    let (fp, b1) = cuckoo_file_fingerprint_and_bucket(
        element.as_slice(),
        hdr.fingerprint_size as usize,
        hdr.num_buckets,
    );
    let b2 = cuckoo_file_alternate_bucket(b1, &fp, hdr.num_buckets);
    let next_num_items = match cuckoo_num_items_after_insert(hdr.num_items) {
        Ok(next) => next,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    // Try primary bucket.
    for slot in 0..hdr.bucket_size {
        let s = match cuckoo_file_read_slot(
            &file,
            b1,
            slot as usize,
            hdr.bucket_size,
            hdr.fingerprint_size,
        ) {
            Ok(s) => s,
            Err(e) => return Ok((atoms::error(), e).encode(env)),
        };
        if s.iter().all(|&b| b == 0) {
            if let Err(e) = cuckoo_file_write_slot(
                &file,
                b1,
                slot as usize,
                hdr.bucket_size,
                hdr.fingerprint_size,
                &fp,
            ) {
                return Ok((atoms::error(), e).encode(env));
            }
            if let Err(e) = cuckoo_file_write_num_items(&file, next_num_items) {
                return Ok((atoms::error(), e).encode(env));
            }
            if let Err(e) = crate::prob_fsync(&file) {
                return Ok((atoms::error(), e).encode(env));
            }
            crate::fadvise_dontneed(&file, 0, 0);
            return Ok((atoms::ok(), 1u64).encode(env));
        }
    }

    // Try alternate bucket.
    for slot in 0..hdr.bucket_size {
        let s = match cuckoo_file_read_slot(
            &file,
            b2,
            slot as usize,
            hdr.bucket_size,
            hdr.fingerprint_size,
        ) {
            Ok(s) => s,
            Err(e) => return Ok((atoms::error(), e).encode(env)),
        };
        if s.iter().all(|&b| b == 0) {
            if let Err(e) = cuckoo_file_write_slot(
                &file,
                b2,
                slot as usize,
                hdr.bucket_size,
                hdr.fingerprint_size,
                &fp,
            ) {
                return Ok((atoms::error(), e).encode(env));
            }
            if let Err(e) = cuckoo_file_write_num_items(&file, next_num_items) {
                return Ok((atoms::error(), e).encode(env));
            }
            if let Err(e) = crate::prob_fsync(&file) {
                return Ok((atoms::error(), e).encode(env));
            }
            crate::fadvise_dontneed(&file, 0, 0);
            return Ok((atoms::ok(), 1u64).encode(env));
        }
    }

    match cuckoo_file_try_eviction(&file, &hdr, fp, b1) {
        Ok(true) => {
            if let Err(e) = cuckoo_file_write_num_items(&file, next_num_items) {
                return Ok((atoms::error(), e).encode(env));
            }
            if let Err(e) = crate::prob_fsync(&file) {
                return Ok((atoms::error(), e).encode(env));
            }
            crate::fadvise_dontneed(&file, 0, 0);
            return Ok((atoms::ok(), 1u64).encode(env));
        }
        Ok(false) => {}
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    }

    crate::fadvise_dontneed(&file, 0, 0);
    Ok((atoms::error(), "filter is full").encode(env))
}

/// Add an element using a deterministic Raft mutation token.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn cuckoo_file_add_at<'a>(
    env: Env<'a>,
    path: String,
    receipt_path: String,
    element: Binary<'a>,
    mutation_index: u64,
    mutation_ordinal: u64,
) -> NifResult<Term<'a>> {
    match cuckoo_file_mutate_at(
        &path,
        &receipt_path,
        element.as_slice(),
        mutation_index,
        mutation_ordinal,
        CuckooMutation::Add,
    ) {
        Ok(result) => Ok((atoms::ok(), result).encode(env)),
        Err(error) => Ok(encode_file_open_error(env, error)),
    }
}

/// Add an element only if it does not already exist.
/// Returns `{:ok, 0}` (already present) or `{:ok, 1}` (added), or `{:error, reason}`.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(
    clippy::needless_pass_by_value,
    clippy::unnecessary_wraps,
    clippy::too_many_lines
)]
pub fn cuckoo_file_addnx<'a>(
    env: Env<'a>,
    path: String,
    element: Binary<'a>,
) -> NifResult<Term<'a>> {
    // Check existence first using the same file.
    let file = match cuckoo_file_open_rw(&path) {
        Ok(f) => f,
        Err(e) => {
            return Ok(encode_file_open_error(env, e));
        }
    };

    let hdr = match cuckoo_read_header(&file) {
        Ok(h) => h,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    let (fp, b1) = cuckoo_file_fingerprint_and_bucket(
        element.as_slice(),
        hdr.fingerprint_size as usize,
        hdr.num_buckets,
    );
    let b2 = cuckoo_file_alternate_bucket(b1, &fp, hdr.num_buckets);

    // Check if exists in either bucket.
    for bucket in cuckoo_file_candidate_buckets(b1, b2) {
        for slot in 0..hdr.bucket_size {
            let s = match cuckoo_file_read_slot(
                &file,
                bucket,
                slot as usize,
                hdr.bucket_size,
                hdr.fingerprint_size,
            ) {
                Ok(s) => s,
                Err(e) => return Ok((atoms::error(), e).encode(env)),
            };
            if s == fp {
                crate::fadvise_dontneed(&file, 0, 0);
                return Ok((atoms::ok(), 0u64).encode(env));
            }
        }
    }

    let next_num_items = match cuckoo_num_items_after_insert(hdr.num_items) {
        Ok(next) => next,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    // Not found, try to add. Try primary bucket.
    for slot in 0..hdr.bucket_size {
        let s = match cuckoo_file_read_slot(
            &file,
            b1,
            slot as usize,
            hdr.bucket_size,
            hdr.fingerprint_size,
        ) {
            Ok(s) => s,
            Err(e) => return Ok((atoms::error(), e).encode(env)),
        };
        if s.iter().all(|&b| b == 0) {
            if let Err(e) = cuckoo_file_write_slot(
                &file,
                b1,
                slot as usize,
                hdr.bucket_size,
                hdr.fingerprint_size,
                &fp,
            ) {
                return Ok((atoms::error(), e).encode(env));
            }
            if let Err(e) = cuckoo_file_write_num_items(&file, next_num_items) {
                return Ok((atoms::error(), e).encode(env));
            }
            if let Err(e) = crate::prob_fsync(&file) {
                return Ok((atoms::error(), e).encode(env));
            }
            crate::fadvise_dontneed(&file, 0, 0);
            return Ok((atoms::ok(), 1u64).encode(env));
        }
    }

    // Try alternate bucket.
    for slot in 0..hdr.bucket_size {
        let s = match cuckoo_file_read_slot(
            &file,
            b2,
            slot as usize,
            hdr.bucket_size,
            hdr.fingerprint_size,
        ) {
            Ok(s) => s,
            Err(e) => return Ok((atoms::error(), e).encode(env)),
        };
        if s.iter().all(|&b| b == 0) {
            if let Err(e) = cuckoo_file_write_slot(
                &file,
                b2,
                slot as usize,
                hdr.bucket_size,
                hdr.fingerprint_size,
                &fp,
            ) {
                return Ok((atoms::error(), e).encode(env));
            }
            if let Err(e) = cuckoo_file_write_num_items(&file, next_num_items) {
                return Ok((atoms::error(), e).encode(env));
            }
            if let Err(e) = crate::prob_fsync(&file) {
                return Ok((atoms::error(), e).encode(env));
            }
            crate::fadvise_dontneed(&file, 0, 0);
            return Ok((atoms::ok(), 1u64).encode(env));
        }
    }

    match cuckoo_file_try_eviction(&file, &hdr, fp, b1) {
        Ok(true) => {
            if let Err(e) = cuckoo_file_write_num_items(&file, next_num_items) {
                return Ok((atoms::error(), e).encode(env));
            }
            if let Err(e) = crate::prob_fsync(&file) {
                return Ok((atoms::error(), e).encode(env));
            }
            crate::fadvise_dontneed(&file, 0, 0);
            return Ok((atoms::ok(), 1u64).encode(env));
        }
        Ok(false) => {}
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    }

    crate::fadvise_dontneed(&file, 0, 0);
    Ok((atoms::error(), "filter is full").encode(env))
}

/// Add an element if absent using a deterministic Raft mutation token.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn cuckoo_file_addnx_at<'a>(
    env: Env<'a>,
    path: String,
    receipt_path: String,
    element: Binary<'a>,
    mutation_index: u64,
    mutation_ordinal: u64,
) -> NifResult<Term<'a>> {
    match cuckoo_file_mutate_at(
        &path,
        &receipt_path,
        element.as_slice(),
        mutation_index,
        mutation_ordinal,
        CuckooMutation::AddNx,
    ) {
        Ok(result) => Ok((atoms::ok(), result).encode(env)),
        Err(error) => Ok(encode_file_open_error(env, error)),
    }
}

/// Delete one occurrence of an element from a cuckoo filter file.
/// Returns `{:ok, 0}` (not found) or `{:ok, 1}` (deleted), or `{:error, reason}`.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn cuckoo_file_del<'a>(env: Env<'a>, path: String, element: Binary<'a>) -> NifResult<Term<'a>> {
    let file = match cuckoo_file_open_rw(&path) {
        Ok(f) => f,
        Err(e) => {
            return Ok(encode_file_open_error(env, e));
        }
    };

    let hdr = match cuckoo_read_header(&file) {
        Ok(h) => h,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    let (fp, b1) = cuckoo_file_fingerprint_and_bucket(
        element.as_slice(),
        hdr.fingerprint_size as usize,
        hdr.num_buckets,
    );
    let b2 = cuckoo_file_alternate_bucket(b1, &fp, hdr.num_buckets);
    let empty = vec![0u8; hdr.fingerprint_size as usize];

    // Try primary bucket first.
    for slot in 0..hdr.bucket_size {
        let s = match cuckoo_file_read_slot(
            &file,
            b1,
            slot as usize,
            hdr.bucket_size,
            hdr.fingerprint_size,
        ) {
            Ok(s) => s,
            Err(e) => return Ok((atoms::error(), e).encode(env)),
        };
        if s == fp {
            let next_num_items = match cuckoo_num_items_after_delete(hdr.num_items) {
                Ok(next) => next,
                Err(e) => return Ok((atoms::error(), e).encode(env)),
            };
            let next_num_deletes = match cuckoo_num_deletes_after_delete(hdr.num_deletes) {
                Ok(next) => next,
                Err(e) => return Ok((atoms::error(), e).encode(env)),
            };
            if let Err(e) = cuckoo_file_write_slot(
                &file,
                b1,
                slot as usize,
                hdr.bucket_size,
                hdr.fingerprint_size,
                &empty,
            ) {
                return Ok((atoms::error(), e).encode(env));
            }
            if let Err(e) = cuckoo_file_write_num_items(&file, next_num_items) {
                return Ok((atoms::error(), e).encode(env));
            }
            if let Err(e) = cuckoo_file_write_num_deletes(&file, next_num_deletes) {
                return Ok((atoms::error(), e).encode(env));
            }
            if let Err(e) = crate::prob_fsync(&file) {
                return Ok((atoms::error(), e).encode(env));
            }
            crate::fadvise_dontneed(&file, 0, 0);
            return Ok((atoms::ok(), 1u64).encode(env));
        }
    }

    // Try alternate bucket.
    for slot in 0..hdr.bucket_size {
        let s = match cuckoo_file_read_slot(
            &file,
            b2,
            slot as usize,
            hdr.bucket_size,
            hdr.fingerprint_size,
        ) {
            Ok(s) => s,
            Err(e) => return Ok((atoms::error(), e).encode(env)),
        };
        if s == fp {
            let next_num_items = match cuckoo_num_items_after_delete(hdr.num_items) {
                Ok(next) => next,
                Err(e) => return Ok((atoms::error(), e).encode(env)),
            };
            let next_num_deletes = match cuckoo_num_deletes_after_delete(hdr.num_deletes) {
                Ok(next) => next,
                Err(e) => return Ok((atoms::error(), e).encode(env)),
            };
            if let Err(e) = cuckoo_file_write_slot(
                &file,
                b2,
                slot as usize,
                hdr.bucket_size,
                hdr.fingerprint_size,
                &empty,
            ) {
                return Ok((atoms::error(), e).encode(env));
            }
            if let Err(e) = cuckoo_file_write_num_items(&file, next_num_items) {
                return Ok((atoms::error(), e).encode(env));
            }
            if let Err(e) = cuckoo_file_write_num_deletes(&file, next_num_deletes) {
                return Ok((atoms::error(), e).encode(env));
            }
            if let Err(e) = crate::prob_fsync(&file) {
                return Ok((atoms::error(), e).encode(env));
            }
            crate::fadvise_dontneed(&file, 0, 0);
            return Ok((atoms::ok(), 1u64).encode(env));
        }
    }

    crate::fadvise_dontneed(&file, 0, 0);
    Ok((atoms::ok(), 0u64).encode(env))
}

/// Delete an element using a deterministic Raft mutation token.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn cuckoo_file_del_at<'a>(
    env: Env<'a>,
    path: String,
    receipt_path: String,
    element: Binary<'a>,
    mutation_index: u64,
    mutation_ordinal: u64,
) -> NifResult<Term<'a>> {
    match cuckoo_file_mutate_at(
        &path,
        &receipt_path,
        element.as_slice(),
        mutation_index,
        mutation_ordinal,
        CuckooMutation::Delete,
    ) {
        Ok(result) => Ok((atoms::ok(), result).encode(env)),
        Err(error) => Ok(encode_file_open_error(env, error)),
    }
}

/// Check if an element may exist in a cuckoo filter file.
/// Returns `{:ok, 0}` or `{:ok, 1}`, or `{:error, reason}`.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn cuckoo_file_exists<'a>(
    env: Env<'a>,
    path: String,
    element: Binary<'a>,
) -> NifResult<Term<'a>> {
    let file = match cuckoo_file_open_read(&path) {
        Ok(f) => f,
        Err(e) => {
            return Ok(encode_file_open_error(env, e));
        }
    };

    let hdr = match cuckoo_read_header(&file) {
        Ok(h) => h,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    let result = match cuckoo_file_exists_in_open_file(&file, &hdr, element.as_slice()) {
        Ok(result) => result,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };
    crate::fadvise_dontneed(&file, 0, 0);
    Ok((atoms::ok(), result).encode(env))
}

/// Check if multiple elements may exist in a cuckoo filter file.
/// Returns `{:ok, [0|1, ...]}`, or `{:error, reason}`.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn cuckoo_file_mexists<'a>(
    env: Env<'a>,
    path: String,
    elements: Vec<Binary<'a>>,
) -> NifResult<Term<'a>> {
    let file = match cuckoo_file_open_read(&path) {
        Ok(f) => f,
        Err(e) => {
            return Ok(encode_file_open_error(env, e));
        }
    };

    let hdr = match cuckoo_read_header(&file) {
        Ok(h) => h,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    let mut results = Vec::with_capacity(elements.len());
    for element in elements {
        match cuckoo_file_exists_in_open_file(&file, &hdr, element.as_slice()) {
            Ok(result) => results.push(result),
            Err(e) => return Ok((atoms::error(), e).encode(env)),
        }
    }

    crate::fadvise_dontneed(&file, 0, 0);
    Ok((atoms::ok(), results).encode(env))
}

/// Count occurrences of an element's fingerprint in a cuckoo filter file.
/// Returns `{:ok, count}` or `{:error, reason}`.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn cuckoo_file_count<'a>(
    env: Env<'a>,
    path: String,
    element: Binary<'a>,
) -> NifResult<Term<'a>> {
    let file = match cuckoo_file_open_read(&path) {
        Ok(f) => f,
        Err(e) => {
            return Ok(encode_file_open_error(env, e));
        }
    };

    let hdr = match cuckoo_read_header(&file) {
        Ok(h) => h,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    let (fp, b1) = cuckoo_file_fingerprint_and_bucket(
        element.as_slice(),
        hdr.fingerprint_size as usize,
        hdr.num_buckets,
    );
    let b2 = cuckoo_file_alternate_bucket(b1, &fp, hdr.num_buckets);

    let mut total = 0u64;
    for bucket in cuckoo_file_candidate_buckets(b1, b2) {
        for slot in 0..hdr.bucket_size {
            let s = match cuckoo_file_read_slot(
                &file,
                bucket,
                slot as usize,
                hdr.bucket_size,
                hdr.fingerprint_size,
            ) {
                Ok(s) => s,
                Err(e) => return Ok((atoms::error(), e).encode(env)),
            };
            if s == fp {
                total += 1;
            }
        }
    }

    crate::fadvise_dontneed(&file, 0, 0);
    Ok((atoms::ok(), total).encode(env))
}

/// Read cuckoo filter file info/metadata.
/// Returns `{:ok, {num_buckets, bucket_size, fingerprint_size, num_items, num_deletes, total_slots, max_kicks}}`
/// or `{:error, reason}`.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value, clippy::unnecessary_wraps)]
pub fn cuckoo_file_info(env: Env, path: String) -> NifResult<Term> {
    let file = match cuckoo_file_open_read(&path) {
        Ok(f) => f,
        Err(e) => {
            return Ok(encode_file_open_error(env, e));
        }
    };

    let hdr = match cuckoo_read_header(&file) {
        Ok(h) => h,
        Err(e) => return Ok((atoms::error(), e).encode(env)),
    };

    let total_slots = (hdr.num_buckets as u64) * (hdr.bucket_size as u64);
    let info = (
        atoms::ok(),
        (
            hdr.num_buckets as u64,
            hdr.bucket_size as u64,
            hdr.fingerprint_size as u64,
            hdr.num_items,
            hdr.num_deletes,
            total_slots,
            hdr.max_kicks as u64,
        ),
    );
    crate::fadvise_dontneed(&file, 0, 0);
    Ok(info.encode(env))
}

// ---------------------------------------------------------------------------
// Async variants of read NIFs — Tokio spawn_blocking, never block BEAM
// ---------------------------------------------------------------------------

/// Async cuckoo exists: spawns on Tokio, sends result to `caller_pid`.
#[rustler::nif(schedule = "DirtyCpu")]
#[allow(clippy::needless_pass_by_value)]
pub fn cuckoo_file_exists_async<'a>(
    env: Env<'a>,
    caller_pid: LocalPid,
    correlation_id: u64,
    path: String,
    element: Binary<'a>,
) -> NifResult<Term<'a>> {
    let element_owned = element.as_slice().to_vec();
    let blocking_task = match crate::async_io::try_spawn_blocking(move || {
        let file = crate::open_random_read_locked(std::path::Path::new(&path)).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                "enoent".to_string()
            } else {
                e.to_string()
            }
        })?;
        let hdr = cuckoo_read_header(&file).map_err(|e| e.clone())?;
        let result = cuckoo_file_exists_in_open_file(&file, &hdr, &element_owned)?;
        crate::fadvise_dontneed(&file, 0, 0);
        Ok(result)
    }) {
        Ok(task) => task,
        Err(reason) => return Ok((atoms::error(), reason).encode(env)),
    };

    crate::async_io::runtime().spawn(async move {
        let result = blocking_task
            .await
            .unwrap_or_else(|e| Err(format!("spawn_blocking: {e}")));

        let mut msg_env = rustler::OwnedEnv::new();
        let _ = msg_env.send_and_clear(&caller_pid, |env| match result {
            Ok(val) => (atoms::tokio_complete(), correlation_id, atoms::ok(), val).encode(env),
            Err(reason) => (
                atoms::tokio_complete(),
                correlation_id,
                atoms::error(),
                reason,
            )
                .encode(env),
        });
    });
    Ok(atoms::ok().encode(env))
}

/// Async cuckoo mexists: one Tokio task and one waiter for the whole batch.
#[rustler::nif(schedule = "DirtyCpu")]
#[allow(clippy::needless_pass_by_value)]
pub fn cuckoo_file_mexists_async<'a>(
    env: Env<'a>,
    caller_pid: LocalPid,
    correlation_id: u64,
    path: String,
    elements: Vec<Binary<'a>>,
) -> NifResult<Term<'a>> {
    let elements_owned: Vec<Vec<u8>> = elements
        .iter()
        .map(|element| element.as_slice().to_vec())
        .collect();
    let blocking_task = match crate::async_io::try_spawn_blocking(move || {
        let file = crate::open_random_read_locked(std::path::Path::new(&path)).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                "enoent".to_string()
            } else {
                e.to_string()
            }
        })?;
        let hdr = cuckoo_read_header(&file)?;
        let mut results = Vec::with_capacity(elements_owned.len());
        for element in elements_owned {
            results.push(cuckoo_file_exists_in_open_file(&file, &hdr, &element)?);
        }
        crate::fadvise_dontneed(&file, 0, 0);
        Ok(results)
    }) {
        Ok(task) => task,
        Err(reason) => return Ok((atoms::error(), reason).encode(env)),
    };

    crate::async_io::runtime().spawn(async move {
        let result = blocking_task
            .await
            .unwrap_or_else(|e| Err(format!("spawn_blocking: {e}")));

        let mut msg_env = rustler::OwnedEnv::new();
        let _ = msg_env.send_and_clear(&caller_pid, |env| match result {
            Ok(val) => (atoms::tokio_complete(), correlation_id, atoms::ok(), val).encode(env),
            Err(reason) => (
                atoms::tokio_complete(),
                correlation_id,
                atoms::error(),
                reason,
            )
                .encode(env),
        });
    });
    Ok(atoms::ok().encode(env))
}

/// Async cuckoo count: spawns on Tokio, sends result to `caller_pid`.
#[rustler::nif(schedule = "DirtyCpu")]
#[allow(clippy::needless_pass_by_value)]
pub fn cuckoo_file_count_async<'a>(
    env: Env<'a>,
    caller_pid: LocalPid,
    correlation_id: u64,
    path: String,
    element: Binary<'a>,
) -> NifResult<Term<'a>> {
    let element_owned = element.as_slice().to_vec();
    let blocking_task = match crate::async_io::try_spawn_blocking(move || {
        let file = crate::open_random_read_locked(std::path::Path::new(&path)).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                "enoent".to_string()
            } else {
                e.to_string()
            }
        })?;
        let hdr = cuckoo_read_header(&file).map_err(|e| e.clone())?;
        let (fp, b1) = cuckoo_file_fingerprint_and_bucket(
            &element_owned,
            hdr.fingerprint_size as usize,
            hdr.num_buckets,
        );
        let b2 = cuckoo_file_alternate_bucket(b1, &fp, hdr.num_buckets);
        let mut total = 0u64;
        for bucket in cuckoo_file_candidate_buckets(b1, b2) {
            for slot in 0..hdr.bucket_size {
                let s = cuckoo_file_read_slot(
                    &file,
                    bucket,
                    slot as usize,
                    hdr.bucket_size,
                    hdr.fingerprint_size,
                )
                .map_err(|e| e.clone())?;
                if s == fp {
                    total += 1;
                }
            }
        }
        crate::fadvise_dontneed(&file, 0, 0);
        Ok(total)
    }) {
        Ok(task) => task,
        Err(reason) => return Ok((atoms::error(), reason).encode(env)),
    };

    crate::async_io::runtime().spawn(async move {
        let result = blocking_task
            .await
            .unwrap_or_else(|e| Err(format!("spawn_blocking: {e}")));

        let mut msg_env = rustler::OwnedEnv::new();
        let _ = msg_env.send_and_clear(&caller_pid, |env| match result {
            Ok(count) => (atoms::tokio_complete(), correlation_id, atoms::ok(), count).encode(env),
            Err(reason) => (
                atoms::tokio_complete(),
                correlation_id,
                atoms::error(),
                reason,
            )
                .encode(env),
        });
    });
    Ok(atoms::ok().encode(env))
}

/// Async cuckoo info: spawns on Tokio, sends result to `caller_pid`.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value)]
pub fn cuckoo_file_info_async(
    env: Env<'_>,
    caller_pid: LocalPid,
    correlation_id: u64,
    path: String,
) -> NifResult<Term<'_>> {
    let blocking_task = match crate::async_io::try_spawn_blocking(move || {
        let file = crate::open_random_read_locked(std::path::Path::new(&path)).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                "enoent".to_string()
            } else {
                e.to_string()
            }
        })?;
        let hdr = cuckoo_read_header(&file).map_err(|e| e.clone())?;
        let total_slots = (hdr.num_buckets as u64) * (hdr.bucket_size as u64);
        crate::fadvise_dontneed(&file, 0, 0);
        Ok((
            hdr.num_buckets as u64,
            hdr.bucket_size as u64,
            hdr.fingerprint_size as u64,
            hdr.num_items,
            hdr.num_deletes,
            total_slots,
            hdr.max_kicks as u64,
        ))
    }) {
        Ok(task) => task,
        Err(reason) => return Ok((atoms::error(), reason).encode(env)),
    };

    crate::async_io::runtime().spawn(async move {
        let result = blocking_task
            .await
            .unwrap_or_else(|e| Err(format!("spawn_blocking: {e}")));

        let mut msg_env = rustler::OwnedEnv::new();
        let _ = msg_env.send_and_clear(&caller_pid, |env| match result {
            Ok(info) => (atoms::tokio_complete(), correlation_id, atoms::ok(), info).encode(env),
            Err(reason) => (
                atoms::tokio_complete(),
                correlation_id,
                atoms::error(),
                reason,
            )
                .encode(env),
        });
    });
    Ok(atoms::ok().encode(env))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------
