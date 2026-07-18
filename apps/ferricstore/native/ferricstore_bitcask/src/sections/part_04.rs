/// Append a batch of records **without** fsync. The data is written to the OS
/// page cache (~1-10us) but not forced to durable storage. The caller must
/// call `v2_fsync` or `v2_fsync_async` later to guarantee durability.
///
/// Returns `{:ok, [{offset, value_size}, ...]}` or `{:error, reason}`.
///
/// ## Scheduler contract
///
/// Runs on a dirty I/O scheduler because page-cache writes may still block
/// under filesystem or memory pressure.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
fn v2_append_batch_nosync<'a>(
    env: Env<'a>,
    path: String,
    records: Vec<(Binary<'a>, Binary<'a>, u64)>,
) -> NifResult<Term<'a>> {
    let p = std::path::Path::new(&path);
    let file_id = parse_file_id(p);

    match log::LogWriter::open_small(p, file_id) {
        Ok(mut writer) => {
            let entries: Vec<(&[u8], &[u8], u64)> = records
                .iter()
                .map(|(k, v, exp)| (k.as_slice(), v.as_slice(), *exp))
                .collect();

            match writer.write_batch_nosync(&entries) {
                Ok(results) => {
                    let tuples: Vec<(u64, usize)> = results;
                    Ok((atoms::ok(), tuples).encode(env))
                }
                Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
            }
        }
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

/// Append a mixed batch of put and delete records **without** fsync.
/// Returns `{:ok, [{:put, offset, value_size} | {:delete, offset, record_size}, ...]}`.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
fn v2_append_ops_batch_nosync<'a>(
    env: Env<'a>,
    path: String,
    records: Vec<NifBatchWrite<'a>>,
) -> NifResult<Term<'a>> {
    let p = std::path::Path::new(&path);
    let file_id = parse_file_id(p);

    match log::LogWriter::open_small(p, file_id) {
        Ok(mut writer) => {
            let entries: Vec<log::BatchWrite<'_>> = records
                .iter()
                .map(|record| match record {
                    NifBatchWrite::Put(key, value, expire_at_ms) => log::BatchWrite::Put {
                        key: key.as_slice(),
                        value: value.as_slice(),
                        expire_at_ms: *expire_at_ms,
                    },
                    NifBatchWrite::Delete(key) => log::BatchWrite::Delete {
                        key: key.as_slice(),
                    },
                })
                .collect();

            match writer.write_ops_batch_nosync(&entries) {
                Ok(results) => {
                    let tuples: Vec<Term<'a>> = results
                        .into_iter()
                        .map(|result| match result {
                            log::BatchWriteResult::Put { offset, value_len } => {
                                (atoms::put(), offset, value_len).encode(env)
                            }
                            log::BatchWriteResult::Delete {
                                offset,
                                record_size,
                            } => (atoms::delete(), offset, record_size).encode(env),
                        })
                        .collect();

                    Ok((atoms::ok(), tuples).encode(env))
                }
                Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
            }
        }
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

/// Append a mixed batch of put and delete records and fsync it under the same
/// per-file append lock. On any append or fsync error the writer restores the
/// original file length before returning an error.
/// Returns `{:ok, [{:put, offset, value_size} | {:delete, offset, record_size}, ...]}`.
#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
fn v2_append_ops_batch<'a>(
    env: Env<'a>,
    path: String,
    records: Vec<NifBatchWrite<'a>>,
) -> NifResult<Term<'a>> {
    let p = std::path::Path::new(&path);
    let file_id = parse_file_id(p);

    match log::LogWriter::open(p, file_id) {
        Ok(mut writer) => {
            let entries: Vec<log::BatchWrite<'_>> = records
                .iter()
                .map(|record| match record {
                    NifBatchWrite::Put(key, value, expire_at_ms) => log::BatchWrite::Put {
                        key: key.as_slice(),
                        value: value.as_slice(),
                        expire_at_ms: *expire_at_ms,
                    },
                    NifBatchWrite::Delete(key) => log::BatchWrite::Delete {
                        key: key.as_slice(),
                    },
                })
                .collect();

            match writer.write_ops_batch(&entries) {
                Ok(results) => {
                    let tuples: Vec<Term<'a>> = results
                        .into_iter()
                        .map(|result| match result {
                            log::BatchWriteResult::Put { offset, value_len } => {
                                (atoms::put(), offset, value_len).encode(env)
                            }
                            log::BatchWriteResult::Delete {
                                offset,
                                record_size,
                            } => (atoms::delete(), offset, record_size).encode(env),
                        })
                        .collect();

                    Ok((atoms::ok(), tuples).encode(env))
                }
                Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
            }
        }
        Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
    }
}

fn lmdb_store(path: &str, map_size: u64) -> Result<Arc<LmdbStore>, String> {
    crate::fs_nif::create_dir_all_nofollow(std::path::Path::new(path))
        .map_err(|e| e.to_string())?;
    let cache_key = std::fs::canonicalize(path)
        .map_err(|e| e.to_string())?
        .to_string_lossy()
        .into_owned();
    let map_size = usize::try_from(map_size).map_err(|_| "lmdb map_size too large".to_string())?;
    let stores = LMDB_STORES.get_or_init(|| Mutex::new(std::collections::HashMap::new()));
    let cell = {
        let mut guard = stores
            .lock()
            .map_err(|_| "lmdb cache poisoned".to_string())?;
        Arc::clone(
            guard
                .entry(cache_key.clone())
                .or_insert_with(|| Arc::new(OnceLock::new())),
        )
    };

    let initialized = cell.get_or_init(|| initialize_lmdb_store(&cache_key, map_size));
    match initialized {
        Ok(store) if store.map_size == map_size => Ok(Arc::clone(store)),
        Ok(store) => Err(format!(
            "lmdb map_size mismatch for {cache_key}: cached={}, requested={map_size}; release the environment before changing map_size",
            store.map_size
        )),
        Err(error) => {
            if let Ok(mut guard) = stores.lock() {
                if guard
                    .get(&cache_key)
                    .is_some_and(|cached| Arc::ptr_eq(cached, &cell))
                {
                    guard.remove(&cache_key);
                }
            }
            Err(error.clone())
        }
    }
}

fn initialize_lmdb_store(cache_key: &str, map_size: usize) -> Result<Arc<LmdbStore>, String> {
    let mut env_options = heed::EnvOpenOptions::new();
    env_options.map_size(map_size).max_dbs(4);
    unsafe {
        env_options.flags(heed::EnvFlags::NO_READ_AHEAD);
    }

    let env = unsafe { env_options.open(cache_key).map_err(|e| e.to_string())? };
    let mut wtxn = env.write_txn().map_err(|e| e.to_string())?;
    let db = env
        .create_database::<heed::types::Bytes, heed::types::Bytes>(&mut wtxn, Some("flow_state"))
        .map_err(|e| e.to_string())?;
    wtxn.commit().map_err(|e| e.to_string())?;

    Ok(Arc::new(LmdbStore { env, db, map_size }))
}

fn lmdb_store_cell_busy(cell: &Arc<LmdbStoreCell>) -> bool {
    Arc::strong_count(cell) > 1
        || cell
            .get()
            .and_then(|result| result.as_ref().ok())
            .is_some_and(|store| Arc::strong_count(store) > 1)
}

#[derive(Debug, Eq, PartialEq)]
enum LmdbCacheRelease {
    Busy(usize),
    Released(usize),
}

fn release_lmdb_cache_entry(
    stores: &mut std::collections::HashMap<String, Arc<LmdbStoreCell>>,
    cache_key: &str,
) -> LmdbCacheRelease {
    if stores.get(cache_key).is_some_and(lmdb_store_cell_busy) {
        LmdbCacheRelease::Busy(1)
    } else {
        LmdbCacheRelease::Released(usize::from(stores.remove(cache_key).is_some()))
    }
}

#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
fn lmdb_get<'a>(env: Env<'a>, path: String, key: Binary<'a>, map_size: u64) -> NifResult<Term<'a>> {
    match lmdb_store(&path, map_size) {
        Ok(store) => {
            let rtxn = match store.env.read_txn() {
                Ok(txn) => txn,
                Err(e) => return Ok((atoms::error(), e.to_string()).encode(env)),
            };

            match store.db.get(&rtxn, key.as_slice()) {
                Ok(Some(value)) => {
                    let mut binary = OwnedBinary::new(value.len()).ok_or_else(|| {
                        rustler::Error::Term(Box::new("failed to allocate binary"))
                    })?;
                    binary.as_mut_slice().copy_from_slice(value);
                    Ok((atoms::ok(), binary.release(env)).encode(env))
                }
                Ok(None) => Ok(atoms::not_found().encode(env)),
                Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
            }
        }
        Err(e) => Ok((atoms::error(), e).encode(env)),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
fn lmdb_get_many<'a>(
    env: Env<'a>,
    path: String,
    keys: Vec<Binary<'a>>,
    map_size: u64,
) -> NifResult<Term<'a>> {
    match lmdb_store(&path, map_size) {
        Ok(store) => {
            let rtxn = match store.env.read_txn() {
                Ok(txn) => txn,
                Err(e) => return Ok((atoms::error(), e.to_string()).encode(env)),
            };

            let mut results = Vec::with_capacity(keys.len());

            for key in keys {
                match store.db.get(&rtxn, key.as_slice()) {
                    Ok(Some(value)) => {
                        let mut binary = OwnedBinary::new(value.len()).ok_or_else(|| {
                            rustler::Error::Term(Box::new("failed to allocate binary"))
                        })?;
                        binary.as_mut_slice().copy_from_slice(value);
                        results.push((atoms::ok(), binary.release(env)).encode(env));
                    }
                    Ok(None) => results.push(atoms::not_found().encode(env)),
                    Err(e) => return Ok((atoms::error(), e.to_string()).encode(env)),
                }
            }

            Ok((atoms::ok(), results).encode(env))
        }
        Err(e) => Ok((atoms::error(), e).encode(env)),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
fn lmdb_put<'a>(
    env: Env<'a>,
    path: String,
    key: Binary<'a>,
    value: Binary<'a>,
    map_size: u64,
) -> NifResult<Term<'a>> {
    lmdb_write_batch_impl(
        env,
        path,
        vec![LmdbBatchWrite::Put(key, value)],
        map_size,
        false,
    )
}

#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
fn lmdb_delete<'a>(
    env: Env<'a>,
    path: String,
    key: Binary<'a>,
    map_size: u64,
) -> NifResult<Term<'a>> {
    lmdb_write_batch_impl(
        env,
        path,
        vec![LmdbBatchWrite::Delete(key)],
        map_size,
        false,
    )
}

#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
fn lmdb_write_batch<'a>(
    env: Env<'a>,
    path: String,
    records: Vec<LmdbBatchWrite<'a>>,
    map_size: u64,
) -> NifResult<Term<'a>> {
    lmdb_write_batch_impl(env, path, records, map_size, false)
}

#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
fn lmdb_write_batch_with_originals<'a>(
    env: Env<'a>,
    path: String,
    records: Vec<LmdbBatchWrite<'a>>,
    map_size: u64,
) -> NifResult<Term<'a>> {
    lmdb_write_batch_impl(env, path, records, map_size, true)
}

#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
fn lmdb_clear<'a>(env: Env<'a>, path: String, map_size: u64) -> NifResult<Term<'a>> {
    match lmdb_store(&path, map_size) {
        Ok(store) => {
            let mut wtxn = match store.env.write_txn() {
                Ok(txn) => txn,
                Err(e) => return Ok((atoms::error(), e.to_string()).encode(env)),
            };

            if let Err(e) = store.db.clear(&mut wtxn) {
                return Ok((atoms::error(), e.to_string()).encode(env));
            }

            match wtxn.commit() {
                Ok(()) => Ok(atoms::ok().encode(env)),
                Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
            }
        }
        Err(e) => Ok((atoms::error(), e).encode(env)),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
fn lmdb_prefix_entries<'a>(
    env: Env<'a>,
    path: String,
    prefix: Binary<'a>,
    limit: u64,
    map_size: u64,
) -> NifResult<Term<'a>> {
    match lmdb_store(&path, map_size) {
        Ok(store) => {
            let rtxn = match store.env.read_txn() {
                Ok(txn) => txn,
                Err(e) => return Ok((atoms::error(), e.to_string()).encode(env)),
            };

            let iter = match store.db.prefix_iter(&rtxn, prefix.as_slice()) {
                Ok(iter) => iter,
                Err(e) => return Ok((atoms::error(), e.to_string()).encode(env)),
            };

            let max = usize::try_from(limit).unwrap_or(usize::MAX);
            let mut entries = Vec::new();

            for item in iter.take(max) {
                match item {
                    Ok((key, value)) => {
                        let key_term = binary_term(env, key)?;
                        let value_term = binary_term(env, value)?;
                        entries.push((key_term, value_term).encode(env));
                    }
                    Err(e) => return Ok((atoms::error(), e.to_string()).encode(env)),
                }
            }

            Ok((atoms::ok(), entries).encode(env))
        }
        Err(e) => Ok((atoms::error(), e).encode(env)),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
fn lmdb_prefix_entries_after<'a>(
    env: Env<'a>,
    path: String,
    prefix: Binary<'a>,
    after_key: Binary<'a>,
    limit: u64,
    map_size: u64,
) -> NifResult<Term<'a>> {
    match lmdb_store(&path, map_size) {
        Ok(store) => {
            let rtxn = match store.env.read_txn() {
                Ok(txn) => txn,
                Err(e) => return Ok((atoms::error(), e.to_string()).encode(env)),
            };

            let max = usize::try_from(limit).unwrap_or(usize::MAX);
            let mut entries = Vec::new();

            if after_key.as_slice().is_empty() {
                let iter = match store.db.prefix_iter(&rtxn, prefix.as_slice()) {
                    Ok(iter) => iter,
                    Err(e) => return Ok((atoms::error(), e.to_string()).encode(env)),
                };

                for item in iter.take(max) {
                    match item {
                        Ok((key, value)) => {
                            let key_term = binary_term(env, key)?;
                            let value_term = binary_term(env, value)?;
                            entries.push((key_term, value_term).encode(env));
                        }
                        Err(e) => return Ok((atoms::error(), e.to_string()).encode(env)),
                    }
                }
            } else {
                let range = (
                    std::ops::Bound::Excluded(after_key.as_slice()),
                    std::ops::Bound::Unbounded,
                );
                let iter = match store.db.range(&rtxn, &range) {
                    Ok(iter) => iter,
                    Err(e) => return Ok((atoms::error(), e.to_string()).encode(env)),
                };

                for item in iter {
                    if entries.len() >= max {
                        break;
                    }

                    match item {
                        Ok((key, value)) => {
                            if !key.starts_with(prefix.as_slice()) {
                                break;
                            }

                            let key_term = binary_term(env, key)?;
                            let value_term = binary_term(env, value)?;
                            entries.push((key_term, value_term).encode(env));
                        }
                        Err(e) => return Ok((atoms::error(), e.to_string()).encode(env)),
                    }
                }
            }

            Ok((atoms::ok(), entries).encode(env))
        }
        Err(e) => Ok((atoms::error(), e).encode(env)),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
fn lmdb_prefix_entries_after_bounded<'a>(
    env: Env<'a>,
    path: String,
    prefix: Binary<'a>,
    after_key: Binary<'a>,
    max_items: u64,
    max_bytes: u64,
    map_size: u64,
) -> NifResult<Term<'a>> {
    match lmdb_store(&path, map_size) {
        Ok(store) => {
            let rtxn = match store.env.read_txn() {
                Ok(txn) => txn,
                Err(e) => return Ok((atoms::error(), e.to_string()).encode(env)),
            };

            let item_cap = usize::try_from(max_items).unwrap_or(usize::MAX);
            let byte_cap = usize::try_from(max_bytes).unwrap_or(usize::MAX);
            let mut entries = Vec::new();
            let mut entry_bytes = 0usize;

            if item_cap == 0 {
                return Ok((atoms::ok(), entries).encode(env));
            }

            if after_key.as_slice().is_empty() {
                let iter = match store.db.prefix_iter(&rtxn, prefix.as_slice()) {
                    Ok(iter) => iter,
                    Err(e) => return Ok((atoms::error(), e.to_string()).encode(env)),
                };

                for item in iter {
                    if entries.len() >= item_cap {
                        break;
                    }

                    match item {
                        Ok((key, value)) => {
                            let row_bytes = key.len().saturating_add(value.len());
                            let next_bytes = entry_bytes.saturating_add(row_bytes);

                            if !entries.is_empty() && next_bytes > byte_cap {
                                break;
                            }

                            let key_term = binary_term(env, key)?;
                            let value_term = binary_term(env, value)?;
                            entries.push((key_term, value_term).encode(env));
                            entry_bytes = next_bytes;

                            if entry_bytes > byte_cap {
                                break;
                            }
                        }
                        Err(e) => return Ok((atoms::error(), e.to_string()).encode(env)),
                    }
                }
            } else {
                let range = (
                    std::ops::Bound::Excluded(after_key.as_slice()),
                    std::ops::Bound::Unbounded,
                );
                let iter = match store.db.range(&rtxn, &range) {
                    Ok(iter) => iter,
                    Err(e) => return Ok((atoms::error(), e.to_string()).encode(env)),
                };

                for item in iter {
                    if entries.len() >= item_cap {
                        break;
                    }

                    match item {
                        Ok((key, value)) => {
                            if !key.starts_with(prefix.as_slice()) {
                                break;
                            }

                            let row_bytes = key.len().saturating_add(value.len());
                            let next_bytes = entry_bytes.saturating_add(row_bytes);

                            if !entries.is_empty() && next_bytes > byte_cap {
                                break;
                            }

                            let key_term = binary_term(env, key)?;
                            let value_term = binary_term(env, value)?;
                            entries.push((key_term, value_term).encode(env));
                            entry_bytes = next_bytes;

                            if entry_bytes > byte_cap {
                                break;
                            }
                        }
                        Err(e) => return Ok((atoms::error(), e.to_string()).encode(env)),
                    }
                }
            }

            Ok((atoms::ok(), entries).encode(env))
        }
        Err(e) => Ok((atoms::error(), e).encode(env)),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
fn lmdb_prefix_entries_reverse<'a>(
    env: Env<'a>,
    path: String,
    prefix: Binary<'a>,
    limit: u64,
    map_size: u64,
) -> NifResult<Term<'a>> {
    match lmdb_store(&path, map_size) {
        Ok(store) => {
            let rtxn = match store.env.read_txn() {
                Ok(txn) => txn,
                Err(e) => return Ok((atoms::error(), e.to_string()).encode(env)),
            };

            let iter = match store.db.rev_prefix_iter(&rtxn, prefix.as_slice()) {
                Ok(iter) => iter,
                Err(e) => return Ok((atoms::error(), e.to_string()).encode(env)),
            };

            let max = usize::try_from(limit).unwrap_or(usize::MAX);
            let mut entries = Vec::new();

            for item in iter.take(max) {
                match item {
                    Ok((key, value)) => {
                        let key_term = binary_term(env, key)?;
                        let value_term = binary_term(env, value)?;
                        entries.push((key_term, value_term).encode(env));
                    }
                    Err(e) => return Ok((atoms::error(), e.to_string()).encode(env)),
                }
            }

            Ok((atoms::ok(), entries).encode(env))
        }
        Err(e) => Ok((atoms::error(), e).encode(env)),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
fn lmdb_prefix_entries_reverse_before<'a>(
    env: Env<'a>,
    path: String,
    prefix: Binary<'a>,
    before_key: Binary<'a>,
    limit: u64,
    map_size: u64,
) -> NifResult<Term<'a>> {
    match lmdb_store(&path, map_size) {
        Ok(store) => {
            let rtxn = match store.env.read_txn() {
                Ok(txn) => txn,
                Err(e) => return Ok((atoms::error(), e.to_string()).encode(env)),
            };

            let max = usize::try_from(limit).unwrap_or(usize::MAX);
            let mut entries = Vec::new();

            if before_key.as_slice().is_empty() {
                let iter = match store.db.rev_prefix_iter(&rtxn, prefix.as_slice()) {
                    Ok(iter) => iter,
                    Err(e) => return Ok((atoms::error(), e.to_string()).encode(env)),
                };

                for item in iter.take(max) {
                    match item {
                        Ok((key, value)) => {
                            let key_term = binary_term(env, key)?;
                            let value_term = binary_term(env, value)?;
                            entries.push((key_term, value_term).encode(env));
                        }
                        Err(e) => return Ok((atoms::error(), e.to_string()).encode(env)),
                    }
                }
            } else {
                let range = (
                    std::ops::Bound::Unbounded,
                    std::ops::Bound::Excluded(before_key.as_slice()),
                );
                let iter = match store.db.rev_range(&rtxn, &range) {
                    Ok(iter) => iter,
                    Err(e) => return Ok((atoms::error(), e.to_string()).encode(env)),
                };

                for item in iter {
                    if entries.len() >= max {
                        break;
                    }

                    match item {
                        Ok((key, value)) => {
                            if !key.starts_with(prefix.as_slice()) {
                                break;
                            }

                            let key_term = binary_term(env, key)?;
                            let value_term = binary_term(env, value)?;
                            entries.push((key_term, value_term).encode(env));
                        }
                        Err(e) => return Ok((atoms::error(), e.to_string()).encode(env)),
                    }
                }
            }

            Ok((atoms::ok(), entries).encode(env))
        }
        Err(e) => Ok((atoms::error(), e).encode(env)),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
fn lmdb_prefix_count<'a>(
    env: Env<'a>,
    path: String,
    prefix: Binary<'a>,
    map_size: u64,
) -> NifResult<Term<'a>> {
    match lmdb_store(&path, map_size) {
        Ok(store) => {
            let rtxn = match store.env.read_txn() {
                Ok(txn) => txn,
                Err(e) => return Ok((atoms::error(), e.to_string()).encode(env)),
            };

            let iter = match store.db.prefix_iter(&rtxn, prefix.as_slice()) {
                Ok(iter) => iter,
                Err(e) => return Ok((atoms::error(), e.to_string()).encode(env)),
            };

            let mut count = 0_u64;

            for item in iter {
                if let Err(e) = item {
                    return Ok((atoms::error(), e.to_string()).encode(env));
                }

                count += 1;
            }

            Ok((atoms::ok(), count).encode(env))
        }
        Err(e) => Ok((atoms::error(), e).encode(env)),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn lmdb_release_all<'a>(env: Env<'a>) -> NifResult<Term<'a>> {
    let stores = match LMDB_STORES.get() {
        Some(stores) => stores,
        None => return Ok((atoms::ok(), 0usize).encode(env)),
    };

    let mut guard = match stores.lock() {
        Ok(guard) => guard,
        Err(_) => return Ok((atoms::error(), "lmdb cache poisoned").encode(env)),
    };

    let busy = guard
        .values()
        .filter(|cell| lmdb_store_cell_busy(cell))
        .count();

    if busy > 0 {
        return Ok((atoms::busy(), busy).encode(env));
    }

    let released = guard.len();
    guard.clear();
    Ok((atoms::ok(), released).encode(env))
}

#[rustler::nif(schedule = "DirtyIo")]
fn lmdb_release<'a>(env: Env<'a>, path: String) -> NifResult<Term<'a>> {
    let stores = match LMDB_STORES.get() {
        Some(stores) => stores,
        None => return Ok((atoms::ok(), 0usize).encode(env)),
    };

    let cache_key = match std::fs::canonicalize(&path) {
        Ok(path) => path.to_string_lossy().into_owned(),
        Err(error) => return Ok((atoms::error(), error.to_string()).encode(env)),
    };

    let mut guard = match stores.lock() {
        Ok(guard) => guard,
        Err(_) => return Ok((atoms::error(), "lmdb cache poisoned").encode(env)),
    };

    match release_lmdb_cache_entry(&mut guard, &cache_key) {
        LmdbCacheRelease::Busy(count) => Ok((atoms::busy(), count).encode(env)),
        LmdbCacheRelease::Released(count) => Ok((atoms::ok(), count).encode(env)),
    }
}

fn lmdb_write_batch_impl<'a>(
    env: Env<'a>,
    path: String,
    mut records: Vec<LmdbBatchWrite<'a>>,
    map_size: u64,
    return_originals: bool,
) -> NifResult<Term<'a>> {
    if records.len() > 1 && lmdb_record_keys_are_unique(&records) {
        records.sort_unstable_by(|left, right| lmdb_record_key(left).cmp(lmdb_record_key(right)));
    }

    match lmdb_store(&path, map_size) {
        Ok(store) => {
            let mut wtxn = match store.env.write_txn() {
                Ok(txn) => txn,
                Err(e) => return Ok((atoms::error(), e.to_string()).encode(env)),
            };

            let mut seen = std::collections::HashSet::new();
            let mut originals: Vec<(Vec<u8>, Option<Vec<u8>>)> = Vec::new();

            for record in &records {
                let key = match record {
                    LmdbBatchWrite::Put(key, _)
                    | LmdbBatchWrite::PutNew(key, _)
                    | LmdbBatchWrite::Delete(key)
                    | LmdbBatchWrite::Compare(key, _)
                    | LmdbBatchWrite::CompareMissing(key) => key.as_slice(),
                };

                if return_originals && seen.insert(key.to_vec()) {
                    match store.db.get(&wtxn, key) {
                        Ok(Some(value)) => {
                            originals.push((key.to_vec(), Some(value.to_vec())));
                        }
                        Ok(None) => originals.push((key.to_vec(), None)),
                        Err(e) => return Ok((atoms::error(), e.to_string()).encode(env)),
                    }
                }

                let result = match record {
                    LmdbBatchWrite::Put(key, value) => {
                        store.db.put(&mut wtxn, key.as_slice(), value.as_slice())
                    }
                    LmdbBatchWrite::PutNew(key, value) => match store.db.get(&wtxn, key.as_slice())
                    {
                        Ok(Some(_)) => Ok(()),
                        Ok(None) => store.db.put(&mut wtxn, key.as_slice(), value.as_slice()),
                        Err(e) => Err(e),
                    },
                    LmdbBatchWrite::Delete(key) => {
                        store.db.delete(&mut wtxn, key.as_slice()).map(|_| ())
                    }
                    LmdbBatchWrite::Compare(key, expected) => {
                        match store.db.get(&wtxn, key.as_slice()) {
                            Ok(Some(current)) if current == expected.as_slice() => Ok(()),
                            Ok(_) => {
                                let key_term = binary_term(env, key.as_slice())?;

                                return Ok(
                                    (atoms::error(), (atoms::compare_failed(), key_term)).encode(env)
                                );
                            }
                            Err(e) => Err(e),
                        }
                    }
                    LmdbBatchWrite::CompareMissing(key) => {
                        match store.db.get(&wtxn, key.as_slice()) {
                            Ok(None) => Ok(()),
                            Ok(Some(_)) => {
                                let key_term = binary_term(env, key.as_slice())?;

                                return Ok(
                                    (atoms::error(), (atoms::compare_failed(), key_term)).encode(env)
                                );
                            }
                            Err(e) => Err(e),
                        }
                    }
                };

                if let Err(e) = result {
                    return Ok((atoms::error(), e.to_string()).encode(env));
                }
            }

            match wtxn.commit() {
                Ok(()) if return_originals => {
                    let mut terms = Vec::with_capacity(originals.len());

                    for (key, original) in originals {
                        let key_term = binary_term(env, &key)?;
                        let original_term = match original {
                            Some(value) => {
                                let value_term = binary_term(env, &value)?;
                                (atoms::value(), value_term).encode(env)
                            }
                            None => atoms::missing().encode(env),
                        };
                        terms.push((key_term, original_term).encode(env));
                    }

                    Ok((atoms::ok(), terms).encode(env))
                }
                Ok(()) => Ok(atoms::ok().encode(env)),
                Err(e) => Ok((atoms::error(), e.to_string()).encode(env)),
            }
        }
        Err(e) => Ok((atoms::error(), e).encode(env)),
    }
}

fn lmdb_record_key<'a>(record: &'a LmdbBatchWrite<'_>) -> &'a [u8] {
    match record {
        LmdbBatchWrite::Put(key, _)
        | LmdbBatchWrite::PutNew(key, _)
        | LmdbBatchWrite::Delete(key)
        | LmdbBatchWrite::Compare(key, _)
        | LmdbBatchWrite::CompareMissing(key) => key.as_slice(),
    }
}

fn lmdb_record_keys_are_unique(records: &[LmdbBatchWrite<'_>]) -> bool {
    let mut seen = std::collections::HashSet::with_capacity(records.len());

    for record in records {
        if !seen.insert(lmdb_record_key(record).to_vec()) {
            return false;
        }
    }

    true
}

fn binary_term<'a>(env: Env<'a>, bytes: &[u8]) -> NifResult<Term<'a>> {
    let mut binary =
        OwnedBinary::new(bytes.len()).ok_or_else(|| rustler::Error::Term(Box::new("oom")))?;
    binary.as_mut_slice().copy_from_slice(bytes);
    Ok(binary.release(env).encode(env))
}
