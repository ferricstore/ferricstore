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
    let map_size = usize::try_from(map_size).map_err(|_| "lmdb map_size too large".to_string())?;
    if let Some(store) = exact_lmdb_store(path, map_size)? {
        return Ok(store);
    }

    crate::fs_nif::create_dir_all_nofollow(std::path::Path::new(path))
        .map_err(|e| e.to_string())?;
    let cache_key = std::fs::canonicalize(path)
        .map_err(|e| e.to_string())?
        .to_string_lossy()
        .into_owned();
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
        Ok(store) if store.map_size == map_size => {
            remember_exact_lmdb_store(path, &cache_key, store)?;
            Ok(Arc::clone(store))
        }
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

fn exact_lmdb_store(path: &str, map_size: usize) -> Result<Option<Arc<LmdbStore>>, String> {
    if !std::path::Path::new(path).is_absolute() {
        return Ok(None);
    }

    let paths = LMDB_VALIDATED_PATHS
        .get_or_init(|| RwLock::new(std::collections::HashMap::new()));
    // Release takes the write side before counting/removing stores, so keep
    // this read guard until the weak lease has been upgraded.
    let guard = paths
        .read()
        .map_err(|_| "lmdb validated path cache poisoned".to_string())?;
    let Some(validated) = guard.get(path) else {
        return Ok(None);
    };
    let cache_key = validated.cache_key.clone();
    if let Some(store) = validated.store.upgrade() {
        if store.map_size != map_size {
            return Err(format!(
                "lmdb map_size mismatch for {cache_key}: cached={}, requested={map_size}; release the environment before changing map_size",
                store.map_size
            ));
        }
        return Ok(Some(store));
    }
    drop(guard);

    let mut guard = paths
        .write()
        .map_err(|_| "lmdb validated path cache poisoned".to_string())?;
    if guard
        .get(path)
        .is_some_and(|validated| validated.store.strong_count() == 0)
    {
        guard.remove(path);
    }
    Ok(None)
}

fn remember_exact_lmdb_store(
    path: &str,
    cache_key: &str,
    store: &Arc<LmdbStore>,
) -> Result<(), String> {
    if !std::path::Path::new(path).is_absolute() {
        return Ok(());
    }

    let paths = LMDB_VALIDATED_PATHS
        .get_or_init(|| RwLock::new(std::collections::HashMap::new()));
    paths
        .write()
        .map_err(|_| "lmdb validated path cache poisoned".to_string())?
        .insert(
            path.to_owned(),
            LmdbValidatedPath {
                cache_key: cache_key.to_owned(),
                store: Arc::downgrade(store),
            },
        );
    Ok(())
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

fn release_lmdb_store(path: &str) -> Result<LmdbCacheRelease, String> {
    release_lmdb_store_with_hook(path, || {})
}

fn release_lmdb_store_with_hook(
    path: &str,
    after_cache_locks: impl FnOnce(),
) -> Result<LmdbCacheRelease, String> {
    // Cache lock order is validated paths then canonical stores. The open path
    // never holds both locks, so release can exclude new leases without a cycle.
    let paths = LMDB_VALIDATED_PATHS
        .get_or_init(|| RwLock::new(std::collections::HashMap::new()));
    let mut path_guard = paths
        .write()
        .map_err(|_| "lmdb validated path cache poisoned".to_string())?;
    let Some(stores) = LMDB_STORES.get() else {
        path_guard.clear();
        return Ok(LmdbCacheRelease::Released(0));
    };
    let cache_key = match path_guard.get(path) {
        Some(validated) => validated.cache_key.clone(),
        None => std::fs::canonicalize(path)
            .map_err(|error| error.to_string())?
            .to_string_lossy()
            .into_owned(),
    };
    let mut store_guard = stores
        .lock()
        .map_err(|_| "lmdb cache poisoned".to_string())?;

    after_cache_locks();

    let released = release_lmdb_cache_entry(&mut store_guard, &cache_key);
    if matches!(released, LmdbCacheRelease::Released(_)) {
        path_guard.retain(|_path, validated| validated.cache_key != cache_key);
    }
    Ok(released)
}

fn release_all_lmdb_stores() -> Result<LmdbCacheRelease, String> {
    let paths = LMDB_VALIDATED_PATHS
        .get_or_init(|| RwLock::new(std::collections::HashMap::new()));
    let mut path_guard = paths
        .write()
        .map_err(|_| "lmdb validated path cache poisoned".to_string())?;
    let Some(stores) = LMDB_STORES.get() else {
        path_guard.clear();
        return Ok(LmdbCacheRelease::Released(0));
    };
    let mut store_guard = stores
        .lock()
        .map_err(|_| "lmdb cache poisoned".to_string())?;
    let busy = store_guard
        .values()
        .filter(|cell| lmdb_store_cell_busy(cell))
        .count();

    if busy > 0 {
        return Ok(LmdbCacheRelease::Busy(busy));
    }

    let released = store_guard.len();
    store_guard.clear();
    path_guard.clear();
    Ok(LmdbCacheRelease::Released(released))
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
fn lmdb_get_many_bounded<'a>(
    env: Env<'a>,
    path: String,
    keys: Term<'a>,
    max_bytes: u64,
    map_size: u64,
) -> NifResult<Term<'a>> {
    lmdb_get_many_bounded_impl(env, path, keys, max_bytes, map_size, false)
}

#[rustler::nif(schedule = "DirtyIo")]
#[allow(clippy::needless_pass_by_value)]
fn lmdb_get_many_prefix_bounded<'a>(
    env: Env<'a>,
    path: String,
    keys: Term<'a>,
    max_bytes: u64,
    map_size: u64,
) -> NifResult<Term<'a>> {
    lmdb_get_many_bounded_impl(env, path, keys, max_bytes, map_size, true)
}

fn lmdb_get_many_bounded_impl<'a>(
    env: Env<'a>,
    path: String,
    keys: Term<'a>,
    max_bytes: u64,
    map_size: u64,
    allow_prefix: bool,
) -> NifResult<Term<'a>> {
    const MAX_KEYS: usize = 4_096;
    const MAX_KEY_BYTES: usize = 8 * 1_024 * 1_024;

    let key_count = keys.list_length()?;

    if key_count > MAX_KEYS {
        return Ok((atoms::error(), atoms::batch_key_budget_exceeded()).encode(env));
    }

    let mut decoded_keys = Vec::with_capacity(key_count);
    let mut key_bytes = 0_usize;

    for key in keys.into_list_iterator()? {
        let key = key.decode::<Binary<'a>>()?;
        let Some(next_key_bytes) = key_bytes.checked_add(key.as_slice().len()) else {
            return Ok((atoms::error(), atoms::batch_key_budget_exceeded()).encode(env));
        };

        if next_key_bytes > MAX_KEY_BYTES {
            return Ok((atoms::error(), atoms::batch_key_budget_exceeded()).encode(env));
        }

        key_bytes = next_key_bytes;
        decoded_keys.push(key);
    }

    match lmdb_store(&path, map_size) {
        Ok(store) => {
            let rtxn = match store.env.read_txn() {
                Ok(txn) => txn,
                Err(e) => return Ok((atoms::error(), e.to_string()).encode(env)),
            };

            let mut values = Vec::with_capacity(decoded_keys.len());
            let mut total_bytes = 0_u64;
            let mut complete = true;

            for key in &decoded_keys {
                match store.db.get(&rtxn, key.as_slice()) {
                    Ok(Some(value)) => {
                        let Ok(value_bytes) = u64::try_from(value.len()) else {
                            return Ok(
                                (atoms::error(), atoms::batch_value_budget_exceeded()).encode(env),
                            );
                        };

                        let Some(next_total) = total_bytes.checked_add(value_bytes) else {
                            if allow_prefix && !values.is_empty() {
                                complete = false;
                                break;
                            }

                            return Ok(
                                (atoms::error(), atoms::batch_value_budget_exceeded()).encode(env),
                            );
                        };

                        if next_total > max_bytes {
                            if allow_prefix && !values.is_empty() {
                                complete = false;
                                break;
                            }

                            return Ok(
                                (atoms::error(), atoms::batch_value_budget_exceeded()).encode(env),
                            );
                        }

                        total_bytes = next_total;
                        values.push(Some(value));
                    }
                    Ok(None) => values.push(None),
                    Err(e) => return Ok((atoms::error(), e.to_string()).encode(env)),
                }
            }

            let mut results = Vec::with_capacity(values.len());

            for value in values {
                match value {
                    Some(value) => {
                        let mut binary = OwnedBinary::new(value.len()).ok_or_else(|| {
                            rustler::Error::Term(Box::new("failed to allocate binary"))
                        })?;
                        binary.as_mut_slice().copy_from_slice(value);
                        results.push((atoms::ok(), binary.release(env)).encode(env));
                    }
                    None => results.push(atoms::not_found().encode(env)),
                }
            }

            if allow_prefix {
                Ok((atoms::ok(), results, total_bytes, complete).encode(env))
            } else {
                Ok((atoms::ok(), results, total_bytes).encode(env))
            }
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
            let mut entries =
                Vec::with_capacity(lmdb_page_capacity(max, usize::MAX));

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
            let mut entries =
                Vec::with_capacity(lmdb_page_capacity(max, usize::MAX));

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

const LMDB_PAGE_PREALLOC_LIMIT: usize = 4_096;

fn lmdb_page_capacity(item_cap: usize, byte_cap: usize) -> usize {
    item_cap.min(byte_cap).min(LMDB_PAGE_PREALLOC_LIMIT)
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
            let mut entries = Vec::with_capacity(lmdb_page_capacity(item_cap, byte_cap));
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
                            if row_bytes > byte_cap && entries.is_empty() {
                                return Ok(
                                    (atoms::error(), atoms::range_entry_too_large()).encode(env)
                                );
                            }
                            let next_bytes = entry_bytes.saturating_add(row_bytes);

                            if next_bytes > byte_cap {
                                break;
                            }

                            let key_term = binary_term(env, key)?;
                            let value_term = binary_term(env, value)?;
                            entries.push((key_term, value_term).encode(env));
                            entry_bytes = next_bytes;
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
                            if row_bytes > byte_cap && entries.is_empty() {
                                return Ok(
                                    (atoms::error(), atoms::range_entry_too_large()).encode(env)
                                );
                            }
                            let next_bytes = entry_bytes.saturating_add(row_bytes);

                            if next_bytes > byte_cap {
                                break;
                            }

                            let key_term = binary_term(env, key)?;
                            let value_term = binary_term(env, value)?;
                            entries.push((key_term, value_term).encode(env));
                            entry_bytes = next_bytes;
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
fn lmdb_range_entries_bounded<'a>(
    env: Env<'a>,
    path: String,
    prefix: Binary<'a>,
    after_key: Binary<'a>,
    before_key: Binary<'a>,
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

            let start = if after_key.as_slice().is_empty() {
                std::ops::Bound::Included(prefix.as_slice())
            } else {
                std::ops::Bound::Excluded(after_key.as_slice())
            };
            let upper = before_key.as_slice();
            let end = if upper.is_empty() {
                std::ops::Bound::Unbounded
            } else {
                std::ops::Bound::Excluded(upper)
            };
            let range = (start, end);
            let iter = match store.db.range(&rtxn, &range) {
                Ok(iter) => iter,
                Err(e) => return Ok((atoms::error(), e.to_string()).encode(env)),
            };

            let item_cap = usize::try_from(max_items).unwrap_or(usize::MAX);
            let byte_cap = usize::try_from(max_bytes).unwrap_or(usize::MAX);
            let mut entries = Vec::with_capacity(lmdb_page_capacity(item_cap, byte_cap));
            let mut entry_bytes = 0usize;
            let mut exhausted = true;

            for item in iter {
                let (key, value) = match item {
                    Ok(entry) => entry,
                    Err(e) => return Ok((atoms::error(), e.to_string()).encode(env)),
                };

                if !key.starts_with(prefix.as_slice()) {
                    break;
                }

                if entries.len() >= item_cap {
                    exhausted = false;
                    break;
                }

                let row_bytes = key.len().saturating_add(value.len());
                if row_bytes > byte_cap && entries.is_empty() {
                    return Ok((atoms::error(), atoms::range_entry_too_large()).encode(env));
                }

                let next_bytes = entry_bytes.saturating_add(row_bytes);
                if next_bytes > byte_cap {
                    exhausted = false;
                    break;
                }

                let key_term = binary_term(env, key)?;
                let value_term = binary_term(env, value)?;
                entries.push((key_term, value_term).encode(env));
                entry_bytes = next_bytes;
            }

            Ok((atoms::ok(), entries, exhausted, entry_bytes).encode(env))
        }
        Err(e) => Ok((atoms::error(), e).encode(env)),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn lmdb_composite_range_entries_bounded<'a>(
    env: Env<'a>,
    path: String,
    prefix: Binary<'a>,
    after_key: Binary<'a>,
    before_key: Binary<'a>,
    max_items: u64,
    max_bytes: u64,
    map_size: u64,
) -> NifResult<Term<'a>> {
    use sha2::{Digest, Sha256};

    match lmdb_store(&path, map_size) {
        Ok(store) => {
            let rtxn = match store.env.read_txn() {
                Ok(txn) => txn,
                Err(error) => return Ok((atoms::error(), error.to_string()).encode(env)),
            };
            let start = if after_key.as_slice().is_empty() {
                std::ops::Bound::Included(prefix.as_slice())
            } else {
                std::ops::Bound::Excluded(after_key.as_slice())
            };
            let end = if before_key.as_slice().is_empty() {
                std::ops::Bound::Unbounded
            } else {
                std::ops::Bound::Excluded(before_key.as_slice())
            };
            let iter = match store.db.range(&rtxn, &(start, end)) {
                Ok(iter) => iter,
                Err(error) => return Ok((atoms::error(), error.to_string()).encode(env)),
            };
            let item_cap = usize::try_from(max_items).unwrap_or(usize::MAX);
            let byte_cap = usize::try_from(max_bytes).unwrap_or(usize::MAX);
            let mut entries = Vec::with_capacity(lmdb_page_capacity(item_cap, byte_cap));
            let mut entry_bytes = 0usize;
            let mut exhausted = true;
            let mut hasher = Sha256::new();

            for item in iter {
                let (key, value) = match item {
                    Ok(entry) => entry,
                    Err(error) => return Ok((atoms::error(), error.to_string()).encode(env)),
                };
                if !key.starts_with(prefix.as_slice()) {
                    break;
                }
                if entries.len() >= item_cap {
                    exhausted = false;
                    break;
                }
                let row_bytes = key.len().saturating_add(value.len());
                if row_bytes > byte_cap && entries.is_empty() {
                    return Ok((atoms::error(), atoms::range_entry_too_large()).encode(env));
                }
                if entry_bytes.saturating_add(row_bytes) > byte_cap {
                    exhausted = false;
                    break;
                }

                let Some((id, state_key, record_version, expire_at_ms)) =
                    flow_composite_codec::decode_entry(key, value, &mut hasher)
                else {
                    return Ok((atoms::error(), atoms::invalid_composite_entry()).encode(env));
                };
                let key_term = binary_term(env, key)?;
                let id_term = binary_term(env, id)?;
                let state_term = binary_term(env, state_key)?;
                entries.push(
                    (
                        key_term,
                        id_term,
                        state_term,
                        record_version,
                        expire_at_ms,
                        row_bytes,
                    )
                        .encode(env),
                );
                entry_bytes = entry_bytes.saturating_add(row_bytes);
            }

            Ok((atoms::ok(), entries, exhausted, entry_bytes).encode(env))
        }
        Err(error) => Ok((atoms::error(), error).encode(env)),
    }
}

// Each source contributes at most `limit` rows; no later row can belong to the
// globally smallest `limit` rows. Heap entries borrow the LMDB snapshots, so
// candidate payloads are not cloned before the exact final byte check.
#[rustler::nif(schedule = "DirtyIo")]
fn lmdb_prefix_merge_entries<'a>(
    env: Env<'a>,
    paths: Vec<String>,
    prefix: Binary<'a>,
    limit: u64,
    max_bytes: u64,
    map_size: u64,
) -> NifResult<Term<'a>> {
    const MAX_PATHS: usize = 1_024;
    const MAX_PREFIX_BYTES: usize = 511;
    const MAX_LIMIT: u64 = 100_000;
    const MAX_BYTES: u64 = 64 * 1_024 * 1_024;
    if paths.is_empty()
        || paths.len() > MAX_PATHS
        || prefix.as_slice().is_empty()
        || prefix.len() > MAX_PREFIX_BYTES
        || limit > MAX_LIMIT
        || max_bytes == 0
        || max_bytes > MAX_BYTES
    {
        return Ok((atoms::error(), "invalid LMDB prefix merge").encode(env));
    }

    let cap = usize::try_from(limit).unwrap_or(usize::MAX);
    if cap == 0 {
        let entries: Vec<Term<'a>> = Vec::new();
        return Ok((atoms::ok(), entries, 0usize).encode(env));
    }
    let byte_cap = usize::try_from(max_bytes).unwrap_or(usize::MAX);
    let mut stores = Vec::with_capacity(paths.len());
    for path in &paths {
        match lmdb_store(path, map_size) {
            Ok(store) => stores.push(store),
            Err(error) => return Ok((atoms::error(), error).encode(env)),
        }
    }
    let mut read_txns = Vec::with_capacity(stores.len());
    for store in &stores {
        match store.env.read_txn() {
            Ok(txn) => read_txns.push(txn),
            Err(error) => return Ok((atoms::error(), error.to_string()).encode(env)),
        }
    }

    let mut selected: std::collections::BinaryHeap<(&[u8], usize, &[u8])> =
        std::collections::BinaryHeap::with_capacity(cap);
    let mut scanned = 0usize;

    for (source, (store, rtxn)) in stores.iter().zip(read_txns.iter()).enumerate() {
        let iter = match store.db.prefix_iter(rtxn, prefix.as_slice()) {
            Ok(iter) => iter,
            Err(error) => return Ok((atoms::error(), error.to_string()).encode(env)),
        };

        for item in iter.take(cap) {
            let (key, value) = match item {
                Ok(entry) => entry,
                Err(error) => return Ok((atoms::error(), error.to_string()).encode(env)),
            };
            scanned = scanned.saturating_add(1);
            let candidate = (key, source, value);
            if selected.len() < cap {
                selected.push(candidate);
            } else if selected.peek().is_some_and(|largest| &candidate < largest) {
                selected.pop();
                selected.push(candidate);
            }
        }
    }

    let selected = selected.into_sorted_vec();
    let selected_bytes = selected.iter().try_fold(0usize, |bytes, (key, _, value)| {
        bytes
            .checked_add(key.len())
            .and_then(|bytes| bytes.checked_add(value.len()))
            .filter(|bytes| *bytes <= byte_cap)
            .ok_or(())
    });
    if selected_bytes.is_err() {
        return Ok(
            (atoms::error(), atoms::prefix_merge_byte_budget_exceeded()).encode(env)
        );
    }

    let entries = selected
        .into_iter()
        .map(|(key, source, value)| {
            Ok(
                (source, binary_term(env, key)?, binary_term(env, value)?).encode(env),
            )
        })
        .collect::<NifResult<Vec<Term<'a>>>>()?;
    Ok((atoms::ok(), entries, scanned).encode(env))
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
            let mut entries =
                Vec::with_capacity(lmdb_page_capacity(max, usize::MAX));

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
            let mut entries =
                Vec::with_capacity(lmdb_page_capacity(max, usize::MAX));

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
    match release_all_lmdb_stores() {
        Ok(LmdbCacheRelease::Busy(count)) => Ok((atoms::busy(), count).encode(env)),
        Ok(LmdbCacheRelease::Released(count)) => Ok((atoms::ok(), count).encode(env)),
        Err(error) => Ok((atoms::error(), error).encode(env)),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn lmdb_release<'a>(env: Env<'a>, path: String) -> NifResult<Term<'a>> {
    match release_lmdb_store(&path) {
        Ok(LmdbCacheRelease::Busy(count)) => Ok((atoms::busy(), count).encode(env)),
        Ok(LmdbCacheRelease::Released(count)) => Ok((atoms::ok(), count).encode(env)),
        Err(error) => Ok((atoms::error(), error).encode(env)),
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
