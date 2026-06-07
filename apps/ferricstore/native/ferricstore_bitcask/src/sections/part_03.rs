#[cfg(test)]
mod copy_records_preserve_tombstones_tests {
    use super::*;

    #[test]
    fn copies_live_records_and_tombstones_in_replay_order() {
        let dir = tempfile::TempDir::new().unwrap();
        let source = dir.path().join("00001.log");
        let dest = dir.path().join("compact_1.log");

        let mut writer = log::LogWriter::open(&source, 1).unwrap();
        let live_offset = writer.write(b"b", b"live", 0).unwrap();
        let tombstone_offset = writer.write_tombstone(b"a").unwrap();
        writer.sync().unwrap();

        let results = copy_records_preserve_tombstones_impl(
            &source,
            &dest,
            1,
            &[live_offset],
            &[tombstone_offset],
        )
        .unwrap();

        assert_eq!(1, results.len());

        let mut reader = log::LogReader::open(&dest).unwrap();
        let first = reader.read_at(results[0].0).unwrap().unwrap();
        assert_eq!(b"b", first.key.as_slice());
        assert_eq!(Some(b"live".to_vec()), first.value);

        let tombstone_new_offset = (log::HEADER_SIZE + b"b".len() + b"live".len()) as u64;
        let second = reader.read_at(tombstone_new_offset).unwrap().unwrap();
        assert_eq!(b"a", second.key.as_slice());
        assert!(second.value.is_none());
    }

    #[test]
    fn compaction_copy_paths_do_not_materialize_live_values() {
        let source = include_str!("part_02.rs");
        let copy_records = source
            .split("fn v2_copy_records(")
            .nth(1)
            .unwrap()
            .split("/// Copy live records and tombstones")
            .next()
            .unwrap();
        let preserve = source
            .split("fn copy_records_preserve_tombstones_impl(")
            .nth(1)
            .unwrap()
            .split("#[cfg(test)]")
            .next()
            .unwrap();

        assert!(
            !copy_records.contains("read_at("),
            "v2_copy_records must raw-copy records without materializing values"
        );
        assert!(
            !preserve.contains("read_at("),
            "copy_records_preserve_tombstones_impl must raw-copy records without materializing values"
        );
    }
}

// ===========================================================================
// v2 Tokio async IO NIFs — pure stateless (no Store resource)
//
// These submit IO work to the global Tokio thread pool and send the result
// back to the calling Erlang process via OwnedEnv::send_and_clear.
// The BEAM Normal scheduler returns immediately — no blocking.
//
// All messages include a correlation_id so the Elixir side can match
// responses to requests, fixing the LIFO pending_reads ordering bug.
// ===========================================================================

/// Async pread: submit a single offset read to Tokio. Returns `:ok` immediately.
///
/// When the read completes, sends `{:tokio_complete, correlation_id, :ok, value_binary}`
/// or `{:tokio_complete, correlation_id, :ok, :nil}` (tombstone/EOF)
/// or `{:tokio_complete, correlation_id, :error, reason}` to the caller.
///
/// The BEAM scheduler is completely free while the Tokio thread does the
/// pread + CRC validation.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value)]
fn v2_pread_at_async(
    env: Env<'_>,
    caller_pid: LocalPid,
    correlation_id: u64,
    path: String,
    offset: u64,
) -> NifResult<Term<'_>> {
    async_io::runtime().spawn(async move {
        // spawn_blocking: IO runs on Tokio's blocking thread pool (up to 512 threads),
        // keeping async worker threads free for coordination.
        let result = tokio::task::spawn_blocking(move || {
            let p = std::path::Path::new(&path);
            std::fs::File::open(p)
                .map_err(|e| log::LogError(e.to_string()))
                .and_then(|file| {
                    fadvise_random(&file);
                    let record = log::pread_record_from_file(&file, offset);
                    if let Ok(Some(ref r)) = record {
                        let size =
                            (log::HEADER_SIZE + r.key.len() + r.value.as_ref().map_or(0, Vec::len))
                                as i64;
                        fadvise_dontneed(&file, offset as i64, size);
                    }
                    record
                })
        })
        .await
        .unwrap_or_else(|e| Err(log::LogError(format!("spawn_blocking failed: {e}"))));

        let mut msg_env = rustler::OwnedEnv::new();
        let _ = msg_env.send_and_clear(&caller_pid, |env| match result {
            Ok(Some(record)) => match record.value {
                Some(value) => {
                    let resource = ResourceArc::new(ValueBuffer { data: value });
                    let binary = resource.make_binary(env, |vb| &vb.data);
                    (atoms::tokio_complete(), correlation_id, atoms::ok(), binary).encode(env)
                }
                None => {
                    // Tombstone at this offset
                    (
                        atoms::tokio_complete(),
                        correlation_id,
                        atoms::ok(),
                        atoms::nil(),
                    )
                        .encode(env)
                }
            },
            Ok(None) => (
                atoms::tokio_complete(),
                correlation_id,
                atoms::error(),
                "offset past EOF",
            )
                .encode(env),
            Err(e) => (
                atoms::tokio_complete(),
                correlation_id,
                atoms::error(),
                e.to_string(),
            )
                .encode(env),
        });
    });
    Ok(atoms::ok().encode(env))
}

#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value)]
fn v2_pread_at_key_async<'a>(
    env: Env<'a>,
    caller_pid: LocalPid,
    correlation_id: u64,
    path: String,
    offset: u64,
    expected_key: Binary<'a>,
) -> NifResult<Term<'a>> {
    let expected_key = expected_key.as_slice().to_vec();

    async_io::runtime().spawn(async move {
        let result = tokio::task::spawn_blocking(move || {
            let p = std::path::Path::new(&path);
            std::fs::File::open(p)
                .map_err(|e| log::LogError(e.to_string()))
                .and_then(|file| {
                    fadvise_random(&file);
                    let value = log::pread_value_for_key_from_file(&file, offset, &expected_key);
                    if let Ok(Some(ref value)) = value {
                        let size = (log::HEADER_SIZE
                            + expected_key.len()
                            + value.as_ref().map_or(0, Vec::len))
                            as i64;
                        fadvise_dontneed(&file, offset as i64, size);
                    }
                    value
                })
        })
        .await
        .unwrap_or_else(|e| Err(log::LogError(format!("spawn_blocking failed: {e}"))));

        let mut msg_env = rustler::OwnedEnv::new();
        let _ = msg_env.send_and_clear(&caller_pid, |env| match result {
            Ok(Some(value)) => match value {
                Some(value) => {
                    let resource = ResourceArc::new(ValueBuffer { data: value });
                    let binary = resource.make_binary(env, |vb| &vb.data);
                    (atoms::tokio_complete(), correlation_id, atoms::ok(), binary).encode(env)
                }
                None => (
                    atoms::tokio_complete(),
                    correlation_id,
                    atoms::ok(),
                    atoms::nil(),
                )
                    .encode(env),
            },
            Ok(None) => (
                atoms::tokio_complete(),
                correlation_id,
                atoms::ok(),
                atoms::nil(),
            )
                .encode(env),
            Err(e) => (
                atoms::tokio_complete(),
                correlation_id,
                atoms::error(),
                e.to_string(),
            )
                .encode(env),
        });
    });
    Ok(atoms::ok().encode(env))
}

/// Async batch pread: submit multiple offset reads to Tokio concurrently.
/// Returns `:ok` immediately.
///
/// Each location is `{path, offset}`. All reads run concurrently on Tokio
/// worker threads. When ALL reads complete, sends a single message:
/// `{:tokio_complete, correlation_id, :ok, [value | nil | {:error, reason}, ...]}`
/// to the caller.
///
/// This is the async counterpart of `v2_pread_batch/2` and is used by the
/// MGET / GET_BATCH cold path. Decode/read failures are reported per index
/// so one corrupt record does not poison unrelated records in the batch.
fn group_pread_locations(
    locations: Vec<(String, u64)>,
) -> (usize, Vec<(String, Vec<(usize, u64)>)>) {
    let count = locations.len();
    let mut grouped: std::collections::HashMap<String, Vec<(usize, u64)>> =
        std::collections::HashMap::new();

    for (index, (path, offset)) in locations.into_iter().enumerate() {
        grouped.entry(path).or_default().push((index, offset));
    }

    (count, grouped.into_iter().collect())
}

fn validate_grouped_pread_groups(groups: &[(String, Vec<(usize, u64)>)]) -> Result<usize, String> {
    let count: usize = groups.iter().map(|(_, reads)| reads.len()).sum();
    let mut seen = vec![false; count];

    for (_path, reads) in groups {
        for (index, _offset) in reads {
            if *index >= count {
                return Err(format!(
                    "grouped pread index out of range: index={index}, count={count}"
                ));
            }

            if std::mem::replace(&mut seen[*index], true) {
                return Err(format!("grouped pread duplicate index: {index}"));
            }
        }
    }

    Ok(count)
}

fn validate_grouped_keyed_pread_groups(
    groups: &[(String, Vec<(usize, u64, Vec<u8>)>)],
) -> Result<usize, String> {
    let count: usize = groups.iter().map(|(_, reads)| reads.len()).sum();
    let mut seen = vec![false; count];

    for (_path, reads) in groups {
        for (index, _offset, _expected_key) in reads {
            if *index >= count {
                return Err(format!(
                    "grouped keyed pread index out of range: index={index}, count={count}"
                ));
            }

            if std::mem::replace(&mut seen[*index], true) {
                return Err(format!("grouped keyed pread duplicate index: {index}"));
            }
        }
    }

    Ok(count)
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum BatchReadValue {
    Value(Vec<u8>),
    Nil,
    Error(String),
}

impl BatchReadValue {
    #[cfg(test)]
    fn as_deref(&self) -> Option<&[u8]> {
        match self {
            BatchReadValue::Value(value) => Some(value.as_slice()),
            BatchReadValue::Nil | BatchReadValue::Error(_) => None,
        }
    }
}

fn missing_file_results<I>(path: &str, reads: I) -> Vec<(usize, BatchReadValue)>
where
    I: IntoIterator<Item = usize>,
{
    reads
        .into_iter()
        .map(|index| {
            (
                index,
                BatchReadValue::Error(format!("missing_file: {path}")),
            )
        })
        .collect()
}

fn pread_batch_for_path(
    path: String,
    reads: Vec<(usize, u64)>,
) -> Result<Vec<(usize, BatchReadValue)>, String> {
    let read_count = reads.len();

    let file = match std::fs::File::open(std::path::Path::new(&path)) {
        Ok(file) => file,
        Err(_e) => {
            return Ok(missing_file_results(
                &path,
                reads.into_iter().map(|(index, _offset)| index),
            ))
        }
    };

    fadvise_random(&file);

    let mut results = Vec::with_capacity(read_count);

    for (index, offset) in sort_reads_by_offset(reads) {
        let value = match log::pread_record_from_file(&file, offset) {
            Ok(Some(record)) => {
                let size = (log::HEADER_SIZE
                    + record.key.len()
                    + record.value.as_ref().map_or(0, Vec::len)) as i64;
                fadvise_dontneed(&file, offset as i64, size);
                record
                    .value
                    .map(BatchReadValue::Value)
                    .unwrap_or(BatchReadValue::Nil)
            }
            Ok(None) => BatchReadValue::Nil,
            Err(e) => BatchReadValue::Error(e.to_string()),
        };

        results.push((index, value));
    }

    Ok(results)
}

fn pread_batch_for_path_keyed(
    path: String,
    reads: Vec<(usize, u64, Vec<u8>)>,
) -> Result<Vec<(usize, BatchReadValue)>, String> {
    let read_count = reads.len();

    let file = match std::fs::File::open(std::path::Path::new(&path)) {
        Ok(file) => file,
        Err(_e) => {
            return Ok(missing_file_results(
                &path,
                reads
                    .into_iter()
                    .map(|(index, _offset, _expected_key)| index),
            ))
        }
    };

    fadvise_random(&file);

    let mut results = Vec::with_capacity(read_count);

    for (index, offset, expected_key) in sort_keyed_reads_by_offset(reads) {
        let value = match log::pread_value_for_key_from_file(&file, offset, &expected_key) {
            Ok(Some(value)) => {
                let size = (log::HEADER_SIZE
                    + expected_key.len()
                    + value.as_ref().map_or(0, Vec::len)) as i64;
                fadvise_dontneed(&file, offset as i64, size);

                value
                    .map(BatchReadValue::Value)
                    .unwrap_or(BatchReadValue::Nil)
            }
            Ok(None) => BatchReadValue::Nil,
            Err(e) => BatchReadValue::Error(e.to_string()),
        };

        results.push((index, value));
    }

    Ok(results)
}

fn sort_reads_by_offset(mut reads: Vec<(usize, u64)>) -> Vec<(usize, u64)> {
    reads.sort_by_key(|(_index, offset)| *offset);
    reads
}

fn sort_keyed_reads_by_offset(mut reads: Vec<(usize, u64, Vec<u8>)>) -> Vec<(usize, u64, Vec<u8>)> {
    reads.sort_by_key(|(_index, offset, _expected_key)| *offset);
    reads
}

fn apply_grouped_pread_results(
    values: &mut [BatchReadValue],
    results: Vec<(usize, BatchReadValue)>,
) {
    for (index, value) in results {
        if let Some(slot) = values.get_mut(index) {
            *slot = value;
        }
    }
}

fn send_pread_batch_result(
    caller_pid: LocalPid,
    correlation_id: u64,
    result: Result<Vec<BatchReadValue>, String>,
) {
    let mut msg_env = rustler::OwnedEnv::new();
    let _ = msg_env.send_and_clear(&caller_pid, |env| match result {
        Ok(values) => {
            let results: Vec<Term> = values
                .into_iter()
                .map(|value| match value {
                    BatchReadValue::Value(value) => {
                        let resource = ResourceArc::new(ValueBuffer { data: value });
                        resource.make_binary(env, |vb| &vb.data).encode(env)
                    }
                    BatchReadValue::Nil => atoms::nil().encode(env),
                    BatchReadValue::Error(reason) => (atoms::error(), reason.as_str()).encode(env),
                })
                .collect();
            (
                atoms::tokio_complete(),
                correlation_id,
                atoms::ok(),
                results,
            )
                .encode(env)
        }
        Err(reason) => (
            atoms::tokio_complete(),
            correlation_id,
            atoms::error(),
            reason.as_str(),
        )
            .encode(env),
    });
}

#[cfg(test)]
fn pread_batch_grouped(locations: Vec<(String, u64)>) -> Result<Vec<BatchReadValue>, String> {
    let (count, groups) = group_pread_locations(locations);
    let mut values = vec![BatchReadValue::Nil; count];

    for (path, reads) in groups {
        apply_grouped_pread_results(&mut values, pread_batch_for_path(path, reads)?);
    }

    Ok(values)
}

#[cfg(test)]
fn pread_batch_grouped_keyed(
    locations: Vec<(String, u64, Vec<u8>)>,
) -> Result<Vec<BatchReadValue>, String> {
    let count = locations.len();
    let mut grouped: std::collections::HashMap<String, Vec<(usize, u64, Vec<u8>)>> =
        std::collections::HashMap::new();

    for (index, (path, offset, expected_key)) in locations.into_iter().enumerate() {
        grouped
            .entry(path)
            .or_default()
            .push((index, offset, expected_key));
    }

    let mut values = vec![BatchReadValue::Nil; count];

    for (path, reads) in grouped {
        apply_grouped_pread_results(&mut values, pread_batch_for_path_keyed(path, reads)?);
    }

    Ok(values)
}

#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value)]
fn v2_pread_batch_path_async(
    env: Env<'_>,
    caller_pid: LocalPid,
    correlation_id: u64,
    path: String,
    offsets: Vec<u64>,
) -> NifResult<Term<'_>> {
    async_io::runtime().spawn(async move {
        let count = offsets.len();
        let reads: Vec<(usize, u64)> = offsets.into_iter().enumerate().collect();

        let result = tokio::task::spawn_blocking(move || {
            let mut values = vec![BatchReadValue::Nil; count];
            apply_grouped_pread_results(&mut values, pread_batch_for_path(path, reads)?);
            Ok(values)
        })
        .await
        .unwrap_or_else(|e| Err(format!("spawn_blocking: {e}")));

        send_pread_batch_result(caller_pid, correlation_id, result);
    });
    Ok(atoms::ok().encode(env))
}

#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value)]
fn v2_pread_batch_path_key_async<'a>(
    env: Env<'a>,
    caller_pid: LocalPid,
    correlation_id: u64,
    path: String,
    reads: Vec<(u64, Binary<'a>)>,
) -> NifResult<Term<'a>> {
    let count = reads.len();
    let reads: Vec<(usize, u64, Vec<u8>)> = reads
        .into_iter()
        .enumerate()
        .map(|(index, (offset, key))| (index, offset, key.as_slice().to_vec()))
        .collect();

    async_io::runtime().spawn(async move {
        let result = tokio::task::spawn_blocking(move || {
            let mut values = vec![BatchReadValue::Nil; count];
            apply_grouped_pread_results(&mut values, pread_batch_for_path_keyed(path, reads)?);
            Ok(values)
        })
        .await
        .unwrap_or_else(|e| Err(format!("spawn_blocking: {e}")));

        send_pread_batch_result(caller_pid, correlation_id, result);
    });
    Ok(atoms::ok().encode(env))
}

#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value)]
fn v2_pread_batch_async(
    env: Env<'_>,
    caller_pid: LocalPid,
    correlation_id: u64,
    locations: Vec<(String, u64)>,
) -> NifResult<Term<'_>> {
    async_io::runtime().spawn(async move {
        let (count, groups) = group_pread_locations(locations);
        let mut handles = Vec::with_capacity(groups.len());

        for (path, reads) in groups {
            handles.push(tokio::task::spawn_blocking(move || {
                pread_batch_for_path(path, reads)
            }));
        }

        let mut result: Result<Vec<BatchReadValue>, String> = Ok(vec![BatchReadValue::Nil; count]);
        for handle in handles {
            match handle.await {
                Ok(Ok(results)) => {
                    if let Ok(values) = &mut result {
                        apply_grouped_pread_results(values, results);
                    }
                }
                Ok(Err(reason)) => {
                    result = Err(reason);
                    break;
                }
                Err(e) => {
                    result = Err(format!("spawn_blocking: {e}"));
                    break;
                }
            }
        }

        send_pread_batch_result(caller_pid, correlation_id, result);
    });
    Ok(atoms::ok().encode(env))
}

#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value)]
fn v2_pread_batch_grouped_async(
    env: Env<'_>,
    caller_pid: LocalPid,
    correlation_id: u64,
    groups: Vec<(String, Vec<(usize, u64)>)>,
) -> NifResult<Term<'_>> {
    async_io::runtime().spawn(async move {
        let count = match validate_grouped_pread_groups(&groups) {
            Ok(count) => count,
            Err(reason) => {
                send_pread_batch_result(caller_pid, correlation_id, Err(reason));
                return;
            }
        };

        let mut handles = Vec::with_capacity(groups.len());

        for (path, reads) in groups {
            handles.push(tokio::task::spawn_blocking(move || {
                pread_batch_for_path(path, reads)
            }));
        }

        let mut result: Result<Vec<BatchReadValue>, String> = Ok(vec![BatchReadValue::Nil; count]);
        for handle in handles {
            match handle.await {
                Ok(Ok(results)) => {
                    if let Ok(values) = &mut result {
                        apply_grouped_pread_results(values, results);
                    }
                }
                Ok(Err(reason)) => {
                    result = Err(reason);
                    break;
                }
                Err(e) => {
                    result = Err(format!("spawn_blocking: {e}"));
                    break;
                }
            }
        }

        send_pread_batch_result(caller_pid, correlation_id, result);
    });
    Ok(atoms::ok().encode(env))
}

#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value)]
fn v2_pread_batch_grouped_key_async<'a>(
    env: Env<'a>,
    caller_pid: LocalPid,
    correlation_id: u64,
    groups: Vec<(String, Vec<(usize, u64, Binary<'a>)>)>,
) -> NifResult<Term<'a>> {
    let groups: Vec<(String, Vec<(usize, u64, Vec<u8>)>)> = groups
        .into_iter()
        .map(|(path, reads)| {
            let reads = reads
                .into_iter()
                .map(|(index, offset, key)| (index, offset, key.as_slice().to_vec()))
                .collect();
            (path, reads)
        })
        .collect();

    async_io::runtime().spawn(async move {
        let count = match validate_grouped_keyed_pread_groups(&groups) {
            Ok(count) => count,
            Err(reason) => {
                send_pread_batch_result(caller_pid, correlation_id, Err(reason));
                return;
            }
        };

        let mut handles = Vec::with_capacity(groups.len());

        for (path, reads) in groups {
            handles.push(tokio::task::spawn_blocking(move || {
                pread_batch_for_path_keyed(path, reads)
            }));
        }

        let mut result: Result<Vec<BatchReadValue>, String> = Ok(vec![BatchReadValue::Nil; count]);
        for handle in handles {
            match handle.await {
                Ok(Ok(results)) => {
                    if let Ok(values) = &mut result {
                        apply_grouped_pread_results(values, results);
                    }
                }
                Ok(Err(reason)) => {
                    result = Err(reason);
                    break;
                }
                Err(e) => {
                    result = Err(format!("spawn_blocking: {e}"));
                    break;
                }
            }
        }

        send_pread_batch_result(caller_pid, correlation_id, result);
    });
    Ok(atoms::ok().encode(env))
}

/// Async fsync: submit fsync to Tokio thread pool. Returns `:ok` immediately.
///
/// Sends `{:tokio_complete, correlation_id, :ok, :ok}` or
/// `{:tokio_complete, correlation_id, :error, reason}` on completion.
///
/// Fsync can block for milliseconds even on NVMe. By offloading to Tokio,
/// the BEAM scheduler stays free.
#[rustler::nif(schedule = "Normal")]
#[allow(clippy::needless_pass_by_value)]
fn v2_fsync_async(
    env: Env<'_>,
    caller_pid: LocalPid,
    correlation_id: u64,
    path: String,
) -> NifResult<Term<'_>> {
    async_io::runtime().spawn(async move {
        let result = tokio::task::spawn_blocking(move || {
            let p = std::path::Path::new(&path);
            std::fs::OpenOptions::new()
                .write(true)
                .open(p)
                .and_then(|f| f.sync_data())
        })
        .await
        .unwrap_or_else(|e| Err(std::io::Error::other(format!("spawn_blocking: {e}"))));

        let mut msg_env = rustler::OwnedEnv::new();
        let _ = msg_env.send_and_clear(&caller_pid, |env| match result {
            Ok(()) => (
                atoms::tokio_complete(),
                correlation_id,
                atoms::ok(),
                atoms::ok(),
            )
                .encode(env),
            Err(e) => (
                atoms::tokio_complete(),
                correlation_id,
                atoms::error(),
                e.to_string(),
            )
                .encode(env),
        });
    });
    Ok(atoms::ok().encode(env))
}

#[rustler::nif]
fn io_uring_available() -> bool {
    io_backend::detect_io_uring()
}
