# Compares the removed duplicated-record layout with the production QueryRow
# layout. The previous envelope exists only in this benchmark as release evidence.

Code.require_file("support/query_performance.exs", __DIR__)
Code.require_file("support/query_storage_fixture.exs", __DIR__)

defmodule Ferricstore.Bench.QueryStorageLayout do
  @moduledoc false

  alias Ferricstore.Bench.{QueryPerformance, QueryStorageFixture}
  alias Ferricstore.Flow.{Codec, Keys, LMDB, RecordHydrator, RecordIdentity}
  alias Ferricstore.Flow.Query.{QueryRecordStore, QueryRowStore}
  alias Ferricstore.Raft.WARaftSegmentReader
  alias Ferricstore.Raft.WARaftSegmentReader.DiskReader

  @maximum_read_bytes 64 * 1_024 * 1_024
  @default_records 10_000
  @write_page_records 256

  def run do
    record_count = QueryPerformance.int_env("BENCH_STORAGE_RECORDS", @default_records, min: 100)

    records_per_log_entry =
      QueryPerformance.int_env("BENCH_STORAGE_RECORDS_PER_LOG_ENTRY", 1, min: 1)

    if records_per_log_entry > 256 do
      raise ArgumentError, "BENCH_STORAGE_RECORDS_PER_LOG_ENTRY must be <= 256"
    end

    diagnostics? = QueryPerformance.bool_env("BENCH_STORAGE_DIAGNOSTICS")
    batch_read_only? = QueryPerformance.bool_env("BENCH_STORAGE_BATCH_READ_ONLY")
    hydration_only? = QueryPerformance.bool_env("BENCH_STORAGE_HYDRATION_ONLY")
    cold_cache? = QueryPerformance.bool_env("BENCH_STORAGE_COLD_CACHE")

    candidate_reader_lanes =
      QueryPerformance.int_env("BENCH_STORAGE_CANDIDATE_READER_LANES", 1, min: 1)

    if (batch_read_only? or hydration_only?) and not diagnostics? do
      raise ArgumentError,
            "diagnostic-only storage modes require BENCH_STORAGE_DIAGNOSTICS=true"
    end

    if batch_read_only? and hydration_only? do
      raise ArgumentError,
            "storage batch-read-only and hydration-only modes are mutually exclusive"
    end

    if candidate_reader_lanes > 16 do
      raise ArgumentError, "BENCH_STORAGE_CANDIDATE_READER_LANES must be <= 16"
    end

    validate_cold_cache_support!(cold_cache?)

    root = temp_root()

    datasets =
      Map.new([:sparse, :typical, :metadata_heavy], fn shape ->
        {shape,
         dataset(
           root,
           shape,
           record_count,
           records_per_log_entry,
           diagnostics?,
           candidate_reader_lanes
         )}
      end)

    try do
      Enum.each(datasets, fn {_shape, dataset} -> preflight!(dataset) end)
      print_storage(datasets)

      comparison_jobs =
        [1, 25, 100]
        |> Enum.reduce(%{}, fn page, jobs ->
          jobs
          |> Map.put("previous duplicated LMDB full-record read/page-#{page}", fn dataset ->
            previous_read(dataset, page)
          end)
          |> Map.put("production QueryRow + authoritative-log read/page-#{page}", fn dataset ->
            production_read(dataset, page)
          end)
        end)

      jobs =
        cond do
          hydration_only? -> diagnostic_hydration_jobs()
          batch_read_only? -> diagnostic_batch_read_jobs()
          true -> maybe_add_diagnostic_jobs(comparison_jobs, diagnostics?)
        end

      Benchee.run(
        jobs,
        [inputs: Map.new(datasets, fn {shape, dataset} -> {Atom.to_string(shape), dataset} end)] ++
          QueryPerformance.benchee_options("query-storage-layout")
      )

      if cold_cache?, do: run_cold_cache(datasets)
    after
      Enum.each(datasets, fn {_shape, dataset} ->
        close_diagnostic_segment(dataset)
        QueryStorageFixture.cleanup(dataset.ctx)
        release(dataset.previous_path)
        release(dataset.production_path)
      end)

      File.rm_rf!(root)
    end
  end

  defp dataset(
         root,
         shape,
         record_count,
         records_per_log_entry,
         diagnostics?,
         candidate_reader_lanes
       ) do
    records = Enum.map(1..record_count, &record(shape, &1))
    ctx = context(Path.join(root, "production-#{shape}"))
    production_path = lmdb_path(ctx)

    storage =
      QueryStorageFixture.write!(ctx, records, page_records: records_per_log_entry)

    previous_path = Path.join(root, "previous-#{shape}")

    previous_record_ops =
      Enum.map(records, fn record ->
        state_key = Keys.state_key(record.id, record.partition_key)
        encoded = Map.fetch!(storage.encoded_by_key, state_key)
        {:put, state_key, LMDB.encode_value(encoded, 0)}
      end)

    previous_ops = previous_record_ops ++ storage.source_catalog_ops

    previous_ops
    |> Enum.chunk_every(@write_page_records)
    |> Enum.each(fn page -> :ok = LMDB.write_batch(previous_path, page) end)

    previous_lmdb_bytes = logical_bytes(previous_record_ops) + storage.source_catalog_bytes
    production_lmdb_bytes = storage.query_row_bytes + storage.source_catalog_bytes

    dataset = %{
      shape: shape,
      ctx: ctx,
      record_count: record_count,
      records_per_log_entry: records_per_log_entry,
      state_keys: Enum.map(records, &Keys.state_key(&1.id, &1.partition_key)),
      production_path: production_path,
      previous_path: previous_path,
      source_bytes: storage.source_bytes,
      source_catalog_bytes: storage.source_catalog_bytes,
      previous_lmdb_bytes: previous_lmdb_bytes,
      production_lmdb_bytes: production_lmdb_bytes,
      previous_total_write_bytes: storage.source_bytes + previous_lmdb_bytes,
      production_total_write_bytes: storage.source_bytes + production_lmdb_bytes,
      previous_physical_lmdb_bytes: QueryPerformance.directory_bytes(previous_path),
      production_physical_lmdb_bytes: QueryPerformance.directory_bytes(production_path)
    }

    if diagnostics?,
      do: add_diagnostics(dataset, storage.encoded_by_key, candidate_reader_lanes),
      else: dataset
  end

  defp preflight!(dataset) do
    for page <- [1, 25, 100] do
      previous = previous_read(dataset, page)
      production = production_read(dataset, page)
      ^previous = production

      preflight_diagnostics!(dataset, page, previous)
    end
  end

  defp preflight_diagnostics!(%{diagnostic_requests: requests} = dataset, page, expected) do
    references = diagnostic_reference_read(dataset, page)
    true = length(references) == length(expected)

    {:ok, hydrated} =
      RecordHydrator.read_many(
        dataset.ctx,
        0,
        Map.fetch!(requests, page),
        max_bytes: @maximum_read_bytes,
        now_ms: 0
      )

    ^expected = hydrated
    ^expected = candidate_inline_record_hydration(dataset, page)

    if page == 1 do
      segment_read = diagnostic_segment_read(dataset)
      ^segment_read = candidate_verified_segment_read(dataset)

      retained_task = Task.async(fn -> candidate_retained_segment_read(dataset) end)
      ^segment_read = Task.await(retained_task)
    end

    encoded = Map.fetch!(dataset.diagnostic_encoded, page)
    ^encoded = diagnostic_authoritative_raw_read(dataset, page)
    ^encoded = production_authoritative_vector_read(dataset, page)
    ^encoded = production_physical_locator_read(dataset, page)
    ^encoded = candidate_registry_authoritative_vector_read(dataset, page)
    ^encoded = candidate_retained_authoritative_raw_read(dataset, page)
    ^encoded = candidate_coalesced_authoritative_raw_read(dataset, page)
    true = diagnostic_locator_checksums(dataset, page)
    ^expected = diagnostic_record_batch_decode(dataset, page)

    :ok
  end

  defp preflight_diagnostics!(_dataset, _page, _expected), do: :ok

  defp maybe_add_diagnostic_jobs(jobs, false), do: jobs

  defp maybe_add_diagnostic_jobs(jobs, true) do
    jobs =
      Enum.reduce([1, 25, 100], jobs, fn page, jobs ->
        jobs
        |> Map.put("diagnostic compact QueryRow reference read/page-#{page}", fn dataset ->
          diagnostic_reference_read(dataset, page)
        end)
        |> Map.put("diagnostic authoritative-log hydration/page-#{page}", fn dataset ->
          diagnostic_authoritative_hydration(dataset, page)
        end)
        |> Map.put("candidate inline-record hydration/page-#{page}", fn dataset ->
          candidate_inline_record_hydration(dataset, page)
        end)
        |> Map.put("diagnostic authoritative raw batch read/page-#{page}", fn dataset ->
          diagnostic_authoritative_raw_read(dataset, page)
        end)
        |> Map.put("production authoritative vector batch read/page-#{page}", fn dataset ->
          production_authoritative_vector_read(dataset, page)
        end)
        |> Map.put("production physical locator batch read/page-#{page}", fn dataset ->
          production_physical_locator_read(dataset, page)
        end)
        |> Map.put("candidate registry + retained-fd vector read/page-#{page}", fn dataset ->
          candidate_registry_authoritative_vector_read(dataset, page)
        end)
        |> Map.put("candidate retained-fd authoritative batch read/page-#{page}", fn dataset ->
          candidate_retained_authoritative_raw_read(dataset, page)
        end)
        |> Map.put(
          "candidate coalesced-adjacent authoritative batch read/page-#{page}",
          fn dataset ->
            candidate_coalesced_authoritative_raw_read(dataset, page)
          end
        )
        |> Map.put("diagnostic locator SHA-256 validation/page-#{page}", fn dataset ->
          diagnostic_locator_checksums(dataset, page)
        end)
        |> Map.put("diagnostic authoritative record batch decode/page-#{page}", fn dataset ->
          diagnostic_record_batch_decode(dataset, page)
        end)
      end)

    jobs
    |> Map.put("diagnostic segment location lookup", &diagnostic_segment_location/1)
    |> Map.put("diagnostic segment read at known location", &diagnostic_segment_read/1)
    |> Map.put("candidate verified single-pread segment read", &candidate_verified_segment_read/1)
    |> Map.put(
      "candidate retained-fd single-pread segment read",
      &candidate_retained_segment_read/1
    )
  end

  defp diagnostic_batch_read_jobs do
    Enum.reduce([25, 100], %{}, fn page, jobs ->
      jobs
      |> Map.put("diagnostic authoritative raw batch read/page-#{page}", fn dataset ->
        diagnostic_authoritative_raw_read(dataset, page)
      end)
      |> Map.put("production authoritative vector batch read/page-#{page}", fn dataset ->
        production_authoritative_vector_read(dataset, page)
      end)
      |> Map.put("production physical locator batch read/page-#{page}", fn dataset ->
        production_physical_locator_read(dataset, page)
      end)
      |> Map.put("candidate registry + retained-fd vector read/page-#{page}", fn dataset ->
        candidate_registry_authoritative_vector_read(dataset, page)
      end)
      |> Map.put("candidate retained-fd authoritative batch read/page-#{page}", fn dataset ->
        candidate_retained_authoritative_raw_read(dataset, page)
      end)
      |> Map.put(
        "candidate coalesced-adjacent authoritative batch read/page-#{page}",
        fn dataset ->
          candidate_coalesced_authoritative_raw_read(dataset, page)
        end
      )
    end)
  end

  defp diagnostic_hydration_jobs do
    Enum.reduce([1, 25, 100], %{}, fn page, jobs ->
      jobs
      |> Map.put("diagnostic authoritative-log hydration/page-#{page}", fn dataset ->
        diagnostic_authoritative_hydration(dataset, page)
      end)
      |> Map.put("candidate inline-record hydration/page-#{page}", fn dataset ->
        candidate_inline_record_hydration(dataset, page)
      end)
    end)
  end

  defp print_storage(datasets) do
    Enum.each(datasets, fn {shape, dataset} ->
      logical_savings =
        percentage_decrease(dataset.previous_lmdb_bytes, dataset.production_lmdb_bytes)

      total_write_savings =
        percentage_decrease(
          dataset.previous_total_write_bytes,
          dataset.production_total_write_bytes
        )

      IO.puts(
        "query_storage shape=#{shape} records=#{dataset.record_count} " <>
          "records_per_log_entry=#{dataset.records_per_log_entry} " <>
          "source_logical_bytes=#{dataset.source_bytes} " <>
          "shared_source_catalog_logical_bytes=#{dataset.source_catalog_bytes} " <>
          "previous_lmdb_logical_bytes=#{dataset.previous_lmdb_bytes} " <>
          "production_lmdb_logical_bytes=#{dataset.production_lmdb_bytes} " <>
          "lmdb_logical_savings_percent=#{logical_savings} " <>
          "total_write_savings_percent=#{total_write_savings} " <>
          "previous_lmdb_physical_bytes=#{dataset.previous_physical_lmdb_bytes} " <>
          "production_lmdb_physical_bytes=#{dataset.production_physical_lmdb_bytes}"
      )
    end)
  end

  defp previous_read(dataset, page) do
    keys = Enum.take(dataset.state_keys, page)

    {:ok, values, _bytes, true} =
      LMDB.get_many_prefix_bounded(dataset.previous_path, keys, @maximum_read_bytes)

    encoded =
      Enum.map(values, fn {:ok, wrapper} ->
        {:ok, record} = LMDB.decode_value(wrapper, 0)
        record
      end)

    Codec.decode_records(encoded)
  end

  defp production_read(dataset, page) do
    keys = Enum.take(dataset.state_keys, page)

    {:ok, records, true} =
      QueryRecordStore.read_many(
        dataset.ctx,
        0,
        dataset.production_path,
        keys,
        0,
        @maximum_read_bytes
      )

    records
  end

  defp diagnostic_reference_read(dataset, page) do
    keys = Enum.take(dataset.state_keys, page)

    {:ok, references, _bytes, true} =
      QueryRowStore.read_references_many(
        dataset.production_path,
        keys,
        0,
        @maximum_read_bytes
      )

    references
  end

  defp diagnostic_authoritative_hydration(dataset, page) do
    {:ok, records} =
      RecordHydrator.read_many(
        dataset.ctx,
        0,
        Map.fetch!(dataset.diagnostic_requests, page),
        max_bytes: @maximum_read_bytes,
        now_ms: 0
      )

    records
  end

  defp candidate_inline_record_hydration(dataset, page) do
    deadline_ms = System.monotonic_time(:millisecond) + 10_000
    requests = Map.fetch!(dataset.diagnostic_requests, page)
    encoded = production_physical_locator_read(dataset, page)

    total_bytes =
      Enum.zip_with(encoded, requests, fn value, {_state_key, locator} ->
        true = is_binary(value)
        true = byte_size(value) == locator.value_size
        :error = Ferricstore.Store.BlobRef.decode(value)
        true = :crypto.hash_equals(:crypto.hash(:sha256, value), locator.checksum)
        byte_size(value)
      end)
      |> Enum.sum()

    true = total_bytes <= @maximum_read_bytes
    records = Codec.decode_records(encoded)

    true =
      Enum.zip_with(records, requests, fn record, {state_key, locator} ->
        Map.get(record, :id) == locator.flow_id and
          Map.get(record, :version) == locator.version and
          RecordIdentity.owns_state_key?(record, state_key)
      end)
      |> Enum.all?()

    true = System.monotonic_time(:millisecond) < deadline_ms
    records
  end

  defp diagnostic_authoritative_raw_read(dataset, page) do
    requests = Map.fetch!(dataset.diagnostic_requests, page)

    values =
      requests
      |> Enum.group_by(fn {_state_key, locator} -> locator.file_id end)
      |> Enum.reduce(%{}, fn {file_id, grouped}, values ->
        keys = Enum.map(grouped, &elem(&1, 0))

        {:ok, group_values} =
          WARaftSegmentReader.read_values_from_location(
            dataset.ctx,
            0,
            file_id,
            keys,
            10_000
          )

        Map.merge(values, group_values)
      end)

    Enum.map(requests, fn {state_key, _locator} -> Map.fetch!(values, state_key) end)
  end

  defp production_authoritative_vector_read(dataset, page) do
    requests = Map.fetch!(dataset.diagnostic_requests, page)

    groups =
      requests
      |> Enum.group_by(fn {_state_key, locator} -> locator.file_id end)
      |> Enum.map(fn {file_id, grouped} ->
        {file_id, Enum.map(grouped, &elem(&1, 0))}
      end)

    {:ok, values} =
      WARaftSegmentReader.read_values_from_locations(dataset.ctx, 0, groups, 10_000)

    Enum.map(requests, fn {state_key, _locator} -> Map.fetch!(values, state_key) end)
  end

  defp production_physical_locator_read(dataset, page) do
    logical_requests = Map.fetch!(dataset.diagnostic_requests, page)

    physical_requests =
      Enum.map(logical_requests, fn {state_key, locator} ->
        %{
          file_id: locator.file_id,
          ordinal: locator.segment_generation,
          offset: locator.offset,
          frame_size: locator.frame_size,
          key: state_key
        }
      end)

    {:ok, values} =
      WARaftSegmentReader.read_physical_values(
        dataset.ctx,
        0,
        physical_requests,
        10_000,
        :live
      )

    Enum.map(logical_requests, fn {state_key, _locator} -> Map.fetch!(values, state_key) end)
  end

  defp candidate_registry_authoritative_vector_read(dataset, page) do
    physical =
      dataset.diagnostic_physical_requests
      |> Map.fetch!(page)
      |> Enum.map(&candidate_registry_location!/1)

    values =
      physical
      |> Enum.group_by(& &1.path)
      |> Enum.reduce(%{}, fn {path, requests}, values ->
        reader = candidate_reader(dataset, path)
        {:ok, entries} = candidate_retained_segment_read_many(reader, requests)

        Enum.zip(requests, entries)
        |> Enum.reduce(values, fn {request, entry}, acc ->
          Map.merge(acc, candidate_projection_values(entry, request.keys))
        end)
      end)

    dataset.diagnostic_requests
    |> Map.fetch!(page)
    |> Enum.map(fn {state_key, _locator} -> Map.fetch!(values, state_key) end)
  end

  defp candidate_registry_location!(request) do
    dir_key = request.path |> Path.dirname() |> Path.expand()

    case :ets.lookup(
           :ferricstore_waraft_segment_offset_registry,
           {dir_key, request.index}
         ) do
      [{{^dir_key, index}, ordinal, offset, encoded_size}]
      when index == request.index and is_integer(ordinal) and ordinal >= 0 and
             is_integer(offset) and offset >= 0 and is_integer(encoded_size) and
             encoded_size >= 8 ->
        %{request | ordinal: ordinal, offset: offset, encoded_size: encoded_size}

      invalid ->
        raise "invalid candidate offset-registry result: #{inspect(invalid)}"
    end
  end

  defp diagnostic_locator_checksums(dataset, page) do
    dataset.diagnostic_encoded
    |> Map.fetch!(page)
    |> Enum.zip(Map.fetch!(dataset.diagnostic_requests, page))
    |> Enum.all?(fn {encoded, {_state_key, locator}} ->
      :crypto.hash_equals(:crypto.hash(:sha256, encoded), locator.checksum)
    end)
  end

  defp diagnostic_record_batch_decode(dataset, page) do
    dataset.diagnostic_encoded
    |> Map.fetch!(page)
    |> Codec.decode_records()
  end

  defp candidate_retained_authoritative_raw_read(dataset, page) do
    values =
      dataset.diagnostic_physical_requests
      |> Map.fetch!(page)
      |> Enum.group_by(& &1.path)
      |> Enum.reduce(%{}, fn {path, physical}, values ->
        reader = candidate_reader(dataset, path)
        {:ok, entries} = candidate_retained_segment_read_many(reader, physical)

        Enum.zip(physical, entries)
        |> Enum.reduce(values, fn {request, entry}, acc ->
          Map.merge(acc, candidate_projection_values(entry, request.keys))
        end)
      end)

    dataset.diagnostic_requests
    |> Map.fetch!(page)
    |> Enum.map(fn {state_key, _locator} -> Map.fetch!(values, state_key) end)
  end

  defp candidate_coalesced_authoritative_raw_read(dataset, page) do
    values =
      dataset.diagnostic_physical_requests
      |> Map.fetch!(page)
      |> Enum.group_by(& &1.path)
      |> Enum.reduce(%{}, fn {path, physical}, values ->
        reader = candidate_reader(dataset, path)
        {:ok, entries} = candidate_coalesced_segment_read_many(reader, physical)

        Enum.zip(physical, entries)
        |> Enum.reduce(values, fn {request, entry}, acc ->
          Map.merge(acc, candidate_projection_values(entry, request.keys))
        end)
      end)

    dataset.diagnostic_requests
    |> Map.fetch!(page)
    |> Enum.map(fn {state_key, _locator} -> Map.fetch!(values, state_key) end)
  end

  defp diagnostic_requests(dataset) do
    Map.new([1, 25, 100], fn page ->
      keys = Enum.take(dataset.state_keys, page)

      requests =
        dataset
        |> diagnostic_reference_read(page)
        |> Enum.zip(keys)
        |> Enum.map(fn {%{locator: locator}, state_key} -> {state_key, locator} end)

      {page, requests}
    end)
  end

  defp add_diagnostics(dataset, encoded_by_key, candidate_reader_lanes) do
    requests = diagnostic_requests(dataset)
    physical_requests = diagnostic_physical_requests(dataset, requests)

    encoded =
      Map.new([1, 25, 100], fn page ->
        values =
          dataset.state_keys
          |> Enum.take(page)
          |> Enum.map(&Map.fetch!(encoded_by_key, &1))

        {page, values}
      end)

    [first_segment] = Map.fetch!(physical_requests, 1)

    readers =
      physical_requests
      |> Map.values()
      |> List.flatten()
      |> Enum.map(& &1.path)
      |> Enum.uniq()
      |> Map.new(fn path ->
        readers =
          Enum.map(1..candidate_reader_lanes, fn _lane ->
            {:ok, reader} = start_retained_segment_reader(path)
            reader
          end)

        {path, readers}
      end)

    Map.merge(dataset, %{
      diagnostic_encoded: encoded,
      diagnostic_requests: requests,
      diagnostic_physical_requests: physical_requests,
      diagnostic_readers: readers,
      diagnostic_segment: %{
        root: first_segment.root,
        index: first_segment.index,
        offset: first_segment.offset,
        encoded_size: first_segment.encoded_size,
        path: first_segment.path
      }
    })
  end

  defp diagnostic_physical_requests(dataset, requests) do
    root = apply_projection_root(dataset.ctx, 0)

    Map.new(requests, fn {page, page_requests} ->
      physical =
        page_requests
        |> Enum.group_by(fn {_state_key, locator} -> locator.file_id end)
        |> Enum.map(fn {{:waraft_apply_projection, index}, grouped} ->
          {:ok, {ordinal, offset, encoded_size}} =
            :ferricstore_waraft_spike_segment_log.location_for_index(root, index)

          %{
            root: root,
            index: index,
            ordinal: ordinal,
            offset: offset,
            encoded_size: encoded_size,
            path: Path.join([List.to_string(root), "segment_log", "#{ordinal}.seg"]),
            keys: Enum.map(grouped, &elem(&1, 0))
          }
        end)

      {page, physical}
    end)
  end

  defp diagnostic_segment_location(dataset) do
    %{root: root, index: index} = dataset.diagnostic_segment
    :ferricstore_waraft_spike_segment_log.location_for_index(root, index)
  end

  defp diagnostic_segment_read(dataset) do
    %{root: root, index: index, offset: offset, encoded_size: encoded_size} =
      dataset.diagnostic_segment

    :ferricstore_waraft_spike_segment_log.read_disk_at(root, index, offset, encoded_size)
  end

  defp candidate_verified_segment_read(dataset) do
    %{path: path} = dataset.diagnostic_segment

    with {:ok, fd} <- open_verified_segment(path) do
      try do
        candidate_segment_pread(fd, dataset.diagnostic_segment)
      after
        :ok = :file.close(fd)
      end
    end
  end

  defp candidate_retained_segment_read(dataset) do
    %{path: path} = dataset.diagnostic_segment
    reader = candidate_reader(dataset, path)
    reference = make_ref()
    send(reader, {:read, self(), reference, dataset.diagnostic_segment})

    receive do
      {^reference, result} -> result
    after
      10_000 -> {:error, :candidate_segment_read_timeout}
    end
  end

  defp candidate_retained_segment_read_many(reader, segments) do
    reference = make_ref()
    send(reader, {:read_many, self(), reference, segments})

    receive do
      {^reference, result} -> result
    after
      10_000 -> {:error, :candidate_segment_batch_read_timeout}
    end
  end

  defp candidate_coalesced_segment_read_many(reader, segments) do
    reference = make_ref()
    send(reader, {:read_many_coalesced, self(), reference, segments})

    receive do
      {^reference, result} -> result
    after
      10_000 -> {:error, :candidate_segment_batch_read_timeout}
    end
  end

  defp candidate_reader(dataset, path) do
    readers = Map.fetch!(dataset.diagnostic_readers, path)
    Enum.at(readers, :erlang.phash2(self(), length(readers)))
  end

  defp candidate_segment_pread(fd, %{index: index, offset: offset, encoded_size: encoded_size}) do
    with {:ok, framed} <- :file.pread(fd, offset, encoded_size) do
      candidate_segment_frame(framed, index, encoded_size)
    end
  end

  defp candidate_segment_frame(framed, index, encoded_size) do
    with true <- byte_size(framed) == encoded_size,
         <<length::unsigned-big-32, checksum::unsigned-big-32, payload::binary>> <- framed,
         true <- byte_size(payload) == length,
         true <- :erlang.crc32(payload) == checksum,
         false <- match?(<<131, 80, _compressed::binary>>, payload),
         {decoded, used} <- :erlang.binary_to_term(payload, [:safe, :used]),
         true <- used == byte_size(payload),
         {^index, {_term, _operation} = entry} <- decoded do
      {:ok, entry}
    else
      _invalid -> {:error, :invalid_candidate_segment_record}
    end
  rescue
    _error -> {:error, :invalid_candidate_segment_record}
  end

  defp candidate_segment_frames(fd, segments) do
    locations = Enum.map(segments, &{&1.offset, &1.encoded_size})

    with {:ok, frames} <- :file.pread(fd, locations),
         true <- length(frames) == length(segments) do
      {:ok,
       Enum.zip_with(frames, segments, fn framed, segment ->
         candidate_segment_frame(framed, segment.index, segment.encoded_size)
       end)}
    else
      _invalid -> {:error, :invalid_candidate_segment_batch}
    end
  end

  defp candidate_coalesced_segment_frames(fd, segments) do
    with {:ok, groups} <- candidate_adjacent_groups(segments),
         {:ok, entries} <- candidate_read_adjacent_groups(fd, groups, %{}) do
      {:ok, Enum.map(0..(length(segments) - 1), &Map.fetch!(entries, &1))}
    end
  end

  defp candidate_adjacent_groups(segments) when is_list(segments) and segments != [] do
    segments
    |> Enum.with_index()
    |> Enum.sort_by(fn {segment, _position} -> segment.offset end)
    |> Enum.reduce_while({:ok, []}, fn {segment, position}, {:ok, groups} ->
      item = {segment, position}

      case groups do
        [%{end_offset: end_offset, items: items} = group | rest]
        when segment.offset == end_offset ->
          next = %{
            group
            | end_offset: end_offset + segment.encoded_size,
              items: [item | items]
          }

          {:cont, {:ok, [next | rest]}}

        [%{end_offset: end_offset} | _rest] when segment.offset < end_offset ->
          {:halt, {:error, :overlapping_candidate_segment_frames}}

        _groups ->
          group = %{
            offset: segment.offset,
            end_offset: segment.offset + segment.encoded_size,
            items: [item]
          }

          {:cont, {:ok, [group | groups]}}
      end
    end)
    |> case do
      {:ok, groups} -> {:ok, Enum.reverse(groups)}
      {:error, _reason} = error -> error
    end
  end

  defp candidate_adjacent_groups(_segments), do: {:error, :invalid_candidate_segment_batch}

  defp candidate_read_adjacent_groups(_fd, [], entries), do: {:ok, entries}

  defp candidate_read_adjacent_groups(
         fd,
         [%{offset: offset, end_offset: end_offset, items: items} | groups],
         entries
       ) do
    bytes = end_offset - offset

    with {:ok, span} <- :file.pread(fd, offset, bytes),
         true <- byte_size(span) == bytes do
      entries =
        Enum.reduce(items, entries, fn {segment, position}, acc ->
          relative_offset = segment.offset - offset
          frame = binary_part(span, relative_offset, segment.encoded_size)

          Map.put(
            acc,
            position,
            candidate_segment_frame(frame, segment.index, segment.encoded_size)
          )
        end)

      candidate_read_adjacent_groups(fd, groups, entries)
    else
      _invalid -> {:error, :invalid_candidate_segment_batch}
    end
  end

  defp candidate_projection_values(
         {:ok, {0, {:ferricstore_segment_apply_projection_batch, _position, entries}}},
         keys
       )
       when is_list(entries) and is_list(keys) do
    keyset = MapSet.new(keys)

    Enum.reduce(entries, %{}, fn
      {key, value, _expire_at_ms}, values when is_binary(value) ->
        if MapSet.member?(keyset, key), do: Map.put(values, key, value), else: values

      _invalid, values ->
        values
    end)
  end

  defp candidate_projection_values(_entry, _keys),
    do: raise("invalid candidate apply-projection entry")

  defp open_verified_segment(path) do
    path_chars = to_charlist(path)

    with {:ok, %File.Stat{type: :regular} = before_open} <- File.lstat(path),
         {:ok, fd} <- :file.open(path_chars, [:read, :raw, :binary]) do
      case {:file.read_file_info(fd), File.lstat(path)} do
        {{:ok, open_record}, {:ok, %File.Stat{type: :regular} = after_open}} ->
          open = File.Stat.from_record(open_record)

          if same_file?(before_open, open) and same_file?(open, after_open) do
            {:ok, fd}
          else
            :ok = :file.close(fd)
            {:error, :segment_identity_changed}
          end

        _invalid ->
          :ok = :file.close(fd)
          {:error, :invalid_segment_file}
      end
    end
  end

  defp same_file?(left, right) do
    left.type == :regular and right.type == :regular and left.inode == right.inode and
      left.major_device == right.major_device and left.minor_device == right.minor_device
  end

  defp start_retained_segment_reader(path) do
    caller = self()
    reference = make_ref()

    pid =
      spawn_link(fn ->
        case open_verified_segment(path) do
          {:ok, fd} ->
            send(caller, {reference, self(), :ok})
            retained_segment_reader_loop(fd)

          {:error, reason} ->
            send(caller, {reference, self(), {:error, reason}})
        end
      end)

    receive do
      {^reference, ^pid, :ok} -> {:ok, pid}
      {^reference, ^pid, {:error, reason}} -> {:error, reason}
    after
      10_000 -> {:error, :candidate_segment_reader_start_timeout}
    end
  end

  defp retained_segment_reader_loop(fd) do
    receive do
      {:read, caller, reference, segment} when is_pid(caller) and is_reference(reference) ->
        send(caller, {reference, candidate_segment_pread(fd, segment)})
        retained_segment_reader_loop(fd)

      {:read_many, caller, reference, segments}
      when is_pid(caller) and is_reference(reference) and is_list(segments) ->
        send(caller, {reference, candidate_segment_frames(fd, segments)})
        retained_segment_reader_loop(fd)

      {:read_many_coalesced, caller, reference, segments}
      when is_pid(caller) and is_reference(reference) and is_list(segments) ->
        send(caller, {reference, candidate_coalesced_segment_frames(fd, segments)})
        retained_segment_reader_loop(fd)

      {:close, caller, reference} when is_pid(caller) and is_reference(reference) ->
        result = :file.close(fd)
        send(caller, {reference, result})
    end
  end

  defp close_diagnostic_segment(%{diagnostic_readers: readers}) do
    Enum.each(readers, fn {_path, path_readers} ->
      Enum.each(path_readers, fn reader ->
        reference = make_ref()
        send(reader, {:close, self(), reference})

        receive do
          {^reference, _result} -> :ok
        after
          1_000 -> :ok
        end
      end)
    end)
  end

  defp close_diagnostic_segment(_dataset), do: :ok

  defp validate_cold_cache_support!(false), do: :ok

  defp validate_cold_cache_support!(true) do
    unless :os.type() == {:unix, :linux} and QueryPerformance.command_available?("vmtouch") do
      raise "BENCH_STORAGE_COLD_CACHE=1 requires Linux and the vmtouch executable"
    end
  end

  defp run_cold_cache(datasets) do
    samples = QueryPerformance.int_env("BENCH_COLD_CACHE_SAMPLES", 5, min: 1)

    metrics =
      for {shape, dataset} <- datasets,
          page <- [25, 100],
          layout <- [:previous, :production],
          into: %{} do
        expected = previous_read(dataset, page)

        latencies =
          for _sample <- 1..samples do
            evict_storage_cache!(dataset, layout)

            {elapsed_ns, result} =
              QueryPerformance.timed_ns(fn -> storage_read(dataset, layout, page) end)

            ^expected = result
            elapsed_ns
          end

        summary = QueryPerformance.latency_summary(latencies)

        QueryPerformance.print_summary(
          "query storage cold #{layout} #{shape} page-#{page}",
          summary
        )

        metric =
          Map.merge(summary, %{
            "layout" => Atom.to_string(layout),
            "shape" => Atom.to_string(shape),
            "page_size" => page,
            "records" => dataset.record_count,
            "records_per_log_entry" => dataset.records_per_log_entry
          })

        {"#{layout}/#{shape}/page-#{page}", metric}
      end

    QueryPerformance.write_manual_metrics("query-storage-layout-cold", metrics)
  end

  defp storage_read(dataset, :previous, page), do: previous_read(dataset, page)
  defp storage_read(dataset, :production, page), do: production_read(dataset, page)

  defp evict_storage_cache!(dataset, :previous) do
    release_for_cold!(dataset.previous_path)
    evict_paths!([Path.join(dataset.previous_path, "data.mdb")])
  end

  defp evict_storage_cache!(dataset, :production) do
    release_for_cold!(dataset.production_path)
    :ok = DiskReader.invalidate(storage_root(dataset.ctx, 0))

    segment_paths =
      dataset.ctx
      |> storage_root(0)
      |> Path.join("apply_projection_log/segment_log/*.seg")
      |> Path.wildcard()

    evict_paths!([Path.join(dataset.production_path, "data.mdb") | segment_paths])
  end

  defp evict_paths!([]), do: raise("query storage cold-cache dataset has no files")

  defp evict_paths!(paths) do
    Enum.each(paths, fn path ->
      unless File.regular?(path), do: raise("cold-cache file is missing: #{path}")

      case System.cmd("vmtouch", ["-e", path], stderr_to_stdout: true) do
        {_output, 0} -> :ok
        {output, status} -> raise "vmtouch failed with status #{status}: #{output}"
      end
    end)
  end

  defp release_for_cold!(path, attempts \\ 20)

  defp release_for_cold!(_path, 0),
    do: raise("LMDB environment stayed busy during cold-cache eviction")

  defp release_for_cold!(path, attempts) do
    case Ferricstore.Bitcask.NIF.lmdb_release(path) do
      {:ok, _released} ->
        :ok

      {:busy, _leases} ->
        Process.sleep(10)
        release_for_cold!(path, attempts - 1)

      other ->
        raise "LMDB release failed before cold-cache eviction: #{inspect(other)}"
    end
  end

  defp apply_projection_root(ctx, shard_index) do
    ctx.data_dir
    |> Path.join("waraft")
    |> Path.join("ferricstore_waraft_backend.#{shard_index + 1}")
    |> Path.join("apply_projection_log")
    |> to_charlist()
  end

  defp storage_root(ctx, shard_index) do
    Path.join([
      ctx.data_dir,
      "waraft",
      "ferricstore_waraft_backend.#{shard_index + 1}"
    ])
  end

  defp logical_bytes(operations) do
    Enum.reduce(operations, 0, fn
      {operation, key, value}, bytes when operation in [:put, :put_new] ->
        bytes + byte_size(key) + byte_size(value)
    end)
  end

  defp percentage_decrease(previous, production) when previous > 0 do
    Float.round((previous - production) * 100.0 / previous, 2)
  end

  defp record(shape, ordinal) do
    id = "run-#{String.pad_leading(Integer.to_string(ordinal), 12, "0")}"

    base = %{
      id: id,
      type: if(rem(ordinal, 3) == 0, do: "invoice", else: "workflow"),
      state: Enum.at(["failed", "running", "completed"], rem(ordinal, 3)),
      version: ordinal,
      priority: rem(ordinal, 10),
      partition_key: "benchmark-tenant",
      created_at_ms: ordinal,
      updated_at_ms: ordinal + 1,
      lease_deadline_ms: ordinal + 10_000,
      attempts: rem(ordinal, 4)
    }

    enrich(base, shape, ordinal)
  end

  defp enrich(record, :sparse, _ordinal), do: record

  defp enrich(record, :typical, ordinal) do
    Map.merge(record, %{
      next_run_at_ms: ordinal + 500,
      run_state: "ready",
      max_active_ms: 30_000,
      root_flow_id: record.id,
      correlation_id: "correlation-#{ordinal}",
      payload_ref: :binary.copy("p", 64),
      result_ref: :binary.copy("r", 64),
      lease_owner: "worker-#{rem(ordinal, 64)}",
      lease_token: "lease-token-#{ordinal}",
      attributes: %{"customer" => "customer-#{ordinal}", "region" => "eu"},
      indexed_attributes: ["customer", "region"],
      state_meta: %{
        record.state => %{"entered_at_ms" => ordinal + 1, "worker" => "worker-1"}
      },
      indexed_state_meta: record.state
    })
  end

  defp enrich(record, :metadata_heavy, ordinal) do
    Map.merge(record, %{
      payload_ref: :binary.copy("p", 64),
      attributes:
        Map.new(1..16, fn index ->
          {"attr#{index}", :binary.copy("a", 64)}
        end),
      state_meta:
        Map.new(1..16, fn index ->
          {"state#{index}",
           %{
             "detail" => :binary.copy("m", 64),
             "entered_at_ms" => ordinal + index
           }}
        end)
    })
  end

  defp context(data_dir) do
    %{
      name: :query_storage_layout_benchmark,
      data_dir: data_dir,
      shard_count: 1,
      slot_map: List.to_tuple(List.duplicate(0, 1_024))
    }
  end

  defp lmdb_path(ctx) do
    ctx.data_dir
    |> Ferricstore.DataDir.shard_data_path(0)
    |> LMDB.path()
  end

  defp release(path) do
    _ = Ferricstore.Bitcask.NIF.lmdb_release(path)
    :ok
  end

  defp temp_root do
    Path.join(
      System.tmp_dir!(),
      "ferricstore-query-storage-layout-#{System.unique_integer([:positive, :monotonic])}"
    )
  end
end

Ferricstore.Bench.QueryStorageLayout.run()
