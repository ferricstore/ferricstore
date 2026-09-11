defmodule Ferricstore.Flow.LMDBRebuilder.ColdState do
  @moduledoc false

  alias Ferricstore.Flow.{Codec, Locator, ProjectionLocator}
  alias Ferricstore.Raft.WARaftSegmentReader
  alias Ferricstore.Store.BlobValue
  alias Ferricstore.Store.ColdRead
  alias Ferricstore.Store.Shard.ETS, as: ShardETS

  @cold_read_timeout_ms 30_000

  defguardp valid_waraft_segment_location(file_id, offset, value_size)
            when is_tuple(file_id) and tuple_size(file_id) == 2 and
                   (elem(file_id, 0) == :waraft_segment or
                      elem(file_id, 0) == :waraft_projection or
                      elem(file_id, 0) == :waraft_apply_projection) and
                   is_integer(elem(file_id, 1)) and elem(file_id, 1) > 0 and
                   is_integer(offset) and offset >= 0 and is_integer(value_size) and
                   value_size >= 0

  def read_and_decode(entries, shard_path), do: read_and_decode(entries, shard_path, nil, nil)

  def read_and_decode(entries, shard_path, shard_index, instance_ctx) do
    {hot, cold} =
      Enum.split_with(entries, fn
        {_key, value, _expire_at_ms, _lfu, _fid, _off, _vsize} when is_binary(value) -> true
        _entry -> false
      end)

    hot_decoded = decode_hot_entries(hot, shard_index, instance_ctx)

    cold_decoded =
      cold
      |> cold_locations(shard_path)
      |> read_cold_locations(shard_index, instance_ctx)

    hot_decoded ++ cold_decoded
  end

  defp decode_hot_entries(entries, shard_index, instance_ctx) do
    physical_locations = hot_waraft_physical_locations(entries, shard_index, instance_ctx)

    Enum.flat_map(entries, fn {key, value, expire_at_ms, _lfu, fid, off, vsize} ->
      case Map.get(physical_locations, fid, :direct) do
        {:ok, physical_location} ->
          key
          |> decode_state_record(
            value,
            expire_at_ms,
            shard_index,
            instance_ctx,
            {fid, off, vsize}
          )
          |> physicalize_decoded_rows(physical_location)

        {:error, reason} ->
          observe_cold_read_error(1, {:waraft_segment_location_failed, reason})
          []

        :direct ->
          decode_state_record(
            key,
            value,
            expire_at_ms,
            shard_index,
            instance_ctx,
            {fid, off, vsize}
          )
      end
    end)
  end

  defp hot_waraft_physical_locations(_entries, shard_index, instance_ctx)
       when not is_map(instance_ctx) or not is_integer(shard_index) or shard_index < 0,
       do: %{}

  defp hot_waraft_physical_locations(entries, shard_index, instance_ctx) do
    entries
    |> Enum.reduce(%{}, fn
      {key, _value, _expire_at_ms, _lfu, file_id, offset, value_size}, acc
      when is_binary(key) and valid_waraft_segment_location(file_id, offset, value_size) ->
        Map.update(acc, file_id, [key], &[key | &1])

      _entry, acc ->
        acc
    end)
    |> Map.new(fn {file_id, reversed_keys} ->
      keys = Enum.reverse(reversed_keys)
      {file_id, physical_waraft_group_location(instance_ctx, shard_index, file_id, keys)}
    end)
  end

  def read_cold_locations([], _shard_index, _instance_ctx), do: []

  def read_cold_locations(locations, shard_index, instance_ctx) do
    {bitcask_locations, waraft_locations} =
      Enum.split_with(locations, fn
        {:bitcask, _path, _key, _expire_at_ms, _source_location} -> true
        _ -> false
      end)

    read_bitcask_cold_locations(bitcask_locations, shard_index, instance_ctx) ++
      read_waraft_cold_locations(waraft_locations, shard_index, instance_ctx)
  end

  def decode_state_record(key, value, expire_at_ms, shard_index, instance_ctx) do
    decode_state_record(key, value, expire_at_ms, shard_index, instance_ctx, nil)
  end

  def decode_state_record(
        key,
        value,
        expire_at_ms,
        shard_index,
        instance_ctx,
        source_location
      ) do
    case materialize_rebuilt_value(value, shard_index, instance_ctx) do
      {:ok, materialized_value} ->
        case decode_source_record(
               key,
               materialized_value,
               expire_at_ms,
               source_location
             ) do
          {:ok, %{id: id, type: type, state: state} = record, locator}
          when is_binary(id) and is_binary(type) and is_binary(state) ->
            [{key, materialized_value, expire_at_ms, record, locator}]

          {:error, reason} ->
            observe_cold_read_error(1, reason)
            []

          _ ->
            observe_cold_read_error(1, :invalid_flow_state_record)
            []
        end

      {:error, reason} ->
        observe_cold_read_error(1, {:blob_materialize_failed, reason})
        []
    end
  rescue
    _ ->
      observe_cold_read_error(1, :flow_state_decode_failed)
      []
  end

  def cold_locations_for_state(shard_path, state_key, expire_at_ms, fid, off, vsize) do
    cold_locations([{state_key, nil, expire_at_ms, nil, fid, off, vsize}], shard_path)
  end

  def publish_mirror_health(instance_ctx, shard_index, stats) do
    degraded? = stats.lmdb_errors > 0 or Map.get(stats, :cold_read_errors, 0) > 0
    flag_idx = shard_index + 1

    case Map.get(instance_ctx || %{}, :flow_lmdb_mirror_degraded) do
      ref when is_reference(ref) ->
        if flag_idx <= :atomics.info(ref).size do
          :atomics.put(ref, flag_idx, if(degraded?, do: 1, else: 0))
        end

      _ ->
        :ok
    end
  rescue
    _ -> :ok
  end

  defp cold_locations(entries, shard_path) do
    Enum.flat_map(entries, fn
      {key, nil, expire_at_ms, _lfu, fid, off, vsize}
      when is_integer(fid) and fid >= 0 and is_integer(off) and is_integer(vsize) and off >= 0 and
             vsize >= 0 ->
        path = ShardETS.file_path(shard_path, fid)
        [{:bitcask, path, key, expire_at_ms, {fid, off, vsize}}]

      {key, nil, expire_at_ms, _lfu, fid, off, vsize}
      when valid_waraft_segment_location(fid, off, vsize) ->
        [{:waraft, fid, key, expire_at_ms, {fid, off, vsize}}]

      _entry ->
        observe_cold_read_error(1, :source_location_unavailable)
        []
    end)
  end

  defp read_bitcask_cold_locations([], _shard_index, _instance_ctx), do: []

  defp read_bitcask_cold_locations(locations, shard_index, instance_ctx) do
    reads =
      Enum.map(locations, fn
        {:bitcask, path, key, _expire_at_ms, {_fid, off, _vsize}} -> {path, off, key}
      end)

    case ColdRead.pread_batch_keyed(reads, @cold_read_timeout_ms) do
      {:ok, values} ->
        locations
        |> Enum.zip(values)
        |> Enum.flat_map(fn
          {{:bitcask, _path, key, expire_at_ms, source_location}, value}
          when is_binary(value) ->
            decode_state_record(
              key,
              value,
              expire_at_ms,
              shard_index,
              instance_ctx,
              source_location
            )

          _ ->
            observe_cold_read_error(1, :missing_value)
            []
        end)

      {:error, reason} ->
        observe_cold_read_error(length(locations), reason)
        []
    end
  end

  defp read_waraft_cold_locations([], _shard_index, _instance_ctx), do: []

  defp read_waraft_cold_locations(locations, shard_index, instance_ctx) do
    locations
    |> Enum.group_by(fn {:waraft, file_id, _key, _expire_at_ms, _source_location} ->
      file_id
    end)
    |> Enum.flat_map(fn {file_id, grouped} ->
      keys =
        Enum.map(grouped, fn {:waraft, _file_id, key, _expire_at_ms, _source_location} ->
          key
        end)

      case read_physical_waraft_group(instance_ctx, shard_index, file_id, keys) do
        {:ok, values_by_key, physical_location} when is_map(values_by_key) ->
          Enum.flat_map(grouped, fn
            {:waraft, _file_id, key, expire_at_ms, source_location} ->
              case Map.get(values_by_key, key) do
                value when is_binary(value) ->
                  key
                  |> decode_state_record(
                    value,
                    expire_at_ms,
                    shard_index,
                    instance_ctx,
                    source_location
                  )
                  |> physicalize_decoded_rows(physical_location)

                _ ->
                  observe_cold_read_error(1, :missing_waraft_value)
                  []
              end
          end)

        {:error, reason} ->
          observe_cold_read_error(length(grouped), {:waraft_segment_read_failed, reason})
          []
      end
    end)
  end

  defp read_physical_waraft_group(instance_ctx, shard_index, file_id, keys) do
    with {:ok, physical_location} <-
           physical_waraft_group_location(instance_ctx, shard_index, file_id, keys),
         {:ok, values_by_key} <-
           WARaftSegmentReader.read_values_from_location(
             instance_ctx,
             shard_index,
             file_id,
             keys
           ) do
      {:ok, values_by_key, physical_location}
    end
  end

  defp physical_waraft_group_location(instance_ctx, shard_index, file_id, keys) do
    with :ok <- ensure_waraft_group_durable(instance_ctx, shard_index, file_id, keys),
         {:ok, physical_location} <-
           WARaftSegmentReader.physical_location(instance_ctx, shard_index, file_id) do
      {:ok, physical_location}
    end
  end

  defp ensure_waraft_group_durable(
         %{data_dir: data_dir},
         shard_index,
         {:waraft_apply_projection, index},
         keys
       )
       when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
              is_integer(index) and index > 0 do
    case WARaftSegmentReader.ensure_apply_projection_entries_durable(
           data_dir,
           shard_index,
           Enum.map(keys, &{index, &1})
         ) do
      {:ok, _removed} -> :ok
      {:error, reason} -> {:error, {:query_row_source_not_durable, reason}}
    end
  end

  defp ensure_waraft_group_durable(_instance_ctx, _shard_index, _file_id, _keys), do: :ok

  defp physicalize_decoded_rows(rows, {ordinal, offset, frame_size})
       when is_integer(ordinal) and ordinal >= 0 and is_integer(offset) and offset >= 0 and
              is_integer(frame_size) and frame_size >= 8 do
    Enum.flat_map(rows, fn
      {key, value, expire_at_ms, record, %Locator{} = locator} ->
        case Locator.relocate(locator,
               segment_generation: ordinal,
               offset: offset,
               frame_size: frame_size
             ) do
          {:ok, physical} ->
            if Locator.hydration_ready?(physical) do
              [{key, value, expire_at_ms, record, physical}]
            else
              observe_cold_read_error(1, :invalid_query_row_source_locator)
              []
            end

          {:error, reason} ->
            observe_cold_read_error(1, {:query_row_source_location_unavailable, reason})
            []
        end

      _invalid ->
        observe_cold_read_error(1, :invalid_flow_state_record)
        []
    end)
  end

  defp physicalize_decoded_rows(rows, _invalid_location) do
    observe_cold_read_error(length(rows), :invalid_query_row_source_location)
    []
  end

  defp observe_cold_read_error(count, reason) do
    previous = Process.get(:flow_lmdb_rebuild_cold_read_errors, 0)
    Process.put(:flow_lmdb_rebuild_cold_read_errors, previous + count)

    :telemetry.execute(
      [:ferricstore, :flow, :lmdb_rebuild, :cold_read_error],
      %{count: count},
      %{reason: reason}
    )
  end

  defp decode_source_record(_key, materialized_value, _expire_at_ms, nil) do
    case Codec.decode_record(materialized_value) do
      record when is_map(record) -> {:ok, record, nil}
    end
  end

  defp decode_source_record(key, materialized_value, expire_at_ms, source_location),
    do: ProjectionLocator.decode_source(key, materialized_value, expire_at_ms, source_location)

  defp materialize_rebuilt_value(value, shard_index, %{data_dir: data_dir} = instance_ctx)
       when is_binary(value) and is_binary(data_dir) and is_integer(shard_index) and
              shard_index >= 0 do
    BlobValue.maybe_materialize(
      data_dir,
      shard_index,
      BlobValue.threshold(instance_ctx),
      value
    )
  end

  defp materialize_rebuilt_value(value, _shard_index, _instance_ctx) when is_binary(value),
    do: {:ok, value}
end
