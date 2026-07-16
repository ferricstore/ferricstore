defmodule Ferricstore.Flow.LMDBRebuilder.ColdState do
  @moduledoc false

  alias Ferricstore.Flow.Codec
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

    hot_decoded =
      Enum.flat_map(hot, fn {key, value, expire_at_ms, _lfu, _fid, _off, _vsize} ->
        decode_state_record(key, value, expire_at_ms, shard_index, instance_ctx)
      end)

    cold_decoded =
      cold
      |> cold_locations(shard_path)
      |> read_cold_locations(shard_index, instance_ctx)

    hot_decoded ++ cold_decoded
  end

  def read_cold_locations([], _shard_index, _instance_ctx), do: []

  def read_cold_locations(locations, shard_index, instance_ctx) do
    {bitcask_locations, waraft_locations} =
      Enum.split_with(locations, fn
        {:bitcask, _path, _off, _key, _expire_at_ms} -> true
        _ -> false
      end)

    read_bitcask_cold_locations(bitcask_locations, shard_index, instance_ctx) ++
      read_waraft_cold_locations(waraft_locations, shard_index, instance_ctx)
  end

  def decode_state_record(key, value, expire_at_ms, shard_index, instance_ctx) do
    case materialize_rebuilt_value(value, shard_index, instance_ctx) do
      {:ok, materialized_value} ->
        case Codec.decode_record(materialized_value) do
          %{id: id, type: type, state: state} = record
          when is_binary(id) and is_binary(type) and is_binary(state) ->
            [{key, materialized_value, expire_at_ms, record}]

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
      when is_integer(fid) and is_integer(off) and is_integer(vsize) and off >= 0 and vsize >= 0 ->
        path = ShardETS.file_path(shard_path, fid)
        [{:bitcask, path, off, key, expire_at_ms}]

      {key, nil, expire_at_ms, _lfu, fid, off, vsize}
      when valid_waraft_segment_location(fid, off, vsize) ->
        [{:waraft, fid, key, expire_at_ms}]

      _entry ->
        []
    end)
  end

  defp read_bitcask_cold_locations([], _shard_index, _instance_ctx), do: []

  defp read_bitcask_cold_locations(locations, shard_index, instance_ctx) do
    reads =
      Enum.map(locations, fn {:bitcask, path, off, key, _expire_at_ms} -> {path, off, key} end)

    case ColdRead.pread_batch_keyed(reads, @cold_read_timeout_ms) do
      {:ok, values} ->
        locations
        |> Enum.zip(values)
        |> Enum.flat_map(fn
          {{:bitcask, _path, _off, key, expire_at_ms}, value} when is_binary(value) ->
            decode_state_record(key, value, expire_at_ms, shard_index, instance_ctx)

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
    |> Enum.group_by(fn {:waraft, file_id, _key, _expire_at_ms} -> file_id end)
    |> Enum.flat_map(fn {file_id, grouped} ->
      keys = Enum.map(grouped, fn {:waraft, _file_id, key, _expire_at_ms} -> key end)

      case WARaftSegmentReader.read_values_from_location(instance_ctx, shard_index, file_id, keys) do
        {:ok, values_by_key} when is_map(values_by_key) ->
          Enum.flat_map(grouped, fn {:waraft, _file_id, key, expire_at_ms} ->
            case Map.get(values_by_key, key) do
              value when is_binary(value) ->
                decode_state_record(key, value, expire_at_ms, shard_index, instance_ctx)

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

  defp observe_cold_read_error(count, reason) do
    previous = Process.get(:flow_lmdb_rebuild_cold_read_errors, 0)
    Process.put(:flow_lmdb_rebuild_cold_read_errors, previous + count)

    :telemetry.execute(
      [:ferricstore, :flow, :lmdb_rebuild, :cold_read_error],
      %{count: count},
      %{reason: reason}
    )
  end

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
