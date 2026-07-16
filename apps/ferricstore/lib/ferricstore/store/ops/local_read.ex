defmodule Ferricstore.Store.Ops.LocalRead do
  @moduledoc false

  alias Ferricstore.BatchResult
  alias Ferricstore.HLC
  alias Ferricstore.Store.BlobRef
  alias Ferricstore.Store.BlobValue
  alias Ferricstore.Store.ColdRead
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.LocalTxStore
  alias Ferricstore.Store.ReadResult
  alias Ferricstore.Store.Router
  alias Ferricstore.Store.Shard.ETS, as: ShardETS
  alias Ferricstore.Store.Shard.ZSetIndex

  @cold_read_error {:error, "ERR cold read failed"}
  @cold_read_timeout_ms 10_000

  defguardp valid_cold_location(file_id, offset, value_size)
            when is_integer(file_id) and file_id >= 0 and is_integer(offset) and offset >= 0 and
                   is_integer(value_size) and value_size >= 0

  defguardp valid_waraft_segment_location(file_id, offset, value_size)
            when is_tuple(file_id) and tuple_size(file_id) == 2 and
                   (elem(file_id, 0) == :waraft_segment or
                      elem(file_id, 0) == :waraft_projection or
                      elem(file_id, 0) == :waraft_apply_projection) and
                   is_integer(elem(file_id, 1)) and elem(file_id, 1) > 0 and
                   is_integer(offset) and offset >= 0 and is_integer(value_size) and
                   value_size >= 0

  def local?(%LocalTxStore{instance_ctx: nil}, _key), do: true

  def local?(%LocalTxStore{} = tx, key),
    do: Router.shard_for(tx.instance_ctx, key) == tx.shard_index

  def local_zset_index_read(%LocalTxStore{} = tx, redis_key, fun) do
    cond do
      not local?(tx, redis_key) ->
        :unavailable

      not tx_zset_index_clean?() ->
        :unavailable

      not local_zset_tables?(tx.shard_state) ->
        :unavailable

      true ->
        case local_zset_index_state(redis_key, tx) do
          {:ok, state} -> fun.(state)
          {:error, {:storage_read_failed, _reason}} = failure -> failure
        end
    end
  end

  def stored_value_size(value) when is_binary(value), do: byte_size(value)
  def stored_value_size(value) when is_integer(value), do: byte_size(Integer.to_string(value))
  def stored_value_size(value) when is_float(value), do: byte_size(Float.to_string(value))
  def stored_value_size(value), do: value |> to_string() |> byte_size()

  def range_from_value(value, start_idx, end_idx) when is_binary(value),
    do: slice_binary_range(value, start_idx, end_idx)

  def range_from_value(value, start_idx, end_idx) when is_integer(value),
    do: value |> Integer.to_string() |> slice_binary_range(start_idx, end_idx)

  def range_from_value(value, start_idx, end_idx) when is_float(value),
    do: value |> Float.to_string() |> slice_binary_range(start_idx, end_idx)

  def range_from_value(value, start_idx, end_idx),
    do: value |> to_string() |> slice_binary_range(start_idx, end_idx)

  def tx_deleted?(key) do
    deleted = Process.get(:tx_deleted_keys, MapSet.new())
    MapSet.member?(deleted, key)
  end

  def tx_pending_meta(key) do
    pending = Process.get(:tx_pending_values, %{})

    case Map.get(pending, key) do
      {value, 0} ->
        {value, 0}

      {value, exp} ->
        if exp > HLC.now_ms() do
          {value, exp}
        else
          tx_drop_pending(key)
          nil
        end

      nil ->
        nil
    end
  end

  def tx_put_pending(key, value, expire_at_ms) do
    pending = Process.get(:tx_pending_values, %{})
    Process.put(:tx_pending_values, Map.put(pending, key, {value, expire_at_ms}))
    tx_undelete(key)
  end

  def tx_drop_pending(key) do
    pending = Process.get(:tx_pending_values, %{})
    Process.put(:tx_pending_values, Map.delete(pending, key))
  end

  def tx_mark_deleted(key) do
    deleted = Process.get(:tx_deleted_keys, MapSet.new())
    Process.put(:tx_deleted_keys, MapSet.put(deleted, key))
  end

  def tx_undelete(key) do
    deleted = Process.get(:tx_deleted_keys, MapSet.new())

    if MapSet.member?(deleted, key) do
      Process.put(:tx_deleted_keys, MapSet.delete(deleted, key))
    end
  end

  def local_read_value(tx, key) do
    case tx_pending_meta(key) do
      {value, _exp} -> normalize_get_value(value)
      nil -> tx |> local_read_value_from_ets(key) |> normalize_get_value()
    end
  end

  def local_exists?(tx, key) do
    cond do
      tx_deleted?(key) ->
        false

      tx_pending_meta(key) != nil ->
        true

      true ->
        case ShardETS.ets_lookup(tx.shard_state, key) do
          {:hit, _value, _exp} -> true
          {:cold, _fid, _off, _vsize, _exp} -> true
          {:error, :invalid_keydir_entry} -> true
          :expired -> false
          :miss -> false
        end
    end
  end

  def local_read_meta(tx, key) do
    case tx_pending_meta(key) do
      {value, exp} -> {value, exp}
      nil -> local_read_meta_from_ets(tx, key)
    end
  end

  def local_batch_get(tx, keys) do
    {local_entries, remote_entries} =
      keys
      |> Enum.with_index()
      |> Enum.reduce({[], []}, fn {key, index}, {local_acc, remote_acc} ->
        if local?(tx, key) do
          if tx_deleted?(key),
            do: {local_acc, remote_acc},
            else: {[{index, key} | local_acc], remote_acc}
        else
          {local_acc, [{index, key} | remote_acc]}
        end
      end)

    results =
      local_entries
      |> Enum.reverse()
      |> local_batch_results(%{}, fn entries ->
        entries
        |> Enum.map(fn {_index, key} -> key end)
        |> then(&local_batch_read_values(tx, &1, tx.shard_state.shard_data_path))
      end)

    results =
      remote_entries
      |> Enum.reverse()
      |> local_batch_results(results, fn entries ->
        entries
        |> Enum.map(fn {_index, key} -> key end)
        |> then(&Router.batch_get(tx.instance_ctx, &1))
      end)

    keys
    |> Enum.with_index()
    |> Enum.map(fn {_key, index} -> Map.get(results, index) end)
  end

  def local_batch_read_values(tx, keys, data_path) do
    tx
    |> local_batch_read_meta(keys, data_path)
    |> Enum.map(fn
      {:error, {:storage_read_failed, _reason}} = failure -> failure
      {value, _exp} -> normalize_get_value(value)
      nil -> nil
    end)
  end

  def local_promoted_batch_read_values(tx, keys, dedicated_path) do
    local_promoted_batch_read(tx, keys, dedicated_path, &local_batch_read_values/3)
  end

  def local_promoted_batch_read_meta(tx, keys, dedicated_path) do
    local_promoted_batch_read(tx, keys, dedicated_path, &local_batch_read_meta/3)
  end

  def local_batch_read_meta(tx, keys, data_path) do
    now = HLC.now_ms()

    {warm_results, cold_reads} =
      keys
      |> Enum.with_index()
      |> Enum.reduce({%{}, []}, fn {key, index}, {results, cold} ->
        case tx_pending_meta(key) do
          {value, exp} ->
            {Map.put(results, index, {value, exp}), cold}

          nil ->
            local_batch_collect_ets(tx, key, index, data_path, now, results, cold)
        end
      end)

    results = local_batch_read_cold(tx, warm_results, Enum.reverse(cold_reads))

    keys
    |> Enum.with_index()
    |> Enum.map(fn {_key, index} -> Map.get(results, index) end)
  end

  def local_materialize_blob_value(tx, value) do
    BlobValue.maybe_materialize(
      tx.shard_state.data_dir,
      tx.shard_index,
      BlobValue.threshold(tx.instance_ctx),
      value
    )
  end

  def local_cold_value_size(tx, key, fid, off, vsize) do
    cond do
      valid_waraft_segment_location(fid, off, vsize) ->
        Router.value_size(tx.instance_ctx, key)

      BlobValue.threshold(tx.instance_ctx) > 0 and BlobRef.encoded_size?(vsize) ->
        path = ShardETS.file_path(tx.shard_state.shard_data_path, fid)

        case ColdRead.pread_keyed(path, off, key, @cold_read_timeout_ms) do
          {:ok, value} ->
            case BlobRef.decode(value) do
              {:ok, %BlobRef{size: logical_size}} -> logical_size
              :error -> vsize
            end

          {:error, reason} ->
            ReadResult.failure({:cold_read_failed, reason})
        end

      true ->
        vsize
    end
  end

  def local_set(tx, key, value, opts) do
    get? = Map.get(opts, :get, false)

    case local_set_current_meta(tx, key, get?) do
      {:error, _reason} = error ->
        error

      current ->
        {old_value, effective_expire} =
          case current do
            nil ->
              {nil, opts.expire_at_ms}

            {old_val, old_exp} ->
              {old_val, if(opts.keepttl, do: old_exp, else: opts.expire_at_ms)}
          end

        skip? =
          cond do
            opts.nx and current != nil -> true
            opts.xx and current == nil -> true
            true -> false
          end

        if skip? do
          if get?, do: old_value, else: nil
        else
          ShardETS.ets_insert(tx.shard_state, key, value, effective_expire)
          tx_put_pending(key, value, effective_expire)
          tx_undelete(key)
          send(self(), {:tx_pending_write, key, value, effective_expire})
          if get?, do: old_value, else: :ok
        end
    end
  end

  def local_read_meta_for_rmw(tx, key) do
    case tx_pending_meta(key) do
      {value, exp} -> {value, exp}
      nil -> local_read_meta_for_rmw_from_ets(tx, key)
    end
  end

  def local_read_value_for_rmw(tx, key) do
    case tx_pending_meta(key) do
      {value, _exp} -> value
      nil -> local_read_value_for_rmw_from_ets(tx, key)
    end
  end

  def promoted_path(%LocalTxStore{} = tx, redis_key) do
    case tx.shard_state.promoted_instances do
      %{^redis_key => %{path: path}} -> path
      _ -> nil
    end
  end

  def shared_log_compound_key?(<<"PM:", _rest::binary>>), do: true
  def shared_log_compound_key?(_key), do: false

  def tx_compound_write_message(tx, redis_key, compound_key, value, expire_at_ms) do
    case promoted_path(tx, redis_key) do
      nil ->
        {:tx_pending_write, compound_key, value, expire_at_ms}

      _dedicated_path ->
        if shared_log_compound_key?(compound_key) do
          {:tx_pending_write, compound_key, value, expire_at_ms}
        else
          {:tx_pending_compound_write, redis_key, compound_key, value, expire_at_ms}
        end
    end
  end

  def tx_compound_delete_message(tx, redis_key, compound_key) do
    case promoted_path(tx, redis_key) do
      nil ->
        {:tx_pending_delete, compound_key}

      _dedicated_path ->
        if shared_log_compound_key?(compound_key) do
          {:tx_pending_delete, compound_key}
        else
          {:tx_pending_compound_delete, redis_key, compound_key}
        end
    end
  end

  def local_promoted_read_value(tx, compound_key, dedicated_path) do
    case tx_pending_meta(compound_key) ||
           local_promoted_read_meta(tx, compound_key, dedicated_path) do
      {:error, {:storage_read_failed, _reason}} = failure -> failure
      {value, _exp} -> value
      nil -> nil
    end
  end

  def local_promoted_read_meta(tx, compound_key, dedicated_path) do
    case tx_pending_meta(compound_key) do
      nil -> local_promoted_read_meta_from_ets(tx, compound_key, dedicated_path)
      meta -> meta
    end
  end

  def merge_tx_pending_prefix(results, prefix) do
    deleted = Process.get(:tx_deleted_keys, MapSet.new())
    prefix_len = byte_size(prefix)
    now_ms = HLC.now_ms()

    base =
      results
      |> Enum.reject(fn {field, _value} -> MapSet.member?(deleted, prefix <> field) end)
      |> Map.new()

    Process.get(:tx_pending_values, %{})
    |> Enum.reduce(base, fn
      {key, {value, exp}}, acc when is_binary(key) and byte_size(key) >= prefix_len ->
        if String.starts_with?(key, prefix) and not MapSet.member?(deleted, key) and
             (exp == 0 or exp > now_ms) do
          field =
            case :binary.split(key, <<0>>) do
              [_pre, sub] -> sub
              _ -> key
            end

          Map.put(acc, field, value)
        else
          acc
        end

      _other, acc ->
        acc
    end)
    |> Map.to_list()
  end

  defp local_zset_index_state(redis_key, tx) do
    data_path = promoted_path(tx, redis_key) || tx.shard_state.shard_data_path
    prefix = CompoundKey.zset_prefix(redis_key)

    ZSetIndex.ensure(tx.shard_state, redis_key, prefix, data_path)
  end

  defp local_zset_tables?(%{zset_score_index: index, zset_score_lookup: lookup})
       when is_atom(index) and is_atom(lookup) do
    :ets.info(index) != :undefined and :ets.info(lookup) != :undefined
  end

  defp local_zset_tables?(_state), do: false

  defp tx_zset_index_clean? do
    map_size(Process.get(:tx_pending_values, %{})) == 0 and
      MapSet.size(Process.get(:tx_deleted_keys, MapSet.new())) == 0
  end

  defp slice_binary_range(value, start_idx, end_idx) do
    size = byte_size(value)
    start_norm = if start_idx < 0, do: max(size + start_idx, 0), else: start_idx
    end_norm = if end_idx < 0, do: size + end_idx, else: end_idx

    start_clamped = min(start_norm, size)
    end_clamped = min(end_norm, size - 1)

    if start_clamped > end_clamped do
      ""
    else
      binary_part(value, start_clamped, end_clamped - start_clamped + 1)
    end
  end

  defp local_read_value_from_ets(tx, key) do
    case ShardETS.ets_lookup_warm_result(tx.shard_state, key) do
      {:hit, value, _exp} -> value
      :expired -> nil
      :miss -> nil
      {:error, reason} -> ReadResult.failure(reason)
    end
  end

  defp local_read_meta_from_ets(tx, key) do
    case ShardETS.ets_lookup_warm_result(tx.shard_state, key) do
      {:hit, value, exp} -> {value, exp}
      :expired -> nil
      :miss -> nil
      {:error, reason} -> ReadResult.failure(reason)
    end
  end

  defp local_batch_results([], results, _read_fun), do: results

  defp local_batch_results(entries, results, read_fun) do
    merge_batch_results(entries, read_fun.(entries), results, &elem(&1, 0))
  end

  @doc false
  def __merge_batch_results_for_test__(indexes, results, backend_results),
    do: merge_batch_results(indexes, backend_results, results, & &1)

  defp merge_batch_results(expected, backend_results, results, index_fun) do
    case BatchResult.map_exact(expected, backend_results, fn entry, value ->
           {index_fun.(entry), value}
         end) do
      {:ok, indexed_values} ->
        Enum.reduce(indexed_values, results, fn {index, value}, acc ->
          Map.put(acc, index, value)
        end)

      {:error, reason} ->
        failure = ReadResult.failure(reason)

        Enum.reduce(expected, results, fn entry, acc ->
          Map.put(acc, index_fun.(entry), failure)
        end)
    end
  end

  defp normalize_get_value(nil), do: nil
  defp normalize_get_value(value) when is_integer(value), do: Integer.to_string(value)

  defp normalize_get_value(value) when is_float(value),
    do: Ferricstore.Store.ValueCodec.format_float(value)

  defp normalize_get_value(value), do: value

  defp local_promoted_batch_read(tx, keys, dedicated_path, read_fun) do
    {shared_entries, dedicated_entries} =
      keys
      |> Enum.with_index()
      |> Enum.split_with(fn {key, _index} -> shared_log_compound_key?(key) end)

    %{}
    |> local_promoted_batch_partition(
      tx,
      shared_entries,
      tx.shard_state.shard_data_path,
      read_fun
    )
    |> local_promoted_batch_partition(tx, dedicated_entries, dedicated_path, read_fun)
    |> then(fn results ->
      keys
      |> Enum.with_index()
      |> Enum.map(fn {_key, index} -> Map.get(results, index) end)
    end)
  end

  defp local_promoted_batch_partition(results, _tx, [], _data_path, _read_fun), do: results

  defp local_promoted_batch_partition(results, tx, entries, data_path, read_fun) do
    partition_keys = Enum.map(entries, fn {key, _index} -> key end)

    merge_batch_results(
      entries,
      read_fun.(tx, partition_keys, data_path),
      results,
      &elem(&1, 1)
    )
  end

  defp local_batch_collect_ets(tx, key, index, data_path, now, results, cold) do
    case ShardETS.ets_lookup_metadata(tx.shard_state, key, now) do
      {:live, {^key, value, exp, _lfu, _fid, _off, _vsize}, :hot} ->
        {Map.put(results, index, {value, exp}), cold}

      {:live, {^key, nil, exp, _lfu, fid, off, vsize}, :cold}
      when valid_cold_location(fid, off, vsize) ->
        path = ShardETS.file_path(data_path, fid)
        {results, [{index, key, path, fid, off, vsize, exp} | cold]}

      {:live, {^key, nil, _exp, _lfu, fid, _off, _vsize}, :cold} ->
        {Map.put(
           results,
           index,
           ReadResult.failure({:unsupported_local_cold_location, fid})
         ), cold}

      {:live, _entry, :pending} ->
        {Map.put(results, index, ReadResult.failure(:pending_cold_write)), cold}

      {:live, _entry, :invalid} ->
        {Map.put(results, index, ReadResult.failure(:invalid_keydir_entry)), cold}

      {:error, :invalid_keydir_entry} ->
        {Map.put(results, index, ReadResult.failure(:invalid_keydir_entry)), cold}

      result when result in [:expired, :miss] ->
        {results, cold}
    end
  rescue
    ArgumentError ->
      {Map.put(results, index, ReadResult.failure(:keydir_unavailable)), cold}
  end

  defp local_batch_read_cold(_tx, results, []), do: results

  defp local_batch_read_cold(tx, results, cold_reads) do
    {unique_reads, fanout_indexes} = dedupe_local_batch_cold_reads(cold_reads)
    unique_results = read_unique_local_batch_cold(tx, unique_reads)

    Enum.reduce(fanout_indexes, results, fn {original_index, unique_index}, acc ->
      case Map.fetch(unique_results, unique_index) do
        {:ok, value} -> Map.put(acc, original_index, value)
        :error -> acc
      end
    end)
  end

  defp dedupe_local_batch_cold_reads(cold_reads) do
    {unique_reads, _index_by_location, fanout_indexes} =
      Enum.reduce(cold_reads, {[], %{}, []}, fn read, {unique_acc, index_acc, fanout_acc} ->
        original_index = elem(read, 0)
        location = local_batch_cold_read_location(read)

        case Map.fetch(index_acc, location) do
          {:ok, unique_index} ->
            {unique_acc, index_acc, [{original_index, unique_index} | fanout_acc]}

          :error ->
            unique_index = map_size(index_acc)
            unique_read = put_elem(read, 0, unique_index)

            {[unique_read | unique_acc], Map.put(index_acc, location, unique_index),
             [{original_index, unique_index} | fanout_acc]}
        end
      end)

    {Enum.reverse(unique_reads), Enum.reverse(fanout_indexes)}
  end

  defp local_batch_cold_read_location({_index, key, path, _fid, off, _vsize, _exp}) do
    {path, off, key}
  end

  defp read_unique_local_batch_cold(tx, cold_reads) do
    reads =
      Enum.map(cold_reads, fn {_index, key, path, fid, off, vsize, exp} ->
        {path, off, key, {Path.dirname(path), exp, fid, off, vsize}}
      end)

    case ColdRead.pread_batch_keyed_current(
           reads,
           fn key, token -> resolve_local_current_cold(tx, key, token) end,
           @cold_read_timeout_ms
         ) do
      {:ok, current_results}
      when is_list(current_results) and length(current_results) == length(cold_reads) ->
        values = Enum.map(current_results, &local_current_read_value/1)
        materialized_values = local_materialize_blob_values(tx, values)

        [cold_reads, current_results, materialized_values]
        |> Enum.zip()
        |> Enum.reduce(%{}, fn
          {{index, key, _path, _fid, _off, _vsize, _exp},
           {:value, _value, {_data_path, exp, fid, off, vsize}}, {:ok, materialized}},
          acc ->
            ShardETS.cold_read_warm_ets(tx.shard_state, key, materialized, exp, fid, off, vsize)
            Map.put(acc, index, {materialized, exp})

          {{index, _key, path, _fid, _off, _vsize, _exp}, _current_result, {:error, reason}},
          acc ->
            ColdRead.emit_pread_error(path, reason)
            Map.put(acc, index, ReadResult.failure({:cold_read_failed, reason}))

          {_read, _current_result, _missing_or_error}, acc ->
            acc
        end)

      {:ok, _bad_results} ->
        emit_local_batch_cold_errors(cold_reads, :batch_result_length_mismatch)

        Map.new(cold_reads, fn {index, _key, _path, _fid, _off, _vsize, _exp} ->
          {index, ReadResult.failure({:cold_read_failed, :batch_result_length_mismatch})}
        end)
    end
  end

  defp local_current_read_value({:value, value, _token}), do: value
  defp local_current_read_value({:error, reason}), do: {:error, reason}
  defp local_current_read_value(:missing), do: nil

  defp resolve_local_current_cold(tx, key, {data_path, _exp, _fid, _off, _vsize}) do
    case ShardETS.ets_lookup_metadata(tx.shard_state, key) do
      {:live, {^key, value, exp, _lfu, fid, off, vsize}, :hot} when is_binary(value) ->
        {:hot, value, {data_path, exp, fid, off, vsize}}

      {:live, {^key, nil, exp, _lfu, fid, off, vsize}, :cold}
      when valid_cold_location(fid, off, vsize) ->
        {:cold, ShardETS.file_path(data_path, fid), off, {data_path, exp, fid, off, vsize}}

      {:live, _entry, :invalid} ->
        {:error, :invalid_keydir_entry}

      {:live, _entry, :pending} ->
        {:error, :pending_cold_write}

      {:live, _entry, _location} ->
        {:error, :unsupported_local_cold_location}

      {:error, :invalid_keydir_entry} ->
        {:error, :invalid_keydir_entry}

      result when result in [:expired, :miss] ->
        :missing
    end
  rescue
    ArgumentError -> {:error, :keydir_unavailable}
  end

  defp local_materialize_blob_values(tx, values) do
    {binary_values, indexed_results} =
      values
      |> Enum.with_index()
      |> Enum.reduce({[], %{}}, fn
        {value, index}, {binary_values, indexed_results} when is_binary(value) ->
          {[{index, value} | binary_values], indexed_results}

        {{:error, reason}, index}, {binary_values, indexed_results} ->
          {binary_values, Map.put(indexed_results, index, {:error, reason})}

        {_unexpected, index}, {binary_values, indexed_results} ->
          {binary_values, Map.put(indexed_results, index, :skip)}
      end)

    indexed_results =
      if binary_values == [] do
        indexed_results
      else
        ordered_values = Enum.reverse(binary_values)

        ordered_values
        |> Enum.map(fn {_index, value} -> value end)
        |> then(fn values ->
          BlobValue.maybe_materialize_many(
            tx.shard_state.data_dir,
            tx.shard_index,
            BlobValue.threshold(tx.instance_ctx),
            values
          )
        end)
        |> then(fn materialized -> Enum.zip(ordered_values, materialized) end)
        |> Enum.reduce(indexed_results, fn {{index, _value}, result}, acc ->
          Map.put(acc, index, result)
        end)
      end

    values
    |> Enum.with_index()
    |> Enum.map(fn {_value, index} -> Map.fetch!(indexed_results, index) end)
  end

  defp emit_local_batch_cold_errors(cold_reads, reason) do
    cold_reads
    |> Enum.reduce(%{}, fn {_index, _key, path, _fid, _off, _vsize, _exp}, acc ->
      Map.update(acc, path, 1, &(&1 + 1))
    end)
    |> Enum.each(fn {path, count} -> ColdRead.emit_pread_error(path, reason, count) end)
  end

  defp local_set_current_meta(tx, key, true) do
    case local_read_meta_for_rmw(tx, key) do
      {nil, _expire_at_ms} -> nil
      result -> result
    end
  end

  defp local_set_current_meta(tx, key, false) do
    if tx_deleted?(key) do
      nil
    else
      case tx_pending_meta(key) do
        {_value, _exp} = pending -> pending_expire_meta(pending)
        nil -> ets_expire_meta(tx, key)
      end
    end
  end

  defp pending_expire_meta({_value, exp}), do: {nil, exp}

  defp ets_expire_meta(tx, key) do
    case ShardETS.ets_lookup_metadata(tx.shard_state, key) do
      {:live, {^key, _value, exp, _lfu, _fid, _off, _vsize}, location}
      when location in [:hot, :cold, :pending] ->
        {nil, exp}

      {:live, _entry, :invalid} ->
        @cold_read_error

      {:error, :invalid_keydir_entry} ->
        @cold_read_error

      :expired ->
        nil

      :miss ->
        nil
    end
  end

  defp local_read_meta_for_rmw_from_ets(tx, key) do
    case ShardETS.ets_lookup_warm_result(tx.shard_state, key) do
      {:hit, value, exp} -> {value, exp}
      :expired -> {nil, 0}
      :miss -> {nil, 0}
      {:error, :cold_read_failed} -> @cold_read_error
    end
  end

  defp local_read_value_for_rmw_from_ets(tx, key) do
    case ShardETS.ets_lookup_warm_result(tx.shard_state, key) do
      {:hit, value, _exp} -> value
      :expired -> nil
      :miss -> nil
      {:error, :cold_read_failed} -> @cold_read_error
    end
  end

  defp local_promoted_read_meta_from_ets(tx, compound_key, dedicated_path) do
    case ShardETS.ets_lookup_metadata(tx.shard_state, compound_key) do
      {:live, {^compound_key, value, exp, _lfu, _fid, _off, _vsize}, :hot} ->
        {value, exp}

      {:live, {^compound_key, nil, exp, _lfu, fid, off, vsize}, :cold}
      when valid_cold_location(fid, off, vsize) ->
        read_promoted_cold_value(tx, compound_key, dedicated_path, fid, off, vsize, exp)

      {:live, {^compound_key, nil, _exp, _lfu, fid, _off, _vsize}, :cold} ->
        ReadResult.failure({:unsupported_local_cold_location, fid})

      {:live, _entry, :pending} ->
        ReadResult.failure(:pending_cold_write)

      {:live, _entry, :invalid} ->
        ReadResult.failure(:invalid_keydir_entry)

      {:error, :invalid_keydir_entry} ->
        ReadResult.failure(:invalid_keydir_entry)

      result when result in [:expired, :miss] ->
        nil
    end
  rescue
    ArgumentError -> ReadResult.failure(:keydir_unavailable)
  end

  defp read_promoted_cold_value(tx, compound_key, dedicated_path, fid, off, vsize, exp) do
    path = ShardETS.file_path(dedicated_path, fid)

    case read_cold_async(path, off, compound_key) do
      {:ok, value} ->
        case local_materialize_blob_value(tx, value) do
          {:ok, materialized} ->
            ShardETS.cold_read_warm_ets(
              tx.shard_state,
              compound_key,
              materialized,
              exp,
              fid,
              off,
              vsize
            )

            {materialized, exp}

          {:error, reason} ->
            ColdRead.emit_pread_error(path, reason)
            ReadResult.failure({:cold_read_failed, reason})
        end

      {:error, reason} ->
        ReadResult.failure({:cold_read_failed, reason})

      invalid ->
        ReadResult.failure({:cold_read_failed, {:invalid_read_result, invalid}})
    end
  end

  defp read_cold_async(path, offset, key) do
    ColdRead.pread_keyed(path, offset, key, @cold_read_timeout_ms)
  end
end
