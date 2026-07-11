defmodule Ferricstore.Store.Shard.Reads do
  @moduledoc "Shard read-path handlers: ETS hot lookup, cold-key pread from Bitcask, exists check, and key enumeration."

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Flow.InternalKey
  alias Ferricstore.HLC
  alias Ferricstore.Store.{BlobValue, ColdRead}
  alias Ferricstore.Store.Shard.ETS, as: ShardETS
  alias Ferricstore.Store.Shard.Flush, as: ShardFlush

  @cold_read_timeout_ms 10_000
  @max_get_many_keys 512
  @max_key_size 65_535
  @max_get_many_key_bytes 1_048_576

  defguardp valid_cold_location(file_id, offset, value_size)
            when is_integer(file_id) and file_id >= 0 and is_integer(offset) and offset >= 0 and
                   is_integer(value_size) and value_size >= 0

  defguardp valid_waraft_segment_location(file_id, offset, value_size)
            when is_tuple(file_id) and tuple_size(file_id) == 2 and
                   (elem(file_id, 0) == :waraft_segment or
                      elem(file_id, 0) == :waraft_projection or
                      elem(file_id, 0) == :waraft_apply_projection) and
                   is_integer(elem(file_id, 1)) and elem(file_id, 1) > 0 and
                   is_integer(offset) and offset >= 0 and
                   is_integer(value_size) and value_size >= 0

  # -------------------------------------------------------------------
  # Read-path handlers (return {:reply, result, state})
  # -------------------------------------------------------------------

  @spec handle_get(binary(), map()) :: {:reply, term(), map()}
  @doc false
  def handle_get(key, state) do
    # Fast path: ETS hit — no need to wait for in-flight writes.
    case ShardETS.ets_lookup(state, key) do
      {:hit, value, _expire_at_ms} ->
        {:reply, value, state}

      :expired ->
        {:reply, nil, state}

      {:cold, fid, off, vsize, exp} ->
        case read_cold_raw(state, fid, off, key) do
          {:ok, value} when is_binary(value) ->
            reply_cold_value(state, key, value, exp, fid, off, vsize)

          _ ->
            {:reply, nil, state}
        end

      :miss ->
        if ShardETS.pending_cold?(state, key) do
          state = ShardFlush.flush_pending_for_read(state)
          {:reply, do_get(state, key), state}
        else
          {:reply, nil, state}
        end
    end
  end

  @spec handle_get_many([binary()], GenServer.from(), map()) ::
          {:noreply, map()} | {:reply, {:error, binary()}, map()}
  @doc false
  def handle_get_many(keys, from, state) when is_list(keys) do
    if length(keys) <= @max_get_many_keys and
         Enum.all?(keys, fn key -> is_binary(key) and byte_size(key) <= @max_key_size end) and
         Enum.reduce(keys, 0, fn key, total -> total + byte_size(key) end) <=
           @max_get_many_key_bytes do
      state = flush_pending_get_many_keys(state, keys)

      spawn(fn ->
        result =
          try do
            get_many_values(keys, state)
          rescue
            _read_error -> {:error, "ERR shard batch read failed"}
          catch
            _kind, _reason -> {:error, "ERR shard batch read failed"}
          end

        GenServer.reply(from, result)
      end)

      {:noreply, state}
    else
      {:reply, {:error, "ERR invalid shard batch read request"}, state}
    end
  end

  defp flush_pending_get_many_keys(state, keys) do
    if Enum.any?(keys, &ShardETS.pending_cold?(state, &1)) do
      ShardFlush.flush_pending_for_read(state)
    else
      state
    end
  end

  defp get_many_values([], _state), do: []

  defp get_many_values(keys, state) do
    {results, file_reads, segment_reads} =
      keys
      |> Enum.with_index()
      |> Enum.reduce({%{}, [], []}, fn {key, index}, {results, file_reads, segment_reads} ->
        case ShardETS.ets_lookup(state, key) do
          {:hit, value, _expire_at_ms} ->
            {Map.put(results, index, value), file_reads, segment_reads}

          :expired ->
            {Map.put(results, index, nil), file_reads, segment_reads}

          :miss ->
            {Map.put(results, index, nil), file_reads, segment_reads}

          {:cold, fid, off, vsize, exp}
          when valid_waraft_segment_location(fid, off, vsize) ->
            read = {index, key, exp, fid, off, vsize}
            {results, file_reads, [read | segment_reads]}

          {:cold, fid, off, vsize, exp} ->
            path = ShardETS.file_path(state.shard_data_path, fid)
            read = {index, key, exp, fid, off, vsize, path}
            {results, [read | file_reads], segment_reads}
        end
      end)

    results = read_get_many_files(state, results, Enum.reverse(file_reads))
    results = read_get_many_segments(state, results, Enum.reverse(segment_reads))

    Enum.map(0..(length(keys) - 1), &Map.get(results, &1))
  end

  defp read_get_many_files(_state, results, []), do: results

  defp read_get_many_files(state, results, reads) do
    locations =
      Enum.map(reads, fn {_index, key, _exp, _fid, off, _vsize, path} ->
        {path, off, key}
      end)

    values =
      case get_many_pread_batch(state, locations) do
        {:ok, values} when is_list(values) and length(values) == length(reads) -> values
        _error -> List.duplicate(:unavailable, length(reads))
      end

    reads
    |> Enum.zip(values)
    |> Enum.reduce(results, fn
      {{index, key, exp, fid, off, vsize, _path}, value}, acc when is_binary(value) ->
        Map.put(acc, index, materialize_get_many_value(state, key, exp, fid, off, vsize, value))

      {{index, _key, _exp, _fid, _off, _vsize, _path}, _error}, acc ->
        Map.put(acc, index, :unavailable)
    end)
  end

  defp read_get_many_segments(_state, results, []), do: results

  defp read_get_many_segments(state, results, reads) do
    Enum.reduce(reads, results, fn {index, key, exp, fid, off, vsize}, acc ->
      value =
        case read_cold_raw(state, fid, off, key) do
          {:ok, value} when is_binary(value) ->
            materialize_get_many_value(state, key, exp, fid, off, vsize, value)

          _error ->
            :unavailable
        end

      Map.put(acc, index, value)
    end)
  end

  defp materialize_get_many_value(state, key, exp, fid, off, vsize, value) do
    case materialize_and_warm_cold_value(state, key, value, exp, fid, off, vsize) do
      nil -> :unavailable
      materialized -> materialized
    end
  end

  defp get_many_pread_batch(state, locations) do
    case Map.get(state, :get_many_pread_batch) do
      fun when is_function(fun, 2) -> fun.(locations, @cold_read_timeout_ms)
      _default -> ColdRead.pread_batch_keyed(locations, @cold_read_timeout_ms)
    end
  end

  @spec handle_get(binary(), GenServer.from(), map()) ::
          {:reply, term(), map()} | {:noreply, map()}
  @doc false
  def handle_get(key, from, state) do
    case ShardETS.ets_lookup(state, key) do
      {:hit, value, _expire_at_ms} ->
        {:reply, value, state}

      :expired ->
        {:reply, nil, state}

      {:cold, fid, off, vsize, exp} when valid_waraft_segment_location(fid, off, vsize) ->
        case read_cold_raw(state, fid, off, key) do
          {:ok, value} when is_binary(value) ->
            reply_cold_value(state, key, value, exp, fid, off, vsize)

          _ ->
            {:reply, nil, state}
        end

      {:cold, fid, off, vsize, exp} ->
        p = ShardETS.file_path(state.shard_data_path, fid)
        submit_cold_read(p, off, key, state, {from, key, exp, fid, off, vsize})

      :miss ->
        if ShardETS.pending_cold?(state, key) do
          state = ShardFlush.flush_pending_for_read(state)
          {:reply, do_get(state, key), state}
        else
          {:reply, nil, state}
        end
    end
  end

  # Returns {file_path, value_offset, value_size} for sendfile optimization,
  # or nil if the key is not found / expired / only in ETS (hot cache).
  # The offset stored in ETS is the RECORD offset (start of header).
  # For sendfile, we need the VALUE offset = record_offset + 26 (header) + key_len.
  @spec handle_get_file_ref(binary(), map()) ::
          {:reply, {binary(), non_neg_integer(), non_neg_integer()} | nil, map()}
  @doc false
  def handle_get_file_ref(key, state) do
    case ShardETS.ets_lookup(state, key) do
      {:hit, _value, _expire_at_ms} ->
        # Key is hot (in ETS). The value may not yet be flushed to disk,
        # so we cannot safely sendfile. Return nil to fall back to normal path.
        {:reply, nil, state}

      :expired ->
        {:reply, nil, state}

      {:cold, fid, off, vsize, _exp} when valid_waraft_segment_location(fid, off, vsize) ->
        {:reply, nil, state}

      {:cold, fid, off, vsize, _exp} ->
        p = ShardETS.file_path(state.shard_data_path, fid)
        {:reply, validated_file_ref(p, off, key, vsize), state}

      :miss ->
        if ShardETS.pending_cold?(state, key) do
          state = ShardFlush.flush_pending_for_read(state)
          {:reply, file_ref_from_lookup(state, key), state}
        else
          {:reply, nil, state}
        end
    end
  end

  @spec handle_get_meta(binary(), map()) :: {:reply, {term(), non_neg_integer()} | nil, map()}
  @doc false
  def handle_get_meta(key, state) do
    case ShardETS.ets_lookup(state, key) do
      {:hit, value, expire_at_ms} ->
        {:reply, {value, expire_at_ms}, state}

      :expired ->
        {:reply, nil, state}

      {:cold, fid, off, vsize, exp} ->
        case read_cold_raw(state, fid, off, key) do
          {:ok, value} when is_binary(value) ->
            reply_cold_meta_value(state, key, value, exp, fid, off, vsize)

          _ ->
            {:reply, nil, state}
        end

      :miss ->
        if ShardETS.pending_cold?(state, key) do
          state = ShardFlush.flush_pending_for_read(state)
          {:reply, do_get_meta(state, key), state}
        else
          {:reply, nil, state}
        end
    end
  end

  @spec handle_get_meta(binary(), GenServer.from(), map()) ::
          {:reply, {term(), non_neg_integer()} | nil, map()} | {:noreply, map()}
  @doc false
  def handle_get_meta(key, from, state) do
    case ShardETS.ets_lookup(state, key) do
      {:hit, value, expire_at_ms} ->
        {:reply, {value, expire_at_ms}, state}

      :expired ->
        {:reply, nil, state}

      {:cold, fid, off, vsize, exp} when valid_waraft_segment_location(fid, off, vsize) ->
        case read_cold_raw(state, fid, off, key) do
          {:ok, value} when is_binary(value) ->
            reply_cold_meta_value(state, key, value, exp, fid, off, vsize)

          _ ->
            {:reply, nil, state}
        end

      {:cold, fid, off, vsize, exp} ->
        p = ShardETS.file_path(state.shard_data_path, fid)
        submit_cold_read(p, off, key, state, {from, key, :meta, exp, fid, off, vsize})

      :miss ->
        if ShardETS.pending_cold?(state, key) do
          state = ShardFlush.flush_pending_for_read(state)
          {:reply, do_get_meta(state, key), state}
        else
          {:reply, nil, state}
        end
    end
  end

  @spec handle_exists(binary(), map()) :: {:reply, boolean(), map()}
  @doc false
  def handle_exists(key, state) do
    case ShardETS.ets_lookup(state, key) do
      {:hit, _value, _expire_at_ms} ->
        {:reply, true, state}

      {:cold, _fid, _off, _vsize, _exp} ->
        # Cold key — value evicted from RAM but key exists on disk.
        {:reply, true, state}

      :expired ->
        {:reply, false, state}

      :miss ->
        if ShardETS.pending_cold?(state, key) do
          {:reply, true, state}
        else
          {:reply, false, state}
        end
    end
  end

  @spec handle_keys(map()) :: {:reply, [binary()], map()}
  @doc false
  def handle_keys(state) do
    # ETS is the read model for live keys, including pending writes that have
    # not reached Bitcask yet. Keep KEYS off the synchronous disk-flush path.
    {:reply, live_keys(state), state}
  end

  # -------------------------------------------------------------------
  # Internal read helpers
  # -------------------------------------------------------------------

  @spec do_get(map(), binary()) :: term() | nil
  @doc false
  def do_get(state, key) do
    case ShardETS.ets_lookup(state, key) do
      {:hit, value, _expire_at_ms} ->
        value

      {:cold, fid, off, vsize, exp} ->
        case read_cold_raw(state, fid, off, key) do
          {:ok, value} when is_binary(value) ->
            materialize_and_warm_cold_value(state, key, value, exp, fid, off, vsize)

          _ ->
            nil
        end

      :expired ->
        nil

      :miss ->
        nil
    end
  end

  defp file_ref_from_lookup(state, key) do
    case ShardETS.ets_lookup(state, key) do
      {:cold, fid, off, vsize, _exp} when valid_waraft_segment_location(fid, off, vsize) ->
        nil

      {:cold, fid, off, vsize, _exp} ->
        p = ShardETS.file_path(state.shard_data_path, fid)
        validated_file_ref(p, off, key, vsize)

      _ ->
        nil
    end
  end

  defp validated_file_ref(path, record_offset, key, value_size) do
    case Ferricstore.Bitcask.NIF.v2_validate_value_ref(path, record_offset, key, value_size) do
      {:ok, {value_offset, ^value_size}} -> {path, value_offset, value_size}
      _ -> nil
    end
  end

  @spec do_get_meta(map(), binary()) :: {term(), non_neg_integer()} | nil
  @doc false
  def do_get_meta(state, key) do
    case ShardETS.ets_lookup(state, key) do
      {:hit, value, expire_at_ms} ->
        {value, expire_at_ms}

      {:cold, fid, off, vsize, exp} ->
        case read_cold_raw(state, fid, off, key) do
          {:ok, value} when is_binary(value) ->
            case materialize_and_warm_cold_value(state, key, value, exp, fid, off, vsize) do
              nil -> nil
              materialized -> {materialized, exp}
            end

          _ ->
            nil
        end

      :expired ->
        nil

      :miss ->
        nil
    end
  end

  # v2 local read for transaction closures. Returns {:ok, value} or {:ok, nil}.
  # Replaces NIF.get_zero_copy(state.store, key) in the 2PC local store.
  @spec v2_local_read(map(), binary()) :: {:ok, term()} | {:error, binary()}
  @doc false
  def v2_local_read(state, key) do
    case :ets.lookup(state.keydir, key) do
      [{^key, value, _exp, _lfu, _fid, _off, _vsize}] when value != nil ->
        {:ok, value}

      [{^key, nil, _exp, _lfu, :pending, _off, _vsize}] ->
        # Not yet flushed to disk — should never reach here. If it does,
        # it means ets_lookup_warm failed to catch the :pending sentinel.
        {:error, "ERR internal: pending entry reached cold read path for #{inspect(key)}"}

      [{^key, nil, _exp, _lfu, fid, off, _vsize}]
      when is_integer(fid) and fid >= 0 and is_integer(off) and off >= 0 ->
        # Cold key -- pread from disk
        p = ShardETS.file_path(state.shard_data_path, fid)

        with {:ok, value} <- read_cold_async(p, off, key),
             {:ok, materialized} <- materialize_blob_value(state, value) do
          {:ok, materialized}
        end

      [{^key, nil, _exp, _lfu, fid, off, vsize}]
      when valid_waraft_segment_location(fid, off, vsize) ->
        with {:ok, value} <- read_cold_raw(state, fid, off, key),
             {:ok, materialized} <- materialize_blob_value(state, value) do
          {:ok, materialized}
        end

      [{^key, nil, _exp, _lfu, _fid, _off, _vsize}] ->
        ShardETS.ets_delete_key(state, key)
        {:ok, nil}

      _ ->
        {:ok, nil}
    end
  end

  defp submit_cold_read(path, offset, expected_key, state, pending_entry) do
    corr_id = state.next_correlation_id + 1

    case NIF.v2_pread_at_key_async(self(), corr_id, path, offset, expected_key) do
      :ok ->
        timer_ref =
          Process.send_after(self(), {:cold_read_timeout, corr_id}, @cold_read_timeout_ms)

        {:noreply,
         %{
           state
           | next_correlation_id: corr_id,
             pending_reads:
               Map.put(state.pending_reads, corr_id, {:pending_read, pending_entry, timer_ref})
         }}

      {:error, reason} ->
        ColdRead.emit_pread_error(path, reason)
        {:reply, nil, state}
    end
  end

  defp read_cold_async(path, offset, expected_key) do
    Ferricstore.Store.ColdRead.pread_keyed(path, offset, expected_key, @cold_read_timeout_ms)
  end

  defp read_cold_raw(state, file_id, _offset, expected_key)
       when valid_waraft_segment_location(file_id, 0, 0) do
    case shard_index(state) do
      idx when is_integer(idx) and idx >= 0 ->
        Ferricstore.Raft.WARaftSegmentReader.read_value_from_location(
          Map.get(state, :instance_ctx),
          idx,
          file_id,
          expected_key
        )

      _ ->
        {:error, :missing_shard_index}
    end
  end

  defp read_cold_raw(state, file_id, offset, expected_key) do
    state.shard_data_path
    |> ShardETS.file_path(file_id)
    |> read_cold_async(offset, expected_key)
  end

  defp shard_index(%{index: index}), do: index
  defp shard_index(%{shard_index: shard_index}), do: shard_index
  defp shard_index(_state), do: nil

  defp reply_cold_value(state, key, value, exp, fid, off, vsize) do
    case materialize_and_warm_cold_value(state, key, value, exp, fid, off, vsize) do
      nil -> {:reply, nil, state}
      materialized -> {:reply, materialized, state}
    end
  end

  defp reply_cold_meta_value(state, key, value, exp, fid, off, vsize) do
    case materialize_and_warm_cold_value(state, key, value, exp, fid, off, vsize) do
      nil -> {:reply, nil, state}
      materialized -> {:reply, {materialized, exp}, state}
    end
  end

  defp materialize_and_warm_cold_value(state, key, value, exp, fid, off, vsize) do
    case materialize_blob_value(state, value) do
      {:ok, materialized} ->
        ShardETS.cold_read_warm_ets(state, key, materialized, exp, fid, off, vsize)
        materialized

      {:error, _reason} ->
        nil
    end
  end

  defp materialize_blob_value(%{data_dir: data_dir, index: shard_index} = state, value) do
    BlobValue.maybe_materialize(data_dir, shard_index, blob_side_channel_threshold(state), value)
  end

  defp blob_side_channel_threshold(%{instance_ctx: ctx}), do: BlobValue.threshold(ctx)
  defp blob_side_channel_threshold(_state), do: 0

  @spec live_keys(map()) :: [binary()]
  @doc false
  def live_keys(state) do
    now = HLC.now_ms()

    {live_keys, expired_keys} =
      :ets.foldl(
        fn
          {key, value, 0, _lfu, _fid, _off, _vsize}, {live, expired} when value != nil ->
            {[key | live], expired}

          {key, nil, 0, _lfu, fid, off, vsize}, {live, expired}
          when valid_cold_location(fid, off, vsize) ->
            {[key | live], expired}

          {key, nil, 0, _lfu, fid, off, vsize}, {live, expired}
          when valid_waraft_segment_location(fid, off, vsize) ->
            {[key | live], expired}

          {key, value, exp, _lfu, _fid, _off, _vsize}, {live, expired}
          when exp > now and value != nil ->
            {[key | live], expired}

          {key, nil, exp, _lfu, fid, off, vsize}, {live, expired}
          when exp > now and valid_cold_location(fid, off, vsize) ->
            {[key | live], expired}

          {key, nil, exp, _lfu, fid, off, vsize}, {live, expired}
          when exp > now and valid_waraft_segment_location(fid, off, vsize) ->
            {[key | live], expired}

          {key, _value, _exp, _lfu, _fid, _off, _vsize}, {live, expired} ->
            {live, [key | expired]}
        end,
        {[], []},
        state.keydir
      )

    Enum.each(expired_keys, &ShardETS.ets_delete_key(state, &1))
    Enum.reject(live_keys, &InternalKey.internal?/1)
  end
end
