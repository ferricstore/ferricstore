defmodule Ferricstore.Store.Shard.Reads do
  @moduledoc "Shard read-path handlers: ETS hot lookup, cold-key pread from Bitcask, exists check, and key enumeration."

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.HLC
  alias Ferricstore.Store.Shard.ETS, as: ShardETS
  alias Ferricstore.Store.Shard.Flush, as: ShardFlush

  @bitcask_header_size 26
  @cold_read_timeout_ms 10_000

  defguardp valid_cold_location(file_id, offset, value_size)
            when is_integer(file_id) and file_id >= 0 and is_integer(offset) and offset >= 0 and
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

      {:cold, fid, off, _vsize, exp} ->
        # Cold key — value evicted from ETS but disk location known.
        p = ShardETS.file_path(state.shard_data_path, fid)

        case read_cold_async(p, off) do
          {:ok, value} when is_binary(value) ->
            ShardETS.cold_read_warm_ets(state, key, value, exp, fid, off, byte_size(value))
            {:reply, value, state}

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

  @spec handle_get(binary(), GenServer.from(), map()) ::
          {:reply, term(), map()} | {:noreply, map()}
  @doc false
  def handle_get(key, from, state) do
    case ShardETS.ets_lookup(state, key) do
      {:hit, value, _expire_at_ms} ->
        {:reply, value, state}

      :expired ->
        {:reply, nil, state}

      {:cold, fid, off, vsize, exp} ->
        p = ShardETS.file_path(state.shard_data_path, fid)
        submit_cold_read(p, off, state, {from, key, exp, fid, off, vsize})

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

      {:cold, fid, off, vsize, _exp} ->
        # Cold key — location known from ETS 7-tuple.
        # Adjust offset to skip header and key bytes to get to the value.
        p = ShardETS.file_path(state.shard_data_path, fid)
        value_offset = off + @bitcask_header_size + byte_size(key)
        {:reply, {p, value_offset, vsize}, state}

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

      {:cold, fid, off, _vsize, exp} ->
        p = ShardETS.file_path(state.shard_data_path, fid)

        case read_cold_async(p, off) do
          {:ok, value} when is_binary(value) ->
            ShardETS.cold_read_warm_ets(state, key, value, exp, fid, off, byte_size(value))
            {:reply, {value, exp}, state}

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

      {:cold, fid, off, vsize, exp} ->
        p = ShardETS.file_path(state.shard_data_path, fid)
        submit_cold_read(p, off, state, {from, key, :meta, exp, fid, off, vsize})

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
        # Zero-copy cold read via v2 pread (ResourceBinary).
        p = ShardETS.file_path(state.shard_data_path, fid)

        case read_cold_async(p, off) do
          {:ok, value} when is_binary(value) ->
            ShardETS.cold_read_warm_ets(state, key, value, exp, fid, off, vsize)
            value

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
      {:cold, fid, off, vsize, _exp} ->
        p = ShardETS.file_path(state.shard_data_path, fid)
        {p, off + @bitcask_header_size + byte_size(key), vsize}

      _ ->
        nil
    end
  end

  @spec do_get_meta(map(), binary()) :: {term(), non_neg_integer()} | nil
  @doc false
  def do_get_meta(state, key) do
    case ShardETS.ets_lookup(state, key) do
      {:hit, value, expire_at_ms} ->
        {value, expire_at_ms}

      {:cold, fid, off, vsize, exp} ->
        p = ShardETS.file_path(state.shard_data_path, fid)

        case read_cold_async(p, off) do
          {:ok, value} when is_binary(value) ->
            ShardETS.cold_read_warm_ets(state, key, value, exp, fid, off, vsize)
            {value, exp}

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
        read_cold_async(p, off)

      [{^key, nil, _exp, _lfu, _fid, _off, _vsize}] ->
        ShardETS.ets_delete_key(state, key)
        {:ok, nil}

      _ ->
        {:ok, nil}
    end
  end

  defp submit_cold_read(path, offset, state, pending_entry) do
    corr_id = state.next_correlation_id + 1

    case NIF.v2_pread_at_async(self(), corr_id, path, offset) do
      :ok ->
        Process.send_after(self(), {:cold_read_timeout, corr_id}, @cold_read_timeout_ms)

        {:noreply,
         %{
           state
           | next_correlation_id: corr_id,
             pending_reads: Map.put(state.pending_reads, corr_id, pending_entry)
         }}

      {:error, _reason} ->
        {:reply, nil, state}
    end
  end

  defp read_cold_async(path, offset) do
    Ferricstore.Store.ColdRead.pread_at(path, offset, @cold_read_timeout_ms)
  end

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

          {key, value, exp, _lfu, _fid, _off, _vsize}, {live, expired}
          when exp > now and value != nil ->
            {[key | live], expired}

          {key, nil, exp, _lfu, fid, off, vsize}, {live, expired}
          when exp > now and valid_cold_location(fid, off, vsize) ->
            {[key | live], expired}

          {key, _value, _exp, _lfu, _fid, _off, _vsize}, {live, expired} ->
            {live, [key | expired]}
        end,
        {[], []},
        state.keydir
      )

    Enum.each(expired_keys, &ShardETS.ets_delete_key(state, &1))
    live_keys
  end
end
