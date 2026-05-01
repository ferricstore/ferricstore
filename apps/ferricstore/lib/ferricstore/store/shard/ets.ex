defmodule Ferricstore.Store.Shard.ETS do
  @moduledoc "ETS keydir operations: lookup, insert, delete, cold-read warming, LFU touch, hot-cache threshold enforcement, and prefix scans."

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.HLC
  alias Ferricstore.Store.{LFU, ValueCodec}

  # -------------------------------------------------------------------
  # ETS lookup / classification
  # -------------------------------------------------------------------

  # v2 7-tuple format: {key, value | nil, expire_at_ms, lfu_counter, file_id, offset, value_size}
  # A hit requires value != nil (hot). value = nil means cold (evicted from RAM).
  # On a hit, probabilistically increments the LFU counter.
  # Returns:
  #   {:hit, value, expire_at_ms}
  #   {:cold, file_id, offset, value_size, expire_at_ms}  -- value evicted, disk location known
  #   :expired
  #   :miss
  @spec ets_lookup(map(), binary()) ::
          {:hit, term(), non_neg_integer()}
          | {:cold, non_neg_integer(), non_neg_integer(), non_neg_integer(), non_neg_integer()}
          | :expired
          | :miss
  @doc false
  def ets_lookup(%{keydir: keydir} = state, key) do
    now = HLC.now_ms()

    case :ets.lookup(keydir, key) do
      [{^key, value, 0, lfu, _fid, _off, _vsize}] when value != nil ->
        lfu_touch(keydir, key, lfu)
        {:hit, value, 0}

      [{^key, nil, 0, _lfu, :pending, _off, _vsize}] ->
        # Background write pending, value evicted before disk write.
        # Cannot read from disk yet. Treat as miss (rare edge case).
        :miss

      [{^key, nil, 0, _lfu, fid, off, vsize}] ->
        # Cold key (evicted from RAM) with no expiry -- disk location known.
        {:cold, fid, off, vsize, 0}

      [{^key, value, exp, lfu, _fid, _off, _vsize}] when exp > now and value != nil ->
        lfu_touch(keydir, key, lfu)
        {:hit, value, exp}

      [{^key, nil, exp, _lfu, :pending, _off, _vsize}] when exp > now ->
        # Background write pending with TTL, value evicted before disk write.
        :miss

      [{^key, nil, exp, _lfu, fid, off, vsize}] when exp > now ->
        # Cold key with valid TTL -- disk location known.
        {:cold, fid, off, vsize, exp}

      [{^key, value, _exp, _lfu, _fid, _off, _vsize}] ->
        # Expired entry -- delete it
        track_binary_delete(state, key, value)
        :ets.delete(keydir, key)
        :expired

      [] ->
        :miss
    end
  end

  # Like ets_lookup/2, but transparently warms cold keys via v2_pread_at.
  # Returns {:hit, value, expire_at_ms}, :expired, or :miss — never {:cold, ...}.
  # Use this for read-modify-write operations that need the value in memory.
  @spec ets_lookup_warm(map(), binary()) :: {:hit, term(), non_neg_integer()} | :expired | :miss
  @doc false
  def ets_lookup_warm(state, key) do
    case ets_lookup(state, key) do
      {:cold, fid, off, _vsize, exp} ->
        p = file_path(state.shard_data_path, fid)

        case NIF.v2_pread_at(p, off) do
          {:ok, value} ->
            cold_read_warm_ets(state, key, value)
            {:hit, value, exp}

          _ ->
            :miss
        end

      other ->
        other
    end
  end

  @spec pending_cold?(map(), binary()) :: boolean()
  @doc false
  def pending_cold?(%{keydir: keydir} = state, key) do
    now = HLC.now_ms()

    case :ets.lookup(keydir, key) do
      [{^key, nil, 0, _lfu, :pending, _off, _vsize}] ->
        true

      [{^key, nil, exp, _lfu, :pending, _off, _vsize}] when exp > now ->
        true

      [{^key, nil, _exp, _lfu, :pending, _off, _vsize}] ->
        track_binary_delete(state, key)
        :ets.delete(keydir, key)
        false

      _ ->
        false
    end
  end

  @spec prefix_has_pending_cold?(:ets.tid(), binary()) :: boolean()
  @doc false
  def prefix_has_pending_cold?(keydir, prefix) do
    now = HLC.now_ms()
    prefix_len = byte_size(prefix)

    ms = [
      {{:"$1", nil, :"$2", :_, :pending, :_, :_},
       [
         {:andalso, {:is_binary, :"$1"},
          {:andalso, {:>=, {:byte_size, :"$1"}, prefix_len},
           {:andalso, {:==, {:binary_part, :"$1", 0, prefix_len}, prefix},
            {:orelse, {:==, :"$2", 0}, {:>, :"$2", now}}}}}
       ], [true]}
    ]

    case :ets.select(keydir, ms, 1) do
      {[_], _cont} -> true
      :"$end_of_table" -> false
    end
  end

  # -------------------------------------------------------------------
  # ETS insert / delete
  # -------------------------------------------------------------------

  @spec ets_insert(map(), binary(), term(), non_neg_integer()) :: true
  @doc false
  def ets_insert(state, key, value, expire_at_ms) do
    threshold = hot_cache_threshold(state)
    v = value_for_ets(value, threshold)
    track_binary_insert(state, key, v)
    :ets.insert(state.keydir, {key, v, expire_at_ms, LFU.initial(), :pending, 0, 0})
  end

  # Inserts a key/value/expiry into the keydir with known disk location (v2).
  @spec ets_insert_with_location(
          map(),
          binary(),
          term(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer()
        ) :: true
  @doc false
  def ets_insert_with_location(state, key, value, expire_at_ms, file_id, offset, value_size) do
    threshold = hot_cache_threshold(state)
    v = value_for_ets(value, threshold)
    track_binary_insert(state, key, v)
    :ets.insert(state.keydir, {key, v, expire_at_ms, LFU.initial(), file_id, offset, value_size})
  end

  # Deletes a key from the keydir table.
  @spec ets_delete_key(map(), binary()) :: true
  @doc false
  def ets_delete_key(state, key) do
    track_binary_delete(state, key)
    :ets.delete(state.keydir, key)
  end

  # -------------------------------------------------------------------
  # Hot cache threshold / value coercion
  # -------------------------------------------------------------------

  # Returns the hot cache max value size threshold from instance ctx.
  @spec hot_cache_threshold(map()) :: non_neg_integer()
  @compile {:inline, hot_cache_threshold: 1}
  @doc false
  def hot_cache_threshold(%{instance_ctx: ctx}) when ctx != nil, do: ctx.hot_cache_max_value_size
  def hot_cache_threshold(_state), do: 65_536

  # Returns nil for values exceeding the hot cache max value size threshold,
  # or the value itself if it fits. This prevents large values from being
  # stored in ETS, avoiding expensive binary copies on every :ets.lookup.
  @spec value_for_ets(term(), non_neg_integer()) :: binary() | nil
  @compile {:inline, value_for_ets: 2}
  @doc false
  def value_for_ets(nil, _threshold), do: nil
  def value_for_ets(value, _threshold) when is_integer(value), do: Integer.to_string(value)
  def value_for_ets(value, _threshold) when is_float(value), do: Float.to_string(value)

  def value_for_ets(value, threshold) when is_binary(value) do
    if byte_size(value) > threshold do
      nil
    else
      value
    end
  end

  @spec to_disk_binary(integer() | float() | binary()) :: binary()
  @compile {:inline, to_disk_binary: 1}
  @doc false
  def to_disk_binary(v) when is_integer(v), do: Integer.to_string(v)
  def to_disk_binary(v) when is_float(v), do: Float.to_string(v)
  def to_disk_binary(v) when is_binary(v), do: v

  # -------------------------------------------------------------------
  # Cold-read warming
  # -------------------------------------------------------------------

  # 3-arity convenience: looks up the cold ETS entry to recover disk location
  # metadata, then delegates to the 7-arity version. Used by async read
  # completion handlers that only have {from, key} and the value from disk.
  @spec cold_read_warm_ets(map(), binary(), binary()) :: :ok | true
  @doc false
  def cold_read_warm_ets(state, key, value) do
    case :ets.lookup(state.keydir, key) do
      [{^key, nil, exp, _lfu, fid, off, vsize}] ->
        cold_read_warm_ets(state, key, value, exp, fid, off, vsize)

      _ ->
        # Entry was already evicted or overwritten — skip warming.
        :ok
    end
  end

  # Re-warms the ETS cache after a successful cold read.
  # Preserves the disk location (file_id, offset, value_size) and expire_at_ms.
  # Values exceeding the hot_cache_max_value_size threshold are NOT warmed --
  # they stay cold (nil) in ETS to avoid expensive binary copies on read.
  # Under memory pressure, skip warming to prevent evict/re-promote thrashing.
  @spec cold_read_warm_ets(
          map(),
          binary(),
          binary(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer()
        ) :: :ok | true
  @doc false
  def cold_read_warm_ets(state, key, value, exp, fid, off, vsize) do
    v = value_for_ets(value, hot_cache_threshold(state))

    if v != nil and Ferricstore.MemoryGuard.skip_promotion?() do
      # Under pressure — don't re-cache, keep cold
      :ok
    else
      # Cold -> warm: previous ETS value was nil, so only add new bytes
      track_binary_cold_to_warm(state, key, v)
      :ets.insert(state.keydir, {key, v, exp, LFU.initial(), fid, off, vsize})
    end
  end

  # -------------------------------------------------------------------
  # Warm from store (cold read + ETS update)
  # -------------------------------------------------------------------

  # v2: cold read via pread_at using disk location from ETS 7-tuple.
  # Applies the hot_cache_max_value_size threshold when re-warming ETS.
  @spec warm_from_store(map(), binary()) :: binary() | nil
  @doc false
  def warm_from_store(state, key) do
    case :ets.lookup(state.keydir, key) do
      [{^key, nil, _exp, _lfu, :pending, _off, _vsize}] ->
        # Background write not yet completed -- cannot read from disk.
        nil

      [{^key, nil, exp, _lfu, fid, off, vsize}] when is_integer(fid) ->
        p = file_path(state.shard_data_path, fid)

        case NIF.v2_pread_at(p, off) do
          {:ok, value} ->
            v = value_for_ets(value, hot_cache_threshold(state))
            track_binary_cold_to_warm(state, key, v)
            :ets.insert(state.keydir, {key, v, exp, LFU.initial(), fid, off, vsize})
            value

          _ ->
            nil
        end

      _ ->
        nil
    end
  end

  # v2: cold read meta via pread_at using disk location from ETS 7-tuple.
  # Applies the hot_cache_max_value_size threshold when re-warming ETS.
  @spec warm_meta_from_store(map(), binary()) :: {binary(), non_neg_integer()} | nil
  @doc false
  def warm_meta_from_store(state, key) do
    case :ets.lookup(state.keydir, key) do
      [{^key, nil, exp, _lfu, fid, off, vsize}] when is_integer(fid) ->
        p = file_path(state.shard_data_path, fid)

        case NIF.v2_pread_at(p, off) do
          {:ok, value} ->
            v = value_for_ets(value, hot_cache_threshold(state))
            track_binary_cold_to_warm(state, key, v)
            :ets.insert(state.keydir, {key, v, exp, LFU.initial(), fid, off, vsize})
            {value, exp}

          _ ->
            nil
        end

      _ ->
        nil
    end
  end

  # -------------------------------------------------------------------
  # File path helper
  # -------------------------------------------------------------------

  # Returns the file path for a given file_id within the shard data directory.
  @spec file_path(binary(), non_neg_integer()) :: binary()
  @doc false
  def file_path(shard_path, file_id) do
    Path.join(shard_path, "#{String.pad_leading(Integer.to_string(file_id), 5, "0")}.log")
  end

  # -------------------------------------------------------------------
  # Prefix-based ETS helpers (replaces O(N) :ets.foldl full-table scans)
  # -------------------------------------------------------------------

  @spec prefix_scan_entries(:ets.tid(), binary(), binary() | nil) :: [{binary(), binary()}]
  @doc false
  def prefix_scan_entries(keydir, prefix, shard_data_path) do
    now = HLC.now_ms()
    prefix_len = byte_size(prefix)
    # Select all 7-tuple fields so we can cold-read nil values
    ms = [
      {{:"$1", :"$2", :"$3", :_, :"$4", :"$5", :"$6"},
       [
         {:andalso, {:is_binary, :"$1"},
          {:andalso, {:>=, {:byte_size, :"$1"}, prefix_len},
           {:==, {:binary_part, :"$1", 0, prefix_len}, prefix}}}
       ], [{{:"$1", :"$2", :"$3", :"$4", :"$5", :"$6"}}]}
    ]

    :ets.select(keydir, ms)
    |> Enum.reduce([], fn {key, value, exp, fid, off, _vsize}, acc ->
      if exp == 0 or exp > now do
        # For cold keys (value=nil), do a disk read to get the actual value
        actual_value = prefix_entry_value(value, shard_data_path, fid, off)

        if actual_value != nil do
          field =
            case :binary.split(key, <<0>>) do
              [_pre, sub] -> sub
              _ -> key
            end

          [{field, actual_value} | acc]
        else
          acc
        end
      else
        acc
      end
    end)
  end

  defp prefix_entry_value(nil, shard_data_path, fid, off)
       when shard_data_path != nil and is_integer(fid) do
    p = file_path(shard_data_path, fid)

    case NIF.v2_pread_at(p, off) do
      {:ok, v} -> v
      _ -> nil
    end
  end

  defp prefix_entry_value(nil, _shard_data_path, _fid, _off), do: nil
  defp prefix_entry_value(value, _shard_data_path, _fid, _off), do: value

  @spec prefix_count_entries(:ets.tid(), binary()) :: non_neg_integer()
  @doc false
  def prefix_count_entries(keydir, prefix) do
    now = HLC.now_ms()
    prefix_len = byte_size(prefix)

    ms = [
      {{:"$1", :_, :"$3", :_, :_, :_, :_},
       [
         {:andalso, {:is_binary, :"$1"},
          {:andalso, {:>=, {:byte_size, :"$1"}, prefix_len},
           {:==, {:binary_part, :"$1", 0, prefix_len}, prefix}}}
       ], [:"$3"]}
    ]

    :ets.select(keydir, ms)
    |> Enum.count(fn exp -> exp == 0 or exp > now end)
  end

  @doc false
  def prefix_collect_keys(keydir, prefix) do
    prefix_len = byte_size(prefix)

    ms = [
      {{:"$1", :_, :_, :_, :_, :_, :_},
       [
         {:andalso, {:is_binary, :"$1"},
          {:andalso, {:>=, {:byte_size, :"$1"}, prefix_len},
           {:==, {:binary_part, :"$1", 0, prefix_len}, prefix}}}
       ], [:"$1"]}
    ]

    :ets.select(keydir, ms)
  end

  # -------------------------------------------------------------------
  # Integer / float coercion — delegates to shared ValueCodec
  # -------------------------------------------------------------------

  @spec coerce_integer(term()) :: {:ok, integer()} | :error
  @doc false
  def coerce_integer(v) when is_integer(v), do: {:ok, v}
  def coerce_integer(v) when is_float(v), do: :error
  def coerce_integer(v) when is_binary(v), do: ValueCodec.parse_integer(v)

  @spec coerce_float(term()) :: {:ok, float()} | :error
  @doc false
  def coerce_float(v) when is_float(v), do: {:ok, v}
  def coerce_float(v) when is_integer(v), do: {:ok, v * 1.0}
  def coerce_float(v) when is_binary(v), do: ValueCodec.parse_float(v)

  # -------------------------------------------------------------------
  # Private helpers
  # -------------------------------------------------------------------

  # LFU touch with time-based decay (Redis-compatible).
  defp lfu_touch(keydir, key, packed_lfu) do
    LFU.touch(keydir, key, packed_lfu)
  end

  # -------------------------------------------------------------------
  # Off-heap binary byte tracking
  # -------------------------------------------------------------------

  # Tracks delta for insert (new value replacing possible existing value).
  # Must be called BEFORE :ets.insert so the old value can be read.
  defp track_binary_insert(
         %{instance_ctx: %{keydir_binary_bytes: ref}, index: idx} = state,
         key,
         new_val
       )
       when ref != nil do
    new_bytes = offheap_size(key) + offheap_size(new_val)

    old_bytes =
      case :ets.lookup(state.keydir, key) do
        [{^key, old_val, _, _, _, _, _}] -> offheap_size(key) + offheap_size(old_val)
        _ -> 0
      end

    delta = new_bytes - old_bytes
    if delta != 0, do: :atomics.add(ref, idx + 1, delta)
  end

  defp track_binary_insert(_, _, _), do: :ok

  # Tracks bytes removed for delete. Must be called BEFORE :ets.delete.
  defp track_binary_delete(%{instance_ctx: %{keydir_binary_bytes: ref}, index: idx} = state, key)
       when ref != nil do
    bytes =
      case :ets.lookup(state.keydir, key) do
        [{^key, val, _, _, _, _, _}] -> offheap_size(key) + offheap_size(val)
        _ -> 0
      end

    if bytes > 0, do: :atomics.sub(ref, idx + 1, bytes)
  end

  defp track_binary_delete(_, _), do: :ok

  # Tracks bytes removed for delete when value is already known (avoids extra lookup).
  defp track_binary_delete(%{instance_ctx: %{keydir_binary_bytes: ref}, index: idx}, key, value)
       when ref != nil do
    bytes = offheap_size(key) + offheap_size(value)
    if bytes > 0, do: :atomics.sub(ref, idx + 1, bytes)
  end

  defp track_binary_delete(_, _, _), do: :ok

  # Tracks bytes added when warming a cold key (nil -> value).
  defp track_binary_cold_to_warm(
         %{instance_ctx: %{keydir_binary_bytes: ref}, index: idx},
         _key,
         new_val
       )
       when ref != nil do
    # Key was already in ETS (cold entry with nil value), so key bytes are unchanged.
    # Only the value bytes are new.
    new_bytes = offheap_size(new_val)
    if new_bytes > 0, do: :atomics.add(ref, idx + 1, new_bytes)
  end

  defp track_binary_cold_to_warm(_, _, _), do: :ok

  defp offheap_size(v) when is_binary(v) and byte_size(v) > 64, do: byte_size(v)
  defp offheap_size(_), do: 0
end
