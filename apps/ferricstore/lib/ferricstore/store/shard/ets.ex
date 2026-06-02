defmodule Ferricstore.Store.Shard.ETS do
  @moduledoc "ETS keydir operations: lookup, insert, delete, cold-read warming, LFU touch, hot-cache threshold enforcement, and prefix scans."

  alias Ferricstore.HLC

  alias Ferricstore.Store.{
    BlobRef,
    BlobValue,
    ColdRead,
    CompoundKey,
    ExpiryTracker,
    LFU,
    ValueCodec
  }

  @cold_batch_read_timeout_ms 10_000

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

      [{^key, nil, 0, _lfu, fid, off, vsize}] when valid_cold_location(fid, off, vsize) ->
        # Cold key (evicted from RAM) with no expiry -- disk location known.
        {:cold, fid, off, vsize, 0}

      [{^key, nil, 0, _lfu, {:flow_history, file_id} = fid, off, vsize}]
      when is_integer(file_id) and file_id >= 0 and is_integer(off) and off >= 0 and
             is_integer(vsize) and vsize >= 0 ->
        {:cold, fid, off, vsize, 0}

      [{^key, nil, 0, _lfu, fid, off, vsize}]
      when valid_waraft_segment_location(fid, off, vsize) ->
        {:cold, fid, off, vsize, 0}

      [{^key, value, exp, lfu, _fid, _off, _vsize}] when exp > now and value != nil ->
        lfu_touch(keydir, key, lfu)
        {:hit, value, exp}

      [{^key, nil, exp, _lfu, :pending, _off, _vsize}] when exp > now ->
        # Background write pending with TTL, value evicted before disk write.
        :miss

      [{^key, nil, exp, _lfu, fid, off, vsize}]
      when exp > now and valid_cold_location(fid, off, vsize) ->
        # Cold key with valid TTL -- disk location known.
        {:cold, fid, off, vsize, exp}

      [{^key, nil, exp, _lfu, {:flow_history, file_id} = fid, off, vsize}]
      when exp > now and is_integer(file_id) and file_id >= 0 and is_integer(off) and
             off >= 0 and is_integer(vsize) and vsize >= 0 ->
        {:cold, fid, off, vsize, exp}

      [{^key, nil, exp, _lfu, fid, off, vsize}]
      when exp > now and valid_waraft_segment_location(fid, off, vsize) ->
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

  # Like ets_lookup/2, but transparently warms cold keys via async pread.
  # Returns {:hit, value, expire_at_ms}, :expired, or :miss — never {:cold, ...}.
  # Use this for read-modify-write operations that need the value in memory.
  @spec ets_lookup_warm(map(), binary()) :: {:hit, term(), non_neg_integer()} | :expired | :miss
  @doc false
  def ets_lookup_warm(state, key) do
    case ets_lookup(state, key) do
      {:cold, fid, off, vsize, exp} when valid_waraft_segment_location(fid, off, vsize) ->
        case read_waraft_segment_value(state, fid, key) do
          {:ok, value} when is_binary(value) ->
            cold_read_warm_ets(state, key, value)
            {:hit, value, exp}

          _ ->
            :miss
        end

      {:cold, fid, off, _vsize, exp} ->
        p = file_path(state.shard_data_path, fid)

        case read_cold_async(state, p, off, key) do
          {:ok, value} when is_binary(value) ->
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
    ets_insert(state, key, value, expire_at_ms, :ets.lookup(state.keydir, key))
  end

  @spec ets_insert(map(), binary(), term(), non_neg_integer(), list()) :: true
  @doc false
  def ets_insert(state, key, value, expire_at_ms, previous) do
    threshold = hot_cache_threshold(state)
    v = value_for_ets(value, threshold)
    {original_fid, original_vsize} = pending_original_location(key, previous)
    track_binary_insert(state, key, v, previous)
    adjust_expiry_for_insert(state, previous, expire_at_ms)

    :ets.insert(
      state.keydir,
      {key, v, expire_at_ms, LFU.initial(), :pending, original_fid, original_vsize}
    )
  end

  defp pending_original_location(key, previous) do
    case previous do
      [{^key, _old_value, _old_exp, _old_lfu, :pending, old_fid, old_vsize}]
      when is_integer(old_fid) and old_fid >= 0 and is_integer(old_vsize) and old_vsize >= 0 ->
        {old_fid, old_vsize}

      [{^key, _old_value, _old_exp, _old_lfu, old_fid, _old_off, old_vsize}]
      when is_integer(old_fid) and old_fid >= 0 and is_integer(old_vsize) and old_vsize >= 0 ->
        {old_fid, old_vsize}

      _ ->
        {nil, 0}
    end
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
    ets_insert_with_location(
      state,
      key,
      value,
      expire_at_ms,
      file_id,
      offset,
      value_size,
      :ets.lookup(state.keydir, key)
    )
  end

  @spec ets_insert_with_location(
          map(),
          binary(),
          term(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          list()
        ) :: true
  @doc false
  def ets_insert_with_location(
        state,
        key,
        value,
        expire_at_ms,
        file_id,
        offset,
        value_size,
        previous
      ) do
    ets_insert_with_location(
      state,
      key,
      value,
      expire_at_ms,
      file_id,
      offset,
      value_size,
      previous,
      hot_cache_threshold(state)
    )
  end

  @spec ets_insert_with_location(
          map(),
          binary(),
          term(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          list(),
          non_neg_integer()
        ) :: true
  @doc false
  def ets_insert_with_location(
        state,
        key,
        value,
        expire_at_ms,
        file_id,
        offset,
        value_size,
        previous,
        threshold
      ) do
    v = value_for_ets(value, threshold)
    track_binary_insert(state, key, v, previous)
    adjust_expiry_for_insert(state, previous, expire_at_ms)
    :ets.insert(state.keydir, {key, v, expire_at_ms, LFU.initial(), file_id, offset, value_size})
  end

  @spec ets_insert_fresh_no_expiry_many_with_location(
          map(),
          [{binary(), binary(), 0}],
          term(),
          non_neg_integer(),
          non_neg_integer()
        ) :: {:ok, non_neg_integer()} | :fallback
  @doc false
  def ets_insert_fresh_no_expiry_many_with_location(state, entries, file_id, offset, threshold)
      when is_list(entries) and is_integer(offset) and offset >= 0 and is_integer(threshold) and
             threshold >= 0 do
    lfu = LFU.initial()

    case fresh_no_expiry_location_records(entries, state, file_id, offset, threshold, lfu) do
      {:ok, [], 0, 0} ->
        {:ok, 0}

      {:ok, records, binary_delta, count} ->
        true = :ets.insert(state.keydir, records)
        add_keydir_binary_delta(state, binary_delta)
        {:ok, count}

      :fallback ->
        :fallback
    end
  end

  def ets_insert_fresh_no_expiry_many_with_location(_, _, _, _, _), do: :fallback

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

  @spec value_for_persisted_ets(binary(), non_neg_integer()) :: binary() | nil
  @compile {:inline, value_for_persisted_ets: 2}
  @doc false
  def value_for_persisted_ets(value, threshold) when is_binary(value) do
    if BlobRef.ref?(value), do: nil, else: value_for_ets(value, threshold)
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
      [{^key, nil, exp, _lfu, fid, off, vsize}] when valid_cold_location(fid, off, vsize) ->
        cold_read_warm_ets(state, key, value, exp, fid, off, vsize)

      [{^key, nil, exp, _lfu, fid, off, vsize}]
      when valid_waraft_segment_location(fid, off, vsize) ->
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
      case warm_matching_cold_entry(state.keydir, key, v, exp, fid, off, vsize) do
        1 ->
          # Cold -> warm: previous ETS value was nil, so only add new bytes.
          track_binary_cold_to_warm(state, key, v)
          true

        _ ->
          :ok
      end
    end
  end

  defp warm_matching_cold_entry(keydir, key, value, exp, fid, off, vsize) do
    lfu = LFU.initial()

    :ets.select_replace(keydir, [
      {
        {key, nil, exp, :"$1", fid, off, vsize},
        [],
        [{{key, value, exp, lfu, fid, off, vsize}}]
      }
    ])
  rescue
    ArgumentError -> 0
  end

  # -------------------------------------------------------------------
  # Warm from store (cold read + ETS update)
  # -------------------------------------------------------------------

  # v2: cold read via async pread using disk location from ETS 7-tuple.
  # Applies the hot_cache_max_value_size threshold when re-warming ETS.
  @spec warm_from_store(map(), binary()) :: binary() | nil
  @doc false
  def warm_from_store(state, key) do
    case :ets.lookup(state.keydir, key) do
      [{^key, nil, _exp, _lfu, :pending, _off, _vsize}] ->
        # Background write not yet completed -- cannot read from disk.
        nil

      [{^key, nil, exp, _lfu, fid, off, vsize}] when valid_cold_location(fid, off, vsize) ->
        p = file_path(state.shard_data_path, fid)

        case read_cold_async(state, p, off, key) do
          {:ok, value} when is_binary(value) ->
            v = value_for_ets(value, hot_cache_threshold(state))
            track_binary_cold_to_warm(state, key, v)
            :ets.insert(state.keydir, {key, v, exp, LFU.initial(), fid, off, vsize})
            value

          _ ->
            nil
        end

      [{^key, nil, exp, _lfu, fid, off, vsize}]
      when valid_waraft_segment_location(fid, off, vsize) ->
        case read_waraft_segment_value(state, fid, key) do
          {:ok, value} when is_binary(value) ->
            v = value_for_ets(value, hot_cache_threshold(state))
            track_binary_cold_to_warm(state, key, v)
            :ets.insert(state.keydir, {key, v, exp, LFU.initial(), fid, off, vsize})
            value

          _ ->
            nil
        end

      [{^key, nil, _exp, _lfu, _fid, _off, _vsize}] ->
        track_binary_delete(state, key, nil)
        :ets.delete(state.keydir, key)
        nil

      _ ->
        nil
    end
  end

  # v2: cold read meta via async pread using disk location from ETS 7-tuple.
  # Applies the hot_cache_max_value_size threshold when re-warming ETS.
  @spec warm_meta_from_store(map(), binary()) :: {binary(), non_neg_integer()} | nil
  @doc false
  def warm_meta_from_store(state, key) do
    case :ets.lookup(state.keydir, key) do
      [{^key, nil, exp, _lfu, fid, off, vsize}] when valid_cold_location(fid, off, vsize) ->
        p = file_path(state.shard_data_path, fid)

        case read_cold_async(state, p, off, key) do
          {:ok, value} when is_binary(value) ->
            v = value_for_ets(value, hot_cache_threshold(state))
            track_binary_cold_to_warm(state, key, v)
            :ets.insert(state.keydir, {key, v, exp, LFU.initial(), fid, off, vsize})
            {value, exp}

          _ ->
            nil
        end

      [{^key, nil, _exp, _lfu, _fid, _off, _vsize}] ->
        track_binary_delete(state, key, nil)
        :ets.delete(state.keydir, key)
        nil

      _ ->
        nil
    end
  end

  # -------------------------------------------------------------------
  # File path helper
  # -------------------------------------------------------------------

  # Returns the file path for a given file_id within the shard data directory.
  @spec file_path(binary(), non_neg_integer() | {:flow_history, non_neg_integer()}) :: binary()
  @doc false
  def file_path(shard_path, {:flow_history, file_id}) do
    Ferricstore.Flow.HistoryProjector.history_file_path(shard_path, file_id)
  end

  def file_path(shard_path, file_id) do
    Path.join(shard_path, "#{String.pad_leading(Integer.to_string(file_id), 5, "0")}.log")
  end

  # -------------------------------------------------------------------
  # Prefix-based ETS helpers (replaces O(N) :ets.foldl full-table scans)
  # -------------------------------------------------------------------

  @spec prefix_scan_entries(map() | :ets.tid(), binary(), binary() | nil) :: [
          {binary(), binary()}
        ]
  @doc false
  def prefix_scan_entries(%{keydir: keydir} = state, prefix, shard_data_path),
    do: do_prefix_scan_entries(state, keydir, prefix, shard_data_path)

  def prefix_scan_entries(keydir, prefix, shard_data_path),
    do: do_prefix_scan_entries(nil, keydir, prefix, shard_data_path)

  defp do_prefix_scan_entries(state, keydir, prefix, shard_data_path) do
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

    {tokens, {cold_entries, _cold_count}} =
      :ets.select(keydir, ms)
      |> Enum.reduce({[], {[], 0}}, fn {key, value, exp, fid, off, vsize},
                                       {tokens, {cold_entries, cold_count}} ->
        cond do
          exp != 0 and exp <= now ->
            maybe_delete_expired_prefix_entry(state, keydir, key)
            {tokens, {cold_entries, cold_count}}

          value == nil and
              not (valid_cold_location(fid, off, vsize) or
                     flow_history_cold_location?(fid, off, vsize) or
                       valid_waraft_segment_location(fid, off, vsize)) ->
            maybe_delete_expired_prefix_entry(state, keydir, key)
            {tokens, {cold_entries, cold_count}}

          value == nil and valid_waraft_segment_location(fid, off, vsize) and state != nil ->
            case read_waraft_segment_value(state, fid, key) do
              {:ok, segment_value} ->
                {[{:value, {prefix_field(key), segment_value}} | tokens],
                 {cold_entries, cold_count}}

              _ ->
                {tokens, {cold_entries, cold_count}}
            end

          value == nil and shard_data_path != nil ->
            field = prefix_field(key)
            file_path = file_path(shard_data_path, fid)
            entry = {field, key, file_path, off}
            {[{:cold, cold_count} | tokens], {[entry | cold_entries], cold_count + 1}}

          value != nil ->
            {[{:value, {prefix_field(key), value}} | tokens], {cold_entries, cold_count}}

          true ->
            {tokens, {cold_entries, cold_count}}
        end
      end)

    cold_values =
      cold_entries
      |> Enum.reverse()
      |> prefix_read_cold_batch_async(state)
      |> List.to_tuple()

    Enum.flat_map(tokens, fn
      {:value, result} ->
        [result]

      {:cold, index} ->
        case elem(cold_values, index) do
          nil -> []
          result -> [result]
        end
    end)
  end

  defp prefix_field(key) do
    case :binary.split(key, <<0>>) do
      [_pre, sub] -> sub
      _ -> key
    end
  end

  @spec prefix_scan_fields(map() | :ets.tid(), binary()) :: [binary()]
  @doc false
  def prefix_scan_fields(%{keydir: keydir} = state, prefix),
    do: do_prefix_scan_fields(state, keydir, prefix)

  def prefix_scan_fields(keydir, prefix),
    do: do_prefix_scan_fields(nil, keydir, prefix)

  defp do_prefix_scan_fields(state, keydir, prefix) do
    now = HLC.now_ms()
    prefix_len = byte_size(prefix)

    prefix_guard =
      {:andalso, {:is_binary, :"$1"},
       {:andalso, {:>=, {:byte_size, :"$1"}, prefix_len},
        {:==, {:binary_part, :"$1", 0, prefix_len}, prefix}}}

    live_exp_guard = {:orelse, {:==, :"$3", 0}, {:>, :"$3", now}}

    valid_bitcask_cold_guard =
      {:andalso, {:is_integer, :"$4"},
       {:andalso, {:>=, :"$4", 0},
        {:andalso, {:is_integer, :"$5"},
         {:andalso, {:>=, :"$5", 0}, {:andalso, {:is_integer, :"$6"}, {:>=, :"$6", 0}}}}}}

    valid_waraft_segment_guard =
      {:andalso, {:is_tuple, :"$4"},
       {:andalso, {:==, {:tuple_size, :"$4"}, 2},
        {:andalso,
         {:orelse, {:==, {:element, 1, :"$4"}, :waraft_segment},
          {:orelse, {:==, {:element, 1, :"$4"}, :waraft_projection},
           {:==, {:element, 1, :"$4"}, :waraft_apply_projection}}},
         {:andalso, {:is_integer, {:element, 2, :"$4"}},
          {:andalso, {:>, {:element, 2, :"$4"}, 0},
           {:andalso, {:is_integer, :"$5"},
            {:andalso, {:>=, :"$5", 0}, {:andalso, {:is_integer, :"$6"}, {:>=, :"$6", 0}}}}}}}}}

    valid_cold_guard = {:orelse, valid_bitcask_cold_guard, valid_waraft_segment_guard}

    visible_value_guard =
      {:orelse, {:"/=", :"$2", nil}, {:orelse, valid_cold_guard, {:==, :"$4", :pending}}}

    ms = [
      {{:"$1", :"$2", :"$3", :_, :"$4", :"$5", :"$6"},
       [{:andalso, prefix_guard, {:andalso, live_exp_guard, visible_value_guard}}], [:"$1"]}
    ]

    keys = :ets.select(keydir, ms)

    if state != nil do
      delete_expired_prefix_entries(state, keydir, prefix, prefix_len, now)
      delete_invalid_cold_prefix_entries(state, keydir, prefix, prefix_len, now)
    end

    Enum.map(keys, &prefix_field/1)
  end

  defp flow_history_cold_location?({:flow_history, file_id}, offset, value_size)
       when is_integer(file_id) and file_id >= 0 and is_integer(offset) and offset >= 0 and
              is_integer(value_size) and value_size >= 0,
       do: true

  defp flow_history_cold_location?(_file_id, _offset, _value_size), do: false

  defp prefix_read_cold_batch_async([], _state), do: []

  defp prefix_read_cold_batch_async(entries, state) do
    locations = Enum.map(entries, fn {_field, key, file_path, off} -> {file_path, off, key} end)

    values =
      case ColdRead.pread_batch_keyed(locations, @cold_batch_read_timeout_ms) do
        {:ok, values} when is_list(values) and length(values) == length(entries) ->
          values

        {:ok, _bad_values} ->
          List.duplicate({:error, :batch_result_length_mismatch}, length(entries))

        {:error, reason} ->
          List.duplicate({:error, reason}, length(entries))
      end

    emit_prefix_cold_read_errors(entries, values)

    Enum.zip(entries, prefix_materialize_blob_values(state, values))
    |> Enum.map(fn
      {{field, _key, _file_path, _off}, {:ok, materialized}} ->
        {field, materialized}

      {_entry, _value} ->
        nil
    end)
  end

  defp prefix_materialize_blob_values(%{data_dir: data_dir, index: shard_index} = state, values) do
    {binary_values, indexed_results} =
      values
      |> Enum.with_index()
      |> Enum.reduce({[], %{}}, fn
        {value, index}, {binary_values, indexed_results} when is_binary(value) ->
          {[{index, value} | binary_values], indexed_results}

        {_value, index}, {binary_values, indexed_results} ->
          {binary_values, Map.put(indexed_results, index, :skip)}
      end)

    indexed_results =
      if binary_values == [] do
        indexed_results
      else
        ordered_values = Enum.reverse(binary_values)
        threshold = blob_side_channel_threshold(state)
        values = Enum.map(ordered_values, fn {_index, value} -> value end)

        ordered_values
        |> Enum.zip(BlobValue.maybe_materialize_many(data_dir, shard_index, threshold, values))
        |> Enum.reduce(indexed_results, fn {{index, _value}, result}, acc ->
          Map.put(acc, index, result)
        end)
      end

    values
    |> Enum.with_index()
    |> Enum.map(fn {_value, index} -> Map.fetch!(indexed_results, index) end)
  end

  defp prefix_materialize_blob_values(_state, values),
    do: Enum.map(values, fn _value -> :skip end)

  defp emit_prefix_cold_read_errors(entries, values) do
    entries
    |> Enum.zip(values)
    |> Enum.reduce(%{}, fn
      {{_field, _key, file_path, _off}, {:error, raw_reason}}, acc ->
        Map.update(acc, {file_path, raw_reason}, 1, &(&1 + 1))

      {_entry, _value}, acc ->
        acc
    end)
    |> Enum.each(fn {{path, raw_reason}, count} ->
      ColdRead.emit_pread_error(path, raw_reason, count)
    end)
  end

  defp read_cold_async(state, path, offset, key) do
    with {:ok, value} <-
           Ferricstore.Store.ColdRead.pread_at(path, offset, key, @cold_batch_read_timeout_ms),
         {:ok, materialized} <- materialize_blob_value(state, value) do
      {:ok, materialized}
    end
  end

  defp read_waraft_segment_value(%{instance_ctx: ctx, index: shard_index} = state, file_id, key) do
    with {:ok, value} <-
           Ferricstore.Raft.WARaftSegmentReader.read_value_from_location(
             ctx,
             shard_index,
             file_id,
             key
           ),
         {:ok, materialized} <- materialize_blob_value(state, value) do
      {:ok, materialized}
    end
  end

  defp read_waraft_segment_value(_state, _file_id, _key), do: {:error, :missing_instance_ctx}

  defp materialize_blob_value(%{data_dir: data_dir, index: shard_index} = state, value) do
    BlobValue.maybe_materialize(data_dir, shard_index, blob_side_channel_threshold(state), value)
  end

  defp materialize_blob_value(_state, value), do: {:ok, value}

  defp blob_side_channel_threshold(%{instance_ctx: ctx}), do: BlobValue.threshold(ctx)
  defp blob_side_channel_threshold(_state), do: 0

  @spec prefix_count_entries(map() | :ets.tid(), binary()) :: non_neg_integer()
  @doc false
  def prefix_count_entries(%{keydir: keydir} = state, prefix),
    do: do_prefix_count_entries(state, keydir, prefix)

  def prefix_count_entries(keydir, prefix),
    do: do_prefix_count_entries(nil, keydir, prefix)

  defp do_prefix_count_entries(state, keydir, prefix) do
    now = HLC.now_ms()
    prefix_len = byte_size(prefix)

    prefix_guard =
      {:andalso, {:is_binary, :"$1"},
       {:andalso, {:>=, {:byte_size, :"$1"}, prefix_len},
        {:==, {:binary_part, :"$1", 0, prefix_len}, prefix}}}

    live_exp_guard = {:orelse, {:==, :"$3", 0}, {:>, :"$3", now}}

    valid_bitcask_cold_guard =
      {:andalso, {:is_integer, :"$4"},
       {:andalso, {:>=, :"$4", 0},
        {:andalso, {:is_integer, :"$5"},
         {:andalso, {:>=, :"$5", 0}, {:andalso, {:is_integer, :"$6"}, {:>=, :"$6", 0}}}}}}

    valid_waraft_segment_guard =
      {:andalso, {:is_tuple, :"$4"},
       {:andalso, {:==, {:tuple_size, :"$4"}, 2},
        {:andalso,
         {:orelse, {:==, {:element, 1, :"$4"}, :waraft_segment},
          {:orelse, {:==, {:element, 1, :"$4"}, :waraft_projection},
           {:==, {:element, 1, :"$4"}, :waraft_apply_projection}}},
         {:andalso, {:is_integer, {:element, 2, :"$4"}},
          {:andalso, {:>, {:element, 2, :"$4"}, 0},
           {:andalso, {:is_integer, :"$5"},
            {:andalso, {:>=, :"$5", 0}, {:andalso, {:is_integer, :"$6"}, {:>=, :"$6", 0}}}}}}}}}

    valid_cold_guard = {:orelse, valid_bitcask_cold_guard, valid_waraft_segment_guard}

    live_hot_ms = [
      {{:"$1", :"$2", :"$3", :_, :_, :_, :_},
       [{:andalso, prefix_guard, {:andalso, live_exp_guard, {:"/=", :"$2", nil}}}], [true]}
    ]

    live_cold_ms = [
      {{:"$1", nil, :"$3", :_, :"$4", :"$5", :"$6"},
       [{:andalso, prefix_guard, {:andalso, live_exp_guard, valid_cold_guard}}], [true]}
    ]

    live_pending_ms = [
      {{:"$1", nil, :"$3", :_, :pending, :_, :_}, [{:andalso, prefix_guard, live_exp_guard}],
       [true]}
    ]

    count =
      :ets.select_count(keydir, live_hot_ms) + :ets.select_count(keydir, live_cold_ms) +
        :ets.select_count(keydir, live_pending_ms)

    if state != nil do
      delete_expired_prefix_entries(state, keydir, prefix, prefix_len, now)
      delete_invalid_cold_prefix_entries(state, keydir, prefix, prefix_len, now)
    end

    count
  end

  defp delete_expired_prefix_entries(state, keydir, prefix, prefix_len, now) do
    expired_ms = [
      {{:"$1", :_, :"$3", :_, :_, :_, :_},
       [
         {:andalso, {:is_binary, :"$1"},
          {:andalso, {:>=, {:byte_size, :"$1"}, prefix_len},
           {:andalso, {:==, {:binary_part, :"$1", 0, prefix_len}, prefix},
            {:andalso, {:"/=", :"$3", 0}, {:"=<", :"$3", now}}}}}
       ], [:"$1"]}
    ]

    keydir
    |> :ets.select(expired_ms)
    |> Enum.each(&delete_prefix_entry(state, keydir, &1))
  end

  defp delete_invalid_cold_prefix_entries(state, keydir, prefix, prefix_len, now) do
    cold_ms = [
      {{:"$1", nil, :"$2", :_, :"$3", :"$4", :"$5"},
       [
         {:andalso, {:is_binary, :"$1"},
          {:andalso, {:>=, {:byte_size, :"$1"}, prefix_len},
           {:andalso, {:==, {:binary_part, :"$1", 0, prefix_len}, prefix},
            {:orelse, {:==, :"$2", 0}, {:>, :"$2", now}}}}}
       ], [{{:"$1", :"$3", :"$4", :"$5"}}]}
    ]

    keydir
    |> :ets.select(cold_ms)
    |> Enum.each(fn {key, fid, off, vsize} ->
      unless fid == :pending or valid_cold_location(fid, off, vsize) or
               valid_waraft_segment_location(fid, off, vsize) do
        delete_prefix_entry(state, keydir, key)
      end
    end)
  end

  defp maybe_delete_expired_prefix_entry(nil, _keydir, _key), do: :ok

  defp maybe_delete_expired_prefix_entry(state, keydir, key),
    do: delete_prefix_entry(state, keydir, key)

  defp delete_prefix_entry(state, keydir, key) do
    track_binary_delete(state, key)
    :ets.delete(keydir, key)
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
         %{instance_ctx: %{keydir_binary_bytes: ref}, index: idx},
         key,
         new_val,
         previous
       )
       when ref != nil do
    new_bytes = offheap_size(key) + offheap_size(new_val)

    old_bytes =
      case previous do
        [{^key, old_val, _, _, _, _, _}] -> offheap_size(key) + offheap_size(old_val)
        _ -> 0
      end

    delta = new_bytes - old_bytes
    if delta != 0, do: :atomics.add(ref, idx + 1, delta)
  end

  defp track_binary_insert(_, _, _, _), do: :ok

  defp fresh_no_expiry_location_records(entries, state, file_id, offset, threshold, lfu) do
    fresh_no_expiry_location_records(
      entries,
      state,
      file_id,
      offset,
      threshold,
      lfu,
      %{},
      [],
      0,
      0
    )
  end

  defp fresh_no_expiry_location_records(
         [],
         _state,
         _file_id,
         _offset,
         _threshold,
         _lfu,
         _seen,
         records,
         binary_delta,
         count
       ) do
    {:ok, records, binary_delta, count}
  end

  defp fresh_no_expiry_location_records(
         [{key, value, 0} | rest],
         %{keydir: keydir} = state,
         file_id,
         offset,
         threshold,
         lfu,
         seen,
         records,
         binary_delta,
         count
       )
       when is_binary(key) and is_binary(value) do
    cond do
      Map.has_key?(seen, key) ->
        :fallback

      :ets.lookup(keydir, key) != [] ->
        :fallback

      not fresh_string_put_without_compound_clear?(keydir, key) ->
        :fallback

      true ->
        ets_value = value_for_ets(value, threshold)

        fresh_no_expiry_location_records(
          rest,
          state,
          file_id,
          offset,
          threshold,
          lfu,
          Map.put(seen, key, true),
          [{key, ets_value, 0, lfu, file_id, offset, byte_size(value)} | records],
          binary_delta + offheap_size(key) + offheap_size(ets_value),
          count + 1
        )
    end
  end

  defp fresh_no_expiry_location_records(
         _entries,
         _state,
         _file_id,
         _offset,
         _threshold,
         _lfu,
         _seen,
         _records,
         _binary_delta,
         _count
       ),
       do: :fallback

  defp fresh_string_put_without_compound_clear?(keydir, key) do
    CompoundKey.internal_key?(key) or :ets.lookup(keydir, CompoundKey.type_key(key)) == []
  end

  defp add_keydir_binary_delta(
         %{instance_ctx: %{keydir_binary_bytes: ref}, index: idx},
         delta
       )
       when ref != nil and delta != 0 do
    :atomics.add(ref, idx + 1, delta)
  end

  defp add_keydir_binary_delta(_, _), do: :ok

  defp adjust_expiry_for_insert(_state, [], 0), do: :ok

  defp adjust_expiry_for_insert(_state, [{_key, _value, 0, _lfu, _fid, _offset, _value_size}], 0),
    do: :ok

  defp adjust_expiry_for_insert(state, previous, expire_at_ms) do
    ExpiryTracker.adjust_for_state(state, ExpiryTracker.entry_expire_at(previous), expire_at_ms)
  end

  # Tracks bytes removed for delete. Must be called BEFORE :ets.delete.
  defp track_binary_delete(%{instance_ctx: %{keydir_binary_bytes: ref}, index: idx} = state, key)
       when ref != nil do
    previous = :ets.lookup(state.keydir, key)
    ExpiryTracker.adjust_for_state(state, ExpiryTracker.entry_expire_at(previous), 0)

    bytes =
      case previous do
        [{^key, val, _, _, _, _, _}] -> offheap_size(key) + offheap_size(val)
        _ -> 0
      end

    if bytes > 0, do: :atomics.sub(ref, idx + 1, bytes)
  end

  defp track_binary_delete(_, _), do: :ok

  # Tracks bytes removed for delete when value is already known (avoids extra lookup).
  defp track_binary_delete(
         %{instance_ctx: %{keydir_binary_bytes: ref}, index: idx} = state,
         key,
         value
       )
       when ref != nil do
    previous = :ets.lookup(state.keydir, key)
    ExpiryTracker.adjust_for_state(state, ExpiryTracker.entry_expire_at(previous), 0)

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
