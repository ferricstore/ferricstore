defmodule Ferricstore.Store.Shard.ETS do
  @moduledoc "ETS keydir operations: lookup, insert, delete, cold-read warming, LFU touch, hot-cache threshold enforcement, and prefix scans."

  alias Ferricstore.ExpiryContext
  alias Ferricstore.Store.Shard.CompoundMemberIndex
  alias Ferricstore.Store.Shard.LogicalKeyIndex
  alias Ferricstore.Store.Shard.NamespaceUsageIndex
  alias Ferricstore.Store.Shard.ZSetIndex
  alias Ferricstore.Store.Shard.ETS.Accounting
  alias Ferricstore.Store.Shard.ETS.PrefixScan

  alias Ferricstore.Store.{
    BlobRef,
    CompoundKey,
    ExpiryTracker,
    Keydir,
    LFU,
    ReadResult,
    ValueCodec
  }

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

  defguardp valid_flow_history_location(file_id, offset, value_size)
            when is_tuple(file_id) and tuple_size(file_id) == 2 and
                   elem(file_id, 0) == :flow_history and is_integer(elem(file_id, 1)) and
                   elem(file_id, 1) >= 0 and is_integer(offset) and offset >= 0 and
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
  #   {:error, :invalid_keydir_entry}
  @spec ets_lookup(map(), binary()) ::
          {:hit, term(), non_neg_integer()}
          | {:cold, term(), non_neg_integer(), non_neg_integer(), non_neg_integer()}
          | :expired
          | :miss
          | {:error, :invalid_keydir_entry}
          | ReadResult.failure()
  @doc false
  def ets_lookup(%{keydir: _keydir} = state, key) do
    ets_lookup(state, key, ExpiryContext.capture())
  end

  @doc false
  @spec ets_lookup(map(), binary(), ExpiryContext.t()) ::
          {:hit, term(), non_neg_integer()}
          | {:cold, term(), non_neg_integer(), non_neg_integer(), non_neg_integer()}
          | :expired
          | :miss
          | {:error, :invalid_keydir_entry}
          | ReadResult.failure()
  def ets_lookup(%{keydir: keydir} = state, key, expiry_context) do
    now = ExpiryContext.now_ms(expiry_context)

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

      [{^key, value, exp, lfu, _fid, _off, _vsize}]
      when is_integer(exp) and exp > now and value != nil ->
        lfu_touch(keydir, key, lfu)
        {:hit, value, exp}

      [{^key, nil, exp, _lfu, :pending, _off, _vsize}] when is_integer(exp) and exp > now ->
        # Background write pending with TTL, value evicted before disk write.
        :miss

      [{^key, nil, exp, _lfu, fid, off, vsize}]
      when is_integer(exp) and exp > now and valid_cold_location(fid, off, vsize) ->
        # Cold key with valid TTL -- disk location known.
        {:cold, fid, off, vsize, exp}

      [{^key, nil, exp, _lfu, {:flow_history, file_id} = fid, off, vsize}]
      when is_integer(exp) and exp > now and is_integer(file_id) and file_id >= 0 and
             is_integer(off) and off >= 0 and is_integer(vsize) and vsize >= 0 ->
        {:cold, fid, off, vsize, exp}

      [{^key, nil, exp, _lfu, fid, off, vsize}]
      when is_integer(exp) and exp > now and valid_waraft_segment_location(fid, off, vsize) ->
        {:cold, fid, off, vsize, exp}

      [{^key, _value, exp, _lfu, _fid, _off, _vsize} = expired_entry]
      when is_integer(exp) and exp > 0 and exp <= now ->
        case ExpiryContext.classify(expiry_context, exp) do
          {:unsafe, :hlc_drift_exceeded} ->
            ReadResult.failure(:hlc_drift_exceeded)

          :expired ->
            delete_exact_entry(state, expired_entry)
            :expired
        end

      [] ->
        :miss

      [_invalid_entry] ->
        {:error, :invalid_keydir_entry}
    end
  end

  @type metadata_location :: :hot | :cold | :pending | :invalid
  @type metadata_lookup ::
          {:live, tuple(), metadata_location()}
          | :expired
          | :miss
          | {:error, :invalid_keydir_entry}
          | ReadResult.failure()

  @doc false
  @spec ets_lookup_metadata(map(), binary()) :: metadata_lookup()
  def ets_lookup_metadata(state, key),
    do: ets_lookup_metadata(state, key, ExpiryContext.capture())

  @doc false
  @spec ets_lookup_metadata(map(), binary(), ExpiryContext.t()) :: metadata_lookup()
  def ets_lookup_metadata(%{keydir: keydir} = state, key, expiry_context) do
    now = ExpiryContext.now_ms(expiry_context)

    case :ets.lookup(keydir, key) do
      [{^key, value, exp, _lfu, _fid, _off, _vsize} = entry]
      when value != nil and is_integer(exp) and (exp == 0 or exp > now) ->
        {:live, entry, :hot}

      [{^key, nil, exp, _lfu, :pending, _off, vsize} = entry]
      when is_integer(exp) and (exp == 0 or exp > now) and is_integer(vsize) and vsize >= 0 ->
        {:live, entry, :pending}

      [{^key, nil, exp, _lfu, fid, off, vsize} = entry]
      when is_integer(exp) and (exp == 0 or exp > now) and
             (valid_cold_location(fid, off, vsize) or
                valid_flow_history_location(fid, off, vsize) or
                valid_waraft_segment_location(fid, off, vsize)) ->
        {:live, entry, :cold}

      [{^key, _value, exp, _lfu, _fid, _off, _vsize} = entry]
      when is_integer(exp) and (exp == 0 or exp > now) ->
        {:live, entry, :invalid}

      [{^key, _value, exp, _lfu, _fid, _off, _vsize} = entry]
      when is_integer(exp) and exp > 0 and exp <= now ->
        case ExpiryContext.classify(expiry_context, exp) do
          {:unsafe, :hlc_drift_exceeded} ->
            ReadResult.failure(:hlc_drift_exceeded)

          :expired ->
            delete_exact_entry(state, entry)
            :expired
        end

      [] ->
        :miss

      [_invalid_entry] ->
        {:error, :invalid_keydir_entry}
    end
  end

  @spec ets_lookup_warm_result(map(), binary()) ::
          {:hit, term(), non_neg_integer()}
          | :expired
          | :miss
          | {:error, :cold_read_failed}
          | ReadResult.failure()
  @doc false
  def ets_lookup_warm_result(state, key) do
    ets_lookup_warm_result(state, key, ExpiryContext.capture())
  end

  @doc false
  @spec ets_lookup_warm_result(map(), binary(), ExpiryContext.t()) ::
          {:hit, term(), non_neg_integer()}
          | :expired
          | :miss
          | {:error, :cold_read_failed}
          | ReadResult.failure()
  def ets_lookup_warm_result(state, key, expiry_context) do
    case ets_lookup(state, key, expiry_context) do
      {:cold, fid, off, vsize, exp} when valid_waraft_segment_location(fid, off, vsize) ->
        case PrefixScan.read_waraft_segment_value(state, fid, key) do
          {:ok, value} when is_binary(value) ->
            cold_read_warm_ets(state, key, value)
            {:hit, value, exp}

          _ ->
            {:error, :cold_read_failed}
        end

      {:cold, fid, off, _vsize, exp} ->
        p = file_path(state.shard_data_path, fid)

        case PrefixScan.read_cold_async(state, p, off, key) do
          {:ok, value} when is_binary(value) ->
            cold_read_warm_ets(state, key, value)
            {:hit, value, exp}

          _ ->
            {:error, :cold_read_failed}
        end

      {:error, :invalid_keydir_entry} ->
        {:error, :cold_read_failed}

      other ->
        other
    end
  end

  @spec pending_cold?(map(), binary()) :: boolean()
  @doc false
  def pending_cold?(%{keydir: keydir} = state, key) do
    expiry_context = ExpiryContext.capture()
    now = ExpiryContext.now_ms(expiry_context)

    case :ets.lookup(keydir, key) do
      [{^key, nil, exp, _lfu, :pending, _off, _vsize}]
      when is_integer(exp) and (exp == 0 or exp > now) ->
        true

      [{^key, nil, exp, _lfu, :pending, _off, _vsize} = entry]
      when is_integer(exp) and exp > 0 and exp <= now ->
        case ExpiryContext.classify(expiry_context, exp) do
          {:unsafe, :hlc_drift_exceeded} ->
            true

          :expired ->
            delete_exact_entry(state, entry)
            false
        end

      [{^key, nil, _invalid_exp, _lfu, :pending, _off, _vsize}] ->
        true

      _ ->
        false
    end
  end

  @spec prefix_has_pending_cold?(:ets.tid(), binary()) :: boolean()
  @doc false
  def prefix_has_pending_cold?(keydir, prefix) do
    now = ExpiryContext.capture() |> ExpiryContext.safe_expiry_cutoff_ms()
    prefix_len = byte_size(prefix)

    ms = [
      {{:"$1", nil, :"$2", :_, :pending, :_, :_},
       [
         {:andalso, {:is_binary, :"$1"},
          {:andalso, {:>=, {:byte_size, :"$1"}, prefix_len},
           {:andalso, {:==, {:binary_part, :"$1", 0, prefix_len}, prefix},
            {:orelse, {:not, {:is_integer, :"$2"}},
             {:orelse, {:"=<", :"$2", 0}, {:>, :"$2", now}}}}}}
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

    true =
      :ets.insert(
        state.keydir,
        {key, v, expire_at_ms, LFU.initial(), :pending, original_fid, original_vsize}
      )

    CompoundMemberIndex.put(compound_member_index(state), key, expire_at_ms)
    :ok = project_logical_key_put(state, key, value, expire_at_ms)
    :ok = project_namespace_usage_put(state, key, value, expire_at_ms)
    true
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

    true =
      :ets.insert(
        state.keydir,
        {key, v, expire_at_ms, LFU.initial(), file_id, offset, value_size}
      )

    CompoundMemberIndex.put(compound_member_index(state), key, expire_at_ms)
    :ok = project_logical_key_put(state, key, value, expire_at_ms)
    :ok = project_namespace_usage_put(state, key, value, expire_at_ms)
    true
  end

  defp compound_member_index(state) do
    Map.get(state, :compound_member_index) || Map.get(state, :compound_member_index_name)
  end

  defp project_logical_key_put(state, key, value, expire_at_ms) do
    LogicalKeyIndex.put(
      Map.get(state, :logical_key_index) || Map.get(state, :logical_key_index_name),
      Map.get(state, :logical_key_slots) || Map.get(state, :logical_key_slots_name),
      key,
      value,
      expire_at_ms
    )
  end

  defp project_logical_key_delete(state, key) do
    LogicalKeyIndex.delete(
      Map.get(state, :logical_key_index) || Map.get(state, :logical_key_index_name),
      Map.get(state, :logical_key_slots) || Map.get(state, :logical_key_slots_name),
      key
    )
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

        Enum.each(entries, fn {key, value, expire_at_ms} ->
          CompoundMemberIndex.put(compound_member_index(state), key, expire_at_ms)
          :ok = project_logical_key_put(state, key, value, expire_at_ms)
          :ok = project_namespace_usage_put(state, key, value, expire_at_ms)
        end)

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
    CompoundMemberIndex.delete(compound_member_index(state), key)
    :ok = project_logical_key_delete(state, key)
    :ok = project_namespace_usage_delete(state, key)
    true
  end

  @spec delete_exact_entry(map(), tuple()) :: boolean()
  @doc false
  def delete_exact_entry(state, entry) do
    delete_exact_entry(state, entry, true)
  end

  @spec delete_exact_entry(map(), tuple(), boolean()) :: boolean()
  @doc false
  def delete_exact_entry(state, entry, update_logical_projection?)
      when is_boolean(update_logical_projection?) do
    if Keydir.delete_exact(state.keydir, entry) do
      delete_apply_projection_cache_entry(state, entry)
      maybe_run_after_exact_keydir_delete_hook(state, entry)
      Accounting.track_binary_delete_entry(state, entry)
      key = elem(entry, 0)
      CompoundMemberIndex.delete(compound_member_index(state), key)
      maybe_project_logical_key_delete(state, key, update_logical_projection?)
      :ok = project_namespace_usage_delete(state, key)
      :ok = ZSetIndex.reconcile_exact_delete(state, entry)
      restore_current_derived_indexes(state, key, update_logical_projection?)
      true
    else
      false
    end
  end

  defp maybe_project_logical_key_delete(state, key, true),
    do: project_logical_key_delete(state, key)

  defp maybe_project_logical_key_delete(_state, _key, false), do: :ok

  defp delete_apply_projection_cache_entry(
         %{index: shard_index, instance_ctx: %{data_dir: data_dir}},
         {key, _value, _expire_at_ms, _lfu, {:waraft_apply_projection, index}, _offset,
          _value_size}
       )
       when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 and
              is_binary(key) and is_integer(index) and index > 0 do
    Ferricstore.Raft.WARaftSegmentReader.delete_apply_projection_entries(data_dir, shard_index, [
      {index, key}
    ])

    :ok
  rescue
    _ -> :ok
  end

  defp delete_apply_projection_cache_entry(_state, _entry), do: :ok

  defp maybe_run_after_exact_keydir_delete_hook(state, entry) do
    case Process.get(:ferricstore_after_exact_keydir_delete_hook) do
      fun when is_function(fun, 2) -> fun.(state, entry)
      _ -> :ok
    end
  end

  defp restore_current_derived_indexes(state, key, update_logical_projection?) do
    case :ets.lookup(state.keydir, key) do
      [{^key, value, expire_at_ms, _lfu, _file_id, _offset, _value_size}]
      when is_integer(expire_at_ms) and expire_at_ms >= 0 ->
        CompoundMemberIndex.put(compound_member_index(state), key, expire_at_ms)

        maybe_project_logical_key_put(
          state,
          key,
          value,
          expire_at_ms,
          update_logical_projection?
        )

        :ok = project_namespace_usage_put(state, key, value, expire_at_ms)

      _missing_or_invalid ->
        :ok
    end
  rescue
    ArgumentError -> :ok
  end

  defp maybe_project_logical_key_put(state, key, value, expire_at_ms, true),
    do: project_logical_key_put(state, key, value, expire_at_ms)

  defp maybe_project_logical_key_put(_state, _key, _value, _expire_at_ms, false), do: :ok

  defp project_namespace_usage_put(state, key, value, expire_at_ms) do
    NamespaceUsageIndex.put(
      namespace_usage_index(state),
      namespace_usage_expiry(state),
      key,
      value,
      expire_at_ms,
      blob_threshold_bytes: Map.get(state, :blob_side_channel_threshold_bytes, 0)
    )
  end

  defp project_namespace_usage_delete(state, key) do
    NamespaceUsageIndex.delete(
      namespace_usage_index(state),
      namespace_usage_expiry(state),
      key
    )
  end

  defp namespace_usage_index(state),
    do: Map.get(state, :namespace_usage_index) || Map.get(state, :namespace_usage_index_name)

  defp namespace_usage_expiry(state),
    do: Map.get(state, :namespace_usage_expiry) || Map.get(state, :namespace_usage_expiry_name)

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
  @spec warm_from_store(map(), binary()) :: binary() | nil | ReadResult.failure()
  @doc false
  def warm_from_store(state, key) do
    case ets_lookup_warm_result(state, key) do
      {:hit, value, _exp} -> value
      {:error, {:storage_read_failed, _reason}} = failure -> failure
      {:error, reason} -> ReadResult.failure(reason)
      result when result in [:expired, :miss] -> nil
    end
  end

  # v2: cold read meta via async pread using disk location from ETS 7-tuple.
  # Applies the hot_cache_max_value_size threshold when re-warming ETS.
  @spec warm_meta_from_store(map(), binary()) ::
          {binary(), non_neg_integer()} | nil | ReadResult.failure()
  @doc false
  def warm_meta_from_store(state, key) do
    case ets_lookup_warm_result(state, key) do
      {:hit, value, exp} -> {value, exp}
      {:error, {:storage_read_failed, _reason}} = failure -> failure
      {:error, reason} -> ReadResult.failure(reason)
      result when result in [:expired, :miss] -> nil
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

  @spec prefix_scan_entries(map() | :ets.tid(), binary(), binary() | nil) ::
          [{binary(), binary()}] | Ferricstore.Store.ReadResult.failure()
  @doc false
  def prefix_scan_entries(state_or_keydir, prefix, shard_data_path),
    do: PrefixScan.prefix_scan_entries(state_or_keydir, prefix, shard_data_path)

  @doc false
  def prefix_scan_entries_bounded(state, prefix, shard_data_path, limits),
    do: PrefixScan.prefix_scan_entries_bounded(state, prefix, shard_data_path, limits)

  @doc false
  def prefix_scan_entries_slice(state, prefix, shard_data_path, start, count, total),
    do:
      PrefixScan.prefix_scan_entries_slice(
        state,
        prefix,
        shard_data_path,
        start,
        count,
        total
      )

  @spec prefix_scan_fields(map() | :ets.tid(), binary()) :: [binary()]
  @doc false
  def prefix_scan_fields(state_or_keydir, prefix),
    do: PrefixScan.prefix_scan_fields(state_or_keydir, prefix)

  @spec prefix_count_entries(map() | :ets.tid(), binary()) ::
          non_neg_integer() | ReadResult.failure()
  @doc false
  def prefix_count_entries(state_or_keydir, prefix),
    do: PrefixScan.prefix_count_entries(state_or_keydir, prefix)

  @doc false
  def prefix_collect_keys(keydir, prefix), do: PrefixScan.prefix_collect_keys(keydir, prefix)

  @doc false
  def prefix_each_key(keydir, prefix, fun), do: PrefixScan.prefix_each_key(keydir, prefix, fun)

  @doc false
  def prefix_collect_keys(keydir, prefix, limit),
    do: PrefixScan.prefix_collect_keys(keydir, prefix, limit)

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
  def coerce_float(v) when is_integer(v), do: ValueCodec.number_to_float(v)
  def coerce_float(v) when is_binary(v), do: ValueCodec.parse_float(v)

  # -------------------------------------------------------------------
  # Private helpers
  # -------------------------------------------------------------------

  # LFU touch with time-based decay.
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
  def track_binary_delete(state, key), do: Accounting.track_binary_delete(state, key)

  # Tracks bytes removed for delete when value is already known (avoids extra lookup).
  def track_binary_delete(state, key, value),
    do: Accounting.track_binary_delete(state, key, value)

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
