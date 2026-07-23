defmodule Ferricstore.Store.Shard.NamespaceUsageIndex do
  @moduledoc false

  alias Ferricstore.Store.{BlobRef, CompoundKey}

  alias Ferricstore.Store.Shard.{
    NamespaceUsageDetails,
    NamespaceUsageFlowAccounting,
    NamespaceUsageScopes
  }

  @lock_key :"$ferricstore_namespace_usage_write_lock"
  @active_key :"$ferricstore_namespace_usage_active"
  @lock_max_sleep_ms 10

  @type table_ref :: atom() | :ets.tid() | nil
  @type usage :: %{
          keys: non_neg_integer(),
          bytes: non_neg_integer(),
          flow_count: non_neg_integer()
        }

  @spec table_names(atom(), non_neg_integer()) :: {atom(), atom()}
  def table_names(instance_name, shard_index)
      when is_atom(instance_name) and is_integer(shard_index) and shard_index >= 0 do
    {
      :"ferricstore_namespace_usage_#{instance_name}_#{shard_index}",
      :"ferricstore_namespace_usage_expiry_#{instance_name}_#{shard_index}"
    }
  end

  @spec ensure_tables!(atom(), atom()) :: {atom(), atom()}
  def ensure_tables!(usage, expiry) when is_atom(usage) and is_atom(expiry) do
    ensure_table!(usage)
    ensure_table!(expiry)
    {usage, expiry}
  end

  @spec active?(table_ref()) :: boolean()
  def active?(usage) do
    with {:ok, usage} <- fetch_table(usage) do
      active_tid?(usage)
    else
      :missing -> false
    end
  rescue
    ArgumentError -> false
  end

  @spec scope_ready?(table_ref(), binary()) :: boolean()
  def scope_ready?(usage, scope) when is_binary(scope) do
    with {:ok, usage} <- fetch_table(usage) do
      :ets.lookup(usage, {:tracked, scope}) == [{{:tracked, scope}, true}]
    else
      :missing -> false
    end
  rescue
    ArgumentError -> false
  end

  def scope_ready?(_usage, _scope), do: false

  @spec invalidate(table_ref(), table_ref()) :: :ok | {:error, term()}
  def invalidate(usage, expiry) do
    with {:ok, usage} <- fetch_table(usage),
         {:ok, expiry} <- fetch_table(expiry) do
      with_write_lock(usage, fn -> clear_catalog_unlocked(usage, expiry) end)
    else
      :missing -> :ok
    end
  rescue
    ArgumentError -> {:error, :namespace_usage_index_unavailable}
  end

  @spec reset(table_ref(), table_ref()) :: :ok | {:error, term()}
  def reset(usage, expiry) do
    with {:ok, usage} <- fetch_table(usage),
         {:ok, expiry} <- fetch_table(expiry) do
      with_write_lock(usage, fn ->
        scopes = NamespaceUsageScopes.tracked(usage)
        clear_catalog_unlocked(usage, expiry)
        NamespaceUsageScopes.initialize(usage, scopes)
      end)
    else
      :missing -> :ok
    end
  rescue
    ArgumentError -> {:error, :namespace_usage_index_unavailable}
  end

  @spec rebuild_tracked(table_ref(), table_ref(), table_ref(), keyword()) ::
          :ok | {:error, term()}
  def rebuild_tracked(usage, expiry, keydir, opts) when is_list(opts) do
    with {:ok, usage} <- fetch_table(usage),
         {:ok, expiry} <- fetch_table(expiry),
         {:ok, keydir} <- fetch_table(keydir),
         {:ok, now_ms, blob_threshold, entry_bytes_fun} <- rebuild_options(opts) do
      with_write_lock(usage, fn ->
        scopes = NamespaceUsageScopes.tracked(usage)

        rebuild_catalog_unlocked(
          usage,
          expiry,
          keydir,
          scopes,
          now_ms,
          blob_threshold,
          entry_bytes_fun
        )
      end)
    else
      :missing -> :ok
      {:error, _reason} = error -> error
    end
  rescue
    ArgumentError -> {:error, :namespace_usage_index_unavailable}
  end

  @spec rebuild_scope(table_ref(), table_ref(), table_ref(), binary(), keyword()) ::
          :ok | {:error, term()}
  def rebuild_scope(usage, expiry, keydir, scope, opts)
      when is_binary(scope) and is_list(opts) do
    with {:ok, usage} <- fetch_table(usage),
         {:ok, expiry} <- fetch_table(expiry),
         {:ok, keydir} <- fetch_table(keydir),
         {:ok, now_ms, blob_threshold, entry_bytes_fun} <- rebuild_options(opts) do
      with_write_lock(usage, fn ->
        if active_tid?(usage) do
          rebuild_scope_aggregate_unlocked(usage, scope)
        else
          rebuild_catalog_unlocked(
            usage,
            expiry,
            keydir,
            [scope],
            now_ms,
            blob_threshold,
            entry_bytes_fun
          )
        end
      end)
    else
      :missing -> {:error, :namespace_usage_index_unavailable}
      {:error, _reason} = error -> error
    end
  rescue
    ArgumentError -> {:error, :namespace_usage_index_unavailable}
  end

  @spec put(table_ref(), table_ref(), binary(), term(), non_neg_integer()) ::
          :ok | {:error, term()}
  def put(usage, expiry, storage_key, value, expire_at_ms) do
    put(usage, expiry, storage_key, value, expire_at_ms, [])
  end

  @spec put(table_ref(), table_ref(), binary(), term(), non_neg_integer(), keyword()) ::
          :ok | {:error, term()}
  def put(usage, expiry, storage_key, value, expire_at_ms, opts)
      when is_binary(storage_key) and is_integer(expire_at_ms) and expire_at_ms >= 0 and
             is_list(opts) do
    with {:ok, usage} <- fetch_table(usage),
         {:ok, expiry} <- fetch_table(expiry) do
      if active_tid?(usage) do
        with {:ok, bytes} <- entry_bytes(storage_key, value, opts) do
          logical_key = CompoundKey.extract_redis_key(storage_key)

          with_write_lock(usage, fn ->
            flow_scope = NamespaceUsageFlowAccounting.scope_for_put(usage, storage_key, value)
            remove_entry_unlocked(usage, expiry, storage_key)

            add_entry_unlocked(
              usage,
              expiry,
              storage_key,
              logical_key,
              bytes,
              expire_at_ms,
              flow_scope
            )
          end)
        end
      else
        :ok
      end
    else
      :missing -> :ok
    end
  rescue
    ArgumentError -> {:error, :namespace_usage_index_unavailable}
  end

  def put(_usage, _expiry, storage_key, _value, expire_at_ms, _opts),
    do: {:error, {:invalid_namespace_usage_entry, storage_key, expire_at_ms}}

  @spec put_exact_bytes(
          table_ref(),
          table_ref(),
          binary(),
          non_neg_integer(),
          non_neg_integer()
        ) :: :ok | {:error, term()}
  def put_exact_bytes(usage, expiry, storage_key, bytes, expire_at_ms)
      when is_binary(storage_key) and is_integer(bytes) and bytes >= byte_size(storage_key) and
             is_integer(expire_at_ms) and expire_at_ms >= 0 do
    with {:ok, usage} <- fetch_table(usage),
         {:ok, expiry} <- fetch_table(expiry) do
      if active_tid?(usage) do
        logical_key = CompoundKey.extract_redis_key(storage_key)

        with_write_lock(usage, fn ->
          flow_scope =
            NamespaceUsageFlowAccounting.existing_or_unknown_scope(usage, storage_key)

          remove_entry_unlocked(usage, expiry, storage_key)

          add_entry_unlocked(
            usage,
            expiry,
            storage_key,
            logical_key,
            bytes,
            expire_at_ms,
            flow_scope
          )
        end)
      else
        :ok
      end
    else
      :missing -> :ok
    end
  rescue
    ArgumentError -> {:error, :namespace_usage_index_unavailable}
  end

  def put_exact_bytes(_usage, _expiry, storage_key, bytes, expire_at_ms),
    do: {:error, {:invalid_namespace_usage_exact_entry, storage_key, bytes, expire_at_ms}}

  @spec delete(table_ref(), table_ref(), binary()) :: :ok | {:error, term()}
  def delete(usage, expiry, storage_key) when is_binary(storage_key) do
    with {:ok, usage} <- fetch_table(usage),
         {:ok, expiry} <- fetch_table(expiry) do
      if active_tid?(usage) do
        with_write_lock(usage, fn -> remove_entry_unlocked(usage, expiry, storage_key) end)
      else
        :ok
      end
    else
      :missing -> :ok
    end
  rescue
    ArgumentError -> {:error, :namespace_usage_index_unavailable}
  end

  def delete(_usage, _expiry, storage_key),
    do: {:error, {:invalid_namespace_usage_key, storage_key}}

  @spec usage(table_ref(), table_ref(), binary(), non_neg_integer()) ::
          {:ok, usage()} | :unavailable | {:error, term()}
  def usage(usage, expiry, scope, now_ms)
      when is_binary(scope) and is_integer(now_ms) and now_ms >= 0 do
    with {:ok, usage} <- fetch_table(usage),
         {:ok, expiry} <- fetch_table(expiry) do
      with_write_lock(usage, fn ->
        purge_expired_unlocked(usage, expiry, now_ms)

        case :ets.lookup(usage, {:tracked, scope}) do
          [{{:tracked, ^scope}, true}] -> {:ok, scope_usage(usage, scope)}
          _missing -> :unavailable
        end
      end)
    else
      :missing -> :unavailable
    end
  rescue
    ArgumentError -> :unavailable
  end

  def usage(_usage, _expiry, _scope, _now_ms),
    do: {:error, :invalid_namespace_usage_request}

  @spec details(table_ref(), table_ref(), binary(), [binary()], non_neg_integer()) ::
          {:ok, map()} | :unavailable | {:error, term()}
  def details(usage, expiry, scope, logical_keys, now_ms)
      when is_binary(scope) and is_list(logical_keys) and is_integer(now_ms) and now_ms >= 0 do
    with {:ok, usage} <- fetch_table(usage),
         {:ok, expiry} <- fetch_table(expiry) do
      with_write_lock(usage, fn ->
        purge_expired_unlocked(usage, expiry, now_ms)

        case :ets.lookup(usage, {:tracked, scope}) do
          [{{:tracked, ^scope}, true}] ->
            aggregate = scope_usage(usage, scope)
            {:ok, NamespaceUsageDetails.build(usage, logical_keys, aggregate)}

          _missing ->
            :unavailable
        end
      end)
    else
      :missing -> :unavailable
    end
  rescue
    ArgumentError -> :unavailable
  end

  def details(_usage, _expiry, _scope, _logical_keys, _now_ms),
    do: {:error, :invalid_namespace_usage_details}

  defp ensure_table!(name) do
    case :ets.whereis(name) do
      :undefined ->
        :ets.new(name, [
          :ordered_set,
          :public,
          :named_table,
          {:read_concurrency, true},
          {:write_concurrency, :auto},
          {:decentralized_counters, true}
        ])

      _existing ->
        name
    end
  rescue
    ArgumentError -> name
  end

  defp fetch_table(table) when is_reference(table), do: {:ok, table}

  defp fetch_table(table) when is_atom(table) do
    case :ets.whereis(table) do
      :undefined -> :missing
      tid -> {:ok, tid}
    end
  end

  defp fetch_table(_table), do: :missing

  defp rebuild_options(opts) do
    now_ms = Keyword.get(opts, :now_ms, 0)
    blob_threshold = Keyword.get(opts, :blob_threshold_bytes, 0)
    entry_bytes_fun = Keyword.get(opts, :entry_bytes_fun)

    if is_integer(now_ms) and now_ms >= 0 and is_integer(blob_threshold) and
         blob_threshold >= 0 and (is_nil(entry_bytes_fun) or is_function(entry_bytes_fun, 1)) do
      {:ok, now_ms, blob_threshold, entry_bytes_fun}
    else
      {:error, :invalid_namespace_usage_rebuild}
    end
  end

  defp rebuild_catalog_unlocked(
         usage,
         expiry,
         keydir,
         scopes,
         now_ms,
         blob_threshold,
         entry_bytes_fun
       ) do
    clear_catalog_unlocked(usage, expiry)
    NamespaceUsageScopes.initialize(usage, scopes)

    result =
      :ets.foldl(
        fn
          _row, {:error, _reason} = error ->
            error

          row, :ok ->
            rebuild_row(usage, expiry, row, now_ms, blob_threshold, entry_bytes_fun)
        end,
        :ok,
        keydir
      )

    case result do
      :ok ->
        :ok

      {:error, _reason} = error ->
        clear_catalog_unlocked(usage, expiry)
        error
    end
  end

  defp rebuild_row(
         usage,
         expiry,
         {storage_key, value, expire_at_ms, _lfu, _file_id, _offset, value_size} = row,
         now_ms,
         blob_threshold,
         entry_bytes_fun
       )
       when is_binary(storage_key) and is_integer(expire_at_ms) and expire_at_ms >= 0 and
              is_integer(value_size) and value_size >= 0 do
    observe_rebuild_row(storage_key)

    if live?(expire_at_ms, now_ms) do
      logical_key = CompoundKey.extract_redis_key(storage_key)

      case rebuild_entry_footprint(
             entry_bytes_fun,
             row,
             storage_key,
             value,
             value_size,
             blob_threshold
           ) do
        {:ok, bytes, flow_scope} ->
          add_entry_unlocked(
            usage,
            expiry,
            storage_key,
            logical_key,
            bytes,
            expire_at_ms,
            flow_scope
          )

        {:error, _reason} = error ->
          error
      end
    else
      :ok
    end
  end

  defp rebuild_row(_usage, _expiry, row, _now_ms, _blob_threshold, _entry_bytes_fun),
    do: {:error, {:invalid_namespace_usage_keydir_row, row}}

  defp rebuild_entry_footprint(nil, _row, storage_key, value, value_size, blob_threshold) do
    bytes = byte_size(storage_key) + logical_value_size(value, value_size, blob_threshold)
    {:ok, bytes, NamespaceUsageFlowAccounting.decoded_scope(storage_key, value)}
  end

  defp rebuild_entry_footprint(
         entry_bytes_fun,
         row,
         storage_key,
         value,
         _value_size,
         _threshold
       ) do
    case entry_bytes_fun.(row) do
      bytes when is_integer(bytes) and bytes >= 0 ->
        {:ok, bytes, NamespaceUsageFlowAccounting.decoded_scope(storage_key, value)}

      {:ok, bytes} when is_integer(bytes) and bytes >= 0 ->
        {:ok, bytes, NamespaceUsageFlowAccounting.decoded_scope(storage_key, value)}

      %{bytes: bytes, flow_scope: flow_scope}
      when is_integer(bytes) and bytes >= 0 and
             (is_binary(flow_scope) or flow_scope in [:unknown, :unscoped, nil]) ->
        {:ok, bytes, flow_scope}

      {:ok, %{bytes: bytes, flow_scope: flow_scope}}
      when is_integer(bytes) and bytes >= 0 and
             (is_binary(flow_scope) or flow_scope in [:unknown, :unscoped, nil]) ->
        {:ok, bytes, flow_scope}

      {:error, _reason} = error ->
        error

      invalid ->
        {:error, {:invalid_namespace_usage_entry_bytes, invalid}}
    end
  rescue
    error -> {:error, {:namespace_usage_entry_bytes_failed, Exception.message(error)}}
  catch
    kind, reason -> {:error, {:namespace_usage_entry_bytes_failed, kind, reason}}
  end

  if Mix.env() == :test do
    defp observe_rebuild_row(storage_key) do
      case Process.get(:ferricstore_namespace_usage_rebuild_visit_hook) do
        hook when is_function(hook, 1) -> hook.(storage_key)
        _missing -> :ok
      end
    end
  else
    defp observe_rebuild_row(_storage_key), do: :ok
  end

  defp rebuild_scope_aggregate_unlocked(usage, scope) do
    :ets.insert(usage, NamespaceUsageScopes.metadata_rows(scope))

    {keys, bytes} =
      :ets.foldl(
        fn
          {{:logical, logical_key}, countable, logical_bytes, _entries, _plain, _internal},
          {keys, bytes}
          when is_binary(logical_key) and is_integer(countable) and countable >= 0 and
                 is_integer(logical_bytes) and logical_bytes >= 0 ->
            if NamespaceUsageScopes.in_scope?(logical_key, scope) do
              {keys + if(countable > 0, do: 1, else: 0), bytes + logical_bytes}
            else
              {keys, bytes}
            end

          _other, aggregate ->
            aggregate
        end,
        {0, 0},
        usage
      )

    :ets.insert(usage, {{:scope, scope}, keys, bytes})

    :ets.insert(
      usage,
      {{:flow_scope, scope}, NamespaceUsageFlowAccounting.count_for_scope(usage, scope)}
    )

    :ok
  end

  defp add_entry_unlocked(
         usage,
         expiry,
         storage_key,
         logical_key,
         bytes,
         expire_at_ms,
         flow_scope
       ) do
    internal? = CompoundKey.internal_key?(storage_key)
    countable? = not internal? or match?(<<"T:", _rest::binary>>, storage_key)
    transfer_base = max(bytes - source_key_component_bytes(storage_key, logical_key), 0)

    {countable_before, logical_bytes, entries, plain_entries, internal_entries} =
      logical_usage(usage, logical_key)

    countable_after = countable_before + if(countable?, do: 1, else: 0)

    :ets.insert(usage, [
      {
        {:entry, storage_key},
        logical_key,
        bytes,
        expire_at_ms,
        countable?,
        internal?,
        transfer_base
      },
      {
        {:logical, logical_key},
        countable_after,
        logical_bytes + bytes,
        entries + 1,
        plain_entries + if(internal?, do: 0, else: 1),
        internal_entries + if(internal?, do: 1, else: 0)
      },
      {{:transfer, logical_key, transfer_base, storage_key}, true}
    ])

    if expire_at_ms > 0,
      do: :ets.insert(expiry, {{expire_at_ms, storage_key}, true})

    key_delta = if countable_before == 0 and countable_after > 0, do: 1, else: 0
    update_matching_scopes(usage, logical_key, key_delta, bytes)
    NamespaceUsageFlowAccounting.add(usage, storage_key, flow_scope)
  end

  defp remove_entry_unlocked(usage, expiry, storage_key) do
    NamespaceUsageFlowAccounting.remove(usage, storage_key)

    case :ets.take(usage, {:entry, storage_key}) do
      [
        {
          {:entry, ^storage_key},
          logical_key,
          bytes,
          expire_at_ms,
          countable?,
          internal?,
          transfer_base
        }
      ] ->
        if expire_at_ms > 0, do: :ets.delete(expiry, {expire_at_ms, storage_key})
        :ets.delete(usage, {:transfer, logical_key, transfer_base, storage_key})

        {countable_before, logical_bytes, entries, plain_entries, internal_entries} =
          logical_usage(usage, logical_key)

        countable_after = countable_before - if(countable?, do: 1, else: 0)
        logical_bytes = logical_bytes - bytes
        entries = entries - 1
        plain_entries = plain_entries - if(internal?, do: 0, else: 1)
        internal_entries = internal_entries - if(internal?, do: 1, else: 0)

        if Enum.any?(
             [countable_after, logical_bytes, entries, plain_entries, internal_entries],
             &(&1 < 0)
           ) do
          raise "namespace usage logical aggregate underflow"
        end

        if entries == 0 do
          :ets.delete(usage, {:logical, logical_key})
        else
          :ets.insert(
            usage,
            {
              {:logical, logical_key},
              countable_after,
              logical_bytes,
              entries,
              plain_entries,
              internal_entries
            }
          )
        end

        key_delta = if countable_before > 0 and countable_after == 0, do: -1, else: 0
        update_matching_scopes(usage, logical_key, key_delta, -bytes)

      [] ->
        :ok
    end
  end

  defp logical_usage(usage, logical_key) do
    case :ets.lookup(usage, {:logical, logical_key}) do
      [
        {
          {:logical, ^logical_key},
          countable,
          bytes,
          entries,
          plain_entries,
          internal_entries
        }
      ] ->
        {countable, bytes, entries, plain_entries, internal_entries}

      [] ->
        {0, 0, 0, 0, 0}
    end
  end

  defp update_matching_scopes(usage, logical_key, key_delta, byte_delta) do
    usage
    |> NamespaceUsageScopes.tracked_for_key(logical_key)
    |> Enum.each(&update_scope_usage(usage, &1, key_delta, byte_delta))

    :ok
  end

  defp update_scope_usage(usage, scope, key_delta, byte_delta) do
    %{keys: keys, bytes: bytes} = scope_usage(usage, scope)
    keys = keys + key_delta
    bytes = bytes + byte_delta

    if keys < 0 or bytes < 0 do
      raise "namespace usage scope aggregate underflow"
    end

    :ets.insert(usage, {{:scope, scope}, keys, bytes})
    :ok
  end

  defp scope_usage(usage, scope) do
    {keys, bytes} =
      case :ets.lookup(usage, {:scope, scope}) do
        [{{:scope, ^scope}, keys, bytes}]
        when is_integer(keys) and keys >= 0 and is_integer(bytes) and bytes >= 0 ->
          {keys, bytes}

        _missing_or_invalid ->
          {0, 0}
      end

    %{keys: keys, bytes: bytes, flow_count: NamespaceUsageFlowAccounting.count(usage, scope)}
  end

  defp purge_expired_unlocked(usage, expiry, now_ms) do
    purge_expired_unlocked(usage, expiry, :ets.first(expiry), now_ms)
  end

  defp purge_expired_unlocked(_usage, _expiry, :"$end_of_table", _now_ms), do: :ok

  defp purge_expired_unlocked(_usage, _expiry, {expire_at_ms, _storage_key}, now_ms)
       when expire_at_ms > now_ms,
       do: :ok

  defp purge_expired_unlocked(usage, expiry, {expire_at_ms, storage_key} = expiry_key, now_ms) do
    next = :ets.next(expiry, expiry_key)

    case :ets.lookup(usage, {:entry, storage_key}) do
      [
        {
          {:entry, ^storage_key},
          _logical_key,
          _bytes,
          ^expire_at_ms,
          _countable,
          _internal,
          _transfer_base
        }
      ] ->
        remove_entry_unlocked(usage, expiry, storage_key)

      _stale ->
        :ets.delete(expiry, expiry_key)
    end

    purge_expired_unlocked(usage, expiry, next, now_ms)
  end

  defp clear_catalog_unlocked(usage, expiry) do
    :ets.select_delete(usage, [
      {{{:entry, :_}, :_, :_, :_, :_, :_, :_}, [], [true]},
      {{{:logical, :_}, :_, :_, :_, :_, :_}, [], [true]},
      {{{:flow, :_}, :_}, [], [true]},
      {{{:transfer, :_, :_, :_}, :_}, [], [true]},
      {{{:scope, :_}, :_, :_}, [], [true]},
      {{{:flow_scope, :_}, :_}, [], [true]},
      {{{:tracked, :_}, :_}, [], [true]},
      {{@active_key, :_}, [], [true]}
    ])

    :ets.delete_all_objects(expiry)
    :ok
  end

  defp source_key_component_bytes(storage_key, logical_key) do
    if CompoundKey.internal_key?(storage_key),
      do: CompoundKey.encoded_redis_key_size(logical_key),
      else: byte_size(logical_key)
  end

  defp live?(0, _now_ms), do: true
  defp live?(expire_at_ms, now_ms), do: expire_at_ms > now_ms

  defp entry_bytes(storage_key, value, opts) do
    blob_threshold = Keyword.get(opts, :blob_threshold_bytes, 0)
    explicit_size = Keyword.get(opts, :logical_value_size)

    cond do
      is_integer(explicit_size) and explicit_size >= 0 ->
        {:ok, byte_size(storage_key) + explicit_size}

      is_integer(blob_threshold) and blob_threshold >= 0 ->
        {:ok,
         byte_size(storage_key) + logical_value_size(value, value_size(value), blob_threshold)}

      true ->
        {:error, :invalid_namespace_usage_value_size}
    end
  end

  defp value_size(value) when is_binary(value), do: byte_size(value)
  defp value_size(value) when is_integer(value), do: byte_size(Integer.to_string(value))
  defp value_size(value) when is_float(value), do: byte_size(Float.to_string(value))
  defp value_size(_value), do: 0

  defp logical_value_size(value, _fallback, blob_threshold)
       when is_binary(value) and blob_threshold > 0 do
    case BlobRef.decode(value) do
      {:ok, %BlobRef{size: size}} -> size
      :error -> byte_size(value)
    end
  end

  defp logical_value_size(value, _fallback, _blob_threshold) when is_binary(value),
    do: byte_size(value)

  defp logical_value_size(value, _fallback, _blob_threshold) when is_integer(value),
    do: byte_size(Integer.to_string(value))

  defp logical_value_size(value, _fallback, _blob_threshold) when is_float(value),
    do: byte_size(Float.to_string(value))

  defp logical_value_size(_value, fallback, _blob_threshold)
       when is_integer(fallback) and fallback >= 0,
       do: fallback

  defp active_tid?(usage), do: :ets.lookup(usage, @active_key) == [{@active_key, true}]

  defp with_write_lock(table, fun) when is_function(fun, 0) do
    case acquire_write_lock(table, 0) do
      :ok ->
        try do
          fun.()
        after
          :ets.delete_object(table, {@lock_key, self()})
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp acquire_write_lock(table, sleep_ms) do
    if :ets.insert_new(table, {@lock_key, self()}) do
      :ok
    else
      case :ets.lookup(table, @lock_key) do
        [{@lock_key, owner}] when owner == self() ->
          {:error, :namespace_usage_index_lock_reentered}

        [{@lock_key, owner}] when is_pid(owner) ->
          if Process.alive?(owner) do
            Process.sleep(sleep_ms)
            acquire_write_lock(table, next_lock_sleep(sleep_ms))
          else
            :ets.delete_object(table, {@lock_key, owner})
            acquire_write_lock(table, 0)
          end

        _invalid ->
          :ets.delete(table, @lock_key)
          acquire_write_lock(table, 0)
      end
    end
  end

  defp next_lock_sleep(0), do: 1
  defp next_lock_sleep(sleep_ms), do: min(sleep_ms * 2, @lock_max_sleep_ms)
end
