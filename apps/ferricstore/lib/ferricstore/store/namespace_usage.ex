defmodule Ferricstore.Store.NamespaceUsage do
  @moduledoc false

  alias Ferricstore.{DataDir, ProbFile, TermCodec}
  alias Ferricstore.Flow.Keys

  alias Ferricstore.Store.{
    BlobRef,
    BlobValue,
    CompoundKey,
    Ops,
    Router
  }

  alias Ferricstore.Store.Shard.{NamespaceUsageFlowAccounting, NamespaceUsageIndex}

  @max_prob_sidecar_bytes 1_073_741_888
  @max_i64 9_223_372_036_854_775_807

  @empty_details %{
    keys: 0,
    bytes: 0,
    flow_count: 0,
    counted_by_key: %{},
    bytes_by_key: %{},
    entries_by_key: %{},
    plain_entries_by_key: %{},
    internal_entries_by_key: %{},
    top_transfer_base_bytes_by_key: %{}
  }

  @spec ensure_scope(map(), binary(), non_neg_integer()) :: :ok | {:error, term()}
  def ensure_scope(%{name: name, keydir_refs: refs} = store, scope, now_ms)
      when is_atom(name) and is_tuple(refs) and is_binary(scope) and is_integer(now_ms) and
             now_ms >= 0 do
    refs
    |> Tuple.to_list()
    |> Enum.with_index()
    |> Enum.reduce_while(:ok, fn {keydir, shard_index}, :ok ->
      {usage, expiry} = NamespaceUsageIndex.table_names(name, shard_index)

      if NamespaceUsageIndex.scope_ready?(usage, scope) do
        {:cont, :ok}
      else
        case NamespaceUsageIndex.rebuild_scope(
               usage,
               expiry,
               keydir,
               scope,
               now_ms: now_ms,
               blob_threshold_bytes: BlobValue.threshold(store),
               entry_bytes_fun: &entry_footprint(store, &1)
             ) do
          :ok -> {:cont, :ok}
          {:error, _reason} = error -> {:halt, error}
        end
      end
    end)
  end

  def ensure_scope(_store, _scope, _now_ms),
    do: {:error, :namespace_usage_index_unavailable}

  @spec usage(map(), binary(), non_neg_integer()) ::
          {:ok, %{keys: non_neg_integer(), bytes: non_neg_integer()}} | {:error, term()}
  def usage(store, scope, now_ms) do
    with {:ok, details} <- details(store, scope, [], now_ms) do
      {:ok, Map.take(details, [:keys, :bytes, :flow_count])}
    end
  end

  @spec details(map(), binary(), [binary()], non_neg_integer()) ::
          {:ok, map()} | {:error, term()}
  def details(%{name: name, keydir_refs: refs}, scope, logical_keys, now_ms)
      when is_atom(name) and is_tuple(refs) and is_binary(scope) and is_list(logical_keys) and
             is_integer(now_ms) and now_ms >= 0 do
    refs
    |> tuple_size()
    |> shard_indexes()
    |> Enum.reduce_while({:ok, @empty_details}, fn shard_index, {:ok, aggregate} ->
      {usage, expiry} = NamespaceUsageIndex.table_names(name, shard_index)

      case NamespaceUsageIndex.details(usage, expiry, scope, logical_keys, now_ms) do
        {:ok, shard_details} ->
          {:cont, {:ok, merge_details(aggregate, shard_details)}}

        :unavailable ->
          {:halt, {:error, :namespace_usage_index_unavailable}}

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
  end

  def details(_store, _scope, _logical_keys, _now_ms),
    do: {:error, :namespace_usage_index_unavailable}

  @spec refresh_keys(map(), [binary()]) :: :ok | {:error, term()}
  def refresh_keys(%{name: name, keydir_refs: refs} = store, keys)
      when is_atom(name) and is_tuple(refs) and is_list(keys) do
    keys
    |> Enum.filter(&is_binary/1)
    |> Enum.map(&CompoundKey.extract_redis_key/1)
    |> Enum.uniq()
    |> Enum.reduce_while(:ok, fn logical_key, :ok ->
      shard_index = Router.shard_for(store, logical_key)
      {usage, expiry} = NamespaceUsageIndex.table_names(name, shard_index)

      if NamespaceUsageIndex.active?(usage) do
        keydir = elem(refs, shard_index)

        case refresh_logical_key(store, keydir, usage, expiry, logical_key) do
          :ok -> {:cont, :ok}
          {:error, _reason} = error -> {:halt, error}
        end
      else
        {:cont, :ok}
      end
    end)
  rescue
    error -> {:error, {:namespace_usage_refresh_failed, Exception.message(error)}}
  catch
    kind, reason -> {:error, {:namespace_usage_refresh_failed, kind, reason}}
  end

  def refresh_keys(_store, _keys), do: :ok

  @spec invalidate(map()) :: :ok
  def invalidate(%{name: name, keydir_refs: refs}) when is_atom(name) and is_tuple(refs) do
    refs
    |> tuple_size()
    |> shard_indexes()
    |> Enum.each(fn shard_index ->
      {usage, expiry} = NamespaceUsageIndex.table_names(name, shard_index)
      _ = NamespaceUsageIndex.invalidate(usage, expiry)
    end)

    :ok
  end

  def invalidate(_store), do: :ok

  @doc false
  @spec entry_bytes(map(), tuple()) :: non_neg_integer()
  def entry_bytes(
        store,
        {key, value, _expire_at_ms, _lfu, file_id, offset, value_size}
      )
      when is_binary(key) do
    logical_key = CompoundKey.extract_redis_key(key)

    byte_size(key) +
      stored_value_size(store, key, value, file_id, offset, value_size) +
      probabilistic_sidecar_bytes(store, key, value, logical_key)
  end

  def entry_bytes(_store, row), do: raise(ArgumentError, "invalid keydir row: #{inspect(row)}")

  defp entry_footprint(store, {key, value, _expire_at_ms, _lfu, _file_id, _offset, _size} = row) do
    %{bytes: entry_bytes(store, row), flow_scope: flow_scope(store, key, value)}
  end

  defp refresh_logical_key(store, keydir, usage, expiry, logical_key) do
    [logical_key, CompoundKey.type_key(logical_key)]
    |> Enum.reduce_while(:ok, fn storage_key, :ok ->
      case :ets.lookup(keydir, storage_key) do
        [
          {^storage_key, _value, expire_at_ms, _lfu, _file_id, _offset, _value_size} = row
        ] ->
          bytes = entry_bytes(store, row)

          case NamespaceUsageIndex.put_exact_bytes(
                 usage,
                 expiry,
                 storage_key,
                 bytes,
                 expire_at_ms
               ) do
            :ok -> {:cont, :ok}
            {:error, _reason} = error -> {:halt, error}
          end

        [] ->
          case NamespaceUsageIndex.delete(usage, expiry, storage_key) do
            :ok -> {:cont, :ok}
            {:error, _reason} = error -> {:halt, error}
          end

        invalid ->
          {:halt, {:error, {:invalid_namespace_usage_keydir_entry, storage_key, invalid}}}
      end
    end)
  end

  defp shard_indexes(0), do: []
  defp shard_indexes(count), do: 0..(count - 1)

  defp merge_details(left, right) do
    %{
      keys: left.keys + right.keys,
      bytes: left.bytes + right.bytes,
      flow_count: left.flow_count + right.flow_count,
      counted_by_key:
        Map.merge(left.counted_by_key, right.counted_by_key, fn _key, a, b -> a or b end),
      bytes_by_key: sum_maps(left.bytes_by_key, right.bytes_by_key),
      entries_by_key: sum_maps(left.entries_by_key, right.entries_by_key),
      plain_entries_by_key: sum_maps(left.plain_entries_by_key, right.plain_entries_by_key),
      internal_entries_by_key:
        sum_maps(left.internal_entries_by_key, right.internal_entries_by_key),
      top_transfer_base_bytes_by_key:
        merge_top_maps(
          left.top_transfer_base_bytes_by_key,
          right.top_transfer_base_bytes_by_key
        )
    }
  end

  defp sum_maps(left, right),
    do: Map.merge(left, right, fn _key, a, b -> a + b end)

  defp merge_top_maps(left, right) do
    Map.merge(left, right, fn _key, a, b -> merge_top_three(a, b) end)
  end

  defp merge_top_three(left, right) do
    values = top_values(left) ++ top_values(right)
    [first, second, third] = values |> Enum.sort(:desc) |> Enum.take(3) |> pad_three()
    {first, second, third, min(length(values), 3)}
  end

  defp top_values({first, second, third, count}) do
    [first, second, third] |> Enum.take(count)
  end

  defp pad_three(values), do: values ++ List.duplicate(0, 3 - length(values))

  defp flow_scope(store, key, value) do
    if Keys.state_key?(key) do
      store
      |> materialized_flow_value(key, value)
      |> then(&NamespaceUsageFlowAccounting.decoded_scope(key, &1))
    end
  end

  defp materialized_flow_value(store, key, value) when is_binary(value) do
    case BlobRef.decode(value) do
      {:ok, %BlobRef{}} -> Ops.get(store, key)
      :error -> value
    end
  end

  defp materialized_flow_value(store, key, _value), do: Ops.get(store, key)

  defp stored_value_size(store, _key, value, _file_id, _offset, _value_size)
       when is_binary(value) do
    if BlobValue.threshold(store) > 0 and BlobRef.encoded_size?(byte_size(value)) do
      case BlobRef.decode(value) do
        {:ok, %BlobRef{size: size}} -> size
        :error -> byte_size(value)
      end
    else
      byte_size(value)
    end
  end

  defp stored_value_size(store, key, nil, file_id, offset, value_size)
       when is_integer(value_size) and value_size >= 0 do
    if BlobValue.threshold(store) > 0 and BlobRef.encoded_size?(value_size) do
      logical_blob_value_size(store, key, file_id, offset)
    else
      value_size
    end
  end

  defp stored_value_size(_store, _key, value, _file_id, _offset, _value_size)
       when is_integer(value),
       do: byte_size(Integer.to_string(value))

  defp stored_value_size(_store, _key, value, _file_id, _offset, _value_size)
       when is_float(value),
       do: byte_size(Float.to_string(value))

  defp stored_value_size(_store, _key, _value, _file_id, _offset, value_size)
       when is_integer(value_size) and value_size >= 0,
       do: value_size

  defp stored_value_size(_store, _key, _value, _file_id, _offset, _value_size), do: 0

  defp logical_blob_value_size(store, _key, :pending, _offset),
    do: configured_max_value_size(store)

  defp logical_blob_value_size(store, _key, _file_id, :pending_offset),
    do: configured_max_value_size(store)

  defp logical_blob_value_size(store, key, _file_id, _offset) do
    case Ops.value_size(store, key) do
      size when is_integer(size) and size >= 0 -> size
      _missing_or_unavailable -> configured_max_value_size(store)
    end
  end

  defp configured_max_value_size(%{max_value_size: value})
       when is_integer(value) and value > 0,
       do: value

  defp configured_max_value_size(_store), do: 1_048_576

  defp probabilistic_sidecar_bytes(
         store,
         <<"T:", _rest::binary>> = storage_key,
         value,
         logical_key
       ) do
    marker =
      if is_binary(value),
        do: value,
        else: Ops.compound_get(store, logical_key, storage_key)

    case CompoundKey.decode_prob_type(marker) do
      {:ok, {type, _create_token}} ->
        sidecar_bytes =
          store
          |> probabilistic_path(logical_key, Atom.to_string(type))
          |> sidecar_file_bytes()

        sidecar_bytes + probabilistic_metadata_token_slack(store, logical_key, type)

      :error ->
        0
    end
  end

  defp probabilistic_sidecar_bytes(_store, _storage_key, _value, _logical_key), do: 0

  defp probabilistic_metadata_token_slack(store, key, type) do
    case Ops.get(store, key) do
      encoded when is_binary(encoded) ->
        with {:ok, {tag, %{} = metadata}} <- TermCodec.decode(encoded),
             true <- tag == probabilistic_metadata_tag(type),
             true <- Map.has_key?(metadata, :create_token) do
          normalized =
            metadata
            |> Map.put(:create_token, @max_i64)
            |> then(&TermCodec.encode({tag, &1}))

          max(byte_size(normalized) - byte_size(encoded), 0)
        else
          _invalid -> 0
        end

      _missing_or_unavailable ->
        0
    end
  rescue
    _invalid -> 0
  end

  defp probabilistic_metadata_tag(:bloom), do: :bloom_meta
  defp probabilistic_metadata_tag(:cms), do: :cms_meta
  defp probabilistic_metadata_tag(:cuckoo), do: :cuckoo_meta
  defp probabilistic_metadata_tag(:topk), do: :topk_meta

  defp probabilistic_path(%{data_dir: data_dir} = store, key, extension)
       when is_binary(data_dir) do
    shard_index = Router.shard_for(store, key)

    data_dir
    |> DataDir.shard_data_path(shard_index)
    |> Path.join("prob")
    |> ProbFile.path(key, extension)
  end

  defp sidecar_file_bytes(path) do
    case File.lstat(path, time: :posix) do
      {:ok, %File.Stat{type: :regular, size: size}} when is_integer(size) and size >= 0 ->
        min(size, @max_prob_sidecar_bytes)

      {:error, :enoent} ->
        0

      _unavailable_or_unsafe ->
        @max_prob_sidecar_bytes
    end
  end
end
