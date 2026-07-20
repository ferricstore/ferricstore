defmodule Ferricstore.Flow.Query.CompositeBackfill do
  @moduledoc false

  alias Ferricstore.Flow.{Keys, LMDB}
  alias Ferricstore.Flow.Query.{CompositeProjection, IndexDefinition}
  alias Ferricstore.Store.Router

  @max_page_records 16
  @max_definitions 16
  @max_exact_integer 9_007_199_254_740_991
  @max_expiry 0xFFFF_FFFF_FFFF_FFFF
  @max_projection_operation_bytes 16 * 1_024 * 1_024

  def project_page(ctx, shard_index, records, definitions),
    do: project_page(ctx, shard_index, records, definitions, [])

  @spec project_page(map(), non_neg_integer(), [map()], [IndexDefinition.t()]) ::
          {:ok,
           %{
             projected_records: non_neg_integer(),
             written_entries: non_neg_integer(),
             write_ops: non_neg_integer(),
             written_bytes: non_neg_integer()
           }}
          | {:error, term()}
  @spec project_page(map(), non_neg_integer(), [map()], [IndexDefinition.t()], keyword()) ::
          {:ok, map()} | {:error, term()}
  def project_page(_ctx, _shard_index, records, _definitions, _opts)
      when is_list(records) and length(records) > @max_page_records,
      do: {:error, :query_backfill_page_too_large}

  def project_page(_ctx, _shard_index, records, [], opts)
      when is_list(records) and is_list(opts) do
    {:ok,
     %{
       projected_records: 0,
       written_entries: 0,
       write_ops: 0,
       written_bytes: 0
     }}
  end

  def project_page(ctx, shard_index, records, definitions, opts)
      when is_map(ctx) and is_integer(shard_index) and shard_index >= 0 and is_list(records) and
             is_list(definitions) and length(definitions) <= @max_definitions and is_list(opts) do
    with :ok <- validate_context(ctx, shard_index),
         :ok <- validate_definitions(definitions),
         :ok <- validate_records(records),
         {:ok, operation_budget} <- projection_budget(opts),
         {:ok, ops} <-
           projection_ops(
             lmdb_path(ctx, shard_index),
             records,
             definitions,
             operation_budget
           ),
         :ok <- LMDB.write_batch(lmdb_path(ctx, shard_index), ops),
         :ok <- verify_current_records(ctx, shard_index, records, opts) do
      {:ok,
       %{
         projected_records: length(records),
         written_entries: count_index_puts(ops),
         write_ops: length(ops),
         written_bytes: written_bytes(ops)
       }}
    else
      {:error, _reason} = error -> error
    end
  end

  def project_page(_ctx, _shard_index, _records, _definitions, _opts),
    do: {:error, :invalid_query_backfill_projection}

  defp projection_ops(path, records, definitions, operation_budget) do
    initial_cache = CompositeProjection.new_cache()

    Enum.reduce_while(records, {:ok, [], initial_cache, 0}, fn record, {:ok, acc, cache, bytes} ->
      with {:ok, action, state_key, projected, expire_at_ms} <- validate_record(record),
           {:ok, ops, cache} <-
             projection_action(
               action,
               path,
               state_key,
               projected,
               expire_at_ms,
               definitions,
               cache
             ),
           next_bytes <- bytes + operation_bytes(ops),
           true <- next_bytes <= operation_budget do
        {:cont, {:ok, :lists.reverse(ops, acc), cache, next_bytes}}
      else
        false -> {:halt, {:error, :query_backfill_projection_budget_exceeded}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, reversed, _cache, _bytes} -> {:ok, Enum.reverse(reversed)}
      {:error, _reason} = error -> error
    end
  end

  defp validate_record(%{state_key: state_key, record: nil, expire_at_ms: 0})
       when is_binary(state_key) and state_key != "" do
    if Keys.state_key?(state_key),
      do: {:ok, :remove, state_key, nil, 0},
      else: {:error, :invalid_query_backfill_record}
  end

  defp validate_record(%{
         state_key: state_key,
         record: record,
         expire_at_ms: expire_at_ms
       })
       when is_binary(state_key) and state_key != "" and is_map(record) and
              is_integer(expire_at_ms) and expire_at_ms >= 0 and expire_at_ms <= @max_expiry do
    case {Map.get(record, :id), Map.get(record, :partition_key), Map.get(record, :version)} do
      {id, partition_key, version}
      when is_binary(id) and (is_nil(partition_key) or is_binary(partition_key)) and
             is_integer(version) and version >= 0 and version <= @max_exact_integer ->
        if Keys.state_key(id, partition_key) == state_key,
          do: {:ok, :reconcile, state_key, record, expire_at_ms},
          else: {:error, :invalid_query_backfill_record}

      _invalid ->
        {:error, :invalid_query_backfill_record}
    end
  end

  defp validate_record(_record), do: {:error, :invalid_query_backfill_record}

  defp projection_action(
         :reconcile,
         path,
         state_key,
         record,
         expire_at_ms,
         definitions,
         cache
       ) do
    CompositeProjection.reconcile(
      path,
      state_key,
      record,
      expire_at_ms,
      definitions,
      cache
    )
  end

  defp projection_action(:remove, path, state_key, _record, _expiry, definitions, cache),
    do: CompositeProjection.remove(path, state_key, definitions, cache)

  defp validate_definitions([%IndexDefinition{} | _] = definitions) do
    if Enum.all?(definitions, &(IndexDefinition.validate(&1) == :ok)),
      do: :ok,
      else: {:error, :invalid_query_backfill_definitions}
  end

  defp validate_definitions(_definitions),
    do: {:error, :invalid_query_backfill_definitions}

  defp projection_budget(opts) do
    case Keyword.get(opts, :max_operation_bytes, @max_projection_operation_bytes) do
      value
      when is_integer(value) and value > 0 and value <= @max_projection_operation_bytes ->
        {:ok, value}

      _invalid ->
        {:error, :invalid_query_backfill_projection_budget}
    end
  end

  defp validate_records(records) do
    with true <- Enum.all?(records, &match?({:ok, _, _, _, _}, validate_record(&1))),
         state_keys <- Enum.map(records, & &1.state_key),
         true <- length(state_keys) == length(Enum.uniq(state_keys)) do
      :ok
    else
      _invalid -> {:error, :invalid_query_backfill_record}
    end
  end

  defp verify_current_records(_ctx, _shard_index, [], _opts), do: :ok

  defp verify_current_records(ctx, shard_index, records, opts) do
    read_values = Keyword.get(opts, :read_values_fun, &Router.read_shard_values/3)
    state_keys = Enum.map(records, & &1.state_key)

    if is_function(read_values, 3) do
      case read_values.(ctx, shard_index, state_keys) do
        {:ok, values} when is_list(values) and length(values) == length(records) ->
          verify_values(records, values)

        :unavailable ->
          {:error, :query_backfill_primary_unavailable}

        {:error, _reason} = error ->
          error

        _invalid ->
          {:error, :invalid_query_backfill_primary_read}
      end
    else
      {:error, :invalid_query_backfill_reader}
    end
  end

  defp verify_values(records, values) do
    Enum.zip(records, values)
    |> Enum.reduce_while(:ok, fn
      {%{record: nil}, nil}, :ok ->
        {:cont, :ok}

      {%{record: expected, state_key: state_key}, encoded}, :ok
      when is_map(expected) and is_binary(encoded) ->
        case decode_record(encoded) do
          {:ok, current} ->
            if same_record_version?(expected, current, state_key),
              do: {:cont, :ok},
              else: {:halt, {:error, :query_backfill_concurrent_change}}

          {:error, _reason} = error ->
            {:halt, error}
        end

      {%{record: nil}, encoded}, :ok when is_binary(encoded) ->
        {:halt, {:error, :query_backfill_concurrent_change}}

      {%{record: expected}, nil}, :ok when is_map(expected) ->
        {:halt, {:error, :query_backfill_concurrent_change}}

      _invalid, :ok ->
        {:halt, {:error, :invalid_query_backfill_primary_read}}
    end)
  end

  defp decode_record(encoded) do
    {:ok, Ferricstore.Flow.decode_record(encoded)}
  rescue
    _error -> {:error, :corrupt_query_backfill_record}
  end

  defp same_record_version?(expected, current, state_key) do
    case {Map.get(current, :id), Map.get(current, :partition_key), Map.get(current, :version)} do
      {id, partition_key, version}
      when is_binary(id) and (is_nil(partition_key) or is_binary(partition_key)) and
             is_integer(version) ->
        Keys.state_key(id, partition_key) == state_key and version == Map.get(expected, :version)

      _invalid ->
        false
    end
  end

  defp validate_context(%{data_dir: data_dir, shard_count: shard_count}, shard_index)
       when is_binary(data_dir) and data_dir != "" and is_integer(shard_count) and
              shard_count > 0 and shard_index < shard_count,
       do: :ok

  defp validate_context(_ctx, _shard_index),
    do: {:error, :invalid_query_backfill_projection_context}

  defp count_index_puts(ops) do
    Enum.count(ops, fn
      {:put, key, _value} -> String.starts_with?(key, IndexDefinition.global_storage_prefix())
      _other -> false
    end)
  end

  defp written_bytes(ops) do
    Enum.reduce(ops, 0, fn
      {:put, key, value}, total -> total + byte_size(key) + byte_size(value)
      _other, total -> total
    end)
  end

  defp operation_bytes(ops) do
    Enum.reduce(ops, 0, fn
      {:put, key, value}, total -> total + byte_size(key) + byte_size(value)
      {:compare, key, value}, total -> total + byte_size(key) + byte_size(value)
      {:compare_missing, key}, total -> total + byte_size(key)
      {:delete, key}, total -> total + byte_size(key)
    end)
  end

  defp lmdb_path(ctx, shard_index) do
    ctx.data_dir
    |> Ferricstore.DataDir.shard_data_path(shard_index)
    |> LMDB.path()
  end
end
