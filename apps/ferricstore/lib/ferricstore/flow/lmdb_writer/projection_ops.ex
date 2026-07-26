defmodule Ferricstore.Flow.LMDBWriter.ProjectionOps do
  @moduledoc false

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Flow.{Keys, Locator, ProjectionLocator}
  alias Ferricstore.Flow.Query.{CompositeProjection, Limits, QueryRowCodec, SourceCatalog}
  alias Ferricstore.Raft.WARaftSegmentReader
  alias Ferricstore.Store.BlobValue

  @default_source_pending_retries 100
  @default_source_pending_sleep_ms 1
  @max_source_pending_retries 1_000
  @max_source_pending_sleep_ms 100
  @max_source_pending_wait_ms 5_000
  @min_composite_prefetch_records 2
  @max_composite_prefetch_records Limits.max_projection_page_records()
  @history_projection_page_size 1_024
  @terminal_states ["completed", "failed", "cancelled"]

  def expand_ops(state, []) do
    state =
      state
      |> Map.put(:terminal_atomic_write?, false)
      |> Map.put(:write_group_sizes, [])

    {:ok, [], state}
  end

  def expand_ops(state, ops) do
    with {:ok, definitions} <- query_index_definitions(state) do
      expansion_state = Map.put(state, :query_index_definitions, definitions)
      ops = coalesce_flow_state_projections(ops)
      projection_keys = projection_prefetch_keys(ops)

      with {:ok, prepared_sources} <-
             prepare_durable_flow_projection_sources(expansion_state, ops),
           {:ok, prepared_sources} <-
             physicalize_prepared_sources(expansion_state, prepared_sources) do
        expansion_state =
          Map.put(expansion_state, :prepared_flow_projection_sources, prepared_sources)

        initial = %{
          ops: [],
          counts: %{},
          count_values: %{},
          terminal_values: %{},
          terminal_reverse_values: %{},
          active_reverse_values: %{},
          composite_projection_cache:
            maybe_prefetch_composite_reverse_values(expansion_state, projection_keys, definitions),
          pending_terminal_count_put_news: MapSet.new(),
          terminal_count_inits: state.terminal_count_inits,
          terminal_atomic_write?: false,
          op_count: 0,
          write_group_sizes: []
        }

        Enum.reduce_while(ops, {:ok, initial}, fn op, {:ok, acc} ->
          before_count = acc.op_count

          case expand_op(expansion_state, op, acc) do
            {:ok, acc} -> {:cont, {:ok, record_write_group(acc, before_count)}}
            {:error, _reason} = error -> {:halt, error}
          end
        end)
      end
      |> case do
        {:ok,
         %{
           ops: expanded,
           terminal_count_inits: terminal_count_inits,
           terminal_atomic_write?: terminal_atomic_write?,
           write_group_sizes: reversed_group_sizes
         }} ->
          state =
            expansion_state
            |> Map.delete(:query_index_definitions)
            |> Map.delete(:prepared_flow_projection_sources)
            |> Map.put(:terminal_count_inits, terminal_count_inits)
            |> Map.put(:terminal_atomic_write?, terminal_atomic_write?)
            |> Map.put(:write_group_sizes, Enum.reverse(reversed_group_sizes))

          {:ok, Enum.reverse(expanded), state}

        {:error, _reason} = error ->
          error
      end
    end
  end

  defp coalesce_flow_state_projections(ops) do
    winners = projection_winners(ops, 0, %{})
    retain_projection_winners(ops, winners, 0, [])
  end

  defp prepare_flow_projection_sources(state, ops) do
    Enum.reduce_while(ops, {:ok, %{}}, fn op, {:ok, prepared} ->
      case prepare_flow_projection_source(state, op) do
        :skip ->
          {:cont, {:ok, prepared}}

        {:ok, state_key, result} ->
          {:cont, {:ok, Map.put(prepared, state_key, result)}}

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
  end

  defp prepare_durable_flow_projection_sources(state, ops) do
    case prepare_flow_projection_sources_once(state, ops) do
      {:error, {:query_row_source_changed, _key}} = changed ->
        {retries, sleep_ms} = source_pending_config()
        retry_durable_flow_projection_sources(state, ops, changed, retries, sleep_ms)

      result ->
        result
    end
  end

  defp prepare_flow_projection_sources_once(state, ops) do
    with {:ok, prepared} <- prepare_flow_projection_sources(state, ops),
         :ok <- ensure_prepared_sources_durable(state, prepared) do
      {:ok, prepared}
    end
  end

  defp retry_durable_flow_projection_sources(_state, _ops, changed, 0, _sleep_ms),
    do: changed

  defp retry_durable_flow_projection_sources(state, ops, _changed, retries, sleep_ms) do
    if sleep_ms > 0, do: Process.sleep(sleep_ms)

    case prepare_flow_projection_sources_once(state, ops) do
      {:error, {:query_row_source_changed, _key}} = changed ->
        retry_durable_flow_projection_sources(state, ops, changed, retries - 1, sleep_ms)

      result ->
        result
    end
  end

  defp prepare_flow_projection_source(state, {:project_flow_state_from_source, key})
       when is_binary(key),
       do: prepared_source_result(key, read_source_flow(state, key))

  defp prepare_flow_projection_source(state, {:project_flow_query_state_from_source, key})
       when is_binary(key),
       do: prepared_source_result(key, read_source_flow(state, key))

  defp prepare_flow_projection_source(
         state,
         {:project_flow_state_from_source, key, expected_version}
       )
       when is_binary(key) and is_integer(expected_version) and expected_version >= 0,
       do: prepared_source_result(key, read_source_flow_at_version(state, key, expected_version))

  defp prepare_flow_projection_source(
         state,
         {:project_flow_query_state_from_source, key, expected_version}
       )
       when is_binary(key) and is_integer(expected_version) and expected_version >= 0,
       do: prepared_source_result(key, read_source_flow_at_version(state, key, expected_version))

  defp prepare_flow_projection_source(_state, _op), do: :skip

  defp prepared_source_result(key, {:ok, _record, _expire_at_ms, %Locator{}} = result),
    do: {:ok, key, result}

  defp prepared_source_result(key, :not_found), do: {:ok, key, :not_found}
  defp prepared_source_result(_key, {:error, _reason} = error), do: error
  defp prepared_source_result(_key, _invalid), do: {:error, :invalid_source_flow_record}

  defp ensure_prepared_sources_durable(state, prepared) when is_map(prepared) do
    refs =
      Enum.reduce(prepared, MapSet.new(), fn
        {key, {:ok, _record, _expire_at_ms, %Locator{file_id: {:waraft_apply_projection, index}}}},
        refs
        when is_binary(key) and is_integer(index) and index > 0 ->
          MapSet.put(refs, {index, key})

        _other, refs ->
          refs
      end)

    ensure_prepared_source_refs_durable(state, MapSet.to_list(refs))
  end

  defp ensure_prepared_source_refs_durable(_state, []), do: :ok

  defp ensure_prepared_source_refs_durable(
         %{instance_ctx: %{data_dir: data_dir}, shard_index: shard_index} = state,
         refs
       )
       when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 do
    case WARaftSegmentReader.ensure_apply_projection_entries_durable(data_dir, shard_index, refs) do
      {:ok, _removed} ->
        :ok

      {:error, {:apply_projection_entry_not_durable, index, key} = reason} ->
        classify_missing_prepared_source(state, index, key, reason)

      {:error, reason} ->
        {:error, {:query_row_source_not_durable, reason}}
    end
  end

  defp ensure_prepared_source_refs_durable(_state, _refs),
    do: {:error, :query_row_source_context_unavailable}

  defp classify_missing_prepared_source(state, index, key, reason) do
    case current_prepared_source_ref(state, index, key) do
      :changed -> {:error, {:query_row_source_changed, key}}
      _current_or_unknown -> {:error, {:query_row_source_not_durable, reason}}
    end
  end

  defp current_prepared_source_ref(state, index, key) do
    case source_keydir(state) do
      nil ->
        :unknown

      keydir ->
        case :ets.lookup(keydir, key) do
          [
            {^key, _value, _expire_at_ms, _lfu, {:waraft_apply_projection, ^index}, _offset,
             _value_size}
          ] ->
            :current

          [{^key, _value, _expire_at_ms, _lfu, _file_id, _offset, _value_size}] ->
            :changed

          [] ->
            :changed

          _invalid ->
            :unknown
        end
    end
  rescue
    ArgumentError -> :unknown
  end

  defp physicalize_prepared_sources(state, prepared) when is_map(prepared) do
    Enum.reduce_while(prepared, {:ok, %{}}, fn
      {key, {:ok, record, expire_at_ms, %Locator{} = locator}}, {:ok, acc} ->
        case physicalize_query_locator(state, locator) do
          {:ok, physical} ->
            {:cont, {:ok, Map.put(acc, key, {:ok, record, expire_at_ms, physical})}}

          {:error, _reason} = error ->
            {:halt, error}
        end

      {key, :not_found}, {:ok, acc} ->
        {:cont, {:ok, Map.put(acc, key, :not_found)}}
    end)
  end

  defp physicalize_query_locator(_state, %Locator{file_id: file_id} = locator)
       when is_integer(file_id) and file_id >= 0 do
    if Locator.hydration_ready?(locator),
      do: {:ok, locator},
      else: {:error, :invalid_query_row_source_locator}
  end

  defp physicalize_query_locator(
         %{instance_ctx: ctx, shard_index: shard_index},
         %Locator{file_id: {_kind, _index} = file_id} = locator
       )
       when is_map(ctx) and is_integer(shard_index) and shard_index >= 0 do
    with {:ok, {ordinal, offset, frame_size}} <-
           WARaftSegmentReader.physical_location(ctx, shard_index, file_id),
         {:ok, physical} <-
           Locator.relocate(locator,
             segment_generation: ordinal,
             offset: offset,
             frame_size: frame_size
           ),
         true <- Locator.hydration_ready?(physical) do
      {:ok, physical}
    else
      {:error, reason} -> {:error, {:query_row_source_location_unavailable, reason}}
      _invalid -> {:error, :invalid_query_row_source_locator}
    end
  end

  defp physicalize_query_locator(_state, _locator),
    do: {:error, :query_row_source_context_unavailable}

  defp prepared_flow_projection_source(
         %{prepared_flow_projection_sources: prepared},
         key
       )
       when is_map(prepared) and is_binary(key) do
    Map.get(prepared, key, {:error, :prepared_source_flow_record_missing})
  end

  defp projection_prefetch_keys(ops),
    do: projection_prefetch_keys(ops, [], 0)

  defp projection_prefetch_keys([], acc, _count), do: Enum.reverse(acc)

  defp projection_prefetch_keys([op | rest], acc, count) do
    case flow_state_projection_key(op) do
      {:ok, state_key} when count < @max_composite_prefetch_records ->
        projection_prefetch_keys(rest, [state_key | acc], count + 1)

      {:ok, _state_key} ->
        :too_many

      :error ->
        projection_prefetch_keys(rest, acc, count)
    end
  end

  defp maybe_prefetch_composite_reverse_values(_state, _state_keys, []),
    do: CompositeProjection.new_cache()

  defp maybe_prefetch_composite_reverse_values(
         %{path: path},
         state_keys,
         _definitions
       )
       when is_binary(path) and is_list(state_keys) and
              length(state_keys) >= @min_composite_prefetch_records and
              length(state_keys) <= @max_composite_prefetch_records do
    cache = CompositeProjection.new_cache()

    case CompositeProjection.prefetch_reverse_values(path, state_keys, cache) do
      {:ok, prefetched} -> prefetched
      {:error, _reason} -> cache
    end
  end

  defp maybe_prefetch_composite_reverse_values(_state, _state_keys, _definitions),
    do: CompositeProjection.new_cache()

  defp projection_winners([], _index, winners), do: winners

  defp projection_winners([op | rest], index, winners) do
    winners =
      case flow_state_projection_key(op) do
        {:ok, state_key} ->
          candidate = {index, op, nil}

          Map.update(winners, state_key, candidate, fn winner ->
            preferred_projection(winner, candidate)
          end)

        :error ->
          winners
      end

    projection_winners(rest, index + 1, winners)
  end

  defp retain_projection_winners([], _winners, _index, acc), do: Enum.reverse(acc)

  defp retain_projection_winners([op | rest], winners, index, acc) do
    acc =
      case flow_state_projection_key(op) do
        {:ok, state_key} ->
          case Map.fetch!(winners, state_key) do
            {^index, _winner, _rank} -> [op | acc]
            {_other_index, _winner, _rank} -> acc
          end

        :error ->
          [op | acc]
      end

    retain_projection_winners(rest, winners, index + 1, acc)
  end

  defp preferred_projection({old_index, old_op, old_rank}, {new_index, new_op, _new_rank}) do
    old_rank = old_rank || projection_rank(old_op)
    new_rank = projection_rank(new_op)

    if new_rank >= old_rank,
      do: {new_index, new_op, new_rank},
      else: {old_index, old_op, old_rank}
  end

  defp projection_rank({:project_flow_state_from_source, _state_key}), do: {3, 0, 1}

  defp projection_rank({:project_flow_state_from_source, _state_key, expected_version}),
    do: {3, expected_version, 1}

  defp projection_rank({:project_flow_query_state_from_source, _state_key}), do: {3, 0, 0}

  defp projection_rank({:project_flow_query_state_from_source, _state_key, expected_version}),
    do: {3, expected_version, 0}

  defp flow_state_projection_key({:project_flow_state_from_source, state_key})
       when is_binary(state_key),
       do: {:ok, state_key}

  defp flow_state_projection_key({:project_flow_state_from_source, state_key, version})
       when is_binary(state_key) and is_integer(version) and version >= 0,
       do: {:ok, state_key}

  defp flow_state_projection_key({:project_flow_query_state_from_source, state_key})
       when is_binary(state_key),
       do: {:ok, state_key}

  defp flow_state_projection_key({:project_flow_query_state_from_source, state_key, version})
       when is_binary(state_key) and is_integer(version) and version >= 0,
       do: {:ok, state_key}

  defp flow_state_projection_key(_op), do: :error

  def expand_op(state, {:project_kv_from_source, key}, acc) when is_binary(key) do
    if Keys.state_key?(key) do
      {:error, :flow_state_requires_compact_source_projection}
    else
      case read_source_value(state, key) do
        {:ok, value, expire_at_ms} ->
          expand_path_op(
            state.path,
            {:put, key, Ferricstore.Flow.LMDB.encode_value(value, expire_at_ms)},
            acc
          )

        :not_found ->
          {:ok, prepend_ops(acc, [{:delete, key}])}

        {:error, _reason} = error ->
          error
      end
    end
  end

  def expand_op(state, {:project_flow_state_from_source, key}, acc) when is_binary(key) do
    case prepared_flow_projection_source(state, key) do
      {:ok, record, expire_at_ms, locator} ->
        expand_flow_state_record(state, key, record, expire_at_ms, locator, acc)

      :not_found ->
        expand_missing_flow_state_projection(state, key, acc)

      {:error, _reason} = error ->
        error
    end
  end

  def expand_op(state, {:project_flow_state_from_source, key, expected_version}, acc)
      when is_binary(key) and is_integer(expected_version) and expected_version >= 0 do
    case prepared_flow_projection_source(state, key) do
      {:ok, record, expire_at_ms, locator} ->
        expand_flow_state_record(state, key, record, expire_at_ms, locator, acc)

      :not_found ->
        expand_missing_flow_state_projection(state, key, acc)

      {:error, _reason} = error ->
        error
    end
  end

  def expand_op(_state, {:project_flow_state, _key, _value, _expire_at_ms}, _acc),
    do: {:error, :inline_flow_projection_unsupported}

  def expand_op(state, {:project_flow_query_state_from_source, key}, acc)
      when is_binary(key) do
    case prepared_flow_projection_source(state, key) do
      {:ok, record, expire_at_ms, locator} ->
        expand_flow_query_state_record(state, key, record, expire_at_ms, locator, acc)

      :not_found ->
        expand_missing_flow_query_state_projection(state, key, acc)

      {:error, _reason} = error ->
        error
    end
  end

  def expand_op(state, {:project_flow_query_state_from_source, key, expected_version}, acc)
      when is_binary(key) and is_integer(expected_version) and expected_version >= 0 do
    case prepared_flow_projection_source(state, key) do
      {:ok, record, expire_at_ms, locator} ->
        expand_flow_query_state_record(state, key, record, expire_at_ms, locator, acc)

      :not_found ->
        expand_missing_flow_query_state_projection(state, key, acc)

      {:error, _reason} = error ->
        error
    end
  end

  def expand_op(_state, {:project_flow_query_state, _key, _value, _expire_at_ms}, _acc),
    do: {:error, :inline_flow_projection_unsupported}

  def expand_op(%{path: path}, {put_mode, key, value} = op, acc)
      when put_mode in [:put, :put_new] and is_binary(key) and is_binary(value) do
    if Keys.state_key?(key),
      do: {:error, :flow_state_requires_compact_source_projection},
      else: expand_path_op(path, op, acc)
  end

  def expand_op(%{path: path}, {:delete, key} = op, acc) when is_binary(key) do
    if Keys.state_key?(key),
      do: {:error, :flow_state_requires_compact_source_projection},
      else: expand_path_op(path, op, acc)
  end

  def expand_op(%{path: path}, op, acc), do: expand_path_op(path, op, acc)

  defp expand_flow_state_record(
         %{path: path} = projection_state,
         key,
         record,
         source_expire_at_ms,
         %Locator{} = locator,
         acc
       ) do
    projection_expire_at_ms = flow_state_projection_expire_at(record, source_expire_at_ms)

    with {:ok, query_row} <-
           encode_query_row(key, record, locator, projection_expire_at_ms),
         {:ok, query_row_ops} <- query_row_put_ops(key, query_row, record),
         acc = maybe_init_terminal_counts_for_active_record(record, acc),
         {:ok, acc} <- expand_path_ops(path, query_row_ops, acc) do
      expand_flow_state_projection(
        projection_state,
        key,
        projection_expire_at_ms,
        record,
        acc
      )
    end
  end

  defp expand_flow_query_state_record(
         projection_state,
         key,
         record,
         source_expire_at_ms,
         %Locator{} = locator,
         acc
       ) do
    projection_expire_at_ms = flow_state_projection_expire_at(record, source_expire_at_ms)

    with {:ok, query_row} <-
           encode_query_row(key, record, locator, projection_expire_at_ms),
         {:ok, query_row_ops} <- query_row_put_ops(key, query_row, record) do
      acc = prepend_ops(acc, query_row_ops)

      expand_composite_projection(
        projection_state,
        key,
        projection_expire_at_ms,
        record,
        acc
      )
    end
  end

  defp encode_query_row(key, record, locator, expire_at_ms) do
    case QueryRowCodec.encode(key, record, locator, expire_at_ms) do
      {:ok, encoded} -> {:ok, encoded}
      :error -> {:error, :invalid_flow_query_row}
    end
  end

  def expand_flow_state_projection(
        %{path: path} = projection_state,
        state_key,
        expire_at_ms,
        record,
        acc
      ) do
    with {:ok, acc} <- standard_flow_state_projection(path, state_key, expire_at_ms, record, acc) do
      expand_composite_projection(projection_state, state_key, expire_at_ms, record, acc)
    end
  end

  def expand_flow_state_projection(path, state_key, expire_at_ms, record, acc)
      when is_binary(path) do
    expand_flow_state_projection(
      %{path: path, query_index_definitions: []},
      state_key,
      expire_at_ms,
      record,
      acc
    )
  end

  defp standard_flow_state_projection(path, state_key, expire_at_ms, record, acc) do
    if Ferricstore.Flow.LMDB.terminal_state?(Map.get(record, :state)) do
      with {:ok, acc} <- maybe_expand_stale_active_delete(path, state_key, acc) do
        expand_path_op(
          path,
          {:terminal_project, Map.fetch!(record, :id), Map.fetch!(record, :type),
           Map.fetch!(record, :state), Map.get(record, :partition_key),
           Map.get(record, :updated_at_ms, 0), state_key, expire_at_ms,
           Map.get(record, :parent_flow_id), Map.get(record, :root_flow_id),
           Map.get(record, :correlation_id), Ferricstore.Flow.Attributes.record(record),
           Ferricstore.Flow.Attributes.indexed_names(record),
           Ferricstore.Flow.StateMeta.record(record),
           Ferricstore.Flow.StateMeta.indexed_key(record)},
          acc
        )
      end
    else
      with {:ok, acc} <- maybe_expand_stale_terminal_delete(path, state_key, acc),
           {:ok, acc} <- maybe_expand_stale_active_delete(path, state_key, acc),
           {:ok, stale_fixed_query_ops} <-
             stale_fixed_query_delete_ops(path, state_key, record) do
        {active_ops, reverse_value} =
          Ferricstore.Flow.LMDB.active_index_put_ops_with_reverse(
            state_key,
            record,
            expire_at_ms
          )

        acc =
          acc
          |> put_in([:active_reverse_values, state_key], reverse_value)
          |> prepend_ops(
            stale_fixed_query_ops ++
              active_ops ++
              flow_attribute_query_ops(record, expire_at_ms, state_key)
          )

        {:ok, acc}
      end
    end
  end

  def flow_state_projection_expire_at(record, fallback_expire_at_ms) when is_map(record) do
    case Map.get(record, :terminal_retention_until_ms) do
      expire_at_ms when is_integer(expire_at_ms) and expire_at_ms > 0 -> expire_at_ms
      _other -> fallback_expire_at_ms
    end
  end

  def expand_missing_flow_state_projection(%{path: path} = projection_state, state_key, acc) do
    with {:ok, acc} <- maybe_expand_stale_terminal_delete(path, state_key, acc),
         {:ok, acc} <- maybe_expand_stale_active_delete(path, state_key, acc),
         {:ok, stale_fixed_query_ops} <- stale_fixed_query_delete_ops(path, state_key, nil),
         {:ok, query_row_ops} <- query_row_delete_ops(state_key),
         acc = prepend_ops(acc, stale_fixed_query_ops ++ query_row_ops) do
      expand_composite_removal(projection_state, state_key, acc)
    end
  end

  def expand_missing_flow_state_projection(path, state_key, acc) when is_binary(path) do
    expand_missing_flow_state_projection(
      %{path: path, query_index_definitions: []},
      state_key,
      acc
    )
  end

  defp expand_missing_flow_query_state_projection(projection_state, state_key, acc) do
    with {:ok, query_row_ops} <- query_row_delete_ops(state_key) do
      acc = prepend_ops(acc, query_row_ops)
      expand_composite_removal(projection_state, state_key, acc)
    end
  end

  defp query_row_put_ops(state_key, query_row, record)
       when is_binary(state_key) and is_binary(query_row) and is_map(record) do
    with type when is_binary(type) and type != "" <- Map.get(record, :type),
         catalog_key <- Keys.type_catalog_member_key(type, state_key),
         {:ok, {:put, source_key, source_value}} <- SourceCatalog.put_op(catalog_key, state_key) do
      {:ok, [{:put, state_key, query_row}, {:put_new, source_key, source_value}]}
    else
      _invalid -> {:error, :invalid_query_source_catalog_entry}
    end
  rescue
    ArgumentError -> {:error, :invalid_query_source_catalog_entry}
  end

  defp query_row_delete_ops(state_key) when is_binary(state_key) do
    {:ok, [{:delete, state_key}]}
  end

  defp expand_path_ops(path, ops, acc) do
    Enum.reduce_while(ops, {:ok, acc}, fn op, {:ok, acc} ->
      case expand_path_op(path, op, acc) do
        {:ok, acc} -> {:cont, {:ok, acc}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp query_index_definitions(%{instance_ctx: instance_ctx, shard_index: shard_index})
       when is_integer(shard_index) and shard_index >= 0 do
    FerricStore.Flow.QueryIndexProvider.projection_definitions(instance_ctx, shard_index)
  end

  defp query_index_definitions(_state), do: {:ok, []}

  defp expand_composite_projection(
         %{path: path, query_index_definitions: definitions},
         state_key,
         expire_at_ms,
         record,
         acc
       ) do
    cache = Map.get(acc, :composite_projection_cache, CompositeProjection.new_cache())

    case CompositeProjection.reconcile(
           path,
           state_key,
           record,
           expire_at_ms,
           definitions,
           cache
         ) do
      {:ok, ops, cache} ->
        {:ok, acc |> Map.put(:composite_projection_cache, cache) |> prepend_ops(ops)}

      {:error, _reason} = error ->
        error
    end
  end

  defp expand_composite_removal(
         %{query_index_definitions: []},
         _state_key,
         acc
       ),
       do: {:ok, acc}

  defp expand_composite_removal(
         %{path: path, query_index_definitions: definitions},
         state_key,
         acc
       ) do
    cache = Map.get(acc, :composite_projection_cache, CompositeProjection.new_cache())

    case CompositeProjection.remove(path, state_key, definitions, cache) do
      {:ok, ops, cache} ->
        {:ok, acc |> Map.put(:composite_projection_cache, cache) |> prepend_ops(ops)}

      {:error, _reason} = error ->
        error
    end
  end

  def maybe_expand_stale_terminal_delete(path, state_key, acc) do
    maybe_expand_stale_terminal_delete(path, state_key, nil, acc)
  end

  def maybe_expand_stale_terminal_delete(path, state_key, keep_terminal_key, acc) do
    reverse_key = Ferricstore.Flow.LMDB.terminal_by_state_key_key(state_key)

    maybe_expand_stale_terminal_delete(
      path,
      state_key,
      reverse_key,
      keep_terminal_key,
      acc,
      fn key -> Ferricstore.Flow.LMDB.get(path, key) end
    )
  end

  @doc false
  def __maybe_expand_stale_terminal_delete_for_test__(path, state_key, acc, get_fun) do
    reverse_key = Ferricstore.Flow.LMDB.terminal_by_state_key_key(state_key)

    maybe_expand_stale_terminal_delete(
      path,
      state_key,
      reverse_key,
      nil,
      acc,
      get_fun
    )
  end

  defp maybe_expand_stale_terminal_delete(
         path,
         state_key,
         reverse_key,
         keep_terminal_key,
         acc,
         get_fun
       ) do
    case get_fun.(reverse_key) do
      {:ok, ^keep_terminal_key} when is_binary(keep_terminal_key) ->
        {:ok, put_terminal_reverse_state(acc, state_key, keep_terminal_key)}

      {:ok, terminal_key} when is_binary(terminal_key) ->
        case get_fun.(terminal_key) do
          {:ok, terminal_value} when is_binary(terminal_value) ->
            case Ferricstore.Flow.LMDB.terminal_index_count_key(terminal_value) do
              {:ok, count_key} ->
                acc = put_terminal_reverse_state(acc, state_key, terminal_key)
                expand_path_op(path, {:terminal_delete, terminal_key, state_key, count_key}, acc)

              _invalid ->
                {:error, :invalid_terminal_index_value}
            end

          :not_found ->
            acc =
              acc
              |> put_terminal_reverse_state(state_key, nil)
              |> Map.put(:terminal_atomic_write?, true)
              |> prepend_ops([
                {:compare, reverse_key, terminal_key},
                {:delete, reverse_key}
              ])

            {:ok, acc}

          {:error, _reason} = error ->
            error

          invalid ->
            {:error, {:invalid_terminal_index_read, invalid}}
        end

      :not_found ->
        {:ok, put_terminal_reverse_state(acc, state_key, nil)}

      {:error, _reason} = error ->
        error

      invalid ->
        {:error, {:invalid_terminal_reverse_read, invalid}}
    end
  end

  def maybe_expand_stale_active_delete(path, state_key, acc) do
    case Map.fetch(acc.active_reverse_values, state_key) do
      {:ok, nil} ->
        {:ok, acc}

      {:ok, reverse_value} when is_binary(reverse_value) ->
        with {:ok, ops} <-
               Ferricstore.Flow.LMDB.active_index_delete_ops_from_reverse_result(
                 state_key,
                 reverse_value
               ) do
          acc =
            acc
            |> put_in([:active_reverse_values, state_key], nil)
            |> prepend_ops(ops)

          {:ok, acc}
        end

      {:ok, invalid} ->
        {:error, {:invalid_active_index_reverse_read, invalid}}

      :error ->
        with {:ok, ops} <-
               Ferricstore.Flow.LMDB.active_index_delete_ops_result(path, state_key) do
          acc =
            acc
            |> put_in([:active_reverse_values, state_key], nil)
            |> prepend_ops(ops)

          {:ok, acc}
        end
    end
  end

  def read_source_value(state, key) do
    {pending_retries, pending_sleep_ms} = source_pending_config()

    state
    |> do_read_source_entry(key, pending_retries, pending_sleep_ms)
    |> without_source_location()
  end

  def read_source_value(state, key, pending_retries) do
    {pending_retries, pending_sleep_ms} =
      normalize_source_pending_config(pending_retries, source_pending_sleep_ms())

    state
    |> do_read_source_entry(key, pending_retries, pending_sleep_ms)
    |> without_source_location()
  end

  defp read_source_flow(state, key) do
    {pending_retries, pending_sleep_ms} = source_pending_config()

    case do_read_source_entry(state, key, pending_retries, pending_sleep_ms) do
      {:ok, value, expire_at_ms, source_location} ->
        located_source_flow(state, key, value, expire_at_ms, source_location)

      other ->
        other
    end
  end

  defp read_source_flow_at_version(state, key, expected_version) do
    {pending_retries, pending_sleep_ms} = source_pending_config()

    do_read_source_flow_at_version(
      state,
      key,
      expected_version,
      pending_retries,
      pending_sleep_ms
    )
  end

  defp do_read_source_flow_at_version(
         state,
         key,
         expected_version,
         pending_retries,
         pending_sleep_ms
       ) do
    case do_read_source_entry(state, key, 0, pending_sleep_ms) do
      {:ok, value, expire_at_ms, source_location} ->
        case ProjectionLocator.decode_source_at_least(
               key,
               value,
               expected_version,
               expire_at_ms,
               source_location
             ) do
          {:ok, record, locator} ->
            {:ok, record, expire_at_ms, locator}

          {:stale, _version} ->
            retry_versioned_source_read(
              state,
              key,
              expected_version,
              pending_retries,
              pending_sleep_ms,
              :stale
            )

          _invalid ->
            {:error, :invalid_source_flow_record}
        end

      :not_found ->
        retry_versioned_source_read(
          state,
          key,
          expected_version,
          pending_retries,
          pending_sleep_ms,
          :not_found
        )

      {:error, {:source_pending, ^key}} ->
        retry_versioned_source_read(
          state,
          key,
          expected_version,
          pending_retries,
          pending_sleep_ms,
          :pending
        )

      {:error, _reason} = error ->
        error
    end
  end

  defp retry_versioned_source_read(
         _state,
         _key,
         expected_version,
         0,
         _pending_sleep_ms,
         :stale
       ),
       do: {:error, {:source_version_unavailable, expected_version}}

  defp retry_versioned_source_read(
         _state,
         _key,
         _expected_version,
         0,
         _pending_sleep_ms,
         :not_found
       ),
       do: :not_found

  defp retry_versioned_source_read(
         _state,
         key,
         _expected_version,
         0,
         _pending_sleep_ms,
         :pending
       ),
       do: {:error, {:source_pending, key}}

  defp retry_versioned_source_read(
         state,
         key,
         expected_version,
         pending_retries,
         pending_sleep_ms,
         _reason
       ) do
    Process.sleep(pending_sleep_ms)

    do_read_source_flow_at_version(
      state,
      key,
      expected_version,
      pending_retries - 1,
      pending_sleep_ms
    )
  end

  defp do_read_source_entry(state, key, pending_retries, pending_sleep_ms) do
    with keydir when not is_nil(keydir) <- source_keydir(state),
         [{^key, cached_value, expire_at_ms, _lfu, file_id, offset, value_size}] <-
           :ets.lookup(keydir, key) do
      source_location = {file_id, offset, value_size}

      case file_id do
        :pending when pending_retries > 0 ->
          Process.sleep(pending_sleep_ms)
          do_read_source_entry(state, key, pending_retries - 1, pending_sleep_ms)

        :pending ->
          {:error, {:source_pending, key}}

        :deleted ->
          :not_found

        file_id when is_integer(file_id) and file_id >= 0 ->
          state
          |> read_source_location(key, cached_value, expire_at_ms, file_id, offset)
          |> with_source_location(source_location)

        {:waraft_segment, _index} = file_id ->
          state
          |> read_source_location(key, cached_value, expire_at_ms, file_id, offset)
          |> with_source_location(source_location)

        {kind, _index} = file_id when kind in [:waraft_projection, :waraft_apply_projection] ->
          state
          |> read_source_location(key, cached_value, expire_at_ms, file_id, offset)
          |> with_source_location(source_location)

        _ when is_binary(cached_value) ->
          state
          |> source_read_result({:ok, cached_value}, expire_at_ms)
          |> with_source_location(source_location)

        _ ->
          state
          |> read_source_location(key, cached_value, expire_at_ms, file_id, offset)
          |> with_source_location(source_location)
      end
    else
      [] -> :not_found
      nil -> {:error, :source_keydir_unavailable}
      _ -> {:error, :source_keydir_bad_entry}
    end
  rescue
    error -> {:error, {:source_read_failed, error}}
  end

  defp without_source_location({:ok, value, expire_at_ms, _source_location}),
    do: {:ok, value, expire_at_ms}

  defp without_source_location(other), do: other

  defp with_source_location({:ok, value, expire_at_ms}, source_location),
    do: {:ok, value, expire_at_ms, source_location}

  defp with_source_location(other, _source_location), do: other

  defp located_source_flow(_state, key, value, expire_at_ms, source_location) do
    with {:ok, record, locator} <-
           ProjectionLocator.decode_source(key, value, expire_at_ms, source_location) do
      {:ok, record, expire_at_ms, locator}
    end
  end

  def source_pending_retries do
    :ferricstore
    |> Application.get_env(:flow_lmdb_source_pending_retries, @default_source_pending_retries)
    |> normalize_bounded_non_negative_integer(
      @default_source_pending_retries,
      @max_source_pending_retries
    )
  end

  def source_pending_sleep_ms do
    :ferricstore
    |> Application.get_env(:flow_lmdb_source_pending_sleep_ms, @default_source_pending_sleep_ms)
    |> normalize_bounded_non_negative_integer(
      @default_source_pending_sleep_ms,
      @max_source_pending_sleep_ms
    )
  end

  @doc false
  def __normalize_source_pending_config_for_test__(pending_retries, pending_sleep_ms) do
    normalize_source_pending_config(pending_retries, pending_sleep_ms)
  end

  def source_keydir(%{instance_ctx: ctx, shard_index: shard_index})
      when is_map(ctx) and is_integer(shard_index) do
    case Map.get(ctx, :keydir_refs) do
      refs when is_tuple(refs) and shard_index >= 0 and shard_index < tuple_size(refs) ->
        elem(refs, shard_index)

      _ ->
        nil
    end
  end

  def source_keydir(_state), do: nil

  def read_source_location(state, _key, _cached_value, expire_at_ms, file_id, offset)
      when is_integer(file_id) and file_id >= 0 and is_integer(offset) and offset >= 0 do
    state.shard_data_path
    |> bitcask_file_path(file_id)
    |> NIF.v2_pread_at(offset)
    |> then(&source_read_result(state, &1, expire_at_ms))
  end

  def read_source_location(_state, _key, _cached_value, _expire_at_ms, :deleted, _offset),
    do: :not_found

  def read_source_location(
        %{instance_ctx: ctx, shard_index: shard_index} = state,
        key,
        _cached_value,
        expire_at_ms,
        {:waraft_segment, _index} = file_id,
        _offset
      ) do
    ctx
    |> WARaftSegmentReader.read_value_from_location(shard_index, file_id, key)
    |> then(&source_segment_read_result(state, &1, expire_at_ms))
  end

  def read_source_location(
        %{instance_ctx: ctx, shard_index: shard_index} = state,
        key,
        _cached_value,
        expire_at_ms,
        {kind, _index} = file_id,
        _offset
      )
      when kind in [:waraft_projection, :waraft_apply_projection] do
    ctx
    |> WARaftSegmentReader.read_value_from_location(shard_index, file_id, key)
    |> then(&source_segment_read_result(state, &1, expire_at_ms))
  end

  def read_source_location(_state, _key, _cached_value, _expire_at_ms, file_id, _offset),
    do: {:error, {:source_location_unavailable, file_id}}

  def source_read_result(state, {:ok, value}, expire_at_ms) when is_binary(value) do
    case materialize_source_value(state, value) do
      {:ok, materialized} -> {:ok, materialized, expire_at_ms}
      {:error, _reason} = error -> error
    end
  end

  def source_read_result(_state, {:error, reason}, _expire_at_ms), do: {:error, reason}

  def source_read_result(_state, other, _expire_at_ms),
    do: {:error, {:bad_source_read, other}}

  def source_segment_read_result(_state, :not_found, _expire_at_ms), do: :not_found

  def source_segment_read_result(state, result, expire_at_ms),
    do: source_read_result(state, result, expire_at_ms)

  defp materialize_source_value(
         %{instance_ctx: %{data_dir: data_dir} = ctx, shard_index: shard_index},
         value
       )
       when is_binary(data_dir) and is_integer(shard_index) and shard_index >= 0 do
    BlobValue.maybe_materialize(
      data_dir,
      shard_index,
      BlobValue.threshold(ctx),
      value
    )
  end

  defp materialize_source_value(_state, value), do: {:ok, value}

  def bitcask_file_path(shard_data_path, file_id) do
    Path.join(shard_data_path, "#{String.pad_leading(Integer.to_string(file_id), 5, "0")}.log")
  end

  def expand_path_op(path, {:terminal_put, terminal_key, value, state_key, count_key}, acc)
      when is_binary(terminal_key) and is_binary(value) and is_binary(state_key) and
             is_binary(count_key) do
    with :ok <- validate_terminal_index_value(terminal_key, value, state_key, count_key),
         {:ok, old_value, acc} <- terminal_value(path, terminal_key, acc),
         :ok <-
           validate_existing_terminal_index_value(
             terminal_key,
             old_value,
             state_key,
             count_key
           ),
         {:ok, count, acc} <- terminal_count(path, count_key, acc),
         existed? = is_binary(old_value),
         {:ok, count, acc} <-
           repair_terminal_count_if_missing(path, count_key, count, existed?, acc),
         {:ok, count_compare} <- terminal_count_compare_op(acc, count_key),
         {:ok, reverse_value, acc} <- terminal_reverse_value(path, state_key, acc),
         {:ok, reverse_compare} <-
           terminal_reverse_put_compare_op(state_key, terminal_key, reverse_value) do
      count = if existed?, do: count, else: count + 1
      count_value = Ferricstore.Flow.LMDB.encode_count(count)
      reverse_key = Ferricstore.Flow.LMDB.terminal_by_state_key_key(state_key)

      ops =
        [
          terminal_value_compare_op(terminal_key, old_value),
          count_compare,
          reverse_compare,
          {:put, terminal_key, value},
          {:put, reverse_key, terminal_key},
          {:put, count_key, count_value}
        ]
        |> maybe_put_expire_key(terminal_key, value, state_key, count_key)
        |> maybe_delete_old_expire_key(
          terminal_key,
          old_value,
          state_key,
          count_key
        )

      acc =
        acc
        |> put_terminal_count_state(count_key, count, count_value)
        |> put_terminal_reverse_state(state_key, terminal_key)
        |> put_in([:terminal_values, terminal_key], value)
        |> Map.put(:terminal_atomic_write?, true)
        |> prepend_ops(ops)

      {:ok, acc}
    end
  end

  def expand_path_op(path, {:terminal_put, terminal_key, value, nil, count_key}, acc)
      when is_binary(terminal_key) and is_binary(value) and is_binary(count_key) do
    with :ok <- validate_terminal_index_value(terminal_key, value, nil, count_key),
         {:ok, old_value, acc} <- terminal_value(path, terminal_key, acc),
         :ok <-
           validate_existing_terminal_index_value(terminal_key, old_value, nil, count_key),
         {:ok, count, acc} <- terminal_count(path, count_key, acc),
         existed? = is_binary(old_value),
         {:ok, count, acc} <-
           repair_terminal_count_if_missing(path, count_key, count, existed?, acc),
         {:ok, count_compare} <- terminal_count_compare_op(acc, count_key) do
      count = if existed?, do: count, else: count + 1
      count_value = Ferricstore.Flow.LMDB.encode_count(count)

      ops =
        [
          terminal_value_compare_op(terminal_key, old_value),
          count_compare,
          {:put, terminal_key, value},
          {:put, count_key, count_value}
        ]
        |> maybe_put_expire_key(terminal_key, value, nil, count_key)
        |> maybe_delete_old_expire_key(terminal_key, old_value, nil, count_key)

      acc =
        acc
        |> put_terminal_count_state(count_key, count, count_value)
        |> put_in([:terminal_values, terminal_key], value)
        |> Map.put(:terminal_atomic_write?, true)
        |> prepend_ops(ops)

      {:ok, acc}
    end
  end

  def expand_path_op(
        path,
        {:terminal_project, id, type, terminal_state, partition_key, updated_at_ms, state_key,
         expire_at_ms, parent_flow_id, root_flow_id, correlation_id, attributes,
         indexed_attributes, state_meta, indexed_state_meta},
        acc
      )
      when is_binary(id) and is_binary(type) and is_binary(terminal_state) and
             is_integer(updated_at_ms) and is_binary(state_key) and is_integer(expire_at_ms) do
    state_index_key = Ferricstore.Flow.Keys.state_index_key(type, terminal_state, partition_key)
    terminal_key = Ferricstore.Flow.LMDB.terminal_index_key(state_index_key, id, updated_at_ms)
    count_key = Ferricstore.Flow.LMDB.terminal_count_key(state_index_key)

    value =
      Ferricstore.Flow.LMDB.encode_terminal_index_value(
        id,
        updated_at_ms,
        expire_at_ms,
        state_key,
        count_key
      )

    next_record = %{
      id: id,
      type: type,
      state: terminal_state,
      partition_key: partition_key,
      updated_at_ms: updated_at_ms,
      attributes: attributes,
      indexed_attributes: indexed_attributes,
      state_meta: state_meta,
      indexed_state_meta: indexed_state_meta,
      parent_flow_id: parent_flow_id,
      root_flow_id: root_flow_id,
      correlation_id: correlation_id
    }

    with {:ok, acc} <-
           maybe_expand_stale_terminal_delete(path, state_key, terminal_key, acc),
         {:ok, acc} <-
           expand_path_op(path, {:terminal_put, terminal_key, value, state_key, count_key}, acc),
         {:ok, stale_fixed_query_ops} <-
           stale_fixed_query_delete_ops(path, state_key, next_record) do
      {:ok,
       prepend_ops(
         acc,
         stale_fixed_query_ops ++
           terminal_project_metadata_ops(
             id,
             partition_key,
             updated_at_ms,
             expire_at_ms,
             state_key,
             parent_flow_id,
             root_flow_id,
             correlation_id,
             type,
             terminal_state,
             attributes,
             indexed_attributes,
             state_meta,
             indexed_state_meta
           )
       )}
    end
  end

  def expand_path_op(_path, {:query_put, query_key, value}, acc)
      when is_binary(query_key) and is_binary(value) do
    {:ok, prepend_ops(acc, [{:put, query_key, value}])}
  end

  def expand_path_op(
        path,
        {:history_project_from_index, flow_index, flow_lookup, id, partition_key, history_key,
         expire_at_ms},
        acc
      )
      when is_binary(id) and is_binary(history_key) and is_integer(expire_at_ms) do
    entries = history_project_from_index_entries(flow_index, flow_lookup, history_key)

    with {:ok, history_ops} <-
           history_put_many_ops(path, id, partition_key, history_key, expire_at_ms, entries) do
      {:ok, prepend_ops(acc, history_ops)}
    end
  end

  def expand_path_op(
        path,
        {:history_put_many, id, partition_key, history_key, expire_at_ms, entries},
        acc
      )
      when is_binary(id) and is_binary(history_key) and is_integer(expire_at_ms) and
             is_list(entries) do
    with {:ok, history_ops} <-
           history_put_many_ops(path, id, partition_key, history_key, expire_at_ms, entries) do
      {:ok, prepend_ops(acc, history_ops)}
    end
  end

  def expand_path_op(_path, {:query_delete, query_key}, acc) when is_binary(query_key) do
    {:ok, prepend_ops(acc, [{:delete, query_key}])}
  end

  def expand_path_op(path, {:history_delete, history_index_key}, acc)
      when is_binary(history_index_key) do
    case Ferricstore.Flow.LMDB.history_index_delete_ops_result(path, history_index_key) do
      {:ok, ops} -> {:ok, prepend_ops(acc, ops)}
      {:error, _reason} = error -> error
    end
  end

  def expand_path_op(_path, {put_mode, key, value} = op, acc)
      when put_mode in [:put, :put_new] and is_binary(key) and is_binary(value) do
    {:ok, prepend_ops(acc, [op])}
  end

  def expand_path_op(path, {:terminal_delete, terminal_key, state_key, count_key}, acc)
      when is_binary(terminal_key) and is_binary(state_key) and is_binary(count_key) do
    with {:ok, old_value, acc} <- terminal_value(path, terminal_key, acc),
         :ok <-
           validate_existing_terminal_index_value(
             terminal_key,
             old_value,
             state_key,
             count_key
           ),
         {:ok, count, acc} <- terminal_count(path, count_key, acc),
         existed? = is_binary(old_value),
         {:ok, count, acc} <-
           repair_terminal_count_if_missing(path, count_key, count, existed?, acc),
         {:ok, count_compare_ops} <-
           terminal_count_compare_ops(acc, count_key, existed?),
         {:ok, reverse_value, acc} <- terminal_reverse_value(path, state_key, acc),
         {:ok, reverse_compare} <-
           terminal_reverse_delete_compare_op(state_key, terminal_key, reverse_value) do
      reverse_key = Ferricstore.Flow.LMDB.terminal_by_state_key_key(state_key)

      {count, count_ops} =
        if existed? do
          count = max(count - 1, 0)
          {count, [{:put, count_key, Ferricstore.Flow.LMDB.encode_count(count)}]}
        else
          {count, []}
        end

      delete_ops =
        [{:delete, terminal_key}, {:delete, reverse_key} | count_ops]
        |> maybe_delete_old_expire_key(
          terminal_key,
          old_value,
          state_key,
          count_key
        )

      ops =
        [
          terminal_value_compare_op(terminal_key, old_value),
          reverse_compare
          | count_compare_ops
        ] ++ delete_ops

      acc =
        acc
        |> put_terminal_reverse_state(state_key, nil)
        |> put_in([:terminal_values, terminal_key], nil)
        |> Map.put(:terminal_atomic_write?, true)
        |> prepend_ops(ops)

      acc =
        if existed? do
          put_terminal_count_state(
            acc,
            count_key,
            count,
            Ferricstore.Flow.LMDB.encode_count(count)
          )
        else
          acc
        end

      {:ok, acc}
    end
  end

  def expand_path_op(path, {:terminal_delete, terminal_key, nil, count_key}, acc)
      when is_binary(terminal_key) and is_binary(count_key) do
    with {:ok, old_value, acc} <- terminal_value(path, terminal_key, acc),
         :ok <-
           validate_existing_terminal_index_value(terminal_key, old_value, nil, count_key),
         {:ok, count, acc} <- terminal_count(path, count_key, acc),
         existed? = is_binary(old_value),
         {:ok, count, acc} <-
           repair_terminal_count_if_missing(path, count_key, count, existed?, acc),
         {:ok, count_compare_ops} <-
           terminal_count_compare_ops(acc, count_key, existed?) do
      {count, count_ops} =
        if existed? do
          count = max(count - 1, 0)
          {count, [{:put, count_key, Ferricstore.Flow.LMDB.encode_count(count)}]}
        else
          {count, []}
        end

      delete_ops =
        [{:delete, terminal_key} | count_ops]
        |> maybe_delete_old_expire_key(terminal_key, old_value, nil, count_key)

      ops =
        [terminal_value_compare_op(terminal_key, old_value) | count_compare_ops] ++ delete_ops

      acc =
        acc
        |> put_in([:terminal_values, terminal_key], nil)
        |> Map.put(:terminal_atomic_write?, true)
        |> prepend_ops(ops)

      acc =
        if existed? do
          put_terminal_count_state(
            acc,
            count_key,
            count,
            Ferricstore.Flow.LMDB.encode_count(count)
          )
        else
          acc
        end

      {:ok, acc}
    end
  end

  def expand_path_op(_path, op, acc), do: {:ok, prepend_ops(acc, [op])}

  def maybe_init_terminal_counts_for_active_record(record, acc) when is_map(record) do
    with state when is_binary(state) <- Map.get(record, :state),
         false <- Ferricstore.Flow.LMDB.terminal_state?(state),
         type when is_binary(type) <- Map.get(record, :type) do
      partition_key = Map.get(record, :partition_key)
      init_key = {type, partition_key}

      if MapSet.member?(acc.terminal_count_inits, init_key) do
        acc
      else
        count_keys =
          Enum.map(@terminal_states, fn terminal_state ->
            state_index_key =
              Ferricstore.Flow.Keys.state_index_key(type, terminal_state, partition_key)

            Ferricstore.Flow.LMDB.terminal_count_key(state_index_key)
          end)

        count_ops =
          Enum.map(count_keys, fn count_key ->
            {:put_new, count_key, Ferricstore.Flow.LMDB.encode_count(0)}
          end)

        pending_count_keys = MapSet.new(count_keys)

        acc
        |> Map.update!(:terminal_count_inits, &MapSet.put(&1, init_key))
        |> Map.update(
          :pending_terminal_count_put_news,
          pending_count_keys,
          &MapSet.union(&1, pending_count_keys)
        )
        |> prepend_ops(count_ops)
      end
    else
      _ -> acc
    end
  end

  def maybe_init_terminal_counts_for_active_record(_value, acc), do: acc

  def terminal_value(path, terminal_key, acc) do
    case Map.fetch(acc.terminal_values, terminal_key) do
      {:ok, value} ->
        {:ok, value, acc}

      :error ->
        case Ferricstore.Flow.LMDB.get(path, terminal_key) do
          {:ok, value} -> {:ok, value, put_in(acc, [:terminal_values, terminal_key], value)}
          :not_found -> {:ok, nil, put_in(acc, [:terminal_values, terminal_key], nil)}
          {:error, _reason} = error -> error
        end
    end
  end

  defp terminal_reverse_value(path, state_key, acc) do
    case Map.fetch(Map.get(acc, :terminal_reverse_values, %{}), state_key) do
      {:ok, value} ->
        {:ok, value, acc}

      :error ->
        reverse_key = Ferricstore.Flow.LMDB.terminal_by_state_key_key(state_key)

        case Ferricstore.Flow.LMDB.get(path, reverse_key) do
          {:ok, value} when is_binary(value) ->
            {:ok, value, put_terminal_reverse_state(acc, state_key, value)}

          :not_found ->
            {:ok, nil, put_terminal_reverse_state(acc, state_key, nil)}

          {:error, _reason} = error ->
            error

          invalid ->
            {:error, {:invalid_terminal_reverse_read, invalid}}
        end
    end
  end

  def terminal_count(path, count_key, acc) do
    case Map.fetch(acc.counts, count_key) do
      {:ok, count} ->
        if Map.has_key?(Map.get(acc, :count_values, %{}), count_key) do
          {:ok, count, acc}
        else
          {:error, :missing_terminal_count_snapshot}
        end

      :error ->
        case Ferricstore.Flow.LMDB.get(path, count_key) do
          :not_found -> missing_terminal_count_result(count_key, acc)
          result -> terminal_count_result(result, count_key, acc)
        end
    end
  end

  defp missing_terminal_count_result(count_key, acc) do
    pending = Map.get(acc, :pending_terminal_count_put_news, MapSet.new())

    if MapSet.member?(pending, count_key) do
      value = Ferricstore.Flow.LMDB.encode_count(0)
      {:ok, 0, put_terminal_count_state(acc, count_key, 0, value)}
    else
      terminal_count_result(:not_found, count_key, acc)
    end
  end

  @doc false
  def __terminal_count_result_for_test__(result, count_key, acc),
    do: terminal_count_result(result, count_key, acc)

  defp terminal_count_result({:ok, value}, count_key, acc) when is_binary(value) do
    case Ferricstore.Flow.LMDB.decode_count(value) do
      {:ok, count} -> {:ok, count, put_terminal_count_state(acc, count_key, count, value)}
      :error -> {:error, :invalid_terminal_count_value}
    end
  end

  defp terminal_count_result(:not_found, count_key, acc),
    do: {:ok, 0, put_terminal_count_state(acc, count_key, 0, :missing)}

  defp terminal_count_result({:error, _reason} = error, _count_key, _acc), do: error

  defp terminal_count_result(invalid, _count_key, _acc),
    do: {:error, {:invalid_terminal_count_read, invalid}}

  def repair_terminal_count_if_missing(path, count_key, 0, true, acc) do
    case terminal_state_index_key_from_count_key(count_key) do
      {:ok, state_index_key} ->
        prefix = Ferricstore.Flow.LMDB.terminal_index_prefix(state_index_key)

        path
        |> Ferricstore.Flow.LMDB.prefix_count(prefix)
        |> terminal_prefix_count_result(count_key, acc)

      :error ->
        {:error, :invalid_terminal_count_key}
    end
  end

  def repair_terminal_count_if_missing(_path, _count_key, count, _existed?, acc),
    do: {:ok, count, acc}

  @doc false
  def __terminal_prefix_count_result_for_test__(result, count_key, acc),
    do: terminal_prefix_count_result(result, count_key, acc)

  defp terminal_prefix_count_result({:ok, count}, count_key, acc)
       when is_integer(count) and count > 0 do
    {:ok, count, put_in(acc, [:counts, count_key], count)}
  end

  defp terminal_prefix_count_result({:ok, 0}, count_key, acc),
    do: {:ok, 1, put_in(acc, [:counts, count_key], 1)}

  defp terminal_prefix_count_result({:error, _reason} = error, _count_key, _acc), do: error

  defp terminal_prefix_count_result(invalid, _count_key, _acc),
    do: {:error, {:invalid_terminal_prefix_count, invalid}}

  def terminal_state_index_key_from_count_key(count_key) when is_binary(count_key) do
    prefix = Ferricstore.Flow.LMDB.terminal_count_prefix()

    if String.starts_with?(count_key, prefix) and byte_size(count_key) > byte_size(prefix) do
      state_index_key =
        binary_part(count_key, byte_size(prefix), byte_size(count_key) - byte_size(prefix))

      case state_index_key do
        <<>> -> :error
        _ -> {:ok, state_index_key}
      end
    else
      :error
    end
  end

  def terminal_state_index_key_from_count_key(_count_key), do: :error

  defp put_terminal_count_state(acc, count_key, count, value) do
    acc
    |> put_in([:counts, count_key], count)
    |> Map.update(:count_values, %{count_key => value}, &Map.put(&1, count_key, value))
  end

  defp put_terminal_reverse_state(acc, state_key, value) do
    Map.update(
      acc,
      :terminal_reverse_values,
      %{state_key => value},
      &Map.put(&1, state_key, value)
    )
  end

  defp terminal_count_compare_op(acc, count_key) do
    case Map.fetch(Map.get(acc, :count_values, %{}), count_key) do
      {:ok, :missing} -> {:ok, {:compare_missing, count_key}}
      {:ok, value} when is_binary(value) -> {:ok, {:compare, count_key, value}}
      _missing_or_invalid -> {:error, :missing_terminal_count_snapshot}
    end
  end

  defp terminal_count_compare_ops(acc, count_key, true) do
    with {:ok, compare_op} <- terminal_count_compare_op(acc, count_key) do
      {:ok, [compare_op]}
    end
  end

  defp terminal_count_compare_ops(_acc, _count_key, false), do: {:ok, []}

  defp terminal_value_compare_op(terminal_key, nil), do: {:compare_missing, terminal_key}

  defp terminal_value_compare_op(terminal_key, value) when is_binary(value),
    do: {:compare, terminal_key, value}

  defp terminal_reverse_put_compare_op(state_key, _terminal_key, nil) do
    {:ok, {:compare_missing, Ferricstore.Flow.LMDB.terminal_by_state_key_key(state_key)}}
  end

  defp terminal_reverse_put_compare_op(state_key, terminal_key, terminal_key) do
    reverse_key = Ferricstore.Flow.LMDB.terminal_by_state_key_key(state_key)
    {:ok, {:compare, reverse_key, terminal_key}}
  end

  defp terminal_reverse_put_compare_op(_state_key, _terminal_key, _other),
    do: {:error, :terminal_reverse_mismatch}

  defp terminal_reverse_delete_compare_op(state_key, terminal_key, terminal_key) do
    reverse_key = Ferricstore.Flow.LMDB.terminal_by_state_key_key(state_key)
    {:ok, {:compare, reverse_key, terminal_key}}
  end

  defp terminal_reverse_delete_compare_op(state_key, _terminal_key, nil) do
    {:ok, {:compare_missing, Ferricstore.Flow.LMDB.terminal_by_state_key_key(state_key)}}
  end

  defp terminal_reverse_delete_compare_op(_state_key, _terminal_key, _other),
    do: {:error, :terminal_reverse_mismatch}

  defp validate_existing_terminal_index_value(_terminal_key, nil, _state_key, _count_key),
    do: :ok

  defp validate_existing_terminal_index_value(terminal_key, value, state_key, count_key),
    do: validate_terminal_index_value(terminal_key, value, state_key, count_key)

  defp validate_terminal_index_value(terminal_key, value, state_key, count_key) do
    with {:ok, {id, updated_at_ms, _expire_at_ms, ^state_key}} <-
           Ferricstore.Flow.LMDB.decode_terminal_index_value(value),
         true <-
           Ferricstore.Flow.LMDB.terminal_index_entry_key?(
             terminal_key,
             id,
             updated_at_ms
           ),
         {:ok, ^count_key} <- Ferricstore.Flow.LMDB.terminal_index_count_key(value) do
      :ok
    else
      _invalid -> {:error, :invalid_terminal_index_value}
    end
  end

  def prepend_ops(acc, ops) do
    acc = %{acc | ops: :lists.reverse(ops, acc.ops)}

    case acc do
      %{op_count: count} -> %{acc | op_count: count + length(ops)}
      _without_count -> acc
    end
  end

  defp record_write_group(%{op_count: after_count} = acc, before_count)
       when after_count > before_count do
    %{acc | write_group_sizes: [after_count - before_count | acc.write_group_sizes]}
  end

  defp record_write_group(acc, _before_count), do: acc

  def history_project_from_index_entries(nil, _flow_lookup, _history_key), do: []
  def history_project_from_index_entries(_flow_index, nil, _history_key), do: []

  def history_project_from_index_entries(flow_index, flow_lookup, history_key) do
    case Ferricstore.Flow.NativeOrderedIndex.get(flow_index, flow_lookup) do
      nil -> []
      native -> history_project_from_native_entries(native, history_key)
    end
  rescue
    ArgumentError -> []
  end

  def history_project_from_native_entries(native, history_key) do
    history_project_from_native_page(native, history_key, :inf, [])
  rescue
    ArgumentError -> []
  end

  defp history_project_from_native_page(native, history_key, max_bound, acc) do
    page =
      Ferricstore.Flow.NativeOrderedIndex.range_slice(
        native,
        history_key,
        :neg_inf,
        max_bound,
        true,
        0,
        @history_projection_page_size
      )

    case page do
      [] ->
        history_project_normalize_entries(acc)

      _entries ->
        {event_id, event_ms} = List.last(page)
        acc = :lists.reverse(page, acc)

        if length(page) < @history_projection_page_size do
          history_project_normalize_entries(acc)
        else
          history_project_from_native_page(
            native,
            history_key,
            {:cursor_before, event_ms, event_id},
            acc
          )
        end
    end
  end

  def history_project_normalize_entries(entries) do
    Enum.map(entries, fn {event_id, event_ms} -> {event_id, trunc(event_ms)} end)
  end

  def terminal_project_metadata_ops(
        id,
        partition_key,
        score,
        expire_at_ms,
        state_key,
        parent_flow_id,
        root_flow_id,
        correlation_id,
        type,
        terminal_state,
        attributes \\ %{},
        indexed_attributes \\ [],
        state_meta \\ %{},
        indexed_state_meta \\ nil
      ) do
    []
    |> terminal_project_metadata_op(
      :parent,
      parent_flow_id,
      partition_key,
      id,
      score,
      expire_at_ms,
      state_key
    )
    |> terminal_project_metadata_op(
      :root,
      root_flow_id,
      partition_key,
      id,
      score,
      expire_at_ms,
      state_key
    )
    |> terminal_project_metadata_op(
      :correlation,
      correlation_id,
      partition_key,
      id,
      score,
      expire_at_ms,
      state_key
    )
    |> terminal_project_attribute_ops(
      attributes,
      type,
      terminal_state,
      partition_key,
      id,
      score,
      expire_at_ms,
      state_key,
      indexed_attributes,
      state_meta,
      indexed_state_meta
    )
  end

  def terminal_project_attribute_ops(
        ops,
        attributes,
        type,
        terminal_state,
        partition_key,
        id,
        score,
        expire_at_ms,
        state_key,
        indexed_attributes,
        state_meta,
        indexed_state_meta
      ) do
    record = %{
      id: id,
      type: type,
      state: terminal_state,
      partition_key: partition_key,
      updated_at_ms: score,
      attributes: attributes,
      indexed_attributes: indexed_attributes,
      state_meta: state_meta,
      indexed_state_meta: indexed_state_meta
    }

    flow_attribute_query_ops(record, expire_at_ms, state_key) ++ ops
  end

  def flow_attribute_query_ops(record, expire_at_ms, state_key) do
    id = Map.get(record, :id)

    entries =
      Ferricstore.Flow.Attributes.index_entries(record) ++
        Ferricstore.Flow.StateMeta.index_entries(record)

    entries
    |> Enum.map(fn {index_key, _id, score} ->
      {query_key, value} =
        Ferricstore.Flow.LMDB.query_index_entry(
          index_key,
          id,
          score,
          expire_at_ms,
          state_key
        )

      {:put, query_key, value}
    end)
  end

  def stale_fixed_query_delete_ops(path, state_key, next_record) do
    next_keys =
      next_record
      |> flow_fixed_query_keys()
      |> MapSet.new()

    with {:ok, old_record} <- old_projected_flow_record(path, state_key) do
      ops =
        old_record
        |> flow_fixed_query_keys()
        |> MapSet.new()
        |> MapSet.difference(next_keys)
        |> Enum.map(&{:delete, &1})

      {:ok, ops}
    end
  end

  def old_projected_flow_record(path, state_key) do
    path
    |> Ferricstore.Flow.LMDB.get(state_key)
    |> old_projected_flow_record_result(state_key)
  end

  @doc false
  def __old_projected_flow_record_result_for_test__(result, state_key),
    do: old_projected_flow_record_result(result, state_key)

  defp old_projected_flow_record_result({:ok, value}, state_key)
       when is_binary(value) and is_binary(state_key) do
    case QueryRowCodec.decode(value, state_key) do
      {:ok, %{record: record}} -> {:ok, record}
      :error -> {:error, :invalid_projected_flow_record}
    end
  end

  defp old_projected_flow_record_result(:not_found, _state_key), do: {:ok, nil}
  defp old_projected_flow_record_result({:error, _reason} = error, _state_key), do: error

  defp old_projected_flow_record_result(invalid, _state_key),
    do: {:error, {:invalid_projected_flow_read, invalid}}

  def flow_attribute_query_keys(nil), do: []

  def flow_attribute_query_keys(record) do
    id = Map.get(record, :id)

    entries =
      Ferricstore.Flow.Attributes.index_entries(record) ++
        Ferricstore.Flow.StateMeta.index_entries(record)

    entries
    |> Enum.map(fn {index_key, _id, score} ->
      Ferricstore.Flow.LMDB.query_index_key(index_key, id, score)
    end)
  end

  defp flow_fixed_query_keys(nil), do: []

  defp flow_fixed_query_keys(record) do
    flow_attribute_query_keys(record) ++ terminal_metadata_query_keys(record)
  end

  defp terminal_metadata_query_keys(record) do
    if Ferricstore.Flow.LMDB.terminal_state?(Map.get(record, :state)) do
      id = Map.get(record, :id)
      partition_key = Map.get(record, :partition_key)
      score = Map.get(record, :updated_at_ms, 0)

      id
      |> terminal_project_metadata_index_keys(
        partition_key,
        Map.get(record, :parent_flow_id),
        Map.get(record, :root_flow_id),
        Map.get(record, :correlation_id)
      )
      |> Enum.map(&Ferricstore.Flow.LMDB.query_index_key(&1, id, score))
    else
      []
    end
  end

  def terminal_project_metadata_op(
        ops,
        :root,
        nil,
        _partition_key,
        _id,
        _score,
        _expire_at_ms,
        _state_key
      ),
      do: ops

  def terminal_project_metadata_op(
        ops,
        :root,
        "",
        _partition_key,
        _id,
        _score,
        _expire_at_ms,
        _state_key
      ),
      do: ops

  def terminal_project_metadata_op(
        ops,
        :root,
        id,
        _partition_key,
        id,
        _score,
        _expire_at_ms,
        _state_key
      ),
      do: ops

  def terminal_project_metadata_op(
        ops,
        kind,
        value,
        partition_key,
        id,
        score,
        expire_at_ms,
        state_key
      )
      when is_binary(value) and value != "" do
    index_key =
      case kind do
        :parent -> Ferricstore.Flow.Keys.parent_index_key(value, partition_key)
        :root -> Ferricstore.Flow.Keys.root_index_key(value, partition_key)
        :correlation -> Ferricstore.Flow.Keys.correlation_index_key(value, partition_key)
      end

    {query_key, value} =
      Ferricstore.Flow.LMDB.query_index_entry(
        index_key,
        id,
        score,
        expire_at_ms,
        state_key
      )

    [{:put, query_key, value} | ops]
  end

  def terminal_project_metadata_op(
        ops,
        _kind,
        _value,
        _partition_key,
        _id,
        _score,
        _expire_at_ms,
        _state_key
      ),
      do: ops

  def terminal_project_metadata_index_keys(
        id,
        partition_key,
        parent_flow_id,
        root_flow_id,
        correlation_id
      ) do
    []
    |> terminal_project_metadata_index_key(:parent, parent_flow_id, partition_key, id)
    |> terminal_project_metadata_index_key(:root, root_flow_id, partition_key, id)
    |> terminal_project_metadata_index_key(:correlation, correlation_id, partition_key, id)
  end

  def terminal_project_metadata_index_key(keys, :root, nil, _partition_key, _id), do: keys
  def terminal_project_metadata_index_key(keys, :root, "", _partition_key, _id), do: keys
  def terminal_project_metadata_index_key(keys, :root, id, _partition_key, id), do: keys

  def terminal_project_metadata_index_key(keys, kind, value, partition_key, _id)
      when is_binary(value) and value != "" do
    key =
      case kind do
        :parent -> Ferricstore.Flow.Keys.parent_index_key(value, partition_key)
        :root -> Ferricstore.Flow.Keys.root_index_key(value, partition_key)
        :correlation -> Ferricstore.Flow.Keys.correlation_index_key(value, partition_key)
      end

    [key | keys]
  end

  def terminal_project_metadata_index_key(keys, _kind, _value, _partition_key, _id), do: keys

  def history_put_many_ops(path, id, partition_key, history_key, expire_at_ms, entries) do
    entries
    |> Enum.reduce_while({:ok, []}, fn
      {event_id, event_ms}, {:ok, reversed_ops}
      when is_binary(event_id) and is_integer(event_ms) and event_ms >= 0 ->
        compound_key = Ferricstore.Flow.Keys.stream_entry_key(id, event_id, partition_key)

        history_index_key =
          Ferricstore.Flow.LMDB.history_index_key(history_key, event_id, event_ms)

        case history_index_value(
               path,
               history_index_key,
               event_id,
               event_ms,
               compound_key,
               expire_at_ms
             ) do
          {:ok, value} ->
            entry_ops =
              [{:put, history_index_key, value}]
              |> maybe_history_expire_put(expire_at_ms, history_index_key)
              |> Enum.reverse()

            {:cont, {:ok, :lists.reverse(entry_ops, reversed_ops)}}

          {:error, _reason} = error ->
            {:halt, error}
        end

      invalid, _acc ->
        {:halt, {:error, {:invalid_history_projection_entry, invalid}}}
    end)
    |> case do
      {:ok, reversed_ops} ->
        {:ok,
         reversed_ops
         |> Enum.reverse()
         |> maybe_history_flow_expire_put(expire_at_ms, history_key)}

      {:error, _reason} = error ->
        error
    end
  end

  def history_index_value(
        path,
        history_index_key,
        event_id,
        event_ms,
        compound_key,
        expire_at_ms
      ) do
    case existing_history_index_location(path, history_index_key) do
      {:ok, {{:flow_history, _file_id} = file_ref, offset, value_size}} ->
        {:ok,
         Ferricstore.Flow.LMDB.encode_history_index_value(
           event_id,
           event_ms,
           compound_key,
           expire_at_ms,
           file_ref,
           offset,
           value_size
         )}

      {:ok, nil} ->
        {:ok,
         Ferricstore.Flow.LMDB.encode_history_index_value(
           event_id,
           event_ms,
           compound_key,
           expire_at_ms
         )}

      {:error, _reason} = error ->
        error
    end
  end

  def existing_history_index_location(path, history_index_key) do
    path
    |> Ferricstore.Flow.LMDB.get(history_index_key)
    |> existing_history_index_location_result()
  end

  @doc false
  def __existing_history_index_location_result_for_test__(result),
    do: existing_history_index_location_result(result)

  defp existing_history_index_location_result(:not_found), do: {:ok, nil}
  defp existing_history_index_location_result({:error, _reason} = error), do: error

  defp existing_history_index_location_result({:ok, value}) when is_binary(value) do
    case Ferricstore.Flow.LMDB.decode_history_index_location(value) do
      {:ok,
       {_event_id, _event_ms, _expire_at_ms, _compound_key, {:flow_history, _file_id} = file_ref,
        offset, value_size}}
      when is_integer(offset) and offset >= 0 and is_integer(value_size) and value_size >= 0 ->
        {:ok, {file_ref, offset, value_size}}

      {:ok, {_event_id, _event_ms, _expire_at_ms, _compound_key, nil, nil, nil}} ->
        {:ok, nil}

      _invalid ->
        {:error, :invalid_history_index_value}
    end
  end

  defp existing_history_index_location_result(invalid),
    do: {:error, {:invalid_history_index_read, invalid}}

  def maybe_history_expire_put(ops, expire_at_ms, history_index_key) do
    case Ferricstore.Flow.LMDB.history_expire_key(expire_at_ms, history_index_key) do
      nil ->
        ops

      expire_key ->
        [
          {:put, expire_key, Ferricstore.Flow.LMDB.encode_history_expire_value(history_index_key)}
          | ops
        ]
    end
  end

  def maybe_history_flow_expire_put(ops, expire_at_ms, history_key) do
    case Ferricstore.Flow.LMDB.history_flow_expire_key(expire_at_ms, history_key) do
      nil ->
        ops

      expire_key ->
        ops ++
          [
            {:put, expire_key,
             Ferricstore.Flow.LMDB.encode_history_flow_expire_value(history_key, expire_at_ms)}
          ]
    end
  end

  def maybe_put_expire_key(ops, terminal_key, value, state_key, count_key) do
    case Ferricstore.Flow.LMDB.decode_terminal_index_value(value) do
      {:ok, {_id, _updated_at_ms, expire_at_ms, _state_key}} when expire_at_ms > 0 ->
        expire_key = Ferricstore.Flow.LMDB.terminal_expire_key(expire_at_ms, terminal_key)

        expire_value =
          Ferricstore.Flow.LMDB.encode_terminal_expire_value(terminal_key, state_key, count_key)

        [{:put, expire_key, expire_value} | ops]

      _ ->
        ops
    end
  end

  def maybe_delete_old_expire_key(ops, _terminal_key, nil, _state_key, _count_key), do: ops

  def maybe_delete_old_expire_key(ops, terminal_key, old_value, state_key, count_key) do
    case Ferricstore.Flow.LMDB.decode_terminal_index_value(old_value) do
      {:ok, {_id, _updated_at_ms, expire_at_ms, _state_key}} when expire_at_ms > 0 ->
        expire_key = Ferricstore.Flow.LMDB.terminal_expire_key(expire_at_ms, terminal_key)

        expire_value =
          Ferricstore.Flow.LMDB.encode_terminal_expire_value(terminal_key, state_key, count_key)

        [{:compare, expire_key, expire_value}, {:delete, expire_key} | ops]

      _ ->
        ops
    end
  end

  defp source_pending_config do
    normalize_source_pending_config(
      Application.get_env(
        :ferricstore,
        :flow_lmdb_source_pending_retries,
        @default_source_pending_retries
      ),
      Application.get_env(
        :ferricstore,
        :flow_lmdb_source_pending_sleep_ms,
        @default_source_pending_sleep_ms
      )
    )
  end

  defp normalize_source_pending_config(pending_retries, pending_sleep_ms) do
    pending_retries =
      normalize_bounded_non_negative_integer(
        pending_retries,
        @default_source_pending_retries,
        @max_source_pending_retries
      )

    pending_sleep_ms =
      normalize_bounded_non_negative_integer(
        pending_sleep_ms,
        @default_source_pending_sleep_ms,
        @max_source_pending_sleep_ms
      )

    pending_retries =
      if pending_sleep_ms == 0 do
        pending_retries
      else
        min(pending_retries, div(@max_source_pending_wait_ms, pending_sleep_ms))
      end

    {pending_retries, pending_sleep_ms}
  end

  defp normalize_bounded_non_negative_integer(value, default, maximum) do
    case Integer.parse(to_string(value)) do
      {integer, ""} when integer >= 0 -> min(integer, maximum)
      _ -> default
    end
  rescue
    _ -> default
  end
end
