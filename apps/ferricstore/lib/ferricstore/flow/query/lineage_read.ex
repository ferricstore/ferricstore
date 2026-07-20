defmodule Ferricstore.Flow.Query.LineageRead do
  @moduledoc false

  alias Ferricstore.CommandTime

  alias Ferricstore.Flow.{
    Keys,
    LMDB,
    LMDBIndexDecode,
    LMDBMirror,
    LMDBQueryWindow,
    LMDBWriter,
    RecordLoader,
    ScopeBinding
  }

  alias Ferricstore.Flow.Query.{Limits, MandatoryScope, RecordProjection, Request}
  alias Ferricstore.Store.Router

  @maximum_exact_integer 9_007_199_254_740_991

  @type boundary :: {non_neg_integer(), binary()} | nil

  @spec read_page(map(), Request.t(), MandatoryScope.t(), boundary()) ::
          {:ok, map()} | {:error, atom()}
  def read_page(ctx, %Request{} = request, %MandatoryScope{} = scope, boundary)
      when is_map(ctx) do
    with :ok <- Request.validate_bound(request),
         :ok <- MandatoryScope.validate(scope),
         :ok <- validate_boundary(boundary),
         {:ok, descriptor} <- lineage_descriptor(request),
         {:ok, metadata} <- MandatoryScope.single_metadata(scope),
         {:ok, physical_partition, _metadata} <-
           ScopeBinding.bind_resolved_read_partition(
             descriptor.value,
             descriptor.partition_key,
             metadata
           ),
         {:ok, scoped_ctx} <- ScopeBinding.put_resolved_read_scope(ctx, metadata),
         index_key <- index_key(descriptor, physical_partition),
         fetch_count <- request.limit + 1,
         {:ok, root_probe} <-
           root_probe(scoped_ctx, descriptor, physical_partition, boundary),
         {:ok, hot_refs} <-
           hot_page_refs(ctx, index_key, fetch_count, boundary, descriptor.direction),
         {:ok, cold_page} <-
           cold_page_refs(
             ctx,
             index_key,
             physical_partition,
             fetch_count,
             boundary,
             descriptor.direction
           ),
         {:ok, merged_page} <-
           select_page_refs(
             hot_refs ++ root_probe.refs,
             cold_page.refs,
             fetch_count,
             descriptor.direction
           ),
         :ok <-
           validate_page_coverage(merged_page.refs, fetch_count, cold_page.exhausted?),
         {:ok, candidate_records, candidate_hydrations, memory_high_water_bytes} <-
           hydrate_records(
             scoped_ctx,
             merged_page.refs,
             physical_partition,
             descriptor,
             root_probe.records
           ) do
      selected = Enum.take(merged_page.refs, request.limit)
      records = Enum.take(candidate_records, request.limit)
      has_more = length(merged_page.refs) > request.limit
      continuation = if has_more, do: selected |> List.last() |> ref_boundary(), else: nil

      {:ok,
       %{
         records: records,
         has_more: has_more,
         continuation: continuation,
         scanned_entries:
           length(hot_refs) + cold_page.scanned_entries + root_probe.scanned_entries,
         hydrated_records: candidate_hydrations + root_probe.hydrated_records,
         duplicate_entries: merged_page.duplicate_entries,
         memory_high_water_bytes: memory_high_water_bytes
       }}
    else
      {:error, reason} when is_binary(reason) -> normalize_error(reason)
      {:error, _reason} = error -> error
    end
  rescue
    _error -> {:error, :query_storage_unavailable}
  catch
    _kind, _reason -> {:error, :query_storage_unavailable}
  end

  defp lineage_descriptor(request) do
    case Request.lineage_descriptor(request) do
      {:ok, %{kind: kind} = descriptor} when kind in [:parent, :root, :correlation] ->
        {:ok, descriptor}

      _unsupported ->
        {:error, :unsupported_query_shape}
    end
  end

  defp index_key(%{kind: :parent, value: value}, partition_key),
    do: Keys.parent_index_key(value, partition_key)

  defp index_key(%{kind: :root, value: value}, partition_key),
    do: Keys.root_index_key(value, partition_key)

  defp index_key(%{kind: :correlation, value: value}, partition_key),
    do: Keys.correlation_index_key(value, partition_key)

  defp root_probe(_ctx, %{kind: kind}, _physical_partition, _boundary)
       when kind in [:parent, :correlation] do
    {:ok, %{refs: [], records: %{}, scanned_entries: 0, hydrated_records: 0}}
  end

  defp root_probe(
         ctx,
         %{kind: :root, value: root_id, direction: direction},
         physical_partition,
         boundary
       ) do
    case RecordLoader.records_for_ids(ctx, [root_id], physical_partition) do
      {:ok, []} ->
        {:ok, %{refs: [], records: %{}, scanned_entries: 1, hydrated_records: 1}}

      {:ok,
       [
         %{
           id: ^root_id,
           root_flow_id: ^root_id,
           partition_key: ^physical_partition,
           updated_at_ms: updated_at_ms
         } = record
       ]}
      when is_integer(updated_at_ms) and updated_at_ms >= 0 and
             updated_at_ms <= @maximum_exact_integer ->
        ref = {root_id, updated_at_ms}
        refs = if ref_after_boundary?(ref, boundary, direction), do: [ref], else: []

        {:ok,
         %{
           refs: refs,
           records: %{root_id => record},
           scanned_entries: 1,
           hydrated_records: 1
         }}

      {:ok, [%{id: ^root_id, partition_key: ^physical_partition}]} ->
        {:ok, %{refs: [], records: %{}, scanned_entries: 1, hydrated_records: 1}}

      {:ok, _invalid} ->
        {:error, :query_storage_inconsistent}

      {:error, "ERR invalid flow record"} ->
        {:error, :query_storage_inconsistent}

      {:error, reason} when is_binary(reason) ->
        {:error, :query_storage_unavailable}

      {:error, _reason} ->
        {:error, :query_storage_unavailable}
    end
  end

  defp hot_page_refs(ctx, index_key, count, nil, direction) do
    ctx
    |> Router.flow_index_rank_range(index_key, 0, count - 1, direction == :desc)
    |> normalize_hot_refs()
  end

  defp hot_page_refs(ctx, index_key, count, {updated_at_ms, id}, :asc) do
    ctx
    |> Router.flow_index_score_range_slice(
      index_key,
      {:cursor_after, updated_at_ms, id},
      :inf,
      false,
      0,
      count
    )
    |> normalize_hot_refs()
  end

  defp hot_page_refs(ctx, index_key, count, {updated_at_ms, id}, :desc) do
    ctx
    |> Router.flow_index_score_range_slice(
      index_key,
      :neg_inf,
      {:cursor_before, updated_at_ms, id},
      true,
      0,
      count
    )
    |> normalize_hot_refs()
  end

  defp normalize_hot_refs({:ok, refs}) when is_list(refs) do
    refs
    |> Enum.reduce_while({:ok, []}, fn
      {id, score}, {:ok, acc} when is_binary(id) and id != "" ->
        if Limits.valid_run_id?(id) do
          case normalize_score(score) do
            {:ok, updated_at_ms} -> {:cont, {:ok, [{id, updated_at_ms} | acc]}}
            :error -> {:halt, {:error, :query_storage_inconsistent}}
          end
        else
          {:halt, {:error, :query_storage_inconsistent}}
        end

      _invalid, _acc ->
        {:halt, {:error, :query_storage_inconsistent}}
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      {:error, _reason} = error -> error
    end
  end

  defp normalize_hot_refs(:unavailable), do: {:error, :query_storage_unavailable}
  defp normalize_hot_refs({:error, _reason}), do: {:error, :query_storage_unavailable}
  defp normalize_hot_refs(_invalid), do: {:error, :query_storage_unavailable}

  defp cold_page_refs(
         ctx,
         index_key,
         physical_partition,
         count,
         boundary,
         direction
       ) do
    shard_index = Router.shard_for(ctx, index_key)

    with :ok <- normalize_flush(LMDBWriter.flush(ctx.name, shard_index)),
         :ok <- normalize_mirror(LMDBMirror.require_healthy(ctx, index_key, physical_partition)),
         {:ok, path} <- exact_lmdb_path(ctx, index_key, physical_partition),
         prefix <- LMDB.query_index_prefix(index_key),
         {:ok, entries} <- cold_page_entries(path, prefix, boundary, direction, count),
         {:ok, decoded} <- LMDBIndexDecode.query_entries_readonly(entries, CommandTime.now_ms()),
         {:ok, refs} <- cold_refs(decoded, physical_partition) do
      {:ok,
       %{
         refs: refs,
         scanned_entries: length(entries),
         exhausted?: length(entries) < count
       }}
    else
      {:error, {:invalid_query_index_value, _key}} ->
        {:error, :query_storage_inconsistent}

      {:error, :invalid_query_index_entries} ->
        {:error, :query_storage_inconsistent}

      {:error, _reason} = error ->
        error
    end
  end

  defp exact_lmdb_path(ctx, index_key, physical_partition) do
    case LMDBMirror.paths_for_index(ctx, index_key, physical_partition) do
      [path] when is_binary(path) -> {:ok, path}
      _invalid -> {:error, :query_storage_unavailable}
    end
  end

  defp cold_page_entries(path, prefix, nil, direction, count),
    do: LMDB.prefix_entries(path, prefix, count, direction == :desc)

  defp cold_page_entries(path, prefix, {updated_at_ms, id}, :asc, count) do
    LMDB.prefix_entries_after(
      path,
      prefix,
      LMDBQueryWindow.cursor_seek_key(prefix, updated_at_ms, id),
      count
    )
  end

  defp cold_page_entries(path, prefix, {updated_at_ms, id}, :desc, count) do
    LMDB.prefix_entries_reverse_before(
      path,
      prefix,
      LMDBQueryWindow.cursor_seek_key(prefix, updated_at_ms, id),
      count
    )
  end

  defp cold_refs(entries, physical_partition) do
    Enum.reduce_while(entries, {:ok, []}, fn
      {id, updated_at_ms, state_key}, {:ok, acc}
      when is_binary(id) and is_integer(updated_at_ms) and updated_at_ms >= 0 and
             updated_at_ms <= @maximum_exact_integer and is_binary(state_key) ->
        if Limits.valid_run_id?(id) and state_key == Keys.state_key(id, physical_partition) do
          {:cont, {:ok, [{id, updated_at_ms} | acc]}}
        else
          {:halt, {:error, :query_storage_inconsistent}}
        end

      _invalid, _acc ->
        {:halt, {:error, :query_storage_inconsistent}}
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      {:error, _reason} = error -> error
    end
  end

  defp select_page_refs(hot_refs, cold_refs, count, direction) do
    refs = hot_refs ++ cold_refs

    with :ok <- validate_consistent_scores(refs) do
      unique_refs =
        refs
        |> Enum.sort_by(fn {id, updated_at_ms} -> {updated_at_ms, id} end, direction)
        |> Enum.uniq_by(&elem(&1, 0))

      {:ok,
       %{
         refs: Enum.take(unique_refs, count),
         duplicate_entries: length(refs) - length(unique_refs)
       }}
    end
  end

  defp validate_consistent_scores(refs) do
    refs
    |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
    |> Enum.reduce_while(:ok, fn {_id, scores}, :ok ->
      if length(Enum.uniq(scores)) == 1,
        do: {:cont, :ok},
        else: {:halt, {:error, :query_storage_inconsistent}}
    end)
  end

  defp validate_page_coverage(refs, count, _cold_exhausted?) when length(refs) >= count,
    do: :ok

  defp validate_page_coverage(_refs, _count, true), do: :ok

  defp validate_page_coverage(_refs, _count, false),
    do: {:error, :query_scan_budget_exceeded}

  defp hydrate_records(_ctx, [], _physical_partition, _descriptor, prefetched),
    do: {:ok, [], 0, :erlang.external_size(prefetched, minor_version: 2)}

  defp hydrate_records(ctx, refs, physical_partition, descriptor, prefetched)
       when is_map(prefetched) do
    ids = Enum.map(refs, &elem(&1, 0))
    missing_ids = Enum.reject(ids, &Map.has_key?(prefetched, &1))

    with {:ok, loaded} <- RecordLoader.records_for_ids(ctx, missing_ids, physical_partition),
         {:ok, records} <- ordered_records(ids, prefetched, loaded),
         :ok <- validate_hydrated_records(records, refs, physical_partition, descriptor),
         {:ok, projected} <- project_records(records) do
      memory_high_water_bytes =
        :erlang.external_size(
          {prefetched, loaded, records, projected},
          minor_version: 2
        )

      {:ok, projected, length(missing_ids), memory_high_water_bytes}
    else
      {:error, "ERR invalid flow record"} -> {:error, :query_storage_inconsistent}
      {:error, "ERR storage read failed"} -> {:error, :query_storage_unavailable}
      {:error, reason} when is_binary(reason) -> {:error, :query_storage_unavailable}
      {:error, _reason} = error -> error
    end
  end

  defp ordered_records(ids, prefetched, loaded) when is_list(loaded) do
    records_by_id =
      Enum.reduce(loaded, prefetched, fn
        %{id: id} = record, acc when is_binary(id) -> Map.put(acc, id, record)
        _invalid, acc -> Map.put(acc, :invalid_record, :invalid)
      end)

    if map_size(records_by_id) == map_size(prefetched) + length(loaded) and
         not Map.has_key?(records_by_id, :invalid_record) do
      ids
      |> Enum.reduce_while({:ok, []}, fn id, {:ok, acc} ->
        case Map.fetch(records_by_id, id) do
          {:ok, record} -> {:cont, {:ok, [record | acc]}}
          :error -> {:halt, {:error, :query_storage_inconsistent}}
        end
      end)
      |> case do
        {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
        {:error, _reason} = error -> error
      end
    else
      {:error, :query_storage_inconsistent}
    end
  end

  defp ordered_records(_ids, _prefetched, _loaded),
    do: {:error, :query_storage_inconsistent}

  defp validate_hydrated_records(records, refs, physical_partition, descriptor)
       when is_list(records) and length(records) == length(refs) do
    expected = Map.new(refs)

    valid =
      Enum.zip(records, refs)
      |> Enum.all?(fn
        {%{id: id, updated_at_ms: updated_at_ms, partition_key: ^physical_partition} = record,
         {id, updated_at_ms}}
        when is_binary(id) and is_integer(updated_at_ms) and updated_at_ms >= 0 and
               updated_at_ms <= @maximum_exact_integer ->
          Map.get(record, descriptor.field) == descriptor.value and
            Map.get(expected, id) == updated_at_ms

        _invalid ->
          false
      end)

    if valid, do: :ok, else: {:error, :query_storage_inconsistent}
  end

  defp validate_hydrated_records(_records, _refs, _physical_partition, _descriptor),
    do: {:error, :query_storage_inconsistent}

  defp project_records(records) do
    records
    |> Enum.reduce_while({:ok, []}, fn record, {:ok, acc} ->
      record
      |> Ferricstore.Flow.RecordProjection.public()
      |> then(&RecordProjection.project_result({:ok, &1}))
      |> case do
        {:ok, projected} when is_map(projected) ->
          {:cont, {:ok, [projected | acc]}}

        _invalid ->
          {:halt, {:error, :query_storage_inconsistent}}
      end
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      {:error, _reason} = error -> error
    end
  rescue
    _error -> {:error, :query_storage_inconsistent}
  end

  defp validate_boundary(nil), do: :ok

  defp validate_boundary({updated_at_ms, id})
       when is_integer(updated_at_ms) and updated_at_ms >= 0 and
              updated_at_ms <= @maximum_exact_integer and is_binary(id) do
    if Limits.valid_run_id?(id), do: :ok, else: {:error, :query_cursor_invalid}
  end

  defp validate_boundary(_boundary), do: {:error, :query_cursor_invalid}

  defp normalize_score(score)
       when is_integer(score) and score >= 0 and score <= @maximum_exact_integer,
       do: {:ok, score}

  defp normalize_score(score)
       when is_float(score) and score >= 0 and score <= @maximum_exact_integer do
    normalized = trunc(score)
    if score == normalized, do: {:ok, normalized}, else: :error
  end

  defp normalize_score(_score), do: :error

  defp ref_boundary({id, updated_at_ms}), do: {updated_at_ms, id}

  defp ref_after_boundary?(_ref, nil, _direction), do: true

  defp ref_after_boundary?({id, updated_at_ms}, {boundary_ms, boundary_id}, :asc),
    do: {updated_at_ms, id} > {boundary_ms, boundary_id}

  defp ref_after_boundary?({id, updated_at_ms}, {boundary_ms, boundary_id}, :desc),
    do: {updated_at_ms, id} < {boundary_ms, boundary_id}

  defp normalize_flush(:ok), do: :ok
  defp normalize_flush(_failure), do: {:error, :query_storage_unavailable}

  defp normalize_mirror(:ok), do: :ok
  defp normalize_mirror(_failure), do: {:error, :query_storage_unavailable}

  defp normalize_error("NOPERM" <> _rest), do: {:error, :unauthorized_scope}
  defp normalize_error("ERR invalid Flow system metadata"), do: {:error, :query_engine_failure}
  defp normalize_error(_reason), do: {:error, :query_storage_unavailable}
end
