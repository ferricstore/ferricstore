defmodule Ferricstore.Flow.ReadAPI do
  @moduledoc false

  alias Ferricstore.CommandTime
  alias Ferricstore.Flow.Attributes
  alias Ferricstore.Flow.HistoryQuery
  alias Ferricstore.Flow.InfoAPI
  alias Ferricstore.Flow.InfoCountRead
  alias Ferricstore.Flow.IndexQuery
  alias Ferricstore.Flow.Keys
  alias Ferricstore.Flow.LMDBIndexRead
  alias Ferricstore.Flow.PolicyAttributeCatalog
  alias Ferricstore.Flow.RecordProjection
  alias Ferricstore.Flow.RecordQuery
  alias Ferricstore.Flow.RecordRead
  alias Ferricstore.Flow.ScopeBinding
  alias Ferricstore.Flow.StateMeta
  alias Ferricstore.Flow.TerminalQuery

  @default_state "queued"
  @default_max_count 1_000
  @default_lmdb_query_scan_limit 10_000
  @max_exact_integer 9_007_199_254_740_991
  @terminal_states ["completed", "failed", "cancelled"]

  def list(ctx, type, opts \\ [])

  def list(ctx, type, opts) when is_binary(type) and is_list(opts) do
    with :ok <- validate_opts(opts),
         :ok <- validate_type(type),
         {:ok, state} <- flow_state(opts),
         {:ok, partition_key} <- optional_auto_partition_key(opts),
         {:ok, count} <- flow_count(opts),
         {:ok, attributes} <- Attributes.from_opts(opts),
         {:ok, query} <- flow_index_query_opts(opts, count),
         {:ok, include_cold?} <- optional_boolean(opts, :include_cold, false),
         {:ok, consistent_projection?} <- optional_boolean(opts, :consistent_projection, false),
         {:ok, ctx, partition_key, _metadata} <-
           ScopeBinding.bind_read_partition_selector(ctx, :runs, partition_key),
         {:ok, records} <-
           list_records(
             ctx,
             type,
             state,
             partition_key,
             count,
             attributes,
             include_cold?,
             consistent_projection?,
             query
           ) do
      {:ok, maybe_project_meta(records, opts)}
    end
  end

  def list(_ctx, type, _opts) when not is_binary(type),
    do: {:error, "ERR flow type must be a non-empty string"}

  def list(_ctx, _type, _opts), do: {:error, "ERR flow opts must be a keyword list"}

  def search(ctx, opts \\ [])

  def search(ctx, opts) when is_list(opts) do
    with :ok <- validate_opts(opts),
         {:ok, type} <- optional_binary_or_nil(opts, :type, nil),
         :ok <- validate_optional_type(type),
         {:ok, state} <- optional_search_state(opts),
         {:ok, partition_key} <- optional_auto_partition_key(opts),
         {:ok, count} <- flow_count(opts),
         {:ok, attributes} <- Attributes.from_opts(opts),
         {:ok, state_meta} <- StateMeta.query_from_opts(opts),
         :ok <- validate_search_filters(attributes, state_meta),
         {:ok, query} <- flow_index_query_opts(opts, count),
         {:ok, consistent_projection?} <- optional_boolean(opts, :consistent_projection, false),
         {:ok, ctx, partition_key, _metadata} <-
           ScopeBinding.bind_read_partition_selector(ctx, :runs, partition_key),
         :ok <-
           validate_indexed_search_attributes(
             ctx,
             type,
             state,
             partition_key,
             attributes,
             consistent_projection?
           ),
         :ok <- validate_indexed_search_state_meta(ctx, type, state_meta),
         {:ok, records} <-
           search_records(
             ctx,
             type,
             state,
             partition_key,
             attributes,
             state_meta,
             query,
             consistent_projection?
           ) do
      {:ok,
       records
       |> filter_optional_type(type)
       |> filter_optional_state(state)
       |> Enum.filter(&IndexQuery.record_matches?(&1, query, @terminal_states))
       |> Enum.filter(&Attributes.matches?(&1, attributes))
       |> Enum.filter(&StateMeta.matches?(&1, state_meta))
       |> RecordQuery.sort_by_update()
       |> RecordQuery.maybe_reverse(query.rev?)
       |> Enum.take(count)
       |> Enum.map(&RecordProjection.public/1)}
    end
  end

  def search(_ctx, _opts), do: {:error, "ERR flow opts must be a keyword list"}

  def attributes(ctx, type, opts \\ [])

  def attributes(ctx, type, opts) when is_binary(type) and is_list(opts) do
    with :ok <- validate_opts(opts),
         :ok <- validate_type(type),
         {:ok, state} <- flow_state(opts),
         {:ok, partition_key} <- optional_auto_partition_key(opts),
         {:ok, count} <- flow_count(opts),
         {:ok, consistent_projection?} <- optional_boolean(opts, :consistent_projection, false),
         {:ok, ctx, partition_key, _metadata} <-
           ScopeBinding.bind_read_partition_selector(ctx, :runs, partition_key),
         {:ok, names} <- policy_indexed_attributes(ctx, type),
         {:ok, entries} <-
           attribute_name_entries(ctx, type, state, partition_key, names, consistent_projection?) do
      {:ok,
       entries
       |> Enum.filter(fn %{count: count} -> count > 0 end)
       |> Enum.sort_by(fn %{name: name, count: count} -> {-count, name} end)
       |> Enum.take(count)}
    end
  end

  def attributes(_ctx, type, _opts) when not is_binary(type),
    do: {:error, "ERR flow type must be a non-empty string"}

  def attributes(_ctx, _type, _opts), do: {:error, "ERR flow opts must be a keyword list"}

  def attribute_values(ctx, type, attr_name, opts \\ [])

  def attribute_values(ctx, type, attr_name, opts)
      when is_binary(type) and is_list(opts) do
    with :ok <- validate_opts(opts),
         :ok <- validate_type(type),
         {:ok, attr_name} <- Attributes.normalize_name(attr_name),
         {:ok, state} <- flow_state(opts),
         {:ok, partition_key} <- optional_auto_partition_key(opts),
         {:ok, count} <- flow_count(opts),
         {:ok, consistent_projection?} <- optional_boolean(opts, :consistent_projection, false),
         {:ok, ctx, partition_key, _metadata} <-
           ScopeBinding.bind_read_partition_selector(ctx, :runs, partition_key),
         {:ok, names} <- policy_indexed_attributes(ctx, type),
         true <- attr_name in names,
         {:ok, entries} <-
           attribute_value_entries(
             ctx,
             type,
             state,
             attr_name,
             partition_key,
             consistent_projection?
           ) do
      {:ok, Enum.take(entries, count)}
    else
      false -> {:ok, []}
      {:error, _reason} = error -> error
    end
  end

  def attribute_values(_ctx, type, _attr_name, _opts) when not is_binary(type),
    do: {:error, "ERR flow type must be a non-empty string"}

  def attribute_values(_ctx, _type, _attr_name, _opts),
    do: {:error, "ERR flow opts must be a keyword list"}

  def terminals(ctx, type, opts \\ [])

  def terminals(ctx, type, opts) when is_binary(type) and is_list(opts) do
    with :ok <- validate_opts(opts),
         :ok <- validate_type(type),
         {:ok, state} <- flow_terminal_state(opts),
         {:ok, partition_key} <- optional_auto_partition_key(opts),
         {:ok, count} <- flow_count(opts),
         {:ok, include_cold?} <- optional_boolean(opts, :include_cold, false),
         {:ok, consistent_projection?} <- optional_boolean(opts, :consistent_projection, false),
         {:ok, rev?} <- optional_boolean(opts, :rev, false),
         {:ok, from_ms} <- optional_non_neg_integer(opts, :from_ms, nil),
         {:ok, to_ms} <- optional_non_neg_integer(opts, :to_ms, nil),
         :ok <- validate_ms_range(from_ms, to_ms),
         {:ok, ctx, partition_key, _metadata} <-
           ScopeBinding.bind_read_partition_selector(ctx, :runs, partition_key),
         query = %{from_ms: from_ms, to_ms: to_ms, rev?: rev?},
         {:ok, records} <-
           RecordRead.terminal_records(
             ctx,
             type,
             state,
             partition_key,
             count,
             include_cold? or consistent_projection?,
             consistent_projection?,
             query,
             @terminal_states,
             @default_lmdb_query_scan_limit
           ) do
      {:ok,
       records
       |> RecordQuery.filter_by_ms(from_ms, to_ms)
       |> RecordQuery.sort_by_update()
       |> RecordQuery.maybe_reverse(rev?)
       |> Enum.take(count)
       |> Enum.map(&RecordProjection.public/1)}
    end
  end

  def terminals(_ctx, type, _opts) when not is_binary(type),
    do: {:error, "ERR flow type must be a non-empty string"}

  def terminals(_ctx, _type, _opts), do: {:error, "ERR flow opts must be a keyword list"}

  def failures(ctx, type, opts \\ [])

  def failures(ctx, type, opts) when is_binary(type) and is_list(opts) do
    terminals(ctx, type, Keyword.put(opts, :state, "failed"))
  end

  def failures(_ctx, type, _opts) when not is_binary(type),
    do: {:error, "ERR flow type must be a non-empty string"}

  def failures(_ctx, _type, _opts), do: {:error, "ERR flow opts must be a keyword list"}

  def by_parent(ctx, parent_flow_id, opts \\ [])

  def by_parent(ctx, parent_flow_id, opts)
      when is_binary(parent_flow_id) and is_list(opts) do
    with :ok <- validate_opts(opts),
         :ok <- validate_id(parent_flow_id),
         {:ok, partition_key} <- optional_partition_key(opts),
         {:ok, count} <- flow_count(opts),
         {:ok, query} <- flow_index_query_opts(opts, count),
         {:ok, include_cold?} <- optional_boolean(opts, :include_cold, false),
         {:ok, consistent_projection?} <- optional_boolean(opts, :consistent_projection, false),
         {:ok, ctx, partition_key, _metadata} <-
           ScopeBinding.bind_read_partition_selector(ctx, :runs, partition_key),
         {:ok, records} <-
           related_index_records(
             ctx,
             partition_key,
             query,
             include_cold? or consistent_projection?,
             consistent_projection?,
             &Keys.parent_index_key(parent_flow_id, &1),
             fn record ->
               Map.get(record, :parent_flow_id) == parent_flow_id and
                 IndexQuery.record_matches?(record, query, @terminal_states)
             end
           ) do
      result =
        RecordRead.filter_index_records(
          records,
          :parent_flow_id,
          parent_flow_id,
          query,
          @terminal_states
        )

      {:ok, Enum.map(result, &RecordProjection.public/1)}
    end
  end

  def by_parent(_ctx, parent_flow_id, _opts) when not is_binary(parent_flow_id),
    do: {:error, "ERR flow parent_flow_id must be a non-empty string"}

  def by_parent(_ctx, _parent_flow_id, _opts),
    do: {:error, "ERR flow opts must be a keyword list"}

  def by_root(ctx, root_flow_id, opts \\ [])

  def by_root(ctx, root_flow_id, opts) when is_binary(root_flow_id) and is_list(opts) do
    with :ok <- validate_opts(opts),
         :ok <- validate_id(root_flow_id),
         {:ok, partition_key} <- optional_partition_key(opts),
         {:ok, count} <- flow_count(opts),
         {:ok, query} <- flow_index_query_opts(opts, count),
         {:ok, include_cold?} <- optional_boolean(opts, :include_cold, false),
         {:ok, consistent_projection?} <- optional_boolean(opts, :consistent_projection, false),
         {:ok, ctx, partition_key, _metadata} <-
           ScopeBinding.bind_read_partition_selector(ctx, :runs, partition_key),
         {:ok, indexed_records} <-
           related_index_records(
             ctx,
             partition_key,
             query,
             include_cold? or consistent_projection?,
             consistent_projection?,
             &Keys.root_index_key(root_flow_id, &1),
             fn record ->
               Map.get(record, :root_flow_id) == root_flow_id and
                 IndexQuery.record_matches?(record, query, @terminal_states)
             end
           ),
         {:ok, root_record} <- RecordRead.root_record(ctx, root_flow_id, partition_key) do
      records =
        [root_record | indexed_records]
        |> Enum.reject(&is_nil/1)
        |> Enum.uniq_by(&Map.get(&1, :id))

      result =
        RecordRead.filter_index_records(
          records,
          :root_flow_id,
          root_flow_id,
          query,
          @terminal_states
        )

      {:ok, Enum.map(result, &RecordProjection.public/1)}
    end
  end

  def by_root(_ctx, root_flow_id, _opts) when not is_binary(root_flow_id),
    do: {:error, "ERR flow root_flow_id must be a non-empty string"}

  def by_root(_ctx, _root_flow_id, _opts), do: {:error, "ERR flow opts must be a keyword list"}

  def by_correlation(ctx, correlation_id, opts \\ [])

  def by_correlation(ctx, correlation_id, opts)
      when is_binary(correlation_id) and is_list(opts) do
    with :ok <- validate_opts(opts),
         :ok <- validate_id(correlation_id),
         {:ok, partition_key} <- optional_partition_key(opts),
         {:ok, count} <- flow_count(opts),
         {:ok, query} <- flow_index_query_opts(opts, count),
         {:ok, include_cold?} <- optional_boolean(opts, :include_cold, false),
         {:ok, consistent_projection?} <- optional_boolean(opts, :consistent_projection, false),
         {:ok, ctx, partition_key, _metadata} <-
           ScopeBinding.bind_read_partition_selector(ctx, :runs, partition_key),
         {:ok, records} <-
           related_index_records(
             ctx,
             partition_key,
             query,
             include_cold? or consistent_projection?,
             consistent_projection?,
             &Keys.correlation_index_key(correlation_id, &1),
             fn record ->
               Map.get(record, :correlation_id) == correlation_id and
                 IndexQuery.record_matches?(record, query, @terminal_states)
             end
           ) do
      result =
        RecordRead.filter_index_records(
          records,
          :correlation_id,
          correlation_id,
          query,
          @terminal_states
        )

      {:ok, Enum.map(result, &RecordProjection.public/1)}
    end
  end

  def by_correlation(_ctx, correlation_id, _opts) when not is_binary(correlation_id),
    do: {:error, "ERR flow correlation_id must be a non-empty string"}

  def by_correlation(_ctx, _correlation_id, _opts),
    do: {:error, "ERR flow opts must be a keyword list"}

  def stuck(ctx, type, opts \\ [])

  def stuck(ctx, type, opts) when is_binary(type) and is_list(opts) do
    with :ok <- validate_opts(opts),
         :ok <- validate_type(type),
         {:ok, partition_key} <- optional_auto_partition_key(opts),
         {:ok, count} <- flow_count(opts),
         {:ok, older_than_ms} <- optional_non_neg_integer(opts, :older_than_ms, 0),
         {:ok, now_ms} <- optional_non_neg_integer(opts, :now_ms, CommandTime.now_ms()),
         cutoff = now_ms - older_than_ms,
         {:ok, ctx, partition_key, _metadata} <-
           ScopeBinding.bind_read_partition_selector(ctx, :runs, partition_key),
         {:ok, records} <- RecordRead.stuck_records(ctx, type, partition_key, cutoff, count) do
      {:ok, Enum.map(records, &RecordProjection.public/1)}
    end
  end

  def stuck(_ctx, type, _opts) when not is_binary(type),
    do: {:error, "ERR flow type must be a non-empty string"}

  def stuck(_ctx, _type, _opts), do: {:error, "ERR flow opts must be a keyword list"}

  def stats(ctx, type, opts \\ [])

  def stats(ctx, type, opts) when is_binary(type) and is_list(opts) do
    with :ok <- validate_opts(opts),
         :ok <- validate_type(type),
         {:ok, state} <- flow_state(opts),
         {:ok, partition_key} <- optional_auto_partition_key(opts),
         {:ok, attributes} <- Attributes.from_opts(opts),
         {:ok, consistent_projection?} <- optional_boolean(opts, :consistent_projection, true),
         {:ok, ctx, partition_key, _metadata} <-
           ScopeBinding.bind_read_partition_selector(ctx, :runs, partition_key),
         {:ok, count} <-
           stats_count_records(
             ctx,
             type,
             state,
             partition_key,
             attributes,
             consistent_projection?
           ) do
      {:ok,
       %{
         type: type,
         state: state,
         attributes: attributes,
         count: count
       }}
    end
  end

  def stats(_ctx, type, _opts) when not is_binary(type),
    do: {:error, "ERR flow type must be a non-empty string"}

  def stats(_ctx, _type, _opts), do: {:error, "ERR flow opts must be a keyword list"}

  defp stats_count_records(_ctx, _type, "any", _partition_key, attributes, _consistent?)
       when map_size(attributes) == 0,
       do: {:error, "ERR flow state any requires attributes"}

  defp stats_count_records(ctx, type, state, partition_key, attributes, consistent?)
       when map_size(attributes) == 0 do
    stats_state_count(ctx, type, state, partition_key, consistent?)
  end

  defp stats_count_records(ctx, type, state, partition_key, attributes, consistent?) do
    search_state = if state == "any", do: nil, else: state

    with {:ok, {name, value}} <-
           select_attribute_filter(
             ctx,
             type,
             search_state,
             partition_key,
             attributes,
             consistent?
           ) do
      stats_count_attribute_exact(
        ctx,
        type,
        search_state,
        partition_key,
        name,
        value,
        attributes,
        consistent?
      )
    end
  end

  defp stats_state_count(ctx, type, state, partition_key, consistent?)
       when state in @terminal_states do
    with {:ok, info} <-
           InfoAPI.info(ctx, type,
             partition_key: partition_key,
             include_cold: true,
             consistent_projection: consistent?
           ) do
      {:ok, Map.get(info, String.to_atom(state), 0)}
    end
  end

  defp stats_state_count(ctx, type, state, :auto, _consistent?) do
    state_keys =
      ScopeBinding.auto_partition_keys(ctx)
      |> Enum.map(&{state, Keys.state_index_key(type, state, &1)})

    with :ok <- validate_index_keys(state_keys),
         {:ok, counts} <-
           InfoCountRead.zset_count_many(ctx, Enum.map(state_keys, fn {_state, key} -> key end)) do
      {:ok, Enum.sum(counts)}
    end
  end

  defp stats_state_count(ctx, type, state, partition_key, _consistent?) do
    key = Keys.state_index_key(type, state, partition_key)

    with :ok <- validate_key_size(key),
         {:ok, [count]} <- InfoCountRead.zset_count_many(ctx, [key]) do
      {:ok, count}
    end
  end

  defp stats_count_attribute_exact(
         ctx,
         type,
         state,
         :auto,
         name,
         value,
         attributes,
         consistent?
       ) do
    scan_limit = stats_attribute_scan_limit()

    ScopeBinding.auto_partition_keys(ctx)
    |> Enum.reduce_while({:ok, 0, scan_limit}, fn partition_key, {:ok, total, remaining} ->
      case stats_count_attribute_exact_partition(
             ctx,
             type,
             state,
             partition_key,
             name,
             value,
             attributes,
             consistent?,
             remaining,
             scan_limit
           ) do
        {:ok, count, scanned} -> {:cont, {:ok, total + count, remaining - scanned}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, count, _remaining} -> {:ok, count}
      {:error, _reason} = error -> error
    end
  end

  defp stats_count_attribute_exact(
         ctx,
         type,
         state,
         partition_key,
         name,
         value,
         attributes,
         consistent?
       ) do
    scan_limit = stats_attribute_scan_limit()

    with {:ok, count, _scanned} <-
           stats_count_attribute_exact_partition(
             ctx,
             type,
             state,
             partition_key,
             name,
             value,
             attributes,
             consistent?,
             scan_limit,
             scan_limit
           ) do
      {:ok, count}
    end
  end

  defp stats_count_attribute_exact_partition(
         ctx,
         type,
         state,
         partition_key,
         name,
         value,
         attributes,
         consistent?,
         remaining,
         scan_limit
       ) do
    with {:ok, candidate_count} <-
           attribute_candidate_count(ctx, type, state, partition_key, name, value, consistent?) do
      cond do
        candidate_count == 0 ->
          {:ok, 0, 0}

        candidate_count > remaining ->
          {:error, "ERR flow stats exact attribute count exceeds scan limit #{scan_limit}"}

        true ->
          with {:ok, records} <-
                 list_records_for_search_attribute(
                   ctx,
                   type,
                   state,
                   partition_key,
                   name,
                   value,
                   default_index_query(candidate_count),
                   consistent?
                 ) do
            {:ok,
             records
             |> Enum.filter(&(Map.get(&1, :type) == type))
             |> filter_optional_state(state)
             |> Enum.filter(&Attributes.matches?(&1, attributes))
             |> length(), candidate_count}
          end
      end
    end
  end

  defp validate_index_keys(state_keys) do
    Enum.reduce_while(state_keys, :ok, fn {_state, key}, :ok ->
      case validate_key_size(key) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp stats_attribute_scan_limit do
    case Application.get_env(
           :ferricstore,
           :flow_stats_attribute_scan_limit,
           @default_lmdb_query_scan_limit
         ) do
      value when is_integer(value) and value > 0 -> value
      _ -> @default_lmdb_query_scan_limit
    end
  end

  defp list_records(
         ctx,
         type,
         state,
         partition_key,
         count,
         attributes,
         include_cold?,
         consistent_projection?,
         query
       )
       when map_size(attributes) == 0 do
    if state == "any" do
      {:error, "ERR flow state any requires attributes"}
    else
      RecordRead.list_records(
        ctx,
        type,
        state,
        partition_key,
        count,
        query,
        include_cold? or consistent_projection?,
        consistent_projection?,
        @terminal_states,
        @default_lmdb_query_scan_limit
      )
    end
  end

  defp list_records(
         ctx,
         type,
         state,
         partition_key,
         count,
         attributes,
         _include_cold?,
         consistent_projection?,
         query
       ) do
    with {:ok, {name, value}} <-
           select_attribute_filter(
             ctx,
             type,
             if(state == "any", do: nil, else: state),
             partition_key,
             attributes,
             consistent_projection?
           ),
         {:ok, records} <-
           list_records_for_search_attribute_filtered(
             ctx,
             type,
             if(state == "any", do: nil, else: state),
             partition_key,
             name,
             value,
             query,
             consistent_projection?,
             fn record ->
               Map.get(record, :type) == type and
                 (state == "any" or Map.get(record, :state) == state) and
                 IndexQuery.record_matches?(record, query, @terminal_states) and
                 Attributes.matches?(record, attributes)
             end
           ) do
      {:ok,
       records
       |> Enum.filter(&(Map.get(&1, :type) == type))
       |> filter_optional_state(if(state == "any", do: nil, else: state))
       |> Enum.filter(&IndexQuery.record_matches?(&1, query, @terminal_states))
       |> Enum.filter(&Attributes.matches?(&1, attributes))
       |> RecordQuery.sort_by_update()
       |> RecordQuery.maybe_reverse(query.rev?)
       |> Enum.take(count)}
    end
  end

  defp list_records_for_search_attribute(
         ctx,
         type,
         state,
         :auto,
         name,
         value,
         query,
         consistent_projection?
       ) do
    RecordQuery.bounded_auto_partition_records(
      ScopeBinding.auto_partition_keys(ctx),
      query.count,
      query.rev?,
      fn partition_key, fetch_count ->
        list_records_for_search_attribute(
          ctx,
          type,
          state,
          partition_key,
          name,
          value,
          %{query | count: fetch_count},
          consistent_projection?
        )
      end
    )
  end

  defp list_records_for_search_attribute(
         ctx,
         type,
         state,
         partition_key,
         name,
         value,
         query,
         consistent_projection?
       ) do
    value = Attributes.index_value(value)

    index_key = attribute_index_key(type, state, name, value, partition_key)

    with :ok <- validate_key_size(index_key) do
      RecordRead.records_for_index(
        ctx,
        index_key,
        partition_key,
        query,
        true,
        consistent_projection?,
        @default_lmdb_query_scan_limit
      )
    end
  end

  defp list_records_for_search_attribute_filtered(
         ctx,
         type,
         state,
         :auto,
         name,
         value,
         query,
         consistent_projection?,
         match_fun
       ) do
    RecordQuery.bounded_auto_partition_filtered_records(
      ScopeBinding.auto_partition_keys(ctx),
      query.count,
      query.rev?,
      fn partition_key, fetch_count, scan_budget ->
        indexed_value = Attributes.index_value(value)
        index_key = attribute_index_key(type, state, name, indexed_value, partition_key)

        with :ok <- validate_key_size(index_key) do
          RecordRead.records_for_index_filtered_with_count(
            ctx,
            index_key,
            partition_key,
            %{query | count: fetch_count},
            true,
            consistent_projection?,
            scan_budget,
            match_fun
          )
        end
      end
    )
  end

  defp list_records_for_search_attribute_filtered(
         ctx,
         type,
         state,
         partition_key,
         name,
         value,
         query,
         consistent_projection?,
         match_fun
       ) do
    value = Attributes.index_value(value)
    index_key = attribute_index_key(type, state, name, value, partition_key)

    with :ok <- validate_key_size(index_key) do
      RecordRead.records_for_index_filtered(
        ctx,
        index_key,
        partition_key,
        query,
        true,
        consistent_projection?,
        @default_lmdb_query_scan_limit,
        match_fun
      )
    end
  end

  defp related_index_records(
         ctx,
         :auto,
         query,
         include_cold?,
         consistent?,
         index_key_fun,
         match_fun
       ) do
    RecordQuery.bounded_auto_partition_filtered_records(
      ScopeBinding.auto_partition_keys(ctx),
      query.count,
      query.rev?,
      fn partition_key, fetch_count, scan_budget ->
        index_key = index_key_fun.(partition_key)

        with :ok <- validate_key_size(index_key) do
          RecordRead.records_for_index_filtered_with_count(
            ctx,
            index_key,
            partition_key,
            %{query | count: fetch_count},
            include_cold?,
            consistent?,
            scan_budget,
            match_fun
          )
        end
      end
    )
  end

  defp related_index_records(
         ctx,
         partition_key,
         query,
         include_cold?,
         consistent?,
         index_key_fun,
         match_fun
       ) do
    index_key = index_key_fun.(partition_key)

    with :ok <- validate_key_size(index_key) do
      RecordRead.records_for_index_filtered(
        ctx,
        index_key,
        partition_key,
        query,
        include_cold?,
        consistent?,
        @default_lmdb_query_scan_limit,
        match_fun
      )
    end
  end

  defp filter_optional_type(records, nil), do: records

  defp filter_optional_type(records, type),
    do: Enum.filter(records, &(Map.get(&1, :type) == type))

  defp filter_optional_state(records, nil), do: records

  defp filter_optional_state(records, state),
    do: Enum.filter(records, &(Map.get(&1, :state) == state))

  defp validate_search_filters(attributes, state_meta)
       when map_size(attributes) > 0 or map_size(state_meta) > 0,
       do: :ok

  defp validate_search_filters(_attributes, _state_meta),
    do: {:error, "ERR flow search requires attributes or state_meta"}

  defp validate_indexed_search_attributes(ctx, nil, state, partition_key, attributes, consistent?) do
    attributes
    |> Map.to_list()
    |> Enum.reduce_while(:ok, fn {name, value}, :ok ->
      if projected_indexed_attribute?(ctx, state, partition_key, name, value, consistent?) do
        {:cont, :ok}
      else
        {:halt, {:error, "ERR flow attribute #{name} is not indexed for broad search"}}
      end
    end)
  end

  defp validate_indexed_search_attributes(
         ctx,
         type,
         _state,
         _partition_key,
         attributes,
         _consistent?
       )
       when is_binary(type) do
    with {:ok, indexed_names} <- policy_indexed_attributes(ctx, type) do
      missing =
        attributes
        |> Map.keys()
        |> Enum.reject(&(&1 in indexed_names))

      case missing do
        [] ->
          :ok

        [name | _rest] ->
          {:error, "ERR flow attribute #{name} is not indexed for broad search"}
      end
    end
  end

  defp validate_indexed_search_state_meta(_ctx, _type, state_meta) when map_size(state_meta) == 0,
    do: :ok

  defp validate_indexed_search_state_meta(_ctx, nil, _state_meta),
    do: {:error, "ERR flow state_meta search requires type"}

  defp validate_indexed_search_state_meta(ctx, type, state_meta) when is_binary(type) do
    with {:ok, indexed_key} <- policy_indexed_state_meta(ctx, type) do
      missing =
        state_meta
        |> StateMeta.candidate_filters()
        |> Enum.map(fn {_state, name, _value} -> name end)
        |> Enum.reject(&(&1 == indexed_key))

      case {indexed_key, missing} do
        {nil, [{_state, name, _value} | _rest]} ->
          {:error, "ERR flow state_meta #{name} is not indexed for broad search"}

        {nil, [name | _rest]} ->
          {:error, "ERR flow state_meta #{name} is not indexed for broad search"}

        {_key, []} ->
          :ok

        {_key, [name | _rest]} ->
          {:error, "ERR flow state_meta #{name} is not indexed for broad search"}
      end
    end
  end

  defp search_records(
         ctx,
         type,
         state,
         partition_key,
         attributes,
         state_meta,
         query,
         consistent_projection?
       )
       when map_size(state_meta) > 0 do
    with {:ok, {meta_state, name, value}} <-
           select_state_meta_filter(ctx, type, partition_key, state_meta, consistent_projection?) do
      list_records_for_search_state_meta_filtered(
        ctx,
        type,
        meta_state,
        partition_key,
        name,
        value,
        query,
        consistent_projection?,
        fn record ->
          (is_nil(type) or Map.get(record, :type) == type) and
            (is_nil(state) or Map.get(record, :state) == state) and
            IndexQuery.record_matches?(record, query, @terminal_states) and
            Attributes.matches?(record, attributes) and
            StateMeta.matches?(record, state_meta)
        end
      )
    end
  end

  defp search_records(
         ctx,
         type,
         state,
         partition_key,
         attributes,
         _state_meta,
         query,
         consistent_projection?
       ) do
    with {:ok, {name, value}} <-
           select_attribute_filter(
             ctx,
             type,
             state,
             partition_key,
             attributes,
             consistent_projection?
           ) do
      list_records_for_search_attribute_filtered(
        ctx,
        type,
        state,
        partition_key,
        name,
        value,
        query,
        consistent_projection?,
        fn record ->
          (is_nil(type) or Map.get(record, :type) == type) and
            (is_nil(state) or Map.get(record, :state) == state) and
            IndexQuery.record_matches?(record, query, @terminal_states) and
            Attributes.matches?(record, attributes)
        end
      )
    end
  end

  defp projected_indexed_attribute?(ctx, state, partition_key, name, values, consistent?) do
    policy_indexed_attribute?(ctx, name) or
      values
      |> attribute_filter_values()
      |> Enum.any?(fn value ->
        case attribute_candidate_count(ctx, nil, state, partition_key, name, value, consistent?) do
          {:ok, count} -> count > 0
          {:error, _reason} -> false
        end
      end)
  end

  defp policy_indexed_attribute?(ctx, name) do
    key = Keys.policy_indexed_attribute_count_key(name)

    case Ferricstore.Stats.with_cache_tracking_disabled(fn ->
           Ferricstore.Store.Router.get(ctx, key)
         end) do
      <<count::unsigned-big-64>> when count > 0 ->
        true

      <<0::unsigned-big-64>> ->
        false

      _missing_or_invalid ->
        if PolicyAttributeCatalog.indexed_member_exists?(ctx, name) do
          :ok = PolicyAttributeCatalog.request_repair(ctx, name)
          true
        else
          false
        end
    end
  end

  defp validate_optional_type(nil), do: :ok
  defp validate_optional_type(type), do: validate_type(type)

  defp optional_search_state(opts) do
    case Keyword.get(opts, :state, nil) do
      nil -> {:ok, nil}
      :any -> {:ok, nil}
      "any" -> {:ok, nil}
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, "ERR flow state must be a non-empty string"}
    end
  end

  defp default_index_query(count) do
    %{
      count: count,
      from_ms: nil,
      to_ms: nil,
      rev?: false,
      state: nil,
      terminal_only?: false
    }
  end

  defp select_attribute_filter(ctx, type, state, partition_key, attributes, consistent?) do
    case attribute_candidate_filters(attributes) do
      [] ->
        {:error, "ERR flow attributes must not be empty"}

      [candidate] ->
        {:ok, candidate}

      candidates ->
        candidates
        |> Enum.map(fn {name, value} = candidate ->
          {attribute_candidate_count(ctx, type, state, partition_key, name, value, consistent?),
           candidate, {name, Attributes.index_value(value)}}
        end)
        |> select_scored_candidate()
        |> then(&{:ok, &1})
    end
  end

  defp attribute_candidate_filters(attributes) do
    attributes
    |> Map.to_list()
    |> Enum.flat_map(fn {name, value} ->
      value
      |> attribute_filter_values()
      |> Enum.map(&{name, &1})
    end)
  end

  defp attribute_filter_values(values) when is_list(values), do: values
  defp attribute_filter_values(value), do: [value]

  defp attribute_candidate_count(ctx, type, state, :auto, name, value, consistent?) do
    ScopeBinding.auto_partition_keys(ctx)
    |> Enum.reduce_while({:ok, 0}, fn partition_key, {:ok, acc} ->
      case attribute_candidate_count(ctx, type, state, partition_key, name, value, consistent?) do
        {:ok, count} -> {:cont, {:ok, acc + count}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp attribute_candidate_count(ctx, type, state, partition_key, name, value, consistent?) do
    value = Attributes.index_value(value)
    index_key = attribute_index_key(type, state, name, value, partition_key)

    if byte_size(index_key) <= Ferricstore.Store.Router.max_key_size() do
      with {:ok, lmdb_count} <-
             LMDBIndexRead.query_count(ctx, index_key, partition_key, consistent?) do
        {:ok, candidate_scan_count(lmdb_count, ram_attribute_candidate_count(ctx, index_key))}
      end
    else
      {:error, "ERR key too large (max #{Ferricstore.Store.Router.max_key_size()} bytes)"}
    end
  end

  defp ram_attribute_candidate_count(ctx, index_key) do
    case InfoCountRead.zset_count_many(ctx, [index_key]) do
      {:ok, [count]} -> count
      _ -> 0
    end
  end

  defp select_state_meta_filter(ctx, type, partition_key, state_meta, consistent?) do
    case StateMeta.candidate_filters(state_meta) do
      [] ->
        {:error, "ERR flow state_meta must not be empty"}

      [candidate] ->
        {:ok, candidate}

      candidates ->
        candidates
        |> Enum.map(fn {meta_state, name, value} = candidate ->
          {state_meta_candidate_count(
             ctx,
             type,
             meta_state,
             partition_key,
             name,
             value,
             consistent?
           ), candidate, {meta_state, name, StateMeta.index_value(value)}}
        end)
        |> select_scored_candidate()
        |> then(&{:ok, &1})
    end
  end

  defp state_meta_candidate_count(ctx, type, meta_state, :auto, name, value, consistent?) do
    ScopeBinding.auto_partition_keys(ctx)
    |> Enum.reduce_while({:ok, 0}, fn partition_key, {:ok, acc} ->
      case state_meta_candidate_count(
             ctx,
             type,
             meta_state,
             partition_key,
             name,
             value,
             consistent?
           ) do
        {:ok, count} -> {:cont, {:ok, acc + count}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp state_meta_candidate_count(ctx, type, meta_state, partition_key, name, value, consistent?) do
    value = StateMeta.index_value(value)
    index_key = Keys.state_meta_index_key(type, meta_state, name, value, partition_key)

    if byte_size(index_key) <= Ferricstore.Store.Router.max_key_size() do
      with {:ok, lmdb_count} <-
             LMDBIndexRead.query_count(ctx, index_key, partition_key, consistent?) do
        {:ok, candidate_scan_count(lmdb_count, ram_attribute_candidate_count(ctx, index_key))}
      end
    else
      {:error, "ERR key too large (max #{Ferricstore.Store.Router.max_key_size()} bytes)"}
    end
  end

  defp list_records_for_search_state_meta_filtered(
         ctx,
         type,
         meta_state,
         :auto,
         name,
         value,
         query,
         consistent_projection?,
         match_fun
       ) do
    RecordQuery.bounded_auto_partition_filtered_records(
      ScopeBinding.auto_partition_keys(ctx),
      query.count,
      query.rev?,
      fn partition_key, fetch_count, scan_budget ->
        indexed_value = StateMeta.index_value(value)

        index_key =
          Keys.state_meta_index_key(type, meta_state, name, indexed_value, partition_key)

        with :ok <- validate_key_size(index_key) do
          RecordRead.records_for_index_filtered_with_count(
            ctx,
            index_key,
            partition_key,
            %{query | count: fetch_count},
            true,
            consistent_projection?,
            scan_budget,
            match_fun
          )
        end
      end
    )
  end

  defp list_records_for_search_state_meta_filtered(
         ctx,
         type,
         meta_state,
         partition_key,
         name,
         value,
         query,
         consistent_projection?,
         match_fun
       ) do
    value = StateMeta.index_value(value)
    index_key = Keys.state_meta_index_key(type, meta_state, name, value, partition_key)

    with :ok <- validate_key_size(index_key) do
      RecordRead.records_for_index_filtered(
        ctx,
        index_key,
        partition_key,
        query,
        true,
        consistent_projection?,
        @default_lmdb_query_scan_limit,
        match_fun
      )
    end
  end

  defp attribute_name_entries(_ctx, _type, _state, _partition_key, [], _consistent?),
    do: {:ok, []}

  defp attribute_name_entries(ctx, type, state, :auto, names, consistent?) do
    names
    |> Enum.reduce_while({:ok, %{}}, fn name, {:ok, acc} ->
      case attribute_name_count(ctx, type, state, :auto, name, consistent?) do
        {:ok, {count, approximate?}} ->
          {:cont, {:ok, Map.put(acc, name, {count, approximate?})}}

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, counts} ->
        {:ok,
         Enum.map(counts, fn {name, {count, approximate?}} ->
           maybe_put_approximate(%{name: name, count: count}, approximate?)
         end)}

      {:error, _reason} = error ->
        error
    end
  end

  defp attribute_name_entries(ctx, type, state, partition_key, names, consistent?) do
    names
    |> Enum.reduce_while({:ok, []}, fn name, {:ok, acc} ->
      case attribute_name_count(ctx, type, state, partition_key, name, consistent?) do
        {:ok, {count, approximate?}} ->
          entry = maybe_put_approximate(%{name: name, count: count}, approximate?)
          {:cont, {:ok, [entry | acc]}}

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, entries} -> {:ok, Enum.reverse(entries)}
      {:error, _reason} = error -> error
    end
  end

  defp attribute_name_count(ctx, type, state, :auto, name, consistent?) do
    partition_keys = ScopeBinding.auto_partition_keys(ctx)
    scan_limit = discovery_auto_bucket_scan_limit(partition_keys)

    partition_keys
    |> Enum.reduce_while({:ok, {0, false}}, fn partition_key, {:ok, {count_acc, approx_acc}} ->
      case attribute_name_prefix_count(
             ctx,
             type,
             state,
             partition_key,
             name,
             consistent?,
             scan_limit
           ) do
        {:ok, {count, approximate?}} ->
          {:cont, {:ok, {count_acc + count, approx_acc or approximate?}}}

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
  end

  defp attribute_name_count(ctx, type, "any", partition_key, name, consistent?) do
    index_key_prefix = Keys.attribute_type_index_prefix(type, name, partition_key)

    attribute_live_count_for_prefix(ctx, index_key_prefix, partition_key, consistent?)
  end

  defp attribute_name_count(ctx, type, state, partition_key, name, consistent?) do
    index_key_prefix = Keys.attribute_index_prefix(type, state, name, partition_key)

    attribute_live_count_for_prefix(ctx, index_key_prefix, partition_key, consistent?)
  end

  defp attribute_name_prefix_count(
         ctx,
         type,
         "any",
         partition_key,
         name,
         consistent?,
         scan_limit
       ) do
    index_key_prefix = Keys.attribute_type_index_prefix(type, name, partition_key)

    attribute_live_count_for_prefix(
      ctx,
      index_key_prefix,
      partition_key,
      consistent?,
      scan_limit
    )
  end

  defp attribute_name_prefix_count(
         ctx,
         type,
         state,
         partition_key,
         name,
         consistent?,
         scan_limit
       ) do
    index_key_prefix = Keys.attribute_index_prefix(type, state, name, partition_key)

    attribute_live_count_for_prefix(
      ctx,
      index_key_prefix,
      partition_key,
      consistent?,
      scan_limit
    )
  end

  defp attribute_live_count_for_prefix(
         ctx,
         index_key_prefix,
         partition_key,
         consistent?,
         scan_limit \\ nil
       ) do
    with {:ok, {counts, approximate?}} <-
           attribute_value_counts_for_prefix(
             ctx,
             index_key_prefix,
             partition_key,
             consistent?,
             scan_limit
           ) do
      {:ok, {Enum.reduce(counts, 0, fn {_value, count}, acc -> acc + count end), approximate?}}
    end
  end

  defp attribute_value_entries(ctx, type, state, attr_name, :auto, consistent?) do
    auto_partition_keys = ScopeBinding.auto_partition_keys(ctx)
    scan_limit = discovery_auto_bucket_scan_limit(auto_partition_keys)

    auto_partition_keys
    |> Enum.reduce_while({:ok, {%{}, false}}, fn partition_key, {:ok, {acc, approx_acc}} ->
      case attribute_value_counts(
             ctx,
             type,
             state,
             attr_name,
             partition_key,
             consistent?,
             scan_limit
           ) do
        {:ok, {counts, approximate?}} ->
          {:cont, {:ok, {merge_counts(acc, counts), approx_acc or approximate?}}}

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, {counts, approximate?}} -> {:ok, top_attribute_values(counts, approximate?)}
      {:error, _reason} = error -> error
    end
  end

  defp attribute_value_entries(ctx, type, state, attr_name, partition_key, consistent?) do
    with {:ok, {counts, approximate?}} <-
           attribute_value_counts(
             ctx,
             type,
             state,
             attr_name,
             partition_key,
             consistent?
           ) do
      {:ok, top_attribute_values(counts, approximate?)}
    end
  end

  defp attribute_value_counts(
         ctx,
         type,
         state,
         attr_name,
         partition_key,
         consistent?,
         scan_limit \\ nil
       )

  defp attribute_value_counts(ctx, type, "any", attr_name, partition_key, consistent?, scan_limit) do
    index_key_prefix = Keys.attribute_type_index_prefix(type, attr_name, partition_key)

    attribute_value_counts_for_prefix(
      ctx,
      index_key_prefix,
      partition_key,
      consistent?,
      scan_limit
    )
  end

  defp attribute_value_counts(ctx, type, state, attr_name, partition_key, consistent?, scan_limit) do
    index_key_prefix = Keys.attribute_index_prefix(type, state, attr_name, partition_key)

    attribute_value_counts_for_prefix(
      ctx,
      index_key_prefix,
      partition_key,
      consistent?,
      scan_limit
    )
  end

  defp attribute_value_counts_for_prefix(
         ctx,
         index_key_prefix,
         partition_key,
         consistent?,
         scan_limit
       ) do
    scan_limit = scan_limit || discovery_scan_limit()

    with {:ok, chunks} <-
           LMDBIndexRead.query_prefix_raw_entries(
             ctx,
             index_key_prefix,
             partition_key,
             scan_limit + 1,
             consistent?
           ) do
      now_ms = CommandTime.now_ms()
      raw_prefix = Ferricstore.Flow.LMDB.query_index_raw_prefix(index_key_prefix)
      raw_count = Enum.reduce(chunks, 0, fn {_path, entries}, acc -> acc + length(entries) end)
      approximate? = raw_count > scan_limit

      with {:ok, counts} <-
             attribute_value_counts_from_chunks(
               chunks,
               raw_prefix,
               now_ms,
               &live_query_index_value?/4
             ) do
        {:ok, {counts, approximate?}}
      end
    end
  end

  defp attribute_value_counts_from_chunks(chunks, raw_prefix, now_ms, classify_fun)
       when is_list(chunks) and is_binary(raw_prefix) and is_integer(now_ms) and
              is_function(classify_fun, 4) do
    Enum.reduce_while(chunks, {:ok, %{}}, fn
      {path, entries}, {:ok, acc} when is_binary(path) and is_list(entries) ->
        case attribute_value_counts_from_entries(
               entries,
               path,
               raw_prefix,
               now_ms,
               classify_fun,
               acc
             ) do
          {:ok, counts} -> {:cont, {:ok, counts}}
          {:error, _reason} = error -> {:halt, error}
        end

      invalid, _acc ->
        {:halt, {:error, {:invalid_query_index_chunk, invalid}}}
    end)
  end

  defp attribute_value_counts_from_entries(
         entries,
         path,
         raw_prefix,
         now_ms,
         classify_fun,
         initial
       ) do
    Enum.reduce_while(entries, {:ok, initial}, fn
      {key, value}, {:ok, acc} when is_binary(key) and is_binary(value) ->
        if String.starts_with?(key, raw_prefix) do
          case classify_fun.(path, key, value, now_ms) do
            {:ok, {:live, discovery_component}} ->
              case attribute_value_from_query_component(discovery_component) do
                {:ok, attribute_value} ->
                  {:cont, {:ok, Map.update(acc, attribute_value, 1, &(&1 + 1))}}

                :error ->
                  {:halt, {:error, {:invalid_query_index_key, key}}}
              end

            {:ok, :expired} ->
              {:cont, {:ok, acc}}

            {:error, _reason} = error ->
              {:halt, error}

            invalid ->
              {:halt, {:error, {:invalid_query_index_classification, invalid}}}
          end
        else
          {:halt, {:error, {:invalid_query_index_key, key}}}
        end

      invalid, _acc ->
        {:halt, {:error, {:invalid_query_index_entry, invalid}}}
    end)
  end

  defp live_query_index_value?(path, key, value, now_ms),
    do: live_query_index_value?(path, key, value, now_ms, &Ferricstore.Flow.LMDB.write_batch/2)

  defp live_query_index_value?(path, key, value, now_ms, delete_fun)
       when is_function(delete_fun, 2) do
    case Ferricstore.Flow.LMDB.decode_query_index_value(value) do
      {:ok,
       {family_digest, index_digest, discovery_component, id, updated_at_ms, expire_at_ms,
        _state_key}} ->
        cond do
          not Ferricstore.Flow.LMDB.query_index_entry_key?(
            key,
            family_digest,
            index_digest,
            id,
            updated_at_ms
          ) ->
            {:error, {:invalid_query_index_value, key}}

          expire_at_ms <= 0 or expire_at_ms > now_ms ->
            {:ok, {:live, discovery_component}}

          true ->
            case delete_fun.(path, [{:delete, key}]) do
              :ok -> {:ok, :expired}
              {:error, _reason} = error -> error
              invalid -> {:error, {:invalid_query_index_delete_result, invalid}}
            end
        end

      :error ->
        {:error, {:invalid_query_index_value, key}}
    end
  end

  defp attribute_value_from_query_component(encoded_value) when is_binary(encoded_value) do
    case Keys.decode_index_component(encoded_value) do
      {:ok, index_value} -> {:ok, Attributes.decode_index_value(index_value)}
      :error -> :error
    end
  end

  defp attribute_value_from_query_component(_invalid), do: :error

  @doc false
  def __attribute_value_counts_from_chunks_for_test__(
        chunks,
        raw_prefix,
        now_ms,
        delete_fun
      )
      when is_function(delete_fun, 2) do
    attribute_value_counts_from_chunks(
      chunks,
      raw_prefix,
      now_ms,
      fn path, key, value, event_now_ms ->
        live_query_index_value?(path, key, value, event_now_ms, delete_fun)
      end
    )
  end

  defp top_attribute_values(counts, approximate?) do
    counts
    |> Enum.map(fn {value, count} -> %{value: value, count: count} end)
    |> Enum.sort_by(fn %{value: value, count: count} -> {-count, inspect(value)} end)
    |> Enum.map(&maybe_put_approximate(&1, approximate?))
  end

  defp maybe_put_approximate(entry, true), do: Map.put(entry, :approximate, true)
  defp maybe_put_approximate(entry, false), do: entry

  defp discovery_scan_limit do
    case Application.get_env(
           :ferricstore,
           :flow_attribute_discovery_scan_limit,
           @default_lmdb_query_scan_limit
         ) do
      value when is_integer(value) and value > 0 -> value
      _ -> @default_lmdb_query_scan_limit
    end
  end

  defp discovery_auto_bucket_scan_limit(partition_keys) do
    bucket_count = max(length(partition_keys), 1)

    discovery_scan_limit()
    |> div(bucket_count)
    |> max(1)
  end

  defp merge_counts(left, right) do
    Enum.reduce(right, left, fn {key, count}, acc ->
      Map.update(acc, key, count, &(&1 + count))
    end)
  end

  defp candidate_scan_count(lmdb_count, ram_count)
       when is_integer(lmdb_count) and lmdb_count >= 0 and is_integer(ram_count) and
              ram_count >= 0 do
    lmdb_count + ram_count
  end

  defp select_scored_candidate(scored_candidates) do
    scored_candidates
    |> Enum.min_by(fn
      {{:ok, count}, _candidate, tie_breaker} when is_integer(count) and count >= 0 ->
        {0, count, tie_breaker}

      {{:error, _reason}, _candidate, tie_breaker} ->
        {1, 0, tie_breaker}
    end)
    |> elem(1)
  end

  @doc false
  def __candidate_scan_count_for_test__(lmdb_count, ram_count),
    do: candidate_scan_count(lmdb_count, ram_count)

  @doc false
  def __select_scored_candidate_for_test__(scored_candidates),
    do: select_scored_candidate(scored_candidates)

  defp attribute_index_key(type, state, name, value, partition_key) do
    cond do
      is_binary(type) and is_binary(state) ->
        Keys.attribute_index_key(type, state, name, value, partition_key)

      is_binary(type) ->
        Keys.attribute_type_index_key(type, name, value, partition_key)

      is_binary(state) ->
        Keys.attribute_state_index_key(state, name, value, partition_key)

      true ->
        Keys.attribute_partition_index_key(name, value, partition_key)
    end
  end

  defp policy_indexed_attributes(ctx, type) do
    case Ferricstore.Flow.Policy.get(ctx, type, []) do
      {:ok, %{indexed_attributes: names}} when is_list(names) -> {:ok, names}
      {:ok, _policy} -> {:ok, []}
      {:error, _reason} = error -> error
    end
  end

  defp policy_indexed_state_meta(ctx, type) do
    case Ferricstore.Flow.Policy.get(ctx, type, []) do
      {:ok, %{indexed_state_meta: key}} when is_binary(key) -> {:ok, key}
      {:ok, _policy} -> {:ok, nil}
      {:error, _reason} = error -> error
    end
  end

  defp validate_opts(opts) do
    if Keyword.keyword?(opts), do: :ok, else: {:error, "ERR flow opts must be a keyword list"}
  end

  defp validate_id(id) when is_binary(id) and id != "", do: :ok
  defp validate_id(_id), do: {:error, "ERR flow id must be a non-empty string"}

  defp validate_type(type) when is_binary(type) and type != "", do: :ok
  defp validate_type(_type), do: {:error, "ERR flow type must be a non-empty string"}

  defp validate_key_size(key) do
    if byte_size(key) <= Ferricstore.Store.Router.max_key_size() do
      :ok
    else
      {:error, "ERR key too large (max #{Ferricstore.Store.Router.max_key_size()} bytes)"}
    end
  end

  defp validate_ms_range(from_ms, to_ms),
    do: HistoryQuery.validate_ms_range(from_ms, to_ms)

  defp maybe_project_meta(records, opts) do
    if Keyword.get(opts, :return) == :meta do
      Enum.map(records, &RecordProjection.meta/1)
    else
      Enum.map(records, &RecordProjection.public/1)
    end
  end

  defp flow_terminal_state(opts), do: TerminalQuery.state(opts, @terminal_states)

  defp flow_index_query_opts(opts, count) do
    with {:ok, from_ms} <- optional_non_neg_integer(opts, :from_ms, nil),
         {:ok, to_ms} <- optional_non_neg_integer(opts, :to_ms, nil),
         {:ok, rev?} <- optional_boolean(opts, :rev, false),
         {:ok, before_id} <- optional_before_id(opts),
         {:ok, state} <- optional_binary_or_nil(opts, :state, nil),
         {:ok, terminal_only?} <- optional_boolean(opts, :terminal_only, false),
         :ok <- validate_ms_range(from_ms, to_ms),
         :ok <- validate_before_id_cursor(before_id, to_ms, rev?) do
      {:ok,
       %{
         count: count,
         from_ms: from_ms,
         to_ms: to_ms,
         rev?: rev?,
         before_id: before_id,
         state: state,
         terminal_only?: terminal_only?
       }}
    end
  end

  defp flow_count(opts) do
    max_count = flow_max_count()

    case Keyword.fetch(opts, :count) do
      :error ->
        {:ok, min(100, max_count)}

      {:ok, value} when is_integer(value) and value > 0 ->
        if value <= max_count do
          {:ok, value}
        else
          {:error, "ERR flow count exceeds maximum #{max_count}"}
        end

      {:ok, _invalid} ->
        {:error, "ERR flow count must be a positive integer"}
    end
  end

  @doc false
  def __flow_count_for_test__(opts), do: flow_count(opts)

  defp flow_max_count do
    case Application.get_env(:ferricstore, :flow_max_count, @default_max_count) do
      value when is_integer(value) and value > 0 -> value
      _ -> @default_max_count
    end
  end

  defp flow_state(opts) do
    case Keyword.get(opts, :state, @default_state) do
      :any -> {:ok, "any"}
      "any" -> {:ok, "any"}
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, "ERR flow state must be a non-empty string"}
    end
  end

  defp optional_partition_key(opts) do
    case Keyword.get(opts, :partition_key, nil) do
      nil -> {:ok, nil}
      value when is_binary(value) and value != "" -> {:ok, value}
      :auto -> {:ok, :auto}
      :any -> {:ok, :auto}
      _ -> {:error, "ERR flow partition_key must be a non-empty string"}
    end
  end

  defp optional_auto_partition_key(opts) do
    case Keyword.get(opts, :partition_key, :auto) do
      nil -> {:ok, nil}
      :auto -> {:ok, :auto}
      :any -> {:ok, :auto}
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, "ERR flow partition_key must be a non-empty string"}
    end
  end

  defp optional_binary_or_nil(opts, key, default) do
    case Keyword.get(opts, key, default) do
      nil -> {:ok, nil}
      :any -> {:ok, nil}
      "any" -> {:ok, nil}
      value when is_binary(value) -> {:ok, value}
      _ -> {:error, "ERR flow #{key} must be a string"}
    end
  end

  defp optional_before_id(opts) do
    case Keyword.fetch(opts, :before_id) do
      :error -> {:ok, nil}
      {:ok, nil} -> {:ok, nil}
      {:ok, value} when is_binary(value) and value != "" -> {:ok, value}
      {:ok, ""} -> {:error, "ERR flow before_id must be a non-empty string"}
      {:ok, _value} -> {:error, "ERR flow before_id must be a string"}
    end
  end

  defp validate_before_id_cursor(nil, _to_ms, _rev?), do: :ok
  defp validate_before_id_cursor(_before_id, to_ms, true) when is_integer(to_ms), do: :ok

  defp validate_before_id_cursor(_before_id, _to_ms, _rev?),
    do: {:error, "ERR flow before_id requires rev: true and to_ms"}

  defp optional_boolean(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_boolean(value) -> {:ok, value}
      _ -> {:error, "ERR flow #{key} must be a boolean"}
    end
  end

  defp optional_non_neg_integer(opts, key, default) do
    case Keyword.fetch(opts, key) do
      {:ok, value}
      when is_integer(value) and value >= 0 and value <= @max_exact_integer ->
        {:ok, value}

      {:ok, value} when is_integer(value) and value > @max_exact_integer ->
        {:error, "ERR flow #{key} exceeds maximum #{@max_exact_integer}"}

      {:ok, _} ->
        {:error, "ERR flow #{key} must be a non-negative integer"}

      :error
      when is_integer(default) and default >= 0 and default <= @max_exact_integer ->
        {:ok, default}

      :error when is_integer(default) and default > @max_exact_integer ->
        {:error, "ERR flow #{key} exceeds maximum #{@max_exact_integer}"}

      :error when is_nil(default) ->
        {:ok, nil}

      :error ->
        {:error, "ERR flow #{key} must be a non-negative integer"}
    end
  end
end
