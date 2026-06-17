defmodule Ferricstore.Flow.ReadAPI do
  @moduledoc false

  alias Ferricstore.CommandTime
  alias Ferricstore.Flow.Attributes
  alias Ferricstore.Flow.HistoryQuery
  alias Ferricstore.Flow.IndexQuery
  alias Ferricstore.Flow.Keys
  alias Ferricstore.Flow.LMDBIndexRead
  alias Ferricstore.Flow.RAMIndexRead
  alias Ferricstore.Flow.RecordProjection
  alias Ferricstore.Flow.RecordQuery
  alias Ferricstore.Flow.RecordRead
  alias Ferricstore.Flow.TerminalQuery

  @default_state "queued"
  @default_max_count 1_000
  @default_lmdb_query_scan_limit 10_000
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
         :ok <- validate_search_attributes(attributes),
         {:ok, query} <- flow_index_query_opts(opts, count),
         {:ok, consistent_projection?} <- optional_boolean(opts, :consistent_projection, false),
         :ok <-
           validate_indexed_search_attributes(
             ctx,
             type,
             state,
             partition_key,
             attributes,
             consistent_projection?
           ),
         {:ok, {name, value}} <-
           select_attribute_filter(
             ctx,
             type,
             state,
             partition_key,
             attributes,
             consistent_projection?
           ),
         {:ok, records} <-
           list_records_for_search_attribute(
             ctx,
             type,
             state,
             partition_key,
             name,
             value,
             attribute_fetch_query(query, attributes),
             consistent_projection?
           ) do
      {:ok,
       records
       |> filter_optional_type(type)
       |> filter_optional_state(state)
       |> Enum.filter(&IndexQuery.record_matches?(&1, query, @terminal_states))
       |> Enum.filter(&Attributes.matches?(&1, attributes))
       |> RecordQuery.sort_by_update()
       |> RecordQuery.maybe_reverse(query.rev?)
       |> Enum.take(count)}
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
         fetch_count =
           RecordQuery.fetch_count(count, from_ms, to_ms, fn count ->
             LMDBIndexRead.query_scan_count(count, @default_lmdb_query_scan_limit)
           end),
         query = %{from_ms: from_ms, to_ms: to_ms, rev?: rev?},
         {:ok, records} <-
           RecordRead.terminal_records(
             ctx,
             type,
             state,
             partition_key,
             fetch_count,
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
       |> Enum.take(count)}
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
         index_key = Ferricstore.Flow.Keys.parent_index_key(parent_flow_id, partition_key),
         :ok <- validate_key_size(index_key),
         {:ok, records} <-
           RecordRead.records_for_index(
             ctx,
             index_key,
             partition_key,
             query,
             include_cold? or consistent_projection?,
             consistent_projection?,
             @default_lmdb_query_scan_limit
           ) do
      {:ok,
       RecordRead.filter_index_records(
         records,
         :parent_flow_id,
         parent_flow_id,
         query,
         @terminal_states
       )}
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
         index_key = Ferricstore.Flow.Keys.root_index_key(root_flow_id, partition_key),
         :ok <- validate_key_size(index_key),
         {:ok, indexed_records} <-
           RecordRead.records_for_index(
             ctx,
             index_key,
             partition_key,
             query,
             include_cold? or consistent_projection?,
             consistent_projection?,
             @default_lmdb_query_scan_limit
           ),
         {:ok, root_record} <- RecordRead.root_record(ctx, root_flow_id, partition_key) do
      records =
        [root_record | indexed_records]
        |> Enum.reject(&is_nil/1)
        |> Enum.uniq_by(&Map.get(&1, :id))

      {:ok,
       RecordRead.filter_index_records(
         records,
         :root_flow_id,
         root_flow_id,
         query,
         @terminal_states
       )}
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
         index_key = Ferricstore.Flow.Keys.correlation_index_key(correlation_id, partition_key),
         :ok <- validate_key_size(index_key),
         {:ok, records} <-
           RecordRead.records_for_index(
             ctx,
             index_key,
             partition_key,
             query,
             include_cold? or consistent_projection?,
             consistent_projection?,
             @default_lmdb_query_scan_limit
           ) do
      {:ok,
       RecordRead.filter_index_records(
         records,
         :correlation_id,
         correlation_id,
         query,
         @terminal_states
       )}
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
         {:ok, records} <- RecordRead.stuck_records(ctx, type, partition_key, cutoff, count) do
      {:ok, records}
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
         {:ok, count} <- flow_stats_count(opts),
         {:ok, records} <-
           list_records(ctx, type, state, partition_key, count, attributes, true, true) do
      {:ok,
       %{
         type: type,
         state: state,
         attributes: attributes,
         count: length(records)
       }}
    end
  end

  def stats(_ctx, type, _opts) when not is_binary(type),
    do: {:error, "ERR flow type must be a non-empty string"}

  def stats(_ctx, _type, _opts), do: {:error, "ERR flow opts must be a keyword list"}

  defp list_records(
         ctx,
         type,
         state,
         partition_key,
         count,
         attributes,
         include_cold?,
         consistent_projection?,
         _query
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
           list_records_for_search_attribute(
             ctx,
             type,
             if(state == "any", do: nil, else: state),
             partition_key,
             name,
             value,
             attribute_fetch_query(query, attributes),
             consistent_projection?
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

  defp attribute_fetch_query(query, attributes) do
    if map_size(attributes) > 1 do
      %{
        query
        | count: LMDBIndexRead.query_scan_count(query.count, @default_lmdb_query_scan_limit)
      }
    else
      query
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
    Ferricstore.Flow.Keys.auto_partition_keys()
    |> Enum.reduce_while({:ok, []}, fn partition_key, {:ok, acc} ->
      case list_records_for_search_attribute(
             ctx,
             type,
             state,
             partition_key,
             name,
             value,
             query,
             consistent_projection?
           ) do
        {:ok, records} -> {:cont, {:ok, RecordQuery.prepend_chunk(records, acc)}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, chunks} -> {:ok, RecordQuery.flatten_chunks(chunks)}
      {:error, _reason} = error -> error
    end
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

  defp filter_optional_type(records, nil), do: records

  defp filter_optional_type(records, type),
    do: Enum.filter(records, &(Map.get(&1, :type) == type))

  defp filter_optional_state(records, nil), do: records

  defp filter_optional_state(records, state),
    do: Enum.filter(records, &(Map.get(&1, :state) == state))

  defp validate_search_attributes(attributes) when map_size(attributes) > 0, do: :ok

  defp validate_search_attributes(_attributes),
    do: {:error, "ERR flow search requires attributes"}

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

  defp projected_indexed_attribute?(ctx, state, partition_key, name, values, consistent?) do
    values
    |> attribute_filter_values()
    |> Enum.any?(fn value ->
      case attribute_candidate_count(ctx, nil, state, partition_key, name, value, consistent?) do
        {:ok, count} -> count > 0
        {:error, _reason} -> false
      end
    end)
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

  defp list_records(
         ctx,
         type,
         state,
         partition_key,
         count,
         attributes,
         include_cold?,
         consistent_projection?
       ) do
    list_records(
      ctx,
      type,
      state,
      partition_key,
      count,
      attributes,
      include_cold?,
      consistent_projection?,
      default_index_query(count)
    )
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
           candidate}
        end)
        |> Enum.min_by(fn {{status, count}, {name, value}} ->
          score = if status == :ok, do: count, else: @default_lmdb_query_scan_limit
          {score, name, Attributes.index_value(value)}
        end)
        |> elem(1)
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
    Keys.auto_partition_keys()
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
        {:ok, max(lmdb_count, ram_attribute_candidate_count(ctx, index_key))}
      end
    else
      {:error, "ERR key too large (max #{Ferricstore.Store.Router.max_key_size()} bytes)"}
    end
  end

  defp ram_attribute_candidate_count(ctx, index_key) do
    query = default_index_query(@default_lmdb_query_scan_limit)

    {:ok, entries} =
      RAMIndexRead.score_entries(ctx, index_key, query, @default_lmdb_query_scan_limit)

    length(entries)
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
    Keys.auto_partition_keys()
    |> Enum.reduce_while({:ok, {0, false}}, fn partition_key, {:ok, {count_acc, approx_acc}} ->
      case attribute_name_prefix_count(ctx, type, state, partition_key, name, consistent?) do
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

  defp attribute_name_prefix_count(ctx, type, "any", partition_key, name, consistent?) do
    index_key_prefix = Keys.attribute_type_index_prefix(type, name, partition_key)

    attribute_raw_count_for_prefix(ctx, index_key_prefix, partition_key, consistent?)
  end

  defp attribute_name_prefix_count(ctx, type, state, partition_key, name, consistent?) do
    index_key_prefix = Keys.attribute_index_prefix(type, state, name, partition_key)

    attribute_raw_count_for_prefix(ctx, index_key_prefix, partition_key, consistent?)
  end

  defp attribute_raw_count_for_prefix(ctx, index_key_prefix, partition_key, consistent?) do
    with {:ok, count} <-
           LMDBIndexRead.query_prefix_count(ctx, index_key_prefix, partition_key, consistent?) do
      {:ok, {count, count > discovery_scan_limit()}}
    end
  end

  defp attribute_live_count_for_prefix(ctx, index_key_prefix, partition_key, consistent?) do
    with {:ok, {counts, approximate?}} <-
           attribute_value_counts_for_prefix(ctx, index_key_prefix, partition_key, consistent?) do
      {:ok, {Enum.reduce(counts, 0, fn {_value, count}, acc -> acc + count end), approximate?}}
    end
  end

  defp attribute_value_entries(ctx, type, state, attr_name, :auto, consistent?) do
    auto_partition_keys = Keys.auto_partition_keys()
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
         scan_limit \\ nil
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

      counts =
        Enum.reduce(chunks, %{}, fn {path, entries}, acc ->
          Enum.reduce(entries, acc, fn {key, value}, acc ->
            case live_query_index_value?(path, key, value, now_ms) do
              true ->
                case attribute_value_from_query_key(key, raw_prefix) do
                  nil -> acc
                  value -> Map.update(acc, value, 1, &(&1 + 1))
                end

              false ->
                acc
            end
          end)
        end)

      {:ok, {counts, approximate?}}
    end
  end

  defp live_query_index_value?(path, key, value, now_ms) do
    case Ferricstore.Flow.LMDB.decode_query_index_value(value) do
      {:ok, {_id, _updated_at_ms, expire_at_ms, _state_key}}
      when expire_at_ms <= 0 or expire_at_ms > now_ms ->
        true

      {:ok, {_id, _updated_at_ms, _expire_at_ms, _state_key}} ->
        Ferricstore.Flow.LMDB.write_batch(path, [{:delete, key}])
        false

      :error ->
        false
    end
  end

  defp attribute_value_from_query_key(key, raw_prefix) do
    prefix_size = byte_size(raw_prefix)

    cond do
      byte_size(key) <= prefix_size ->
        nil

      binary_part(key, 0, prefix_size) != raw_prefix ->
        nil

      true ->
        rest = binary_part(key, prefix_size, byte_size(key) - prefix_size)

        case :binary.match(rest, <<0>>) do
          {value_size, 1} ->
            rest
            |> binary_part(0, value_size)
            |> Attributes.decode_index_value()

          :nomatch ->
            nil
        end
    end
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
      records
    end
  end

  defp flow_terminal_state(opts), do: TerminalQuery.state(opts, @terminal_states)

  defp flow_index_query_opts(opts, count) do
    with {:ok, from_ms} <- optional_non_neg_integer(opts, :from_ms, nil),
         {:ok, to_ms} <- optional_non_neg_integer(opts, :to_ms, nil),
         {:ok, rev?} <- optional_boolean(opts, :rev, false),
         {:ok, state} <- optional_binary_or_nil(opts, :state, nil),
         {:ok, terminal_only?} <- optional_boolean(opts, :terminal_only, false),
         :ok <- validate_ms_range(from_ms, to_ms) do
      {:ok,
       %{
         count: count,
         from_ms: from_ms,
         to_ms: to_ms,
         rev?: rev?,
         state: state,
         terminal_only?: terminal_only?
       }}
    end
  end

  defp flow_count(opts) do
    case Keyword.get(opts, :count, 100) do
      value when is_integer(value) and value > 0 ->
        max_count = flow_max_count()

        if value <= max_count do
          {:ok, value}
        else
          {:error, "ERR flow count exceeds maximum #{max_count}"}
        end

      _ ->
        {:error, "ERR flow count must be a positive integer"}
    end
  end

  defp flow_stats_count(opts), do: flow_count(Keyword.put_new(opts, :count, flow_max_count()))

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
      :any -> {:ok, :any}
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

  defp optional_boolean(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_boolean(value) -> {:ok, value}
      _ -> {:error, "ERR flow #{key} must be a boolean"}
    end
  end

  defp optional_non_neg_integer(opts, key, default) do
    case Keyword.fetch(opts, key) do
      {:ok, value} when is_integer(value) and value >= 0 -> {:ok, value}
      {:ok, _} -> {:error, "ERR flow #{key} must be a non-negative integer"}
      :error when is_integer(default) and default >= 0 -> {:ok, default}
      :error when is_nil(default) -> {:ok, nil}
      :error -> {:error, "ERR flow #{key} must be a non-negative integer"}
    end
  end
end
