defmodule Ferricstore.Flow.ReadAPI do
  @moduledoc false

  alias Ferricstore.CommandTime
  alias Ferricstore.Flow.HistoryQuery
  alias Ferricstore.Flow.LMDBIndexRead
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
         {:ok, include_cold?} <- optional_boolean(opts, :include_cold, false),
         {:ok, consistent_projection?} <- optional_boolean(opts, :consistent_projection, false),
         {:ok, records} <-
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
           ) do
      {:ok, records}
    end
  end

  def list(_ctx, type, _opts) when not is_binary(type),
    do: {:error, "ERR flow type must be a non-empty string"}

  def list(_ctx, _type, _opts), do: {:error, "ERR flow opts must be a keyword list"}

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

  defp flow_max_count do
    case Application.get_env(:ferricstore, :flow_max_count, @default_max_count) do
      value when is_integer(value) and value > 0 -> value
      _ -> @default_max_count
    end
  end

  defp flow_state(opts) do
    case Keyword.get(opts, :state, @default_state) do
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
