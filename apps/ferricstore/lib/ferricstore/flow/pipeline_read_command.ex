defmodule Ferricstore.Flow.PipelineReadCommand do
  @moduledoc false

  alias Ferricstore.Flow.Codec
  alias Ferricstore.Flow.{InfoAPI, ReadAPI}
  alias Ferricstore.Store.Router

  @default_max_count 10_000
  @default_payload_return_max_bytes 64 * 1024

  def command(_ctx, {:get, id, opts}) when is_binary(id) and is_list(opts) do
    with :ok <- validate_id(id),
         :ok <- validate_opts(opts),
         {:ok, payload_return} <- payload_return_opts(opts, false),
         {:ok, partition_key} <- optional_partition_key(opts),
         :ok <- validate_key_size(Ferricstore.Flow.Keys.state_key(id, partition_key)) do
      {:get, id, partition_key, payload_return}
    end
  end

  def command(ctx, {:flow_get, id, opts}) do
    command(ctx, {:get, id, opts})
  end

  def command(_ctx, {:history, id, opts}) when is_binary(id) and is_list(opts) do
    with {:ok, {partition_key, history_key, query, include_cold?, consistent?, value_return}} <-
           history_query_attrs(id, opts) do
      {:history, id, partition_key, history_key, query, include_cold?, consistent?, value_return}
    end
  end

  def command(ctx, {:flow_history, id, opts}) do
    command(ctx, {:history, id, opts})
  end

  def command(ctx, {:list, type, opts}) when is_binary(type) and is_list(opts),
    do: read_result(fn -> ReadAPI.list(ctx, type, opts) end)

  def command(ctx, {:flow_list, type, opts}) do
    command(ctx, {:list, type, opts})
  end

  def command(ctx, {:terminals, type, opts}) when is_binary(type) and is_list(opts),
    do: read_result(fn -> ReadAPI.terminals(ctx, type, opts) end)

  def command(ctx, {:flow_terminals, type, opts}) do
    command(ctx, {:terminals, type, opts})
  end

  def command(ctx, {:failures, type, opts}) when is_binary(type) and is_list(opts),
    do: read_result(fn -> ReadAPI.failures(ctx, type, opts) end)

  def command(ctx, {:flow_failures, type, opts}) do
    command(ctx, {:failures, type, opts})
  end

  def command(ctx, {:by_parent, parent_flow_id, opts})
      when is_binary(parent_flow_id) and is_list(opts),
      do: read_result(fn -> ReadAPI.by_parent(ctx, parent_flow_id, opts) end)

  def command(ctx, {:flow_by_parent, parent_flow_id, opts}) do
    command(ctx, {:by_parent, parent_flow_id, opts})
  end

  def command(ctx, {:by_root, root_flow_id, opts})
      when is_binary(root_flow_id) and is_list(opts),
      do: read_result(fn -> ReadAPI.by_root(ctx, root_flow_id, opts) end)

  def command(ctx, {:flow_by_root, root_flow_id, opts}) do
    command(ctx, {:by_root, root_flow_id, opts})
  end

  def command(ctx, {:by_correlation, correlation_id, opts})
      when is_binary(correlation_id) and is_list(opts),
      do: read_result(fn -> ReadAPI.by_correlation(ctx, correlation_id, opts) end)

  def command(ctx, {:flow_by_correlation, correlation_id, opts}) do
    command(ctx, {:by_correlation, correlation_id, opts})
  end

  def command(ctx, {:info, type, opts}) when is_binary(type) and is_list(opts),
    do: read_result(fn -> InfoAPI.info(ctx, type, opts) end)

  def command(ctx, {:flow_info, type, opts}) do
    command(ctx, {:info, type, opts})
  end

  def command(ctx, {:stuck, type, opts}) when is_binary(type) and is_list(opts),
    do: read_result(fn -> ReadAPI.stuck(ctx, type, opts) end)

  def command(ctx, {:flow_stuck, type, opts}) do
    command(ctx, {:stuck, type, opts})
  end

  def command(_ctx, _op), do: {:error, "ERR unsupported flow pipeline read command"}

  def decode_get(nil), do: {:ok, nil}

  def decode_get(value) when is_binary(value) do
    {:ok, Codec.decode_record(value)}
  rescue
    _ -> {:ok, nil}
  end

  def decode_get({:error, _reason} = error), do: error
  def decode_get(_other), do: {:ok, nil}

  def history_results(history_ops, ctx) do
    Ferricstore.Flow.PipelineHistoryRead.results(history_ops, ctx, %{
      read: &Ferricstore.Flow.HistoryRead.read/8,
      fetch_count: &Ferricstore.Flow.HistoryRead.query_fetch_count/1,
      hot_range: &Ferricstore.Flow.HistoryRead.hot_range/5,
      rank_range_many: &Router.flow_index_rank_range_many/2,
      fallback: &Ferricstore.Flow.HistoryRead.hot_fallback_scan/4,
      from_event_ids: &Ferricstore.Flow.HistoryRead.from_event_ids/6,
      apply_query: &Ferricstore.Flow.HistoryRead.apply_query/2
    })
  end

  defp read_result(read_fun), do: {:other, read_fun}

  defp history_query_attrs(id, opts) do
    with :ok <- validate_opts(opts),
         :ok <- validate_id(id),
         {:ok, partition_key} <- optional_partition_key(opts),
         history_key = Ferricstore.Flow.Keys.history_key(id, partition_key),
         :ok <- validate_key_size(history_key),
         {:ok, count} <- flow_count(opts),
         {:ok, include_cold?} <- optional_boolean(opts, :include_cold, true),
         {:ok, consistent?} <- optional_boolean(opts, :consistent_projection, true),
         {:ok, value_return} <- history_value_return_opts(opts),
         {:ok, query} <- flow_history_query_opts(opts, count) do
      {:ok, {partition_key, history_key, query, include_cold?, consistent?, value_return}}
    end
  end

  defp validate_opts(opts) do
    if Keyword.keyword?(opts), do: :ok, else: {:error, "ERR flow opts must be a keyword list"}
  end

  defp validate_id(id) when is_binary(id) and id != "", do: :ok
  defp validate_id(_id), do: {:error, "ERR flow id must be a non-empty string"}

  defp validate_key_size(key) do
    if byte_size(key) <= Router.max_key_size() do
      :ok
    else
      {:error, "ERR key too large (max #{Router.max_key_size()} bytes)"}
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

  defp payload_return_opts(opts, default_enabled?) do
    with {:ok, full?} <- optional_boolean(opts, :full, default_enabled?),
         {:ok, enabled?} <- optional_boolean(opts, :payload, full?),
         {:ok, max_bytes} <-
           optional_non_neg_integer(
             opts,
             :payload_max_bytes,
             flow_payload_return_max_bytes()
           ) do
      {:ok, %{enabled?: enabled?, max_bytes: max_bytes}}
    end
  end

  defp history_value_return_opts(opts) do
    with {:ok, enabled?} <- optional_boolean(opts, :values, false),
         {:ok, max_bytes} <-
           optional_non_neg_integer(
             opts,
             :payload_max_bytes,
             flow_payload_return_max_bytes()
           ) do
      {:ok, %{enabled?: enabled?, max_bytes: max_bytes}}
    end
  end

  defp flow_payload_return_max_bytes do
    case Application.get_env(
           :ferricstore,
           :flow_payload_return_max_bytes,
           @default_payload_return_max_bytes
         ) do
      value when is_integer(value) and value >= 0 -> value
      _ -> @default_payload_return_max_bytes
    end
  end

  defp flow_history_query_opts(opts, count) do
    with {:ok, from_event} <- optional_binary_or_nil(opts, :from_event, nil),
         {:ok, to_event} <- optional_binary_or_nil(opts, :to_event, nil),
         {:ok, from_ms} <- optional_non_neg_integer(opts, :from_ms, nil),
         {:ok, to_ms} <- optional_non_neg_integer(opts, :to_ms, nil),
         {:ok, from_version} <- optional_non_neg_integer(opts, :from_version, nil),
         {:ok, to_version} <- optional_non_neg_integer(opts, :to_version, nil),
         {:ok, rev?} <- optional_boolean(opts, :rev, false),
         {:ok, event} <- optional_binary_or_nil(opts, :event, nil),
         {:ok, worker} <- optional_binary_or_nil(opts, :worker, nil),
         :ok <- Ferricstore.Flow.HistoryQuery.validate_ms_range(from_ms, to_ms),
         :ok <- Ferricstore.Flow.HistoryQuery.validate_version_range(from_version, to_version),
         :ok <-
           Ferricstore.Flow.HistoryQuery.validate_event_range(
             from_event,
             to_event,
             &Ferricstore.Flow.HistoryEvent.ms/1
           ) do
      {:ok,
       %{
         count: count,
         from_event: from_event,
         to_event: to_event,
         from_ms: from_ms,
         to_ms: to_ms,
         from_version: from_version,
         to_version: to_version,
         rev?: rev?,
         event: event,
         worker: worker
       }}
    end
  end

  defp optional_partition_key(opts) do
    case Keyword.get(opts, :partition_key, nil) do
      nil -> {:ok, nil}
      :global -> {:ok, nil}
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, "ERR flow partition_key must be a non-empty string or :global"}
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
