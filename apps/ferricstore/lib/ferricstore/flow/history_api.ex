defmodule Ferricstore.Flow.HistoryAPI do
  @moduledoc false

  alias Ferricstore.Flow.{HistoryRead, PayloadReturn}
  alias Ferricstore.Store.Router

  import Ferricstore.Flow.Options,
    only: [
      optional_binary_or_nil: 3,
      optional_boolean: 3,
      optional_non_neg_integer: 3
    ]

  @default_max_count 10_000
  @max_exact_integer 9_007_199_254_740_991

  def history(ctx, id, opts \\ [])

  def history(ctx, id, opts) when is_binary(id) and is_list(opts) do
    with {:ok,
          {partition_key, history_key, query, include_cold?, consistent_projection?, value_return}} <-
           prepare(id, opts) do
      HistoryRead.read(
        ctx,
        id,
        partition_key,
        history_key,
        query,
        include_cold?,
        consistent_projection?,
        value_return
      )
    end
  end

  def history(_ctx, id, _opts) when not is_binary(id),
    do: {:error, "ERR flow id must be a non-empty string"}

  def history(_ctx, _id, _opts), do: {:error, "ERR flow opts must be a keyword list"}

  @doc false
  def prepare(id, opts) when is_binary(id) and is_list(opts) do
    with :ok <- validate_opts(opts),
         :ok <- validate_id(id),
         {:ok, partition_key} <- optional_partition_key(opts),
         history_key = Ferricstore.Flow.Keys.history_key(id, partition_key),
         :ok <- validate_key_size(history_key),
         {:ok, count} <- flow_count(opts),
         {:ok, include_cold?} <- optional_boolean(opts, :include_cold, true),
         {:ok, consistent_projection?} <- optional_boolean(opts, :consistent_projection, true),
         {:ok, value_return} <- PayloadReturn.history_options(opts),
         {:ok, query} <- flow_history_query_opts(opts, count) do
      {:ok,
       {partition_key, history_key, query, include_cold?, consistent_projection?, value_return}}
    end
  end

  def prepare(id, _opts) when not is_binary(id),
    do: {:error, "ERR flow id must be a non-empty string"}

  def prepare(_id, _opts), do: {:error, "ERR flow opts must be a keyword list"}

  defp validate_opts(opts) do
    if Keyword.keyword?(opts) do
      :ok
    else
      {:error, "ERR flow opts must be a keyword list"}
    end
  end

  defp validate_id(id) when is_binary(id) and id != "", do: :ok
  defp validate_id(_id), do: {:error, "ERR flow id must be a non-empty string"}

  defp optional_partition_key(opts) do
    case Keyword.get(opts, :partition_key, nil) do
      nil -> {:ok, nil}
      :global -> {:ok, nil}
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, "ERR flow partition_key must be a non-empty string or :global"}
    end
  end

  defp validate_key_size(key) do
    if byte_size(key) <= Router.max_key_size() do
      :ok
    else
      {:error, "ERR key too large (max #{Router.max_key_size()} bytes)"}
    end
  end

  defp flow_count(opts) do
    max = flow_max_count()

    case Keyword.fetch(opts, :count) do
      :error ->
        {:ok, min(@default_max_count, max)}

      {:ok, value} when is_integer(value) and value > 0 ->
        if value <= max do
          {:ok, value}
        else
          {:error, "ERR flow count exceeds maximum #{max}"}
        end

      {:ok, _invalid} ->
        {:error, "ERR flow count must be a positive integer"}
    end
  end

  defp flow_max_count do
    case Application.get_env(:ferricstore, :flow_max_count, @default_max_count) do
      value when is_integer(value) and value > 0 -> value
      _ -> @default_max_count
    end
  end

  defp flow_history_query_opts(opts, count) do
    with {:ok, from_event} <- optional_binary_or_nil(opts, :from_event, nil),
         {:ok, to_event} <- optional_binary_or_nil(opts, :to_event, nil),
         :ok <- validate_optional_event_cursor(:from_event, from_event),
         :ok <- validate_optional_event_cursor(:to_event, to_event),
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
      query = %{
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
      }

      {:ok, query}
    end
  end

  defp validate_optional_event_cursor(_key, nil), do: :ok

  defp validate_optional_event_cursor(key, event_id) when is_binary(event_id) do
    case :binary.split(event_id, "-", [:global]) do
      [milliseconds, version] ->
        if canonical_event_integer?(milliseconds) and canonical_event_integer?(version),
          do: :ok,
          else: {:error, "ERR flow #{key} must be a history event id"}

      _invalid ->
        {:error, "ERR flow #{key} must be a history event id"}
    end
  end

  defp canonical_event_integer?(value) do
    case Integer.parse(value) do
      {number, ""} when number >= 0 and number <= @max_exact_integer ->
        value == Integer.to_string(number)

      _invalid ->
        false
    end
  end
end
