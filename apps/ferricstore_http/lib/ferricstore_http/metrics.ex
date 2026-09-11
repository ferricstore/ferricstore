defmodule FerricstoreHttp.Metrics do
  @moduledoc false

  use GenServer

  alias FerricstoreHttp.Auth.Cache

  @table __MODULE__
  @duration_buckets_ms [5, 10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 30_000]
  @auth_cache_sources [:hit, :miss, :coalesced, :bypass, :timeout]

  @spec start_link(term()) :: GenServer.on_start()
  def start_link(_opts), do: GenServer.start_link(__MODULE__, :ok, name: __MODULE__)

  @spec observe(map()) :: :ok
  def observe(metrics) when is_map(metrics) do
    if :ets.whereis(@table) != :undefined, do: record(metrics)
    :ok
  end

  @spec observe_auth_cache(atom()) :: :ok
  def observe_auth_cache(source) do
    if :ets.whereis(@table) != :undefined do
      increment({:auth_cache, normalize_auth_cache_source(source)}, 1)
    end

    :ok
  end

  @spec observe_command_batch(pos_integer(), non_neg_integer()) :: :ok
  def observe_command_batch(request_count, command_count)
      when is_integer(request_count) and request_count > 0 and is_integer(command_count) and
             command_count >= 0 do
    if :ets.whereis(@table) != :undefined do
      increment(:command_batches, 1)
      increment(:command_batch_requests, request_count)
      increment(:command_batch_commands, command_count)
      if request_count > 1, do: increment(:command_multi_request_batches, 1)
    end

    :ok
  end

  @spec command_batch_stats() :: %{
          batches: non_neg_integer(),
          commands: non_neg_integer(),
          multi_request_batches: non_neg_integer(),
          requests: non_neg_integer()
        }
  def command_batch_stats do
    %{
      batches: counter(:command_batches),
      commands: counter(:command_batch_commands),
      multi_request_batches: counter(:command_multi_request_batches),
      requests: counter(:command_batch_requests)
    }
  end

  @spec render() :: binary()
  def render do
    entries = if :ets.whereis(@table) == :undefined, do: [], else: :ets.tab2list(@table)

    request_lines =
      for {{:requests, method, status}, count} <- entries do
        ~s(ferricstore_http_requests_total{method="#{method}",status="#{status}"} #{count})
      end

    [
      "# TYPE ferricstore_http_requests_total counter",
      Enum.sort(request_lines),
      "# TYPE ferricstore_http_request_duration_seconds histogram",
      duration_lines(entries),
      auth_cache_lines(entries),
      command_batch_lines(entries),
      admission_lines(),
      ""
    ]
    |> List.flatten()
    |> Enum.join("\n")
  end

  @impl GenServer
  def init(:ok) do
    table = :ets.new(@table, [:named_table, :public, :set, write_concurrency: true])
    {:ok, table}
  end

  defp record(metrics) do
    req = Map.get(metrics, :req, %{})
    method = normalize_method(Map.get(req, :method))
    status = normalize_status(Map.get(metrics, :resp_status))
    duration_us = duration_us(metrics)

    increment({:requests, method, status}, 1)
    increment({:duration_count, method}, 1)
    increment({:duration_us, method}, duration_us)
    increment({:duration_bucket, method, exact_bucket(duration_us)}, 1)
  end

  defp normalize_method(method)
       when method in ["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"], do: method

  defp normalize_method(_method), do: "OTHER"

  defp normalize_status(status) when is_integer(status) and status >= 100 and status <= 599,
    do: status

  defp normalize_status(_status), do: 0

  defp normalize_auth_cache_source(source) when source in @auth_cache_sources, do: source
  defp normalize_auth_cache_source(_source), do: :other

  defp duration_us(%{req_start: start_time, req_end: end_time})
       when is_integer(start_time) and is_integer(end_time) do
    System.convert_time_unit(end_time - start_time, :native, :microsecond)
  end

  defp duration_us(_metrics), do: 0

  defp increment(key, amount) do
    _value = :ets.update_counter(@table, key, {2, amount}, {key, 0})
    :ok
  end

  defp duration_lines(entries) do
    methods = for {{:duration_count, method}, _count} <- entries, do: method

    methods
    |> Enum.uniq()
    |> Enum.sort()
    |> Enum.flat_map(&duration_method_lines(entries, &1))
  end

  defp duration_method_lines(entries, method) do
    count = entry(entries, {:duration_count, method})
    sum_seconds = entry(entries, {:duration_us, method}) / 1_000_000

    buckets =
      Enum.map(@duration_buckets_ms, fn bucket_ms ->
        value = cumulative_bucket(entries, method, bucket_ms)

        ~s(ferricstore_http_request_duration_seconds_bucket{method="#{method}",le="#{bucket_ms / 1_000}"} #{value})
      end)

    buckets ++
      [
        ~s(ferricstore_http_request_duration_seconds_bucket{method="#{method}",le="+Inf"} #{count}),
        ~s(ferricstore_http_request_duration_seconds_count{method="#{method}"} #{count}),
        ~s(ferricstore_http_request_duration_seconds_sum{method="#{method}"} #{sum_seconds})
      ]
  end

  defp exact_bucket(duration_us) do
    Enum.find(@duration_buckets_ms, :infinity, &(duration_us <= &1 * 1_000))
  end

  defp cumulative_bucket(entries, method, bucket_ms) do
    Enum.reduce(@duration_buckets_ms, 0, fn exact_ms, total ->
      if exact_ms <= bucket_ms,
        do: total + entry(entries, {:duration_bucket, method, exact_ms}),
        else: total
    end)
  end

  defp entry(entries, key) do
    case List.keyfind(entries, key, 0) do
      {^key, value} -> value
      nil -> 0
    end
  end

  defp counter(key) do
    case :ets.whereis(@table) do
      :undefined -> 0
      _table -> :ets.lookup_element(@table, key, 2, 0)
    end
  end

  defp admission_lines do
    case FerricstoreHttp.Admission.stats() do
      nil ->
        []

      %{in_flight: in_flight, limit: limit} ->
        [
          "# TYPE ferricstore_http_in_flight_requests gauge",
          "ferricstore_http_in_flight_requests #{in_flight}",
          "# TYPE ferricstore_http_in_flight_request_limit gauge",
          "ferricstore_http_in_flight_request_limit #{limit}"
        ]
    end
  end

  defp auth_cache_lines(entries) do
    sources = @auth_cache_sources ++ [:other]
    stats = Cache.stats()

    [
      "# TYPE ferricstore_http_auth_cache_requests_total counter",
      Enum.map(sources, fn source ->
        value = entry(entries, {:auth_cache, source})
        ~s(ferricstore_http_auth_cache_requests_total{result="#{source}"} #{value})
      end),
      "# TYPE ferricstore_http_auth_cache_entries gauge",
      "ferricstore_http_auth_cache_entries #{stats.entries}",
      "# TYPE ferricstore_http_auth_cache_entry_limit gauge",
      "ferricstore_http_auth_cache_entry_limit #{stats.max_entries}",
      "# TYPE ferricstore_http_auth_cache_pending gauge",
      "ferricstore_http_auth_cache_pending #{stats.pending}"
    ]
  end

  defp command_batch_lines(entries) do
    [
      "# TYPE ferricstore_http_command_batches_total counter",
      "ferricstore_http_command_batches_total #{entry(entries, :command_batches)}",
      "# TYPE ferricstore_http_command_batch_requests_total counter",
      "ferricstore_http_command_batch_requests_total #{entry(entries, :command_batch_requests)}",
      "# TYPE ferricstore_http_command_batch_commands_total counter",
      "ferricstore_http_command_batch_commands_total #{entry(entries, :command_batch_commands)}",
      "# TYPE ferricstore_http_command_multi_request_batches_total counter",
      "ferricstore_http_command_multi_request_batches_total #{entry(entries, :command_multi_request_batches)}"
    ]
  end
end
