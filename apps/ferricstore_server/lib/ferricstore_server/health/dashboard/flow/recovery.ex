defmodule FerricstoreServer.Health.Dashboard.Flow.Recovery do
  @moduledoc false

  alias FerricstoreServer.Health.Dashboard.Access, as: DashboardAccess
  alias FerricstoreServer.Health.Dashboard.Flow.PolicyRetention

  import FerricstoreServer.Health.Dashboard.Flow.Calls
  import FerricstoreServer.Health.Dashboard.Flow.Sample
  import FerricstoreServer.Health.Dashboard.FlowRecord

  @flow_dashboard_sample_limit 400
  @flow_dashboard_recent_limit 40

  @spec collect_page(keyword()) :: map()
  def collect_page(opts \\ []) when is_list(opts) do
    filters = flow_failures_filters_from_opts(opts)
    acl_username = DashboardAccess.keyspace_acl_username(opts)

    sampled_records =
      @flow_dashboard_sample_limit
      |> collect_flow_records_sample()
      |> DashboardAccess.filter_flow_records_for_acl(acl_username)

    available_types =
      sampled_records
      |> flow_available_types()
      |> maybe_include_flow_type(filters.type)

    {queried_records, exact_scan_status} =
      if filters.scan_exact do
        flow_recovery_query_records(filters, available_types)
      else
        {[], %{failures: :skipped, stuck: :skipped}}
      end

    records =
      sampled_records
      |> merge_flow_records(
        DashboardAccess.filter_flow_records_for_acl(queried_records, acl_username)
      )
      |> filter_flow_records_by_type(filters.type)
      |> filter_flow_records_by_partition(filters.partition_key)
      |> filter_flow_records_by_name(filters.q)

    candidates =
      records
      |> Enum.filter(&flow_recovery_candidate?/1)
      |> Enum.sort_by(&flow_recovery_sort_key/1)
      |> Enum.take(filters.limit)

    %{
      candidates: candidates,
      summary: flow_recovery_summary(candidates),
      filters: filters,
      available_types: available_types,
      total_sampled: length(sampled_records),
      filtered_sampled: length(records),
      sample_limit: @flow_dashboard_sample_limit,
      exact_scan_status: exact_scan_status,
      flash: Keyword.get(opts, :flash),
      generated_at_ms: System.system_time(:millisecond)
    }
  end

  @spec opts_from_query(binary()) :: keyword()
  def opts_from_query(query) when is_binary(query) do
    params = URI.decode_query(query)

    []
    |> maybe_put_query_opt(:type, normalize_flow_type_filter(Map.get(params, "type")))
    |> maybe_put_query_opt(
      :partition_key,
      normalize_flow_partition_query(Map.get(params, "partition_key"))
    )
    |> maybe_put_query_opt(:q, normalize_flow_name_filter(Map.get(params, "q")))
    |> maybe_put_query_opt(:limit, normalize_flow_limit_filter(Map.get(params, "limit")))
    |> maybe_put_query_opt(:scan_exact, normalize_flow_boolean_filter(Map.get(params, "exact")))
    |> maybe_put_query_opt(:flash, flash_from_params(params))
    |> Enum.reverse()
  end

  def opts_from_query(_query), do: []

  @spec flash_from_query(binary()) :: map() | nil
  def flash_from_query(query) when is_binary(query) do
    query
    |> URI.decode_query()
    |> flash_from_params()
  rescue
    _ -> nil
  end

  def flash_from_query(_query), do: nil

  @spec page_filters(map()) :: map()
  def page_filters(data) when is_map(data) do
    Map.get(data, :filters, %{
      type: nil,
      partition_key: nil,
      q: nil,
      limit: @flow_dashboard_recent_limit,
      scan_exact: false
    })
  end

  @spec apply_form(map()) :: {:ok, map()} | {:error, binary()}
  def apply_form(params) when is_map(params) do
    with "reclaim" <- Map.get(params, "action", "reclaim"),
         :ok <- flow_failures_reclaim_confirmed(params),
         {:ok, type} <- flow_dashboard_required_form_value(params, "type", "flow type"),
         {:ok, worker} <- flow_dashboard_optional_form_value(params, "worker"),
         {:ok, limit} <- flow_dashboard_form_positive_integer(params, "limit", 25, 200),
         {:ok, lease_ms} <-
           flow_dashboard_form_positive_integer(params, "lease_ms", 30_000, 3_600_000),
         partition_key = normalize_flow_partition_query(Map.get(params, "partition_key")),
         opts =
           [
             worker: worker || "dashboard-recovery",
             limit: limit,
             lease_ms: lease_ms
           ]
           |> maybe_put_query_opt(:partition_key, partition_key)
           |> Enum.reverse(),
         {:ok, reclaimed} <- flow_dashboard_flow_reclaim(type, opts) do
      {:ok, %{type: type, reclaimed: length(reclaimed), worker: opts[:worker]}}
    else
      other when other not in [{:error, nil}, nil] and is_binary(other) ->
        {:error, "ERR unsupported recovery action #{other}"}

      {:error, reason} when is_binary(reason) ->
        {:error, reason}

      {:error, reason} ->
        {:error, inspect(reason)}

      other ->
        {:error, "ERR unexpected recovery result: #{inspect(other, limit: 8)}"}
    end
  end

  def apply_form(_params), do: {:error, "ERR recovery form must be a map"}

  @spec flow_failures_filters_from_opts(keyword()) :: map()
  defp flow_failures_filters_from_opts(opts) when is_list(opts) do
    %{
      type: normalize_flow_type_filter(Keyword.get(opts, :type)),
      partition_key: normalize_flow_partition_query(Keyword.get(opts, :partition_key)),
      q: normalize_flow_name_filter(Keyword.get(opts, :q)),
      limit: normalize_flow_limit_filter(Keyword.get(opts, :limit)),
      scan_exact: normalize_flow_boolean_filter(Keyword.get(opts, :scan_exact))
    }
  end

  @spec flash_from_params(map()) :: map() | nil
  defp flash_from_params(params) when is_map(params) do
    case Map.get(params, "status") do
      "reclaimed" ->
        %{
          kind: :ok,
          message:
            "Reclaimed #{PolicyRetention.query_integer(params, "count")} expired lease(s) for #{Map.get(params, "type", "Flow")}"
        }

      "error" ->
        %{kind: :error, message: Map.get(params, "message", "Recovery action failed")}

      _ ->
        nil
    end
  end

  @spec flow_failures_reclaim_confirmed(map()) :: :ok | {:error, binary()}
  defp flow_failures_reclaim_confirmed(params) do
    case Map.get(params, "confirm_reclaim") do
      value when value in ["true", "on", "yes", "1"] ->
        :ok

      _ ->
        {:error, "ERR reclaim requires confirm_reclaim=true after reviewing expired leases"}
    end
  end

  @spec flow_recovery_query_records(map(), [binary()]) ::
          {[map()],
           %{
             failures: :ok | :skipped | {:error, term()},
             stuck: :ok | :skipped | {:error, term()}
           }}
  defp flow_recovery_query_records(%{type: type} = filters, _available_types)
       when is_binary(type) and type != "" do
    flow_recovery_query_records_for_types([type], filters)
  end

  defp flow_recovery_query_records(filters, available_types) do
    available_types
    |> Enum.take(16)
    |> flow_recovery_query_records_for_types(filters)
  end

  @spec flow_recovery_query_records_for_types([binary()], map()) ::
          {[map()],
           %{
             failures: :ok | :skipped | {:error, term()},
             stuck: :ok | :skipped | {:error, term()}
           }}
  defp flow_recovery_query_records_for_types(types, filters) do
    timeout_ms = flow_dashboard_list_fetch_timeout_ms()

    opts =
      [
        count: filters.limit,
        include_cold: true,
        consistent_projection: true
      ]
      |> maybe_put_query_opt(:partition_key, filters.partition_key)
      |> Enum.reverse()

    Enum.reduce(types, {[], %{failures: :skipped, stuck: :skipped}}, fn type,
                                                                        {acc_records, acc_status} ->
      {failures, failures_status} =
        flow_recovery_exact_source(fn -> flow_dashboard_flow_failures(type, opts) end, timeout_ms)

      {stuck, stuck_status} =
        flow_recovery_exact_source(
          fn -> flow_dashboard_flow_stuck(type, Keyword.put(opts, :older_than_ms, 0)) end,
          timeout_ms
        )

      {
        acc_records ++ failures ++ stuck,
        %{
          failures: flow_recovery_merge_status(acc_status.failures, failures_status),
          stuck: flow_recovery_merge_status(acc_status.stuck, stuck_status)
        }
      }
    end)
  end

  @spec flow_recovery_exact_source((-> term()), non_neg_integer()) ::
          {[map()], :ok | {:error, term()}}
  defp flow_recovery_exact_source(fun, timeout_ms) do
    case bounded_dashboard_call(fun, timeout_ms, :flow_recovery_exact) do
      {:ok, {:ok, records}} when is_list(records) -> {records, :ok}
      {:ok, {:error, reason}} -> {[], {:error, reason}}
      {:error, reason} -> {[], {:error, reason}}
      other -> {[], {:error, {:unexpected, other}}}
    end
  end

  defp flow_recovery_merge_status({:error, _} = error, _next), do: error
  defp flow_recovery_merge_status(_previous, {:error, _} = error), do: error
  defp flow_recovery_merge_status(:skipped, :ok), do: :ok
  defp flow_recovery_merge_status(:ok, :ok), do: :ok

  defp flow_recovery_candidate?(record) do
    flow_failed?(record) or flow_expired_lease?(record) or flow_max_attempts_reached?(record)
  end

  defp flow_recovery_sort_key(record) do
    priority =
      cond do
        flow_expired_lease?(record) -> 0
        flow_failed?(record) -> 1
        flow_max_attempts_reached?(record) -> 2
        true -> 3
      end

    {priority, -flow_record_updated_at_ms(record), flow_record_id(record)}
  end

  defp flow_recovery_summary(records) do
    %{
      total: length(records),
      failed: Enum.count(records, &flow_failed?/1),
      expired_leases: Enum.count(records, &flow_expired_lease?/1),
      maxed: Enum.count(records, &flow_max_attempts_reached?/1)
    }
  end

  defp maybe_include_flow_type(types, type) when is_binary(type) and type != "" do
    types
    |> Kernel.++([type])
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp maybe_include_flow_type(types, _type), do: types

  defp flow_dashboard_required_form_value(params, key, label) do
    case flow_dashboard_optional_form_value(params, key) do
      {:ok, value} when is_binary(value) -> {:ok, value}
      _ -> {:error, "ERR #{label} is required"}
    end
  end

  defp flow_dashboard_optional_form_value(params, key) when is_map(params) do
    value =
      params
      |> Map.get(key, "")
      |> to_string()
      |> String.trim()

    {:ok, if(value == "", do: nil, else: value)}
  end

  defp flow_dashboard_form_positive_integer(params, key, default, max_value) do
    value = params |> Map.get(key, "") |> to_string() |> String.trim()

    case value do
      "" ->
        {:ok, default}

      _ ->
        case Integer.parse(value) do
          {parsed, ""} when parsed >= 1 and parsed <= max_value ->
            {:ok, parsed}

          {parsed, ""} when parsed > max_value ->
            {:error, "ERR #{key} exceeds maximum #{max_value}"}

          _ ->
            {:error, "ERR #{key} must be a positive integer"}
        end
    end
  end
end
