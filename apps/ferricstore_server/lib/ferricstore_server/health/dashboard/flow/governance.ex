defmodule FerricstoreServer.Health.Dashboard.Flow.Governance do
  @moduledoc false

  alias Ferricstore.Flow.Query.Builder
  alias FerricstoreServer.Health.QueryDecoder

  import FerricstoreServer.Health.Dashboard.Flow.Calls,
    only: [
      bounded_dashboard_call: 3,
      flow_dashboard_flow_query: 2,
      flow_dashboard_list_fetch_timeout_ms: 0
    ]

  @default_limit 100
  @max_limit 100
  @state_meta_idle "Enter partition, workflow type, metadata state, key, and value"

  def opts_from_query(query) when is_binary(query) do
    params = QueryDecoder.decode(query)

    [
      limit: normalize_limit(Map.get(params, "limit")),
      scope: normalize_text(Map.get(params, "scope")),
      status: normalize_status(Map.get(params, "approval_status") || Map.get(params, "status")),
      flow_id: normalize_text(Map.get(params, "flow_id")),
      circuit_status: normalize_circuit_status(Map.get(params, "circuit_status")),
      meta_type: normalize_text(Map.get(params, "meta_type")),
      meta_state: normalize_text(Map.get(params, "meta_state")),
      meta_key: normalize_text(Map.get(params, "meta_key")),
      meta_value: normalize_text(Map.get(params, "meta_value")),
      meta_value_type: normalize_meta_value_type(Map.get(params, "meta_value_type")),
      meta_partition_key: normalize_text(Map.get(params, "meta_partition_key")),
      flash: flash_from_params(params)
    ]
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
  end

  def opts_from_query(_query), do: [limit: @default_limit]

  def collect_page(opts \\ []) when is_list(opts) do
    limit = normalize_limit(Keyword.get(opts, :limit))

    filters = %{
      limit: limit,
      scope: Keyword.get(opts, :scope),
      status: Keyword.get(opts, :status),
      flow_id: Keyword.get(opts, :flow_id),
      circuit_status: Keyword.get(opts, :circuit_status),
      meta_type: Keyword.get(opts, :meta_type),
      meta_state: Keyword.get(opts, :meta_state),
      meta_key: Keyword.get(opts, :meta_key),
      meta_value: Keyword.get(opts, :meta_value),
      meta_value_type: normalize_meta_value_type(Keyword.get(opts, :meta_value_type)),
      meta_partition_key: Keyword.get(opts, :meta_partition_key)
    }

    state_meta_result = collect_state_meta_result(filters)
    overview_opts = overview_opts(opts, limit)

    case FerricStore.flow_governance_overview(overview_opts) do
      {:ok, overview} ->
        overview
        |> Map.put(:filters, filters)
        |> Map.put(:state_meta_result, state_meta_result)
        |> Map.put(:flash, Keyword.get(opts, :flash))

      {:error, reason} ->
        %{
          approvals: [],
          budgets: [],
          limits: [],
          circuits: [],
          counts: %{
            approvals: 0,
            pending_approvals: 0,
            budgets: 0,
            limits: 0,
            circuits: 0,
            open_circuits: 0,
            half_open_circuits: 0
          },
          filters: filters,
          state_meta_result: state_meta_result,
          flash: Keyword.get(opts, :flash),
          error: reason
        }
    end
  end

  @spec apply_form(map()) :: {:ok, binary()} | {:error, binary()}
  def apply_form(params), do: apply_form(params, approver: "dashboard")

  @spec apply_form(map(), keyword()) :: {:ok, binary()} | {:error, binary()}
  def apply_form(params, opts) when is_map(params) and is_list(opts) do
    scope = normalize_text(Map.get(params, "scope"))

    case {Map.get(params, "action"), scope} do
      {"approve_approval", _scope} ->
        apply_approval_decision(params, :approve, opts)

      {"reject_approval", _scope} ->
        apply_approval_decision(params, :reject, opts)

      {"open_circuit", nil} ->
        {:error, "ERR circuit scope is required"}

      {"open_circuit", scope} ->
        opts = [
          now_ms: System.system_time(:millisecond),
          open_ms: positive_integer(params, "open_ms", 30_000),
          failure_threshold: positive_integer(params, "failure_threshold", 3)
        ]

        case FerricStore.flow_circuit_open(scope, opts) do
          {:ok, circuit} ->
            {:ok, "opened circuit #{Map.get(circuit, :scope, scope)}"}

          {:error, reason} ->
            {:error, reason}
        end

      {"close_circuit", nil} ->
        {:error, "ERR circuit scope is required"}

      {"close_circuit", scope} ->
        case FerricStore.flow_circuit_close(scope, now_ms: System.system_time(:millisecond)) do
          {:ok, circuit} ->
            {:ok, "closed circuit #{Map.get(circuit, :scope, scope)}"}

          {:error, reason} ->
            {:error, reason}
        end

      {_action, _scope} ->
        {:error, "ERR unsupported governance action"}
    end
  end

  def apply_form(_params, _opts), do: {:error, "ERR governance form must be a map"}

  @spec form_command(map()) :: binary()
  def form_command(%{"action" => "close_circuit"}), do: "FLOW.CIRCUIT.CLOSE"
  def form_command(%{"action" => "open_circuit"}), do: "FLOW.CIRCUIT.OPEN"
  def form_command(%{"action" => "approve_approval"}), do: "FLOW.APPROVAL.APPROVE"
  def form_command(%{"action" => "reject_approval"}), do: "FLOW.APPROVAL.REJECT"
  def form_command(_params), do: "FLOW.GOVERNANCE.OVERVIEW"

  @spec redirect_location(map(), {:ok, binary()} | {:error, binary()}) :: binary()
  def redirect_location(params, result) when is_map(params) do
    filters =
      params
      |> Map.take(["scope", "approval_status", "flow_id", "circuit_status", "limit"])
      |> Enum.reject(fn {_key, value} -> value in [nil, ""] end)
      |> Map.new()

    result_params =
      case result do
        {:ok, message} -> %{"status" => "ok", "message" => message}
        {:error, reason} -> %{"status" => "error", "message" => reason}
      end

    "/dashboard/flow/governance?" <> URI.encode_query(Map.merge(filters, result_params))
  end

  defp apply_approval_decision(params, action, opts) do
    id = normalize_text(Map.get(params, "approval_id"))
    approver = normalize_text(Keyword.get(opts, :approver))

    with true <- is_binary(id) or {:error, "ERR approval id is required"},
         true <- is_binary(approver) or {:error, "ERR approval actor is required"},
         :ok <- validate_approval_confirmation(params),
         {:ok, approval} <- validate_expected_approval(id, params),
         decision_opts =
           [approver: approver, now_ms: System.system_time(:millisecond)]
           |> maybe_put_opt(:reason, normalize_text(Map.get(params, "decision_reason"))),
         result <- apply_approval_api(action, id, decision_opts) do
      case result do
        {:ok, _decided} -> {:ok, "#{approval_decision_label(action)} approval #{approval.id}"}
        {:error, reason} -> {:error, governance_error_message(reason)}
      end
    else
      {:error, reason} -> {:error, governance_error_message(reason)}
    end
  end

  defp validate_approval_confirmation(%{"confirm_action" => "true"}), do: :ok

  defp validate_approval_confirmation(_params),
    do: {:error, "approval decision confirmation is required"}

  defp validate_expected_approval(id, params) do
    with {:ok, expected_requested_at_ms} <-
           parse_non_negative_integer(Map.get(params, "expected_requested_at_ms")),
         "pending" <- normalize_text(Map.get(params, "expected_status")),
         expected_scope when is_binary(expected_scope) <-
           normalize_text(Map.get(params, "approval_scope")),
         {:ok, %{} = approval} <- FerricStore.flow_approval_get(id),
         :pending <- Map.get(approval, :status),
         ^expected_requested_at_ms <- Map.get(approval, :requested_at_ms),
         ^expected_scope <- Map.get(approval, :scope) do
      {:ok, approval}
    else
      {:ok, nil} -> {:error, "approval changed or was deleted; refresh before retrying"}
      {:error, reason} -> {:error, reason}
      _other -> {:error, "approval changed; refresh before retrying"}
    end
  end

  defp parse_non_negative_integer(value) when is_binary(value) do
    case value |> String.trim() |> Integer.parse() do
      {parsed, ""} when parsed >= 0 -> {:ok, parsed}
      _other -> {:error, "approval request timestamp is required; refresh before retrying"}
    end
  end

  defp parse_non_negative_integer(_value),
    do: {:error, "approval request timestamp is required; refresh before retrying"}

  defp apply_approval_api(:approve, id, opts), do: FerricStore.flow_approval_approve(id, opts)
  defp apply_approval_api(:reject, id, opts), do: FerricStore.flow_approval_reject(id, opts)
  defp approval_decision_label(:approve), do: "approved"
  defp approval_decision_label(:reject), do: "rejected"

  defp governance_error_message(reason) when is_binary(reason), do: reason

  defp governance_error_message(%{message: message}) when is_binary(message), do: message
  defp governance_error_message(reason), do: inspect(reason)

  defp maybe_put_opt(opts, _key, nil), do: opts
  defp maybe_put_opt(opts, key, value), do: Keyword.put(opts, key, value)

  defp normalize_limit(value) when is_integer(value), do: value |> max(1) |> min(@max_limit)

  defp normalize_limit(value) when is_binary(value) do
    case Integer.parse(String.trim(value)) do
      {parsed, ""} when parsed > 0 -> normalize_limit(parsed)
      _other -> @default_limit
    end
  end

  defp normalize_limit(_value), do: @default_limit

  defp normalize_text(value) when is_binary(value) do
    value = String.trim(value)
    if value == "", do: nil, else: value
  end

  defp normalize_text(_value), do: nil

  defp normalize_status(value) when value in ["pending", "approved", "rejected", "expired"],
    do: value

  defp normalize_status(_value), do: nil

  defp normalize_circuit_status(value) when value in ["open", "half_open", "closed"], do: value
  defp normalize_circuit_status(_value), do: nil

  defp normalize_meta_value_type(value) when value in ["string", "integer", "float", "boolean"],
    do: value

  defp normalize_meta_value_type(_value), do: "string"

  defp overview_opts(opts, limit) do
    opts
    |> Keyword.take([:scope, :status, :flow_id, :circuit_status, :partition_key])
    |> Keyword.put(:limit, limit)
  end

  defp collect_state_meta_result(filters) do
    case state_meta_search_opts(filters) do
      {:idle, message} ->
        %{status: :idle, command: "FLOW.QUERY", rows: [], message: message}

      {:error, reason} ->
        %{status: :error, command: "FLOW.QUERY", rows: [], message: inspect(reason)}

      {:ok, %{query: query, params: params}} ->
        case bounded_dashboard_call(
               fn -> flow_dashboard_flow_query(query, params) end,
               flow_dashboard_list_fetch_timeout_ms(),
               :governance_state_meta
             ) do
          {:ok, {:ok, rows}} when is_list(rows) ->
            query_result(rows)

          {:ok, {:ok, %{records: rows}}} when is_list(rows) ->
            query_result(rows)

          {:ok, {:error, reason}} ->
            %{status: :error, command: "FLOW.QUERY", rows: [], message: inspect(reason)}

          {:error, :timeout} ->
            %{status: :timeout, command: "FLOW.QUERY", rows: [], message: "query timed out"}

          {:error, reason} ->
            %{status: :error, command: "FLOW.QUERY", rows: [], message: inspect(reason)}

          _other ->
            %{
              status: :error,
              command: "FLOW.QUERY",
              rows: [],
              message: "unexpected query result"
            }
        end
    end
  end

  defp state_meta_search_opts(filters) when is_map(filters) do
    with {:ok, partition_key} <- required_filter(filters, :meta_partition_key),
         {:ok, type} <- required_filter(filters, :meta_type),
         {:ok, state} <- required_filter(filters, :meta_state),
         {:ok, key} <- required_filter(filters, :meta_key),
         {:ok, raw_value} <- required_filter(filters, :meta_value),
         {:ok, value} <- parse_meta_value(raw_value, Map.get(filters, :meta_value_type, "string")) do
      Builder.build(:search, %{
        partition_key: partition_key,
        type: type,
        state_meta: {state, key, value},
        limit: Map.get(filters, :limit, @default_limit)
      })
    else
      {:missing, _key} -> {:idle, @state_meta_idle}
      {:error, _reason} = error -> error
    end
  end

  defp required_filter(filters, key) do
    case Map.get(filters, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:missing, key}
    end
  end

  defp parse_meta_value(value, "string"), do: {:ok, value}

  defp parse_meta_value(value, "integer") do
    case Integer.parse(value) do
      {parsed, ""} -> {:ok, parsed}
      _ -> {:error, "ERR state_meta value must be an integer"}
    end
  end

  defp parse_meta_value(value, "float") do
    case Float.parse(value) do
      {parsed, ""} -> {:ok, parsed}
      _ -> {:error, "ERR state_meta value must be a float"}
    end
  end

  defp parse_meta_value("true", "boolean"), do: {:ok, true}
  defp parse_meta_value("false", "boolean"), do: {:ok, false}

  defp parse_meta_value(_value, "boolean"),
    do: {:error, "ERR state_meta value must be true or false"}

  defp parse_meta_value(value, _type), do: {:ok, value}

  defp query_result(rows),
    do: %{status: :ok, command: "FLOW.QUERY", rows: rows, message: "#{length(rows)} row(s)"}

  defp positive_integer(params, key, default) do
    case params |> Map.get(key, "") |> to_string() |> String.trim() |> Integer.parse() do
      {value, ""} when value > 0 -> value
      _other -> default
    end
  end

  defp flash_from_params(%{"status" => "ok", "message" => message}),
    do: %{kind: :ok, message: message}

  defp flash_from_params(%{"status" => "error", "message" => message}),
    do: %{kind: :error, message: message}

  defp flash_from_params(_params), do: nil
end
