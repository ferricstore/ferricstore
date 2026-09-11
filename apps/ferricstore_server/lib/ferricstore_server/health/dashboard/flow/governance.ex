defmodule FerricstoreServer.Health.Dashboard.Flow.Governance do
  @moduledoc false

  alias Ferricstore.Flow.Query.Builder
  alias Ferricstore.Flow.Governance.CircuitStore
  alias FerricstoreServer.Health.QueryDecoder
  alias FerricstoreServer.Health.Dashboard.Flow.QueryResult
  alias FerricstoreServer.Health.Dashboard.{Access, Flow.ManagementActions}

  import FerricstoreServer.Health.Dashboard.Flow.Calls,
    only: [
      bounded_dashboard_call: 3,
      flow_dashboard_flow_query: 2,
      flow_dashboard_list_fetch_timeout_ms: 0
    ]

  @default_limit 100
  @max_limit 100
  @state_meta_idle "Enter partition, workflow type, metadata state, key, and value"
  @draft_fields ~w(action scope approval_id approval_scope failure_threshold open_ms decision_reason circuit_review_scope)
  @filter_fields [
    {"scope", :scope},
    {"approval_status", :status},
    {"flow_id", :flow_id},
    {"circuit_status", :circuit_status},
    {"limit", :limit},
    {"meta_type", :meta_type},
    {"meta_state", :meta_state},
    {"meta_key", :meta_key},
    {"meta_value", :meta_value},
    {"meta_value_type", :meta_value_type},
    {"meta_partition_key", :meta_partition_key},
    {"meta_cursor", :meta_cursor}
  ]

  def filter_params(filters) do
    Enum.reduce(@filter_fields, %{}, fn {name, key}, params ->
      value = Map.get(filters, key, Map.get(filters, name))

      if is_nil(value) or (value == "" and name != "meta_value"),
        do: params,
        else: Map.put(params, name, value)
    end)
  end

  def metadata_path(filters, cursor) do
    params = filters |> Map.put(:meta_cursor, cursor) |> filter_params()
    "/dashboard/flow/governance?" <> URI.encode_query(params)
  end

  def opts_from_query(query) when is_binary(query) do
    params = QueryDecoder.decode(query)

    [
      limit: normalize_limit(Map.get(params, "limit")),
      scope: normalize_text(Map.get(params, "scope")),
      status: normalize_status(Map.get(params, "approval_status") || Map.get(params, "status")),
      flow_id: normalize_text(Map.get(params, "flow_id")),
      circuit_status: normalize_circuit_status(Map.get(params, "circuit_status")),
      circuit_review_scope: normalize_text(Map.get(params, "circuit_review_scope")),
      meta_type: normalize_text(Map.get(params, "meta_type")),
      meta_state: normalize_text(Map.get(params, "meta_state")),
      meta_key: normalize_text(Map.get(params, "meta_key")),
      meta_value: Map.get(params, "meta_value"),
      meta_cursor: Map.get(params, "meta_cursor"),
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
      meta_cursor: Keyword.get(opts, :meta_cursor),
      meta_value_type: normalize_meta_value_type(Keyword.get(opts, :meta_value_type)),
      meta_partition_key: Keyword.get(opts, :meta_partition_key)
    }

    username = Access.keyspace_acl_username(opts)
    capabilities = ManagementActions.governance(username, filters.scope)
    filters = Map.put(filters, :action_capabilities, capabilities)

    filters =
      Map.put(
        filters,
        :circuit_review,
        collect_circuit_review(Keyword.get(opts, :circuit_review_scope), username)
      )

    state_meta_result = collect_state_meta_result(filters)
    overview_opts = overview_opts(opts, limit)

    case FerricStore.flow_governance_overview(overview_opts) do
      {:ok, overview} ->
        overview
        |> Map.put(:action_capabilities, capabilities)
        |> Map.update(
          :circuits,
          [],
          &Enum.map(&1, fn row ->
            Map.put(
              row,
              :action_capabilities,
              ManagementActions.governance(username, Map.get(row, :scope))
            )
          end)
        )
        |> Map.update(
          :approvals,
          [],
          &Enum.map(&1, fn row ->
            Map.put(
              row,
              :action_capabilities,
              ManagementActions.governance(username, Map.get(row, :scope))
            )
          end)
        )
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
          action_capabilities: capabilities,
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
        with {:ok, review} <- circuit_confirmation(params),
             {:ok, open_ms} <- circuit_positive_integer(params, "open_ms", 30_000),
             {:ok, threshold} <- circuit_positive_integer(params, "failure_threshold", 3),
             {:ok, circuit} <-
               FerricStore.flow_circuit_open(scope,
                 expected_review: review,
                 now_ms: System.system_time(:millisecond),
                 open_ms: open_ms,
                 failure_threshold: threshold
               ) do
          {:ok, "opened circuit #{Map.get(circuit, :scope, scope)}"}
        end

      {"close_circuit", nil} ->
        {:error, "ERR circuit scope is required"}

      {"close_circuit", scope} ->
        with {:ok, review} <- circuit_confirmation(params),
             {:ok, circuit} <-
               FerricStore.flow_circuit_close(scope,
                 expected_review: review,
                 now_ms: System.system_time(:millisecond)
               ) do
          {:ok, "closed circuit #{Map.get(circuit, :scope, scope)}"}
        end

      {_action, _scope} ->
        {:error, "ERR unsupported governance action"}
    end
  end

  def apply_form(_params, _opts), do: {:error, "ERR governance form must be a map"}

  def error_page(params, reason) do
    draft =
      params
      |> Map.take(@draft_fields)
      |> Enum.filter(fn {_key, value} -> is_binary(value) end)
      |> Map.new()

    # Recover submitted input without reading overview, metadata, or target records.
    %{
      governance_form_only?: true,
      action_draft: draft,
      filters:
        params
        |> Map.put("scope", Map.get(params, "return_scope", Map.get(params, "scope")))
        |> filter_params(),
      review: nil,
      error: reason
    }
  end

  def review_requirement(%{"action" => action} = params)
      when action in ["open_circuit", "close_circuit"],
      do: {"FLOW.CIRCUIT.GET", key: {Map.get(params, "scope", ""), :read}}

  def review_requirement(%{"action" => action})
      when action in ["approve_approval", "reject_approval"],
      do: {"FLOW.APPROVAL.GET", key: {"*", :read}}

  def review_requirement(_params), do: {"FLOW.GOVERNANCE.OVERVIEW", key: {"*", :read}}

  def review_page(params, opts \\ []) do
    page = error_page(params, nil)
    username = Access.keyspace_acl_username(opts)

    if ManagementActions.allowed?(username, review_requirement(params)) do
      case fresh_review(page.action_draft, username) do
        {:ok, review} -> {:ok, Map.put(page, :review, review)}
        {:error, reason} -> {:error, Map.put(page, :error, reason)}
      end
    else
      {:error, Map.put(page, :error, "Current target review is not authorized.")}
    end
  end

  defp fresh_review(%{"action" => action, "scope" => scope}, username)
       when action in ["open_circuit", "close_circuit"] and scope != "" do
    case collect_circuit_review(scope, username) do
      %{status: :ok, circuit: nil} when action == "close_circuit" ->
        {:error, "Circuit is no longer configured in this exact scope. No close was submitted."}

      %{status: :ok} = review ->
        {:ok, review}

      %{message: message} ->
        {:error, message}

      _ ->
        {:error, "Enter the exact circuit scope before reviewing."}
    end
  end

  defp fresh_review(
         %{"action" => action, "approval_id" => id, "approval_scope" => scope},
         username
       )
       when action in ["approve_approval", "reject_approval"] and id != "" and scope != "" do
    # Approval IDs are globally keyed, matching the native GET command's read scope.
    case bounded_dashboard_call(
           fn -> FerricStore.flow_approval_get(id) end,
           flow_dashboard_list_fetch_timeout_ms(),
           :approval_review
         ) do
      {:ok, {:ok, %{scope: ^scope, status: :pending} = approval}} ->
        {:ok,
         %{
           approval: approval,
           action_capabilities: ManagementActions.governance(username, scope)
         }}

      {:ok, {:ok, _}} ->
        {:error, "Approval is no longer pending in this exact scope. No decision was submitted."}

      _ ->
        {:error, "Approval review unavailable. Retry the exact target review."}
    end
  end

  defp fresh_review(_draft, _username),
    do: {:error, "Choose a valid governance target to review."}

  defp collect_circuit_review(scope, username) when is_binary(scope) and scope != "" do
    if ManagementActions.allowed?(username, {"FLOW.CIRCUIT.GET", key: {scope, :read}}) do
      case bounded_dashboard_call(
             fn -> FerricStore.flow_circuit_get(scope) end,
             flow_dashboard_list_fetch_timeout_ms(),
             :circuit_review
           ) do
        {:ok, {:ok, circuit}} ->
          %{
            status: :ok,
            scope: scope,
            circuit: circuit,
            fingerprint:
              if(is_nil(circuit),
                do: CircuitStore.missing_review_fingerprint(scope),
                else: CircuitStore.review_fingerprint(circuit)
              ),
            action_capabilities: ManagementActions.governance(username, scope)
          }

        _ ->
          %{
            status: :error,
            scope: scope,
            message: "Circuit review unavailable. Retry the exact scope lookup."
          }
      end
    else
      %{
        status: :error,
        scope: scope,
        message: "Circuit review requires FLOW.CIRCUIT.GET and read access to this scope."
      }
    end
  end

  defp collect_circuit_review(_scope, _username), do: nil

  defp circuit_confirmation(%{"confirm_action" => "true", "expected_review" => review})
       when is_binary(review) and byte_size(review) == 43,
       do: {:ok, review}

  defp circuit_confirmation(_params),
    do:
      {:error,
       "ERR circuit review and confirmation are required; review the circuit before retrying"}

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
      |> Map.put("scope", Map.get(params, "return_scope", Map.get(params, "scope")))
      |> filter_params()

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
            QueryResult.success("FLOW.QUERY", %{records: rows})

          {:ok, {:ok, %{records: rows} = response}} when is_list(rows) ->
            QueryResult.success("FLOW.QUERY", response)

          {:ok, {:ok, %{"records" => rows} = response}} when is_list(rows) ->
            QueryResult.success("FLOW.QUERY", response)

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
         raw_value when is_binary(raw_value) <- Map.get(filters, :meta_value),
         {:ok, value} <- parse_meta_value(raw_value, Map.get(filters, :meta_value_type, "string")) do
      Builder.build(:search, %{
        partition_key: partition_key,
        type: type,
        state_meta: {state, key, value},
        limit: Map.get(filters, :limit, @default_limit),
        cursor: Map.get(filters, :meta_cursor)
      })
    else
      {:missing, _key} -> {:idle, @state_meta_idle}
      {:error, _reason} = error -> error
      nil -> {:idle, @state_meta_idle}
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
    case Integer.parse(String.trim(value)) do
      {parsed, ""} -> {:ok, parsed}
      _ -> {:error, "ERR state_meta value must be an integer"}
    end
  end

  defp parse_meta_value(value, "float") do
    case Float.parse(String.trim(value)) do
      {parsed, ""} -> {:ok, parsed}
      _ -> {:error, "ERR state_meta value must be a float"}
    end
  end

  defp parse_meta_value(value, "boolean") do
    case String.trim(value) do
      "true" -> {:ok, true}
      "false" -> {:ok, false}
      _ -> {:error, "ERR state_meta value must be true or false"}
    end
  end

  defp parse_meta_value(value, _type), do: {:ok, value}

  defp circuit_positive_integer(params, key, default) do
    case Map.get(params, key) do
      nil ->
        {:ok, default}

      value when is_binary(value) ->
        case Integer.parse(String.trim(value)) do
          {parsed, ""} when parsed > 0 ->
            {:ok, parsed}

          _ ->
            {:error, "ERR #{key} must be a positive integer; review the circuit before retrying"}
        end

      _ ->
        {:error, "ERR #{key} must be a positive integer; review the circuit before retrying"}
    end
  end

  defp flash_from_params(%{"status" => "ok", "message" => message}),
    do: %{kind: :ok, message: message}

  defp flash_from_params(%{"status" => "error", "message" => message}),
    do: %{kind: :error, message: message}

  defp flash_from_params(_params), do: nil
end
