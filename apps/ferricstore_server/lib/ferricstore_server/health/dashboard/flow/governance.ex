defmodule FerricstoreServer.Health.Dashboard.Flow.Governance do
  @moduledoc false

  @default_limit 100
  @max_limit 500

  def opts_from_query(query) when is_binary(query) do
    params = URI.decode_query(query)

    [
      limit: normalize_limit(Map.get(params, "limit")),
      scope: normalize_text(Map.get(params, "scope")),
      status: normalize_status(Map.get(params, "status")),
      flow_id: normalize_text(Map.get(params, "flow_id")),
      circuit_status: normalize_circuit_status(Map.get(params, "circuit_status")),
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
      circuit_status: Keyword.get(opts, :circuit_status)
    }

    case FerricStore.flow_governance_overview(Keyword.merge(opts, limit: limit)) do
      {:ok, overview} ->
        overview
        |> Map.put(:filters, filters)
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
          flash: Keyword.get(opts, :flash),
          error: reason
        }
    end
  end

  @spec apply_form(map()) :: {:ok, binary()} | {:error, binary()}
  def apply_form(params) when is_map(params) do
    scope = normalize_text(Map.get(params, "scope"))

    case {Map.get(params, "action"), scope} do
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

  def apply_form(_params), do: {:error, "ERR governance form must be a map"}

  @spec form_command(map()) :: binary()
  def form_command(%{"action" => "close_circuit"}), do: "FLOW.CIRCUIT.CLOSE"
  def form_command(%{"action" => "open_circuit"}), do: "FLOW.CIRCUIT.OPEN"
  def form_command(_params), do: "FLOW.GOVERNANCE.OVERVIEW"

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

  defp normalize_status(value) when value in ["pending", "approved", "rejected"], do: value
  defp normalize_status(_value), do: nil

  defp normalize_circuit_status(value) when value in ["open", "half_open", "closed"], do: value
  defp normalize_circuit_status(_value), do: nil

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
