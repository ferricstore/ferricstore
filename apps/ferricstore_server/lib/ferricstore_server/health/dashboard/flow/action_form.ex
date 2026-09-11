defmodule FerricstoreServer.Health.Dashboard.Flow.ActionForm do
  @moduledoc false

  alias FerricstoreServer.Health.Dashboard.Flow.Sample
  alias FerricstoreServer.Health.Endpoint.FlowPaths

  @fields ~w(to_event run_at_ms schedule_mode run_at_utc expect_state expected_version reviewed_type reviewed_state reviewed_version signal transition_to idempotency_key if_state)
  @scope_fields ~w(history_count history_before history_after history_event)

  def return_params(params) do
    params
    |> Map.take(@scope_fields)
    |> Enum.flat_map(fn
      {key, value} when is_binary(value) -> [{key, value}]
      {key, value} when is_integer(value) -> [{key, to_string(value)}]
      _ -> []
    end)
    |> Map.new()
  end

  def error_page(id, action, params, reason) do
    partition = Sample.normalize_flow_partition_query(Map.get(params, "partition_key"))
    scope = return_params(params)

    # A mutation-only principal may recover its own draft without record/history reads.
    %{
      id: id,
      action: action,
      partition_key: partition,
      record: %{id: id, partition_key: partition},
      action_draft: string_fields(params, @fields),
      history_page: %{current_live_params: scope},
      review_url: FlowPaths.flow_detail_location(id, partition, scope),
      error: reason
    }
  end

  def value(data, field, default \\ ""),
    do: data |> Map.get(:action_draft, %{}) |> Map.get(field, default)

  def scope(%{history_page: %{current_live_params: scope}}) when is_map(scope),
    do: return_params(scope)

  def scope(_data), do: %{}

  def resolve_schedule(params, now_ms \\ System.system_time(:millisecond)) do
    case Map.get(params, "schedule_mode") do
      "keep" -> {:ok, nil}
      "now" -> {:ok, now_ms}
      "at" -> parse_utc_schedule(Map.get(params, "run_at_utc"))
      nil -> legacy_schedule(Map.get(params, "run_at_ms"))
      _ -> {:error, "ERR choose Keep event schedule, Run now, or UTC date and time"}
    end
  end

  defp parse_utc_schedule(value) when is_binary(value) do
    value = if byte_size(value) == 16, do: value <> ":00", else: value

    with true <- Regex.match?(~r/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?$/, value),
         {:ok, datetime, 0} <- DateTime.from_iso8601(value <> "Z"),
         ms when ms >= 0 <- DateTime.to_unix(datetime, :millisecond) do
      {:ok, ms}
    else
      _ -> {:error, "ERR enter a valid UTC date and time on or after 1970-01-01"}
    end
  end

  defp parse_utc_schedule(_value), do: {:error, "ERR enter a UTC date and time"}

  defp legacy_schedule(value) when value in [nil, ""], do: {:ok, nil}
  defp legacy_schedule(value) when is_integer(value) and value >= 0, do: {:ok, value}

  defp legacy_schedule(value) when is_binary(value) do
    case Integer.parse(String.trim(value)) do
      {ms, ""} when ms >= 0 -> {:ok, ms}
      _ -> {:error, "ERR run_at_ms must be a non-negative integer"}
    end
  end

  defp legacy_schedule(_value), do: {:error, "ERR run_at_ms must be a non-negative integer"}

  defp string_fields(params, fields) do
    params |> Map.take(fields) |> Enum.filter(fn {_, value} -> is_binary(value) end) |> Map.new()
  end
end
