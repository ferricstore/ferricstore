defmodule FerricstoreServer.Health.Dashboard.Flow.Schedules do
  @moduledoc false

  @default_limit 100
  @max_limit 500

  @spec opts_from_query(binary()) :: keyword()
  def opts_from_query(query) when is_binary(query) do
    params = URI.decode_query(query)

    []
    |> put_opt(:state, normalize_state(Map.get(params, "state")))
    |> put_opt(:kind, normalize_kind(Map.get(params, "kind")))
    |> put_opt(:q, normalize_text(Map.get(params, "q")))
    |> put_opt(:limit, normalize_limit(Map.get(params, "limit")))
    |> put_opt(:flash, flash_from_params(params))
    |> Enum.reverse()
  end

  def opts_from_query(_query), do: []

  @spec collect_page(keyword()) :: map()
  def collect_page(opts \\ []) when is_list(opts) do
    filters = filters_from_opts(opts)

    list_opts =
      [
        state: filters.state,
        count: filters.limit
      ]
      |> put_opt(:kind, filters.kind)

    schedules =
      case FerricStore.flow_schedule_list(list_opts) do
        {:ok, rows} -> filter_schedules(rows, filters)
        {:error, reason} -> [%{id: "ERR", state: "error", error: reason}]
      end

    %{
      schedules: schedules,
      failed_schedules: Enum.filter(schedules, &(Map.get(&1, :state) == "failed")),
      summary: schedule_summary(schedules),
      filters: filters,
      flash: Keyword.get(opts, :flash),
      generated_at_ms: System.system_time(:millisecond)
    }
  end

  @spec apply_form(map()) :: {:ok, binary()} | {:error, binary()}
  def apply_form(params) when is_map(params) do
    id = params |> Map.get("id", "") |> String.trim()
    action = params |> Map.get("action", "") |> String.trim()
    now_ms = System.system_time(:millisecond)

    case {action, id} do
      {_action, ""} ->
        {:error, "schedule id is required"}

      {"fire", id} ->
        apply_result(FerricStore.flow_schedule_fire(id, now_ms: now_ms), "fired #{id}")

      {"pause", id} ->
        apply_result(FerricStore.flow_schedule_pause(id, now_ms: now_ms), "paused #{id}")

      {"resume", id} ->
        apply_result(FerricStore.flow_schedule_resume(id, now_ms: now_ms), "resumed #{id}")

      {"delete", id} ->
        apply_result(FerricStore.flow_schedule_delete(id, now_ms: now_ms), "deleted #{id}")

      _other ->
        {:error, "unsupported schedule action"}
    end
  end

  def apply_form(_params), do: {:error, "invalid schedule form"}

  @spec form_command(map()) :: binary()
  def form_command(params) when is_map(params) do
    case Map.get(params, "action") do
      "fire" -> "FLOW.SCHEDULE.FIRE"
      "pause" -> "FLOW.SCHEDULE.PAUSE"
      "resume" -> "FLOW.SCHEDULE.RESUME"
      "delete" -> "FLOW.SCHEDULE.DELETE"
      _other -> "FLOW.SCHEDULE.GET"
    end
  end

  def form_command(_params), do: "FLOW.SCHEDULE.GET"

  defp apply_result(:ok, message), do: {:ok, message}
  defp apply_result({:ok, _value}, message), do: {:ok, message}
  defp apply_result({:error, reason}, _message), do: {:error, reason}

  defp filters_from_opts(opts) do
    %{
      state: Keyword.get(opts, :state, :all),
      kind: Keyword.get(opts, :kind),
      q: Keyword.get(opts, :q),
      limit: Keyword.get(opts, :limit, @default_limit)
    }
  end

  defp filter_schedules(schedules, %{q: nil}), do: schedules

  defp filter_schedules(schedules, %{q: query}) do
    downcased = String.downcase(query)

    Enum.filter(schedules, fn schedule ->
      schedule
      |> Map.get(:id, "")
      |> to_string()
      |> String.downcase()
      |> String.contains?(downcased)
    end)
  end

  defp schedule_summary(schedules) do
    schedules
    |> Enum.frequencies_by(&Map.get(&1, :state, "unknown"))
    |> Map.put(:total, length(schedules))
  end

  defp normalize_state(nil), do: :all
  defp normalize_state(""), do: :all
  defp normalize_state("all"), do: :all

  defp normalize_state(value) when value in ~w(active paused running completed failed cancelled),
    do: value

  defp normalize_state(_value), do: :all

  defp normalize_kind(nil), do: nil
  defp normalize_kind(""), do: nil

  defp normalize_kind(value) when value in ~w(one_shot delay interval cron),
    do: String.to_existing_atom(value)

  defp normalize_kind(_value), do: nil

  defp normalize_text(nil), do: nil
  defp normalize_text(""), do: nil
  defp normalize_text(value) when is_binary(value), do: String.trim(value)

  defp normalize_limit(value) when is_binary(value) do
    case Integer.parse(value) do
      {limit, ""} when limit > 0 -> min(limit, @max_limit)
      _other -> @default_limit
    end
  end

  defp normalize_limit(_value), do: @default_limit

  defp put_opt(opts, _key, nil), do: opts
  defp put_opt(opts, key, value), do: [{key, value} | opts]

  defp flash_from_params(%{"status" => "ok", "message" => message}),
    do: %{kind: :ok, message: message}

  defp flash_from_params(%{"status" => "error", "message" => message}),
    do: %{kind: :error, message: message}

  defp flash_from_params(_params), do: nil
end
