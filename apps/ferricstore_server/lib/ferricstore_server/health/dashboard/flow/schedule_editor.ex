defmodule FerricstoreServer.Health.Dashboard.Flow.ScheduleEditor do
  @moduledoc false

  def editing?(params), do: Map.get(params, "editing") == "true"

  def draft(schedule) do
    target = Map.get(schedule, :target) || %{}

    with {:ok, payload} <- encode_payload(target),
         {:ok, initial} <- utc(schedule[:initial_run_at_ms]),
         {:ok, start_at} <- utc(schedule[:start_at_ms]),
         {:ok, end_at} <- utc(schedule[:end_at_ms]) do
      {:ok,
       %{
         "id" => schedule.id,
         "editing" => "true",
         "overwrite" => "true",
         "original_state" => schedule.state,
         "original_version" => to_string(schedule.version),
         "schedule_kind" => to_string(schedule.kind),
         "cron" => text(schedule, :cron),
         "every_ms" => text(schedule, :every_ms),
         "delay_ms" => text(schedule, :delay_ms),
         "at_utc" => if(schedule.kind == :one_shot, do: initial, else: ""),
         "start_at_utc" => start_at,
         "end_at_utc" => end_at,
         "target_type" => text(target, :type),
         "target_partition" => text(target, :partition_key),
         "target_payload" => payload,
         "overlap_policy" => text(schedule, :overlap_policy),
         "timezone" => text(schedule, :timezone),
         "max_fires" => text(schedule, :max_fires)
       }}
    end
  end

  def check_original(params, current) do
    if editing?(params) and
         (is_nil(current) or Map.get(params, "original_state") != current.state or
            Map.get(params, "original_version") != to_string(current.version)) do
      {:error,
       "Schedule changed since this editor was loaded. Discard changes and reload before editing again."}
    else
      :ok
    end
  end

  def preserve_options(options, params, current) do
    if editing?(params) and current do
      submitted = Keyword.fetch!(options, :target) |> Map.new()

      target =
        current.target |> Map.drop([:type, :partition_key, :payload]) |> Map.merge(submitted)

      # A definition loaded from an API may contain non-JSON values. Identical JSON
      # in the editor means untouched payload, so preserve its original term.
      submitted_payload = Map.get(params, "target_payload", "")

      target =
        case encode_payload(current.target) do
          {:ok, ^submitted_payload} ->
            if Map.has_key?(current.target, :payload),
              do: Map.put(target, :payload, current.target.payload),
              else: target

          _ ->
            target
        end

      options = Keyword.put(options, :target, Map.to_list(target))

      if Map.get(params, "schedule_kind") == to_string(current.kind) do
        keys =
          if Keyword.get(options, :overlap_policy) == :queue_after_previous,
            do: [:catchup_policy, :overlap_retry_ms],
            else: [:catchup_policy]

        options =
          Enum.reduce(keys, options, fn key, result ->
            case Map.get(current, key) do
              nil -> result
              value -> Keyword.put(result, key, value)
            end
          end)

        if current.kind in [:interval, :cron] and is_nil(current[:start_at_ms]) and
             not Keyword.has_key?(options, :start_at_ms) and
             is_integer(current[:initial_run_at_ms]) do
          Keyword.put(options, :at_ms, current.initial_run_at_ms)
        else
          options
        end
      else
        options
      end
    else
      options
    end
  end

  defp encode_payload(target) do
    case Map.fetch(target, :payload) do
      :error ->
        {:ok, ""}

      {:ok, value} ->
        case Jason.encode(value) do
          {:ok, json} ->
            {:ok, json}

          {:error, _} ->
            {:error,
             "This schedule payload cannot be represented as JSON. Edit it with FLOW.SCHEDULE.CREATE to preserve its definition."}
        end
    end
  end

  defp utc(nil), do: {:ok, ""}

  defp utc(ms) when is_integer(ms) and ms >= 0 do
    case DateTime.from_unix(ms, :millisecond) do
      {:ok, date} when date.year <= 9999 ->
        {:ok, date |> DateTime.to_naive() |> NaiveDateTime.to_iso8601()}

      _ ->
        calendar_error()
    end
  end

  defp utc(_value), do: calendar_error()

  defp calendar_error do
    {:error,
     "Schedule timestamp is outside the dashboard calendar range (1970-9999). Edit with FLOW.SCHEDULE.CREATE; the stored definition has not changed."}
  end

  defp text(map, key), do: to_string(Map.get(map, key) || "")
end
