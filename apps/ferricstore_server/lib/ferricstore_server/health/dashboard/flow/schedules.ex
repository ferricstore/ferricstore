defmodule FerricstoreServer.Health.Dashboard.Flow.Schedules do
  @moduledoc false

  alias FerricstoreServer.Health.QueryDecoder

  @default_limit 100
  @max_limit 500
  @draft_fields ~w(id schedule_kind cron every_ms delay_ms target_type target_partition
                   overlap_policy timezone max_fires target_payload overwrite)

  @spec opts_from_query(binary()) :: keyword()
  def opts_from_query(query) when is_binary(query) do
    query |> QueryDecoder.decode() |> opts_from_params()
  end

  def opts_from_query(_query), do: []

  defp opts_from_params(params) do
    []
    |> put_opt(:state, normalize_state(Map.get(params, "state")))
    |> put_opt(:kind, normalize_kind(Map.get(params, "kind")))
    |> put_opt(:q, normalize_text(Map.get(params, "q")))
    |> put_opt(:limit, normalize_limit(Map.get(params, "limit")))
    |> put_opt(:flash, flash_from_params(params))
    |> Enum.reverse()
  end

  # A create-only principal need not have catalog read permission. Error rendering
  # reflects the bounded request draft without reading any schedules or policies.
  def create_error_page(params, reason) do
    %{
      draft: Map.take(params, @draft_fields),
      filters: params |> opts_from_params() |> filters_from_opts(),
      flash: %{kind: :error, message: error_message(reason)},
      catalog_loaded: false
    }
  end

  defp error_message(reason) when is_binary(reason), do: reason
  defp error_message(_reason), do: "Schedule could not be created. Review the form and retry."

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

      {"create", id} ->
        apply_create_schedule(id, params)

      {"fire", id} ->
        with :ok <- validate_destructive_confirmation(params),
             {:ok, opts} <- schedule_mutation_opts(params, now_ms) do
          apply_result(FerricStore.flow_schedule_fire(id, opts), "fired #{id}")
        end

      {"pause", id} ->
        with {:ok, opts} <- schedule_mutation_opts(params, now_ms) do
          apply_result(FerricStore.flow_schedule_pause(id, opts), "paused #{id}")
        end

      {"resume", id} ->
        with {:ok, opts} <- schedule_mutation_opts(params, now_ms) do
          apply_result(FerricStore.flow_schedule_resume(id, opts), "resumed #{id}")
        end

      {"delete", id} ->
        with :ok <- validate_destructive_confirmation(params),
             {:ok, opts} <- schedule_mutation_opts(params, now_ms) do
          apply_result(FerricStore.flow_schedule_delete(id, opts), "deleted #{id}")
        end

      _other ->
        {:error, "unsupported schedule action"}
    end
  end

  def apply_form(_params), do: {:error, "invalid schedule form"}

  @spec form_command(map()) :: binary()
  def form_command(params) when is_map(params) do
    case Map.get(params, "action") do
      "create" -> "FLOW.SCHEDULE.CREATE"
      "fire" -> "FLOW.SCHEDULE.FIRE"
      "pause" -> "FLOW.SCHEDULE.PAUSE"
      "resume" -> "FLOW.SCHEDULE.RESUME"
      "delete" -> "FLOW.SCHEDULE.DELETE"
      _other -> "FLOW.SCHEDULE.GET"
    end
  end

  def form_command(_params), do: "FLOW.SCHEDULE.GET"

  @spec redirect_location(map(), {:ok, binary()} | {:error, binary()}) :: binary()
  def redirect_location(params, result) when is_map(params) do
    filters =
      params
      |> Map.take(["state", "kind", "q", "limit"])
      |> Enum.reject(fn {_key, value} -> value in [nil, ""] end)
      |> Map.new()

    result_params =
      case result do
        {:ok, message} -> %{"status" => "ok", "message" => message}
        {:error, reason} -> %{"status" => "error", "message" => reason}
      end

    "/dashboard/flow/schedules?" <> URI.encode_query(Map.merge(filters, result_params))
  end

  defp apply_result(:ok, message), do: {:ok, message}
  defp apply_result({:ok, _value}, message), do: {:ok, message}
  defp apply_result({:error, reason}, _message), do: {:error, reason}

  defp validate_destructive_confirmation(%{"confirm_action" => "true"}), do: :ok

  defp validate_destructive_confirmation(_params),
    do: {:error, "schedule action confirmation is required"}

  defp schedule_mutation_opts(params, now_ms) do
    with {:ok, expected_version} <- parse_expected_version(Map.get(params, "expected_version")),
         expected_state when is_binary(expected_state) and expected_state != "" <-
           params |> Map.get("expected_state", "") |> String.trim() do
      {:ok,
       [
         expected_state: expected_state,
         expected_version: expected_version,
         now_ms: now_ms
       ]}
    else
      {:error, reason} -> {:error, reason}
      _other -> {:error, "schedule state is required; refresh before retrying"}
    end
  end

  defp parse_expected_version(value) when is_binary(value) do
    case value |> String.trim() |> Integer.parse() do
      {version, ""} when version >= 0 -> {:ok, version}
      _other -> {:error, "schedule version is required; refresh before retrying"}
    end
  end

  defp parse_expected_version(_value),
    do: {:error, "schedule version is required; refresh before retrying"}

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

  defp normalize_state(value) when is_binary(value),
    do: value |> String.trim() |> normalize_state_value()

  defp normalize_state(_value), do: :all

  defp normalize_state_value(value) when value in ["", "all"], do: :all

  defp normalize_state_value(value)
       when value in ~w(active paused running completed failed cancelled),
       do: value

  defp normalize_state_value(_value), do: :all

  defp normalize_kind(nil), do: nil

  defp normalize_kind(value) when is_binary(value),
    do: value |> String.trim() |> normalize_kind_value()

  defp normalize_kind(_value), do: nil

  defp normalize_kind_value(""), do: nil

  defp normalize_kind_value(value) when value in ~w(one_shot delay interval cron),
    do: String.to_existing_atom(value)

  defp normalize_kind_value(_value), do: nil

  defp normalize_text(nil), do: nil

  defp normalize_text(value) when is_binary(value) do
    case String.trim(value) do
      "" -> nil
      normalized -> normalized
    end
  end

  defp normalize_limit(value) when is_binary(value) do
    case value |> String.trim() |> Integer.parse() do
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

  defp apply_create_schedule(id, params) do
    schedule_kind = params |> Map.get("schedule_kind", "cron") |> String.trim()
    target_type = params |> Map.get("target_type", "") |> String.trim()
    target_partition = params |> Map.get("target_partition", "") |> String.trim()
    target_payload_raw = params |> Map.get("target_payload", "") |> String.trim()
    overlap_policy_raw = params |> Map.get("overlap_policy", "skip") |> String.trim()
    overwrite = (params |> Map.get("overwrite", "false") |> String.trim()) in ["true", "on", "1"]
    timezone = params |> Map.get("timezone", "Etc/UTC") |> String.trim()

    cond do
      target_type == "" ->
        {:error, "target workflow type is required"}

      true ->
        with {:ok, timing_opts} <- parse_timing_opts(schedule_kind, params),
             {:ok, payload} <- parse_payload_json(target_payload_raw),
             {:ok, overlap_policy} <-
               parse_overlap_policy(schedule_kind, overlap_policy_raw),
             {:ok, max_fires} <-
               parse_max_fires(schedule_kind, Map.get(params, "max_fires")) do
          target_opts =
            [type: target_type]
            |> maybe_put_kw(:partition_key, if(target_partition != "", do: target_partition))
            |> maybe_put_kw(:payload, payload)

          create_opts =
            timing_opts
            |> Keyword.put(:target, target_opts)
            |> Keyword.put(:overwrite, overwrite)
            |> maybe_put_kw(
              :timezone,
              if(schedule_kind == "cron" and timezone != "", do: timezone)
            )
            |> maybe_put_kw(
              :overlap_policy,
              overlap_policy
            )
            |> maybe_put_kw(:max_fires, max_fires)

          apply_result(
            FerricStore.flow_schedule_create(id, create_opts),
            "created schedule #{id}"
          )
        end
    end
  end

  defp parse_overlap_policy(kind, policy) when kind in ["cron", "interval"] do
    case policy do
      "allow" ->
        {:ok, :allow}

      "skip" ->
        {:ok, :skip}

      "queue_after_previous" ->
        {:ok, :queue_after_previous}

      "fail_schedule" ->
        {:ok, :fail_schedule}

      _other ->
        {:error,
         "overlap policy must be one of: allow, skip, queue_after_previous, fail_schedule"}
    end
  end

  defp parse_overlap_policy(_kind, _policy), do: {:ok, nil}

  defp parse_timing_opts("cron", params) do
    case params |> Map.get("cron", "") |> String.trim() do
      "" -> {:error, "cron expression is required"}
      cron -> {:ok, [cron: cron]}
    end
  end

  defp parse_timing_opts("interval", params) do
    case params |> Map.get("every_ms", "") |> String.trim() |> Integer.parse() do
      {ms, ""} when ms > 0 -> {:ok, [every_ms: ms]}
      _other -> {:error, "interval (every_ms) must be a positive integer in milliseconds"}
    end
  end

  defp parse_timing_opts("delay", params) do
    case params |> Map.get("delay_ms", "") |> String.trim() |> Integer.parse() do
      {ms, ""} when ms >= 0 -> {:ok, [delay_ms: ms]}
      _other -> {:error, "delay (delay_ms) must be a non-negative integer in milliseconds"}
    end
  end

  defp parse_timing_opts(_other, _params), do: {:error, "unsupported schedule kind"}

  defp parse_payload_json(""), do: {:ok, nil}

  defp parse_payload_json(raw) when is_binary(raw) do
    case Jason.decode(raw) do
      {:ok, decoded} -> {:ok, decoded}
      {:error, _reason} -> {:error, "target payload must be valid JSON"}
    end
  end

  defp parse_payload_json(_), do: {:ok, nil}

  defp parse_max_fires(_kind, value) when value in [nil, ""], do: {:ok, nil}

  defp parse_max_fires(kind, raw) when kind in ["cron", "interval"] and is_binary(raw) do
    case raw |> String.trim() |> Integer.parse() do
      {int, ""} when int > 0 -> {:ok, int}
      _other -> {:error, "max fires must be a positive integer"}
    end
  end

  defp parse_max_fires(_kind, raw) when is_binary(raw) do
    if String.trim(raw) == "",
      do: {:ok, nil},
      else: {:error, "max fires is only supported for recurring schedules"}
  end

  defp parse_max_fires(_kind, _raw), do: {:error, "max fires must be a positive integer"}

  defp maybe_put_kw(opts, _key, nil), do: opts
  defp maybe_put_kw(opts, key, value), do: Keyword.put(opts, key, value)
end
