defmodule FerricstoreServer.Health.Dashboard.Flow.Schedules do
  @moduledoc false

  alias FerricstoreServer.Health.QueryDecoder
  alias FerricstoreServer.Health.Dashboard.{Access, Flow.ManagementActions}
  alias Ferricstore.Flow.Schedule
  alias FerricstoreServer.Health.Dashboard.Flow.{DurationFields, ScheduleEditor}

  @default_limit 100
  @max_limit 500
  @draft_fields ~w(id schedule_kind cron every_ms delay_ms at_utc start_at_utc end_at_utc target_type target_partition
                   overlap_policy timezone max_fires target_payload overwrite editing original_state original_version
                   every_ms_unit delay_ms_unit)
  @review_fields ~w(expected_state expected_version reviewed_at_ms review_fingerprint)
  @review_lifetime_ms 5 * 60_000

  def replacement?(params),
    do: ScheduleEditor.editing?(params) or Map.get(params, "overwrite") in ["true", "on", "1"]

  def preview_form(params, now_ms \\ System.system_time(:millisecond)) do
    id = Map.get(params, "id", "")

    with {:ok, current} <- reviewed_current(params, id),
         :ok <- ScheduleEditor.check_original(params, current),
         {:ok, opts} <- create_options(params, current),
         {:ok, planned} <- Schedule.preview(id, Keyword.put(opts, :now_ms, now_ms)) do
      fields = %{
        "expected_state" => if(current, do: current.state, else: ""),
        "expected_version" => if(current, do: to_string(current.version), else: ""),
        "reviewed_at_ms" => to_string(now_ms)
      }

      fields = Map.put(fields, "review_fingerprint", review_fingerprint(params, fields, opts))

      {:ok,
       %{
         draft: Map.take(params, @draft_fields),
         filters: params |> form_filters(),
         review: %{current: current, planned: planned, fields: fields},
         catalog_loaded: false
       }}
    end
  end

  defp reviewed_current(params, id) do
    if replacement?(params) do
      case FerricStore.flow_schedule_get(id) do
        {:ok, nil} ->
          {:error, "Schedule does not exist. Clear replacement to create a new schedule."}

        result ->
          result
      end
    else
      {:ok, nil}
    end
  end

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
    |> put_opt(:id, Map.get(params, "id"))
    |> put_opt(:edit, Map.get(params, "edit") == "true")
    |> put_opt(:limit, normalize_limit(Map.get(params, "limit")))
    |> put_opt(:flash, flash_from_params(params))
    |> Enum.reverse()
  end

  # A create-only principal need not have catalog read permission. Error rendering
  # reflects the bounded request draft without reading any schedules or policies.
  def create_error_page(params, reason) do
    field_errors =
      case create_options_with_fields(params) do
        {:error, {field, ^reason}} -> %{field => reason}
        _ -> %{}
      end

    %{
      draft: Map.take(params, @draft_fields),
      field_errors: field_errors,
      filters: form_filters(params),
      flash: %{kind: :error, message: error_message(reason)},
      catalog_loaded: false
    }
  end

  defp error_message(reason) when is_binary(reason), do: reason
  defp error_message(_reason), do: "Schedule could not be created. Review the form and retry."

  @spec collect_page(keyword()) :: map()
  def collect_page(opts \\ []) when is_list(opts) do
    filters = filters_from_opts(opts)
    username = Access.keyspace_acl_username(opts)
    capabilities = ManagementActions.schedules(username)

    capabilities =
      Map.put(
        capabilities,
        :edit,
        capabilities.create and
          ManagementActions.allowed?(username, {"FLOW.SCHEDULE.GET", key: {"*", :read}})
      )

    catalog_limit = if filters.q, do: @max_limit, else: filters.limit

    list_opts =
      [
        state: filters.state,
        count: catalog_limit
      ]
      |> put_opt(:kind, filters.kind)

    result =
      if filters.id do
        case FerricStore.flow_schedule_get(filters.id) do
          {:ok, nil} -> {:ok, []}
          {:ok, row} -> {:ok, [row]}
          {:error, reason} -> {:error, reason}
        end
      else
        FerricStore.flow_schedule_list(list_opts)
      end

    {schedules, limited?} =
      case result do
        {:ok, rows} ->
          selected = if filters.id, do: rows, else: filter_schedules(rows, filters)

          {Enum.take(selected, filters.limit),
           is_nil(filters.id) and length(rows) >= catalog_limit}

        {:error, reason} ->
          {[%{id: "ERR", state: "error", error: reason}], false}
      end

    schedules = Enum.map(schedules, &Map.put(&1, :action_capabilities, capabilities))

    data = %{
      schedules: schedules,
      action_capabilities: capabilities,
      scan_limited?: limited?,
      failed_schedules: Enum.filter(schedules, &(Map.get(&1, :state) == "failed")),
      summary: schedule_summary(schedules),
      filters: filters,
      flash: Keyword.get(opts, :flash),
      generated_at_ms: System.system_time(:millisecond)
    }

    if Keyword.get(opts, :edit, false) and is_binary(filters.id) and capabilities.edit do
      case schedules do
        [%{error: _}] ->
          data

        [schedule] ->
          case ScheduleEditor.draft(schedule) do
            {:ok, draft} -> Map.merge(data, %{draft: draft, hydrated_edit: true})
            {:error, reason} -> Map.put(data, :flash, %{kind: :error, message: reason})
          end

        [] ->
          Map.put(data, :flash, %{
            kind: :error,
            message: "Schedule was not found. It may have been deleted."
          })
      end
    else
      data
    end
  end

  @spec apply_form(map()) :: {:ok, binary()} | {:error, binary()}
  def apply_form(params) when is_map(params) do
    id = Map.get(params, "id", "")
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

    filters =
      case {Map.get(params, "action"), result, Map.get(params, "return_id")} do
        {"delete", {:ok, _}, _} -> filters
        {_, _, id} when is_binary(id) and id != "" -> Map.put(filters, "id", id)
        _ -> filters
      end

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
      id:
        case Keyword.get(opts, :id) do
          id when is_binary(id) and id != "" -> id
          _ -> nil
        end,
      limit: Keyword.get(opts, :limit, @default_limit)
    }
  end

  defp form_filters(params) do
    params
    |> Map.put("id", Map.get(params, "return_id", ""))
    |> opts_from_params()
    |> filters_from_opts()
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
    with {:ok, current} <- reviewed_current(params, id),
         :ok <- ScheduleEditor.check_original(params, current),
         {:ok, create_opts} <- create_options(params, current),
         {:ok, review_opts} <- validate_create_review(params, create_opts) do
      apply_result(
        FerricStore.flow_schedule_create(id, Keyword.merge(create_opts, review_opts)),
        if(replacement?(params), do: "replaced schedule #{id}", else: "created schedule #{id}")
      )
    end
  end

  defp validate_create_review(params, options) do
    if replacement?(params) or Map.has_key?(params, "review_fingerprint") do
      fields = Map.take(params, @review_fields)

      with true <- not replacement?(params) or Map.get(params, "confirm_replace") == "true",
           fingerprint when is_binary(fingerprint) <- Map.get(fields, "review_fingerprint"),
           true <- fingerprint == review_fingerprint(params, fields, options),
           {reviewed_at, ""} <- Integer.parse(Map.get(fields, "reviewed_at_ms", "")),
           age = System.system_time(:millisecond) - reviewed_at,
           true <- age >= 0 and age <= @review_lifetime_ms do
        if replacement?(params) do
          schedule_mutation_opts(params, reviewed_at)
        else
          {:ok, [now_ms: reviewed_at]}
        end
      else
        _ ->
          {:error, "Schedule review is missing, changed, or expired. Review the schedule again."}
      end
    else
      {:ok, []}
    end
  end

  defp review_fingerprint(params, fields, options) do
    {Map.get(params, "id"), Enum.sort(options),
     Map.take(fields, ~w(expected_state expected_version reviewed_at_ms))}
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.url_encode64(padding: false)
  end

  defp create_options(params, current) do
    case create_options_with_fields(params) do
      {:error, {_field, reason}} -> {:error, reason}
      {:ok, options} -> {:ok, ScheduleEditor.preserve_options(options, params, current)}
    end
  end

  defp create_options_with_fields(params) do
    case DurationFields.normalize(params, ~w(every_ms delay_ms)) do
      {:ok, normalized} -> create_normalized_options(normalized)
      error -> error
    end
  end

  defp create_normalized_options(params) do
    schedule_kind = params |> Map.get("schedule_kind", "cron") |> String.trim()
    target_type = Map.get(params, "target_type", "")
    target_partition = Map.get(params, "target_partition", "")
    target_payload_raw = params |> Map.get("target_payload", "") |> String.trim()
    overlap_policy_raw = params |> Map.get("overlap_policy", "skip") |> String.trim()
    overwrite = replacement?(params)
    timezone = params |> Map.get("timezone", "Etc/UTC") |> String.trim()

    cond do
      Map.get(params, "id", "") == "" ->
        {:error, {"id", "schedule id is required"}}

      target_type == "" ->
        {:error, {"target_type", "target workflow type is required"}}

      true ->
        with :ok <- field_result("schedule_kind", validate_timing_fields(schedule_kind, params)),
             {:ok, timing_opts} <-
               field_result(timing_field(schedule_kind), parse_timing_opts(schedule_kind, params)),
             {:ok, bounds} <- parse_bounds(schedule_kind, params),
             {:ok, payload} <-
               field_result("target_payload", parse_payload_json(target_payload_raw)),
             {:ok, overlap_policy} <-
               field_result(
                 "overlap_policy",
                 parse_overlap_policy(schedule_kind, overlap_policy_raw)
               ),
             {:ok, max_fires} <-
               field_result(
                 "max_fires",
                 parse_max_fires(schedule_kind, Map.get(params, "max_fires"))
               ) do
          target_opts =
            [type: target_type]
            |> maybe_put_kw(:partition_key, if(target_partition != "", do: target_partition))
            |> maybe_put_kw(:payload, payload)

          create_opts =
            timing_opts
            |> Keyword.merge(bounds)
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

          {:ok, create_opts}
        end
    end
  end

  defp field_result(field, {:error, message}), do: {:error, {field, message}}
  defp field_result(_field, result), do: result

  defp timing_field("cron"), do: "cron"
  defp timing_field("interval"), do: "every_ms"
  defp timing_field("delay"), do: "delay_ms"
  defp timing_field("one_shot"), do: "at_utc"
  defp timing_field(_kind), do: "schedule_kind"

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

  defp parse_timing_opts("one_shot", params) do
    with {:ok, instant} <- parse_utc(Map.get(params, "at_utc"), "Run at (UTC)", true) do
      {:ok, [at_ms: instant]}
    end
  end

  defp parse_timing_opts(_other, _params), do: {:error, "unsupported schedule kind"}

  defp validate_timing_fields(kind, params) do
    timing_fields = [
      {"cron", "cron"},
      {"interval", "every_ms"},
      {"delay", "delay_ms"},
      {"one_shot", "at_utc"}
    ]

    incompatible =
      Enum.find(timing_fields, fn {mode, field} ->
        mode != kind and normalize_text(Map.get(params, field)) != nil
      end)

    cond do
      incompatible != nil ->
        {:error, "only the selected schedule kind may include timing fields"}

      kind not in ["cron", "interval"] and
          Enum.any?(~w(start_at_utc end_at_utc), &(normalize_text(Map.get(params, &1)) != nil)) ->
        {:error, "start and end bounds are only supported for recurring schedules"}

      true ->
        :ok
    end
  end

  defp parse_bounds(kind, params) when kind in ["cron", "interval"] do
    with {:ok, start_at} <-
           field_result(
             "start_at_utc",
             parse_utc(Map.get(params, "start_at_utc"), "Start at (UTC)", false)
           ),
         {:ok, end_at} <-
           field_result(
             "end_at_utc",
             parse_utc(Map.get(params, "end_at_utc"), "End at (UTC)", false)
           ) do
      if is_integer(start_at) and is_integer(end_at) and end_at <= start_at do
        {:error, {"end_at_utc", "End at (UTC) must be after Start at (UTC)"}}
      else
        {:ok, [] |> maybe_put_kw(:start_at_ms, start_at) |> maybe_put_kw(:end_at_ms, end_at)}
      end
    end
  end

  defp parse_bounds(_kind, _params), do: {:ok, []}

  defp parse_utc(value, label, required?) do
    case normalize_text(value) do
      nil when not required? ->
        {:ok, nil}

      nil ->
        {:error, "#{label} is required"}

      text ->
        text = if byte_size(text) == 16, do: text <> ":00", else: text

        with true <- Regex.match?(~r/\A\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?\z/, text),
             {:ok, naive} <- NaiveDateTime.from_iso8601(text),
             {:ok, date} <- DateTime.from_naive(naive, "Etc/UTC"),
             millis when millis >= 0 <- DateTime.to_unix(date, :millisecond) do
          {:ok, millis}
        else
          _ -> {:error, "#{label} must be a valid UTC date and time on or after 1970-01-01"}
        end
    end
  end

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
