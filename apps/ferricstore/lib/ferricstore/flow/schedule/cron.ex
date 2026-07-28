defmodule Ferricstore.Flow.Schedule.Cron do
  @moduledoc false

  @minute_ms 60_000
  @calendar_range_error "ERR flow schedule timestamp is outside supported calendar range"
  @no_match_error "ERR flow schedule cron has no matching time in search window"

  @spec next_run_at_ms(binary(), non_neg_integer(), binary(), pos_integer()) ::
          {:ok, non_neg_integer()} | {:error, binary()}
  def next_run_at_ms(expr, after_ms, timezone, search_minutes)
      when is_binary(expr) and is_integer(after_ms) and after_ms >= -1 and is_binary(timezone) and
             is_integer(search_minutes) and search_minutes > 0 do
    with {:ok, cron} <- parse(expr),
         :ok <- feasible_calendar?(cron),
         {:ok, after_datetime} <- shifted_datetime(after_ms, timezone) do
      upper_ms = after_ms + (search_minutes + 1) * @minute_ms
      max_days = div(search_minutes, 24 * 60) + 2
      times = allowed_times(cron)

      find_candidate(
        cron,
        DateTime.to_date(after_datetime),
        times,
        timezone,
        after_ms,
        upper_ms,
        0,
        max_days
      )
    end
  end

  def next_run_at_ms(_expr, _after_ms, _timezone, _search_minutes),
    do: {:error, "ERR flow schedule cron arguments are invalid"}

  @spec parse(binary()) :: {:ok, map()} | {:error, binary()}
  def parse(expr) when is_binary(expr) do
    case String.split(expr, ~r/\s+/, trim: true) do
      [minute, hour, day, month, weekday] ->
        with {:ok, minute_set, _minute_any?} <- cron_field(minute, 0, 59, %{}),
             {:ok, hour_set, _hour_any?} <- cron_field(hour, 0, 23, %{}),
             {:ok, day_set, day_any?} <- cron_field(day, 1, 31, %{}),
             {:ok, month_set, _month_any?} <- cron_field(month, 1, 12, month_aliases()),
             {:ok, weekday_set, weekday_any?} <-
               cron_field(weekday, 0, 7, weekday_aliases()) do
          {:ok,
           %{
             minute: minute_set,
             hour: hour_set,
             day: day_set,
             day_any?: day_any?,
             month: month_set,
             weekday: normalize_weekday_set(weekday_set),
             weekday_any?: weekday_any?
           }}
        end

      _ ->
        {:error, "ERR flow schedule cron must have 5 fields"}
    end
  end

  def parse(_expr), do: {:error, "ERR flow schedule cron must be a string"}

  @spec validate_timezone(binary()) :: :ok | {:error, binary()}
  def validate_timezone(timezone) when is_binary(timezone) do
    case shifted_datetime(0, timezone) do
      {:ok, _datetime} -> :ok
      {:error, _reason} = error -> error
    end
  end

  def validate_timezone(_timezone),
    do: {:error, "ERR flow schedule timezone is invalid or unavailable"}

  defp feasible_calendar?(%{day_any?: true}), do: :ok
  defp feasible_calendar?(%{weekday_any?: false}), do: :ok

  defp feasible_calendar?(cron) do
    possible? =
      Enum.any?(cron.month, fn month ->
        max_day = Date.days_in_month(Date.new!(2000, month, 1))
        Enum.any?(cron.day, &(&1 <= max_day))
      end)

    if possible?,
      do: :ok,
      else: {:error, "ERR flow schedule cron day cannot occur in the selected months"}
  end

  defp allowed_times(cron) do
    for hour <- Enum.sort(cron.hour), minute <- Enum.sort(cron.minute), do: {hour, minute}
  end

  defp find_candidate(
         _cron,
         _start_date,
         _times,
         _timezone,
         _after_ms,
         _upper_ms,
         day_offset,
         max_days
       )
       when day_offset > max_days,
       do: {:error, @no_match_error}

  defp find_candidate(
         cron,
         start_date,
         times,
         timezone,
         after_ms,
         upper_ms,
         day_offset,
         max_days
       ) do
    case safe_date_add(start_date, day_offset) do
      {:ok, date} ->
        case candidate_for_date(cron, date, times, timezone, after_ms, upper_ms) do
          {:ok, candidate_ms} ->
            {:ok, candidate_ms}

          :none ->
            find_candidate(
              cron,
              start_date,
              times,
              timezone,
              after_ms,
              upper_ms,
              day_offset + 1,
              max_days
            )

          {:error, _reason} = error ->
            error
        end

      :calendar_limit ->
        {:error, @calendar_range_error}
    end
  end

  defp candidate_for_date(cron, date, times, timezone, after_ms, upper_ms) do
    weekday = date |> Date.day_of_week() |> rem(7)

    if MapSet.member?(cron.month, date.month) and cron_day_match?(cron, date.day, weekday) do
      times
      |> Enum.reduce_while(:none, fn {hour, minute}, best ->
        naive = NaiveDateTime.new!(date, Time.new!(hour, minute, 0))

        case candidate_instants(naive, timezone) do
          {:ok, instants} ->
            case valid_candidate(instants, after_ms, upper_ms) do
              :none ->
                {:cont, best}

              {:ok, candidate_ms, :ordered} when best == :none ->
                {:halt, {:ok, candidate_ms}}

              {:ok, candidate_ms, _ordering} ->
                {:cont, min_candidate(best, candidate_ms)}
            end

          {:error, _reason} = error ->
            {:halt, error}
        end
      end)
      |> normalize_candidate()
    else
      :none
    end
  end

  defp valid_candidate(instants, after_ms, upper_ms) do
    case Enum.find_index(instants, &(&1 > after_ms and &1 <= upper_ms)) do
      nil -> :none
      0 -> {:ok, hd(instants), :ordered}
      index -> {:ok, Enum.at(instants, index), :ambiguous_late}
    end
  end

  defp min_candidate(:none, candidate_ms), do: {:candidate, candidate_ms}

  defp min_candidate({:candidate, best_ms}, candidate_ms),
    do: {:candidate, min(best_ms, candidate_ms)}

  defp normalize_candidate({:candidate, candidate_ms}), do: {:ok, candidate_ms}
  defp normalize_candidate(result), do: result

  defp candidate_instants(naive, timezone) do
    case DateTime.from_naive(naive, timezone, Tz.TimeZoneDatabase) do
      {:ok, datetime} ->
        {:ok, [DateTime.to_unix(datetime, :millisecond)]}

      {:ambiguous, first, second} ->
        {:ok,
         [DateTime.to_unix(first, :millisecond), DateTime.to_unix(second, :millisecond)]
         |> Enum.sort()}

      {:gap, _before, _after} ->
        {:ok, []}

      {:error, _reason} ->
        {:error, "ERR flow schedule timezone is invalid or unavailable"}
    end
  end

  defp shifted_datetime(ms, timezone) do
    case DateTime.from_unix(ms, :millisecond) do
      {:ok, datetime} ->
        case DateTime.shift_zone(datetime, timezone, Tz.TimeZoneDatabase) do
          {:ok, shifted} -> {:ok, shifted}
          {:error, _reason} -> {:error, "ERR flow schedule timezone is invalid or unavailable"}
        end

      {:error, _reason} ->
        {:error, @calendar_range_error}
    end
  end

  defp safe_date_add(date, offset) do
    case Date.add(date, offset) do
      %Date{year: year} = candidate when year <= 9_999 -> {:ok, candidate}
      _outside_supported_calendar -> :calendar_limit
    end
  rescue
    ArgumentError -> :calendar_limit
  end

  defp cron_field(field, min, max, aliases) do
    any? = field in ["*", "?"]
    parts = if any?, do: ["*"], else: String.split(field, ",", trim: false)

    if Enum.any?(parts, &(&1 == "")) do
      {:error, "ERR flow schedule cron field is invalid"}
    else
      Enum.reduce_while(parts, {:ok, MapSet.new(), any?}, fn part, {:ok, acc, any?} ->
        case cron_part(part, min, max, aliases) do
          {:ok, values} -> {:cont, {:ok, MapSet.union(acc, values), any?}}
          {:error, _reason} = error -> {:halt, error}
        end
      end)
    end
  end

  defp cron_part(part, min, max, aliases) do
    [range_part, step_part] = split_cron_step(part)

    with {:ok, step} <- cron_step(step_part),
         {:ok, first, last} <- cron_range(range_part, min, max, aliases) do
      values = first |> Stream.iterate(&(&1 + step)) |> Enum.take_while(&(&1 <= last))
      {:ok, MapSet.new(values)}
    end
  end

  defp split_cron_step(part) do
    case String.split(part, "/", parts: 2) do
      [range_part] -> [range_part, nil]
      [range_part, step_part] -> [range_part, step_part]
    end
  end

  defp cron_step(nil), do: {:ok, 1}

  defp cron_step(value) do
    case Integer.parse(value) do
      {step, ""} when step > 0 -> {:ok, step}
      _ -> {:error, "ERR flow schedule cron step must be positive"}
    end
  end

  defp cron_range("*", min, max, _aliases), do: {:ok, min, max}
  defp cron_range("?", min, max, _aliases), do: {:ok, min, max}

  defp cron_range(value, min, max, aliases) do
    case String.split(value, "-", parts: 2) do
      [single] ->
        with {:ok, parsed} <- cron_value(single, aliases),
             :ok <- cron_value_in_range(parsed, min, max) do
          {:ok, parsed, parsed}
        end

      [first, last] ->
        with {:ok, parsed_first} <- cron_value(first, aliases),
             {:ok, parsed_last} <- cron_value(last, aliases),
             :ok <- cron_value_in_range(parsed_first, min, max),
             :ok <- cron_value_in_range(parsed_last, min, max),
             :ok <- cron_range_order(parsed_first, parsed_last) do
          {:ok, parsed_first, parsed_last}
        end
    end
  end

  defp cron_value(value, aliases) do
    case Map.fetch(aliases, String.upcase(value)) do
      {:ok, aliased} ->
        {:ok, aliased}

      :error ->
        case Integer.parse(value) do
          {parsed, ""} -> {:ok, parsed}
          _ -> {:error, "ERR flow schedule cron field is invalid"}
        end
    end
  end

  defp cron_value_in_range(value, min, max) when value >= min and value <= max, do: :ok

  defp cron_value_in_range(_value, _min, _max),
    do: {:error, "ERR flow schedule cron value out of range"}

  defp cron_range_order(first, last) when first <= last, do: :ok
  defp cron_range_order(_first, _last), do: {:error, "ERR flow schedule cron range is invalid"}

  defp cron_day_match?(%{day_any?: true, weekday_any?: true}, _day, _weekday), do: true

  defp cron_day_match?(%{day_any?: true} = cron, _day, weekday),
    do: MapSet.member?(cron.weekday, weekday)

  defp cron_day_match?(%{weekday_any?: true} = cron, day, _weekday),
    do: MapSet.member?(cron.day, day)

  defp cron_day_match?(cron, day, weekday),
    do: MapSet.member?(cron.day, day) or MapSet.member?(cron.weekday, weekday)

  defp normalize_weekday_set(set) do
    set
    |> Enum.map(fn
      7 -> 0
      value -> value
    end)
    |> MapSet.new()
  end

  defp month_aliases do
    %{
      "JAN" => 1,
      "FEB" => 2,
      "MAR" => 3,
      "APR" => 4,
      "MAY" => 5,
      "JUN" => 6,
      "JUL" => 7,
      "AUG" => 8,
      "SEP" => 9,
      "OCT" => 10,
      "NOV" => 11,
      "DEC" => 12
    }
  end

  defp weekday_aliases do
    %{
      "SUN" => 0,
      "MON" => 1,
      "TUE" => 2,
      "WED" => 3,
      "THU" => 4,
      "FRI" => 5,
      "SAT" => 6
    }
  end
end
