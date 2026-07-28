defmodule Ferricstore.Flow.Schedule.Listing do
  @moduledoc false

  alias Ferricstore.Flow.Schedule
  alias Ferricstore.Flow.Schedule.Catalog

  @active_state "active"
  @terminal_states ["completed", "failed", "cancelled"]
  @all_states ["active", "paused", "running" | @terminal_states]
  @default_page_size 256
  @max_page_size 512
  @max_exact_integer 9_007_199_254_740_991
  @option_keys [:count, :from_ms, :kind, :rev, :state, :target_type, :timezone, :to_ms]

  @spec list(FerricStore.Instance.t(), keyword()) :: {:ok, [map()]} | {:error, binary()}
  def list(ctx, opts) when is_list(opts) do
    with :ok <- validate_options(opts),
         {:ok, states} <- list_states(opts),
         {:ok, count} <- list_count(opts),
         {:ok, rev?} <- optional_boolean(opts, :rev, false),
         {:ok, filters} <- list_filters(opts),
         direction = if(rev?, do: :desc, else: :asc),
         {:ok, summaries} <- collect_summaries(ctx, states, filters, count, direction),
         {:ok, schedules} <- hydrate_summaries(ctx, summaries) do
      schedules =
        schedules
        |> Enum.reject(&is_nil/1)
        |> Enum.filter(&matches_schedule?(&1, states, filters))
        |> sort_schedules(direction)
        |> Enum.take(count)

      {:ok, schedules}
    end
  end

  def list(_ctx, _opts), do: {:error, "ERR flow schedule opts must be a keyword list"}

  defp collect_summaries(ctx, states, filters, count, direction) do
    reducer = fn page, selected ->
      page
      |> Enum.filter(&matches_summary?(&1, states, filters))
      |> Enum.reduce(selected, &put_newest_summary/2)
      |> trim_summaries(count, direction)
    end

    with {:ok, selected} <- Catalog.reduce_summaries(ctx, page_size(), %{}, reducer) do
      {:ok, selected |> Map.values() |> sort_summaries(direction)}
    end
  end

  defp put_newest_summary(summary, selected) do
    Map.update(selected, summary.id, summary, fn current ->
      if summary.version >= current.version, do: summary, else: current
    end)
  end

  defp trim_summaries(selected, count, direction) when map_size(selected) > count do
    selected
    |> Map.values()
    |> sort_summaries(direction)
    |> Enum.take(count)
    |> Map.new(&{&1.id, &1})
  end

  defp trim_summaries(selected, _count, _direction), do: selected

  defp sort_summaries(summaries, direction), do: sort_schedules(summaries, direction)

  defp sort_schedules(schedules, direction),
    do: Enum.sort_by(schedules, &ordering_key(&1, direction), direction)

  defp hydrate_summaries(ctx, summaries) when length(summaries) <= 4 do
    Enum.reduce_while(summaries, {:ok, []}, fn summary, {:ok, acc} ->
      case Schedule.get(ctx, summary.id) do
        {:ok, schedule} -> {:cont, {:ok, [schedule | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      {:error, _reason} = error -> error
    end
  end

  defp hydrate_summaries(ctx, summaries) do
    summaries
    |> Task.async_stream(
      fn summary -> Schedule.get(ctx, summary.id) end,
      max_concurrency: min(8, System.schedulers_online()),
      ordered: true,
      on_timeout: :kill_task,
      timeout: 30_000
    )
    |> Enum.reduce_while({:ok, []}, fn
      {:ok, {:ok, schedule}}, {:ok, acc} -> {:cont, {:ok, [schedule | acc]}}
      {:ok, {:error, _reason} = error}, _acc -> {:halt, error}
      {:exit, _reason}, _acc -> {:halt, {:error, "ERR flow schedule hydration failed"}}
    end)
    |> case do
      {:ok, reversed} -> {:ok, Enum.reverse(reversed)}
      {:error, _reason} = error -> error
    end
  end

  defp validate_options(opts) do
    if Keyword.keyword?(opts) do
      keys = Keyword.keys(opts)

      case Enum.find(keys, &(&1 not in @option_keys)) do
        nil ->
          case first_duplicate(keys, MapSet.new()) do
            nil -> :ok
            key -> {:error, "ERR duplicate flow schedule option #{key}"}
          end

        key ->
          {:error, "ERR unsupported flow schedule option #{key}"}
      end
    else
      {:error, "ERR flow schedule opts must be a keyword list"}
    end
  end

  defp first_duplicate([], _seen), do: nil

  defp first_duplicate([key | keys], seen) do
    if MapSet.member?(seen, key),
      do: key,
      else: first_duplicate(keys, MapSet.put(seen, key))
  end

  defp list_states(opts) do
    case Keyword.get(opts, :state, @active_state) do
      :all ->
        {:ok, @all_states}

      "all" ->
        {:ok, @all_states}

      state when state in @all_states ->
        {:ok, [state]}

      _ ->
        {:error,
         "ERR flow schedule state must be active, paused, running, completed, failed, " <>
           "cancelled, or all"}
    end
  end

  defp list_count(opts) do
    case Keyword.get(opts, :count, 100) do
      value when is_integer(value) and value > 0 -> {:ok, min(value, 1_000)}
      _ -> {:error, "ERR flow schedule count must be a positive integer"}
    end
  end

  defp page_size do
    case Application.get_env(:ferricstore, :flow_schedule_list_page_size, @default_page_size) do
      value when is_integer(value) and value > 0 -> min(value, @max_page_size)
      _invalid -> @default_page_size
    end
  end

  defp list_filters(opts) do
    with {:ok, kind} <- optional_kind(opts),
         {:ok, target_type} <- optional_binary(opts, :target_type),
         {:ok, timezone} <- optional_binary(opts, :timezone),
         {:ok, from_ms} <- optional_non_neg_integer(opts, :from_ms),
         {:ok, to_ms} <- optional_non_neg_integer(opts, :to_ms),
         :ok <- validate_time_range(from_ms, to_ms) do
      {:ok,
       %{
         kind: kind,
         target_type: target_type,
         timezone: normalize_timezone(timezone),
         from_ms: from_ms,
         to_ms: to_ms
       }}
    end
  end

  defp validate_time_range(from_ms, to_ms)
       when is_integer(from_ms) and is_integer(to_ms) and from_ms > to_ms,
       do: {:error, "ERR flow schedule from_ms must be less than or equal to to_ms"}

  defp validate_time_range(_from_ms, _to_ms), do: :ok

  defp optional_kind(opts) do
    case Keyword.get(opts, :kind) do
      nil -> {:ok, nil}
      kind when kind in [:one_shot, :delay, :interval, :cron] -> {:ok, kind}
      _ -> {:error, "ERR flow schedule kind must be :one_shot, :delay, :interval, or :cron"}
    end
  end

  defp optional_binary(opts, key) do
    case Keyword.get(opts, key) do
      nil -> {:ok, nil}
      value when is_binary(value) and value != "" -> {:ok, value}
      _ -> {:error, "ERR flow schedule #{key} must be a non-empty string"}
    end
  end

  defp optional_non_neg_integer(opts, key) do
    case Keyword.get(opts, key) do
      nil ->
        {:ok, nil}

      value when is_integer(value) and value >= 0 and value <= @max_exact_integer ->
        {:ok, value}

      value when is_integer(value) and value > @max_exact_integer ->
        {:error, "ERR flow schedule #{key} exceeds maximum #{@max_exact_integer}"}

      _ ->
        {:error, "ERR flow schedule #{key} must be a non-negative integer"}
    end
  end

  defp optional_boolean(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_boolean(value) -> {:ok, value}
      _ -> {:error, "ERR flow schedule #{key} must be a boolean"}
    end
  end

  defp matches_summary?(summary, states, filters) do
    summary.state in states and
      filter_match?(filters.kind, summary.kind) and
      filter_match?(filters.target_type, summary.target_type) and
      filter_match?(filters.timezone, summary.timezone) and
      due_in_range?(summary.next_run_at_ms, filters.from_ms, filters.to_ms)
  end

  defp matches_schedule?(schedule, states, filters) do
    schedule.state in states and
      filter_match?(filters.kind, schedule.kind) and
      filter_match?(filters.target_type, get_in(schedule, [:target, :type])) and
      filter_match?(filters.timezone, Map.get(schedule, :timezone)) and
      due_in_range?(schedule.next_run_at_ms, filters.from_ms, filters.to_ms)
  end

  defp filter_match?(nil, _value), do: true
  defp filter_match?(value, value), do: true
  defp filter_match?(_expected, _value), do: false

  defp due_in_range?(nil, nil, nil), do: true
  defp due_in_range?(nil, _from_ms, _to_ms), do: false
  defp due_in_range?(due_ms, nil, nil), do: is_integer(due_ms)
  defp due_in_range?(due_ms, from_ms, nil), do: is_integer(due_ms) and due_ms >= from_ms
  defp due_in_range?(due_ms, nil, to_ms), do: is_integer(due_ms) and due_ms <= to_ms

  defp due_in_range?(due_ms, from_ms, to_ms),
    do: is_integer(due_ms) and due_ms >= from_ms and due_ms <= to_ms

  defp ordering_key(%{next_run_at_ms: due_ms, id: id}, :asc) when is_integer(due_ms),
    do: {0, due_ms, id}

  defp ordering_key(%{id: id}, :asc), do: {1, 0, id}

  defp ordering_key(%{next_run_at_ms: due_ms, id: id}, :desc) when is_integer(due_ms),
    do: {1, due_ms, id}

  defp ordering_key(%{id: id}, :desc), do: {0, 0, id}

  defp normalize_timezone("UTC"), do: "Etc/UTC"
  defp normalize_timezone(timezone), do: timezone
end
