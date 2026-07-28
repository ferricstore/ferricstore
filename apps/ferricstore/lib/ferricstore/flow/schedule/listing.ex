defmodule Ferricstore.Flow.Schedule.Listing do
  @moduledoc false

  alias Ferricstore.Flow.RecordRead
  alias Ferricstore.Flow.Schedule
  alias Ferricstore.Flow.Schedule.Catalog

  @schedule_type "__ferricstore_schedule"
  @schedule_id_prefix "__ferricstore_schedule__:"
  @active_state "active"
  @terminal_states ["completed", "failed", "cancelled"]
  @all_states ["active", "paused", "running" | @terminal_states]
  @default_scan_limit 1_000
  @max_exact_integer 9_007_199_254_740_991
  @option_keys [:count, :from_ms, :kind, :rev, :state, :target_type, :timezone, :to_ms]

  @spec list(FerricStore.Instance.t(), keyword()) :: {:ok, [map()]} | {:error, binary()}
  def list(ctx, opts) when is_list(opts) do
    with :ok <- validate_options(opts),
         {:ok, states} <- list_states(opts),
         {:ok, count} <- list_count(opts),
         {:ok, rev?} <- optional_boolean(opts, :rev, false),
         {:ok, filters} <- list_filters(opts),
         {:ok, partition_keys} <- Catalog.partition_keys(ctx),
         {:ok, records} <- collect_candidates(ctx, states, partition_keys, scan_limit()),
         {:ok, schedules} <- hydrate_records(ctx, records) do
      direction = if rev?, do: :desc, else: :asc

      schedules =
        schedules
        |> Enum.reject(&is_nil/1)
        |> Enum.uniq_by(& &1.id)
        |> Enum.filter(&matches_filters?(&1, filters))
        |> Enum.sort_by(&{sort_due(&1), &1.id}, direction)
        |> Enum.take(count)

      {:ok, schedules}
    end
  end

  def list(_ctx, _opts), do: {:error, "ERR flow schedule opts must be a keyword list"}

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

  defp collect_candidates(ctx, states, partition_keys, limit) do
    routes = for state <- states, partition_key <- partition_keys, do: {state, partition_key}

    routes
    |> Enum.reduce_while({:ok, [], limit}, fn {state, partition_key}, {:ok, chunks, remaining} ->
      case partition_records(ctx, state, partition_key, remaining + 1) do
        {:ok, records} when length(records) <= remaining ->
          {:cont, {:ok, [records | chunks], remaining - length(records)}}

        {:ok, _overflow} ->
          {:halt, {:error, "ERR flow schedule query candidate limit exceeded (#{limit})"}}

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, chunks, _remaining} -> {:ok, chunks |> Enum.reverse() |> List.flatten()}
      {:error, _reason} = error -> error
    end
  end

  defp partition_records(ctx, state, partition_key, count) do
    query = %{
      count: count,
      from_ms: nil,
      to_ms: nil,
      rev?: false,
      before_id: nil,
      state: state,
      terminal_only?: state in @terminal_states
    }

    RecordRead.list_records(
      ctx,
      @schedule_type,
      state,
      partition_key,
      count,
      query,
      true,
      true,
      @terminal_states,
      count
    )
  end

  defp hydrate_records(ctx, records) do
    records
    |> Enum.reduce_while({:ok, [], %{}}, fn record, {:ok, schedules, cache} ->
      id = Map.get(record, :id)

      case Map.fetch(cache, id) do
        {:ok, schedule} ->
          {:cont, {:ok, [schedule | schedules], cache}}

        :error ->
          case hydrate_record(ctx, record) do
            {:ok, schedule} ->
              {:cont, {:ok, [schedule | schedules], Map.put(cache, id, schedule)}}

            {:error, _reason} = error ->
              {:halt, error}
          end
      end
    end)
    |> case do
      {:ok, schedules, _cache} -> {:ok, Enum.reverse(schedules)}
      {:error, _reason} = error -> error
    end
  end

  defp hydrate_record(ctx, %{id: @schedule_id_prefix <> id}), do: Schedule.get(ctx, id)
  defp hydrate_record(_ctx, _record), do: {:ok, nil}

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

  defp scan_limit do
    case Application.get_env(:ferricstore, :flow_schedule_list_scan_limit, @default_scan_limit) do
      value when is_integer(value) and value > 0 -> min(value, @default_scan_limit)
      _invalid -> @default_scan_limit
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

  defp matches_filters?(schedule, filters) do
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

  defp sort_due(%{next_run_at_ms: due_ms}) when is_integer(due_ms), do: due_ms
  defp sort_due(_schedule), do: 9_223_372_036_854_775_807

  defp normalize_timezone("UTC"), do: "Etc/UTC"
  defp normalize_timezone(timezone), do: timezone
end
