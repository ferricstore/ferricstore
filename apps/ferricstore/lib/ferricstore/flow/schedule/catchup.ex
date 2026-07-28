defmodule Ferricstore.Flow.Schedule.Catchup do
  @moduledoc false

  @max_exact_integer 9_007_199_254_740_991

  @type coalesced_count :: non_neg_integer()

  @spec policy(atom(), keyword()) :: {:ok, :fire_once | nil} | {:error, binary()}
  def policy(:interval, opts) do
    case Keyword.get(opts, :catchup_policy, :fire_once) do
      :fire_once -> {:ok, :fire_once}
      _other -> {:error, "ERR flow schedule catchup_policy must be :fire_once"}
    end
  end

  def policy(_kind, opts) do
    if Keyword.has_key?(opts, :catchup_policy) do
      {:error, "ERR flow schedule catchup_policy is only supported for interval schedules"}
    else
      {:ok, nil}
    end
  end

  @spec policy_for(map()) :: :fire_once | nil
  def policy_for(%{kind: :interval} = definition),
    do: Map.get(definition, :catchup_policy, :fire_once)

  def policy_for(_definition), do: nil

  @spec next_interval(map(), non_neg_integer(), non_neg_integer()) ::
          {:ok, non_neg_integer(), coalesced_count()}
          | {:complete, :end_at_ms, coalesced_count()}
          | {:error, :timestamp_limit, coalesced_count()}
          | {:error, binary()}
  def next_interval(%{every_ms: every_ms} = definition, due_at_ms, now_ms) do
    if valid_interval_input?(every_ms, due_at_ms, now_ms) do
      plan_next_interval(definition, every_ms, due_at_ms, now_ms)
    else
      {:error, "ERR schedule payload missing"}
    end
  end

  def next_interval(_definition, _due_at_ms, _now_ms),
    do: {:error, "ERR schedule payload missing"}

  defp plan_next_interval(definition, every_ms, due_at_ms, now_ms) do
    horizon_ms = catchup_horizon(definition, now_ms)
    elapsed_ms = max(horizon_ms - due_at_ms, 0)

    coalesced_count =
      if elapsed_ms < every_ms,
        do: 0,
        else: div(elapsed_ms, every_ms)

    if end_reached?(definition, now_ms) do
      {:complete, :end_at_ms, coalesced_count}
    else
      next_base_ms = if coalesced_count > 0, do: now_ms, else: due_at_ms

      if next_base_ms > @max_exact_integer - every_ms do
        {:error, :timestamp_limit, coalesced_count}
      else
        {:ok, next_base_ms + every_ms, coalesced_count}
      end
    end
  end

  @spec record(map(), non_neg_integer(), coalesced_count()) :: map()
  def record(definition, _now_ms, 0), do: definition

  def record(definition, now_ms, coalesced_count)
      when is_integer(coalesced_count) and coalesced_count > 0 do
    definition
    |> Map.update(:coalesced_count, coalesced_count, fn current ->
      min(current + coalesced_count, @max_exact_integer)
    end)
    |> Map.put(:last_catchup_at_ms, now_ms)
    |> Map.put(:last_coalesced_count, coalesced_count)
  end

  defp catchup_horizon(%{end_at_ms: end_at_ms}, now_ms) when is_integer(end_at_ms),
    do: min(end_at_ms, now_ms)

  defp catchup_horizon(_definition, now_ms), do: now_ms

  defp end_reached?(%{end_at_ms: end_at_ms}, now_ms) when is_integer(end_at_ms),
    do: end_at_ms <= now_ms

  defp end_reached?(_definition, _now_ms), do: false

  defp valid_interval_input?(every_ms, due_at_ms, now_ms),
    do:
      is_integer(every_ms) and every_ms > 0 and is_integer(due_at_ms) and due_at_ms >= 0 and
        is_integer(now_ms) and now_ms >= 0
end
