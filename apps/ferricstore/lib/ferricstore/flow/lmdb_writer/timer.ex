defmodule Ferricstore.Flow.LMDBWriter.Timer do
  @moduledoc false

  @default_lagged_flush_quiet_ms 250
  @default_lagged_flush_max_lag_ms 30_000
  @max_timer_ms 4_294_967_295

  def ensure_timer(%{timer_ref: nil, flush_interval_ms: interval} = state) do
    delay = bounded_delay_ms(interval, timer_jitter_ms(Map.get(state, :flush_jitter_ms, 0)))
    %{state | timer_ref: Process.send_after(self(), :flush, delay)}
  end

  def ensure_timer(state), do: state

  def ensure_projection_outbox_timer(%{timer_ref: nil} = state) do
    delay =
      state
      |> Map.get(:flush_max_lag_ms, @default_lagged_flush_max_lag_ms)
      |> max(1)

    %{state | timer_ref: Process.send_after(self(), :flush, delay)}
  end

  def ensure_projection_outbox_timer(state), do: state

  def ensure_timer_with_delay(%{timer_ref: nil} = state, delay_ms)
      when is_integer(delay_ms) and delay_ms > 0 do
    %{state | timer_ref: Process.send_after(self(), :flush, delay_ms)}
  end

  def ensure_timer_with_delay(state, _delay_ms), do: state

  def flush_on_max_ops?(%{flush_on_max_ops?: true} = state), do: state.count >= state.max_ops
  def flush_on_max_ops?(_state), do: false

  def maybe_defer_timer_flush(%{pending: [], pending_after_flush: []}), do: :flush

  def maybe_defer_timer_flush(%{flush_on_max_ops?: true, count: count, max_ops: max_ops})
      when is_integer(count) and is_integer(max_ops) and count >= max_ops,
      do: :flush

  def maybe_defer_timer_flush(%{last_enqueue_at: nil}), do: :flush
  def maybe_defer_timer_flush(%{first_pending_at: nil}), do: :flush

  def maybe_defer_timer_flush(state) do
    case timer_flush_decision(state) do
      {:defer, delay_ms} ->
        {:defer, %{state | timer_ref: Process.send_after(self(), :flush, delay_ms)}}

      :flush ->
        :flush
    end
  end

  def timer_flush_decision(%{pending: [], pending_after_flush: []}), do: :flush

  def timer_flush_decision(%{flush_on_max_ops?: true, count: count, max_ops: max_ops})
      when is_integer(count) and is_integer(max_ops) and count >= max_ops,
      do: :flush

  def timer_flush_decision(%{last_enqueue_at: nil}), do: :flush
  def timer_flush_decision(%{first_pending_at: nil}), do: :flush

  def timer_flush_decision(state), do: timer_flush_decision(state, System.monotonic_time())

  def timer_flush_decision(%{pending: [], pending_after_flush: []}, _now), do: :flush

  def timer_flush_decision(%{flush_on_max_ops?: true, count: count, max_ops: max_ops}, _now)
      when is_integer(count) and is_integer(max_ops) and count >= max_ops,
      do: :flush

  def timer_flush_decision(%{last_enqueue_at: nil}, _now), do: :flush
  def timer_flush_decision(%{first_pending_at: nil}, _now), do: :flush

  def timer_flush_decision(state, now) do
    quiet_ms =
      normalize_non_negative_integer(state.flush_quiet_ms, @default_lagged_flush_quiet_ms)

    max_lag_ms =
      normalize_non_negative_integer(state.flush_max_lag_ms, @default_lagged_flush_max_lag_ms)

    cond do
      quiet_ms == 0 or max_lag_ms == 0 ->
        :flush

      true ->
        idle_ms = elapsed_ms(state.last_enqueue_at, now)
        pending_age_ms = elapsed_ms(state.first_pending_at, now)

        if idle_ms < quiet_ms and pending_age_ms < max_lag_ms do
          {:defer, max(1, min(quiet_ms - idle_ms, max_lag_ms - pending_age_ms))}
        else
          :flush
        end
    end
  end

  @doc false
  def __bounded_delay_for_test__(interval_ms, jitter_ms),
    do: bounded_delay_ms(interval_ms, jitter_ms)

  defp elapsed_ms(started_at, now) when is_integer(started_at) and is_integer(now) do
    max(System.convert_time_unit(now - started_at, :native, :millisecond), 0)
  end

  defp timer_jitter_ms(jitter_ms) do
    jitter_ms = normalize_bounded_timer_ms(jitter_ms, 0)

    if jitter_ms == 0 do
      0
    else
      :erlang.phash2({self(), System.unique_integer([:monotonic])}, jitter_ms + 1)
    end
  end

  defp bounded_delay_ms(interval_ms, jitter_ms) do
    interval_ms = normalize_bounded_timer_ms(interval_ms, 0)
    jitter_ms = normalize_bounded_timer_ms(jitter_ms, 0)
    min(interval_ms + jitter_ms, @max_timer_ms)
  end

  defp normalize_bounded_timer_ms(value, default) do
    value
    |> normalize_non_negative_integer(default)
    |> min(@max_timer_ms)
  end

  defp normalize_non_negative_integer(value, _default) when is_integer(value) and value >= 0,
    do: value

  defp normalize_non_negative_integer(value, default) when is_binary(value) do
    case Integer.parse(value) do
      {integer, ""} when integer >= 0 -> integer
      _ -> default
    end
  end

  defp normalize_non_negative_integer(_value, default), do: default
end
