defmodule Ferricstore.LatencyTrace do
  @moduledoc """
  Request-local latency trace collector.

  This is intentionally process-local and opt-in. Normal hot-path calls pay only
  the small `enabled?/0` check in selected boundaries; traced native requests set
  the collector in the lane process and receive the accumulated timings in the
  response payload.
  """

  @key :ferricstore_latency_trace
  @enabled_key {__MODULE__, :enabled}
  @command_tag :ferricstore_latency_trace
  @result_tag :ferricstore_latency_trace_result

  @spec start(map()) :: term()
  def start(initial \\ %{}) when is_map(initial) do
    :persistent_term.put(@enabled_key, true)
    previous = Process.get(@key, :undefined)
    Process.put(@key, initial)
    previous
  end

  @spec finish(term()) :: map()
  def finish(previous) do
    trace = Process.get(@key, %{})
    restore(previous)
    trace
  end

  @spec enabled?() :: boolean()
  def enabled? do
    :persistent_term.get(@enabled_key, false) and is_map(Process.get(@key))
  end

  @spec span(binary(), (-> result)) :: result when result: term()
  def span(key, fun) when is_binary(key) and is_function(fun, 0) do
    if enabled?() do
      started_us = System.monotonic_time(:microsecond)

      try do
        fun.()
      after
        duration_us = max(System.monotonic_time(:microsecond) - started_us, 0)
        add(key, duration_us)
      end
    else
      fun.()
    end
  end

  defmacro maybe_span(key, do: block) do
    quote do
      if Ferricstore.LatencyTrace.enabled?() do
        Ferricstore.LatencyTrace.span(unquote(key), fn -> unquote(block) end)
      else
        unquote(block)
      end
    end
  end

  @spec add(binary(), non_neg_integer()) :: :ok
  def add(key, duration_us)
      when is_binary(key) and is_integer(duration_us) and duration_us >= 0 do
    case Process.get(@key) do
      trace when is_map(trace) ->
        Process.put(@key, Map.update(trace, key, duration_us, &(&1 + duration_us)))
        :ok

      _other ->
        :ok
    end
  end

  @spec merge(map()) :: :ok
  def merge(trace) when is_map(trace) do
    Enum.each(trace, fn
      {key, value} when is_binary(key) and is_integer(value) and value >= 0 ->
        add(key, value)

      _other ->
        :ok
    end)
  end

  @spec maybe_wrap_command(term()) :: term()
  def maybe_wrap_command(command) do
    if enabled?(), do: {@command_tag, command}, else: command
  end

  @spec wrap_command(term()) :: term()
  def wrap_command(command), do: {@command_tag, command}

  @spec command_wrapper?(term()) :: boolean()
  def command_wrapper?({@command_tag, _command}), do: true
  def command_wrapper?(_command), do: false

  @spec unwrap_command(term()) :: term()
  def unwrap_command({@command_tag, command}), do: command
  def unwrap_command(command), do: command

  @spec wrap_result(term(), map()) :: term()
  def wrap_result({:applied_at, index, result}, trace) when is_map(trace) do
    {:applied_at, index, {@result_tag, result, trace}}
  end

  def wrap_result(result, trace) when is_map(trace), do: {@result_tag, result, trace}

  @spec merge_result(term()) :: term()
  def merge_result({@result_tag, result, trace}) when is_map(trace) do
    merge(trace)
    merge_result(result)
  end

  def merge_result({:applied_at, index, result}) do
    {:applied_at, index, merge_result(result)}
  end

  def merge_result({:ok, results}) when is_list(results) do
    {:ok, Enum.map(results, &merge_result/1)}
  end

  def merge_result(results) when is_list(results), do: Enum.map(results, &merge_result/1)
  def merge_result(result), do: result

  defp restore(:undefined), do: Process.delete(@key)
  defp restore(previous), do: Process.put(@key, previous)
end
