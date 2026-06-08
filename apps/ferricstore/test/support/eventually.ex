defmodule Ferricstore.Test.Eventually do
  @moduledoc """
  Small polling helpers for tests that must wait for asynchronous state.

  Prefer these helpers over fixed sleeps when the test is waiting for a
  condition. Fixed sleeps are still valid for tests that intentionally verify
  timeout behavior or scheduler blocking.
  """

  @type assertion_fun :: (-> any())

  @doc """
  Re-runs `fun` until it stops raising or the timeout expires.
  """
  @spec assert_eventually(assertion_fun(), keyword()) :: :ok
  def assert_eventually(fun, opts \\ []) when is_function(fun, 0) do
    timeout_ms = Keyword.get(opts, :timeout, 5_000)
    interval_ms = Keyword.get(opts, :interval, 25)
    deadline = System.monotonic_time(:millisecond) + timeout_ms

    do_assert_eventually(fun, deadline, interval_ms, nil)
  end

  @doc """
  Polls a boolean-returning function until it returns truthy.
  """
  @spec eventually((-> as_boolean(term())), keyword()) :: boolean()
  def eventually(fun, opts \\ []) when is_function(fun, 0) do
    timeout_ms = Keyword.get(opts, :timeout, 5_000)
    interval_ms = Keyword.get(opts, :interval, 25)
    deadline = System.monotonic_time(:millisecond) + timeout_ms

    do_eventually(fun, deadline, interval_ms)
  end

  defp do_assert_eventually(fun, deadline, interval_ms, last_error) do
    try do
      fun.()
      :ok
    rescue
      error ->
        if System.monotonic_time(:millisecond) >= deadline do
          reraise error, __STACKTRACE__
        else
          Process.sleep(interval_ms)
          do_assert_eventually(fun, deadline, interval_ms, error)
        end
    catch
      kind, reason ->
        if System.monotonic_time(:millisecond) >= deadline do
          if last_error, do: raise(last_error), else: :erlang.raise(kind, reason, __STACKTRACE__)
        else
          Process.sleep(interval_ms)
          do_assert_eventually(fun, deadline, interval_ms, last_error)
        end
    end
  end

  defp do_eventually(fun, deadline, interval_ms) do
    if fun.() do
      true
    else
      if System.monotonic_time(:millisecond) >= deadline do
        false
      else
        Process.sleep(interval_ms)
        do_eventually(fun, deadline, interval_ms)
      end
    end
  end
end
