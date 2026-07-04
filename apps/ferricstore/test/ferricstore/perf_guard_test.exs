defmodule Ferricstore.PerfGuardTest do
  use ExUnit.Case, async: false

  @moduletag :global_state
  @moduletag :perf_guard
  @moduletag timeout: 120_000

  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  @write_p95_limit_us 500_000
  @batch_limit_us 5_000_000

  setup do
    ShardHelpers.flush_all_keys()

    on_exit(fn ->
      ShardHelpers.flush_all_keys()
    end)

    :ok
  end

  test "single-key write/read p95 stays inside the PR guard budget" do
    ctx = FerricStore.Instance.get(:default)

    timings =
      for i <- 1..80 do
        key = "perf-guard:single:#{System.unique_integer([:positive])}:#{i}"
        value = "value-#{i}"

        timed_us(fn ->
          assert :ok = Router.put(ctx, key, value, 0)
          assert Router.get(ctx, key) == value
        end)
      end

    assert percentile(timings, 95) < @write_p95_limit_us
  end

  test "multi-shard batch write fanout completes inside the PR guard budget" do
    ctx = FerricStore.Instance.get(:default)

    pairs =
      0..63
      |> Enum.map(fn i ->
        key = "perf-guard:batch:#{System.unique_integer([:positive])}:#{i}"
        {key, "batch-value-#{i}"}
      end)

    elapsed_us =
      timed_us(fn ->
        assert Enum.all?(Router.batch_quorum_put(ctx, pairs), &(&1 == :ok))
      end)

    assert elapsed_us < @batch_limit_us

    Enum.each(pairs, fn {key, value} ->
      assert Router.get(ctx, key) == value
    end)
  end

  defp timed_us(fun) do
    started = System.monotonic_time(:microsecond)
    fun.()
    System.monotonic_time(:microsecond) - started
  end

  defp percentile(values, percentile) do
    sorted = Enum.sort(values)
    index = ceil(length(sorted) * percentile / 100) - 1
    Enum.at(sorted, max(index, 0))
  end
end
