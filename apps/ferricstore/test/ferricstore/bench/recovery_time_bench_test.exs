defmodule Ferricstore.Bench.RecoveryTimeBenchTest do
  @moduledoc """
  Benchmarks in-process supervised shard recovery cost when Ra has entries
  beyond the last emitted release cursor.

  This is a regression metric that can run inside ExUnit. It does not replace
  an external kill-9/full-BEAM restart harness because Ra may still write
  recovery checkpoints during supervised restarts.

  Run with:

      mix test apps/ferricstore/test/ferricstore/bench/recovery_time_bench_test.exs --include bench --timeout 180000

  Set `FERRICSTORE_RECOVERY_BENCH_WRITES=N` to change the write count.
  """

  use ExUnit.Case, async: false
  @moduletag :global_state

  @moduletag :bench
  @moduletag timeout: 180_000

  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  test "benchmark: supervised shard recovery time with unreleased Ra entries" do
    writes = bench_writes()
    original_interval = Application.get_env(:ferricstore, :release_cursor_interval)

    # Keep the cursor interval above the benchmark write count so this measures
    # replay from unreleased entries instead of a recently compacted log.
    Application.put_env(:ferricstore, :release_cursor_interval, max(writes * 2, 20_000))

    ctx = ShardHelpers.setup_isolated_data_dir()

    on_exit(fn ->
      restore_release_cursor_interval(original_interval)
      ShardHelpers.teardown_isolated_data_dir(ctx)
    end)

    instance_ctx = FerricStore.Instance.get(:default)
    prefix = "recovery_bench_#{System.unique_integer([:positive])}_"

    {write_us, :ok} =
      :timer.tc(fn ->
        for i <- 1..writes do
          Router.put(instance_ctx, prefix <> Integer.to_string(i), "v#{i}")
        end

        :ok
      end)

    applied_before_restart = max_atomic(instance_ctx, :last_applied_index)
    released_before_restart = max_atomic(instance_ctx, :last_released_cursor_index)
    gap_before_restart = max(applied_before_restart - released_before_restart, 0)

    {recovery_us, :ok} =
      :timer.tc(fn ->
        shard_count = :persistent_term.get(:ferricstore_shard_count, 4)

        for i <- 0..(shard_count - 1) do
          ShardHelpers.kill_shard_safely(i, timeout: 30_000)
        end

        :ok
      end)

    restarted_ctx = FerricStore.Instance.get(:default)

    ShardHelpers.eventually(
      fn ->
        Router.get(restarted_ctx, prefix <> "1") == "v1" and
          Router.get(restarted_ctx, prefix <> Integer.to_string(writes)) == "v#{writes}"
      end,
      "benchmark keys should be readable after restart",
      300,
      50
    )

    IO.puts("""

      recovery bench writes=#{writes}
      write_time_ms=#{div(write_us, 1000)}
      supervised_shard_recovery_time_ms=#{div(recovery_us, 1000)}
      release_cursor_gap_before_restart=#{gap_before_restart}
      last_applied_before_restart=#{applied_before_restart}
      last_released_before_restart=#{released_before_restart}
    """)

    assert recovery_us > 0
    assert gap_before_restart > 0
    assert applied_before_restart >= released_before_restart
  end

  defp bench_writes do
    case System.get_env("FERRICSTORE_RECOVERY_BENCH_WRITES") do
      nil -> 2_000
      raw -> raw |> String.to_integer() |> max(1)
    end
  rescue
    _ -> 2_000
  end

  defp restore_release_cursor_interval(nil) do
    Application.delete_env(:ferricstore, :release_cursor_interval)
  end

  defp restore_release_cursor_interval(interval) do
    Application.put_env(:ferricstore, :release_cursor_interval, interval)
  end

  defp max_atomic(ctx, field) do
    case Map.get(ctx, field) do
      ref when is_reference(ref) ->
        size = :atomics.info(ref).size

        1..size
        |> Enum.map(&:atomics.get(ref, &1))
        |> Enum.max(fn -> 0 end)

      _ ->
        0
    end
  rescue
    _ -> 0
  end
end
