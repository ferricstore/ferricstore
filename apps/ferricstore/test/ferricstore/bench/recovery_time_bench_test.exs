defmodule Ferricstore.Bench.RecoveryTimeBenchTest do
  @moduledoc """
  Benchmarks restart recovery cost when Ra has entries beyond the last emitted
  release cursor.

  Run with:

      mix test apps/ferricstore/test/ferricstore/bench/recovery_time_bench_test.exs --include bench --timeout 180000

  Set `FERRICSTORE_RECOVERY_BENCH_WRITES=N` to change the write count.
  """

  use ExUnit.Case, async: false

  @moduletag :bench
  @moduletag timeout: 180_000

  alias Ferricstore.Store.Router
  alias Ferricstore.Test.ShardHelpers

  test "benchmark: restart recovery time with unreleased Ra entries" do
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

    data_dir = Application.fetch_env!(:ferricstore, :data_dir)
    server_started? = application_started?(:ferricstore_server)

    {restart_us, :ok} =
      :timer.tc(fn ->
        restart_ferricstore(data_dir, server_started?)
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
      restart_time_ms=#{div(restart_us, 1000)}
      release_cursor_gap_before_restart=#{gap_before_restart}
      last_applied_before_restart=#{applied_before_restart}
      last_released_before_restart=#{released_before_restart}
    """)

    assert restart_us > 0
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

  defp restart_ferricstore(data_dir, server_started?) do
    stop_app_if_started(:ferricstore_server)
    stop_app_if_started(:ferricstore)

    try do
      :ra_system.stop(Ferricstore.Raft.Cluster.system_name())
    catch
      _, _ -> :ok
    end

    Application.put_env(:ferricstore, :data_dir, data_dir)
    {:ok, _} = Application.ensure_all_started(:ferricstore)
    ShardHelpers.wait_shards_alive(30_000)

    if server_started? do
      {:ok, _} = Application.ensure_all_started(:ferricstore_server)
    end

    Ferricstore.Health.set_ready(true)
    :ok
  end

  defp application_started?(app) do
    Enum.any?(Application.started_applications(), fn {started_app, _desc, _vsn} ->
      started_app == app
    end)
  end

  defp stop_app_if_started(app) do
    if application_started?(app) do
      _ = Application.stop(app)
    end
  end
end
