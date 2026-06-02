defmodule Ferricstore.Flow.RetentionSweeperTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Flow.RetentionSweeper

  test "catch-up scheduling treats history-only cleanup at limit as backlog" do
    name = :"flow_retention_sweeper_test_#{System.unique_integer([:positive])}"
    parent = self()

    {:ok, pid} =
      RetentionSweeper.start_link(
        name: name,
        initial_delay_ms: 60_000,
        interval_ms: 60_000,
        catchup_delay_ms: 1,
        limit: 5,
        cleanup_fun: fn opts ->
          send(parent, {:cleanup_called, opts})
          {:ok, %{flows: 0, history: 5, values: 0}}
        end
      )

    on_exit(fn ->
      if Process.alive?(pid), do: GenServer.stop(pid)
    end)

    send(pid, :sweep)

    assert_receive {:cleanup_called, [limit: 5]}, 500

    info = RetentionSweeper.info(name)
    assert info.consecutive_limit_hits == 1
    assert info.last_sweep.limit_hit? == true
    assert info.last_sweep.history == 5
  end

  test "pressure mode uses pressure limit and triggers compaction" do
    name = :"flow_retention_sweeper_test_#{System.unique_integer([:positive])}"
    parent = self()

    {:ok, pid} =
      RetentionSweeper.start_link(
        name: name,
        initial_delay_ms: 60_000,
        interval_ms: 60_000,
        pressure_interval_ms: 5,
        limit: 5,
        pressure_limit: 20,
        pressure_compaction_interval_ms: 60_000,
        pressure_detector_fun: fn -> true end,
        cleanup_fun: fn opts ->
          send(parent, {:cleanup_called, opts})
          {:ok, %{flows: 1, history: 2, values: 3}}
        end,
        compaction_fun: fn ->
          send(parent, :compaction_called)
          :ok
        end
      )

    on_exit(fn ->
      if Process.alive?(pid), do: GenServer.stop(pid)
    end)

    send(pid, :sweep)

    assert_receive {:cleanup_called, [limit: 20]}, 500
    assert_receive :compaction_called, 500

    info = RetentionSweeper.info(name)
    assert info.last_sweep.pressure? == true
    assert info.last_sweep.limit == 20
    assert info.last_sweep.compaction_triggered? == true
  end

  test "pressure compaction is throttled" do
    name = :"flow_retention_sweeper_test_#{System.unique_integer([:positive])}"
    parent = self()

    {:ok, pid} =
      RetentionSweeper.start_link(
        name: name,
        initial_delay_ms: 60_000,
        interval_ms: 60_000,
        pressure_interval_ms: 5,
        limit: 5,
        pressure_limit: 20,
        pressure_compaction_interval_ms: 60_000,
        pressure_detector_fun: fn -> true end,
        cleanup_fun: fn _opts -> {:ok, %{flows: 1, history: 0, values: 0}} end,
        compaction_fun: fn ->
          send(parent, :compaction_called)
          :ok
        end
      )

    on_exit(fn ->
      if Process.alive?(pid), do: GenServer.stop(pid)
    end)

    send(pid, :sweep)
    assert_receive :compaction_called, 500
    Process.sleep(10)
    send(pid, :sweep)
    refute_receive :compaction_called, 100
  end
end
