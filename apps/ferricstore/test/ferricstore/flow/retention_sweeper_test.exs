defmodule Ferricstore.Flow.RetentionSweeperTest do
  use ExUnit.Case, async: false
  @moduletag :flow

  alias Ferricstore.Flow.RetentionSweeper

  test "test runtime keeps the application sweeper out of explicit cleanup tests" do
    assert Application.fetch_env!(:ferricstore, :flow_retention_sweeper_initial_delay_ms) ==
             86_400_000

    assert Application.fetch_env!(:ferricstore, :flow_retention_sweeper_interval_ms) ==
             86_400_000
  end

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
        pressure_detector_fun: fn -> false end,
        cleanup_fun: fn opts ->
          send(parent, {:cleanup_called, opts})
          {:ok, %{flows: 0, history: 5, values: 0, continuation: "next"}}
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

  test "catch-up scheduling continues after an underfilled cleanup pass does work" do
    name = :"flow_retention_sweeper_test_#{System.unique_integer([:positive])}"
    parent = self()
    calls = :atomics.new(1, [])

    {:ok, pid} =
      RetentionSweeper.start_link(
        name: name,
        initial_delay_ms: 60_000,
        interval_ms: 60_000,
        catchup_delay_ms: 1,
        limit: 100,
        pressure_detector_fun: fn -> false end,
        cleanup_fun: fn opts ->
          call = :atomics.add_get(calls, 1, 1)
          send(parent, {:cleanup_called, call, opts})

          if call == 1 do
            {:ok, %{flows: 0, history: 1, values: 0, continuation: "next"}}
          else
            {:ok, %{flows: 0, history: 0, values: 0, continuation: nil}}
          end
        end
      )

    on_exit(fn ->
      if Process.alive?(pid), do: GenServer.stop(pid)
    end)

    send(pid, :sweep)

    assert_receive {:cleanup_called, 1, [limit: 100]}, 500
    assert_receive {:cleanup_called, 2, [limit: 100, continuation: "next"]}, 500
  end

  test "completed pass does not schedule a final empty catch-up" do
    name = :"flow_retention_sweeper_test_#{System.unique_integer([:positive])}"
    parent = self()

    {:ok, pid} =
      RetentionSweeper.start_link(
        name: name,
        initial_delay_ms: 60_000,
        interval_ms: 60_000,
        catchup_delay_ms: 1,
        limit: 5,
        pressure_detector_fun: fn -> false end,
        cleanup_fun: fn opts ->
          send(parent, {:cleanup_called, opts})
          {:ok, %{flows: 5, history: 100, values: 100, continuation: nil}}
        end
      )

    on_exit(fn ->
      if Process.alive?(pid), do: GenServer.stop(pid)
    end)

    send(pid, :sweep)

    assert_receive {:cleanup_called, [limit: 5]}, 500
    refute_receive {:cleanup_called, _opts}, 50

    info = RetentionSweeper.info(name)
    assert info.consecutive_limit_hits == 0
    assert info.last_sweep.limit_hit? == false
    assert info.last_sweep.more? == false
  end

  test "catch-up scheduling treats active timeouts and terminal cleanup as one budget" do
    name = :"flow_retention_sweeper_test_#{System.unique_integer([:positive])}"
    parent = self()

    {:ok, pid} =
      RetentionSweeper.start_link(
        name: name,
        initial_delay_ms: 60_000,
        interval_ms: 60_000,
        catchup_delay_ms: 1,
        limit: 5,
        pressure_detector_fun: fn -> false end,
        cleanup_fun: fn opts ->
          send(parent, {:cleanup_called, opts})
          {:ok, %{flows: 2, history: 0, values: 0, active_timeouts: 3, continuation: "next"}}
        end
      )

    on_exit(fn ->
      if Process.alive?(pid), do: GenServer.stop(pid)
    end)

    send(pid, :sweep)

    assert_receive {:cleanup_called, [limit: 5]}, 500
    assert_receive {:cleanup_called, [limit: 5, continuation: "next"]}, 500

    info = RetentionSweeper.info(name)
    assert info.consecutive_limit_hits >= 1
    assert info.last_sweep.limit_hit? == true
  end

  test "sustained backlog pauses between bounded catch-up bursts" do
    name = :"flow_retention_sweeper_test_#{System.unique_integer([:positive])}"
    parent = self()

    {:ok, pid} =
      RetentionSweeper.start_link(
        name: name,
        initial_delay_ms: 60_000,
        interval_ms: 60_000,
        catchup_delay_ms: 0,
        catchup_burst_limit: 2,
        catchup_pause_ms: 100,
        limit: 5,
        pressure_detector_fun: fn -> false end,
        cleanup_fun: fn opts ->
          send(parent, {:cleanup_called, System.monotonic_time(:millisecond), opts})
          {:ok, %{flows: 1, history: 0, values: 0, continuation: "next"}}
        end
      )

    on_exit(fn ->
      if Process.alive?(pid), do: GenServer.stop(pid)
    end)

    send(pid, :sweep)

    assert_receive {:cleanup_called, _first_at, [limit: 5]}, 500

    assert_receive {:cleanup_called, second_at, [limit: 5, continuation: "next"]},
                   500

    assert_receive {:cleanup_called, third_at, [limit: 5, continuation: "next"]}, 250
    assert third_at - second_at >= 80
  end

  test "pressure mode uses pressure limit and triggers compaction" do
    name = :"flow_retention_sweeper_test_#{System.unique_integer([:positive])}"
    parent = self()

    {:ok, pid} =
      RetentionSweeper.start_link(
        name: name,
        initial_delay_ms: 60_000,
        interval_ms: 60_000,
        pressure_interval_ms: 60_000,
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
