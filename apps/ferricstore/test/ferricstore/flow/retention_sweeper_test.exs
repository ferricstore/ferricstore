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
end
