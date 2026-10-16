defmodule Ferricstore.Flow.LMDBWriter.QuarantineTest do
  # Changes :flow_lmdb_quarantine_retry_ms for one test.
  use ExUnit.Case, async: false

  alias Ferricstore.Flow.LMDBWriter.Quarantine

  test "only failures of one Flow's own data are isolatable" do
    assert Quarantine.isolatable?(:invalid_source_flow_record)
    assert Quarantine.isolatable?({:compare_failed, "flow-active-by-state:k"})
    assert Quarantine.isolatable?({:invalid_terminal_reverse_read, :bad})

    refute Quarantine.isolatable?(:lmdb_flush_coordinator_unavailable)
    refute Quarantine.isolatable?({:source_pending, "k"})
    refute Quarantine.isolatable?({:source_location_unavailable, "k"})
    refute Quarantine.isolatable?({:lmdb_env_busy, 1})
    refute Quarantine.isolatable?("Input/output error (os error 5)")
    refute Quarantine.isolatable?({:throw, :boom})
  end

  test "maps source projection ops and after-flush actions to their Flow state key" do
    assert {:ok, "s"} = Quarantine.op_state_key({:project_flow_state_from_source, "s"})
    assert {:ok, "s"} = Quarantine.op_state_key({:project_flow_state_from_source, "s", 3})
    assert {:ok, "s"} = Quarantine.op_state_key({:project_flow_query_state_from_source, "s", 3})
    assert :error = Quarantine.op_state_key({:put, "s", "v"})
    assert :error = Quarantine.op_state_key({:project_kv_from_source, "s"})

    assert {:ok, "s"} = Quarantine.action_state_key(prune("s"))
    assert {:ok, "s"} = Quarantine.action_state_key({:defer_after_flush, 10, prune("s")})

    assert {:ok, "s"} =
             Quarantine.action_state_key({:hibernate_flow_evict_hot, %{state_key: "s"}})

    assert {:ok, "s"} = Quarantine.action_state_key({:delete_flow_tombstone, :ets, "s"})
    assert :error = Quarantine.action_state_key({:cleanup_stale_cold_due, "path", []})
  end

  test "a held Flow's later ops and actions wait with it; other Flows pass through" do
    {state, :new} =
      Quarantine.hold(%{}, "held", {:project_flow_state_from_source, "held", 1}, :bad, 0)

    ops = [
      {:project_flow_state_from_source, "other", 1},
      {:project_flow_state_from_source, "held", 2},
      {:put, "raw", "v"},
      {:project_flow_query_state_from_source, "held", 3},
      {:project_flow_state_from_source, "held", 4}
    ]

    actions = [prune("held"), prune("other"), {:defer_after_flush, 5, prune("held")}]

    {state, kept_ops, kept_actions} = Quarantine.divert(state, ops, actions)

    assert kept_ops == [{:project_flow_state_from_source, "other", 1}, {:put, "raw", "v"}]
    assert kept_actions == [prune("other")]

    entry = Quarantine.entries(state)["held"]

    # One op per kind is enough: projections re-read the current record.
    assert entry.ops == [
             {:project_flow_query_state_from_source, "held", 3},
             {:project_flow_state_from_source, "held", 4}
           ]

    assert entry.after_flush == [prune("held"), {:defer_after_flush, 5, prune("held")}]
    assert Quarantine.reply(state) == {:error, {:flow_projection_quarantined, 1}}
  end

  test "held projections are retried with backoff and released" do
    previous = Application.get_env(:ferricstore, :flow_lmdb_quarantine_retry_ms)
    Application.put_env(:ferricstore, :flow_lmdb_quarantine_retry_ms, 100)

    on_exit(fn ->
      case previous do
        nil -> Application.delete_env(:ferricstore, :flow_lmdb_quarantine_retry_ms)
        value -> Application.put_env(:ferricstore, :flow_lmdb_quarantine_retry_ms, value)
      end
    end)

    {state, :new} = Quarantine.hold(%{}, "k", {:project_flow_state_from_source, "k"}, :bad, 1_000)

    assert Quarantine.due(state, 1_099) == []
    assert Quarantine.next_retry_in_ms(state, 1_050) == 50
    assert [{"k", _entry}] = Quarantine.due(state, 1_100)

    state = Quarantine.back_off(state, "k", :still_bad, 1_100)
    assert Quarantine.entries(state)["k"].next_retry_ms == 1_300
    assert Quarantine.entries(state)["k"].reason == :still_bad

    state = Quarantine.release(state, "k")
    assert Quarantine.empty?(state)
    assert Quarantine.reply(state) == :ok
    assert Quarantine.next_retry_in_ms(state, 0) == nil
  end

  defp prune(state_key),
    do:
      {:prune_terminal_flow, "data", 0, :ets, :zi, :zl, :fi, :fl, state_key, "type", "completed",
       nil, nil, nil, nil, "id", 1, 0}
end
