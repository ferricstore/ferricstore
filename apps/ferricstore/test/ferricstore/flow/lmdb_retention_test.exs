defmodule Ferricstore.Flow.LMDB.RetentionTest do
  use ExUnit.Case, async: true

  alias Ferricstore.Flow.LMDB
  alias Ferricstore.Flow.LMDB.Retention

  setup do
    path =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_lmdb_retention_#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf!(path) end)
    %{path: path}
  end

  test "terminal sweep fails closed when the exact counter is corrupt", %{path: path} do
    index_key = "state:completed"
    state_key = "state-key"
    terminal_key = LMDB.terminal_index_key(index_key, "flow-1", 5)
    count_key = LMDB.terminal_count_key(index_key)
    expire_key = LMDB.terminal_expire_key(10, terminal_key)

    terminal_value =
      LMDB.encode_terminal_index_value("flow-1", 5, 10, state_key, count_key)

    expire_value = LMDB.encode_terminal_expire_value(terminal_key, state_key, count_key)

    assert :ok =
             LMDB.write_batch(path, [
               {:put, terminal_key, terminal_value},
               {:put, expire_key, expire_value},
               {:put, count_key, "corrupt"}
             ])

    assert {:error, :invalid_terminal_count_value} =
             LMDB.sweep_expired_terminal(path, 20, 10)

    assert {:ok, ^terminal_value} = LMDB.get(path, terminal_key)
    assert {:ok, ^expire_value} = LMDB.get(path, expire_key)
    assert {:ok, "corrupt"} = LMDB.get(path, count_key)
  end

  test "terminal sweep rejects key and value identity mismatches", %{path: path} do
    index_key = "state:completed"
    state_key = "state-key"
    terminal_key = LMDB.terminal_index_key(index_key, "flow-1", 5)
    count_key = LMDB.terminal_count_key(index_key)
    expire_key = LMDB.terminal_expire_key(10, terminal_key)

    terminal_value =
      LMDB.encode_terminal_index_value("other-flow", 5, 10, state_key, count_key)

    expire_value = LMDB.encode_terminal_expire_value(terminal_key, state_key, count_key)
    count_value = LMDB.encode_count(1)

    assert :ok =
             LMDB.write_batch(path, [
               {:put, terminal_key, terminal_value},
               {:put, expire_key, expire_value},
               {:put, count_key, count_value}
             ])

    assert {:error, {:invalid_terminal_index_value, ^terminal_key}} =
             LMDB.sweep_expired_terminal(path, 20, 10)

    assert {:ok, ^terminal_value} = LMDB.get(path, terminal_key)
    assert {:ok, ^expire_value} = LMDB.get(path, expire_key)
    assert {:ok, ^count_value} = LMDB.get(path, count_key)
  end

  test "terminal sweep rejects expiry markers that point at a different terminal row", %{
    path: path
  } do
    index_key = "state:completed"
    terminal_key_a = LMDB.terminal_index_key(index_key, "flow-a", 5)
    terminal_key_b = LMDB.terminal_index_key(index_key, "flow-b", 5)
    count_key = LMDB.terminal_count_key(index_key)
    state_key_b = "state-key-b"
    expire_key = LMDB.terminal_expire_key(10, terminal_key_a)

    terminal_value_b =
      LMDB.encode_terminal_index_value("flow-b", 5, 10, state_key_b, count_key)

    mismatched_expire_value =
      LMDB.encode_terminal_expire_value(terminal_key_b, state_key_b, count_key)

    count_value = LMDB.encode_count(1)

    assert :ok =
             LMDB.write_batch(path, [
               {:put, terminal_key_b, terminal_value_b},
               {:put, expire_key, mismatched_expire_value},
               {:put, count_key, count_value}
             ])

    assert {:error, {:invalid_terminal_expire_value, ^expire_key}} =
             LMDB.sweep_expired_terminal(path, 20, 10)

    assert {:ok, ^terminal_value_b} = LMDB.get(path, terminal_key_b)
    assert {:ok, ^mismatched_expire_value} = LMDB.get(path, expire_key)
    assert {:ok, ^count_value} = LMDB.get(path, count_key)
  end

  test "terminal sweep rejects expiry markers that name a different counter", %{path: path} do
    index_key = "state:completed"
    terminal_key = LMDB.terminal_index_key(index_key, "flow-1", 5)
    count_key = LMDB.terminal_count_key(index_key)
    foreign_count_key = LMDB.terminal_count_key("state:failed")
    state_key = "state-key"
    expire_key = LMDB.terminal_expire_key(10, terminal_key)

    terminal_value =
      LMDB.encode_terminal_index_value("flow-1", 5, 10, state_key, count_key)

    mismatched_expire_value =
      LMDB.encode_terminal_expire_value(terminal_key, state_key, foreign_count_key)

    count_value = LMDB.encode_count(1)
    foreign_count_value = LMDB.encode_count(5)

    assert :ok =
             LMDB.write_batch(path, [
               {:put, terminal_key, terminal_value},
               {:put, expire_key, mismatched_expire_value},
               {:put, count_key, count_value},
               {:put, foreign_count_key, foreign_count_value}
             ])

    assert {:error, {:invalid_terminal_expire_value, ^expire_key}} =
             LMDB.sweep_expired_terminal(path, 20, 10)

    assert {:ok, ^terminal_value} = LMDB.get(path, terminal_key)
    assert {:ok, ^mismatched_expire_value} = LMDB.get(path, expire_key)
    assert {:ok, ^count_value} = LMDB.get(path, count_key)
    assert {:ok, ^foreign_count_value} = LMDB.get(path, foreign_count_key)
  end

  test "terminal sweep write plans abort when the counter changes after planning", %{path: path} do
    index_key = "state:completed"
    terminal_key = LMDB.terminal_index_key(index_key, "flow-1", 5)
    count_key = LMDB.terminal_count_key(index_key)
    state_key = "state-key"
    expire_key = LMDB.terminal_expire_key(10, terminal_key)

    terminal_value =
      LMDB.encode_terminal_index_value("flow-1", 5, 10, state_key, count_key)

    expire_value = LMDB.encode_terminal_expire_value(terminal_key, state_key, count_key)
    count_value = LMDB.encode_count(1)

    assert :ok =
             LMDB.write_batch(path, [
               {:put, terminal_key, terminal_value},
               {:put, expire_key, expire_value},
               {:put, count_key, count_value}
             ])

    assert {:ok, entries} = LMDB.prefix_entries(path, LMDB.terminal_expire_prefix(), 10)

    assert {:ok, write_ops, 1} =
             Retention.__terminal_sweep_write_plan_for_test__(path, entries, 20)

    replacement_count = LMDB.encode_count(2)
    assert :ok = LMDB.write_batch(path, [{:put, count_key, replacement_count}])

    assert {:error, {:compare_failed, ^count_key}} = LMDB.write_batch(path, write_ops)
    assert {:ok, ^terminal_value} = LMDB.get(path, terminal_key)
    assert {:ok, ^expire_value} = LMDB.get(path, expire_key)
    assert {:ok, ^replacement_count} = LMDB.get(path, count_key)
  end

  test "terminal sweep plans preserve a concurrently replaced reverse owner", %{path: path} do
    index_key = "state:completed"
    terminal_key = LMDB.terminal_index_key(index_key, "flow-1", 5)
    replacement_terminal_key = LMDB.terminal_index_key("state:failed", "flow-1", 20)
    count_key = LMDB.terminal_count_key(index_key)
    state_key = "state-key"
    reverse_key = LMDB.terminal_by_state_key_key(state_key)
    expire_key = LMDB.terminal_expire_key(10, terminal_key)

    terminal_value =
      LMDB.encode_terminal_index_value("flow-1", 5, 10, state_key, count_key)

    expire_value = LMDB.encode_terminal_expire_value(terminal_key, state_key, count_key)
    count_value = LMDB.encode_count(1)

    assert :ok =
             LMDB.write_batch(path, [
               {:put, terminal_key, terminal_value},
               {:put, reverse_key, terminal_key},
               {:put, expire_key, expire_value},
               {:put, count_key, count_value}
             ])

    assert {:ok, entries} = LMDB.prefix_entries(path, LMDB.terminal_expire_prefix(), 10)

    assert {:ok, write_ops, 1} =
             Retention.__terminal_sweep_write_plan_for_test__(path, entries, 20)

    assert {:compare, reverse_key, terminal_key} in write_ops
    assert :ok = LMDB.write_batch(path, [{:put, reverse_key, replacement_terminal_key}])

    assert {:error, {:compare_failed, ^reverse_key}} = LMDB.write_batch(path, write_ops)
    assert {:ok, ^terminal_value} = LMDB.get(path, terminal_key)
    assert {:ok, ^replacement_terminal_key} = LMDB.get(path, reverse_key)
    assert {:ok, ^expire_value} = LMDB.get(path, expire_key)
    assert {:ok, ^count_value} = LMDB.get(path, count_key)
  end

  test "history sweep preserves markers when the target row is corrupt", %{path: path} do
    history_key = "history-key"
    history_index_key = LMDB.history_index_key(history_key, "event-1", 5)
    expire_key = LMDB.history_expire_key(10, history_index_key)
    expire_value = LMDB.encode_history_expire_value(history_index_key)

    assert :ok =
             LMDB.write_batch(path, [
               {:put, history_index_key, "corrupt"},
               {:put, expire_key, expire_value}
             ])

    assert {:error, {:invalid_history_index_value, ^history_index_key}} =
             LMDB.sweep_expired_history(path, 20, 10)

    assert {:ok, "corrupt"} = LMDB.get(path, history_index_key)
    assert {:ok, ^expire_value} = LMDB.get(path, expire_key)
  end

  test "history sweep rejects key and value identity mismatches", %{path: path} do
    history_key = "history-key"
    history_index_key = LMDB.history_index_key(history_key, "event-1", 5)
    expire_key = LMDB.history_expire_key(10, history_index_key)
    history_value = LMDB.encode_history_index_value("event-2", 5, "compound-key", 10)
    expire_value = LMDB.encode_history_expire_value(history_index_key)

    assert :ok =
             LMDB.write_batch(path, [
               {:put, history_index_key, history_value},
               {:put, expire_key, expire_value}
             ])

    assert {:error, {:invalid_history_index_value, ^history_index_key}} =
             LMDB.sweep_expired_history(path, 20, 10)

    assert {:ok, ^history_value} = LMDB.get(path, history_index_key)
    assert {:ok, ^expire_value} = LMDB.get(path, expire_key)
  end

  test "history sweep rejects expiry markers that point at a different history row", %{
    path: path
  } do
    history_key = "history-key"
    history_index_key_a = LMDB.history_index_key(history_key, "event-a", 5)
    history_index_key_b = LMDB.history_index_key(history_key, "event-b", 5)
    expire_key = LMDB.history_expire_key(10, history_index_key_a)
    history_value_b = LMDB.encode_history_index_value("event-b", 5, "compound-key", 10)
    mismatched_expire_value = LMDB.encode_history_expire_value(history_index_key_b)

    assert :ok =
             LMDB.write_batch(path, [
               {:put, history_index_key_b, history_value_b},
               {:put, expire_key, mismatched_expire_value}
             ])

    assert {:error, {:invalid_history_expire_value, ^expire_key}} =
             LMDB.sweep_expired_history(path, 20, 10)

    assert {:ok, ^history_value_b} = LMDB.get(path, history_index_key_b)
    assert {:ok, ^mismatched_expire_value} = LMDB.get(path, expire_key)
  end

  test "history sweep write plans abort when the row changes after planning", %{path: path} do
    history_key = "history-key"
    history_index_key = LMDB.history_index_key(history_key, "event-1", 5)
    expire_key = LMDB.history_expire_key(10, history_index_key)
    history_value = LMDB.encode_history_index_value("event-1", 5, "compound-a", 10)
    expire_value = LMDB.encode_history_expire_value(history_index_key)

    assert :ok =
             LMDB.write_batch(path, [
               {:put, history_index_key, history_value},
               {:put, expire_key, expire_value}
             ])

    assert {:ok, entries} = LMDB.prefix_entries(path, LMDB.history_expire_prefix(), 10)

    assert {:ok, write_ops, 1} =
             Retention.__history_sweep_write_plan_for_test__(path, entries, 20)

    replacement_value = LMDB.encode_history_index_value("event-1", 5, "compound-b", 10)
    assert :ok = LMDB.write_batch(path, [{:put, history_index_key, replacement_value}])

    assert {:error, {:compare_failed, ^history_index_key}} =
             LMDB.write_batch(path, write_ops)

    assert {:ok, ^replacement_value} = LMDB.get(path, history_index_key)
    assert {:ok, ^expire_value} = LMDB.get(path, expire_key)
  end

  test "active timeout scans fail closed on corrupt ordered rows", %{path: path} do
    index_key = Ferricstore.Flow.Keys.active_timeout_index_key()
    active_key = LMDB.active_index_key(index_key, "flow-1", 5)

    assert :ok = LMDB.write_batch(path, [{:put, active_key, "corrupt"}])

    assert {:error, {:invalid_active_timeout_index_value, ^active_key}} =
             LMDB.expired_active_timeout_state_keys(path, 20, 10)
  end

  test "active timeout scans reject key and value deadline mismatches", %{path: path} do
    index_key = Ferricstore.Flow.Keys.active_timeout_index_key()
    active_key = LMDB.active_index_key(index_key, "flow-1", 5)
    active_value = LMDB.encode_active_index_value(index_key, "flow-1", 50, 0, "state-key")

    assert :ok = LMDB.write_batch(path, [{:put, active_key, active_value}])

    assert {:error, {:invalid_active_timeout_index_value, ^active_key}} =
             LMDB.expired_active_timeout_state_keys(path, 20, 10)
  end

  test "terminal state-key discovery reports corrupt terminal rows", %{path: path} do
    index_key = "state:completed"
    state_key = "state-key"
    terminal_key = LMDB.terminal_index_key(index_key, "flow-1", 5)
    count_key = LMDB.terminal_count_key(index_key)
    expire_key = LMDB.terminal_expire_key(10, terminal_key)
    expire_value = LMDB.encode_terminal_expire_value(terminal_key, state_key, count_key)

    assert :ok =
             LMDB.write_batch(path, [
               {:put, terminal_key, "corrupt"},
               {:put, expire_key, expire_value}
             ])

    assert {:error, {:invalid_terminal_index_value, ^terminal_key}} =
             LMDB.expired_terminal_state_keys(path, 20, 10)
  end

  test "flow-wide history expiry preserves its marker when any index row is corrupt", %{
    path: path
  } do
    history_key = "history-key"
    history_index_key = LMDB.history_index_key(history_key, "event-1", 5)
    flow_expire_key = LMDB.history_flow_expire_key(10, history_key)
    flow_expire_value = LMDB.encode_history_flow_expire_value(history_key, 10)

    assert :ok =
             LMDB.write_batch(path, [
               {:put, history_index_key, "corrupt"},
               {:put, flow_expire_key, flow_expire_value}
             ])

    assert {:error, {:invalid_history_index_value, ^history_index_key}} =
             LMDB.sweep_expired_history(path, 20, 10)

    assert {:ok, ^flow_expire_value} = LMDB.get(path, flow_expire_key)
  end

  test "sweeps preserve malformed expiry keys instead of losing cleanup pointers", %{path: path} do
    terminal_key = LMDB.terminal_expire_prefix() <> "malformed"
    history_key = LMDB.history_expire_prefix() <> "malformed"
    history_flow_key = LMDB.history_flow_expire_prefix() <> "malformed"

    assert :ok =
             LMDB.write_batch(path, [
               {:put, terminal_key, "terminal-marker"},
               {:put, history_key, "history-marker"},
               {:put, history_flow_key, "history-flow-marker"}
             ])

    assert {:error, {:invalid_terminal_expire_key, ^terminal_key}} =
             LMDB.sweep_expired_terminal(path, 20, 10)

    assert {:error, {:invalid_history_expire_key, ^history_key}} =
             LMDB.sweep_expired_history(path, 20, 10)

    assert {:ok, "terminal-marker"} = LMDB.get(path, terminal_key)
    assert {:ok, "history-marker"} = LMDB.get(path, history_key)
    assert {:ok, "history-flow-marker"} = LMDB.get(path, history_flow_key)

    assert :ok = LMDB.write_batch(path, [{:delete, history_key}])

    assert {:error, {:invalid_history_flow_expire_key, ^history_flow_key}} =
             LMDB.sweep_expired_history(path, 20, 10)

    assert {:ok, "history-flow-marker"} = LMDB.get(path, history_flow_key)
  end

  test "history sweep rejects a non-canonical expiry separator before deleting data", %{
    path: path
  } do
    history_key = "history-key"
    history_index_key = LMDB.history_index_key(history_key, "event-1", 5)

    history_value =
      LMDB.encode_history_index_value("event-1", 5, "compound-key", 10)

    malformed_expire_key =
      LMDB.history_expire_prefix() <>
        String.pad_leading("10", 20, "0") <> "X" <> history_index_key

    expire_value = LMDB.encode_history_expire_value(history_index_key)

    assert :ok =
             LMDB.write_batch(path, [
               {:put, history_index_key, history_value},
               {:put, malformed_expire_key, expire_value}
             ])

    assert {:error, {:invalid_history_expire_key, ^malformed_expire_key}} =
             LMDB.sweep_expired_history(path, 20, 10)

    assert {:ok, ^history_value} = LMDB.get(path, history_index_key)
    assert {:ok, ^expire_value} = LMDB.get(path, malformed_expire_key)
  end

  test "flow-wide history sweep requires the marker cutoff to match its ordered key", %{
    path: path
  } do
    history_key = "history-key"
    history_index_key = LMDB.history_index_key(history_key, "50-1", 50)
    history_value = LMDB.encode_history_index_value("50-1", 50, "compound-key", 0)
    flow_expire_key = LMDB.history_flow_expire_key(10, history_key)
    mismatched_value = LMDB.encode_history_flow_expire_value(history_key, 100)

    assert :ok =
             LMDB.write_batch(path, [
               {:put, history_index_key, history_value},
               {:put, flow_expire_key, mismatched_value}
             ])

    assert {:error, {:invalid_history_flow_expire_value, ^flow_expire_key}} =
             LMDB.sweep_expired_history(path, 20, 10)

    assert {:ok, ^history_value} = LMDB.get(path, history_index_key)
    assert {:ok, ^mismatched_value} = LMDB.get(path, flow_expire_key)
  end

  test "flow-wide history sweep requires the marker key and value to name the same history", %{
    path: path
  } do
    history_index_key = LMDB.history_index_key("history-b", "5-1", 5)
    history_value = LMDB.encode_history_index_value("5-1", 5, "compound-key", 0)
    flow_expire_key = LMDB.history_flow_expire_key(10, "history-a")
    mismatched_value = LMDB.encode_history_flow_expire_value("history-b", 10)

    assert :ok =
             LMDB.write_batch(path, [
               {:put, history_index_key, history_value},
               {:put, flow_expire_key, mismatched_value}
             ])

    assert {:error, {:invalid_history_flow_expire_value, ^flow_expire_key}} =
             LMDB.sweep_expired_history(path, 20, 10)

    assert {:ok, ^history_value} = LMDB.get(path, history_index_key)
    assert {:ok, ^mismatched_value} = LMDB.get(path, flow_expire_key)
  end

  test "flow-wide history sweep shares one total event budget across markers", %{path: path} do
    ops =
      for flow_id <- 1..2,
          event_ms <- 1..3,
          reduce: [] do
        acc ->
          history_key = "history-#{flow_id}"
          history_index_key = LMDB.history_index_key(history_key, "#{event_ms}-1", event_ms)

          [
            {:put, history_index_key,
             LMDB.encode_history_index_value(
               "#{event_ms}-1",
               event_ms,
               "compound-#{flow_id}-#{event_ms}",
               0
             )}
            | acc
          ]
      end

    flow_marker_ops =
      for flow_id <- 1..2 do
        history_key = "history-#{flow_id}"

        {:put, LMDB.history_flow_expire_key(10, history_key),
         LMDB.encode_history_flow_expire_value(history_key, 10)}
      end

    assert :ok = LMDB.write_batch(path, flow_marker_ops ++ ops)
    assert {:ok, 2} = LMDB.sweep_expired_history(path, 20, 2)

    remaining =
      Enum.reduce(1..2, 0, fn flow_id, count ->
        {:ok, flow_count} =
          LMDB.prefix_count(path, LMDB.history_index_prefix("history-#{flow_id}"))

        count + flow_count
      end)

    assert remaining == 4
  end
end
