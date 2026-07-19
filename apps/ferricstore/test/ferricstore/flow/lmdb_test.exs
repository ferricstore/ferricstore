defmodule Ferricstore.Flow.LMDBUnitTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Flow.LMDB
  alias Ferricstore.Flow.LMDB.Access

  setup do
    previous = Application.get_env(:ferricstore, :flow_lmdb_map_size)

    on_exit(fn ->
      case previous do
        nil -> Application.delete_env(:ferricstore, :flow_lmdb_map_size)
        value -> Application.put_env(:ferricstore, :flow_lmdb_map_size, value)
      end
    end)
  end

  test "flush marker reads fail safe on storage errors and invalid replies" do
    assert LMDB.__normalize_flush_marker_read_for_test__({:ok, <<1>>})
    refute LMDB.__normalize_flush_marker_read_for_test__(:not_found)
    assert LMDB.__normalize_flush_marker_read_for_test__({:error, :busy})
    assert LMDB.__normalize_flush_marker_read_for_test__(:invalid)
  end

  test "active reverse delete planning preserves corruption and read failures" do
    state_key = "state-key"
    reverse_key = LMDB.active_by_state_key_key(state_key)
    active_key = LMDB.active_index_key("index-key", "flow-id", 10)
    reverse_value = LMDB.encode_active_index_reverse_value([active_key])

    assert {:ok, [{:compare_missing, ^reverse_key}]} =
             LMDB.__active_index_delete_ops_result_for_test__(state_key, :not_found)

    assert {:ok,
            [
              {:compare, ^reverse_key, ^reverse_value},
              {:delete, ^reverse_key},
              {:delete, ^active_key}
            ]} =
             LMDB.__active_index_delete_ops_result_for_test__(state_key, {:ok, reverse_value})

    assert {:error, :invalid_active_index_reverse} =
             LMDB.__active_index_delete_ops_result_for_test__(state_key, {:ok, "corrupt"})

    assert {:error, :busy} =
             LMDB.__active_index_delete_ops_result_for_test__(state_key, {:error, :busy})
  end

  test "active reverse delete planning rejects rows owned by another state" do
    path =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_lmdb_active_owner_#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf!(path) end)

    state_key = "state-a"
    foreign_state_key = "state-b"
    active_key = LMDB.active_index_key("index-key", "flow-b", 10)

    active_value =
      LMDB.encode_active_index_value(
        "index-key",
        "flow-b",
        10,
        0,
        foreign_state_key
      )

    reverse_key = LMDB.active_by_state_key_key(state_key)
    reverse_value = LMDB.encode_active_index_reverse_value([active_key])

    assert :ok =
             LMDB.write_batch(path, [
               {:put, reverse_key, reverse_value},
               {:put, active_key, active_value}
             ])

    assert {:error, {:active_index_reverse_state_mismatch, ^active_key}} =
             LMDB.active_index_delete_ops_result(path, state_key)

    assert {:ok, ^reverse_value} = LMDB.get(path, reverse_key)
    assert {:ok, ^active_value} = LMDB.get(path, active_key)
  end

  test "active index projection rejects non-canonical update timestamps" do
    base_record = %{
      id: "flow-id",
      type: "job",
      state: "queued",
      updated_at_ms: 10
    }

    for invalid <- ["10", 10.5, -1, nil] do
      assert_raise ArgumentError, ~r/active index score/, fn ->
        base_record
        |> Map.put(:updated_at_ms, invalid)
        |> then(&LMDB.active_index_put_ops_with_reverse("state-key", &1, 0))
      end
    end

    assert_raise ArgumentError, ~r/active index score/, fn ->
      base_record
      |> Map.delete(:updated_at_ms)
      |> then(&LMDB.active_index_put_ops_with_reverse("state-key", &1, 0))
    end
  end

  test "active projection includes due-any only when that index is enabled" do
    record = %{
      id: "flow-id",
      type: "job",
      state: "queued",
      updated_at_ms: 10,
      next_run_at_ms: 20,
      priority: 1,
      partition_key: "tenant-a"
    }

    due_any_entry =
      {Ferricstore.Flow.Keys.due_any_key("job", 1, "tenant-a"), "flow-id", 20}

    refute due_any_entry in LMDB.active_projection_entries(record, due_any?: false)
    assert due_any_entry in LMDB.active_projection_entries(record, due_any?: true)
  end

  test "LMDB map sizes remain valid when mutable configuration is malformed" do
    Application.put_env(:ferricstore, :flow_lmdb_map_size, "huge")
    assert LMDB.map_size() == 16 * 1024 * 1024 * 1024
    assert Access.map_size() == LMDB.map_size()

    Application.put_env(:ferricstore, :flow_lmdb_map_size, 0)
    assert LMDB.map_size() == 16 * 1024 * 1024 * 1024
    assert Access.map_size() == LMDB.map_size()

    Application.put_env(:ferricstore, :flow_lmdb_map_size, 18_446_744_073_709_551_616)
    assert LMDB.map_size() == 16 * 1024 * 1024 * 1024
    assert Access.map_size() == LMDB.map_size()

    Application.put_env(:ferricstore, :flow_lmdb_map_size, 64 * 1024 * 1024)
    assert LMDB.map_size() == 64 * 1024 * 1024
    assert Access.map_size() == LMDB.map_size()
  end

  test "LMDB environment discovery rejects symlinked directories and database files" do
    root =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_lmdb_env_present_#{System.unique_integer([:positive])}"
      )

    real_env = Path.join(root, "real")
    linked_env = Path.join(root, "linked")
    file_link_env = Path.join(root, "file-link")
    outside_data = Path.join(root, "outside-data.mdb")

    File.mkdir_p!(real_env)
    File.mkdir_p!(file_link_env)
    File.write!(Path.join(real_env, "data.mdb"), "data")
    File.write!(outside_data, "data")
    File.ln_s!(real_env, linked_env)
    File.ln_s!(outside_data, Path.join(file_link_env, "data.mdb"))
    on_exit(fn -> File.rm_rf!(root) end)

    assert LMDB.env_present?(real_env)
    refute LMDB.env_present?(linked_env)
    refute LMDB.env_present?(file_link_env)
  end

  test "history index delete planning preserves metadata read and decode failures" do
    history_key = LMDB.history_index_key("history-owner", "event-1", 10)

    assert {:ok, [{:compare_missing, ^history_key}]} =
             LMDB.__history_index_delete_ops_result_for_test__(history_key, :not_found)

    assert {:error, :busy} =
             LMDB.__history_index_delete_ops_result_for_test__(history_key, {:error, :busy})

    assert {:error, :invalid_history_index_value} =
             LMDB.__history_index_delete_ops_result_for_test__(history_key, {:ok, "corrupt"})

    value = LMDB.encode_history_index_value("event-1", 10, "compound-key", 20)
    expire_key = LMDB.history_expire_key(20, history_key)

    assert {:ok,
            [
              {:compare, ^history_key, ^value},
              {:compare, ^expire_key, _expire_value},
              {:delete, ^expire_key},
              {:delete, ^history_key}
            ]} =
             LMDB.__history_index_delete_ops_result_for_test__(history_key, {:ok, value})

    mismatched = LMDB.encode_history_index_value("event-2", 10, "compound-key", 20)

    assert {:error, :invalid_history_index_value} =
             LMDB.__history_index_delete_ops_result_for_test__(history_key, {:ok, mismatched})
  end

  test "LMDB exposes only result-bearing current deletion and history read contracts" do
    refute function_exported?(LMDB, :active_index_delete_ops, 2)
    refute function_exported?(LMDB, :active_index_delete_ops_from_reverse, 2)
    refute function_exported?(LMDB, :history_index_delete_ops, 2)
    refute function_exported?(LMDB, :terminal_index_delete_ops, 3)
    refute function_exported?(LMDB, :history_compound_entries, 3)
    refute function_exported?(LMDB, :history_compound_location_entries, 3)
    refute function_exported?(LMDB, :active_timeout_index_put_ops, 4)
    assert function_exported?(LMDB, :active_timeout_index_put_ops, 3)
  end

  test "terminal index delete planning requires a readable exact counter" do
    terminal_key = LMDB.terminal_index_key("state-index", "flow-id", 10)
    state_key = "state-key"
    count_key = "count-key"

    terminal_value =
      LMDB.encode_terminal_index_value("flow-id", 10, 20, state_key, count_key)

    count_value = LMDB.encode_count(3)
    expire_key = LMDB.terminal_expire_key(20, terminal_key)
    reverse_key = LMDB.terminal_by_state_key_key(state_key)

    assert {:ok, ops} =
             LMDB.__terminal_index_delete_ops_result_for_test__(
               terminal_key,
               state_key,
               {:ok, terminal_value},
               {:ok, count_value}
             )

    assert {:put, count_key, LMDB.encode_count(2)} in ops
    assert {:compare, terminal_key, terminal_value} in ops
    assert {:compare, count_key, count_value} in ops

    assert {:compare, expire_key,
            LMDB.encode_terminal_expire_value(terminal_key, state_key, count_key)} in ops

    assert {:delete, expire_key} in ops
    assert {:delete, terminal_key} in ops
    assert {:delete, reverse_key} in ops

    assert {:error, :busy} =
             LMDB.__terminal_index_delete_ops_result_for_test__(
               terminal_key,
               state_key,
               {:ok, terminal_value},
               {:error, :busy}
             )

    assert {:error, :invalid_terminal_index_value} =
             LMDB.__terminal_index_delete_ops_result_for_test__(
               terminal_key,
               state_key,
               {:ok, "corrupt"},
               :not_found
             )

    mismatched =
      LMDB.encode_terminal_index_value("other-flow", 10, 20, state_key, count_key)

    assert {:error, :invalid_terminal_index_value} =
             LMDB.__terminal_index_delete_ops_result_for_test__(
               terminal_key,
               state_key,
               {:ok, mismatched},
               {:ok, count_value}
             )
  end

  test "state artifact terminal reverse reads preserve storage failures" do
    assert {:ok, nil} = LMDB.__terminal_reverse_read_for_test__(:not_found)
    assert {:ok, "terminal-key"} = LMDB.__terminal_reverse_read_for_test__({:ok, "terminal-key"})
    assert {:error, :busy} = LMDB.__terminal_reverse_read_for_test__({:error, :busy})

    assert {:error, :invalid_terminal_reverse_read} =
             LMDB.__terminal_reverse_read_for_test__({:ok, 123})
  end

  test "state artifact deletion preserves state when its terminal reverse is dangling" do
    path =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_lmdb_state_delete_#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf!(path) end)

    state_key = "state-key"
    reverse_key = LMDB.terminal_by_state_key_key(state_key)
    terminal_key = "missing-terminal-key"

    assert :ok =
             LMDB.write_batch(path, [
               {:put, state_key, "state-value"},
               {:put, reverse_key, terminal_key}
             ])

    assert {:error, :missing_terminal_index} = LMDB.delete_state_artifacts(path, state_key)
    assert {:ok, "state-value"} = LMDB.get(path, state_key)
    assert {:ok, ^terminal_key} = LMDB.get(path, reverse_key)
  end

  test "deletion plans abort when indexed metadata changes after planning" do
    path =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_lmdb_delete_cas_#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf!(path) end)

    active_state_key = "active-state"
    active_key = LMDB.active_index_key("active-index", "flow-a", 10)

    active_value =
      LMDB.encode_active_index_value("active-index", "flow-a", 10, 0, active_state_key)

    active_reverse_key = LMDB.active_by_state_key_key(active_state_key)
    active_reverse = LMDB.encode_active_index_reverse_value([active_key])

    assert :ok =
             LMDB.write_batch(path, [
               {:put, active_reverse_key, active_reverse},
               {:put, active_key, active_value}
             ])

    assert {:ok, active_delete_ops} =
             LMDB.active_index_delete_ops_result(path, active_state_key)

    replacement_active_key = LMDB.active_index_key("active-index", "flow-a", 11)
    replacement_reverse = LMDB.encode_active_index_reverse_value([replacement_active_key])
    assert :ok = LMDB.write_batch(path, [{:put, active_reverse_key, replacement_reverse}])

    assert {:error, {:compare_failed, ^active_reverse_key}} =
             LMDB.write_batch(path, active_delete_ops)

    assert {:ok, ^replacement_reverse} = LMDB.get(path, active_reverse_key)
    assert {:ok, ^active_value} = LMDB.get(path, active_key)

    history_key = LMDB.history_index_key("history-owner", "event-1", 10)
    history_value = LMDB.encode_history_index_value("event-1", 10, "compound-a", 20)
    assert :ok = LMDB.write_batch(path, [{:put, history_key, history_value}])
    assert {:ok, history_delete_ops} = LMDB.history_index_delete_ops_result(path, history_key)

    replacement_history_value =
      LMDB.encode_history_index_value("event-1", 10, "compound-b", 20)

    assert :ok = LMDB.write_batch(path, [{:put, history_key, replacement_history_value}])

    assert {:error, {:compare_failed, ^history_key}} =
             LMDB.write_batch(path, history_delete_ops)

    assert {:ok, ^replacement_history_value} = LMDB.get(path, history_key)

    terminal_state_key = "terminal-state"
    terminal_key = LMDB.terminal_index_key("terminal-index", "flow-t", 10)
    count_key = LMDB.terminal_count_key("terminal-index")
    count_value = LMDB.encode_count(1)

    terminal_value =
      LMDB.encode_terminal_index_value("flow-t", 10, 20, terminal_state_key, count_key)

    assert :ok =
             LMDB.write_batch(path, [
               {:put, terminal_key, terminal_value},
               {:put, count_key, count_value}
             ])

    assert {:ok, terminal_delete_ops} =
             LMDB.terminal_index_delete_ops_result(path, terminal_key, terminal_state_key)

    replacement_count = LMDB.encode_count(2)
    assert :ok = LMDB.write_batch(path, [{:put, count_key, replacement_count}])

    assert {:error, {:compare_failed, ^count_key}} =
             LMDB.write_batch(path, terminal_delete_ops)

    assert {:ok, ^terminal_value} = LMDB.get(path, terminal_key)
    assert {:ok, ^replacement_count} = LMDB.get(path, count_key)
  end

  test "terminal deletion plans preserve a concurrently replaced reverse owner" do
    path =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_lmdb_terminal_reverse_cas_#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf!(path) end)

    state_key = "terminal-state"
    reverse_key = LMDB.terminal_by_state_key_key(state_key)
    terminal_key = LMDB.terminal_index_key("terminal-index", "flow-t", 10)
    replacement_terminal_key = LMDB.terminal_index_key("failed-index", "flow-t", 20)
    count_key = LMDB.terminal_count_key("terminal-index")
    count_value = LMDB.encode_count(1)

    terminal_value =
      LMDB.encode_terminal_index_value("flow-t", 10, 0, state_key, count_key)

    assert :ok =
             LMDB.write_batch(path, [
               {:put, terminal_key, terminal_value},
               {:put, reverse_key, terminal_key},
               {:put, count_key, count_value}
             ])

    assert {:ok, delete_ops} =
             LMDB.terminal_index_delete_ops_result(path, terminal_key, state_key)

    assert {:compare, reverse_key, terminal_key} in delete_ops
    assert :ok = LMDB.write_batch(path, [{:put, reverse_key, replacement_terminal_key}])

    assert {:error, {:compare_failed, ^reverse_key}} = LMDB.write_batch(path, delete_ops)
    assert {:ok, ^terminal_value} = LMDB.get(path, terminal_key)
    assert {:ok, ^replacement_terminal_key} = LMDB.get(path, reverse_key)
    assert {:ok, ^count_value} = LMDB.get(path, count_key)
  end

  test "missing terminal deletion plans do not erase current state ownership" do
    path =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_lmdb_missing_terminal_owner_#{System.unique_integer([:positive])}"
      )

    on_exit(fn -> File.rm_rf!(path) end)

    state_key = "terminal-state"
    state_value = "current-state"
    reverse_key = LMDB.terminal_by_state_key_key(state_key)
    missing_terminal_key = LMDB.terminal_index_key("completed-index", "flow-t", 10)
    current_terminal_key = LMDB.terminal_index_key("failed-index", "flow-t", 20)

    assert :ok =
             LMDB.write_batch(path, [
               {:put, state_key, state_value},
               {:put, reverse_key, current_terminal_key}
             ])

    assert {:ok, delete_ops} =
             LMDB.terminal_index_delete_ops_result(path, missing_terminal_key, state_key)

    assert delete_ops == [{:compare_missing, missing_terminal_key}]
    assert :ok = LMDB.write_batch(path, delete_ops)
    assert {:ok, ^state_value} = LMDB.get(path, state_key)
    assert {:ok, ^current_terminal_key} = LMDB.get(path, reverse_key)
  end

  test "prefix page reduction consumes every page without an item ceiling" do
    scan_fun = fn
      nil -> {:ok, [{"flow:a", "1"}, {"flow:b", "2"}]}
      "flow:b" -> {:ok, [{"flow:c", "3"}]}
    end

    reduce_fun = fn entries, acc ->
      {:ok, acc ++ Enum.map(entries, fn {key, _value} -> key end)}
    end

    assert {:ok, ["flow:a", "flow:b", "flow:c"]} =
             LMDB.__reduce_prefix_pages_for_test__(
               "flow:",
               2,
               [],
               scan_fun,
               reduce_fun
             )
  end

  test "prefix page reduction fails closed on storage errors and cursor regressions" do
    reduce_fun = fn _entries, acc -> {:ok, acc} end

    assert {:error, :busy} =
             LMDB.__reduce_prefix_pages_for_test__(
               "flow:",
               2,
               :acc,
               fn _cursor -> {:error, :busy} end,
               reduce_fun
             )

    stalled_scan = fn
      nil -> {:ok, [{"flow:a", "1"}, {"flow:b", "2"}]}
      "flow:b" -> {:ok, [{"flow:b", "again"}]}
    end

    assert {:error, :non_monotonic_prefix_page} =
             LMDB.__reduce_prefix_pages_for_test__(
               "flow:",
               2,
               :acc,
               stalled_scan,
               reduce_fun
             )

    assert {:error, :invalid_prefix_page} =
             LMDB.__reduce_prefix_pages_for_test__(
               "flow:",
               2,
               :acc,
               fn _cursor -> {:ok, [{"other:a", "1"}]} end,
               reduce_fun
             )
  end
end
