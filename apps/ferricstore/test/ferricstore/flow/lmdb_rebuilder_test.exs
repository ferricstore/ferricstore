defmodule Ferricstore.Flow.LMDBRebuilderTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Flow.Keys
  alias Ferricstore.Flow.LMDBFlushCoordinator
  alias Ferricstore.Flow.LMDBRebuilder

  setup do
    previous = Application.get_env(:ferricstore, :flow_lmdb_history_rebuild_page_size)

    on_exit(fn ->
      case previous do
        nil -> Application.delete_env(:ferricstore, :flow_lmdb_history_rebuild_page_size)
        value -> Application.put_env(:ferricstore, :flow_lmdb_history_rebuild_page_size, value)
      end
    end)
  end

  test "state entry scans distinguish an empty shard from a missing keydir" do
    keydir = :ets.new(:lmdb_rebuilder_keydir, [:set])

    assert {:ok, []} = LMDBRebuilder.__select_state_entries_for_test__(keydir)

    :ets.delete(keydir)

    assert {:error, :source_keydir_unavailable} =
             LMDBRebuilder.__select_state_entries_for_test__(keydir)
  end

  test "history projection page sizes remain positive and bounded" do
    Application.put_env(:ferricstore, :flow_lmdb_history_rebuild_page_size, 0)
    assert LMDBRebuilder.__history_projection_scan_limit_for_test__() == 4_096

    Application.put_env(:ferricstore, :flow_lmdb_history_rebuild_page_size, "all")
    assert LMDBRebuilder.__history_projection_scan_limit_for_test__() == 4_096

    Application.put_env(:ferricstore, :flow_lmdb_history_rebuild_page_size, 2_000_000)
    assert LMDBRebuilder.__history_projection_scan_limit_for_test__() == 65_536

    Application.put_env(:ferricstore, :flow_lmdb_history_rebuild_page_size, 5_000)
    assert LMDBRebuilder.__history_projection_scan_limit_for_test__() == 5_000
  end

  test "state entry rebuilds reduce bounded pages without materializing the full keydir" do
    keydir = :ets.new(:lmdb_rebuilder_paged_keydir, [:set])

    rows =
      for index <- 1..1_200 do
        {Keys.state_key("flow-#{index}"), "value", 0, 0, 0, index, 5}
      end

    true = :ets.insert(keydir, rows)

    ordinary_rows =
      for index <- 1..100 do
        {"ordinary-#{index}", "value", 0, 0, 0, index, 5}
      end

    true = :ets.insert(keydir, ordinary_rows)

    assert {:ok, {1_200, page_sizes}} =
             LMDBRebuilder.__reduce_state_entries_for_test__(
               keydir,
               {0, []},
               fn entries, {count, page_sizes} ->
                 {count + length(entries), [length(entries) | page_sizes]}
               end
             )

    assert Enum.all?(page_sizes, &(&1 > 0 and &1 <= 512))
    assert length(page_sizes) > 1
  end

  test "state entry scans do not misclassify reducer failures as a missing keydir" do
    keydir = :ets.new(:lmdb_rebuilder_reducer_failure_keydir, [:set])
    state_key = Keys.state_key("flow-reducer-failure")
    true = :ets.insert(keydir, {state_key, "value", 0, 0, 0, 1, 5})

    assert_raise ArgumentError, "projection reducer failed", fn ->
      LMDBRebuilder.__reduce_state_entries_for_test__(keydir, :acc, fn _entries, _acc ->
        raise ArgumentError, "projection reducer failed"
      end)
    end
  end

  test "online reconciliation takes an exclusive LMDB flush permit" do
    instance_name =
      String.to_atom("lmdb_rebuilder_exclusive_#{System.unique_integer([:positive, :monotonic])}")

    start_supervised!({LMDBFlushCoordinator, instance_name: instance_name, max_concurrent: 2})

    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_lmdb_reconcile_exclusive_#{System.unique_integer([:positive])}"
      )

    shard_path = Ferricstore.DataDir.shard_data_path(data_dir, 0)
    keydir = :ets.new(:lmdb_rebuilder_exclusive_keydir, [:set])
    parent = self()

    on_exit(fn -> File.rm_rf!(data_dir) end)

    holder =
      Task.async(fn ->
        LMDBFlushCoordinator.with_shard_permit(instance_name, 0, fn ->
          send(parent, :reconcile_holder_acquired)

          receive do
            :release_reconcile_holder -> :ok
          end
        end)
      end)

    assert_receive :reconcile_holder_acquired

    reconcile =
      Task.async(fn ->
        LMDBRebuilder.reconcile_shard(
          shard_path,
          keydir,
          0,
          %{name: instance_name},
          nil,
          nil,
          nil,
          nil
        )
      end)

    assert Task.yield(reconcile, 100) == nil
    send(holder.pid, :release_reconcile_holder)
    assert :ok = Task.await(holder)
    assert :ok = Task.await(reconcile)
  end

  test "online reconciliation preserves active metadata for keydir-evicted cold flows" do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_lmdb_reconcile_cold_#{System.unique_integer([:positive])}"
      )

    shard_path = Ferricstore.DataDir.shard_data_path(data_dir, 0)
    lmdb_path = Ferricstore.Flow.LMDB.path(shard_path)
    keydir = :ets.new(:lmdb_rebuilder_cold_preservation_keydir, [:set])

    on_exit(fn -> File.rm_rf!(data_dir) end)

    cold = active_record("cold-flow", "tenant-cold", 1)
    cold_state_key = Keys.state_key(cold.id, cold.partition_key)
    cold_reverse_key = Ferricstore.Flow.LMDB.active_by_state_key_key(cold_state_key)
    cold_encoded = Ferricstore.Flow.encode_record(cold)

    cold_ops =
      [
        {:put, cold_state_key, Ferricstore.Flow.LMDB.encode_value(cold_encoded, 0)}
        | Ferricstore.Flow.LMDB.active_timeout_index_put_ops(cold_state_key, cold, 0)
      ]

    assert :ok = Ferricstore.Flow.LMDB.write_batch(lmdb_path, cold_ops)
    assert {:ok, cold_reverse} = Ferricstore.Flow.LMDB.get(lmdb_path, cold_reverse_key)

    hot = active_record("hot-flow", "tenant-hot", 2)
    hot_state_key = Keys.state_key(hot.id, hot.partition_key)
    hot_encoded = Ferricstore.Flow.encode_record(hot)

    true =
      :ets.insert(
        keydir,
        {hot_state_key, hot_encoded, 0, 0, :hot, 0, byte_size(hot_encoded)}
      )

    assert :ok =
             LMDBRebuilder.reconcile_shard(
               shard_path,
               keydir,
               0,
               nil,
               nil,
               nil,
               nil,
               nil
             )

    assert {:ok, ^cold_reverse} = Ferricstore.Flow.LMDB.get(lmdb_path, cold_reverse_key)
  end

  test "online reconciliation preserves a hot-pruned terminal projection and repairs its count" do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_lmdb_reconcile_terminal_#{System.unique_integer([:positive])}"
      )

    shard_path = Ferricstore.DataDir.shard_data_path(data_dir, 0)
    lmdb_path = Ferricstore.Flow.LMDB.path(shard_path)
    keydir = :ets.new(:lmdb_rebuilder_terminal_preservation_keydir, [:set])
    record = terminal_record("terminal-flow", "partition-terminal", 10)
    state_key = Keys.state_key(record.id, record.partition_key)
    registry_key = Keys.registry_key(record.id, record.partition_key)
    state_index_key = Keys.state_index_key(record.type, record.state, record.partition_key)
    count_key = Ferricstore.Flow.LMDB.terminal_count_key(state_index_key)

    terminal_key =
      Ferricstore.Flow.LMDB.terminal_index_key(
        state_index_key,
        record.id,
        record.updated_at_ms
      )

    terminal_value =
      Ferricstore.Flow.LMDB.encode_terminal_index_value(
        record.id,
        record.updated_at_ms,
        0,
        state_key,
        count_key
      )

    reverse_key = Ferricstore.Flow.LMDB.terminal_by_state_key_key(state_key)
    encoded = Ferricstore.Flow.encode_record(record)

    on_exit(fn -> File.rm_rf!(data_dir) end)

    true = :ets.insert(keydir, {registry_key, <<1>>, 0, 0, :hot, 0, 1})

    assert :ok =
             Ferricstore.Flow.LMDB.write_batch(lmdb_path, [
               {:put, state_key, Ferricstore.Flow.LMDB.encode_value(encoded, 0)},
               {:put, terminal_key, terminal_value},
               {:put, reverse_key, terminal_key}
             ])

    assert :not_found = Ferricstore.Flow.LMDB.get(lmdb_path, count_key)

    assert :ok =
             LMDBRebuilder.reconcile_shard(
               shard_path,
               keydir,
               0,
               nil,
               nil,
               nil,
               nil,
               nil,
               prune_terminal_keydir?: true
             )

    assert {:ok, ^terminal_value} = Ferricstore.Flow.LMDB.get(lmdb_path, terminal_key)
    assert {:ok, ^terminal_key} = Ferricstore.Flow.LMDB.get(lmdb_path, reverse_key)
    assert {:ok, 1} = Ferricstore.Flow.LMDB.terminal_count(lmdb_path, state_index_key)
  end

  test "online reconciliation removes a stale terminal projection for cold active state" do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_lmdb_reconcile_cold_active_terminal_#{System.unique_integer([:positive])}"
      )

    shard_path = Ferricstore.DataDir.shard_data_path(data_dir, 0)
    lmdb_path = Ferricstore.Flow.LMDB.path(shard_path)
    keydir = :ets.new(:lmdb_rebuilder_cold_active_terminal_keydir, [:set])
    record = active_record("cold-active-flow", "partition-cold-active", 15)
    state_key = Keys.state_key(record.id, record.partition_key)
    registry_key = Keys.registry_key(record.id, record.partition_key)
    terminal_index_key = Keys.state_index_key(record.type, "completed", record.partition_key)
    count_key = Ferricstore.Flow.LMDB.terminal_count_key(terminal_index_key)

    terminal_key =
      Ferricstore.Flow.LMDB.terminal_index_key(
        terminal_index_key,
        record.id,
        record.updated_at_ms - 1
      )

    terminal_value =
      Ferricstore.Flow.LMDB.encode_terminal_index_value(
        record.id,
        record.updated_at_ms - 1,
        0,
        state_key,
        count_key
      )

    reverse_key = Ferricstore.Flow.LMDB.terminal_by_state_key_key(state_key)
    encoded = Ferricstore.Flow.encode_record(record)

    on_exit(fn -> File.rm_rf!(data_dir) end)

    true = :ets.insert(keydir, {registry_key, <<1>>, 0, 0, :hot, 0, 1})

    assert :ok =
             Ferricstore.Flow.LMDB.write_batch(lmdb_path, [
               {:put, state_key, Ferricstore.Flow.LMDB.encode_value(encoded, 0)},
               {:put, terminal_key, terminal_value},
               {:put, reverse_key, terminal_key},
               {:put, count_key, Ferricstore.Flow.LMDB.encode_count(1)}
             ])

    assert :ok =
             LMDBRebuilder.reconcile_shard(
               shard_path,
               keydir,
               0,
               nil,
               nil,
               nil,
               nil,
               nil,
               prune_terminal_keydir?: true
             )

    assert {:ok, _state_blob} = Ferricstore.Flow.LMDB.get(lmdb_path, state_key)
    assert :not_found = Ferricstore.Flow.LMDB.get(lmdb_path, terminal_key)
    assert :not_found = Ferricstore.Flow.LMDB.get(lmdb_path, reverse_key)
    assert :not_found = Ferricstore.Flow.LMDB.get(lmdb_path, count_key)
  end

  test "online reconciliation repairs a missing count before deleting a stale terminal" do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_lmdb_reconcile_stale_terminal_#{System.unique_integer([:positive])}"
      )

    shard_path = Ferricstore.DataDir.shard_data_path(data_dir, 0)
    lmdb_path = Ferricstore.Flow.LMDB.path(shard_path)
    keydir = :ets.new(:lmdb_rebuilder_stale_terminal_keydir, [:set])
    record = terminal_record("stale-terminal", "partition-stale-terminal", 20)
    state_key = Keys.state_key(record.id, record.partition_key)
    state_index_key = Keys.state_index_key(record.type, record.state, record.partition_key)
    count_key = Ferricstore.Flow.LMDB.terminal_count_key(state_index_key)

    terminal_key =
      Ferricstore.Flow.LMDB.terminal_index_key(
        state_index_key,
        record.id,
        record.updated_at_ms
      )

    terminal_value =
      Ferricstore.Flow.LMDB.encode_terminal_index_value(
        record.id,
        record.updated_at_ms,
        0,
        state_key,
        count_key
      )

    reverse_key = Ferricstore.Flow.LMDB.terminal_by_state_key_key(state_key)
    encoded = Ferricstore.Flow.encode_record(record)

    on_exit(fn -> File.rm_rf!(data_dir) end)

    assert :ok =
             Ferricstore.Flow.LMDB.write_batch(lmdb_path, [
               {:put, state_key, Ferricstore.Flow.LMDB.encode_value(encoded, 0)},
               {:put, terminal_key, terminal_value},
               {:put, reverse_key, terminal_key}
             ])

    assert :ok =
             LMDBRebuilder.reconcile_shard(
               shard_path,
               keydir,
               0,
               nil,
               nil,
               nil,
               nil,
               nil,
               prune_terminal_keydir?: true
             )

    assert :not_found = Ferricstore.Flow.LMDB.get(lmdb_path, state_key)
    assert :not_found = Ferricstore.Flow.LMDB.get(lmdb_path, terminal_key)
    assert :not_found = Ferricstore.Flow.LMDB.get(lmdb_path, reverse_key)
    assert :not_found = Ferricstore.Flow.LMDB.get(lmdb_path, count_key)
  end

  test "online reconciliation deletes stale terminals sharing one count key atomically" do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_lmdb_reconcile_stale_terminal_group_#{System.unique_integer([:positive])}"
      )

    shard_path = Ferricstore.DataDir.shard_data_path(data_dir, 0)
    lmdb_path = Ferricstore.Flow.LMDB.path(shard_path)
    keydir = :ets.new(:lmdb_rebuilder_stale_terminal_group_keydir, [:set])

    projections =
      for {id, sequence} <- [{"stale-terminal-a", 30}, {"stale-terminal-b", 40}] do
        id
        |> terminal_record("partition-stale-terminal-group", sequence)
        |> terminal_projection()
      end

    [first, second] = projections
    assert first.count_key == second.count_key

    on_exit(fn -> File.rm_rf!(data_dir) end)

    assert :ok =
             Ferricstore.Flow.LMDB.write_batch(
               lmdb_path,
               Enum.flat_map(projections, & &1.ops) ++
                 [{:put, first.count_key, Ferricstore.Flow.LMDB.encode_count(2)}]
             )

    assert :ok =
             LMDBRebuilder.reconcile_shard(
               shard_path,
               keydir,
               0,
               nil,
               nil,
               nil,
               nil,
               nil,
               prune_terminal_keydir?: true
             )

    for projection <- projections do
      assert :not_found = Ferricstore.Flow.LMDB.get(lmdb_path, projection.state_key)
      assert :not_found = Ferricstore.Flow.LMDB.get(lmdb_path, projection.terminal_key)
      assert :not_found = Ferricstore.Flow.LMDB.get(lmdb_path, projection.reverse_key)
    end

    assert :not_found = Ferricstore.Flow.LMDB.get(lmdb_path, first.count_key)
  end

  test "history rebuild staging retains exact latest events without an all-events map" do
    history_key = "f:{f}:h:flow-1"

    entries = [
      {history_key, "300-1", 300, "compound-300"},
      {"f:{f}:h:flow-2", "250-1", 250, "other-flow"},
      {history_key, "100-1", 100, "compound-100"},
      {history_key, "200-1", 200, "compound-200"}
    ]

    assert LMDBRebuilder.__retained_staged_history_entries_for_test__(
             entries,
             history_key,
             2
           ) == [
             {"200-1", 200, "compound-200"},
             {"300-1", 300, "compound-300"}
           ]

    source =
      File.read!(Path.expand("../../../lib/ferricstore/flow/lmdb_rebuilder.ex", __DIR__))

    refute source =~ "history_entries_by_key"
  end

  defp active_record(id, partition_key, sequence) do
    %{
      id: id,
      type: "job",
      state: "queued",
      version: 1,
      attempts: 0,
      fencing_token: 0,
      created_at_ms: sequence,
      updated_at_ms: sequence,
      next_run_at_ms: 10_000,
      priority: 0,
      partition_key: partition_key,
      state_enter_seq: sequence,
      root_flow_id: id
    }
  end

  defp terminal_record(id, partition_key, sequence) do
    id
    |> active_record(partition_key, sequence)
    |> Map.merge(%{state: "completed", version: 3, next_run_at_ms: 0})
  end

  defp terminal_projection(record) do
    state_key = Keys.state_key(record.id, record.partition_key)
    state_index_key = Keys.state_index_key(record.type, record.state, record.partition_key)
    count_key = Ferricstore.Flow.LMDB.terminal_count_key(state_index_key)

    terminal_key =
      Ferricstore.Flow.LMDB.terminal_index_key(
        state_index_key,
        record.id,
        record.updated_at_ms
      )

    terminal_value =
      Ferricstore.Flow.LMDB.encode_terminal_index_value(
        record.id,
        record.updated_at_ms,
        0,
        state_key,
        count_key
      )

    reverse_key = Ferricstore.Flow.LMDB.terminal_by_state_key_key(state_key)
    encoded = Ferricstore.Flow.encode_record(record)

    %{
      count_key: count_key,
      state_key: state_key,
      terminal_key: terminal_key,
      reverse_key: reverse_key,
      ops: [
        {:put, state_key, Ferricstore.Flow.LMDB.encode_value(encoded, 0)},
        {:put, terminal_key, terminal_value},
        {:put, reverse_key, terminal_key}
      ]
    }
  end
end
