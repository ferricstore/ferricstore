defmodule Ferricstore.StandaloneModeApplicationTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Store.Router

  setup do
    old_data_dir = Application.get_env(:ferricstore, :data_dir)
    old_raft_mode = Application.get_env(:ferricstore, :raft_mode)

    old_standalone_durability_hook =
      Application.get_env(:ferricstore, :standalone_durability_hook)

    old_standalone_fsync_max_delay_ms =
      Application.get_env(:ferricstore, :standalone_fsync_max_delay_ms)

    old_standalone_fsync_max_ops =
      Application.get_env(:ferricstore, :standalone_fsync_max_ops)

    old_standalone_cross_shard_tx_hook =
      Application.get_env(:ferricstore, :standalone_cross_shard_tx_hook)

    old_standalone_tx_log_fsync_dir_hook =
      Application.get_env(:ferricstore, :standalone_tx_log_fsync_dir_hook)

    server_started? = application_started?(:ferricstore_server)

    tmp_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-standalone-mode-#{System.unique_integer([:positive])}"
      )

    stop_app_if_started(:ferricstore_server)
    stop_app_if_started(:ferricstore)
    stop_ra_system()
    File.rm_rf!(tmp_dir)

    Application.put_env(:ferricstore, :data_dir, tmp_dir)
    Application.put_env(:ferricstore, :raft_mode, :manual)

    {:ok, _} = Application.ensure_all_started(:ferricstore)
    wait_local_shards_alive()

    on_exit(fn ->
      stop_app_if_started(:ferricstore_server)
      stop_app_if_started(:ferricstore)
      stop_ra_system()

      restore_env(:data_dir, old_data_dir)
      restore_env(:raft_mode, old_raft_mode)
      restore_env(:standalone_durability_hook, old_standalone_durability_hook)
      restore_env(:standalone_fsync_max_delay_ms, old_standalone_fsync_max_delay_ms)
      restore_env(:standalone_fsync_max_ops, old_standalone_fsync_max_ops)
      restore_env(:standalone_cross_shard_tx_hook, old_standalone_cross_shard_tx_hook)
      restore_env(:standalone_tx_log_fsync_dir_hook, old_standalone_tx_log_fsync_dir_hook)
      Ferricstore.ReplicationMode.put_current(:raft)

      {:ok, _} = Application.ensure_all_started(:ferricstore)
      Ferricstore.Test.ShardHelpers.wait_shards_alive()

      if server_started? do
        {:ok, _} = Application.ensure_all_started(:ferricstore_server)
      end

      File.rm_rf!(tmp_dir)
    end)

    {:ok, tmp_dir: tmp_dir}
  end

  test "manual mode starts without Raft and keeps Bitcask-durable writes", %{tmp_dir: tmp_dir} do
    assert Ferricstore.ReplicationMode.current() == :standalone
    assert :ra_system.fetch(Ferricstore.Raft.Cluster.system_name()) == :undefined

    children = Supervisor.which_children(Ferricstore.Supervisor)
    child_ids = Enum.map(children, fn {id, _pid, _type, _mods} -> id end)
    refute Enum.any?(child_ids, &(to_string(&1) =~ "batcher_"))

    assert {:ok, %{replication_mode: :standalone, cluster_id: cluster_id}} =
             Ferricstore.ReplicationMode.read(tmp_dir)

    assert is_binary(cluster_id)

    ctx = FerricStore.Instance.get(:default)
    key = "standalone:persist"
    version_before = Router.get_version(ctx, key)

    assert :ok = Router.put(ctx, key, "value", 0)
    assert Router.get(ctx, key) == "value"
    assert Router.get_version(ctx, key) == version_before + 1

    assert :ok = Application.stop(:ferricstore)
    stop_ra_system()
    {:ok, _} = Application.ensure_all_started(:ferricstore)
    wait_local_shards_alive()

    assert Ferricstore.ReplicationMode.current() == :standalone
    assert :ra_system.fetch(Ferricstore.Raft.Cluster.system_name()) == :undefined
    assert Router.get(FerricStore.Instance.get(:default), key) == "value"
  end

  test "enabling mode gates standalone writes" do
    Ferricstore.ReplicationMode.put_current(:enabling)

    assert {:error, "ERR cluster promotion in progress"} =
             Router.put(FerricStore.Instance.get(:default), "promotion-gated", "value", 0)

    Ferricstore.ReplicationMode.put_current(:standalone)
  end

  test "standalone durability failure does not publish ETS and pauses all shards" do
    ctx = FerricStore.Instance.get(:default)
    key = "standalone:fsync-failure"
    shard_idx = Router.shard_for(ctx, key)
    other_key = key_on_different_shard(ctx, shard_idx)
    version_before = Router.get_version(ctx, key)

    assert :ok = Router.put(ctx, key, "old", 0)
    assert Router.get(ctx, key) == "old"
    version_after_old = Router.get_version(ctx, key)

    Application.put_env(:ferricstore, :standalone_durability_hook, fn _path, batch ->
      assert Enum.any?(batch, &match?({:put, ^key, "new", 0}, &1))
      {:error, :simulated_eio}
    end)

    assert {:error, message} = Router.put(ctx, key, "new", 0)
    assert message =~ "ERR standalone durability failure"
    assert message =~ "write not applied"

    assert Ferricstore.ReplicationMode.current() == :standalone
    refute Ferricstore.Health.ready?()
    assert :atomics.get(ctx.disk_pressure, shard_idx + 1) == 1
    assert Router.get(ctx, key) == "old"
    assert Router.get_version(ctx, key) == version_after_old

    Application.delete_env(:ferricstore, :standalone_durability_hook)

    assert {:error, "ERR shard writes paused for sync"} =
             Router.put(ctx, key, "second-value", 0)

    assert {:error, "ERR shard writes paused for sync"} =
             Router.put(ctx, other_key, "other-value", 0)

    assert version_after_old == version_before + 1
  end

  test "cluster durability status reports fail-closed state and resume clears it" do
    ctx = FerricStore.Instance.get(:default)
    key = "standalone:durability-repair"

    Application.put_env(:ferricstore, :standalone_durability_hook, fn _path, batch ->
      assert Enum.any?(batch, &match?({:put, ^key, "broken", 0}, &1))
      {:error, :simulated_eio}
    end)

    assert {:error, message} = Router.put(ctx, key, "broken", 0)
    assert message =~ "ERR standalone durability failure"

    status = Ferricstore.Commands.Cluster.handle("CLUSTER.DURABILITY", ["STATUS"], %{})
    assert status =~ "repair_required: true"
    assert status =~ "paused_shards: #{ctx.shard_count}"
    assert status =~ "disk_pressure_shards: #{ctx.shard_count}"

    Application.delete_env(:ferricstore, :standalone_durability_hook)

    assert :ok = Ferricstore.Commands.Cluster.handle("CLUSTER.DURABILITY", ["RESUME"], %{})
    assert Ferricstore.Health.ready?()

    for shard_idx <- 0..(ctx.shard_count - 1) do
      assert :atomics.get(ctx.disk_pressure, shard_idx + 1) == 0
    end

    assert :ok = Router.put(ctx, key, "repaired", 0)
    assert Router.get(ctx, key) == "repaired"

    status = Ferricstore.Commands.Cluster.handle("CLUSTER.DURABILITY", ["STATUS"], %{})
    assert status =~ "repair_required: false"
    assert status =~ "paused_shards: 0"
    assert status =~ "disk_pressure_shards: 0"
  after
    Application.delete_env(:ferricstore, :standalone_durability_hook)
  end

  test "standalone group commit batches writes that arrive before fsync starts" do
    ctx = FerricStore.Instance.get(:default)
    key1 = "standalone:group-commit:1"
    key2 = key_on_same_shard(ctx, Router.shard_for(ctx, key1))

    Application.put_env(:ferricstore, :standalone_fsync_max_delay_ms, 100)

    parent = self()

    Application.put_env(:ferricstore, :standalone_durability_hook, fn _path, batch ->
      send(
        parent,
        {:standalone_batch, Enum.map(batch, fn {:put, key, value, _exp} -> {key, value} end)}
      )

      :passthrough
    end)

    task1 = Task.async(fn -> Router.put(ctx, key1, "one", 0) end)
    task2 = Task.async(fn -> Router.put(ctx, key2, "two", 0) end)

    assert :ok = Task.await(task1, 30_000)
    assert :ok = Task.await(task2, 30_000)

    assert_receive {:standalone_batch, batch}, 5_000
    assert Enum.sort(batch) == Enum.sort([{key1, "one"}, {key2, "two"}])
    assert Router.get(ctx, key1) == "one"
    assert Router.get(ctx, key2) == "two"
  after
    Application.delete_env(:ferricstore, :standalone_fsync_max_delay_ms)
    Application.delete_env(:ferricstore, :standalone_durability_hook)
  end

  test "standalone cross-shard command commits through durable tx path", %{tmp_dir: tmp_dir} do
    ctx = FerricStore.Instance.get(:default)
    key_a = "standalone:cross-shard:msetnx:a"
    key_b = key_on_different_shard(ctx, Router.shard_for(ctx, key_a))
    tx_log_path = Path.join(tmp_dir, "standalone_cross_shard_tx.log")

    assert 1 =
             Ferricstore.Commands.Strings.handle(
               "MSETNX",
               [key_a, "value-a", key_b, "value-b"],
               %{}
             )

    assert Router.get(ctx, key_a) == "value-a"
    assert Router.get(ctx, key_b) == "value-b"
    refute File.exists?(tx_log_path)
  end

  test "standalone cross-shard commit marker failure is not acknowledged" do
    ctx = FerricStore.Instance.get(:default)
    key_a = "standalone:cross-shard:commit-fail:a"
    key_b = key_on_different_shard(ctx, Router.shard_for(ctx, key_a))
    parent = self()

    Application.put_env(:ferricstore, :standalone_tx_log_fsync_dir_hook, fn path ->
      count = Process.get(:standalone_tx_log_fsync_count, 0)
      Process.put(:standalone_tx_log_fsync_count, count + 1)
      send(parent, {:tx_log_fsync_dir, count, path})

      if count == 0 do
        :ok
      else
        {:error, :simulated_commit_marker_fsync_failure}
      end
    end)

    assert {:error, message} =
             Ferricstore.Commands.Strings.handle(
               "MSETNX",
               [key_a, "value-a", key_b, "value-b"],
               %{}
             )

    assert message =~ "ERR standalone durability failure"
    assert message =~ "simulated_commit_marker_fsync_failure"
    assert_receive {:tx_log_fsync_dir, 0, _path}, 5_000
    assert_receive {:tx_log_fsync_dir, 1, _path}, 5_000
  after
    Application.delete_env(:ferricstore, :standalone_tx_log_fsync_dir_hook)
  end

  test "standalone MULTI coordinator spans shards through durable tx log", %{tmp_dir: tmp_dir} do
    ctx = FerricStore.Instance.get(:default)
    key_a = "standalone:cross-shard:multi:a"
    key_b = key_on_different_shard(ctx, Router.shard_for(ctx, key_a))
    tx_log_path = Path.join(tmp_dir, "standalone_cross_shard_tx.log")

    assert [:ok, :ok, "value-a", "value-b"] =
             Ferricstore.Transaction.Coordinator.execute(
               [
                 {"SET", [key_a, "value-a"]},
                 {"SET", [key_b, "value-b"]},
                 {"GET", [key_a]},
                 {"GET", [key_b]}
               ],
               %{},
               nil
             )

    assert Router.get(ctx, key_a) == "value-a"
    assert Router.get(ctx, key_b) == "value-b"
    refute File.exists?(tx_log_path)
  end

  test "standalone prepared cross-shard tx rolls forward on restart", %{tmp_dir: tmp_dir} do
    parent = self()
    ctx = FerricStore.Instance.get(:default)
    key_a = "standalone:cross-shard:recover:a"
    key_b = key_on_different_shard(ctx, Router.shard_for(ctx, key_a))
    tx_log_path = Path.join(tmp_dir, "standalone_cross_shard_tx.log")

    Application.put_env(:ferricstore, :standalone_cross_shard_tx_hook, fn
      {:prepared, _txid, _groups} ->
        send(parent, :standalone_cross_shard_tx_prepared)
        {:error, :simulated_crash_after_prepare}
    end)

    assert {:error, message} =
             Ferricstore.Commands.Strings.handle(
               "MSETNX",
               [key_a, "value-a", key_b, "value-b"],
               %{}
             )

    assert message =~ "ERR standalone durability failure"
    assert_receive :standalone_cross_shard_tx_prepared, 5_000
    assert Router.get(ctx, key_a) == nil
    assert Router.get(ctx, key_b) == nil
    assert File.exists?(tx_log_path)

    Application.delete_env(:ferricstore, :standalone_cross_shard_tx_hook)

    assert :ok = Application.stop(:ferricstore)
    stop_ra_system()
    {:ok, _} = Application.ensure_all_started(:ferricstore)
    wait_local_shards_alive()

    ctx = FerricStore.Instance.get(:default)
    assert Router.get(ctx, key_a) == "value-a"
    assert Router.get(ctx, key_b) == "value-b"
    refute File.exists?(tx_log_path)
  end

  test "standalone tx-log recovery skips corrupt entries and rolls forward valid prepare", %{
    tmp_dir: tmp_dir
  } do
    parent = self()
    ctx = FerricStore.Instance.get(:default)
    key_a = "standalone:cross-shard:corrupt:a"
    key_b = key_on_different_shard(ctx, Router.shard_for(ctx, key_a))
    tx_log_path = Path.join(tmp_dir, "standalone_cross_shard_tx.log")

    handler = {__MODULE__, self(), :corrupt_tx_log}

    :telemetry.attach(
      handler,
      [:ferricstore, :standalone_tx_log, :corrupt_entry],
      fn _event, measurements, _metadata, test_pid ->
        send(test_pid, {:standalone_tx_log_corrupt_entries, measurements.count})
      end,
      parent
    )

    Application.put_env(:ferricstore, :standalone_cross_shard_tx_hook, fn
      {:prepared, _txid, _groups} ->
        send(parent, :standalone_cross_shard_tx_prepared_for_corrupt_log)
        {:error, :simulated_crash_after_prepare}
    end)

    assert {:error, message} =
             Ferricstore.Commands.Strings.handle(
               "MSETNX",
               [key_a, "value-a", key_b, "value-b"],
               %{}
             )

    assert message =~ "ERR standalone durability failure"
    assert_receive :standalone_cross_shard_tx_prepared_for_corrupt_log, 5_000
    assert File.exists?(tx_log_path)

    File.write!(tx_log_path, "not-base64\n", [:append])

    File.write!(
      tx_log_path,
      Base.encode64(:erlang.term_to_binary({:wrong_magic, :entry})) <> "\n",
      [
        :append
      ]
    )

    Application.delete_env(:ferricstore, :standalone_cross_shard_tx_hook)

    assert :ok = Application.stop(:ferricstore)
    stop_ra_system()
    {:ok, _} = Application.ensure_all_started(:ferricstore)
    wait_local_shards_alive()

    assert_receive {:standalone_tx_log_corrupt_entries, skipped}, 5_000
    assert skipped >= 2

    ctx = FerricStore.Instance.get(:default)
    assert Router.get(ctx, key_a) == "value-a"
    assert Router.get(ctx, key_b) == "value-b"
    refute File.exists?(tx_log_path)
  after
    :telemetry.detach({__MODULE__, self(), :corrupt_tx_log})
    Application.delete_env(:ferricstore, :standalone_cross_shard_tx_hook)
  end

  test "standalone tx-log keeps pending prepare visible when recovery append fails", %{
    tmp_dir: tmp_dir
  } do
    tx_log_path = Path.join(tmp_dir, "standalone_cross_shard_tx.log")
    missing_path = Path.join([tmp_dir, "missing-shard-dir", "00000.log"])

    assert {:ok, _txid} =
             Ferricstore.Store.StandaloneTxLog.prepare(tmp_dir, [
               {missing_path, [{:put, "standalone:tx-log:bad-recover", "value", 0}]}
             ])

    assert {:error, {:recover_tx_failed, _txid, {_path, _reason}}} =
             Ferricstore.Store.StandaloneTxLog.recover(tmp_dir)

    assert File.exists?(tx_log_path)
  end

  test "standalone prepared Flow child spawn tx rolls forward on restart", %{tmp_dir: tmp_dir} do
    test_pid = self()
    ctx = FerricStore.Instance.get(:default)
    parent = "standalone-flow-spawn-parent-recover:#{System.unique_integer([:positive])}"
    child_same = "standalone-flow-spawn-child-same:#{System.unique_integer([:positive])}"
    child_other = "standalone-flow-spawn-child-other:#{System.unique_integer([:positive])}"
    parent_partition = "standalone-flow-spawn-parent-partition"

    parent_shard =
      Router.shard_for(ctx, Ferricstore.Flow.Keys.state_key(parent, parent_partition))

    child_same_partition = flow_partition_on_same_shard(ctx, child_same, parent_shard)
    child_other_partition = flow_partition_on_different_shard(ctx, child_other, parent_shard)
    tx_log_path = Path.join(tmp_dir, "standalone_cross_shard_tx.log")

    assert {:ok, created_parent} =
             FerricStore.flow_create(parent,
               type: "parent",
               state: "dispatch",
               partition_key: parent_partition
             )

    Application.put_env(:ferricstore, :standalone_cross_shard_tx_hook, fn
      {:prepared, _txid, groups} ->
        send(test_pid, {:standalone_flow_spawn_tx_prepared, groups})
        {:error, :simulated_crash_after_flow_spawn_prepare}

      _event ->
        :ok
    end)

    assert {:error, reason} =
             FerricStore.flow_spawn_children(
               parent,
               [
                 %{id: child_same, type: "child", partition_key: child_same_partition},
                 %{id: child_other, type: "child", partition_key: child_other_partition}
               ],
               group_id: "fanout",
               wait: :all,
               wait_state: "waiting_children",
               on_child_failed: :ignore,
               on_parent_closed: :abandon_children,
               exhaust_to: %{success: "children_done", failure: "children_failed"},
               partition_key: parent_partition,
               from_state: "dispatch",
               fencing_token: created_parent.fencing_token
             )

    assert inspect(reason) =~ "standalone"
    assert_receive {:standalone_flow_spawn_tx_prepared, prepared_groups}, 5_000
    assert is_list(prepared_groups)
    assert File.exists?(tx_log_path)

    assert {:ok, still_dispatch} = FerricStore.flow_get(parent, partition_key: parent_partition)
    assert still_dispatch.state == "dispatch"
    assert {:ok, nil} = FerricStore.flow_get(child_same, partition_key: child_same_partition)
    assert {:ok, nil} = FerricStore.flow_get(child_other, partition_key: child_other_partition)

    Application.delete_env(:ferricstore, :standalone_cross_shard_tx_hook)

    assert :ok = Application.stop(:ferricstore)
    stop_ra_system()
    {:ok, _} = Application.ensure_all_started(:ferricstore)
    wait_local_shards_alive()

    assert {:ok, waiting_parent} = FerricStore.flow_get(parent, partition_key: parent_partition)
    assert waiting_parent.state == "waiting_children"
    assert waiting_parent.child_groups["fanout"]["summary"]["total"] == 2
    assert waiting_parent.child_groups["fanout"]["children"][child_same] == "running"
    assert waiting_parent.child_groups["fanout"]["children"][child_other] == "running"

    assert {:ok, same_child} =
             FerricStore.flow_get(child_same, partition_key: child_same_partition)

    assert same_child.parent_flow_id == parent
    assert same_child.parent_partition_key == parent_partition

    assert {:ok, other_child} =
             FerricStore.flow_get(child_other, partition_key: child_other_partition)

    assert other_child.parent_flow_id == parent
    assert other_child.parent_partition_key == parent_partition
    refute File.exists?(tx_log_path)
  after
    Application.delete_env(:ferricstore, :standalone_cross_shard_tx_hook)
  end

  test "standalone prepared Flow child terminal tx rolls forward on restart", %{tmp_dir: tmp_dir} do
    test_pid = self()
    ctx = FerricStore.Instance.get(:default)
    parent = "standalone-flow-parent-recover:#{System.unique_integer([:positive])}"
    child = "standalone-flow-child-recover:#{System.unique_integer([:positive])}"
    parent_partition = "standalone-flow-parent-partition"

    parent_shard =
      Router.shard_for(ctx, Ferricstore.Flow.Keys.state_key(parent, parent_partition))

    child_partition = flow_partition_on_different_shard(ctx, child, parent_shard)
    tx_log_path = Path.join(tmp_dir, "standalone_cross_shard_tx.log")

    assert {:ok, created_parent} =
             FerricStore.flow_create(parent,
               type: "parent",
               state: "dispatch",
               partition_key: parent_partition
             )

    assert {:ok, _waiting} =
             FerricStore.flow_spawn_children(
               parent,
               [%{id: child, type: "child", partition_key: child_partition}],
               group_id: "fanout",
               wait: :all,
               wait_state: "waiting_children",
               on_child_failed: :ignore,
               on_parent_closed: :cancel_children,
               exhaust_to: %{success: "children_done", failure: "children_failed"},
               partition_key: parent_partition,
               from_state: "dispatch",
               fencing_token: created_parent.fencing_token
             )

    assert {:ok, spawned_child} = FerricStore.flow_get(child, partition_key: child_partition)

    Application.put_env(:ferricstore, :standalone_cross_shard_tx_hook, fn
      {:prepared, _txid, groups} ->
        send(test_pid, {:standalone_flow_cross_shard_tx_prepared, groups})
        {:error, :simulated_crash_after_flow_prepare}

      _event ->
        :ok
    end)

    assert {:error, reason} =
             FerricStore.flow_cancel(child,
               partition_key: child_partition,
               fencing_token: spawned_child.fencing_token,
               reason: "cancelled for recovery test"
             )

    assert inspect(reason) =~ "standalone"
    assert_receive {:standalone_flow_cross_shard_tx_prepared, prepared_groups}, 5_000
    assert is_list(prepared_groups)
    assert File.exists?(tx_log_path)

    assert {:ok, still_waiting} = FerricStore.flow_get(parent, partition_key: parent_partition)
    assert still_waiting.state == "waiting_children"

    Application.delete_env(:ferricstore, :standalone_cross_shard_tx_hook)

    assert :ok = Application.stop(:ferricstore)
    stop_ra_system()
    {:ok, _} = Application.ensure_all_started(:ferricstore)
    wait_local_shards_alive()

    assert {:ok, done_parent} = FerricStore.flow_get(parent, partition_key: parent_partition)
    assert done_parent.state == "children_done"
    assert done_parent.child_groups["fanout"]["children"][child] == "cancelled"
    assert done_parent.child_groups["fanout"]["summary"]["cancelled"] == 1

    assert {:ok, done_child} = FerricStore.flow_get(child, partition_key: child_partition)
    assert done_child.state == "cancelled"
    refute File.exists?(tx_log_path)

    assert :ok = Application.stop(:ferricstore)
    stop_ra_system()
    assert {:ok, _txid} = Ferricstore.Store.StandaloneTxLog.prepare(tmp_dir, prepared_groups)
    assert :ok = Ferricstore.Store.StandaloneTxLog.recover(tmp_dir)
    {:ok, _} = Application.ensure_all_started(:ferricstore)
    wait_local_shards_alive()

    assert {:ok, replayed_parent} = FerricStore.flow_get(parent, partition_key: parent_partition)
    assert replayed_parent.state == "children_done"
    assert replayed_parent.child_groups["fanout"]["children"][child] == "cancelled"
    assert replayed_parent.child_groups["fanout"]["summary"]["cancelled"] == 1

    assert {:ok, replayed_child} = FerricStore.flow_get(child, partition_key: child_partition)
    assert replayed_child.state == "cancelled"
    refute File.exists?(tx_log_path)
  after
    Application.delete_env(:ferricstore, :standalone_cross_shard_tx_hook)
  end

  test "standalone prepared Flow child complete with real lease rolls forward idempotently", %{
    tmp_dir: tmp_dir
  } do
    test_pid = self()
    ctx = FerricStore.Instance.get(:default)
    parent = "standalone-flow-complete-parent-recover:#{System.unique_integer([:positive])}"
    child = "standalone-flow-complete-child-recover:#{System.unique_integer([:positive])}"
    parent_partition = "standalone-flow-complete-parent-partition"

    parent_shard =
      Router.shard_for(ctx, Ferricstore.Flow.Keys.state_key(parent, parent_partition))

    child_partition = flow_partition_on_different_shard(ctx, child, parent_shard)
    tx_log_path = Path.join(tmp_dir, "standalone_cross_shard_tx.log")

    assert {:ok, created_parent} =
             FerricStore.flow_create(parent,
               type: "parent",
               state: "dispatch",
               partition_key: parent_partition
             )

    assert {:ok, _waiting} =
             FerricStore.flow_spawn_children(
               parent,
               [%{id: child, type: "child", partition_key: child_partition}],
               group_id: "fanout",
               wait: :all,
               wait_state: "waiting_children",
               on_child_failed: :ignore,
               on_parent_closed: :cancel_children,
               exhaust_to: %{success: "children_done", failure: "children_failed"},
               partition_key: parent_partition,
               from_state: "dispatch",
               fencing_token: created_parent.fencing_token
             )

    claimed_child =
      claim_flow_due_no_hang(ctx, "child", child_partition, "standalone-complete-worker")

    Application.put_env(:ferricstore, :standalone_cross_shard_tx_hook, fn
      {:prepared, _txid, groups} ->
        send(test_pid, {:standalone_flow_complete_tx_prepared, groups})
        {:error, :simulated_crash_after_flow_complete_prepare}

      _event ->
        :ok
    end)

    assert {:error, reason} =
             FerricStore.flow_complete(child, claimed_child.lease_token,
               partition_key: child_partition,
               fencing_token: claimed_child.fencing_token,
               result: "done"
             )

    assert inspect(reason) =~ "standalone"
    assert_receive {:standalone_flow_complete_tx_prepared, prepared_groups}, 5_000
    assert is_list(prepared_groups)
    assert File.exists?(tx_log_path)

    Application.delete_env(:ferricstore, :standalone_cross_shard_tx_hook)

    assert :ok = Application.stop(:ferricstore)
    stop_ra_system()
    {:ok, _} = Application.ensure_all_started(:ferricstore)
    wait_local_shards_alive()

    assert {:ok, done_parent} = FerricStore.flow_get(parent, partition_key: parent_partition)
    assert done_parent.state == "children_done"
    assert done_parent.child_groups["fanout"]["children"][child] == "completed"
    assert done_parent.child_groups["fanout"]["summary"]["completed"] == 1

    assert {:ok, done_child} = FerricStore.flow_get(child, partition_key: child_partition)
    assert done_child.state == "completed"
    refute File.exists?(tx_log_path)

    assert :ok = Application.stop(:ferricstore)
    stop_ra_system()
    assert {:ok, _txid} = Ferricstore.Store.StandaloneTxLog.prepare(tmp_dir, prepared_groups)
    assert :ok = Ferricstore.Store.StandaloneTxLog.recover(tmp_dir)
    {:ok, _} = Application.ensure_all_started(:ferricstore)
    wait_local_shards_alive()

    assert {:ok, replayed_parent} = FerricStore.flow_get(parent, partition_key: parent_partition)
    assert replayed_parent.state == "children_done"
    assert replayed_parent.child_groups["fanout"]["children"][child] == "completed"
    assert replayed_parent.child_groups["fanout"]["summary"]["completed"] == 1

    assert {:ok, parent_history} =
             FerricStore.flow_history(parent, partition_key: parent_partition)

    parent_events = flow_history_events(parent_history)
    assert Enum.count(parent_events, &(&1 == "child_completed")) == 1
    refute File.exists?(tx_log_path)
  after
    Application.delete_env(:ferricstore, :standalone_cross_shard_tx_hook)
  end

  test "standalone multi-child duplicate terminal replay does not double-count parent summary", %{
    tmp_dir: tmp_dir
  } do
    test_pid = self()
    ctx = FerricStore.Instance.get(:default)
    parent = "standalone-flow-multi-child-parent:#{System.unique_integer([:positive])}"
    child_a = "standalone-flow-multi-child-a:#{System.unique_integer([:positive])}"
    child_b = "standalone-flow-multi-child-b:#{System.unique_integer([:positive])}"
    child_c = "standalone-flow-multi-child-c:#{System.unique_integer([:positive])}"
    parent_partition = "standalone-flow-multi-child-parent-partition"

    parent_shard =
      Router.shard_for(ctx, Ferricstore.Flow.Keys.state_key(parent, parent_partition))

    part_a = flow_partition_on_same_shard(ctx, child_a, parent_shard)
    part_b = flow_partition_on_different_shard(ctx, child_b, parent_shard)
    part_c = flow_partition_on_different_shard(ctx, child_c, Router.shard_for(ctx, child_b))

    assert {:ok, created_parent} =
             FerricStore.flow_create(parent,
               type: "parent",
               state: "dispatch",
               partition_key: parent_partition
             )

    assert {:ok, _waiting} =
             FerricStore.flow_spawn_children(
               parent,
               [
                 %{id: child_a, type: "child", partition_key: part_a},
                 %{id: child_b, type: "child", partition_key: part_b},
                 %{id: child_c, type: "child", partition_key: part_c}
               ],
               group_id: "fanout",
               wait: :all,
               wait_state: "waiting_children",
               on_child_failed: :ignore,
               on_parent_closed: :cancel_children,
               exhaust_to: %{success: "children_done", failure: "children_failed"},
               partition_key: parent_partition,
               from_state: "dispatch",
               fencing_token: created_parent.fencing_token
             )

    assert {:ok, child_a_record} = FerricStore.flow_get(child_a, partition_key: part_a)
    assert {:ok, child_b_record} = FerricStore.flow_get(child_b, partition_key: part_b)
    assert {:ok, child_c_record} = FerricStore.flow_get(child_c, partition_key: part_c)

    Application.put_env(:ferricstore, :standalone_cross_shard_tx_hook, fn
      {:prepared, _txid, groups} ->
        send(test_pid, {:standalone_flow_multi_child_tx_prepared, groups})
        {:error, :simulated_crash_after_flow_multi_child_prepare}

      _event ->
        :ok
    end)

    assert {:error, reason} =
             FerricStore.flow_cancel_many(
               nil,
               [
                 %{
                   id: child_a,
                   partition_key: part_a,
                   fencing_token: child_a_record.fencing_token
                 },
                 %{
                   id: child_b,
                   partition_key: part_b,
                   fencing_token: child_b_record.fencing_token
                 },
                 %{
                   id: child_c,
                   partition_key: part_c,
                   fencing_token: child_c_record.fencing_token
                 }
               ],
               reason: "duplicate replay coverage"
             )

    assert inspect(reason) =~ "standalone"
    assert_receive {:standalone_flow_multi_child_tx_prepared, prepared_groups}, 5_000

    Application.delete_env(:ferricstore, :standalone_cross_shard_tx_hook)

    assert :ok = Application.stop(:ferricstore)
    stop_ra_system()
    {:ok, _} = Application.ensure_all_started(:ferricstore)
    wait_local_shards_alive()

    assert {:ok, resolved_parent} = FerricStore.flow_get(parent, partition_key: parent_partition)
    assert resolved_parent.state == "children_done"
    assert resolved_parent.child_groups["fanout"]["summary"]["cancelled"] == 3

    assert :ok = Application.stop(:ferricstore)
    stop_ra_system()
    assert {:ok, _txid_a} = Ferricstore.Store.StandaloneTxLog.prepare(tmp_dir, prepared_groups)
    assert {:ok, _txid_b} = Ferricstore.Store.StandaloneTxLog.prepare(tmp_dir, prepared_groups)
    assert :ok = Ferricstore.Store.StandaloneTxLog.recover(tmp_dir)
    {:ok, _} = Application.ensure_all_started(:ferricstore)
    wait_local_shards_alive()

    assert {:ok, replayed_parent} = FerricStore.flow_get(parent, partition_key: parent_partition)
    assert replayed_parent.state == "children_done"
    assert replayed_parent.child_groups["fanout"]["summary"]["cancelled"] == 3

    assert {:ok, parent_history} =
             FerricStore.flow_history(parent, partition_key: parent_partition)

    parent_events = flow_history_events(parent_history)
    assert Enum.count(parent_events, &(&1 == "child_cancelled")) == 3

    assert {:ok, []} =
             FerricStore.flow_claim_due("child",
               partition_key: part_a,
               worker: "standalone-replay-drain-check",
               limit: 10
             )
  after
    Application.delete_env(:ferricstore, :standalone_cross_shard_tx_hook)
  end

  test "standalone cross-shard tx log fsyncs parent directory", %{tmp_dir: tmp_dir} do
    parent = self()

    Application.put_env(:ferricstore, :standalone_tx_log_fsync_dir_hook, fn path ->
      send(parent, {:tx_log_fsync_dir, path})
      :ok
    end)

    assert {:ok, txid} =
             Ferricstore.Store.StandaloneTxLog.prepare(tmp_dir, [
               {Path.join(tmp_dir, "00000.log"), [{:put, "key", "value", 0}]}
             ])

    assert :ok = Ferricstore.Store.StandaloneTxLog.commit(tmp_dir, txid)
    assert_receive {:tx_log_fsync_dir, ^tmp_dir}, 5_000
  after
    Application.delete_env(:ferricstore, :standalone_tx_log_fsync_dir_hook)
  end

  test "standalone cross-shard tx waits for earlier pending standalone writes" do
    ctx = FerricStore.Instance.get(:default)
    key_a = key_on_different_shard(ctx, 0)
    key_b = key_on_different_shard(ctx, Router.shard_for(ctx, key_a))
    parent = self()

    Application.put_env(:ferricstore, :standalone_fsync_max_delay_ms, 1)

    Application.put_env(:ferricstore, :standalone_durability_hook, fn _path, batch ->
      if Enum.any?(batch, &match?({:put, ^key_a, "pending", 0}, &1)) do
        send(parent, {:durability_hook_blocked, self()})

        receive do
          :release_durability_hook -> :passthrough
        after
          5_000 -> {:error, :durability_hook_timeout}
        end
      else
        :passthrough
      end
    end)

    put_task = Task.async(fn -> Router.put(ctx, key_a, "pending", 0) end)
    assert_receive {:durability_hook_blocked, hook_pid}, 5_000

    tx_task =
      Task.async(fn ->
        Ferricstore.Commands.Strings.handle("MSETNX", [key_a, "tx-a", key_b, "tx-b"], %{})
      end)

    refute Task.yield(tx_task, 100)

    send(hook_pid, :release_durability_hook)
    assert :ok = Task.await(put_task, 5_000)
    assert 0 = Task.await(tx_task, 5_000)
    assert Router.get(ctx, key_a) == "pending"
    assert Router.get(ctx, key_b) == nil
  after
    Application.delete_env(:ferricstore, :standalone_fsync_max_delay_ms)
    Application.delete_env(:ferricstore, :standalone_durability_hook)
  end

  test "standalone cross-shard barrier holds newer writes during sync drain" do
    ctx = FerricStore.Instance.get(:default)
    key = key_on_same_shard(ctx, 0)
    shard = elem(ctx.shard_names, 0)

    assert :ok = GenServer.call(shard, :standalone_cross_shard_barrier_acquire, 5_000)

    put_task = Task.async(fn -> Router.put(ctx, key, "younger", 0) end)

    assert eventually(fn ->
             %{waiting_count: waiting} =
               GenServer.call(shard, :standalone_commit_debug, 5_000)

             waiting == 1
           end)

    assert %{} = GenServer.call(shard, {:standalone_commit_sync, {:cross_shard_tx, []}}, 5_000)
    refute Task.yield(put_task, 100)

    assert :ok = GenServer.call(shard, :standalone_cross_shard_barrier_release, 5_000)
    assert :ok = Task.await(put_task, 5_000)
    assert Router.get(ctx, key) == "younger"
  after
    ctx = FerricStore.Instance.get(:default)
    _ = GenServer.call(elem(ctx.shard_names, 0), :standalone_cross_shard_barrier_release, 5_000)
  end

  test "standalone cross-shard barrier release rejects queued writes after fail-closed pause" do
    ctx = FerricStore.Instance.get(:default)
    key = key_on_same_shard(ctx, 0)
    shard = elem(ctx.shard_names, 0)

    assert :ok = GenServer.call(shard, :standalone_cross_shard_barrier_acquire, 5_000)

    put_task = Task.async(fn -> Router.put(ctx, key, "must-not-apply", 0) end)

    assert eventually(fn ->
             %{waiting_count: waiting} =
               GenServer.call(shard, :standalone_commit_debug, 5_000)

             waiting == 1
           end)

    assert :ok = GenServer.call(shard, {:pause_writes}, 5_000)
    assert :ok = GenServer.call(shard, :standalone_cross_shard_barrier_release, 5_000)

    assert {:error, message} = Task.await(put_task, 5_000)
    assert message =~ "ERR standalone durability failure"
    assert Router.get(ctx, key) == nil
  after
    ctx = FerricStore.Instance.get(:default)
    _ = GenServer.call(elem(ctx.shard_names, 0), :standalone_cross_shard_barrier_release, 5_000)
  end

  test "standalone cross-shard tx fences coordinator shard on fail-closed pause" do
    ctx = FerricStore.Instance.get(:default)
    remote_key_a = key_on_same_shard(ctx, 1)
    remote_key_b = key_on_same_shard(ctx, 2)
    coordinator_key = key_on_same_shard(ctx, 0)
    parent = self()

    Application.put_env(:ferricstore, :standalone_cross_shard_tx_hook, fn
      {:prepared, _txid, _groups} ->
        send(parent, {:standalone_cross_shard_tx_prepared, self()})

        receive do
          :release_standalone_cross_shard_tx ->
            {:error, :simulated_crash_after_prepare}
        after
          5_000 ->
            {:error, :standalone_cross_shard_tx_hook_timeout}
        end

      _other ->
        :ok
    end)

    tx_task =
      Task.async(fn ->
        Ferricstore.Commands.Strings.handle(
          "MSETNX",
          [remote_key_a, "tx-a", remote_key_b, "tx-b"],
          %{}
        )
      end)

    assert_receive {:standalone_cross_shard_tx_prepared, hook_pid}, 5_000

    put_task = Task.async(fn -> Router.put(ctx, coordinator_key, "must-not-apply", 0) end)
    send(hook_pid, :release_standalone_cross_shard_tx)

    assert {:error, tx_message} = Task.await(tx_task, 5_000)
    assert tx_message =~ "ERR standalone durability failure"

    assert {:error, put_message} = Task.await(put_task, 5_000)

    assert put_message in [
             "ERR standalone durability failure: :prior_standalone_write_failed",
             "ERR shard writes paused for sync"
           ]

    assert Router.get(ctx, coordinator_key) == nil
  after
    Application.delete_env(:ferricstore, :standalone_cross_shard_tx_hook)
  end

  test "standalone batch read-modify-write commands see staged values before publish" do
    ctx = FerricStore.Instance.get(:default)
    key = "standalone:group-commit:incr"

    Application.put_env(:ferricstore, :standalone_fsync_max_delay_ms, 50)
    Application.put_env(:ferricstore, :standalone_fsync_max_ops, 2)

    parent = self()

    Application.put_env(:ferricstore, :standalone_durability_hook, fn _path, batch ->
      send(parent, {:standalone_incr_batch, batch})
      :passthrough
    end)

    task1 = Task.async(fn -> Router.incr(ctx, key, 1) end)
    task2 = Task.async(fn -> Router.incr(ctx, key, 2) end)

    assert {:ok, 1} = Task.await(task1, 30_000)
    assert {:ok, 3} = Task.await(task2, 30_000)

    assert_receive {:standalone_incr_batch, [{:put, ^key, "1", 0}, {:put, ^key, "3", 0}]},
                   5_000

    assert Router.get(ctx, key) == "3"
  after
    Application.delete_env(:ferricstore, :standalone_fsync_max_delay_ms)
    Application.delete_env(:ferricstore, :standalone_fsync_max_ops)
    Application.delete_env(:ferricstore, :standalone_durability_hook)
  end

  test "standalone zset fsync failure does not leak score index" do
    ctx = FerricStore.Instance.get(:default)
    key = "standalone:zset-fsync-failure"
    member = "ghost"
    member_key = Ferricstore.Store.CompoundKey.zset_member(key, member)
    shard_idx = Router.shard_for(ctx, key)
    keydir = elem(ctx.keydir_refs, shard_idx)
    {zset_index, zset_lookup} = Ferricstore.Store.Shard.ZSetIndex.table_names(ctx.name, shard_idx)

    Application.put_env(:ferricstore, :standalone_durability_hook, fn _path, batch ->
      assert Enum.any?(batch, &match?({:put, ^member_key, "5.0", 0}, &1))
      {:error, :simulated_eio}
    end)

    assert {:error, message} = FerricStore.zincrby(key, 5.0, member)
    assert message =~ "ERR standalone durability failure"
    assert message =~ "write not applied"

    assert [] = :ets.lookup(keydir, Ferricstore.Store.CompoundKey.type_key(key))
    assert [] = :ets.lookup(keydir, member_key)
    refute Router.exists?(ctx, key)
    assert [] = Ferricstore.Store.Shard.ZSetIndex.range(zset_index, key, :neg_inf, :inf, false)

    assert 0 =
             Ferricstore.Store.Shard.ZSetIndex.count(zset_index, zset_lookup, key, :neg_inf, :inf)
  after
    Application.delete_env(:ferricstore, :standalone_durability_hook)
  end

  test "standalone compound batch fsync failure does not publish hash state" do
    ctx = FerricStore.Instance.get(:default)
    key = "standalone:hash-fsync-failure"
    field = "field"
    field_key = Ferricstore.Store.CompoundKey.hash_field(key, field)
    type_key = Ferricstore.Store.CompoundKey.type_key(key)
    shard_idx = Router.shard_for(ctx, key)
    keydir = elem(ctx.keydir_refs, shard_idx)

    Application.put_env(:ferricstore, :standalone_durability_hook, fn _path, batch ->
      assert Enum.any?(batch, &match?({:put, ^field_key, "value", 0}, &1))
      {:error, :simulated_eio}
    end)

    assert {:error, message} = FerricStore.hset(key, %{field => "value"})
    assert message =~ "ERR standalone durability failure"
    assert message =~ "write not applied"

    assert [] = :ets.lookup(keydir, type_key)
    assert [] = :ets.lookup(keydir, field_key)
    refute Router.exists?(ctx, key)
  after
    Application.delete_env(:ferricstore, :standalone_durability_hook)
  end

  test "standalone probabilistic create cleans sidecar if metadata fsync fails" do
    ctx = FerricStore.Instance.get(:default)
    key = "standalone:prob-create-fsync-failure"

    Application.put_env(:ferricstore, :standalone_durability_hook, fn _path, batch ->
      assert Enum.any?(batch, &match?({:put, ^key, _value, 0}, &1))
      {:error, :simulated_eio}
    end)

    assert {:error, _reason} = FerricStore.bf_reserve(key, 0.01, 100)
    refute Enum.any?(prob_file_paths(ctx, key, "bloom"), &File.exists?/1)
    refute Router.exists?(ctx, key)
  after
    Application.delete_env(:ferricstore, :standalone_durability_hook)
  end

  test "standalone probabilistic delete keeps sidecar until tombstone is durable" do
    ctx = FerricStore.Instance.get(:default)
    key = "standalone:prob-delete-fsync-failure"

    assert :ok = FerricStore.bf_reserve(key, 0.01, 100)
    path = prob_meta_path(ctx, key)
    assert File.exists?(path)

    Application.put_env(:ferricstore, :standalone_durability_hook, fn _path, batch ->
      assert Enum.any?(batch, &match?({:delete, ^key, ^path}, &1))
      {:error, :simulated_eio}
    end)

    assert {:error, _reason} = FerricStore.del(key)
    assert File.exists?(path)
    assert Router.exists?(ctx, key)
  after
    Application.delete_env(:ferricstore, :standalone_durability_hook)
  end

  test "standalone probabilistic delete removes sidecar after durable tombstone" do
    ctx = FerricStore.Instance.get(:default)
    key = "standalone:prob-delete-success"

    assert :ok = FerricStore.bf_reserve(key, 0.01, 100)
    path = prob_meta_path(ctx, key)
    assert File.exists?(path)

    assert {:ok, 1} = FerricStore.del(key)
    refute File.exists?(path)
    refute Router.exists?(ctx, key)
  end

  test "standalone Flow fsync failure does not leak ordered indexes" do
    ctx = FerricStore.Instance.get(:default)
    id = "standalone-flow-fsync-failure"
    type = "standalone-flow-index-failure"
    partition_key = "tenant-flow-index-failure"
    state_key = Ferricstore.Flow.Keys.state_key(id, partition_key)
    due_key = Ferricstore.Flow.Keys.due_key(type, "queued", 0, partition_key)
    state_index_key = Ferricstore.Flow.Keys.state_index_key(type, "queued", partition_key)
    history_key = Ferricstore.Flow.Keys.history_key(id, partition_key)
    shard_idx = Router.shard_for(ctx, state_key)
    keydir = elem(ctx.keydir_refs, shard_idx)
    {_flow_index, flow_lookup} = Ferricstore.Flow.OrderedIndex.table_names(ctx.name, shard_idx)

    Application.put_env(:ferricstore, :standalone_durability_hook, fn _path, batch ->
      assert Enum.any?(batch, &match?({:put, ^state_key, _value, 0}, &1))
      {:error, :simulated_eio}
    end)

    assert {:error, message} =
             FerricStore.flow_create(id,
               type: type,
               state: "queued",
               partition_key: partition_key
             )

    assert message =~ "ERR standalone durability failure"
    assert message =~ "write not applied"

    assert [] = :ets.lookup(keydir, state_key)
    assert {:ok, nil} = FerricStore.flow_get(id, partition_key: partition_key)
    assert :miss = Ferricstore.Flow.OrderedIndex.score_of(flow_lookup, due_key, id)
    assert :miss = Ferricstore.Flow.OrderedIndex.score_of(flow_lookup, state_index_key, id)
    assert 0 = Ferricstore.Flow.OrderedIndex.count_all(flow_lookup, history_key)
    assert 0 = Ferricstore.Flow.OrderedIndex.count_all(flow_lookup, due_key)
    assert 0 = Ferricstore.Flow.OrderedIndex.count_all(flow_lookup, state_index_key)
  after
    Application.delete_env(:ferricstore, :standalone_durability_hook)
  end

  test "standalone reads do not see staged writes while fsync is blocked" do
    ctx = FerricStore.Instance.get(:default)
    key = "standalone:blocked-fsync-visibility"
    parent = self()

    Application.put_env(:ferricstore, :standalone_durability_hook, fn _path, batch ->
      assert Enum.any?(batch, &match?({:put, ^key, "visible-after-fsync", 0}, &1))
      send(parent, {:durability_hook_blocked, self()})

      receive do
        :release_durability_hook -> :passthrough
      after
        5_000 -> {:error, :durability_hook_timeout}
      end
    end)

    task = Task.async(fn -> Router.put(ctx, key, "visible-after-fsync", 0) end)
    assert_receive {:durability_hook_blocked, hook_pid}, 5_000

    assert Router.get(ctx, key) == nil

    send(hook_pid, :release_durability_hook)
    assert :ok = Task.await(task, 5_000)
    assert Router.get(ctx, key) == "visible-after-fsync"
  after
    Application.delete_env(:ferricstore, :standalone_durability_hook)
  end

  test "standalone fsync wait does not block the shard mailbox" do
    ctx = FerricStore.Instance.get(:default)
    key = "standalone:blocked-fsync-mailbox"
    shard_idx = Router.shard_for(ctx, key)
    shard = elem(ctx.shard_names, shard_idx)
    parent = self()

    Application.put_env(:ferricstore, :standalone_durability_hook, fn _path, batch ->
      assert Enum.any?(batch, &match?({:put, ^key, "value", 0}, &1))
      send(parent, {:durability_hook_blocked, self()})

      receive do
        :release_durability_hook -> :passthrough
      after
        5_000 -> {:error, :durability_hook_timeout}
      end
    end)

    task = Task.async(fn -> Router.put(ctx, key, "value", 0) end)
    assert_receive {:durability_hook_blocked, hook_pid}, 5_000

    assert {file_id, file_path} = GenServer.call(shard, :get_active_file, 100)
    assert is_integer(file_id)
    assert is_binary(file_path)

    send(hook_pid, :release_durability_hook)
    assert :ok = Task.await(task, 5_000)
  after
    Application.delete_env(:ferricstore, :standalone_durability_hook)
  end

  test "standalone command key audit covers multi-key writes" do
    compound_key = Ferricstore.Store.CompoundKey.zset_member("zset-key", "member")
    owner_ref = make_ref()
    flow_a = Ferricstore.Flow.Keys.state_key("flow-a", "tenant-a")
    flow_b = Ferricstore.Flow.Keys.state_key("flow-b", "tenant-a")
    flow_batch = Ferricstore.Flow.Keys.state_key("__complete_batch__", "tenant-a")

    assert standalone_keys({:list_op_lmove, "source", "destination", :left, :right}) ==
             ["destination", "source"]

    assert standalone_keys({:pfmerge, "destination", ["source-a", "source-b"], [:sketch_a]}) ==
             ["destination", "source-a", "source-b"]

    assert standalone_keys({:cms_merge, "destination", ["source-a", "source-b"], [1, 1], nil}) ==
             ["destination", "source-a", "source-b"]

    assert standalone_keys({:lock_keys, ["source", "destination"], owner_ref, 1_000}) ==
             ["destination", "source"]

    assert standalone_keys({:unlock_keys, ["source", "destination"], owner_ref}) ==
             ["destination", "source"]

    assert standalone_keys({:compound_put, compound_key, "1.0", 0}) == ["zset-key"]
    assert standalone_keys({:locked_delete_prefix, compound_key, owner_ref}) == ["zset-key"]

    assert standalone_keys({:compound_batch_put, "hash-key", [{compound_key, "1", 0}]}) == [
             "hash-key"
           ]

    expected_flow_keys = Enum.sort([flow_batch, flow_a, flow_b])

    assert standalone_keys(
             {:flow_complete_many, flow_batch,
              %{
                records: [
                  %{id: "flow-a", partition_key: "tenant-a"},
                  %{id: "flow-b", partition_key: "tenant-a"}
                ]
              }}
           ) == expected_flow_keys

    assert standalone_keys({:flow_claim_due, "f:{tenant-a}:d", %{limit: 10}}) == [
             "__standalone_global__"
           ]

    assert standalone_keys({:server_command, {:opaque, :write}}) == ["__standalone_global__"]

    assert standalone_keys(
             {:cross_shard_tx,
              [
                {0,
                 [
                   {0, {"MSETNX", ["a", "1", "b", "2"], {:msetnx, ["a", "1", "b", "2"]}}}
                 ], nil}
              ]}
           ) == ["a", "b"]

    assert standalone_keys(
             {:cross_shard_tx,
              [
                {0,
                 [
                   {0,
                    {"LMOVE", ["src", "dst", "left", "right"],
                     {:lmove, "src", "dst", :left, :right}}}
                 ], nil}
              ]}
           ) == ["dst", "src"]

    assert standalone_keys({:cross_shard_tx, %{0 => [{:put, "k", "v", 0}]}}) == [
             "__standalone_global__"
           ]
  end

  test "standalone global command key conflicts with ordinary key writes" do
    refute standalone_conflict?({:put, "a", "1", 0}, {:put, "b", "2", 0})
    assert standalone_conflict?({:put, "a", "1", 0}, {:put, "a", "2", 0})

    assert standalone_conflict?(
             {:flow_claim_due, "f:{tenant-a}:d", %{limit: 1}},
             {:put, "a", "1", 0}
           )

    assert standalone_conflict?({:put, "a", "1", 0}, {:server_command, :opaque})
  end

  test "standalone blocked fsync does not let same-key write publish early" do
    ctx = FerricStore.Instance.get(:default)
    key = "standalone:same-key-fsync-order"
    shard = elem(ctx.shard_names, Router.shard_for(ctx, key))
    parent = self()

    Application.put_env(:ferricstore, :standalone_durability_hook, fn _path, batch ->
      if Enum.any?(batch, &match?({:put, ^key, "one", 0}, &1)) do
        send(parent, {:durability_hook_blocked, self()})

        receive do
          :release_durability_hook -> :passthrough
        after
          5_000 -> {:error, :durability_hook_timeout}
        end
      else
        :passthrough
      end
    end)

    first = Task.async(fn -> Router.put(ctx, key, "one", 0) end)
    assert_receive {:durability_hook_blocked, hook_pid}, 5_000

    second = Task.async(fn -> Router.put(ctx, key, "two", 0) end)
    refute Task.yield(second, 100)

    assert eventually(fn ->
             debug = GenServer.call(shard, :standalone_commit_debug, 100)
             debug.batch_count == 0 and debug.waiting_count == 1 and debug.inflight_count == 1
           end)

    assert Router.get(ctx, key) == nil

    send(hook_pid, :release_durability_hook)
    assert :ok = Task.await(first, 5_000)
    assert :ok = Task.await(second, 5_000)
    assert Router.get(ctx, key) == "two"
  after
    Application.delete_env(:ferricstore, :standalone_durability_hook)
  end

  test "standalone different-key write enters next batch while fsync is blocked" do
    ctx = FerricStore.Instance.get(:default)
    first_key = "standalone:different-key-fsync:first"
    shard_idx = Router.shard_for(ctx, first_key)
    second_key = key_on_same_shard(ctx, shard_idx)
    shard = elem(ctx.shard_names, shard_idx)
    parent = self()

    Application.put_env(:ferricstore, :standalone_durability_hook, fn _path, batch ->
      cond do
        Enum.any?(batch, &match?({:put, ^first_key, "one", 0}, &1)) ->
          send(parent, {:first_durability_hook_blocked, self(), batch})

          receive do
            :release_first_durability_hook -> :passthrough
          after
            5_000 -> {:error, :first_durability_hook_timeout}
          end

        Enum.any?(batch, &match?({:put, ^second_key, "two", 0}, &1)) ->
          send(parent, {:second_durability_hook_blocked, self(), batch})

          receive do
            :release_second_durability_hook -> :passthrough
          after
            5_000 -> {:error, :second_durability_hook_timeout}
          end

        true ->
          :passthrough
      end
    end)

    first = Task.async(fn -> Router.put(ctx, first_key, "one", 0) end)
    assert_receive {:first_durability_hook_blocked, first_hook_pid, first_batch}, 5_000
    assert Enum.any?(first_batch, &match?({:put, ^first_key, "one", 0}, &1))
    refute Enum.any?(first_batch, &match?({:put, ^second_key, "two", 0}, &1))

    second = Task.async(fn -> Router.put(ctx, second_key, "two", 0) end)

    assert eventually(fn ->
             debug = GenServer.call(shard, :standalone_commit_debug, 100)
             debug.batch_count == 1 and debug.waiting_count == 0
           end)

    send(first_hook_pid, :release_first_durability_hook)
    assert_receive {:second_durability_hook_blocked, second_hook_pid, second_batch}, 5_000
    assert Enum.any?(second_batch, &match?({:put, ^second_key, "two", 0}, &1))
    refute Enum.any?(second_batch, &match?({:put, ^first_key, "one", 0}, &1))

    assert :ok = Task.await(first, 5_000)
    refute Task.yield(second, 100)
    assert Router.get(ctx, second_key) == nil

    send(second_hook_pid, :release_second_durability_hook)
    assert :ok = Task.await(second, 5_000)
    assert Router.get(ctx, first_key) == "one"
    assert Router.get(ctx, second_key) == "two"
  after
    Application.delete_env(:ferricstore, :standalone_durability_hook)
  end

  test "standalone shard crash kills blocked fsync worker before publish" do
    ctx = FerricStore.Instance.get(:default)
    key = "standalone:blocked-fsync-shard-crash"
    shard_idx = Router.shard_for(ctx, key)
    shard = elem(ctx.shard_names, shard_idx)
    parent = self()

    Application.put_env(:ferricstore, :standalone_durability_hook, fn _path, batch ->
      assert Enum.any?(batch, &match?({:put, ^key, "value", 0}, &1))
      send(parent, {:durability_hook_blocked, self()})

      receive do
        :release_durability_hook -> :passthrough
      after
        30_000 -> {:error, :durability_hook_timeout}
      end
    end)

    write_pid =
      spawn(fn ->
        result =
          try do
            Router.put(ctx, key, "value", 0)
          catch
            :exit, reason -> {:exit, reason}
          end

        send(parent, {:write_result, result})
      end)

    assert_receive {:durability_hook_blocked, hook_pid}, 5_000
    hook_ref = Process.monitor(hook_pid)
    Process.exit(Process.whereis(shard), :kill)

    assert_receive {:DOWN, ^hook_ref, :process, ^hook_pid, _reason}, 5_000
    assert_receive {:write_result, {:exit, _reason}}, 5_000
    refute Process.alive?(write_pid)

    wait_local_shards_alive()
    assert Router.get(ctx, key) == nil
  after
    Application.delete_env(:ferricstore, :standalone_durability_hook)
  end

  test "standalone explicit flush waits for blocked fsync batch" do
    ctx = FerricStore.Instance.get(:default)
    key = "standalone:flush-waits-for-fsync"
    shard = elem(ctx.shard_names, Router.shard_for(ctx, key))
    parent = self()

    Application.put_env(:ferricstore, :standalone_durability_hook, fn _path, batch ->
      assert Enum.any?(batch, &match?({:put, ^key, "value", 0}, &1))
      send(parent, {:durability_hook_blocked, self()})

      receive do
        :release_durability_hook -> :passthrough
      after
        5_000 -> {:error, :durability_hook_timeout}
      end
    end)

    write = Task.async(fn -> Router.put(ctx, key, "value", 0) end)
    assert_receive {:durability_hook_blocked, hook_pid}, 5_000

    flush =
      Task.async(fn ->
        GenServer.call(shard, :flush, 30_000)
      end)

    refute Task.yield(flush, 100)
    send(hook_pid, :release_durability_hook)

    assert :ok = Task.await(write, 5_000)
    assert :ok = Task.await(flush, 5_000)
    assert Router.get(ctx, key) == "value"
  after
    Application.delete_env(:ferricstore, :standalone_durability_hook)
  end

  test "standalone shutdown waits for blocked fsync batch" do
    ctx = FerricStore.Instance.get(:default)
    key = "standalone:shutdown-waits-for-fsync"
    parent = self()

    Application.put_env(:ferricstore, :standalone_durability_hook, fn _path, batch ->
      assert Enum.any?(batch, &match?({:put, ^key, "value", 0}, &1))
      send(parent, {:durability_hook_blocked, self()})

      receive do
        :release_durability_hook -> :passthrough
      after
        5_000 -> {:error, :durability_hook_timeout}
      end
    end)

    write = Task.async(fn -> Router.put(ctx, key, "value", 0) end)
    assert_receive {:durability_hook_blocked, hook_pid}, 5_000

    stop = Task.async(fn -> Application.stop(:ferricstore) end)
    refute Task.yield(stop, 100)
    refute Task.yield(write, 100)

    send(hook_pid, :release_durability_hook)
    assert :ok = Task.await(write, 5_000)
    assert :ok = Task.await(stop, 5_000)

    Application.delete_env(:ferricstore, :standalone_durability_hook)
    assert {:ok, _} = Application.ensure_all_started(:ferricstore)
    wait_local_shards_alive()
    assert Router.get(FerricStore.Instance.get(:default), key) == "value"
  after
    Application.delete_env(:ferricstore, :standalone_durability_hook)
  end

  test "standalone promotion flush primitive waits for blocked fsync batch" do
    ctx = FerricStore.Instance.get(:default)
    key = "standalone:promotion-flush-waits-for-fsync"
    parent = self()

    Application.put_env(:ferricstore, :standalone_durability_hook, fn _path, batch ->
      assert Enum.any?(batch, &match?({:put, ^key, "value", 0}, &1))
      send(parent, {:durability_hook_blocked, self()})

      receive do
        :release_durability_hook -> :passthrough
      after
        5_000 -> {:error, :durability_hook_timeout}
      end
    end)

    write = Task.async(fn -> Router.put(ctx, key, "value", 0) end)
    assert_receive {:durability_hook_blocked, hook_pid}, 5_000

    resume = Task.async(fn -> Ferricstore.Cluster.Manager.resume_standalone_durability() end)
    refute Task.yield(resume, 100)
    refute Task.yield(write, 100)

    send(hook_pid, :release_durability_hook)
    assert :ok = Task.await(write, 5_000)
    assert :ok = Task.await(resume, 5_000)
    assert Router.get(ctx, key) == "value"
  after
    Application.delete_env(:ferricstore, :standalone_durability_hook)
  end

  test "standalone write ack does not depend on Flow LMDB writer availability" do
    ctx = FerricStore.Instance.get(:default)
    key = "standalone:flow-lmdb-unavailable"
    shard_idx = Router.shard_for(ctx, key)
    stop_flow_lmdb_writer(ctx, shard_idx)

    assert :ok = Router.put(ctx, key, "value", 0)
    assert Router.get(ctx, key) == "value"
    assert Ferricstore.Health.ready?()
  end

  test "standalone staged Flow state delete enqueues LMDB projection delete" do
    ctx = FerricStore.Instance.get(:default)
    id = "standalone-flow-delete-projection:#{System.unique_integer([:positive])}"
    state_key = Ferricstore.Flow.Keys.state_key(id, nil)
    shard_idx = Router.shard_for(ctx, state_key)
    lmdb_path = flow_lmdb_path(ctx, shard_idx)

    assert {:ok, %{id: ^id, state: "completed"}} =
             FerricStore.flow_create(id, type: "projection-delete", state: "completed")

    assert :ok = Ferricstore.Flow.LMDBWriter.flush(ctx.name, shard_idx, 30_000)
    assert {:ok, _} = Ferricstore.Flow.LMDB.get(lmdb_path, state_key)

    assert :ok = Router.delete(ctx, state_key)
    assert :ok = Ferricstore.Flow.LMDBWriter.flush(ctx.name, shard_idx, 30_000)
    assert :not_found = Ferricstore.Flow.LMDB.get(lmdb_path, state_key)
  end

  test "standalone staged cold Flow state preserves terminal version metadata" do
    ctx = FerricStore.Instance.get(:default)
    id = "standalone-flow-terminal-lfu:#{System.unique_integer([:positive])}"
    state_key = Ferricstore.Flow.Keys.state_key(id, nil)
    shard_idx = Router.shard_for(ctx, state_key)
    keydir = elem(ctx.keydir_refs, shard_idx)

    assert {:ok, %{id: ^id, state: "completed", version: version}} =
             FerricStore.flow_create(id, type: "terminal-lfu", state: "completed")

    assert [
             {^state_key, nil, expire_at_ms, {:flow_state_version, ^version, _lfu}, fid, off,
              vsize}
           ] =
             :ets.lookup(keydir, state_key)

    assert is_integer(expire_at_ms)
    assert is_integer(fid)
    assert is_integer(off)
    assert is_integer(vsize)
  end

  test "standalone Flow command ack does not depend on Flow LMDB writer availability" do
    ctx = FerricStore.Instance.get(:default)
    id = "standalone-flow-lmdb-unavailable:#{System.unique_integer([:positive])}"
    state_key = Ferricstore.Flow.Keys.state_key(id, nil)
    shard_idx = Router.shard_for(ctx, state_key)
    stop_flow_lmdb_writer(ctx, shard_idx)

    assert {:ok, %{id: ^id, state: "queued"}} =
             FerricStore.flow_create(id, type: "cold-projection", state: "queued")

    assert {:ok, %{id: ^id, state: "queued"}} = FerricStore.flow_get(id)
    assert Ferricstore.Health.ready?()
  end

  defp flow_lmdb_path(ctx, shard_idx) do
    ctx.data_dir
    |> Ferricstore.DataDir.shard_data_path(shard_idx)
    |> Ferricstore.Flow.LMDB.path()
  end

  defp standalone_keys(command) do
    Ferricstore.Store.Shard.__standalone_command_keys_for_test__(command)
  end

  defp standalone_conflict?(left, right) do
    Ferricstore.Store.Shard.__standalone_command_keys_conflict_for_test__(left, right)
  end

  defp prob_meta_path(ctx, key) do
    value = Router.get(ctx, key)
    {:bloom_meta, %{path: path}} = :erlang.binary_to_term(value, [:safe])
    path
  end

  defp prob_file_paths(ctx, key, ext) do
    safe = Base.url_encode64(key, padding: false)

    0..(tuple_size(ctx.keydir_refs) - 1)
    |> Enum.map(fn shard_idx ->
      shard_path = Ferricstore.DataDir.shard_data_path(ctx.data_dir, shard_idx)
      Path.join([shard_path, "prob", "#{safe}.#{ext}"])
    end)
  end

  defp eventually(fun, timeout_ms \\ 2_000) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms

    Stream.repeatedly(fn -> Process.sleep(20) end)
    |> Enum.reduce_while(false, fn _, _ ->
      if fun.() do
        {:halt, true}
      else
        if System.monotonic_time(:millisecond) > deadline do
          {:halt, false}
        else
          {:cont, false}
        end
      end
    end)
  end

  test "enabling marker stays fail-closed on restart without stable node name", %{
    tmp_dir: tmp_dir
  } do
    :ok = Ferricstore.ReplicationMode.mark_enabling!(tmp_dir, 4, 123)

    assert :ok = Application.stop(:ferricstore)
    stop_ra_system()

    assert {:ok, _} = Application.ensure_all_started(:ferricstore)
    wait_local_shards_alive()

    assert {:ok,
            %{
              replication_mode: :enabling,
              promotion_epoch: 123,
              shard_count: 4,
              barrier_indices: barrier_indices
            }} = Ferricstore.ReplicationMode.read(tmp_dir)

    assert barrier_indices == %{}
    assert Ferricstore.ReplicationMode.current() == :enabling
    refute Ferricstore.Health.ready?()
    assert :ra_system.fetch(Ferricstore.Raft.Cluster.system_name()) == :undefined
  end

  defp key_on_different_shard(ctx, shard_idx) do
    Enum.find_value(1..10_000, fn i ->
      key = "standalone:other-shard:#{i}"
      if Router.shard_for(ctx, key) != shard_idx, do: key
    end)
  end

  defp key_on_same_shard(ctx, shard_idx) do
    Enum.find_value(1..10_000, fn i ->
      key = "standalone:same-shard:#{i}"
      if Router.shard_for(ctx, key) == shard_idx, do: key
    end)
  end

  defp flow_partition_on_same_shard(ctx, flow_id, shard_idx) do
    Enum.find_value(1..10_000, fn i ->
      partition = "standalone:flow:same-shard:#{i}"
      key = Ferricstore.Flow.Keys.state_key(flow_id, partition)

      if Router.shard_for(ctx, key) == shard_idx, do: partition
    end)
  end

  defp flow_partition_on_different_shard(ctx, flow_id, shard_idx) do
    Enum.find_value(1..10_000, fn i ->
      partition = "standalone:flow:other-shard:#{i}"
      key = Ferricstore.Flow.Keys.state_key(flow_id, partition)

      if Router.shard_for(ctx, key) != shard_idx, do: partition
    end)
  end

  defp claim_flow_due_no_hang(ctx, type, partition_key, worker) do
    due_key = Ferricstore.Flow.Keys.due_key(type, "queued", 0, partition_key)
    shard_idx = Router.shard_for(ctx, due_key)
    shard = elem(ctx.shard_names, shard_idx)

    task =
      Task.async(fn ->
        FerricStore.flow_claim_due(type,
          state: "queued",
          partition_key: partition_key,
          worker: worker,
          limit: 1,
          lease_ms: 60_000,
          now_ms: 9_000_000_000_000,
          reclaim_expired: false
        )
      end)

    case Task.yield(task, 10_000) || Task.shutdown(task, :brutal_kill) do
      {:ok, {:ok, [claimed]}} ->
        claimed

      {:ok, other} ->
        flunk("unexpected flow_claim_due result: #{inspect(other)}")

      nil ->
        debug =
          try do
            GenServer.call(shard, :standalone_commit_debug, 1_000)
          catch
            kind, reason -> {kind, reason}
          end

        flunk(
          "flow_claim_due timed out for #{type}/#{partition_key}; shard=#{inspect(shard)} debug=#{inspect(debug)}"
        )
    end
  end

  defp flow_history_events(history) do
    Enum.map(history, fn {_event_id, fields} -> fields["event"] end)
  end

  defp stop_flow_lmdb_writer(ctx, shard_idx) do
    child_id = :"flow_lmdb_writer_#{shard_idx}"
    writer_name = Ferricstore.Flow.LMDBWriter.name(ctx.name, shard_idx)

    assert is_pid(Process.whereis(writer_name))
    assert :ok = Supervisor.terminate_child(Ferricstore.Supervisor, child_id)
    assert :ok = Supervisor.delete_child(Ferricstore.Supervisor, child_id)
    refute Process.whereis(writer_name)
  end

  defp wait_local_shards_alive(timeout_ms \\ 30_000) do
    shard_count = Application.get_env(:ferricstore, :shard_count, 4)
    deadline = System.monotonic_time(:millisecond) + timeout_ms

    Enum.each(0..(shard_count - 1), fn i ->
      name = :"Ferricstore.Store.Shard.#{i}"

      Enum.reduce_while(Stream.repeatedly(fn -> Process.sleep(20) end), :waiting, fn _, _ ->
        pid = Process.whereis(name)

        cond do
          is_pid(pid) and Process.alive?(pid) ->
            GenServer.call(name, :flush, 30_000)
            {:halt, :ok}

          System.monotonic_time(:millisecond) > deadline ->
            raise "Shard #{inspect(name)} did not start within #{timeout_ms}ms"

          true ->
            {:cont, :waiting}
        end
      end)
    end)
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

  defp stop_ra_system do
    try do
      :ra_system.stop(Ferricstore.Raft.Cluster.system_name())
    catch
      _, _ -> :ok
    end
  end

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)
end
