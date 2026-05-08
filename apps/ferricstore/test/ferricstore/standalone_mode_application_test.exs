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

  test "standalone cross-shard tx waits for earlier pending standalone writes" do
    ctx = FerricStore.Instance.get(:default)
    key_a = "standalone:cross-shard:pending-source"
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
