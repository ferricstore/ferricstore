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

  test "standalone group commit publishes only after the 1ms durable batch succeeds" do
    ctx = FerricStore.Instance.get(:default)
    key1 = "standalone:group-commit:1"
    key2 = key_on_same_shard(ctx, Router.shard_for(ctx, key1))

    Application.put_env(:ferricstore, :standalone_fsync_max_delay_ms, 1)

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
    assert {key1, "one"} in batch
    assert {key2, "two"} in batch
    assert Router.get(ctx, key1) == "one"
    assert Router.get(ctx, key2) == "two"
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

  test "standalone write ack does not depend on Flow LMDB writer availability" do
    ctx = FerricStore.Instance.get(:default)
    key = "standalone:flow-lmdb-unavailable"
    shard_idx = Router.shard_for(ctx, key)
    stop_flow_lmdb_writer(ctx, shard_idx)

    assert :ok = Router.put(ctx, key, "value", 0)
    assert Router.get(ctx, key) == "value"
    assert Ferricstore.Health.ready?()
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
