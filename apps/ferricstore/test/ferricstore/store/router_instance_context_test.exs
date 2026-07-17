defmodule Ferricstore.Store.RouterInstanceContextTest do
  use ExUnit.Case, async: false

  alias Ferricstore.CommandTime
  alias Ferricstore.CrossShardOp
  alias Ferricstore.Store.Router
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.Promotion
  alias Ferricstore.Test.IsolatedInstance
  alias Ferricstore.Test.ShardHelpers

  setup do
    ShardHelpers.wait_shards_alive()
    ctx = IsolatedInstance.checkout(shard_count: 2)
    on_exit(fn -> IsolatedInstance.checkin(ctx) end)
    {:ok, ctx: ctx}
  end

  test "Router LMOVE uses the caller instance context", %{ctx: ctx} do
    {source, destination} = same_shard_keys(ctx)

    assert 2 = Router.list_op(ctx, source, {:rpush, ["first", "second"]})
    assert ["first", "second"] = Router.list_op(ctx, source, {:lrange, 0, -1})
    version_before_move = Router.get_version(ctx, source)

    assert "first" = Router.list_op(ctx, source, {:lmove, destination, :left, :right})
    assert Router.get_version(ctx, source) > version_before_move
    assert ["second"] = Router.list_op(ctx, source, {:lrange, 0, -1})
    assert ["first"] = Router.list_op(ctx, destination, {:lrange, 0, -1})
  end

  test "Router cross-shard LMOVE works inside a non-Raft instance", %{ctx: ctx} do
    handler_id =
      "standalone-cross-shard-journal-#{System.unique_integer([:positive, :monotonic])}"

    parent = self()

    :ok =
      :telemetry.attach_many(
        handler_id,
        [
          [:ferricstore, :standalone_tx_log, :prepare],
          [:ferricstore, :standalone_tx_log, :commit]
        ],
        fn event, measurements, metadata, _config ->
          send(parent, {:standalone_tx_log, event, measurements, metadata})
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    {source, destination} = different_shard_keys(ctx)

    assert 2 = Router.list_op(ctx, source, {:rpush, ["first", "second"]})

    assert "first" = Router.list_op(ctx, source, {:lmove, destination, :left, :right})
    assert ["second"] = Router.list_op(ctx, source, {:lrange, 0, -1})
    assert ["first"] = Router.list_op(ctx, destination, {:lrange, 0, -1})

    assert_receive {:standalone_tx_log, [:ferricstore, :standalone_tx_log, :prepare],
                    %{groups: groups}, %{status: :ok}},
                   1_000

    assert groups >= 2

    assert_receive {:standalone_tx_log, [:ferricstore, :standalone_tx_log, :commit], %{count: 1},
                    %{status: :ok}},
                   1_000
  end

  test "cross-shard publication does not expose a mixed batch snapshot", %{ctx: ctx} do
    {first, second} = different_shard_keys(ctx)
    assert :ok = Router.put(ctx, first, "old-1", 0)
    assert :ok = Router.put(ctx, second, "old-2", 0)

    previous_hook = Application.get_env(:ferricstore, :cross_shard_transaction_hook)
    calls = :atomics.new(1, signed: false)
    parent = self()
    release_ref = make_ref()

    Application.put_env(:ferricstore, :cross_shard_transaction_hook, fn
      {:published_group, shard_index} ->
        if :atomics.add_get(calls, 1, 1) == 1 do
          send(parent, {:first_transaction_group_published, self(), shard_index})

          receive do
            {:continue_publication, ^release_ref} -> :ok
          after
            2_000 -> :ok
          end
        end

        :ok

      _event ->
        :ok
    end)

    on_exit(fn ->
      if previous_hook do
        Application.put_env(:ferricstore, :cross_shard_transaction_hook, previous_hook)
      else
        Application.delete_env(:ferricstore, :cross_shard_transaction_hook)
      end
    end)

    writer =
      Task.async(fn ->
        CrossShardOp.execute(
          [{first, :write}, {second, :write}],
          fn store ->
            :ok = store.put.(first, "new-1", 0)
            :ok = store.put.(second, "new-2", 0)
          end,
          instance: ctx
        )
      end)

    assert_receive {:first_transaction_group_published, publisher, _shard_index}, 1_000

    reader =
      Task.async(fn ->
        send(parent, :cross_shard_batch_reader_started)
        Router.batch_get(ctx, [first, second])
      end)

    assert_receive :cross_shard_batch_reader_started, 1_000

    try do
      refute Task.yield(reader, 100)
    after
      send(publisher, {:continue_publication, release_ref})
    end

    assert Task.await(writer, 2_000) == :ok
    assert Task.await(reader, 2_000) == ["new-1", "new-2"]
    assert Router.batch_get(ctx, [first, second]) == ["new-1", "new-2"]
  end

  test "client death during cross-shard execution does not strand participant barriers", %{
    ctx: ctx
  } do
    {source, destination} = different_shard_keys(ctx)
    parent = self()

    caller =
      spawn(fn ->
        CrossShardOp.execute(
          [{source, :write}, {destination, :write}],
          fn store ->
            send(parent, {:cross_shard_execute_entered, self()})

            receive do
              :continue_cross_shard_execute ->
                store.put.(source, "transaction", 0)
            end
          end,
          instance: ctx
        )
      end)

    assert_receive {:cross_shard_execute_entered, coordinator_pid}, 1_000

    pending_write =
      Task.async(fn -> Router.put(ctx, destination, "after-client-exit", 0) end)

    refute Task.yield(pending_write, 50)
    Process.exit(caller, :kill)
    send(coordinator_pid, :continue_cross_shard_execute)

    assert {:ok, :ok} = Task.yield(pending_write, 2_000)
    assert "transaction" == Router.get(ctx, source)
    assert "after-client-exit" == Router.get(ctx, destination)
  end

  test "client death during delayed participant acquire still releases the protocol", %{ctx: ctx} do
    {source, destination} = different_shard_keys(ctx)
    participant_index = max(Router.shard_for(ctx, source), Router.shard_for(ctx, destination))
    participant = ctx |> Router.shard_name(participant_index) |> Process.whereis()
    parent = self()

    :ok = :sys.suspend(participant)

    on_exit(fn ->
      if Process.alive?(participant) do
        try do
          :sys.resume(participant)
        catch
          :exit, _reason -> :ok
        end
      end
    end)

    caller =
      spawn(fn ->
        CrossShardOp.execute(
          [{source, :write}, {destination, :write}],
          fn _store ->
            send(parent, {:delayed_cross_shard_acquire_entered, self()})

            receive do
              :continue_delayed_cross_shard_execute -> :ok
            end
          end,
          instance: ctx
        )
      end)

    assert :ok = wait_for_barrier_acquire_message(participant, 100)
    Process.exit(caller, :kill)
    :ok = :sys.resume(participant)

    assert_receive {:delayed_cross_shard_acquire_entered, coordinator_pid}, 1_000
    send(coordinator_pid, :continue_delayed_cross_shard_execute)

    pending_write = Task.async(fn -> Router.put(ctx, destination, "after-delayed-acquire", 0) end)
    assert {:ok, :ok} = Task.yield(pending_write, 2_000)
    assert "after-delayed-acquire" == Router.get(ctx, destination)
  end

  test "cross-shard journal rolls back a coordinator crash between shard fsyncs", %{ctx: ctx} do
    previous_trap_exit = Process.flag(:trap_exit, true)
    on_exit(fn -> Process.flag(:trap_exit, previous_trap_exit) end)

    handler_id = "standalone-recovery-#{System.unique_integer([:positive, :monotonic])}"
    parent = self()

    :ok =
      :telemetry.attach(
        handler_id,
        [:ferricstore, :standalone_tx_log, :recover],
        fn event, measurements, metadata, _config ->
          send(parent, {:standalone_recovery, event, measurements, metadata})
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    {source, destination} = different_shard_keys(ctx)
    coordinator = min(Router.shard_for(ctx, source), Router.shard_for(ctx, destination))
    coordinator_name = elem(ctx.shard_names, coordinator)

    assert 2 = Router.list_op(ctx, source, {:rpush, ["first", "second"]})

    previous_hook = Application.get_env(:ferricstore, :standalone_durability_hook)
    calls = :atomics.new(1, signed: false)

    Application.put_env(:ferricstore, :standalone_durability_hook, fn _path, _batch ->
      case :atomics.add_get(calls, 1, 1) do
        1 -> :passthrough
        2 -> exit(:kill)
        _later -> :passthrough
      end
    end)

    on_exit(fn ->
      if previous_hook do
        Application.put_env(:ferricstore, :standalone_durability_hook, previous_hook)
      else
        Application.delete_env(:ferricstore, :standalone_durability_hook)
      end
    end)

    assert {:error, {:standalone_cross_shard_failed, _reason}} =
             Router.list_op(ctx, source, {:lmove, destination, :left, :right})

    assert_receive {:EXIT, _pid, :kill}, 1_000
    assert Process.whereis(coordinator_name) == nil

    assert :persistent_term.get(
             {Ferricstore.Store.StandaloneTxLog, Path.expand(ctx.data_dir)},
             false
           ) == false

    assert {:ok, _pid} =
             Ferricstore.Store.Shard.start_link(
               index: coordinator,
               data_dir: ctx.data_dir,
               instance_ctx: ctx
             )

    assert_receive {:standalone_recovery, [:ferricstore, :standalone_tx_log, :recover],
                    %{pending: 1, replayed: 1}, %{status: :ok}},
                   1_000

    assert ["first", "second"] = Router.list_op(ctx, source, {:lrange, 0, -1})
    assert [] = Router.list_op(ctx, destination, {:lrange, 0, -1})
  end

  test "direct list creation stamps type metadata before later compound commands", %{ctx: ctx} do
    key = "router:instance:type:list:#{System.unique_integer([:positive])}"

    assert 1 = Router.list_op(ctx, key, {:rpush, ["first"]})

    assert {:error, "WRONGTYPE" <> _} =
             Ferricstore.Commands.Hash.handle("HSET", [key, "field", "value"], ctx)
  end

  test "custom batch async put does not use the default Raft batcher", %{ctx: ctx} do
    key = "router:instance:async-batch:#{System.unique_integer([:positive])}"

    assert :ok = Router.batch_put(ctx, [{key, "custom"}])
    assert "custom" == Router.get(ctx, key)
  end

  test "custom compound put does not use the default Raft batcher", %{ctx: ctx} do
    key = "router:instance:async-compound:#{System.unique_integer([:positive])}"
    field_key = CompoundKey.hash_field(key, "field")

    assert :ok = Router.compound_put(ctx, key, field_key, "custom", 0)
    assert "custom" == Router.compound_get(ctx, key, field_key)
  end

  test "custom compound delete does not use the default Raft batcher", %{ctx: ctx} do
    key = "router:instance:async-compound-del:#{System.unique_integer([:positive])}"
    field_key = CompoundKey.hash_field(key, "field")

    assert :ok = Router.compound_put(ctx, key, field_key, "before", 0)

    assert :ok = Router.compound_delete(ctx, key, field_key)
    assert nil == Router.compound_get(ctx, key, field_key)
  end

  test "custom writes stay local when WARaft owns the default instance", %{ctx: ctx} do
    key = "router:instance:waraft-local:#{System.unique_integer([:positive])}"
    field_key = CompoundKey.hash_field(key, "field")

    assert :ok = Router.put(ctx, key, "custom")
    assert "custom" == Router.get(ctx, key)

    assert :ok = Router.compound_put(ctx, key, field_key, "field-value", 0)
    assert "field-value" == Router.compound_get(ctx, key, field_key)
  end

  test "custom write version survives shard process restart", %{ctx: ctx} do
    key = "router:instance:version-restart:#{System.unique_integer([:positive])}"
    idx = Router.shard_for(ctx, key)
    shard_name = elem(ctx.shard_names, idx)

    assert :ok = Router.put(ctx, key, "before")
    version_before = Router.get_version(ctx, key)
    assert version_before > 0

    shard_name
    |> Process.whereis()
    |> GenServer.stop(:normal, 5_000)

    {:ok, _pid} =
      Ferricstore.Store.Shard.start_link(
        index: idx,
        data_dir: ctx.data_dir,
        instance_ctx: ctx
      )

    assert version_before == Router.get_version(ctx, key)

    assert :ok = Router.put(ctx, key, "after")
    assert Router.get_version(ctx, key) > version_before
  end

  test "promoted routing uses stamped command time", %{ctx: ctx} do
    key = "router:instance:promoted-time:#{System.unique_integer([:positive])}"
    idx = Router.shard_for(ctx, key)
    marker = Promotion.marker_key(key)
    stamped_now = Ferricstore.HLC.now_ms() - 60_000
    marker_expire_at = stamped_now + 30_000

    assert marker_expire_at < Ferricstore.HLC.now_ms()

    :ets.insert(elem(ctx.keydir_refs, idx), {marker, "hash", marker_expire_at, 1, 0, 0, 0})
    :atomics.put(ctx.disk_pressure, idx + 1, 1)

    field_key = CompoundKey.hash_field(key, "field")

    assert :ok =
             CommandTime.with_now_ms(stamped_now, fn ->
               Router.compound_put(ctx, key, field_key, "value", 0)
             end)
  end

  defp same_shard_keys(ctx) do
    base = System.unique_integer([:positive])
    default_ctx = FerricStore.Instance.get(:default)

    keys =
      for i <- 1..200 do
        "router:instance:#{base}:#{i}"
      end

    Enum.find_value(keys, fn source ->
      Enum.find_value(keys, fn
        ^source ->
          nil

        destination ->
          same_in_ctx? = Router.shard_for(ctx, source) == Router.shard_for(ctx, destination)

          same_in_default? =
            Router.shard_for(default_ctx, source) == Router.shard_for(default_ctx, destination)

          if same_in_ctx? and same_in_default?, do: {source, destination}
      end)
    end)
  end

  defp different_shard_keys(ctx) do
    base = System.unique_integer([:positive])

    keys =
      for i <- 1..200 do
        "router:instance:cross:#{base}:#{i}"
      end

    Enum.find_value(keys, fn source ->
      Enum.find_value(keys, fn
        ^source ->
          nil

        destination ->
          if Router.shard_for(ctx, source) != Router.shard_for(ctx, destination),
            do: {source, destination}
      end)
    end)
  end

  defp wait_for_barrier_acquire_message(_pid, 0), do: {:error, :barrier_acquire_not_queued}

  defp wait_for_barrier_acquire_message(pid, attempts_left) do
    queued? =
      pid
      |> Process.info(:messages)
      |> elem(1)
      |> Enum.any?(fn
        {:"$gen_call", _from, {:standalone_cross_shard_barrier_acquire, _owner}} -> true
        _message -> false
      end)

    if queued? do
      :ok
    else
      Process.sleep(10)
      wait_for_barrier_acquire_message(pid, attempts_left - 1)
    end
  end
end
