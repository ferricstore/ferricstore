defmodule Ferricstore.Store.RouterInstanceContextTest do
  use ExUnit.Case, async: false

  alias Ferricstore.CommandTime
  alias Ferricstore.CrossShardOp
  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Raft.ApplyContext
  alias Ferricstore.Store.Router
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.Promotion
  alias Ferricstore.Store.Shard.CompoundMemberIndex
  alias Ferricstore.Store.StandaloneTxLog
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

  test "Router shard entry batches preserve absolute expiry", %{ctx: ctx} do
    key = "batch-entry:#{System.unique_integer([:positive, :monotonic])}"
    expire_at_ms = System.system_time(:millisecond) + 60_000
    assert :ok = Router.put(ctx, key, "value", expire_at_ms)

    assert {:ok, [{"value", ^expire_at_ms}]} =
             Router.read_shard_entries(ctx, Router.shard_for(ctx, key), [key])
  end

  test "custom shards keep unrelated writes independent during a conditional flush", %{ctx: ctx} do
    test_pid = self()
    durability_calls = :atomics.new(1, signed: false)
    previous_hook = Application.get_env(:ferricstore, :standalone_durability_hook)

    Application.put_env(:ferricstore, :standalone_durability_hook, fn _path, batch ->
      case :atomics.add_get(durability_calls, 1, 1) do
        1 ->
          send(test_pid, {:durability_batch, self(), length(batch)})

          receive do
            :continue -> :passthrough
          after
            5_000 -> :passthrough
          end

        _ ->
          :passthrough
      end
    end)

    on_exit(fn -> restore_env(:standalone_durability_hook, previous_hook) end)

    {conditional_key, unrelated_key} = different_shard_keys(ctx)
    opts = %{expire_at_ms: 0, get: false, keepttl: false, nx: true, xx: false}
    conditional = Task.async(fn -> Router.set(ctx, conditional_key, "conditional", opts) end)

    assert_receive {:durability_batch, worker, 1}, 1_000

    unrelated = Task.async(fn -> Router.put(ctx, unrelated_key, "unrelated", 0) end)
    assert {:ok, :ok} = Task.yield(unrelated, 1_000)
    assert "unrelated" == Router.get(ctx, unrelated_key)

    send(worker, :continue)
    assert :ok = Task.await(conditional, 5_000)
    assert "conditional" == Router.get(ctx, conditional_key)
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

  @tag :standalone_cross_shard_command_error
  test "standalone cross-shard command errors do not pause the coordinator", %{ctx: ctx} do
    {source, destination} = different_shard_keys(ctx)
    coordinator = min(Router.shard_for(ctx, source), Router.shard_for(ctx, destination))
    coordinator_name = elem(ctx.shard_names, coordinator)
    coordinator_pid = Process.whereis(coordinator_name)

    assert {:error, "ERR no such key"} =
             Ferricstore.Commands.Generic.handle("RENAME", [source, destination], ctx)

    assert %{writes_paused: false, last_flush_error: nil} = :sys.get_state(coordinator_pid)

    coordinator_key =
      if Router.shard_for(ctx, source) == coordinator, do: source, else: destination

    assert :ok = Router.put(ctx, coordinator_key, "after-command-error", 0)
    assert "after-command-error" == Router.get(ctx, coordinator_key)
  end

  test "visible terminal after file-sync failure prevents compensation", %{ctx: ctx} do
    previous_trap_exit = Process.flag(:trap_exit, true)
    on_exit(fn -> Process.flag(:trap_exit, previous_trap_exit) end)

    {first, second} = different_shard_keys(ctx)
    assert :ok = Router.put(ctx, first, "old-first", 0)
    assert :ok = Router.put(ctx, second, "old-second", 0)

    append_calls = :atomics.new(1, signed: false)
    previous_hook = Application.get_env(:ferricstore, :standalone_tx_log_append_hook)

    Application.put_env(:ferricstore, :standalone_tx_log_append_hook, fn path, payload, limit ->
      case :atomics.add_get(append_calls, 1, 1) do
        2 ->
          with :ok <- Ferricstore.FS.append_sync_nofollow_bounded(path, payload, limit) do
            {:error, :eio}
          end

        _ ->
          Ferricstore.FS.append_sync_nofollow_bounded(path, payload, limit)
      end
    end)

    on_exit(fn ->
      if previous_hook do
        Application.put_env(:ferricstore, :standalone_tx_log_append_hook, previous_hook)
      else
        Application.delete_env(:ferricstore, :standalone_tx_log_append_hook)
      end
    end)

    assert :ok =
             CrossShardOp.execute(
               [{first, :write}, {second, :write}],
               fn store ->
                 :ok = store.put.(first, "new-first", 0)
                 :ok = store.put.(second, "new-second", 0)
               end,
               instance: ctx
             )

    assert :atomics.get(append_calls, 1) == 2
    assert Router.batch_get(ctx, [first, second]) == ["new-first", "new-second"]

    Application.delete_env(:ferricstore, :standalone_tx_log_append_hook)

    indexes = Enum.uniq([Router.shard_for(ctx, first), Router.shard_for(ctx, second)])

    Enum.each(indexes, fn index ->
      shard_name = Router.shard_name(ctx, index)
      shard_pid = Process.whereis(shard_name)
      Process.exit(shard_pid, :kill)

      assert :ok =
               ShardHelpers.eventually(
                 fn -> Process.whereis(shard_name) == nil end,
                 "shard #{index} did not stop before restart",
                 50,
                 20
               )
    end)

    Enum.each(indexes, fn index ->
      assert {:ok, _pid} =
               Ferricstore.Store.Shard.start_link(
                 index: index,
                 data_dir: ctx.data_dir,
                 instance_ctx: ctx
               )
    end)

    assert Router.batch_get(ctx, [first, second]) == ["new-first", "new-second"]
  end

  test "failed prepare cannot replay an orphan over a later participant write", %{ctx: ctx} do
    previous_trap_exit = Process.flag(:trap_exit, true)
    on_exit(fn -> Process.flag(:trap_exit, previous_trap_exit) end)

    {first, second} = different_shard_keys(ctx)

    {coordinator_key, participant_key} =
      if Router.shard_for(ctx, first) < Router.shard_for(ctx, second) do
        {first, second}
      else
        {second, first}
      end

    assert :ok = Router.put(ctx, coordinator_key, "old-coordinator", 0)
    assert :ok = Router.put(ctx, participant_key, "old-participant", 0)

    previous_append_hook = Application.get_env(:ferricstore, :standalone_tx_log_append_hook)
    previous_file_hook = Application.get_env(:ferricstore, :standalone_tx_log_fsync_file_hook)

    Application.put_env(:ferricstore, :standalone_tx_log_append_hook, fn path, payload, limit ->
      with :ok <- Ferricstore.FS.append_sync_nofollow_bounded(path, payload, limit) do
        {:error, :prepare_append_eio}
      end
    end)

    Application.put_env(:ferricstore, :standalone_tx_log_fsync_file_hook, fn _path ->
      {:error, :prepare_file_eio}
    end)

    on_exit(fn ->
      if previous_append_hook do
        Application.put_env(:ferricstore, :standalone_tx_log_append_hook, previous_append_hook)
      else
        Application.delete_env(:ferricstore, :standalone_tx_log_append_hook)
      end

      if previous_file_hook do
        Application.put_env(:ferricstore, :standalone_tx_log_fsync_file_hook, previous_file_hook)
      else
        Application.delete_env(:ferricstore, :standalone_tx_log_fsync_file_hook)
      end
    end)

    assert {:error,
            {:standalone_durability_failed,
             {:bitcask_append_failed, {:standalone_tx_prepare_failed, :prepare_append_eio}}}} =
             CrossShardOp.execute(
               [{coordinator_key, :write}, {participant_key, :write}],
               fn store ->
                 :ok = store.put.(coordinator_key, "new-coordinator", 0)
                 :ok = store.put.(participant_key, "new-participant", 0)
               end,
               instance: ctx
             )

    assert {:error,
            {:standalone_cross_shard_busy,
             {:standalone_durability_failed, :prior_standalone_write_failed}}} =
             CrossShardOp.execute(
               [{coordinator_key, :write}, {participant_key, :write}],
               fn _store -> flunk("cross-shard writes must remain blocked until recovery") end,
               instance: ctx
             )

    Application.delete_env(:ferricstore, :standalone_tx_log_append_hook)
    Application.delete_env(:ferricstore, :standalone_tx_log_fsync_file_hook)

    assert :ok = Router.put(ctx, participant_key, "later-participant", 0)
    assert Router.get(ctx, participant_key) == "later-participant"

    indexes =
      Enum.uniq([Router.shard_for(ctx, coordinator_key), Router.shard_for(ctx, participant_key)])

    pids = Enum.map(indexes, &{&1, Process.whereis(Router.shard_name(ctx, &1))})

    Enum.each(pids, fn {_index, pid} -> Process.exit(pid, :kill) end)

    Enum.each(indexes, fn index ->
      assert :ok =
               ShardHelpers.eventually(
                 fn -> Process.whereis(Router.shard_name(ctx, index)) == nil end,
                 "shard #{index} did not stop before restart",
                 100,
                 20
               )
    end)

    Enum.each(indexes, fn index ->
      assert {:ok, _pid} =
               Ferricstore.Store.Shard.start_link(
                 index: index,
                 data_dir: ctx.data_dir,
                 instance_ctx: ctx
               )
    end)

    assert Router.get(ctx, coordinator_key) == "old-coordinator"
    assert Router.get(ctx, participant_key) == "later-participant"
  end

  test "failed prepare rollback fences participant routes until recovery", %{ctx: ctx} do
    previous_trap_exit = Process.flag(:trap_exit, true)
    on_exit(fn -> Process.flag(:trap_exit, previous_trap_exit) end)

    {first, second} = different_shard_keys(ctx)

    {coordinator_key, participant_key} =
      if Router.shard_for(ctx, first) < Router.shard_for(ctx, second) do
        {first, second}
      else
        {second, first}
      end

    assert :ok = Router.put(ctx, coordinator_key, "old-coordinator", 0)
    assert :ok = Router.put(ctx, participant_key, "old-participant", 0)

    previous_append_hook = Application.get_env(:ferricstore, :standalone_tx_log_append_hook)
    previous_file_hook = Application.get_env(:ferricstore, :standalone_tx_log_fsync_file_hook)
    previous_dir_hook = Application.get_env(:ferricstore, :standalone_tx_log_fsync_dir_hook)

    Application.put_env(:ferricstore, :standalone_tx_log_append_hook, fn path, payload, limit ->
      with :ok <- Ferricstore.FS.append_sync_nofollow_bounded(path, payload, limit) do
        {:error, :prepare_append_eio}
      end
    end)

    Application.put_env(:ferricstore, :standalone_tx_log_fsync_file_hook, fn _path ->
      {:error, :prepare_file_eio}
    end)

    Application.put_env(:ferricstore, :standalone_tx_log_fsync_dir_hook, fn _path ->
      {:error, :rollback_eio}
    end)

    on_exit(fn ->
      if previous_append_hook do
        Application.put_env(:ferricstore, :standalone_tx_log_append_hook, previous_append_hook)
      else
        Application.delete_env(:ferricstore, :standalone_tx_log_append_hook)
      end

      if previous_file_hook do
        Application.put_env(:ferricstore, :standalone_tx_log_fsync_file_hook, previous_file_hook)
      else
        Application.delete_env(:ferricstore, :standalone_tx_log_fsync_file_hook)
      end

      if previous_dir_hook do
        Application.put_env(:ferricstore, :standalone_tx_log_fsync_dir_hook, previous_dir_hook)
      else
        Application.delete_env(:ferricstore, :standalone_tx_log_fsync_dir_hook)
      end
    end)

    assert {:error,
            {:standalone_durability_failed,
             {:bitcask_append_failed,
              {:standalone_tx_prepare_failed,
               {:standalone_tx_prepare_recovery_required, _txid, :prepare_append_eio,
                :prepare_file_eio, :rollback_eio}}}}} =
             CrossShardOp.execute(
               [{coordinator_key, :write}, {participant_key, :write}],
               fn store ->
                 :ok = store.put.(coordinator_key, "new-coordinator", 0)
                 :ok = store.put.(participant_key, "new-participant", 0)
               end,
               instance: ctx
             )

    assert {:error, "ERR shard writes paused for sync"} =
             Router.put(ctx, participant_key, "must-not-publish", 0)

    assert Router.get(ctx, participant_key) == "old-participant"

    Application.delete_env(:ferricstore, :standalone_tx_log_append_hook)
    Application.delete_env(:ferricstore, :standalone_tx_log_fsync_file_hook)
    Application.delete_env(:ferricstore, :standalone_tx_log_fsync_dir_hook)

    indexes =
      Enum.uniq([Router.shard_for(ctx, coordinator_key), Router.shard_for(ctx, participant_key)])

    pids = Enum.map(indexes, &{&1, Process.whereis(Router.shard_name(ctx, &1))})
    Enum.each(pids, fn {_index, pid} -> Process.exit(pid, :kill) end)

    Enum.each(indexes, fn index ->
      assert :ok =
               ShardHelpers.eventually(
                 fn -> Process.whereis(Router.shard_name(ctx, index)) == nil end,
                 "shard #{index} did not stop before restart",
                 100,
                 20
               )
    end)

    Enum.each(indexes, fn index ->
      assert {:ok, _pid} =
               Ferricstore.Store.Shard.start_link(
                 index: index,
                 data_dir: ctx.data_dir,
                 instance_ctx: ctx
               )
    end)

    assert Router.get(ctx, coordinator_key) == "old-coordinator"
    assert Router.get(ctx, participant_key) == "old-participant"
    assert :ok = Router.put(ctx, participant_key, "after-recovery", 0)
  end

  test "compensation failure fences partial participant appends until recovery", %{ctx: ctx} do
    previous_trap_exit = Process.flag(:trap_exit, true)
    on_exit(fn -> Process.flag(:trap_exit, previous_trap_exit) end)

    {first, second} = different_shard_keys(ctx)
    coordinator_index = min(Router.shard_for(ctx, first), Router.shard_for(ctx, second))
    participant_index = max(Router.shard_for(ctx, first), Router.shard_for(ctx, second))

    {coordinator_key, participant_key} =
      if Router.shard_for(ctx, first) == coordinator_index do
        {first, second}
      else
        {second, first}
      end

    assert :ok = Router.put(ctx, coordinator_key, "old-coordinator", 0)
    assert :ok = Router.put(ctx, participant_key, "old-participant", 0)

    coordinator_name = Router.shard_name(ctx, coordinator_index)
    participant_name = Router.shard_name(ctx, participant_index)
    {_, coordinator_file_path} = GenServer.call(coordinator_name, :get_active_file)
    {_, participant_file_path} = GenServer.call(participant_name, :get_active_file)
    append_stage = :atomics.new(1, signed: false)
    previous_hook = Application.get_env(:ferricstore, :pending_append_hook)

    Application.put_env(:ferricstore, :pending_append_hook, fn path, batch ->
      cond do
        path == participant_file_path and :atomics.get(append_stage, 1) == 0 ->
          :atomics.put(append_stage, 1, 1)

          [{:put, key, value, expire_at_ms}] = batch
          assert {:ok, _locations} = NIF.v2_append_batch(path, [{key, value, expire_at_ms}])
          {:error, :participant_append_eio}

        path == coordinator_file_path and :atomics.get(append_stage, 1) == 1 ->
          :atomics.put(append_stage, 1, 2)
          {:error, :compensation_append_eio}

        true ->
          :passthrough
      end
    end)

    on_exit(fn -> restore_env(:pending_append_hook, previous_hook) end)

    assert {:error,
            {:standalone_durability_failed,
             {:cross_shard_compensation_failed,
              {:standalone_tx_compensation_recovery_required, txid,
               {:compensation_append_failed, :compensation_append_eio}}}}} =
             CrossShardOp.execute(
               [{coordinator_key, :write}, {participant_key, :write}],
               fn store ->
                 :ok = store.put.(coordinator_key, "new-coordinator", 0)
                 :ok = store.put.(participant_key, "new-participant", 0)
               end,
               instance: ctx
             )

    assert is_binary(txid)
    assert :atomics.get(append_stage, 1) == 2
    assert StandaloneTxLog.recovery_required?(ctx.data_dir)

    assert {:error, "ERR shard writes paused for sync"} =
             Router.put(ctx, participant_key, "must-not-publish", 0)

    assert Router.get(ctx, coordinator_key) == "old-coordinator"
    assert Router.get(ctx, participant_key) == "old-participant"

    Application.delete_env(:ferricstore, :pending_append_hook)
    restart_shards(ctx, [coordinator_index, participant_index])

    refute StandaloneTxLog.recovery_required?(ctx.data_dir)
    refute File.exists?(Path.join(ctx.data_dir, "standalone_cross_shard_tx.log"))
    assert Router.get(ctx, coordinator_key) == "old-coordinator"
    assert Router.get(ctx, participant_key) == "old-participant"
    assert :ok = Router.put(ctx, participant_key, "after-compensation-recovery", 0)
  end

  test "abort-terminal persistence failure fences participants until recovery", %{ctx: ctx} do
    previous_trap_exit = Process.flag(:trap_exit, true)
    on_exit(fn -> Process.flag(:trap_exit, previous_trap_exit) end)

    {first, second} = different_shard_keys(ctx)
    coordinator_index = min(Router.shard_for(ctx, first), Router.shard_for(ctx, second))
    participant_index = max(Router.shard_for(ctx, first), Router.shard_for(ctx, second))

    {coordinator_key, participant_key} =
      if Router.shard_for(ctx, first) == coordinator_index do
        {first, second}
      else
        {second, first}
      end

    assert :ok = Router.put(ctx, coordinator_key, "old-coordinator", 0)
    assert :ok = Router.put(ctx, participant_key, "old-participant", 0)

    participant_name = Router.shard_name(ctx, participant_index)
    {_, participant_file_path} = GenServer.call(participant_name, :get_active_file)
    participant_failed = :atomics.new(1, signed: false)
    journal_append_calls = :atomics.new(1, signed: false)
    directory_fsync_calls = :atomics.new(1, signed: false)
    previous_pending_hook = Application.get_env(:ferricstore, :pending_append_hook)
    previous_append_hook = Application.get_env(:ferricstore, :standalone_tx_log_append_hook)
    previous_dir_hook = Application.get_env(:ferricstore, :standalone_tx_log_fsync_dir_hook)

    Application.put_env(:ferricstore, :pending_append_hook, fn path, _batch ->
      if path == participant_file_path and :atomics.get(participant_failed, 1) == 0 do
        :atomics.put(participant_failed, 1, 1)
        {:error, :participant_append_eio}
      else
        :passthrough
      end
    end)

    Application.put_env(:ferricstore, :standalone_tx_log_append_hook, fn path, payload, limit ->
      case :atomics.add_get(journal_append_calls, 1, 1) do
        2 -> {:error, :abort_append_eio}
        _ -> Ferricstore.FS.append_sync_nofollow_bounded(path, payload, limit)
      end
    end)

    Application.put_env(:ferricstore, :standalone_tx_log_fsync_dir_hook, fn path ->
      case :atomics.add_get(directory_fsync_calls, 1, 1) do
        2 -> {:error, :abort_restore_eio}
        _ -> NIF.v2_fsync_dir(path)
      end
    end)

    on_exit(fn ->
      restore_env(:pending_append_hook, previous_pending_hook)
      restore_env(:standalone_tx_log_append_hook, previous_append_hook)
      restore_env(:standalone_tx_log_fsync_dir_hook, previous_dir_hook)
    end)

    assert {:error,
            {:standalone_durability_failed,
             {:cross_shard_compensation_failed,
              {:standalone_tx_abort_recovery_required, txid, _abort_reason}}}} =
             CrossShardOp.execute(
               [{coordinator_key, :write}, {participant_key, :write}],
               fn store ->
                 :ok = store.put.(coordinator_key, "new-coordinator", 0)
                 :ok = store.put.(participant_key, "new-participant", 0)
               end,
               instance: ctx
             )

    assert is_binary(txid)
    assert :atomics.get(participant_failed, 1) == 1
    assert :atomics.get(journal_append_calls, 1) == 2
    assert :atomics.get(directory_fsync_calls, 1) >= 2
    assert StandaloneTxLog.recovery_required?(ctx.data_dir)

    assert {:error, "ERR shard writes paused for sync"} =
             Router.put(ctx, participant_key, "must-not-publish", 0)

    assert Router.get(ctx, coordinator_key) == "old-coordinator"
    assert Router.get(ctx, participant_key) == "old-participant"

    Application.delete_env(:ferricstore, :pending_append_hook)
    Application.delete_env(:ferricstore, :standalone_tx_log_append_hook)
    Application.delete_env(:ferricstore, :standalone_tx_log_fsync_dir_hook)
    restart_shards(ctx, [coordinator_index, participant_index])

    refute StandaloneTxLog.recovery_required?(ctx.data_dir)
    refute File.exists?(Path.join(ctx.data_dir, "standalone_cross_shard_tx.log"))
    assert Router.get(ctx, coordinator_key) == "old-coordinator"
    assert Router.get(ctx, participant_key) == "old-participant"
    assert :ok = Router.put(ctx, participant_key, "after-abort-recovery", 0)
  end

  test "standalone cross-shard callbacks cannot read undeclared keys", %{ctx: ctx} do
    {first, second} = different_shard_keys(ctx)
    undeclared = first <> ":undeclared"
    assert :ok = Router.put(ctx, undeclared, "secret", 0)

    assert {:error, {:cross_shard_footprint_violation, :read, ^undeclared}} =
             CrossShardOp.execute(
               [{first, :read}, {second, :read}],
               fn store -> store.get.(undeclared) end,
               instance: ctx
             )

    assert "secret" == Router.get(ctx, undeclared)
  end

  test "standalone cross-shard callbacks cannot write read-only keys", %{ctx: ctx} do
    {first, second} = different_shard_keys(ctx)
    assert :ok = Router.put(ctx, first, "original", 0)

    assert {:error, {:cross_shard_footprint_violation, :write, ^first}} =
             CrossShardOp.execute(
               [{first, :read}, {second, :read}],
               fn store -> store.put.(first, "replacement", 0) end,
               instance: ctx
             )

    assert "original" == Router.get(ctx, first)
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

  test "coordinator death after journal commit restarts participants before releasing barriers",
       %{
         ctx: ctx
       } do
    previous_trap_exit = Process.flag(:trap_exit, true)
    on_exit(fn -> Process.flag(:trap_exit, previous_trap_exit) end)

    {first, second} = different_shard_keys(ctx)
    assert :ok = Router.put(ctx, first, "old-1", 0)
    assert :ok = Router.put(ctx, second, "old-2", 0)

    first_index = Router.shard_for(ctx, first)
    second_index = Router.shard_for(ctx, second)
    coordinator_index = min(first_index, second_index)
    participant_index = max(first_index, second_index)
    coordinator_name = Router.shard_name(ctx, coordinator_index)
    participant_name = Router.shard_name(ctx, participant_index)
    coordinator_pid = Process.whereis(coordinator_name)
    participant_pid = Process.whereis(participant_name)
    parent = self()
    fsync_calls = :atomics.new(1, signed: false)
    previous_hook = Application.get_env(:ferricstore, :standalone_tx_log_fsync_dir_hook)

    Application.put_env(:ferricstore, :standalone_tx_log_fsync_dir_hook, fn path ->
      :ok = NIF.v2_fsync_dir(path)

      if :atomics.add_get(fsync_calls, 1, 1) == 2 do
        send(parent, :standalone_journal_commit_durable)
        Process.exit(self(), :kill)
      end

      :ok
    end)

    on_exit(fn ->
      if previous_hook do
        Application.put_env(:ferricstore, :standalone_tx_log_fsync_dir_hook, previous_hook)
      else
        Application.delete_env(:ferricstore, :standalone_tx_log_fsync_dir_hook)
      end
    end)

    spawn(fn ->
      result =
        CrossShardOp.execute(
          [{first, :write}, {second, :write}],
          fn store ->
            :ok = store.put.(first, "new-1", 0)
            :ok = store.put.(second, "new-2", 0)
          end,
          instance: ctx
        )

      send(parent, {:cross_shard_crash_result, result})
    end)

    assert_receive :standalone_journal_commit_durable, 2_000

    assert_receive {:cross_shard_crash_result,
                    {:error, {:standalone_cross_shard_failed, _reason}}},
                   5_000

    assert_receive {:EXIT, ^coordinator_pid, :killed}, 2_000
    assert Process.whereis(coordinator_name) == nil

    assert :ok =
             ShardHelpers.eventually(
               fn ->
                 Process.whereis(participant_name) == nil
               end,
               "participant released a potentially stale keydir instead of restarting",
               100,
               20
             )

    assert_receive {:EXIT, ^participant_pid, _reason}, 2_000

    for index <- [coordinator_index, participant_index] do
      assert {:ok, _pid} =
               Ferricstore.Store.Shard.start_link(
                 index: index,
                 data_dir: ctx.data_dir,
                 instance_ctx: ctx
               )
    end

    values = Router.batch_get(ctx, [first, second])

    assert values == ["new-1", "new-2"],
           "recovered #{inspect(values)} for shard indexes #{inspect([first_index, second_index])}"
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
    source_index = Router.shard_for(ctx, source)
    destination_index = Router.shard_for(ctx, destination)
    coordinator = min(source_index, destination_index)
    participant = max(source_index, destination_index)
    coordinator_name = elem(ctx.shard_names, coordinator)
    participant_name = elem(ctx.shard_names, participant)
    participant_pid = Process.whereis(participant_name)

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

    assert :ok =
             ShardHelpers.eventually(
               fn -> Process.whereis(participant_name) == nil end,
               "participant released a potentially stale keydir instead of restarting",
               100,
               20
             )

    assert_receive {:EXIT, ^participant_pid, _reason}, 2_000

    refute StandaloneTxLog.recovery_required?(ctx.data_dir)

    for index <- [coordinator, participant] do
      assert {:ok, _pid} =
               Ferricstore.Store.Shard.start_link(
                 index: index,
                 data_dir: ctx.data_dir,
                 instance_ctx: ctx
               )
    end

    assert_receive {:standalone_recovery, [:ferricstore, :standalone_tx_log, :recover],
                    %{pending: 1, replayed: 1}, %{status: :ok}},
                   1_000

    assert ["first", "second"] = Router.list_op(ctx, source, {:lrange, 0, -1})
    assert [] = Router.list_op(ctx, destination, {:lrange, 0, -1})
  end

  test "compensation recovery fences a restarted participant until owner recovery", %{ctx: ctx} do
    previous_trap_exit = Process.flag(:trap_exit, true)
    on_exit(fn -> Process.flag(:trap_exit, previous_trap_exit) end)

    {first, second} = different_shard_keys(ctx)
    coordinator_index = min(Router.shard_for(ctx, first), Router.shard_for(ctx, second))
    participant_index = max(Router.shard_for(ctx, first), Router.shard_for(ctx, second))

    {coordinator_key, participant_key} =
      if Router.shard_for(ctx, first) == coordinator_index do
        {first, second}
      else
        {second, first}
      end

    assert :ok = Router.put(ctx, coordinator_key, "old-coordinator", 0)
    assert :ok = Router.put(ctx, participant_key, "old-participant", 0)

    coordinator_name = Router.shard_name(ctx, coordinator_index)
    participant_name = Router.shard_name(ctx, participant_index)
    {_, coordinator_file_path} = GenServer.call(coordinator_name, :get_active_file)
    {_, participant_file_path} = GenServer.call(participant_name, :get_active_file)
    append_stage = :atomics.new(1, signed: false)
    previous_hook = Application.get_env(:ferricstore, :pending_append_hook)

    Application.put_env(:ferricstore, :pending_append_hook, fn path, batch ->
      cond do
        path == participant_file_path and :atomics.get(append_stage, 1) == 0 ->
          :atomics.put(append_stage, 1, 1)
          [{:put, key, value, expire_at_ms}] = batch
          assert {:ok, _locations} = NIF.v2_append_batch(path, [{key, value, expire_at_ms}])
          {:error, :participant_append_eio}

        path == coordinator_file_path and :atomics.get(append_stage, 1) == 1 ->
          :atomics.put(append_stage, 1, 2)
          {:error, :compensation_append_eio}

        true ->
          :passthrough
      end
    end)

    on_exit(fn -> restore_env(:pending_append_hook, previous_hook) end)

    assert {:error,
            {:standalone_durability_failed,
             {:cross_shard_compensation_failed,
              {:standalone_tx_compensation_recovery_required, txid, _reason}}}} =
             CrossShardOp.execute(
               [{coordinator_key, :write}, {participant_key, :write}],
               fn store ->
                 :ok = store.put.(coordinator_key, "new-coordinator", 0)
                 :ok = store.put.(participant_key, "new-participant", 0)
               end,
               instance: ctx
             )

    assert is_binary(txid)
    assert :atomics.get(append_stage, 1) == 2
    assert StandaloneTxLog.recovery_required?(ctx.data_dir)

    Application.delete_env(:ferricstore, :pending_append_hook)

    participant_pid = Process.whereis(participant_name)
    Process.exit(participant_pid, :kill)

    assert :ok =
             ShardHelpers.eventually(
               fn -> Process.whereis(participant_name) == nil end,
               "participant did not stop before restart",
               100,
               20
             )

    assert {:ok, _participant_pid} =
             Ferricstore.Store.Shard.start_link(
               index: participant_index,
               data_dir: ctx.data_dir,
               instance_ctx: ctx
             )

    assert StandaloneTxLog.recovery_required?(ctx.data_dir)

    assert {:error, "ERR shard writes paused for sync"} =
             Router.put(ctx, participant_key, "must-not-publish", 0)

    opts = %{expire_at_ms: 0, get: false, keepttl: false, nx: true, xx: false}

    assert {:error, "ERR shard writes paused for sync"} =
             Router.set(ctx, participant_key, "must-not-publish", opts)

    coordinator_pid = Process.whereis(coordinator_name)
    Process.exit(coordinator_pid, :kill)

    assert :ok =
             ShardHelpers.eventually(
               fn -> Process.whereis(coordinator_name) == nil end,
               "coordinator did not stop before recovery restart",
               100,
               20
             )

    assert {:ok, _coordinator_pid} =
             Ferricstore.Store.Shard.start_link(
               index: coordinator_index,
               data_dir: ctx.data_dir,
               instance_ctx: ctx
             )

    refute StandaloneTxLog.recovery_required?(ctx.data_dir)

    participant_pid = Process.whereis(participant_name)
    Process.exit(participant_pid, :kill)

    assert :ok =
             ShardHelpers.eventually(
               fn -> Process.whereis(participant_name) == nil end,
               "participant did not stop before keydir rebuild",
               100,
               20
             )

    assert {:ok, _participant_pid} =
             Ferricstore.Store.Shard.start_link(
               index: participant_index,
               data_dir: ctx.data_dir,
               instance_ctx: ctx
             )

    assert "old-coordinator" == Router.get(ctx, coordinator_key)
    assert "old-participant" == Router.get(ctx, participant_key)
    assert :ok = Router.put(ctx, participant_key, "after-recovery", 0)
  end

  test "durable recovery fence survives a VM-like restart with participant first", %{ctx: ctx} do
    previous_trap_exit = Process.flag(:trap_exit, true)
    on_exit(fn -> Process.flag(:trap_exit, previous_trap_exit) end)

    {first, second} = different_shard_keys(ctx)
    coordinator_index = min(Router.shard_for(ctx, first), Router.shard_for(ctx, second))
    participant_index = max(Router.shard_for(ctx, first), Router.shard_for(ctx, second))

    coordinator_key =
      if Router.shard_for(ctx, first) == coordinator_index, do: first, else: second

    participant_key = if coordinator_key == first, do: second, else: first

    assert :ok = Router.put(ctx, coordinator_key, "durable-old-coordinator", 0)
    assert :ok = Router.put(ctx, participant_key, "durable-old-participant", 0)

    coordinator_name = Router.shard_name(ctx, coordinator_index)
    participant_name = Router.shard_name(ctx, participant_index)
    {_, coordinator_file_path} = GenServer.call(coordinator_name, :get_active_file)
    {_, participant_file_path} = GenServer.call(participant_name, :get_active_file)
    append_stage = :atomics.new(1, signed: false)
    previous_hook = Application.get_env(:ferricstore, :pending_append_hook)

    Application.put_env(:ferricstore, :pending_append_hook, fn path, batch ->
      cond do
        path == participant_file_path and :atomics.get(append_stage, 1) == 0 ->
          :atomics.put(append_stage, 1, 1)
          [{:put, key, value, expire_at_ms}] = batch
          assert {:ok, _locations} = NIF.v2_append_batch(path, [{key, value, expire_at_ms}])
          {:error, :participant_append_eio}

        path == coordinator_file_path and :atomics.get(append_stage, 1) == 1 ->
          :atomics.put(append_stage, 1, 2)
          {:error, :compensation_append_eio}

        true ->
          :passthrough
      end
    end)

    on_exit(fn -> restore_env(:pending_append_hook, previous_hook) end)

    assert {:error,
            {:standalone_durability_failed,
             {:cross_shard_compensation_failed,
              {:standalone_tx_compensation_recovery_required, _txid, _reason}}}} =
             CrossShardOp.execute(
               [{coordinator_key, :write}, {participant_key, :write}],
               fn store ->
                 :ok = store.put.(coordinator_key, "durable-new-coordinator", 0)
                 :ok = store.put.(participant_key, "durable-new-participant", 0)
               end,
               instance: ctx
             )

    assert StandaloneTxLog.recovery_required?(ctx.data_dir)
    Application.delete_env(:ferricstore, :pending_append_hook)

    for index <- [coordinator_index, participant_index] do
      name = Router.shard_name(ctx, index)
      pid = Process.whereis(name)
      Process.exit(pid, :kill)

      assert :ok =
               ShardHelpers.eventually(
                 fn -> Process.whereis(name) == nil end,
                 "shard did not stop for VM-like restart",
                 100,
                 20
               )
    end

    :persistent_term.erase({StandaloneTxLog, :recovery_required, Path.expand(ctx.data_dir)})
    assert StandaloneTxLog.recovery_required?(ctx.data_dir)

    assert {:ok, _participant_pid} =
             Ferricstore.Store.Shard.start_link(
               index: participant_index,
               data_dir: ctx.data_dir,
               instance_ctx: ctx
             )

    assert StandaloneTxLog.recovery_required?(ctx.data_dir)

    assert {:error, "ERR shard writes paused for sync"} =
             Router.put(ctx, participant_key, "must-not-overwrite-undo", 0)

    assert {:ok, _coordinator_pid} =
             Ferricstore.Store.Shard.start_link(
               index: coordinator_index,
               data_dir: ctx.data_dir,
               instance_ctx: ctx
             )

    refute StandaloneTxLog.recovery_required?(ctx.data_dir)

    participant_pid = Process.whereis(participant_name)
    Process.exit(participant_pid, :kill)

    assert :ok =
             ShardHelpers.eventually(
               fn -> Process.whereis(participant_name) == nil end,
               "participant did not stop before post-recovery rebuild",
               100,
               20
             )

    assert {:ok, _participant_pid} =
             Ferricstore.Store.Shard.start_link(
               index: participant_index,
               data_dir: ctx.data_dir,
               instance_ctx: ctx
             )

    assert "durable-old-coordinator" == Router.get(ctx, coordinator_key)
    assert "durable-old-participant" == Router.get(ctx, participant_key)
  end

  test "prepare fsync failure persists the recovery fence before participant release", %{ctx: ctx} do
    {first, second} = different_shard_keys(ctx)
    fsync_calls = :atomics.new(1, signed: false)
    previous_hook = Application.get_env(:ferricstore, :standalone_tx_log_fsync_dir_hook)

    Application.put_env(:ferricstore, :standalone_tx_log_fsync_dir_hook, fn path ->
      if :atomics.add_get(fsync_calls, 1, 1) == 1 do
        {:error, :prepare_fsync_eio}
      else
        NIF.v2_fsync_dir(path)
      end
    end)

    on_exit(fn -> restore_env(:standalone_tx_log_fsync_dir_hook, previous_hook) end)

    assert {:error, _reason} =
             CrossShardOp.execute(
               [{first, :write}, {second, :write}],
               fn store ->
                 :ok = store.put.(first, "prepare-failure-1", 0)
                 :ok = store.put.(second, "prepare-failure-2", 0)
               end,
               instance: ctx
             )

    assert :atomics.get(fsync_calls, 1) >= 2
    assert StandaloneTxLog.recovery_required?(ctx.data_dir)
    assert File.exists?(StandaloneTxLog.recovery_marker_path(ctx.data_dir))

    assert {:error, "ERR shard writes paused for sync"} =
             Router.put(ctx, first, "must-not-write", 0)

    Application.delete_env(:ferricstore, :standalone_tx_log_fsync_dir_hook)
    assert :ok = StandaloneTxLog.recover(ctx.data_dir)
    refute StandaloneTxLog.recovery_required?(ctx.data_dir)
  end

  test "participant-first startup fences a pending journal without a recovery marker", %{
    ctx: ctx
  } do
    previous_trap_exit = Process.flag(:trap_exit, true)
    on_exit(fn -> Process.flag(:trap_exit, previous_trap_exit) end)

    {first, second} = different_shard_keys(ctx)
    coordinator_index = min(Router.shard_for(ctx, first), Router.shard_for(ctx, second))
    participant_index = max(Router.shard_for(ctx, first), Router.shard_for(ctx, second))

    coordinator_key =
      if Router.shard_for(ctx, first) == coordinator_index, do: first, else: second

    participant_key = if coordinator_key == first, do: second, else: first
    resume_key = participant_key <> ":resume"
    coordinator_name = Router.shard_name(ctx, coordinator_index)
    participant_name = Router.shard_name(ctx, participant_index)

    assert :ok = Router.put(ctx, coordinator_key, "old-coordinator", 0)
    assert :ok = Router.put(ctx, participant_key, "old-participant", 0)

    {_, coordinator_file_path} = GenServer.call(coordinator_name, :get_active_file)
    {_, participant_file_path} = GenServer.call(participant_name, :get_active_file)

    assert {:ok, _txid} =
             StandaloneTxLog.prepare(ctx.data_dir, [
               {coordinator_file_path, [{:put, coordinator_key, "prepared-coordinator", 0}]},
               {participant_file_path, [{:put, participant_key, "prepared-participant", 0}]}
             ])

    refute StandaloneTxLog.recovery_required?(ctx.data_dir)

    assert {:ok, _} =
             NIF.v2_append_batch(
               coordinator_file_path,
               [{coordinator_key, "prepared-coordinator", 0}]
             )

    assert {:ok, _} =
             NIF.v2_append_batch(
               participant_file_path,
               [{participant_key, "prepared-participant", 0}]
             )

    running_key = participant_key <> ":running"
    assert :ok = Router.put(ctx, running_key, "running-before-restart", 0)

    for index <- [coordinator_index, participant_index] do
      name = Router.shard_name(ctx, index)
      pid = Process.whereis(name)
      Process.exit(pid, :kill)

      assert :ok =
               ShardHelpers.eventually(
                 fn -> Process.whereis(name) == nil end,
                 "shard did not stop before participant-first journal restart",
                 100,
                 20
               )
    end

    :persistent_term.erase({StandaloneTxLog, :recovery_required, Path.expand(ctx.data_dir)})

    assert {:ok, participant_pid} =
             Ferricstore.Store.Shard.start_link(
               index: participant_index,
               data_dir: ctx.data_dir,
               instance_ctx: ctx
             )

    assert %{writes_paused: true} = :sys.get_state(participant_pid)

    assert {:error, "ERR shard writes paused for sync"} =
             Router.put(ctx, participant_key, "must-not-overwrite-prepared", 0)

    assert {:ok, _coordinator_pid} =
             Ferricstore.Store.Shard.start_link(
               index: coordinator_index,
               data_dir: ctx.data_dir,
               instance_ctx: ctx
             )

    assert %{writes_paused: false, last_flush_error: nil} = :sys.get_state(participant_pid)
    assert "prepared-coordinator" == Router.get(ctx, coordinator_key)
    assert "prepared-participant" == Router.get(ctx, participant_key)
    assert "running-before-restart" == Router.get(ctx, running_key)

    assert :ok = Router.put(ctx, resume_key, "resumed", 0)

    Process.exit(participant_pid, :kill)

    assert :ok =
             ShardHelpers.eventually(
               fn -> Process.whereis(participant_name) == nil end,
               "participant did not stop before post-recovery restart",
               100,
               20
             )

    assert {:ok, _participant_pid} =
             Ferricstore.Store.Shard.start_link(
               index: participant_index,
               data_dir: ctx.data_dir,
               instance_ctx: ctx
             )

    assert "prepared-participant" == Router.get(ctx, participant_key)
    assert "running-before-restart" == Router.get(ctx, running_key)
    assert "resumed" == Router.get(ctx, resume_key)
    refute File.exists?(Path.join(ctx.data_dir, "standalone_cross_shard_tx.log"))
  end

  test "non-owner restart does not fence on a small committed journal", %{ctx: ctx} do
    {_owner_key, participant_key} = non_owner_shard_keys(ctx)
    participant_index = Router.shard_for(ctx, participant_key)
    participant_name = Router.shard_name(ctx, participant_index)
    {_, participant_file_path} = GenServer.call(participant_name, :get_active_file)

    assert {:ok, txid} =
             StandaloneTxLog.prepare(ctx.data_dir, [
               {participant_file_path, [{:put, participant_key, "committed", 0}]}
             ])

    assert :ok = StandaloneTxLog.commit(ctx.data_dir, txid)
    assert File.exists?(Path.join(ctx.data_dir, "standalone_cross_shard_tx.log"))
    assert is_nil(StandaloneTxLog.startup_recovery_reason(ctx.data_dir))

    assert :ok = GenServer.stop(participant_name, :normal, 5_000)

    assert {:ok, participant_pid} =
             Ferricstore.Store.Shard.start_link(
               index: participant_index,
               data_dir: ctx.data_dir,
               instance_ctx: ctx
             )

    assert %{writes_paused: false, last_flush_error: nil} = :sys.get_state(participant_pid)
    assert :ok = Router.put(ctx, participant_key, "after-committed-journal", 0)
  end

  test "non-owner restart does not fence on a small aborted journal", %{ctx: ctx} do
    {_owner_key, participant_key} = non_owner_shard_keys(ctx)
    participant_index = Router.shard_for(ctx, participant_key)
    participant_name = Router.shard_name(ctx, participant_index)
    {_, participant_file_path} = GenServer.call(participant_name, :get_active_file)

    assert {:ok, txid} =
             StandaloneTxLog.prepare(ctx.data_dir, [
               {participant_file_path, [{:put, participant_key, "aborted", 0}]}
             ])

    assert :ok = StandaloneTxLog.abort(ctx.data_dir, txid)
    assert File.exists?(Path.join(ctx.data_dir, "standalone_cross_shard_tx.log"))
    assert is_nil(StandaloneTxLog.startup_recovery_reason(ctx.data_dir))

    assert :ok = GenServer.stop(participant_name, :normal, 5_000)

    assert {:ok, participant_pid} =
             Ferricstore.Store.Shard.start_link(
               index: participant_index,
               data_dir: ctx.data_dir,
               instance_ctx: ctx
             )

    assert %{writes_paused: false, last_flush_error: nil} = :sys.get_state(participant_pid)
    assert :ok = Router.put(ctx, participant_key, "after-aborted-journal", 0)
  end

  test "non-owner restart fences on a corrupt journal", %{ctx: ctx} do
    {_owner_key, participant_key} = non_owner_shard_keys(ctx)
    participant_index = Router.shard_for(ctx, participant_key)
    participant_name = Router.shard_name(ctx, participant_index)
    journal_path = Path.join(ctx.data_dir, "standalone_cross_shard_tx.log")
    File.write!(journal_path, "not-a-valid-entry\n")

    assert :ok = GenServer.stop(participant_name, :normal, 5_000)

    assert {:ok, participant_pid} =
             Ferricstore.Store.Shard.start_link(
               index: participant_index,
               data_dir: ctx.data_dir,
               instance_ctx: ctx
             )

    assert %{writes_paused: true} = :sys.get_state(participant_pid)

    assert {:error, "ERR shard writes paused for sync"} =
             Router.put(ctx, participant_key, "must-not-write-with-corrupt-journal", 0)
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

  @tag :direct_tx_atomicity
  test "custom shard EXEC rolls back staged writes on a fatal result budget error", %{ctx: ctx} do
    {read_key, staged_key} = same_shard_keys(ctx)
    shard_index = Router.shard_for(ctx, read_key)
    shard_name = elem(ctx.shard_names, shard_index)
    shard_pid = Process.whereis(shard_name)
    limited_context = ApplyContext.new(transaction_result_byte_budget: 5)

    assert :ok = Router.put(ctx, read_key, "1234", 0)

    :sys.replace_state(shard_pid, fn state ->
      %{
        state
        | apply_context: limited_context,
          apply_context_encoded: ApplyContext.encode(limited_context)
      }
    end)

    entries =
      Enum.map(
        [
          {"SET", [staged_key, "staged"]},
          {"GET", [read_key]}
        ],
        fn {command, args} ->
          {:ok, prepared} = Ferricstore.Commands.PreparedCommand.prepare(command, args)
          {:ok, entry} = Ferricstore.Transaction.ExecutionEntry.from_prepared(prepared)
          entry
        end
      )

    assert {:error, :transaction_result_byte_budget_exceeded} =
             GenServer.call(shard_name, {:tx_execute, entries, nil})

    assert %{writes_paused: false, last_flush_error: nil} = :sys.get_state(shard_pid)
    assert nil == Router.get(ctx, staged_key)
    assert "1234" == Router.get(ctx, read_key)
    assert :ok = Router.put(ctx, staged_key, "after-governance-error", 0)
  end

  @tag :direct_tx_durability
  test "custom shard EXEC pauses writes after a durability failure", %{ctx: ctx} do
    {key, later_key} = same_shard_keys(ctx)
    shard_index = Router.shard_for(ctx, key)
    shard_name = elem(ctx.shard_names, shard_index)
    shard_pid = Process.whereis(shard_name)
    previous_hook = Application.get_env(:ferricstore, :standalone_durability_hook)

    Application.put_env(:ferricstore, :standalone_durability_hook, fn _path, _batch ->
      {:error, :enospc}
    end)

    on_exit(fn ->
      if previous_hook do
        Application.put_env(:ferricstore, :standalone_durability_hook, previous_hook)
      else
        Application.delete_env(:ferricstore, :standalone_durability_hook)
      end
    end)

    {:ok, prepared_set} =
      Ferricstore.Commands.PreparedCommand.prepare("SET", [key, "must-not-publish"])

    {:ok, set_entry} =
      Ferricstore.Transaction.ExecutionEntry.from_prepared(prepared_set)

    reason = {:bitcask_append_failed, :enospc}

    assert {:error, {:standalone_durability_failed, ^reason}} =
             GenServer.call(shard_name, {:tx_execute, [set_entry], nil})

    assert %{writes_paused: true, last_flush_error: ^reason} = :sys.get_state(shard_pid)
    assert nil == Router.get(ctx, key)
    assert {:error, "ERR shard writes paused for sync"} = Router.put(ctx, later_key, "blocked", 0)
  end

  @tag :direct_tx_write_version
  test "custom shard EXEC advances WATCH versions only for published mutations", %{ctx: ctx} do
    {existing_key, missing_key} = same_shard_keys(ctx)
    shard_index = Router.shard_for(ctx, existing_key)
    shard_name = elem(ctx.shard_names, shard_index)

    assert :ok = Router.put(ctx, existing_key, "original", 0)
    before_noop = Router.get_version(ctx, existing_key)

    noop_entries =
      Enum.map(
        [
          {"SET", [existing_key, "ignored", "NX"]},
          {"DEL", [missing_key]}
        ],
        fn {command, args} ->
          {:ok, prepared} = Ferricstore.Commands.PreparedCommand.prepare(command, args)
          {:ok, entry} = Ferricstore.Transaction.ExecutionEntry.from_prepared(prepared)
          entry
        end
      )

    assert [nil, 0] = GenServer.call(shard_name, {:tx_execute, noop_entries, nil})
    assert Router.get_version(ctx, existing_key) == before_noop
    assert "original" == Router.get(ctx, existing_key)
    assert nil == Router.get(ctx, missing_key)

    {:ok, prepared_set} =
      Ferricstore.Commands.PreparedCommand.prepare("SET", [missing_key, "written"])

    {:ok, set_entry} =
      Ferricstore.Transaction.ExecutionEntry.from_prepared(prepared_set)

    assert [:ok] = GenServer.call(shard_name, {:tx_execute, [set_entry], nil})
    assert Router.get_version(ctx, existing_key) == before_noop + 1
    assert "written" == Router.get(ctx, missing_key)
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

  @tag :compound_cardinality_index
  test "lazy Router expiry removes exact compound catalog metadata", %{ctx: ctx} do
    key = "router:instance:expired-compound:#{System.unique_integer([:positive])}"
    field_key = CompoundKey.hash_field(key, "field")
    prefix = CompoundKey.hash_prefix(key)
    shard_index = Router.shard_for(ctx, key)
    index = CompoundMemberIndex.table_name(ctx.name, shard_index)

    assert :ok =
             CommandTime.with_now_ms(5, fn ->
               Router.compound_put(ctx, key, field_key, "value", 10)
             end)

    assert {:ok, [^field_key]} = CompoundMemberIndex.keys_for_prefix(index, prefix)

    assert nil ==
             CommandTime.with_now_ms(20, fn ->
               Router.compound_get(ctx, key, field_key)
             end)

    assert {:ok, []} = CompoundMemberIndex.keys_for_prefix(index, prefix)
    assert 0 = Router.compound_count(ctx, key, prefix)
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

  defp restart_shards(ctx, indexes) do
    pids = Enum.map(indexes, &{&1, Process.whereis(Router.shard_name(ctx, &1))})
    Enum.each(pids, fn {_index, pid} -> Process.exit(pid, :kill) end)

    Enum.each(indexes, fn index ->
      assert :ok =
               ShardHelpers.eventually(
                 fn -> Process.whereis(Router.shard_name(ctx, index)) == nil end,
                 "shard #{index} did not stop before restart",
                 100,
                 20
               )
    end)

    Enum.each(indexes, fn index ->
      assert {:ok, _pid} =
               Ferricstore.Store.Shard.start_link(
                 index: index,
                 data_dir: ctx.data_dir,
                 instance_ctx: ctx
               )
    end)
  end

  defp non_owner_shard_keys(ctx) do
    {first, second} = different_shard_keys(ctx)

    if Router.shard_for(ctx, first) == 0 do
      {first, second}
    else
      {second, first}
    end
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

  defp restore_env(key, nil), do: Application.delete_env(:ferricstore, key)
  defp restore_env(key, value), do: Application.put_env(:ferricstore, key, value)
end
