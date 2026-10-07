defmodule Ferricstore.Store.SeparatedCompactionTest do
  @moduledoc "Explicit isolated regression cases for the archived separate-output candidate."
  use ExUnit.Case, async: false
  @moduletag :global_state

  alias FerricStore.Impl
  alias Ferricstore.Commands.Hash
  alias Ferricstore.Raft.WARaftBackend
  alias Ferricstore.Store.{CompactionPlan, CompoundKey, Promotion, Router}
  alias Ferricstore.Store.Shard.Compound.SeparatedCompaction
  alias Ferricstore.Test.ShardHelpers

  setup do
    snapshot = ShardHelpers.replace_default_apply_context(promotion_threshold: 5)
    ShardHelpers.flush_all_keys()
    ctx = FerricStore.Instance.get(:default)
    key = "separated:#{System.unique_integer([:positive])}"
    seed(ctx, key)
    shard = Router.shard_name(ctx, Router.shard_for(ctx, key))
    ShardHelpers.eventually(fn -> GenServer.call(shard, {:promoted?, key}) end, "not promoted")
    state = :sys.get_state(shard)
    path = state.promoted_instances[key].path

    on_exit(fn ->
      ShardHelpers.restore_default_apply_context(snapshot)
      SeparatedCompaction.cleanup_staging(ctx.data_dir, state.index)
      ShardHelpers.flush_all_keys()
    end)

    %{ctx: ctx, key: key, state: state, path: path}
  end

  test "foreground writes continue during copying and newer values/tombstones win on replay", f do
    field = CompoundKey.hash_field(f.key, "seed-100")
    :ets.update_element(f.state.keydir, field, [{2, nil}, {4, 77}])
    task = start(f, [:after_rotation])

    try do
      assert_receive {:phase, :after_rotation, _, job}, 5_000
      assert File.exists?(job.tail)
      refute File.exists?(job.installed)
      assert {:ok, 0} = Impl.hset(f.ctx, f.key, %{"seed-1" => "newer"})
      assert Hash.handle("HDEL", [f.key, "seed-599"], ShardHelpers.router_store(f.ctx)) == 1

      assert {:ok, 2} =
               Impl.hset(f.ctx, f.key, %{
                 "aaa" => "new-before-cursor",
                 "zzz" => "new-after-cursor"
               })

      send(task.pid, :continue)
      assert {:ok, _} = Task.await(task, 10_000)
      refute File.exists?(Path.join(f.path, "00000.log"))
      assert [{^field, nil, 0, 77, _, _, _}] = :ets.lookup(f.state.keydir, field)
      assert recovered(f, "seed-1") == "newer"
      assert recovered(f, "seed-599") == nil
      assert recovered(f, "aaa") == "new-before-cursor"
      assert recovered(f, "zzz") == "new-after-cursor"
      assert recovered(f, "seed-100") == "seed"
      assert {:ok, "seed"} = Impl.hget(f.ctx, f.key, "seed-100")
    after
      Task.shutdown(task, :brutal_kill)
    end
  end

  test "a kill after output installation retains both sources and the acknowledged tail", f do
    task = start(f, [:after_rotation, :after_output_install])

    try do
      assert_receive {:phase, :after_rotation, _, job}, 5_000
      assert {:ok, 0} = Impl.hset(f.ctx, f.key, %{"seed-1" => "acknowledged"})
      assert Hash.handle("HDEL", [f.key, "seed-599"], ShardHelpers.router_store(f.ctx)) == 1
      send(task.pid, :continue)
      assert_receive {:phase, :after_output_install, _, _}, 5_000
      assert File.exists?(job.installed)
      Task.shutdown(task, :brutal_kill)
      assert File.exists?(Path.join(f.path, "00000.log"))
      assert recovered(f, "seed-1") == "acknowledged"
      assert recovered(f, "seed-599") == nil
      assert recovered(f, "seed-600") == "seed"
    after
      Task.shutdown(task, :brutal_kill)
    end
  end

  test "an interrupted ascending cleanup cannot resurrect a historical deleted field", f do
    File.touch!(Path.join(f.path, "00001.log"))
    assert Hash.handle("HDEL", [f.key, "seed-599"], ShardHelpers.router_store(f.ctx)) == 1
    File.touch!(Path.join(f.path, "00002.log"))
    task = start(f, [{:after_source_remove, 0}])

    try do
      assert_receive {:phase, {:after_source_remove, 0}, _, job}, 5_000
      refute File.exists?(Path.join(f.path, "00000.log"))
      assert File.exists?(Path.join(f.path, "00001.log"))
      assert File.exists?(job.installed)
      Task.shutdown(task, :brutal_kill)
      assert recovered(f, "seed-599") == nil
      assert recovered(f, "seed-1") == "seed"
      assert recovered(f, "seed-600") == "seed"
    after
      Task.shutdown(task, :brutal_kill)
    end
  end

  test "delete/recreate invalidates staged output without modifying the replacement", f do
    task = start(f, [:before_publish])

    try do
      assert_receive {:phase, :before_publish, _, job}, 5_000
      assert {:ok, 1} = Impl.del(f.ctx, [f.key])
      seed(f.ctx, f.key)
      shard = Router.shard_name(f.ctx, f.state.index)

      ShardHelpers.eventually(
        fn -> GenServer.call(shard, {:promoted?, f.key}) end,
        "not repromoted"
      )

      assert {:ok, 0} = Impl.hset(f.ctx, f.key, %{"seed-1" => "replacement"})
      send(task.pid, :continue)
      assert {:error, _} = Task.await(task, 10_000)
      refute File.exists?(job.installed)
      assert recovered(f, "seed-1") == "replacement"
      assert recovered(f, "seed-600") == "seed"
    after
      Task.shutdown(task, :brutal_kill)
    end
  end

  test "a valid-record-boundary manifest truncation cannot install an incomplete plan", f do
    parent = self()

    task =
      start(f, [], fn
        :before_publish, job ->
          path = CompactionPlan.path(job.stage, job.output_fid)
          bytes = File.read!(path)

          <<_header::binary-size(16), size::unsigned-big-32, _crc::unsigned-big-32, _::binary>> =
            bytes

          File.write!(path, binary_part(bytes, 0, 24 + size))
          send(parent, {:staged_job, job})
          :ok

        _, _ ->
          :ok
      end)

    assert {:error, _} = Task.await(task, 10_000)
    assert_receive {:staged_job, job}
    refute File.exists?(job.installed)
    assert File.exists?(Path.join(f.path, "00000.log"))
    assert recovered(f, "seed-600") == "seed"
  end

  test "a wrong-key locator is rejected by output validation before publication", f do
    a = CompoundKey.hash_field(f.key, "seed-1")
    b = CompoundKey.hash_field(f.key, "seed-2")
    [b_row] = :ets.lookup(f.state.keydir, b)
    :ets.update_element(f.state.keydir, a, {6, elem(b_row, 5)})
    task = start(f, [])
    assert {:error, _} = Task.await(task, 10_000)
    refute File.exists?(Path.join(f.path, "00001.log"))
    assert File.exists?(Path.join(f.path, "00000.log"))
  end

  test "one existing large cold value is streamed without losing its locator", f do
    field = CompoundKey.hash_field(f.key, "seed-1")
    value = :binary.copy("v", 2 * 1_048_576)
    source = Path.join(f.path, "00000.log")

    {:ok, {offset, _record_size}} =
      Ferricstore.Bitcask.NIF.v2_append_record(source, field, value, 0)

    :ets.update_element(f.state.keydir, field, [{2, nil}, {6, offset}, {7, byte_size(value)}])
    task = start(f, [])
    assert {:ok, _} = Task.await(task, 10_000)
    assert [{^field, nil, 0, _, fid, new_offset, size}] = :ets.lookup(f.state.keydir, field)
    assert size == byte_size(value)

    assert {:ok, ^value} =
             Ferricstore.Store.ColdRead.pread_keyed(
               Path.join(f.path, String.pad_leading(Integer.to_string(fid), 5, "0") <> ".log"),
               new_offset,
               field,
               10_000
             )
  end

  test "a non-owner cannot rotate or publish a collection", f do
    task = Task.async(fn -> SeparatedCompaction.run(f.state, f.key, f.path, :none) end)
    assert {:error, _} = Task.await(task)
    refute File.exists?(Path.join(f.path, "00002.log"))
  end

  test "a reserved publication turn completes ahead of new writers", f do
    token = Promotion.acquire_compaction_latch(f.state, f.key)
    parent = self()

    publisher =
      Task.async(fn ->
        Promotion.with_compaction_turn(f.state, f.key, fn ->
          send(parent, :turn_reserved)
          token = Promotion.acquire_compaction_latch(f.state, f.key)
          send(parent, :publisher_acquired)

          try do
            receive do
              :publish -> :ok
            after
              5_000 -> raise("publisher was not resumed")
            end
          after
            Promotion.release_compaction_latch(token)
          end
        end)
      end)

    assert_receive :turn_reserved

    writer =
      Task.async(fn ->
        token = Promotion.acquire_compaction_latch(f.state, f.key)
        send(parent, :writer_acquired)
        Promotion.release_compaction_latch(token)
      end)

    try do
      Promotion.release_compaction_latch(token)
      assert_receive :publisher_acquired, 1_000
      refute_received :writer_acquired
      send(publisher.pid, :publish)
      assert :ok = Task.await(publisher)
      assert :ok = Task.await(writer)
      assert_receive :writer_acquired
    after
      send(publisher.pid, :publish)
      Task.shutdown(publisher, :brutal_kill)
      Task.shutdown(writer, :brutal_kill)
      {table, key} = token
      :ets.delete_object(table, {key, self()})
    end
  end

  test "a killed turn owner cannot leave foreground writes blocked", f do
    parent = self()

    owner =
      Task.async(fn ->
        Promotion.with_compaction_turn(f.state, f.key, fn ->
          send(parent, :turn_reserved)

          receive do
            :stop -> :ok
          end
        end)
      end)

    assert_receive :turn_reserved
    Task.shutdown(owner, :brutal_kill)
    assert {:ok, 0} = Impl.hset(f.ctx, f.key, %{"seed-1" => "after-turn-owner-death"})
    table = elem(f.ctx.latch_refs, f.state.index)
    assert :ets.lookup(table, {:promoted_compaction_turn, f.key}) == []
  end

  test "snapshot copying excludes private staging and delays canonical installation", f do
    flow_id = "snapshot-control-#{f.key}"

    partition =
      Stream.iterate(0, &(&1 + 1))
      |> Enum.find_value(fn i ->
        partition = "separate-snapshot-#{i}"
        flow_key = Ferricstore.Flow.Keys.state_key(flow_id, partition)
        if Router.shard_for(f.ctx, flow_key) == f.state.index, do: partition
      end)

    assert :ok =
             FerricStore.flow_create(flow_id,
               partition_key: partition,
               type: "separate-snapshot",
               state: "queued",
               payload: "snapshot-control"
             )

    assert :ok = Ferricstore.Flow.HistoryProjector.flush(f.ctx, f.state.index)
    task = start(f, [:after_rotation])
    assert_receive {:phase, :after_rotation, _, job}, 5_000
    parent = self()
    previous = Application.fetch_env(:ferricstore, :waraft_snapshot_create_hook)
    once = :atomics.new(1, signed: false)

    Application.put_env(:ferricstore, :waraft_snapshot_create_hook, fn
      {:copied, :data} ->
        if :atomics.compare_exchange(once, 1, 0, 1) == :ok do
          send(parent, {:snapshot_copying, self()})

          receive do
            :continue_snapshot -> :ok
          after
            10_000 -> raise("snapshot was not resumed")
          end
        end

        :ok

      _ ->
        :ok
    end)

    snapshot_task = Task.async(fn -> WARaftBackend.create_snapshot(f.state.index) end)

    try do
      assert_receive {:snapshot_copying, storage}, 5_000
      send(task.pid, :continue)
      assert Task.yield(task, 50) == nil
      refute File.exists?(job.installed)
      send(storage, :continue_snapshot)
      assert {:ok, {:raft_log_pos, index, term}} = Task.await(snapshot_task, 15_000)
      assert {:ok, _} = Task.await(task, 10_000)

      snapshot =
        Path.join([
          f.ctx.data_dir,
          "waraft",
          "ferricstore_waraft_backend.#{f.state.index + 1}",
          "snapshot.#{index}.#{term}"
        ])

      copied = Path.join([snapshot, "dedicated", Path.basename(f.path)])
      assert File.exists?(Path.join(copied, "00000.log"))
      assert File.exists?(Path.join(copied, "00002.log"))
      refute File.exists?(Path.join(snapshot, "compaction_staging"))
    after
      case previous do
        {:ok, value} -> Application.put_env(:ferricstore, :waraft_snapshot_create_hook, value)
        :error -> Application.delete_env(:ferricstore, :waraft_snapshot_create_hook)
      end

      Task.shutdown(snapshot_task, :brutal_kill)
      Task.shutdown(task, :brutal_kill)
    end
  end

  defp seed(ctx, key) do
    for fields <- Enum.chunk_every(1..600, 64),
        do: {:ok, _} = Impl.hset(ctx, key, Map.new(fields, &{"seed-#{&1}", "seed"}))
  end

  defp start(f, pauses, hook \\ fn _, _ -> :ok end) do
    parent = self()

    Task.async(fn ->
      token = Promotion.acquire_compaction_latch(f.state, f.key)

      Process.put(:ferricstore_separated_compaction_hook, fn phase, job ->
        if phase in pauses do
          send(parent, {:phase, phase, self(), job})

          receive do
            :continue -> :ok
          after
            10_000 -> raise("compaction was not resumed")
          end
        end

        hook.(phase, job)
      end)

      try do
        SeparatedCompaction.run(f.state, f.key, f.path, token)
      after
        {table, key} = token
        :ets.delete_object(table, {key, self()})
      end
    end)
  end

  defp recovered(f, field) do
    keydir = :ets.new(:separate_compaction_recovery, [:public, :set])

    try do
      :ets.insert(keydir, :ets.lookup(f.state.keydir, Promotion.marker_key(f.key)))

      recovered =
        Promotion.recover_promoted(f.state.shard_data_path, keydir, f.ctx.data_dir, f.state.index)

      assert Map.has_key?(recovered, f.key)

      case :ets.lookup(keydir, CompoundKey.hash_field(f.key, field)) do
        [] -> nil
        [row] -> elem(row, 1)
      end
    after
      :ets.delete(keydir)
    end
  end
end
