defmodule Ferricstore.Flow.HistoryProjectorRequestTest do
  use ExUnit.Case, async: false

  alias Ferricstore.Flow.{
    HistoryProjectedIndex,
    HistoryProjector,
    Keys,
    NativeOrderedIndex,
    OrderedIndex
  }

  setup do
    unique = System.unique_integer([:positive, :monotonic])
    name = :"history_request_#{unique}"
    suffix = Base.url_encode64(:crypto.strong_rand_bytes(8), padding: false)

    dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore_history_request_#{System.pid()}_#{unique}_#{suffix}"
      )

    refute File.exists?(dir)
    keydir = :ets.new(:history_request_keydir, [:set, :public])
    {index, lookup} = OrderedIndex.table_names(name, 0)
    NativeOrderedIndex.reset(index, lookup)

    settings = [
      flow_history_projector_flush_interval_ms: 60_000,
      flow_history_projector_batch_size: 25_000
    ]

    previous =
      Map.new(settings ++ [flow_history_projector_fsync_hook: nil], fn {key, _} ->
        {key, Application.fetch_env(:ferricstore, key)}
      end)

    for {key, value} <- settings, do: Application.put_env(:ferricstore, key, value)

    ctx = %{
      name: name,
      keydir_refs: {keydir},
      checkpoint_flags: :atomics.new(1, signed: false),
      disk_pressure: :atomics.new(1, signed: false),
      write_version: :counters.new(1, [:write_concurrency]),
      keydir_binary_bytes: :atomics.new(1, signed: true),
      flow_history_projected_index: :atomics.new(1, signed: false),
      flow_history_requested_index: :atomics.new(1, signed: false),
      flow_history_projector_flush_failures: :atomics.new(1, signed: false)
    }

    pid =
      start_supervised!(
        {HistoryProjector,
         shard_index: 0, shard_data_path: dir, instance_ctx: ctx, recover_on_init: false}
      )

    assert HistoryProjectedIndex.read(dir) == 0

    on_exit(fn ->
      stop_if_alive(HistoryProjector.name(ctx, 0))
      Ferricstore.Test.LMDBFixture.cleanup_data_dir!(dir)

      for {key, saved} <- previous do
        case saved do
          {:ok, value} -> Application.put_env(:ferricstore, key, value)
          :error -> Application.delete_env(:ferricstore, key)
        end
      end
    end)

    %{ctx: ctx, dir: dir, pid: pid}
  end

  test "a queued request burst shares durable history work and its flush barrier", %{
    ctx: ctx,
    dir: dir,
    pid: pid
  } do
    parent = self()
    syncs = :atomics.new(1, [])

    Application.put_env(:ferricstore, :flow_history_projector_fsync_hook, fn _path ->
      if :atomics.add_get(syncs, 1, 1) == 1 do
        send(parent, {:history_sync_blocked, self()})

        receive do
          :release_history_sync -> :ok
        end
      end

      :ok
    end)

    :ok = :sys.suspend(pid)

    for index <- 1..100 do
      assert :ok = HistoryProjector.enqueue_async(ctx, 0, [entry(index)], index)
      assert :requested = HistoryProjector.request(ctx, 0, dir, index)
    end

    flush = Task.async(fn -> HistoryProjector.flush(ctx, 0) end)
    :ok = :sys.resume(pid)

    try do
      assert_receive {:history_sync_blocked, ^pid}, 5_000
      assert HistoryProjectedIndex.read(dir) == 0
      refute HistoryProjector.durable?(ctx, 0, dir, 100)
      assert Task.yield(flush, 20) == nil
      send(pid, :release_history_sync)
      assert :ok = Task.await(flush, 10_000)
      assert HistoryProjectedIndex.read(dir) == 100
      assert HistoryProjector.durable?(ctx, 0, dir, 100)
      assert :atomics.get(syncs, 1) <= 2

      for index <- [1, 50, 100] do
        expected = "history-#{index}"
        assert {:ok, ^expected} = HistoryProjector.scan_event_value(dir, entry(index).key)
      end
    after
      send(pid, :release_history_sync)
      Task.shutdown(flush, :brutal_kill)
    end
  end

  test "queued requests cannot publish history that has not been flushed", %{
    ctx: ctx,
    dir: dir,
    pid: pid
  } do
    :ok = :sys.suspend(pid)
    for index <- 1..100, do: assert(:requested = HistoryProjector.request(ctx, 0, dir, index))
    :ok = :sys.resume(pid)
    assert {:error, :flush_failed} = HistoryProjector.flush(ctx, 0)
    assert HistoryProjectedIndex.read(dir) == 0
    refute HistoryProjector.durable?(ctx, 0, dir, 100)
  end

  test "a failed shared sync cannot authorize the requested history watermark", %{
    ctx: ctx,
    dir: dir,
    pid: pid
  } do
    Application.put_env(:ferricstore, :flow_history_projector_fsync_hook, fn _ ->
      {:error, :synthetic_eio}
    end)

    :ok = :sys.suspend(pid)

    for index <- 1..10 do
      assert :ok = HistoryProjector.enqueue_async(ctx, 0, [entry(index)], index)
      HistoryProjector.request(ctx, 0, dir, index)
    end

    :ok = :sys.resume(pid)
    assert {:error, :flush_failed} = HistoryProjector.flush(ctx, 0)
    assert HistoryProjectedIndex.read(dir) == 0
    refute HistoryProjector.durable?(ctx, 0, dir, 10)
  end

  test "an unseen higher requested atomic cannot advance the handled replay cut", %{
    ctx: ctx,
    dir: dir,
    pid: pid
  } do
    :ok = :sys.suspend(pid)
    assert :ok = HistoryProjector.enqueue_async(ctx, 0, [entry(1)], 1)
    assert :requested = HistoryProjector.request(ctx, 0, dir, 1)
    :atomics.put(ctx.flow_history_requested_index, 1, 100)
    :ok = :sys.resume(pid)
    assert :ok = HistoryProjector.flush(ctx, 0)
    assert HistoryProjectedIndex.read(dir) == 1
    refute HistoryProjector.durable?(ctx, 0, dir, 100)
  end

  # Raft replay cannot bring these entries back: apply already moved the Flow
  # on, so applying its command again writes no history.
  test "a projector stopped with unflushed history hands it to its successor", %{
    ctx: ctx,
    dir: dir,
    pid: pid
  } do
    for index <- 1..3,
        do: assert(:ok = HistoryProjector.enqueue_async(ctx, 0, [entry(index)], index))

    assert {:ok, 3} = HistoryProjector.pending_count(ctx, 0)

    # Casts still in its mailbox when it stops.
    :ok = :sys.suspend(pid)

    for index <- 4..5,
        do: assert(:ok = HistoryProjector.enqueue_async(ctx, 0, [entry(index)], index))

    :ok = stop_supervised(HistoryProjector)

    start_supervised!(
      {HistoryProjector,
       shard_index: 0, shard_data_path: dir, instance_ctx: ctx, recover_on_init: false}
    )

    assert :ok = HistoryProjector.flush(ctx, 0)
    assert history_log_keys(dir) == Enum.map(1..5, &entry(&1).key)
  end

  test "a projector that crashes with unflushed history leaves it to its restart", %{
    ctx: ctx,
    dir: dir,
    pid: pid
  } do
    for index <- 1..3,
        do: assert(:ok = HistoryProjector.enqueue_async(ctx, 0, [entry(index)], index))

    :ok = :sys.suspend(pid)

    for index <- 4..5,
        do: assert(:ok = HistoryProjector.enqueue_async(ctx, 0, [entry(index)], index))

    ref = Process.monitor(pid)
    :ok = :sys.terminate(pid, :simulated_crash)
    assert_receive {:DOWN, ^ref, :process, ^pid, :simulated_crash}
    assert history_log_keys(dir) == []

    assert Ferricstore.Test.Eventually.eventually(fn ->
             successor = Process.whereis(HistoryProjector.name(ctx, 0))

             is_pid(successor) and successor != pid and
               HistoryProjector.flush(ctx, 0) == :ok and
               history_log_keys(dir) == Enum.map(1..5, &entry(&1).key)
           end)
  end

  test "a projector started for another data directory drops a hand-over", %{
    ctx: ctx,
    dir: dir
  } do
    for index <- 1..2,
        do: assert(:ok = HistoryProjector.enqueue_async(ctx, 0, [entry(index)], index))

    :ok = stop_supervised(HistoryProjector)
    other = dir <> "-other"
    on_exit(fn -> Ferricstore.Test.LMDBFixture.cleanup_data_dir!(other) end)

    start_supervised!(
      {HistoryProjector,
       shard_index: 0, shard_data_path: other, instance_ctx: ctx, recover_on_init: false}
    )

    assert :ok = HistoryProjector.flush(ctx, 0)
    assert history_log_keys(other) == []
    assert history_log_keys(dir) == []
  end

  test "history waiting in the overflow queue is pending, tombstones and abandoned rows are not",
       %{ctx: ctx} do
    :ok = stop_supervised(HistoryProjector)
    projector = HistoryProjector.name(ctx, 0)
    on_exit(fn -> HistoryProjector.Pending.discard(projector) end)
    assert {:ok, false} = HistoryProjector.history_pending?(ctx, 0)

    tombstone = %{key: entry(1).key, tombstone: true, shard_index: 0}
    assert :ok = HistoryProjector.enqueue_async(ctx, 0, [tombstone], 1)

    # A writer that died before committing its row; the drain drops the row.
    {writer, ref} =
      spawn_monitor(fn -> HistoryProjector.Pending.append_overflow(projector, [entry(2)]) end)

    assert_receive {:DOWN, ^ref, :process, ^writer, :normal}
    assert {:ok, false} = HistoryProjector.history_pending?(ctx, 0)

    assert :ok = HistoryProjector.enqueue_async(ctx, 0, [entry(3)], 3)
    assert {:ok, true} = HistoryProjector.history_pending?(ctx, 0)
  end

  defp history_log_keys(dir) do
    {:ok, file_ids} = Ferricstore.Flow.HistoryProjector.Files.list(dir)

    Enum.flat_map(file_ids, fn file_id ->
      {:ok, keys} =
        Ferricstore.Flow.HistoryProjector.Log.reduce_metadata_pages_buffered(
          HistoryProjector.history_file_path(dir, file_id),
          0,
          [],
          fn {key, _offset, _size, _expire_at_ms, _tombstone?}, acc -> [key | acc] end
        )

      Enum.reverse(keys)
    end)
  end

  defp entry(index) do
    id = "request-#{index}"
    event_ms = 1_000 + index
    event_id = "#{event_ms}-1"

    %{
      key: Keys.stream_entry_key(id, event_id, nil),
      expire_at_ms: 0,
      history_key: Keys.history_key(id),
      event_id: event_id,
      event_ms: event_ms,
      version: 1,
      value: "history-#{index}",
      ra_index: index
    }
  end

  defp stop_if_alive(name) do
    if Process.whereis(name), do: GenServer.stop(name)
  catch
    :exit, :noproc -> :ok
    :exit, {:noproc, _} -> :ok
  end
end
