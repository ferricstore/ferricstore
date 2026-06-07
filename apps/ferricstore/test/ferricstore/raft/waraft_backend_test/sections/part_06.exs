defmodule Ferricstore.Raft.WARaftBackendTest.Sections.Part06 do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      import ExUnit.CaptureLog

      alias Ferricstore.ErrorReasons
      alias Ferricstore.Raft.Cluster, as: RaftCluster
      alias Ferricstore.Raft.WARaftBackend
      alias Ferricstore.Raft.WARaftStorage
      alias Ferricstore.Store.{BlobRef, BlobStore}
      alias Ferricstore.Store.CompoundKey
      alias Ferricstore.Store.Router
      alias Ferricstore.Raft.WARaftBackendTest.LabelCounter
      alias Ferricstore.Raft.WARaftBackendTest.OversizedLabel

  test "WARaft generic batches coalesce behind an in-flight flush by default", %{ctx: ctx} do
    previous_generic_window = Application.get_env(:ferricstore, :waraft_generic_batch_window_ms)
    previous_during_flush = Application.get_env(:ferricstore, :waraft_generic_batch_during_flush)
    previous_hook = Application.get_env(:ferricstore, :waraft_backend_batcher_call_hook)

    try do
      Application.put_env(:ferricstore, :waraft_generic_batch_window_ms, 0)
      Application.delete_env(:ferricstore, :waraft_generic_batch_during_flush)
      Application.put_env(:ferricstore, :waraft_backend_batcher_call_hook, {:block, self()})

      assert :ok =
               WARaftBackend.start(ctx,
                 log_module: :ferricstore_waraft_spike_segment_log,
                 commit_batch_interval_ms: 1,
                 commit_batch_max: 10_000
               )

      first =
        Task.async(fn ->
          WARaftBackend.write_batch(0, [
            {:put, "generic-default-flush:1:a", "v1a", 0},
            {:put, "generic-default-flush:1:b", "v1b", 0}
          ])
        end)

      assert_receive {:waraft_backend_batcher_call, :__commit_batch_direct__, first_ref,
                      first_worker},
                     1_000

      second =
        Task.async(fn ->
          WARaftBackend.write_batch(0, [
            {:put, "generic-default-flush:2:a", "v2a", 0},
            {:put, "generic-default-flush:2:b", "v2b", 0}
          ])
        end)

      third =
        Task.async(fn ->
          WARaftBackend.write_batch(0, [
            {:put, "generic-default-flush:3:a", "v3a", 0},
            {:put, "generic-default-flush:3:b", "v3b", 0}
          ])
        end)

      refute_receive {:waraft_backend_batcher_call, :__commit_batch_direct__, _ref, _worker}, 50

      send(first_worker, {first_ref, :continue})
      assert {:ok, [:ok, :ok]} = Task.await(first, 5_000)

      assert_receive {:waraft_backend_batcher_call, :__commit_batch_direct__, second_ref,
                      second_worker},
                     1_000

      refute_receive {:waraft_backend_batcher_call, :__commit_batch_direct__, _ref, _worker}, 50
      send(second_worker, {second_ref, :continue})

      assert {:ok, [:ok, :ok]} = Task.await(second, 5_000)
      assert {:ok, [:ok, :ok]} = Task.await(third, 5_000)

      assert "v1a" == Router.get(ctx, "generic-default-flush:1:a")
      assert "v2a" == Router.get(ctx, "generic-default-flush:2:a")
      assert "v3a" == Router.get(ctx, "generic-default-flush:3:a")
    after
      restore_env(:waraft_generic_batch_window_ms, previous_generic_window)
      restore_env(:waraft_generic_batch_during_flush, previous_during_flush)
      restore_env(:waraft_backend_batcher_call_hook, previous_hook)
    end
  end

  test "default WARaft generic batches coalesce behind an in-flight flush without a static window",
       %{ctx: ctx} do
    previous_generic_window = Application.get_env(:ferricstore, :waraft_generic_batch_window_ms)
    previous_during_flush = Application.get_env(:ferricstore, :waraft_generic_batch_during_flush)
    previous_hook = Application.get_env(:ferricstore, :waraft_backend_batcher_call_hook)
    handler_id = {__MODULE__, :generic_batch_during_flush, make_ref()}

    :telemetry.attach(
      handler_id,
      [:ferricstore, :waraft, :batcher, :hot_flush],
      &__MODULE__.handle_namespace_batcher_telemetry/4,
      self()
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    try do
      Application.put_env(:ferricstore, :waraft_generic_batch_window_ms, 0)
      Application.put_env(:ferricstore, :waraft_generic_batch_during_flush, true)
      Application.put_env(:ferricstore, :waraft_backend_batcher_call_hook, {:block, self()})

      assert :ok =
               WARaftBackend.start(ctx,
                 log_module: :ferricstore_waraft_spike_segment_log,
                 commit_batch_interval_ms: 1,
                 commit_batch_max: 10_000
               )

      first =
        Task.async(fn ->
          WARaftBackend.write_batch(0, [
            {:put, "generic-during-flush:1:a", "v1a", 0},
            {:put, "generic-during-flush:1:b", "v1b", 0}
          ])
        end)

      assert_receive {:waraft_backend_batcher_call, :__commit_batch_direct__, first_ref,
                      first_worker},
                     1_000

      second =
        Task.async(fn ->
          WARaftBackend.write_batch(0, [
            {:put, "generic-during-flush:2:a", "v2a", 0},
            {:put, "generic-during-flush:2:b", "v2b", 0}
          ])
        end)

      third =
        Task.async(fn ->
          WARaftBackend.write_batch(0, [
            {:put, "generic-during-flush:3:a", "v3a", 0},
            {:put, "generic-during-flush:3:b", "v3b", 0}
          ])
        end)

      refute_receive {:waraft_backend_batcher_call, :__commit_batch_direct__, _ref, _worker}, 50

      send(first_worker, {first_ref, :continue})
      assert {:ok, [:ok, :ok]} = Task.await(first, 5_000)

      assert_receive {:waraft_backend_batcher_call, :__commit_batch_direct__, second_ref,
                      second_worker},
                     1_000

      refute_receive {:waraft_backend_batcher_call, :__commit_batch_direct__, _ref, _worker}, 50

      send(second_worker, {second_ref, :continue})

      assert {:ok, [:ok, :ok]} = Task.await(second, 5_000)
      assert {:ok, [:ok, :ok]} = Task.await(third, 5_000)

      assert_receive {:waraft_namespace_batcher_flush,
                      [:ferricstore, :waraft, :batcher, :hot_flush],
                      %{batch_size: 4, group_count: 2}, %{kind: :batch}},
                     1_000

      assert "v1a" == Router.get(ctx, "generic-during-flush:1:a")
      assert "v2a" == Router.get(ctx, "generic-during-flush:2:a")
      assert "v3a" == Router.get(ctx, "generic-during-flush:3:a")
    after
      restore_env(:waraft_generic_batch_window_ms, previous_generic_window)
      restore_env(:waraft_generic_batch_during_flush, previous_during_flush)
      restore_env(:waraft_backend_batcher_call_hook, previous_hook)
    end
  end

  test "WARaft hot put batcher queues the next slot while previous flush commits", %{ctx: ctx} do
    previous_window = Application.get_env(:ferricstore, :waraft_hot_batch_window_ms)
    previous_hook = Application.get_env(:ferricstore, :waraft_backend_batcher_call_hook)

    try do
      Application.put_env(:ferricstore, :waraft_hot_batch_window_ms, 1)
      Application.put_env(:ferricstore, :waraft_backend_batcher_call_hook, {:block, self()})

      assert :ok =
               WARaftBackend.start(ctx,
                 log_module: :ferricstore_waraft_spike_segment_log,
                 commit_batch_interval_ms: 1,
                 commit_batch_max: 10_000
               )

      first =
        Task.async(fn ->
          WARaftBackend.write_put_batch(0, [{"hot-put-async-flush:1", "v1", 0}])
        end)

      assert_receive {:waraft_backend_batcher_call, :__commit_put_batch_direct__, first_ref,
                      first_worker},
                     1_000

      second =
        Task.async(fn ->
          WARaftBackend.write_put_batch(0, [{"hot-put-async-flush:2", "v2", 0}])
        end)

      refute_receive {:waraft_backend_batcher_call, :__commit_put_batch_direct__, _ref, _worker},
                     50

      send(first_worker, {first_ref, :continue})
      assert {:ok, [:ok]} = Task.await(first, 5_000)

      assert_receive {:waraft_backend_batcher_call, :__commit_put_batch_direct__, second_ref,
                      second_worker},
                     1_000

      send(second_worker, {second_ref, :continue})

      assert {:ok, [:ok]} = Task.await(second, 5_000)

      assert "v1" == Router.get(ctx, "hot-put-async-flush:1")
      assert "v2" == Router.get(ctx, "hot-put-async-flush:2")
    after
      restore_env(:waraft_hot_batch_window_ms, previous_window)
      restore_env(:waraft_backend_batcher_call_hook, previous_hook)
    end
  end

  test "WARaft hot put batcher replies to queued async callers when stopped", %{ctx: ctx} do
    previous_window = Application.get_env(:ferricstore, :waraft_hot_batch_window_ms)
    previous_hook = Application.get_env(:ferricstore, :waraft_backend_batcher_call_hook)

    try do
      Application.put_env(:ferricstore, :waraft_hot_batch_window_ms, 1)
      Application.put_env(:ferricstore, :waraft_backend_batcher_call_hook, {:block, self()})

      assert :ok =
               WARaftBackend.start(ctx,
                 log_module: :ferricstore_waraft_spike_segment_log,
                 commit_batch_interval_ms: 1,
                 commit_batch_max: 10_000
               )

      first_reply_ref = make_ref()

      assert :ok =
               WARaftBackend.write_put_batch_async(
                 0,
                 [{"hot-put-stop-inflight:1", "v1", 0}],
                 {self(), first_reply_ref}
               )

      assert_receive {:waraft_backend_batcher_call, :__commit_put_batch_direct__, first_ref,
                      first_worker},
                     1_000

      second_reply_ref = make_ref()

      assert :ok =
               WARaftBackend.write_put_batch_async(
                 0,
                 [{"hot-put-stop-inflight:2", "v2", 0}],
                 {self(), second_reply_ref}
               )

      refute_receive {:waraft_backend_batcher_call, :__commit_put_batch_direct__, _ref, _worker},
                     50

      stop_task = Task.async(fn -> Ferricstore.Raft.WARaftBackend.Batcher.stop(0) end)
      assert nil == Task.yield(stop_task, 50)

      send(first_worker, {first_ref, :continue})
      assert_receive {^first_reply_ref, {:ok, [:ok]}}, 5_000

      assert_receive {:waraft_backend_batcher_call, :__commit_put_batch_direct__, second_ref,
                      second_worker},
                     5_000

      send(second_worker, {second_ref, :continue})
      assert_receive {^second_reply_ref, {:ok, [:ok]}}, 5_000
      assert :ok = Task.await(stop_task, 5_000)

      assert "v1" == Router.get(ctx, "hot-put-stop-inflight:1")
      assert "v2" == Router.get(ctx, "hot-put-stop-inflight:2")
    after
      restore_env(:waraft_hot_batch_window_ms, previous_window)
      restore_env(:waraft_backend_batcher_call_hook, previous_hook)
    end
  end

  test "WARaft hot put batcher flush waits for queued work behind in-flight flush", %{ctx: ctx} do
    previous_window = Application.get_env(:ferricstore, :waraft_hot_batch_window_ms)
    previous_hook = Application.get_env(:ferricstore, :waraft_backend_batcher_call_hook)

    try do
      Application.put_env(:ferricstore, :waraft_hot_batch_window_ms, 1)
      Application.put_env(:ferricstore, :waraft_backend_batcher_call_hook, {:block, self()})

      assert :ok =
               WARaftBackend.start(ctx,
                 log_module: :ferricstore_waraft_spike_segment_log,
                 commit_batch_interval_ms: 1,
                 commit_batch_max: 10_000
               )

      first_reply_ref = make_ref()

      assert :ok =
               WARaftBackend.write_put_batch_async(
                 0,
                 [{"hot-put-flush-inflight:1", "v1", 0}],
                 {self(), first_reply_ref}
               )

      assert_receive {:waraft_backend_batcher_call, :__commit_put_batch_direct__, first_ref,
                      first_worker},
                     1_000

      second_reply_ref = make_ref()

      assert :ok =
               WARaftBackend.write_put_batch_async(
                 0,
                 [{"hot-put-flush-inflight:2", "v2", 0}],
                 {self(), second_reply_ref}
               )

      flush_task =
        Task.async(fn -> Ferricstore.Raft.WARaftBackend.Batcher.flush(0, 5_000) end)

      assert nil == Task.yield(flush_task, 50)

      send(first_worker, {first_ref, :continue})
      assert_receive {^first_reply_ref, {:ok, [:ok]}}, 5_000

      assert_receive {:waraft_backend_batcher_call, :__commit_put_batch_direct__, second_ref,
                      second_worker},
                     5_000

      send(second_worker, {second_ref, :continue})
      assert_receive {^second_reply_ref, {:ok, [:ok]}}, 5_000
      assert :ok = Task.await(flush_task, 5_000)

      assert "v1" == Router.get(ctx, "hot-put-flush-inflight:1")
      assert "v2" == Router.get(ctx, "hot-put-flush-inflight:2")
    after
      restore_env(:waraft_hot_batch_window_ms, previous_window)
      restore_env(:waraft_backend_batcher_call_hook, previous_hook)
    end
  end

  test "WARaft hot put batcher coalesces writes that arrive during an in-flight flush", %{
    ctx: ctx
  } do
    previous_window = Application.get_env(:ferricstore, :waraft_hot_batch_window_ms)
    previous_hook = Application.get_env(:ferricstore, :waraft_backend_batcher_call_hook)
    handler_id = {__MODULE__, :hot_put_during_flush, make_ref()}

    :telemetry.attach(
      handler_id,
      [:ferricstore, :waraft, :batcher, :hot_flush],
      &__MODULE__.handle_namespace_batcher_telemetry/4,
      self()
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    try do
      Application.put_env(:ferricstore, :waraft_hot_batch_window_ms, 1)
      Application.put_env(:ferricstore, :waraft_backend_batcher_call_hook, {:block, self()})

      assert :ok =
               WARaftBackend.start(ctx,
                 log_module: :ferricstore_waraft_spike_segment_log,
                 commit_batch_interval_ms: 1,
                 commit_batch_max: 10_000
               )

      first =
        Task.async(fn ->
          WARaftBackend.write_put_batch(0, [{"hot-put-during-flush:1", "v1", 0}])
        end)

      assert_receive {:waraft_backend_batcher_call, :__commit_put_batch_direct__, first_ref,
                      first_worker},
                     1_000

      second =
        Task.async(fn ->
          WARaftBackend.write_put_batch(0, [{"hot-put-during-flush:2", "v2", 0}])
        end)

      third =
        Task.async(fn ->
          WARaftBackend.write_put_batch(0, [{"hot-put-during-flush:3", "v3", 0}])
        end)

      refute_receive {:waraft_backend_batcher_call, :__commit_put_batch_direct__, _ref, _worker},
                     50

      send(first_worker, {first_ref, :continue})
      assert {:ok, [:ok]} = Task.await(first, 5_000)

      assert_receive {:waraft_backend_batcher_call, :__commit_put_batch_direct__, second_ref,
                      second_worker},
                     1_000

      refute_receive {:waraft_backend_batcher_call, :__commit_put_batch_direct__, _ref, _worker},
                     50

      send(second_worker, {second_ref, :continue})

      assert {:ok, [:ok]} = Task.await(second, 5_000)
      assert {:ok, [:ok]} = Task.await(third, 5_000)

      assert_receive {:waraft_namespace_batcher_flush,
                      [:ferricstore, :waraft, :batcher, :hot_flush],
                      %{batch_size: 2, group_count: 2}, %{kind: :put_batch}},
                     1_000

      assert "v1" == Router.get(ctx, "hot-put-during-flush:1")
      assert "v2" == Router.get(ctx, "hot-put-during-flush:2")
      assert "v3" == Router.get(ctx, "hot-put-during-flush:3")
    after
      restore_env(:waraft_hot_batch_window_ms, previous_window)
      restore_env(:waraft_backend_batcher_call_hook, previous_hook)
    end
  end

  test "Router multi-shard WARaft put batches still use hot per-shard coalescing", %{
    root: root
  } do
    previous_window = Application.get_env(:ferricstore, :waraft_hot_batch_window_ms)
    handler_id = {__MODULE__, :router_hot_put_flush, make_ref()}
    multi_root = Path.join(root, "router-multishard-hot-put")
    Ferricstore.DataDir.ensure_layout!(multi_root, 2)
    Ferricstore.Store.ActiveFile.init(2)
    multi_ctx = build_ctx(multi_root, shard_count: 2)

    :telemetry.attach(
      handler_id,
      [:ferricstore, :waraft, :batcher, :hot_flush],
      &__MODULE__.handle_namespace_batcher_telemetry/4,
      self()
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    try do
      Application.put_env(:ferricstore, :waraft_hot_batch_window_ms, 25)

      assert :ok =
               WARaftBackend.start(multi_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log,
                 commit_batch_interval_ms: 1,
                 commit_batch_max: 10_000
               )

      k0a = key_for_shard(multi_ctx, 0, "router-hot-put-a")
      k0b = key_for_shard(multi_ctx, 0, "router-hot-put-b")
      k1a = key_for_shard(multi_ctx, 1, "router-hot-put-a")
      k1b = key_for_shard(multi_ctx, 1, "router-hot-put-b")

      tasks = [
        Task.async(fn ->
          Router.__forwarded_batch_quorum_put_entries__(
            multi_ctx,
            [{k0a, "v0a", 0}, {k1a, "v1a", 0}],
            nil
          )
        end),
        Task.async(fn ->
          Router.__forwarded_batch_quorum_put_entries__(
            multi_ctx,
            [{k0b, "v0b", 0}, {k1b, "v1b", 0}],
            nil
          )
        end)
      ]

      assert [[:ok, :ok], [:ok, :ok]] = Enum.map(tasks, &Task.await(&1, 5_000))

      flushes =
        for _ <- 1..2 do
          assert_receive {:waraft_namespace_batcher_flush,
                          [:ferricstore, :waraft, :batcher, :hot_flush],
                          %{batch_size: 2, group_count: 2},
                          %{kind: :put_batch, shard_index: shard_index}},
                         1_000

          shard_index
        end

      assert MapSet.new(flushes) == MapSet.new([0, 1])

      assert "v0a" == Router.get(multi_ctx, k0a)
      assert "v0b" == Router.get(multi_ctx, k0b)
      assert "v1a" == Router.get(multi_ctx, k1a)
      assert "v1b" == Router.get(multi_ctx, k1b)
    after
      restore_env(:waraft_hot_batch_window_ms, previous_window)
      FerricStore.Instance.cleanup(multi_ctx.name)
      File.rm_rf!(multi_root)
    end
  end

  test "default WARaft hot delete batches coalesce before segment append", %{ctx: ctx} do
    previous_window = Application.get_env(:ferricstore, :waraft_hot_batch_window_ms)
    handler_id = {__MODULE__, :hot_delete_flush, make_ref()}

    :telemetry.attach(
      handler_id,
      [:ferricstore, :waraft, :batcher, :hot_flush],
      &__MODULE__.handle_namespace_batcher_telemetry/4,
      self()
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    try do
      Application.put_env(:ferricstore, :waraft_hot_batch_window_ms, 25)

      assert :ok =
               WARaftBackend.start(ctx,
                 log_module: :ferricstore_waraft_spike_segment_log,
                 commit_batch_interval_ms: 1,
                 commit_batch_max: 10_000
               )

      assert {:ok, [:ok, :ok]} =
               WARaftBackend.write_put_batch(0, [
                 {"hot-delete-batch:1", "v1", 0},
                 {"hot-delete-batch:2", "v2", 0}
               ])

      assert "v1" == Router.get(ctx, "hot-delete-batch:1")
      assert "v2" == Router.get(ctx, "hot-delete-batch:2")

      tasks =
        for i <- 1..2 do
          Task.async(fn ->
            WARaftBackend.write_delete_batch(0, ["hot-delete-batch:#{i}"])
          end)
        end

      assert [{:ok, [:ok]}, {:ok, [:ok]}] = Enum.map(tasks, &Task.await(&1, 5_000))

      assert_receive {:waraft_namespace_batcher_flush,
                      [:ferricstore, :waraft, :batcher, :hot_flush],
                      %{batch_size: 2, group_count: 2}, %{kind: :delete_batch}},
                     1_000

      assert nil == Router.get(ctx, "hot-delete-batch:1")
      assert nil == Router.get(ctx, "hot-delete-batch:2")
    after
      restore_env(:waraft_hot_batch_window_ms, previous_window)
    end
  end

  test "single-member WARaft commit batch timer is anchored under continuous load", %{
    ctx: ctx
  } do
    assert :ok =
             WARaftBackend.start(ctx,
               log_module: :ferricstore_waraft_spike_segment_log,
               commit_batch_interval_ms: 30,
               commit_batch_max: 10_000
             )

    acceptor = :wa_raft_acceptor.registered_name(:ferricstore_waraft_backend, 1)
    first_ref = make_ref()
    started_ms = System.monotonic_time(:millisecond)

    :ok = raw_waraft_async_put(acceptor, first_ref, "batch-timer:first", "v0")

    producer =
      Task.async(fn ->
        for i <- 1..80 do
          Process.sleep(2)
          :ok = raw_waraft_async_put(acceptor, make_ref(), "batch-timer:#{i}", "v#{i}")
        end
      end)

    assert_receive {^first_ref, :ok}, 120
    assert System.monotonic_time(:millisecond) - started_ms < 120
    assert List.duplicate(:ok, 80) == Task.await(producer, 5_000)
    assert_eventually(fn -> Router.get(ctx, "batch-timer:first") end, "v0")
  end

  test "segment log append emits success telemetry with record count and bytes", %{ctx: ctx} do
    parent = self()
    handler_id = {__MODULE__, :segment_log_append_success, make_ref()}

    :telemetry.attach(
      handler_id,
      [:ferricstore, :waraft, :segment_log, :append],
      &__MODULE__.handle_segment_log_telemetry/4,
      parent
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    assert :ok =
             WARaftBackend.start(ctx,
               log_module: :ferricstore_waraft_spike_segment_log
             )

    assert {:ok, [:ok, :ok]} =
             WARaftBackend.write_put_batch(0, [
               {"segment-log-telemetry:k1", "v1", 0},
               {"segment-log-telemetry:k2", "v2", 0}
             ])

    assert_receive {:waraft_segment_log_telemetry, [:ferricstore, :waraft, :segment_log, :append],
                    %{count: count, bytes: bytes, duration: duration},
                    %{path: path, result: :ok, new_segment: new_segment}},
                   1_000

    assert count >= 1
    assert bytes > 0
    assert duration >= 0
    assert String.ends_with?(path, ".seg")
    assert is_boolean(new_segment)
  end

  test "segment log append emits error telemetry on fsync failure", %{ctx: ctx} do
    parent = self()
    handler_id = {__MODULE__, :segment_log_append_error, make_ref()}
    previous_hook = Application.get_env(:ferricstore, :waraft_segment_log_file_sync_hook)

    :telemetry.attach(
      handler_id,
      [:ferricstore, :waraft, :segment_log, :append],
      &__MODULE__.handle_segment_log_telemetry/4,
      parent
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    try do
      assert :ok =
               WARaftBackend.start(ctx,
                 log_module: :ferricstore_waraft_spike_segment_log
               )

      Application.put_env(:ferricstore, :waraft_segment_log_file_sync_hook, {:fail_once, self()})

      assert Ferricstore.ErrorReasons.write_timeout_unknown() ==
               WARaftBackend.write(0, {:put, "segment-log-telemetry-fail:k", "v1", 0})

      assert_receive {:waraft_segment_log_telemetry,
                      [:ferricstore, :waraft, :segment_log, :append],
                      %{count: count, bytes: bytes, duration: duration},
                      %{
                        path: path,
                        result: :error,
                        reason: {_, _}
                      }},
                     1_000

      assert count >= 1
      assert bytes > 0
      assert duration >= 0
      assert String.ends_with?(path, ".seg")
    after
      restore_env(:waraft_segment_log_file_sync_hook, previous_hook)
    end
  end

  test "Router WARaft batch put submits shard groups concurrently", %{root: root} do
    Ferricstore.DataDir.ensure_layout!(root, 2)
    Ferricstore.Store.ActiveFile.init(2)
    ctx = build_ctx(root, shard_count: 2)

    on_exit(fn ->
      FerricStore.Instance.cleanup(ctx.name)
    end)

    assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

    key0 = key_for_shard(ctx, 0, "waraft-parallel-batch")
    key1 = key_for_shard(ctx, 1, "waraft-parallel-batch")
    previous_hook = Application.get_env(:ferricstore, :standalone_durability_hook)

    try do
      Application.put_env(:ferricstore, :standalone_durability_hook, fn _file_path, batch ->
        if Enum.any?(batch, &delayed_parallel_batch_record?/1), do: Process.sleep(200)
        :passthrough
      end)

      started_ms = System.monotonic_time(:millisecond)
      assert [:ok, :ok] = Router.batch_quorum_put(ctx, [{key0, "v0"}, {key1, "v1"}])
      elapsed_ms = System.monotonic_time(:millisecond) - started_ms

      assert elapsed_ms < 320
      assert "v0" == Router.get(ctx, key0)
      assert "v1" == Router.get(ctx, key1)
    after
      restore_env(:standalone_durability_hook, previous_hook)
    end
  end

  test "storage apply failure blocks later positions until restart", %{root: root, ctx: ctx} do
    previous_mode = Application.get_env(:ferricstore, :waraft_storage_apply_mode)

    try do
      Application.put_env(:ferricstore, :waraft_storage_apply_mode, :bitcask_keydir)
      assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
      assert {:ok, pre_write_position} = WARaftBackend.storage_position(0)

      payload = "v1"
      {encoded_ref, ref} = missing_legacy_blob_ref(payload)

      assert Ferricstore.ErrorReasons.write_timeout_unknown() ==
               WARaftBackend.write(0, {:put_blob_ref, "apply-block:k", encoded_ref, 0})

      assert Ferricstore.ErrorReasons.write_timeout_unknown() ==
               WARaftBackend.write(0, {:put, "apply-block:after", "v2", 0})

      assert {:ok, ^pre_write_position} = WARaftBackend.storage_position(0)
      assert nil == Router.get(ctx, "apply-block:k")
      assert nil == Router.get(ctx, "apply-block:after")

      assert :ok = WARaftBackend.stop()
      write_legacy_blob!(ctx, 0, ref, payload)
      FerricStore.Instance.cleanup(ctx.name)

      restarted_ctx = build_ctx(root)

      assert :ok =
               WARaftBackend.start(restarted_ctx,
                 log_module: :ferricstore_waraft_spike_segment_log
               )

      assert_eventually(fn -> Router.get(restarted_ctx, "apply-block:k") end, "v1")
      assert_eventually(fn -> Router.get(restarted_ctx, "apply-block:after") end, "v2")
    after
      restore_env(:waraft_storage_apply_mode, previous_mode)
    end
  end

  test "storage blocked state emits telemetry for first failure and later rejected applies", %{
    ctx: ctx
  } do
    parent = self()
    handler_id = {__MODULE__, :storage_blocked, make_ref()}

    :telemetry.attach(
      handler_id,
      [:ferricstore, :waraft, :storage_blocked],
      &__MODULE__.handle_test_telemetry/4,
      parent
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    previous_mode = Application.get_env(:ferricstore, :waraft_storage_apply_mode)

    try do
      Application.put_env(:ferricstore, :waraft_storage_apply_mode, :bitcask_keydir)
      assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      {encoded_ref, _ref} = missing_legacy_blob_ref("v1")

      assert Ferricstore.ErrorReasons.write_timeout_unknown() ==
               WARaftBackend.write(0, {:put_blob_ref, "telemetry-block:k", encoded_ref, 0})

      assert_receive {:waraft_storage_blocked, [:ferricstore, :waraft, :storage_blocked],
                      %{count: 1},
                      %{
                        operation: :apply_failure,
                        reason: {:blob_ref_unavailable, _reason},
                        shard_index: 0
                      }}

      assert Ferricstore.ErrorReasons.write_timeout_unknown() ==
               WARaftBackend.write(0, {:put, "telemetry-block:after", "v2", 0})

      assert_receive {:waraft_storage_blocked, [:ferricstore, :waraft, :storage_blocked],
                      %{count: 1},
                      %{
                        operation: :blocked_apply,
                        reason: {:blob_ref_unavailable, _reason},
                        shard_index: 0
                      }}
    after
      restore_env(:waraft_storage_apply_mode, previous_mode)
    end
  end

  test "blocked storage refuses snapshot creation instead of exporting a newer volatile position",
       %{
         ctx: ctx
       } do
    previous_mode = Application.get_env(:ferricstore, :waraft_storage_apply_mode)

    try do
      Application.put_env(:ferricstore, :waraft_storage_apply_mode, :bitcask_keydir)
      assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      {encoded_ref, _ref} = missing_legacy_blob_ref("v1")

      assert Ferricstore.ErrorReasons.write_timeout_unknown() ==
               WARaftBackend.write(0, {:put_blob_ref, "snapshot-block:k", encoded_ref, 0})

      assert {:error, {:storage_blocked, {:blob_ref_unavailable, _reason}}} =
               WARaftBackend.create_snapshot(0)
    after
      restore_env(:waraft_storage_apply_mode, previous_mode)
    end
  end

  test "deterministic command errors still advance WARaft storage replay position", %{ctx: ctx} do
    assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
    assert {:ok, {:raft_log_pos, pre_index, _pre_term}} = WARaftBackend.storage_position(0)

    bad_command = {:unknown_for_replay_position_test, "k"}

    assert {:error, {:unknown_command, ^bad_command}} = WARaftBackend.write(0, bad_command)
    assert {:ok, {:raft_log_pos, post_index, _post_term}} = WARaftBackend.storage_position(0)
    assert post_index > pre_index
  end

  test "startup storage replay wait tolerates trailing Raft no-op entries", %{
    root: root,
    ctx: ctx
  } do
    previous_wait = Application.get_env(:ferricstore, :waraft_start_wait_timeout_ms)

    try do
      Application.put_env(:ferricstore, :waraft_start_wait_timeout_ms, 100)

      key = "startup-noop-tail:k"
      assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
      assert :ok = WARaftBackend.write(0, {:put, key, "v1", 0})
      assert {:ok, {:raft_log_pos, applied_index, term}} = WARaftBackend.storage_position(0)
      assert :ok = WARaftBackend.stop()
      FerricStore.Instance.cleanup(ctx.name)

      append_raw_waraft_segment_record!(
        root,
        0,
        {applied_index + 1, {term + 1, {make_ref(), :noop}}}
      )

      restarted_ctx = build_ctx(root)

      try do
        assert :ok =
                 WARaftBackend.start(restarted_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert "v1" == Router.get(restarted_ctx, key)
      after
        FerricStore.Instance.cleanup(restarted_ctx.name)
      end
    after
      restore_env(:waraft_start_wait_timeout_ms, previous_wait)
    end
  end

    end
  end
end
