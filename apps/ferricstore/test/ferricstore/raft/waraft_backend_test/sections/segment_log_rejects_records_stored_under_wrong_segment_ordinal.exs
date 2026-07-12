defmodule Ferricstore.Raft.WARaftBackendTest.Sections.SegmentLogRejectsRecordsStoredUnderWrongSegmentOrdinal do
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

      test "segment log rejects records stored under the wrong segment ordinal", %{
        root: root,
        ctx: ctx
      } do
        previous = Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)

        try do
          Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, 2)

          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
          assert :ok = WARaftBackend.write(0, {:put, "log-ordinal-mismatch:k", "v", 0})
          WARaftBackend.stop()

          segment_dir = waraft_segment_log_dir(root, 0)

          {last_ordinal, last_segment} =
            segment_dir
            |> File.ls!()
            |> Enum.filter(&String.ends_with?(&1, ".seg"))
            |> Enum.map(fn filename ->
              ordinal = filename |> Path.rootname(".seg") |> String.to_integer()
              {ordinal, Path.join(segment_dir, filename)}
            end)
            |> Enum.max_by(&elem(&1, 0))

          wrong_segment = Path.join(segment_dir, "#{last_ordinal + 1}.seg")
          File.rename!(last_segment, wrong_segment)

          assert {:error, reason} =
                   WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          assert inspect(reason) =~ "segment_ordinal_mismatch"
        after
          WARaftBackend.stop()
          restore_env(:waraft_segment_log_records_per_segment, previous)
        end
      end

      test "segment log rejects duplicate numeric segment ordinals", %{
        root: root,
        ctx: ctx
      } do
        previous = Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)

        try do
          Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, 16)

          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
          assert :ok = WARaftBackend.write(0, {:put, "log-duplicate-ordinal:k", "v", 0})
          WARaftBackend.stop()

          segment_dir = waraft_segment_log_dir(root, 0)
          segment_zero = Path.join(segment_dir, "0.seg")
          duplicate_zero = Path.join(segment_dir, "00.seg")

          assert File.exists?(segment_zero)
          File.write!(duplicate_zero, "")

          assert {:error, reason} =
                   WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          assert inspect(reason) =~ "duplicate_segment_ordinal" or
                   inspect(reason) =~ "noncanonical_segment_filename"
        after
          WARaftBackend.stop()
          restore_env(:waraft_segment_log_records_per_segment, previous)
        end
      end

      test "segment log rejects non-canonical numeric segment filenames", %{
        root: root,
        ctx: ctx
      } do
        previous = Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)

        try do
          Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, 2)

          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
          assert :ok = WARaftBackend.write(0, {:put, "log-noncanonical-segment:k", "v", 0})
          WARaftBackend.stop()

          segment_dir = waraft_segment_log_dir(root, 0)
          canonical = Path.join(segment_dir, "1.seg")
          noncanonical = Path.join(segment_dir, "01.seg")

          assert File.exists?(canonical)
          File.rename!(canonical, noncanonical)

          assert {:error, reason} =
                   WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          assert inspect(reason) =~ "noncanonical_segment_filename"
        after
          WARaftBackend.stop()
          restore_env(:waraft_segment_log_records_per_segment, previous)
        end
      end

      test "segment log fails closed when pending rewrite marker is oversized", %{
        root: root,
        ctx: ctx
      } do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
        assert :ok = WARaftBackend.write(0, {:put, "log-rewrite-marker-oversized:k", "v", 0})
        WARaftBackend.stop()

        segment_dir = waraft_segment_log_dir(root, 0)
        log_root = Path.dirname(segment_dir)
        staging = Path.join(log_root, ".rewrite.staging.too_large")
        backup = Path.join(log_root, ".rewrite.backup.too_large")
        marker_path = Path.join(log_root, "segment_log.rewrite.term")

        marker = %{
          version: 1,
          dir: String.to_charlist(segment_dir),
          staging: String.to_charlist(staging),
          backup: String.to_charlist(backup),
          label: :binary.copy("x", 1_048_576)
        }

        File.write!(marker_path, :erlang.term_to_binary(marker))

        assert {:error, reason} =
                 WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

        assert inspect(reason) =~ "rewrite_marker_file_too_large"
        assert File.exists?(marker_path)
      after
        WARaftBackend.stop()
      end

      test "segment log rejects pending rewrite marker with symlink backup", %{
        root: root,
        ctx: ctx
      } do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
        assert :ok = WARaftBackend.write(0, {:put, "log-rewrite-marker-symlink:k", "v", 0})
        WARaftBackend.stop()

        segment_dir = waraft_segment_log_dir(root, 0)
        log_root = Path.dirname(segment_dir)
        staging = Path.join(log_root, "segment_log.rewrite.staging.symlink")
        backup = Path.join(log_root, "segment_log.rewrite.backup.symlink")
        marker_path = Path.join(log_root, "segment_log.rewrite.term")
        outside_backup = Path.join(root, "outside-rewrite-backup")

        File.mkdir_p!(staging)
        File.mkdir_p!(outside_backup)
        assert :ok = File.ln_s(outside_backup, backup)

        marker = %{
          version: 1,
          dir: String.to_charlist(segment_dir),
          staging: String.to_charlist(staging),
          backup: String.to_charlist(backup)
        }

        File.write!(marker_path, :erlang.term_to_binary(marker))

        assert {:error, reason} =
                 WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

        assert inspect(reason) =~ "unsafe_rewrite_path"
        assert {:ok, %{type: :directory}} = File.lstat(segment_dir)
        assert File.exists?(marker_path)
      after
        WARaftBackend.stop()
      end

      test "segment log startup errors are returned without MatchError noise", %{
        root: root,
        ctx: ctx
      } do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
        assert :ok = WARaftBackend.write(0, {:put, "log-open-error-noise:k", "v", 0})
        WARaftBackend.stop()

        segment_dir = waraft_segment_log_dir(root, 0)
        log_root = Path.dirname(segment_dir)
        staging = Path.join(log_root, "segment_log.rewrite.staging.noise")
        backup = Path.join(log_root, "segment_log.rewrite.backup.noise")
        marker_path = Path.join(log_root, "segment_log.rewrite.term")
        outside_backup = Path.join(root, "outside-rewrite-backup-noise")

        File.mkdir_p!(staging)
        File.mkdir_p!(outside_backup)
        assert :ok = File.ln_s(outside_backup, backup)

        marker = %{
          version: 1,
          dir: String.to_charlist(segment_dir),
          staging: String.to_charlist(staging),
          backup: String.to_charlist(backup)
        }

        File.write!(marker_path, :erlang.term_to_binary(marker))

        logs =
          capture_log(fn ->
            assert {:error, reason} =
                     WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

            assert inspect(reason) =~ "unsafe_rewrite_path"
            Process.sleep(50)
          end)

        refute logs =~ "MatchError"
        refute logs =~ "no match of right hand side value"
      after
        WARaftBackend.stop()
      end

      test "segment log fails closed when segment sizing metadata is missing for existing segments",
           %{root: root, ctx: ctx} do
        previous = Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)

        try do
          Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, 2)

          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
          assert :ok = WARaftBackend.write(0, {:put, "log-config-missing:k", "v", 0})
          WARaftBackend.stop()

          root
          |> waraft_segment_log_dir(0)
          |> Path.join("segment_config.term")
          |> File.rm!()

          assert {:error, _reason} =
                   WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
        after
          WARaftBackend.stop()
          restore_env(:waraft_segment_log_records_per_segment, previous)
        end
      end

      test "single-member WARaft batches concurrent commits before segment fsync", %{ctx: ctx} do
        previous_hook = Application.get_env(:ferricstore, :waraft_segment_log_file_sync_hook)

        try do
          assert :ok =
                   WARaftBackend.start(ctx,
                     log_module: :ferricstore_waraft_spike_segment_log,
                     commit_batch_interval_ms: 50,
                     commit_batch_max: 10
                   )

          Application.put_env(:ferricstore, :waraft_segment_log_file_sync_hook, {:notify, self()})

          tasks =
            for i <- 1..2 do
              Task.async(fn ->
                WARaftBackend.write(0, {:put, "single-member-batch:k#{i}", "v#{i}", 0})
              end)
            end

          assert [:ok, :ok] = Enum.map(tasks, &Task.await(&1, 5_000))

          assert_receive {:waraft_segment_log_file_sync, path}, 1_000
          refute_receive {:waraft_segment_log_file_sync, _path}, 100
          assert String.ends_with?(path, ".seg")
          assert "v1" == Router.get(ctx, "single-member-batch:k1")
          assert "v2" == Router.get(ctx, "single-member-batch:k2")
        after
          restore_env(:waraft_segment_log_file_sync_hook, previous_hook)
        end
      end

      test "single-member WARaft persistent segment writer does not ack or apply before fsync",
           %{ctx: ctx} do
        previous_hook = Application.get_env(:ferricstore, :waraft_segment_log_file_sync_hook)

        previous_writer_mode =
          Application.get_env(:ferricstore, :waraft_segment_log_file_writer_mode)

        try do
          Application.put_env(:ferricstore, :waraft_segment_log_file_writer_mode, :persistent)

          assert :ok =
                   WARaftBackend.start(ctx,
                     log_module: :ferricstore_waraft_spike_segment_log,
                     commit_batch_interval_ms: 0,
                     commit_batch_max: 10
                   )

          Application.put_env(:ferricstore, :waraft_segment_log_file_sync_hook, {:block, self()})

          task =
            Task.async(fn ->
              WARaftBackend.write(0, {:put, "persistent-sync-gate:k", "v", 0})
            end)

          assert_receive {:waraft_segment_log_file_sync_blocked, _path, _method, waiter, ref},
                         1_000

          assert Task.yield(task, 50) == nil
          assert nil == Router.get(ctx, "persistent-sync-gate:k")

          send(waiter, {ref, :continue})

          assert :ok == Task.await(task, 5_000)
          assert "v" == Router.get(ctx, "persistent-sync-gate:k")
        after
          restore_env(:waraft_segment_log_file_sync_hook, previous_hook)
          restore_env(:waraft_segment_log_file_writer_mode, previous_writer_mode)
        end
      end

      test "single-member WARaft async segment append keeps server responsive during fsync",
           %{ctx: ctx} do
        previous_hook = Application.get_env(:ferricstore, :waraft_segment_log_file_sync_hook)
        previous_public_async = Application.get_env(:ferricstore, :waraft_async_log_append)
        previous_async = Application.get_env(:ferricstore_waraft_backend, :raft_async_log_append)

        try do
          Application.put_env(:ferricstore, :waraft_async_log_append, true)

          assert :ok =
                   WARaftBackend.start(ctx,
                     log_module: :ferricstore_waraft_spike_segment_log,
                     commit_batch_interval_ms: 0,
                     commit_batch_max: 10
                   )

          Application.put_env(:ferricstore, :waraft_segment_log_file_sync_hook, {:block, self()})

          write_task =
            Task.async(fn ->
              WARaftBackend.write(0, {:put, "async-append-sync-gate:k", "v", 0})
            end)

          assert_receive {:waraft_segment_log_file_sync_blocked, _path, _method, waiter, ref},
                         1_000

          status_task = Task.async(fn -> WARaftBackend.status(0) end)
          assert status = Task.await(status_task, 1_000)
          assert is_list(status)

          assert Task.yield(write_task, 50) == nil
          assert nil == Router.get(ctx, "async-append-sync-gate:k")

          send(waiter, {ref, :continue})

          assert :ok == Task.await(write_task, 5_000)
          assert "v" == Router.get(ctx, "async-append-sync-gate:k")
        after
          restore_env(:waraft_segment_log_file_sync_hook, previous_hook)
          restore_env(:waraft_async_log_append, previous_public_async)
          restore_waraft_app_env(:raft_async_log_append, previous_async)
        end
      end

      test "single-member WARaft clears async append state after resign before completion",
           %{root: root, ctx: ctx} do
        previous_hook = Application.get_env(:ferricstore, :waraft_segment_log_file_sync_hook)
        previous_public_async = Application.get_env(:ferricstore, :waraft_async_log_append)
        previous_async = Application.get_env(:ferricstore_waraft_backend, :raft_async_log_append)

        try do
          Application.put_env(:ferricstore, :waraft_async_log_append, true)

          assert :ok =
                   WARaftBackend.start(ctx,
                     log_module: :ferricstore_waraft_spike_segment_log,
                     commit_batch_interval_ms: 0,
                     commit_batch_max: 10
                   )

          Application.put_env(:ferricstore, :waraft_segment_log_file_sync_hook, {:block, self()})

          blocked_write =
            Task.async(fn ->
              WARaftBackend.write(0, {:put, "async-append-resign:blocked", "blocked", 0})
            end)

          assert_receive {:waraft_segment_log_file_sync_blocked, _path, _method, waiter, ref},
                         1_000

          assert Task.yield(blocked_write, 50) == nil

          server = waraft_server_name(0)
          assert :ok = :wa_raft_server.resign(server)

          send(waiter, {ref, :continue})

          assert {:ok, blocked_result} = Task.yield(blocked_write, 5_000)

          assert blocked_result in [
                   {:error, :not_leader_after_submit},
                   Ferricstore.ErrorReasons.write_timeout_unknown()
                 ]

          Application.delete_env(:ferricstore, :waraft_segment_log_file_sync_hook)

          assert :ok = :wa_raft_server.promote(server, :next, true)

          next_write =
            Task.async(fn ->
              WARaftBackend.write(0, {:put, "async-append-resign:after", "after", 0})
            end)

          assert {:ok, :ok} = Task.yield(next_write, 1_000)
          assert nil == Router.get(ctx, "async-append-resign:blocked")
          assert "after" == Router.get(ctx, "async-append-resign:after")

          assert :ok = WARaftBackend.stop()
          FerricStore.Instance.cleanup(ctx.name)
          restarted_ctx = build_ctx(root)

          assert :ok =
                   WARaftBackend.start(restarted_ctx,
                     log_module: :ferricstore_waraft_spike_segment_log,
                     commit_batch_interval_ms: 0,
                     commit_batch_max: 10
                   )

          assert nil == Router.get(restarted_ctx, "async-append-resign:blocked")
          assert "after" == Router.get(restarted_ctx, "async-append-resign:after")
        after
          restore_env(:waraft_segment_log_file_sync_hook, previous_hook)
          restore_env(:waraft_async_log_append, previous_public_async)
          restore_waraft_app_env(:raft_async_log_append, previous_async)
        end
      end

      test "single-member WARaft stop does not let in-flight async append commit after shutdown",
           %{root: root, ctx: ctx} do
        previous_hook = Application.get_env(:ferricstore, :waraft_segment_log_file_sync_hook)
        previous_public_async = Application.get_env(:ferricstore, :waraft_async_log_append)
        previous_async = Application.get_env(:ferricstore_waraft_backend, :raft_async_log_append)

        try do
          Application.put_env(:ferricstore, :waraft_async_log_append, true)
          key = "async-append-stop:blocked"

          assert :ok =
                   WARaftBackend.start(ctx,
                     log_module: :ferricstore_waraft_spike_segment_log,
                     commit_batch_interval_ms: 0,
                     commit_batch_max: 10
                   )

          Application.put_env(:ferricstore, :waraft_segment_log_file_sync_hook, {:block, self()})

          blocked_write =
            Task.async(fn ->
              WARaftBackend.write(0, {:put, key, "blocked", 0})
            end)

          assert_receive {:waraft_segment_log_file_sync_blocked, _path, _method, waiter, ref},
                         1_000

          assert Task.yield(blocked_write, 50) == nil
          assert nil == Router.get(ctx, key)

          restore_env(:waraft_segment_log_file_sync_hook, previous_hook)
          stop_task = Task.async(fn -> WARaftBackend.stop() end)

          worker_stopped = eventually(fn -> not Process.alive?(waiter) end)
          unless worker_stopped, do: send(waiter, {ref, :continue})

          assert worker_stopped
          assert :ok = Task.await(stop_task, 10_000)

          restore_env(:waraft_segment_log_file_sync_hook, previous_hook)
          Process.sleep(100)

          _ = Task.shutdown(blocked_write, :brutal_kill)

          restarted_ctx = build_ctx(root)

          assert :ok =
                   WARaftBackend.start(restarted_ctx,
                     log_module: :ferricstore_waraft_spike_segment_log,
                     commit_batch_interval_ms: 0,
                     commit_batch_max: 10
                   )

          assert nil == Router.get(restarted_ctx, key)
        after
          WARaftBackend.stop()
          restore_env(:waraft_segment_log_file_sync_hook, previous_hook)
          restore_env(:waraft_async_log_append, previous_public_async)
          restore_waraft_app_env(:raft_async_log_append, previous_async)
        end
      end

      test "pause_writes_for_sync waits for write_many entries awaiting async segment append",
           %{ctx: ctx} do
        previous_hook = Application.get_env(:ferricstore, :waraft_segment_log_file_sync_hook)
        previous_public_async = Application.get_env(:ferricstore, :waraft_async_log_append)
        previous_async = Application.get_env(:ferricstore_waraft_backend, :raft_async_log_append)

        try do
          Application.put_env(:ferricstore, :waraft_async_log_append, true)

          assert :ok =
                   WARaftBackend.start(ctx,
                     log_module: :ferricstore_waraft_spike_segment_log,
                     commit_batch_interval_ms: 0,
                     commit_batch_max: 10
                   )

          Application.put_env(:ferricstore, :waraft_segment_log_file_sync_hook, {:block, self()})

          write_task =
            Task.async(fn ->
              WARaftBackend.write_many([{0, {:put, "async-write-many-sync:k", "v", 0}}])
            end)

          assert_receive {:waraft_segment_log_file_sync_blocked, _path, _method, waiter, ref},
                         1_000

          assert Task.yield(write_task, 50) == nil

          pause_task =
            Task.async(fn ->
              Ferricstore.Raft.Batcher.pause_writes_for_sync(0, 5_000)
            end)

          refute Task.yield(pause_task, 50),
                 "pause must wait for write_many entries submitted before the pause"

          restore_env(:waraft_segment_log_file_sync_hook, previous_hook)
          send(waiter, {ref, :continue})

          assert [:ok] == Task.await(write_task, 5_000)
          assert {:ok, :ok} == Task.yield(pause_task, 5_000)
          assert "v" == Router.get(ctx, "async-write-many-sync:k")

          assert :ok = Ferricstore.Raft.Batcher.resume_writes_for_sync(0, 1_000)
        after
          restore_env(:waraft_segment_log_file_sync_hook, previous_hook)
          restore_env(:waraft_async_log_append, previous_public_async)
          restore_waraft_app_env(:raft_async_log_append, previous_async)
        end
      end

      test "single-member WARaft async segment append batches commands that arrive during fsync",
           %{ctx: ctx} do
        previous_hook = Application.get_env(:ferricstore, :waraft_segment_log_file_sync_hook)
        previous_public_async = Application.get_env(:ferricstore, :waraft_async_log_append)
        previous_async = Application.get_env(:ferricstore_waraft_backend, :raft_async_log_append)
        handler_id = {__MODULE__, :async_segment_append_batches, make_ref()}

        :telemetry.attach(
          handler_id,
          [:ferricstore, :waraft, :segment_log, :append],
          &__MODULE__.handle_segment_log_telemetry/4,
          self()
        )

        on_exit(fn -> :telemetry.detach(handler_id) end)

        try do
          Application.put_env(:ferricstore, :waraft_async_log_append, true)

          assert :ok =
                   WARaftBackend.start(ctx,
                     log_module: :ferricstore_waraft_spike_segment_log,
                     commit_batch_interval_ms: 0,
                     commit_batch_max: 10
                   )

          Application.put_env(:ferricstore, :waraft_segment_log_file_sync_hook, {:block, self()})

          first =
            Task.async(fn -> WARaftBackend.write(0, {:put, "async-append-batch:1", "v1", 0}) end)

          assert_receive {:waraft_segment_log_file_sync_blocked, _path1, _method1, waiter1, ref1},
                         1_000

          second =
            Task.async(fn -> WARaftBackend.write(0, {:put, "async-append-batch:2", "v2", 0}) end)

          third =
            Task.async(fn -> WARaftBackend.write(0, {:put, "async-append-batch:3", "v3", 0}) end)

          assert is_list(WARaftBackend.status(0))
          assert Task.yield(second, 50) == nil
          assert Task.yield(third, 50) == nil
          assert nil == Router.get(ctx, "async-append-batch:2")
          assert nil == Router.get(ctx, "async-append-batch:3")

          send(waiter1, {ref1, :continue})
          assert :ok == Task.await(first, 5_000)

          assert_receive {:waraft_segment_log_telemetry,
                          [:ferricstore, :waraft, :segment_log, :append], %{count: 1},
                          %{result: :ok}},
                         1_000

          assert_receive {:waraft_segment_log_file_sync_blocked, _path2, _method2, waiter2, ref2},
                         1_000

          assert Task.yield(second, 50) == nil
          assert Task.yield(third, 50) == nil

          send(waiter2, {ref2, :continue})

          assert :ok == Task.await(second, 5_000)
          assert :ok == Task.await(third, 5_000)

          assert_receive {:waraft_segment_log_telemetry,
                          [:ferricstore, :waraft, :segment_log, :append], %{count: 2},
                          %{result: :ok}},
                         1_000

          assert "v1" == Router.get(ctx, "async-append-batch:1")
          assert "v2" == Router.get(ctx, "async-append-batch:2")
          assert "v3" == Router.get(ctx, "async-append-batch:3")
        after
          restore_env(:waraft_segment_log_file_sync_hook, previous_hook)
          restore_env(:waraft_async_log_append, previous_public_async)
          restore_waraft_app_env(:raft_async_log_append, previous_async)
        end
      end

      test "single-member WARaft async segment append still trims log without a later write",
           %{ctx: ctx} do
        previous_public_async = Application.get_env(:ferricstore, :waraft_async_log_append)
        previous_async = Application.get_env(:ferricstore_waraft_backend, :raft_async_log_append)

        try do
          Application.put_env(:ferricstore, :waraft_async_log_append, true)

          assert :ok =
                   WARaftBackend.start(ctx,
                     log_module: :ferricstore_waraft_spike_segment_log,
                     log_rotation_interval: 2,
                     log_rotation_keep: 2,
                     max_retained_entries: 2,
                     apply_log_batch_size: 8,
                     commit_batch_interval_ms: 0,
                     commit_batch_max: 1
                   )

          for n <- 1..24 do
            assert :ok = WARaftBackend.write(0, {:put, "async-trim:k#{n}", "v#{n}", 0})
          end

          log_table = waraft_log_table(0)
          max_idle_tail_entries = 2 + 2 + 1

          assert_eventually(
            fn ->
              case :ets.info(log_table, :size) do
                # Batched rotation may retain one idle async-completion entry in
                # addition to the configured keep and rotation intervals.
                size when is_integer(size) and size <= max_idle_tail_entries ->
                  :trimmed

                size ->
                  {:not_trimmed, size}
              end
            end,
            :trimmed,
            1_000
          )

          assert "v1" == Router.get(ctx, "async-trim:k1")
          assert "v24" == Router.get(ctx, "async-trim:k24")
        after
          restore_env(:waraft_async_log_append, previous_public_async)
          restore_waraft_app_env(:raft_async_log_append, previous_async)
        end
      end

      test "single-member WARaft async segment append fsync failure does not apply before restart",
           %{root: root, ctx: ctx} do
        previous_hook = Application.get_env(:ferricstore, :waraft_segment_log_file_sync_hook)
        previous_public_async = Application.get_env(:ferricstore, :waraft_async_log_append)
        previous_async = Application.get_env(:ferricstore_waraft_backend, :raft_async_log_append)

        try do
          Application.put_env(:ferricstore, :waraft_async_log_append, true)

          assert :ok =
                   WARaftBackend.start(ctx,
                     log_module: :ferricstore_waraft_spike_segment_log,
                     commit_batch_interval_ms: 0,
                     commit_batch_max: 10
                   )

          Application.put_env(
            :ferricstore,
            :waraft_segment_log_file_sync_hook,
            {:fail_once, self()}
          )

          assert Ferricstore.ErrorReasons.write_timeout_unknown() ==
                   WARaftBackend.write(0, {:put, "async-log-file-sync-fail:k", "v1", 0})

          assert_receive {:waraft_segment_log_file_sync, _path}, 1_000
          assert nil == Router.get(ctx, "async-log-file-sync-fail:k")

          assert :ok = WARaftBackend.stop()
          FerricStore.Instance.cleanup(ctx.name)

          restarted_ctx = build_ctx(root)

          assert :ok =
                   WARaftBackend.start(restarted_ctx,
                     log_module: :ferricstore_waraft_spike_segment_log,
                     commit_batch_interval_ms: 0,
                     commit_batch_max: 10
                   )

          assert nil == Router.get(restarted_ctx, "async-log-file-sync-fail:k")
        after
          restore_env(:waraft_segment_log_file_sync_hook, previous_hook)
          restore_env(:waraft_async_log_append, previous_public_async)
          restore_waraft_app_env(:raft_async_log_append, previous_async)
        end
      end

      test "default WARaft hot put batches coalesce before segment append", %{ctx: ctx} do
        previous_window = Application.get_env(:ferricstore, :waraft_hot_batch_window_ms)
        handler_id = {__MODULE__, :hot_put_flush, make_ref()}

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

          tasks =
            for i <- 1..2 do
              Task.async(fn ->
                WARaftBackend.write_put_batch(0, [{"hot-put-batch:#{i}", "v#{i}", 0}])
              end)
            end

          assert [{:ok, [:ok]}, {:ok, [:ok]}] = Enum.map(tasks, &Task.await(&1, 5_000))

          assert_receive {:waraft_namespace_batcher_flush,
                          [:ferricstore, :waraft, :batcher, :hot_flush],
                          %{batch_size: 2, group_count: 2} = measurements, %{kind: :put_batch}},
                         1_000

          assert is_integer(measurements.queue_age_us)
          assert measurements.queue_age_us >= 0
          assert is_integer(measurements.flush_duration_us)
          assert measurements.flush_duration_us >= 0
          assert is_integer(measurements.total_duration_us)
          assert measurements.total_duration_us >= measurements.queue_age_us
          assert measurements.total_duration_us >= measurements.flush_duration_us

          assert "v1" == Router.get(ctx, "hot-put-batch:1")
          assert "v2" == Router.get(ctx, "hot-put-batch:2")
        after
          restore_env(:waraft_hot_batch_window_ms, previous_window)
        end
      end

      test "WARaft hot put batch async API replies to an explicit waiter", %{ctx: ctx} do
        previous_window = Application.get_env(:ferricstore, :waraft_hot_batch_window_ms)

        try do
          Application.put_env(:ferricstore, :waraft_hot_batch_window_ms, 1)

          assert :ok =
                   WARaftBackend.start(ctx,
                     log_module: :ferricstore_waraft_spike_segment_log,
                     commit_batch_interval_ms: 1,
                     commit_batch_max: 10_000
                   )

          {from, token} = Ferricstore.Raft.ReplyAwaiter.new()

          assert :ok =
                   WARaftBackend.write_put_batch_async(
                     0,
                     [
                       {"hot-put-async-waiter:1", "v1", 0}
                     ],
                     from
                   )

          assert {:ok, [:ok]} =
                   Ferricstore.Raft.ReplyAwaiter.await(
                     token,
                     5_000,
                     ErrorReasons.write_timeout_unknown()
                   )

          assert "v1" == Router.get(ctx, "hot-put-async-waiter:1")
        after
          restore_env(:waraft_hot_batch_window_ms, previous_window)
        end
      end

      test "default WARaft generic batches coalesce before segment append", %{ctx: ctx} do
        previous_window = Application.get_env(:ferricstore, :waraft_hot_batch_window_ms)

        previous_generic_window =
          Application.get_env(:ferricstore, :waraft_generic_batch_window_ms)

        handler_id = {__MODULE__, :hot_generic_batch_flush, make_ref()}

        :telemetry.attach(
          handler_id,
          [:ferricstore, :waraft, :batcher, :hot_flush],
          &__MODULE__.handle_namespace_batcher_telemetry/4,
          self()
        )

        on_exit(fn -> :telemetry.detach(handler_id) end)

        try do
          Application.put_env(:ferricstore, :waraft_hot_batch_window_ms, 25)
          Application.put_env(:ferricstore, :waraft_generic_batch_window_ms, 25)

          assert :ok =
                   WARaftBackend.start(ctx,
                     log_module: :ferricstore_waraft_spike_segment_log,
                     commit_batch_interval_ms: 1,
                     commit_batch_max: 10_000
                   )

          tasks =
            for i <- 1..2 do
              Task.async(fn ->
                WARaftBackend.write_batch(0, [
                  {:put, "hot-generic-batch:#{i}:a", "v#{i}a", 0},
                  {:put, "hot-generic-batch:#{i}:b", "v#{i}b", 0}
                ])
              end)
            end

          assert [{:ok, [:ok, :ok]}, {:ok, [:ok, :ok]}] = Enum.map(tasks, &Task.await(&1, 5_000))

          assert_receive {:waraft_namespace_batcher_flush,
                          [:ferricstore, :waraft, :batcher, :hot_flush],
                          %{batch_size: 4, group_count: 2}, %{kind: :batch}},
                         1_000

          assert "v1a" == Router.get(ctx, "hot-generic-batch:1:a")
          assert "v1b" == Router.get(ctx, "hot-generic-batch:1:b")
          assert "v2a" == Router.get(ctx, "hot-generic-batch:2:a")
          assert "v2b" == Router.get(ctx, "hot-generic-batch:2:b")
        after
          restore_env(:waraft_hot_batch_window_ms, previous_window)
          restore_env(:waraft_generic_batch_window_ms, previous_generic_window)
        end
      end
    end
  end
end
