defmodule Ferricstore.Raft.WARaftBackendTest.Sections.StorageMetadataHotWritesFsyncJournalRewritingCurrentMetadata do
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

      test "storage metadata hot writes fsync journal without rewriting current metadata", %{
        ctx: ctx
      } do
        test_pid = self()

        previous_hook =
          Application.get_env(:ferricstore, :waraft_storage_metadata_fsync_file_hook)

        previous_every = Application.get_env(:ferricstore, :waraft_storage_metadata_persist_every)

        previous_payload_every =
          Application.get_env(:ferricstore, :waraft_bitcask_payload_fsync_every)

        try do
          Application.put_env(:ferricstore, :waraft_storage_metadata_persist_every, 1)
          Application.put_env(:ferricstore, :waraft_bitcask_payload_fsync_every, 1)

          Application.put_env(:ferricstore, :waraft_storage_metadata_fsync_file_hook, fn path ->
            send(test_pid, {:storage_metadata_fsync, path})
            :ok
          end)

          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
          drain_storage_metadata_fsyncs()

          assert :ok = WARaftBackend.write(0, {:put, "metadata-fsync:k", "v", 0})

          assert_receive {:storage_metadata_fsync, path}, 1_000
          paths = [path | collect_storage_metadata_fsyncs()]

          assert Enum.any?(paths, &String.ends_with?(&1, "ferricstore_storage.term.journal"))
          refute Enum.any?(paths, &String.contains?(&1, "ferricstore_storage.term.tmp."))
        after
          restore_env(:waraft_storage_metadata_fsync_file_hook, previous_hook)
          restore_env(:waraft_storage_metadata_persist_every, previous_every)
          restore_env(:waraft_bitcask_payload_fsync_every, previous_payload_every)
        end
      end

      test "storage apply phase telemetry breaks down hot write tail latency", %{ctx: ctx} do
        assert {:ok, _apps} = Application.ensure_all_started(:telemetry)

        handler_id = {__MODULE__, :storage_apply_phase_profile, make_ref()}

        :telemetry.attach(
          handler_id,
          [:ferricstore, :waraft, :storage, :apply_phase],
          &__MODULE__.handle_storage_apply_phase_telemetry/4,
          self()
        )

        on_exit(fn -> :telemetry.detach(handler_id) end)

        previous_every = Application.get_env(:ferricstore, :waraft_storage_metadata_persist_every)

        previous_payload_every =
          Application.get_env(:ferricstore, :waraft_bitcask_payload_fsync_every)

        try do
          Application.put_env(:ferricstore, :waraft_storage_metadata_persist_every, 1)
          Application.put_env(:ferricstore, :waraft_bitcask_payload_fsync_every, 1)

          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
          assert :ok = WARaftBackend.write(0, {:put, "apply-phase:k", "v", 0})

          assert_receive {:waraft_storage_apply_phase,
                          [:ferricstore, :waraft, :storage, :apply_phase],
                          %{duration_us: cache_us},
                          %{
                            phase: :apply_projection_cache,
                            shard_index: 0,
                            position: {:raft_log_pos, _, _}
                          }},
                         1_000

          assert is_integer(cache_us) and cache_us >= 0

          assert_receive {:waraft_storage_apply_phase,
                          [:ferricstore, :waraft, :storage, :apply_phase],
                          %{duration_us: projection_us},
                          %{phase: :recovery_projection, shard_index: 0}},
                         1_000

          assert is_integer(projection_us) and projection_us >= 0

          assert_receive {:waraft_storage_apply_phase,
                          [:ferricstore, :waraft, :storage, :apply_phase],
                          %{duration_us: metadata_us},
                          %{phase: :storage_metadata, shard_index: 0}},
                         1_000

          assert is_integer(metadata_us) and metadata_us >= 0
        after
          restore_env(:waraft_storage_metadata_persist_every, previous_every)
          restore_env(:waraft_bitcask_payload_fsync_every, previous_payload_every)
        end
      end

      test "hot metadata persistence does not checkpoint segment projection on apply", %{ctx: ctx} do
        assert {:ok, _apps} = Application.ensure_all_started(:telemetry)

        handler_id = {__MODULE__, :hot_apply_no_segment_projection_checkpoint, make_ref()}

        :telemetry.attach(
          handler_id,
          [:ferricstore, :waraft, :segment_log, :append],
          &__MODULE__.handle_segment_log_telemetry/4,
          self()
        )

        on_exit(fn -> :telemetry.detach(handler_id) end)

        previous_every = Application.get_env(:ferricstore, :waraft_storage_metadata_persist_every)

        previous_payload_every =
          Application.get_env(:ferricstore, :waraft_bitcask_payload_fsync_every)

        try do
          Application.put_env(:ferricstore, :waraft_storage_metadata_persist_every, 1)
          Application.put_env(:ferricstore, :waraft_bitcask_payload_fsync_every, 1)

          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
          flush_segment_append_telemetry()

          assert :ok = WARaftBackend.write(0, {:put, "hot-no-checkpoint:k", "v", 0})
          assert "v" == Router.get(ctx, "hot-no-checkpoint:k")

          refute_receive {:waraft_segment_log_telemetry,
                          [:ferricstore, :waraft, :segment_log, :append], _measurements,
                          %{kind: :segment_projection}},
                         200
        after
          restore_env(:waraft_storage_metadata_persist_every, previous_every)
          restore_env(:waraft_bitcask_payload_fsync_every, previous_payload_every)
        end
      end

      test "segment projection checkpoint runs in background with pending guard", %{ctx: ctx} do
        assert {:ok, _apps} = Application.ensure_all_started(:telemetry)

        parent = self()
        handler_id = {__MODULE__, :segment_projection_checkpoint_pending_guard, make_ref()}

        :telemetry.attach_many(
          handler_id,
          [
            [:ferricstore, :waraft, :segment_projection_checkpoint, :start],
            [:ferricstore, :waraft, :segment_projection_checkpoint, :stop]
          ],
          &__MODULE__.handle_segment_projection_checkpoint_telemetry/4,
          parent
        )

        on_exit(fn -> :telemetry.detach(handler_id) end)

        previous_every =
          Application.get_env(:ferricstore, :waraft_segment_projection_checkpoint_every)

        previous_interval =
          Application.get_env(:ferricstore, :waraft_segment_projection_checkpoint_min_interval_ms)

        previous_hook =
          Application.get_env(:ferricstore, :waraft_segment_projection_checkpoint_hook)

        hook = fn
          :before_write, metadata ->
            send(parent, {:checkpoint_before_write, self(), metadata})

            receive do
              :release_checkpoint -> :ok
            after
              5_000 -> :ok
            end

          _phase, _metadata ->
            :ok
        end

        try do
          Application.put_env(:ferricstore, :waraft_segment_projection_checkpoint_every, 2)

          Application.put_env(
            :ferricstore,
            :waraft_segment_projection_checkpoint_min_interval_ms,
            0
          )

          Application.put_env(:ferricstore, :waraft_segment_projection_checkpoint_hook, hook)

          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          assert :ok = WARaftBackend.write(0, {:put, "checkpoint-bg:k1", "v1", 0})

          assert_receive {:checkpoint_before_write, checkpoint_pid,
                          %{position: {:raft_log_pos, checkpoint_index, _}}},
                         1_000

          assert is_integer(checkpoint_index) and checkpoint_index > 0

          assert_receive {:waraft_segment_projection_checkpoint,
                          [:ferricstore, :waraft, :segment_projection_checkpoint, :start],
                          %{entries: entries},
                          %{position: {:raft_log_pos, ^checkpoint_index, _}}},
                         1_000

          assert entries >= 1

          assert :ok = WARaftBackend.write(0, {:put, "checkpoint-bg:k2", "v2", 0})
          assert "v2" == Router.get(ctx, "checkpoint-bg:k2")
          refute_receive {:checkpoint_before_write, _other_pid, _metadata}, 100

          status = waraft_storage_status(0)
          assert Keyword.fetch!(status, :segment_projection_checkpoint_pending?) == true

          send(checkpoint_pid, :release_checkpoint)

          assert_receive {:waraft_segment_projection_checkpoint,
                          [:ferricstore, :waraft, :segment_projection_checkpoint, :stop],
                          %{duration_us: duration_us},
                          %{result: :ok, position: {:raft_log_pos, ^checkpoint_index, _}}},
                         2_000

          assert is_integer(duration_us) and duration_us >= 0

          assert_eventually(
            fn ->
              status = waraft_storage_status(0)

              {
                Keyword.fetch!(status, :segment_projection_checkpoint_pending?),
                position_index(Keyword.fetch!(status, :segment_projection_position))
              }
            end,
            {false, checkpoint_index}
          )
        after
          restore_env(:waraft_segment_projection_checkpoint_every, previous_every)
          restore_env(:waraft_segment_projection_checkpoint_min_interval_ms, previous_interval)
          restore_env(:waraft_segment_projection_checkpoint_hook, previous_hook)
        end
      end

      test "trim reuses background segment projection checkpoint", %{ctx: ctx} do
        assert {:ok, _apps} = Application.ensure_all_started(:telemetry)

        parent = self()
        checkpoint_handler_id = {__MODULE__, :segment_projection_trim_checkpoint, make_ref()}
        trim_handler_id = {__MODULE__, :segment_projection_trim_reuse, make_ref()}

        :telemetry.attach(
          checkpoint_handler_id,
          [:ferricstore, :waraft, :segment_projection_checkpoint, :stop],
          &__MODULE__.handle_segment_projection_checkpoint_telemetry/4,
          parent
        )

        :telemetry.attach(
          trim_handler_id,
          [:ferricstore, :waraft, :segment_projection_trim, :checkpoint_reuse],
          &__MODULE__.handle_segment_projection_trim_telemetry/4,
          parent
        )

        on_exit(fn ->
          :telemetry.detach(checkpoint_handler_id)
          :telemetry.detach(trim_handler_id)
        end)

        previous_every =
          Application.get_env(:ferricstore, :waraft_segment_projection_checkpoint_every)

        previous_interval =
          Application.get_env(:ferricstore, :waraft_segment_projection_checkpoint_min_interval_ms)

        try do
          Application.put_env(:ferricstore, :waraft_segment_projection_checkpoint_every, 3)

          Application.put_env(
            :ferricstore,
            :waraft_segment_projection_checkpoint_min_interval_ms,
            0
          )

          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          assert :ok = WARaftBackend.write(0, {:put, "checkpoint-trim:k1", "v1", 0})
          assert :ok = WARaftBackend.write(0, {:put, "checkpoint-trim:k2", "v2", 0})

          assert_receive {:waraft_segment_projection_checkpoint,
                          [:ferricstore, :waraft, :segment_projection_checkpoint, :stop],
                          _measurements,
                          %{
                            result: :ok,
                            position: {:raft_log_pos, checkpoint_index, _},
                            entries: entries
                          }},
                         2_000

          assert checkpoint_index >= 4
          assert entries >= 2

          log = waraft_segment_log_record(0)

          assert {:ok, _state} =
                   :ferricstore_waraft_spike_segment_log.trim(log, checkpoint_index, %{})

          assert_receive {:waraft_segment_projection_trim,
                          [:ferricstore, :waraft, :segment_projection_trim, :checkpoint_reuse],
                          %{relocations: relocations},
                          %{
                            trim_index: ^checkpoint_index,
                            checkpoint_index: ^checkpoint_index
                          }},
                         1_000

          assert relocations >= 1
          assert "v1" == Router.get(ctx, "checkpoint-trim:k1")

          assert [
                   {"checkpoint-trim:k1", "v1", 0, _lfu, {:waraft_projection, projection_index},
                    projection_offset, 2}
                 ] = :ets.lookup(elem(ctx.keydir_refs, 0), "checkpoint-trim:k1")

          assert is_integer(projection_index) and projection_index > 0
          assert is_integer(projection_offset) and projection_offset > 0
        after
          restore_env(:waraft_segment_projection_checkpoint_every, previous_every)
          restore_env(:waraft_segment_projection_checkpoint_min_interval_ms, previous_interval)
        end
      end

      test "background segment projection checkpoint does not clobber active projection rows", %{
        root: root,
        ctx: ctx
      } do
        assert {:ok, _apps} = Application.ensure_all_started(:telemetry)

        parent = self()
        checkpoint_handler_id = {__MODULE__, :segment_projection_no_clobber, make_ref()}

        :telemetry.attach(
          checkpoint_handler_id,
          [:ferricstore, :waraft, :segment_projection_checkpoint, :stop],
          &__MODULE__.handle_segment_projection_checkpoint_telemetry/4,
          parent
        )

        on_exit(fn -> :telemetry.detach(checkpoint_handler_id) end)

        previous_every =
          Application.get_env(:ferricstore, :waraft_segment_projection_checkpoint_every)

        previous_interval =
          Application.get_env(:ferricstore, :waraft_segment_projection_checkpoint_min_interval_ms)

        try do
          Application.put_env(:ferricstore, :waraft_segment_projection_checkpoint_every, :never)

          Application.put_env(
            :ferricstore,
            :waraft_segment_projection_checkpoint_min_interval_ms,
            0
          )

          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          z_key = Ferricstore.Flow.Keys.value_key("checkpoint-clobber:z", :payload, 1, "p")
          m_key = Ferricstore.Flow.Keys.value_key("checkpoint-clobber:m", :payload, 1, "p")
          a_key = Ferricstore.Flow.Keys.value_key("checkpoint-clobber:a", :payload, 1, "p")

          assert :ok = WARaftBackend.write(0, {:put, z_key, "z", 0})
          assert :ok = WARaftBackend.write(0, {:put, m_key, "m", 0})

          log = waraft_segment_log_record(0)
          trim_index = :ferricstore_waraft_spike_segment_log.last_index(log)
          assert {:ok, _state} = :ferricstore_waraft_spike_segment_log.trim(log, trim_index, %{})

          assert [
                   {^z_key, _value, 0, _lfu, {:waraft_projection, old_projection_index},
                    old_projection_offset, 1}
                 ] = :ets.lookup(elem(ctx.keydir_refs, 0), z_key)

          assert is_integer(old_projection_index) and old_projection_index > 0
          assert is_integer(old_projection_offset) and old_projection_offset > 0

          Application.put_env(:ferricstore, :waraft_segment_projection_checkpoint_every, 1)

          assert :ok = WARaftBackend.write(0, {:put, a_key, "a", 0})

          assert_receive {:waraft_segment_projection_checkpoint,
                          [:ferricstore, :waraft, :segment_projection_checkpoint, :stop],
                          _measurements, %{result: :ok}},
                         2_000

          assert "z" == Router.get(ctx, z_key)

          assert [
                   {^z_key, _value, 0, _lfu, {:waraft_projection, ^old_projection_index},
                    ^old_projection_offset, 1}
                 ] = :ets.lookup(elem(ctx.keydir_refs, 0), z_key)

          assert File.exists?(
                   Path.join([
                     root,
                     "waraft",
                     "ferricstore_waraft_backend.1",
                     "segment_projection_checkpoint_log"
                   ])
                 )
        after
          restore_env(:waraft_segment_projection_checkpoint_every, previous_every)
          restore_env(:waraft_segment_projection_checkpoint_min_interval_ms, previous_interval)
        end
      end

      test "background segment projection checkpoint is serialized with projection trim", %{
        ctx: ctx
      } do
        assert {:ok, _apps} = Application.ensure_all_started(:telemetry)

        parent = self()
        checkpoint_handler_id = {__MODULE__, :segment_projection_checkpoint_trim_race, make_ref()}

        :telemetry.attach(
          checkpoint_handler_id,
          [:ferricstore, :waraft, :segment_projection_checkpoint, :stop],
          &__MODULE__.handle_segment_projection_checkpoint_telemetry/4,
          parent
        )

        on_exit(fn -> :telemetry.detach(checkpoint_handler_id) end)

        previous_every =
          Application.get_env(:ferricstore, :waraft_segment_projection_checkpoint_every)

        previous_interval =
          Application.get_env(:ferricstore, :waraft_segment_projection_checkpoint_min_interval_ms)

        previous_hook =
          Application.get_env(:ferricstore, :waraft_segment_projection_checkpoint_hook)

        previous_relocate_hook =
          Application.get_env(:ferricstore, :waraft_segment_projection_before_relocate_hook)

        hook = fn
          :before_write, metadata ->
            send(parent, {:checkpoint_before_write, self(), metadata})

            receive do
              :release_checkpoint -> :ok
            after
              5_000 -> :ok
            end

          _phase, _metadata ->
            :ok
        end

        relocate_hook = fn shard_index, _projection_root, relocations ->
          send(parent, {:trim_before_relocate, shard_index, length(relocations)})
          :ok
        end

        try do
          Application.put_env(:ferricstore, :waraft_segment_projection_checkpoint_every, :never)

          Application.put_env(
            :ferricstore,
            :waraft_segment_projection_checkpoint_min_interval_ms,
            0
          )

          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          z_key = Ferricstore.Flow.Keys.value_key("checkpoint-race:z", :payload, 1, "p")
          m_key = Ferricstore.Flow.Keys.value_key("checkpoint-race:m", :payload, 1, "p")
          a_key = Ferricstore.Flow.Keys.value_key("checkpoint-race:a", :payload, 1, "p")

          assert :ok = WARaftBackend.write(0, {:put, z_key, "z", 0})
          assert :ok = WARaftBackend.write(0, {:put, m_key, "m", 0})

          log = waraft_segment_log_record(0)
          first_trim_index = :ferricstore_waraft_spike_segment_log.last_index(log)

          assert {:ok, _state} =
                   :ferricstore_waraft_spike_segment_log.trim(log, first_trim_index, %{})

          assert [
                   {^z_key, _value, 0, _lfu, {:waraft_projection, old_projection_index},
                    old_projection_offset, 1}
                 ] = :ets.lookup(elem(ctx.keydir_refs, 0), z_key)

          Application.put_env(:ferricstore, :waraft_segment_projection_checkpoint_every, 1)
          Application.put_env(:ferricstore, :waraft_segment_projection_checkpoint_hook, hook)

          Application.put_env(
            :ferricstore,
            :waraft_segment_projection_before_relocate_hook,
            relocate_hook
          )

          assert :ok = WARaftBackend.write(0, {:put, a_key, "a", 0})

          assert_receive {:checkpoint_before_write, checkpoint_pid,
                          %{position: {:raft_log_pos, checkpoint_index, _}}},
                         1_000

          assert checkpoint_index > first_trim_index

          trim_ref = make_ref()

          spawn(fn ->
            send(
              parent,
              {trim_ref, :ferricstore_waraft_spike_segment_log.trim(log, checkpoint_index, %{})}
            )
          end)

          refute_receive {^trim_ref, _trim_result}, 100
          refute_receive {:trim_before_relocate, _shard_index, _relocations}, 100

          send(checkpoint_pid, :release_checkpoint)

          assert_receive {^trim_ref, {:ok, _state}}, 2_000

          assert_receive {:trim_before_relocate, 0, relocation_count} when relocation_count > 0,
                         2_000

          assert_receive {:waraft_segment_projection_checkpoint,
                          [:ferricstore, :waraft, :segment_projection_checkpoint, :stop],
                          _measurements, %{result: :ok}},
                         2_000

          assert "z" == Router.get(ctx, z_key)

          assert [
                   {^z_key, _value, 0, _lfu, {:waraft_projection, current_projection_index},
                    current_projection_offset, 1}
                 ] = :ets.lookup(elem(ctx.keydir_refs, 0), z_key)

          assert {current_projection_index, current_projection_offset} !=
                   {old_projection_index, old_projection_offset}
        after
          restore_env(:waraft_segment_projection_checkpoint_every, previous_every)
          restore_env(:waraft_segment_projection_checkpoint_min_interval_ms, previous_interval)
          restore_env(:waraft_segment_projection_checkpoint_hook, previous_hook)
          restore_env(:waraft_segment_projection_before_relocate_hook, previous_relocate_hook)
        end
      end

      test "storage metadata journal symlink does not append outside shard root", %{
        root: root,
        ctx: ctx
      } do
        test_pid = self()

        previous_hook =
          Application.get_env(:ferricstore, :waraft_storage_metadata_fsync_file_hook)

        previous_every = Application.get_env(:ferricstore, :waraft_storage_metadata_persist_every)

        previous_payload_every =
          Application.get_env(:ferricstore, :waraft_bitcask_payload_fsync_every)

        try do
          Application.put_env(:ferricstore, :waraft_storage_metadata_persist_every, 1)
          Application.put_env(:ferricstore, :waraft_bitcask_payload_fsync_every, 1)

          Application.put_env(:ferricstore, :waraft_storage_metadata_fsync_file_hook, fn path ->
            send(test_pid, {:storage_metadata_fsync, path})
            :ok
          end)

          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
          assert :ok = WARaftBackend.write(0, {:put, "metadata-journal-symlink:warmup", "v", 0})
          _ = collect_storage_metadata_fsyncs()

          journal_path = waraft_storage_metadata_journal_path(root, 0)
          outside_path = Path.join(root, "outside-storage-metadata-journal")
          sentinel = "outside-sentinel"

          File.rm(journal_path)
          File.write!(outside_path, sentinel)
          assert :ok = File.ln_s(outside_path, journal_path)
          assert {:ok, %{type: :symlink}} = File.lstat(journal_path)

          assert Ferricstore.ErrorReasons.write_timeout_unknown() ==
                   WARaftBackend.write(0, {:put, "metadata-journal-symlink:k", "v", 0})

          assert File.read!(outside_path) == sentinel
        after
          restore_env(:waraft_storage_metadata_fsync_file_hook, previous_hook)
          restore_env(:waraft_storage_metadata_persist_every, previous_every)
          restore_env(:waraft_bitcask_payload_fsync_every, previous_payload_every)
        end
      end

      test "storage metadata journal recovery streams records instead of reading whole journal" do
        source =
          Ferricstore.Test.SourceFiles.waraft_storage_source()

        refute source =~ "File.read(journal_path)",
               "metadata journal recovery must not materialize the full journal in BEAM memory"

        assert source =~ "File.open(journal_path, [:read, :binary])"
        assert source =~ "read_metadata_journal_record"
      end

      @tag :persisted_apply_context_encoding
      test "storage metadata persists apply context without an Elixir struct", %{
        root: root,
        ctx: ctx
      } do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

        persisted = waraft_storage_metadata(root, 0)

        assert persisted.apply_context ==
                 Ferricstore.Raft.ApplyContext.encode(ctx.apply_context)
      end

      test "restart fails closed on oversized storage metadata journal record", %{
        root: root,
        ctx: ctx
      } do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
        assert :ok = WARaftBackend.write(0, {:put, "metadata:oversized-journal", "value", 0})
        assert "value" == Router.get(ctx, "metadata:oversized-journal")

        metadata =
          root
          |> waraft_latest_storage_metadata(0)
          |> Map.put(:label, :binary.copy("x", 2 * 1024 * 1024))

        assert :ok = WARaftBackend.stop()

        payload = :erlang.term_to_binary(metadata)
        record = <<"FSMJ1", byte_size(payload)::32, :erlang.crc32(payload)::32, payload::binary>>

        File.write!(waraft_storage_metadata_path(root, 0), <<131, 116, 0, 0, 0, 3, "torn">>)

        File.write!(
          waraft_storage_metadata_previous_path(root, 0),
          <<131, 116, 0, 0, 0, 3, "also-torn">>
        )

        File.write!(waraft_storage_metadata_journal_path(root, 0), record)

        FerricStore.Instance.cleanup(ctx.name)
        restarted_ctx = build_ctx(root)

        assert {:error, reason} =
                 WARaftBackend.start(restarted_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert inspect(reason) =~ "metadata_journal_record_too_large"
        assert shard_payload_present?(restarted_ctx, 0)
      end

      test "restart fails closed on oversized current storage metadata file", %{
        root: root,
        ctx: ctx
      } do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
        assert :ok = WARaftBackend.write(0, {:put, "metadata:oversized-current", "value", 0})
        assert "value" == Router.get(ctx, "metadata:oversized-current")

        metadata =
          root
          |> waraft_latest_storage_metadata(0)
          |> Map.put(:label, :binary.copy("x", 2 * 1024 * 1024))

        assert :ok = WARaftBackend.stop()

        File.write!(waraft_storage_metadata_path(root, 0), :erlang.term_to_binary(metadata))
        File.rm(waraft_storage_metadata_previous_path(root, 0))
        File.rm(waraft_storage_metadata_journal_path(root, 0))

        FerricStore.Instance.cleanup(ctx.name)
        restarted_ctx = build_ctx(root)

        assert {:error, reason} =
                 WARaftBackend.start(restarted_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert inspect(reason) =~ "storage_metadata_file_too_large"
        assert shard_payload_present?(restarted_ctx, 0)
      end

      test "storage metadata hot persistence can lag without blocking acknowledged replay", %{
        root: root,
        ctx: ctx
      } do
        test_pid = self()

        previous_hook =
          Application.get_env(:ferricstore, :waraft_storage_metadata_fsync_file_hook)

        previous_every = Application.get_env(:ferricstore, :waraft_storage_metadata_persist_every)

        try do
          Application.put_env(:ferricstore, :waraft_storage_metadata_persist_every, 128)

          Application.put_env(:ferricstore, :waraft_storage_metadata_fsync_file_hook, fn path ->
            send(test_pid, {:storage_metadata_fsync, path})
            :ok
          end)

          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
          drain_storage_metadata_fsyncs()
          assert {:ok, persisted_before} = WARaftBackend.storage_position(0)

          assert :ok = WARaftBackend.write(0, {:put, "metadata-lag:k1", "v1", 0})
          assert :ok = WARaftBackend.write(0, {:put, "metadata-lag:k2", "v2", 0})

          assert {:ok, {:raft_log_pos, live_index, _live_term}} =
                   WARaftBackend.storage_position(0)

          assert {:raft_log_pos, persisted_index, _persisted_term} = persisted_before
          assert live_index > persisted_index
          refute_receive {:storage_metadata_fsync, _path}, 100

          assert :ok = WARaftBackend.stop()
          rewind_waraft_storage_position!(root, 0, persisted_before)
          FerricStore.Instance.cleanup(ctx.name)

          restarted_ctx = build_ctx(root)

          assert :ok =
                   WARaftBackend.start(restarted_ctx,
                     log_module: :ferricstore_waraft_spike_segment_log
                   )

          assert_eventually(fn -> Router.get(restarted_ctx, "metadata-lag:k1") end, "v1")
          assert_eventually(fn -> Router.get(restarted_ctx, "metadata-lag:k2") end, "v2")
        after
          restore_env(:waraft_storage_metadata_fsync_file_hook, previous_hook)
          restore_env(:waraft_storage_metadata_persist_every, previous_every)
        end
      end

      test "WARaft storage recovery trusts durable segment projection newer than metadata", %{
        root: root,
        ctx: ctx
      } do
        storage_root = waraft_storage_root(root, 0)
        key = "projection-newer-than-metadata:k"
        position = {:raft_log_pos, 42, 7}

        File.mkdir_p!(storage_root)

        assert :ok =
                 :ferricstore_waraft_spike_segment_log.write_projection(
                   to_charlist(Path.join(storage_root, "segment_projection_log")),
                   position,
                   [{key, "v1", 0}]
                 )

        write_waraft_storage_metadata!(root, 0, %{
          version: 1,
          position: {:raft_log_pos, 1, 1},
          label: nil,
          config: nil,
          apply_context: ctx.apply_context
        })

        context_key = {{WARaftBackend, :context}, :ferricstore_waraft_backend}
        :persistent_term.put(context_key, ctx)

        handle =
          WARaftStorage.open(%{table: :ferricstore_waraft_backend, partition: 1}, storage_root)

        try do
          assert WARaftStorage.position(handle) == position

          assert [
                   {^key, "v1", 0, _lfu, {:waraft_projection, 1}, _offset, 2}
                 ] = :ets.lookup(elem(ctx.keydir_refs, 0), key)
        after
          WARaftStorage.close(handle)
          :persistent_term.erase(context_key)
        end
      end

      test "WARaft storage recovery fast-forwards one-node segment keydir when metadata lags", %{
        root: root,
        ctx: ctx
      } do
        key = key_for_shard(ctx, 0, "storage-fast-forward")

        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
        assert :ok = WARaftBackend.write(0, {:put, key, "v1", 0})

        assert {:ok, {:raft_log_pos, applied_index, _term} = applied_position} =
                 WARaftBackend.storage_position(0)

        assert applied_index > 1
        assert :ok = WARaftBackend.stop()

        storage_root = waraft_storage_root(root, 0)
        metadata = waraft_latest_storage_metadata(root, 0)

        assert {{:raft_log_pos, 1, 1}, %{participants: participants, witness: []}} =
                 metadata.config

        assert length(participants) == 1

        assert {:ok, folded_entries} =
                 :ferricstore_waraft_spike_segment_log.fold_disk(
                   storage_root,
                   fn index, entry, acc -> [{index, entry} | acc] end,
                   []
                 )

        assert Enum.any?(folded_entries, fn
                 {^applied_index, {_term, {:default, {_corr, {:ttb, _payload}}}}} -> true
                 {^applied_index, {_term, {:default, {_corr, {:put, ^key, "v1", 0}}}}} -> true
                 {^applied_index, {_term, {_corr, {:ttb, _payload}}}} -> true
                 {^applied_index, {_term, {_corr, {:put, ^key, "v1", 0}}}} -> true
                 _other -> false
               end)

        File.rm_rf!(Path.join(storage_root, "segment_projection_log"))

        write_waraft_storage_metadata!(
          root,
          0,
          Map.put(metadata, :position, {:raft_log_pos, 1, 1})
        )

        FerricStore.Instance.cleanup(ctx.name)
        restarted_ctx = build_ctx(root)
        context_key = {{WARaftBackend, :context}, :ferricstore_waraft_backend}
        :persistent_term.put(context_key, restarted_ctx)

        try do
          log =
            capture_log(fn ->
              handle =
                WARaftStorage.open(
                  %{table: :ferricstore_waraft_backend, partition: 1},
                  storage_root
                )

              try do
                assert [
                         {^key, "v1", 0, _lfu, {:waraft_segment, ^applied_index}, _offset, 2}
                       ] = :ets.lookup(elem(restarted_ctx.keydir_refs, 0), key)

                assert WARaftStorage.position(handle) == applied_position
              after
                WARaftStorage.close(handle)
              end
            end)

          refute log =~ "unrecognized command: :noop"
        after
          :persistent_term.erase(context_key)
          FerricStore.Instance.cleanup(restarted_ctx.name)
        end
      end

      test "WARaft storage recovery fails closed when metadata target is past replayed segment log",
           %{
             root: root,
             ctx: ctx
           } do
        storage_root = waraft_storage_root(root, 0)
        File.mkdir_p!(storage_root)

        write_waraft_storage_metadata!(root, 0, %{
          version: 1,
          position: {:raft_log_pos, 50, 7},
          label: nil,
          config: nil,
          apply_context: ctx.apply_context
        })

        FerricStore.Instance.cleanup(ctx.name)
        restarted_ctx = build_ctx(root)
        context_key = {{WARaftBackend, :context}, :ferricstore_waraft_backend}
        :persistent_term.put(context_key, restarted_ctx)

        try do
          assert_raise RuntimeError, ~r/segment_projected_keydir_recovery_incomplete/, fn ->
            WARaftStorage.open(%{table: :ferricstore_waraft_backend, partition: 1}, storage_root)
          end
        after
          :persistent_term.erase(context_key)
          FerricStore.Instance.cleanup(restarted_ctx.name)
        end
      end
    end
  end
end
