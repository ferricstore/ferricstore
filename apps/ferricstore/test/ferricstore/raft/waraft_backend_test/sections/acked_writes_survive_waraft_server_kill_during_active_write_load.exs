defmodule Ferricstore.Raft.WARaftBackendTest.Sections.AckedWritesSurviveWaraftServerKillDuringActiveWriteLoad do
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

      test "acked writes survive WARaft server kill during active write load", %{
        root: root,
        ctx: ctx
      } do
        assert :ok =
                 WARaftBackend.start(ctx,
                   log_module: :ferricstore_waraft_spike_segment_log,
                   commit_batch_interval_ms: 5,
                   commit_batch_max: 32
                 )

        parent = self()

        writer =
          Task.async(fn ->
            for i <- 1..80 do
              key = "kill-load:#{i}"
              value = "v#{i}"
              result = WARaftBackend.write(0, {:put, key, value, 0})
              send(parent, {:waraft_kill_load_result, key, value, result})
            end
          end)

        acked_before_kill = wait_for_kill_load_acks(5, [])
        kill_waraft_server!(0)

        _ = Task.yield(writer, 5_000) || Task.shutdown(writer, :brutal_kill)
        acked = drain_kill_load_results(acked_before_kill)

        assert length(acked) >= 5

        assert :ok = WARaftBackend.stop()
        FerricStore.Instance.cleanup(ctx.name)

        restarted_ctx = build_ctx(root)

        assert :ok =
                 WARaftBackend.start(restarted_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        for {key, value} <- acked do
          assert_eventually(fn -> Router.get(restarted_ctx, key) end, value)
        end
      end

      @tag :shard_kill
      test "acked writes survive one WARaft server kill during multi-shard write load", %{
        root: root
      } do
        shard_count = 4
        victim_shard = 2
        multi_root = Path.join(root, "multi-shard-kill")
        File.mkdir_p!(multi_root)
        Ferricstore.DataDir.ensure_layout!(multi_root, shard_count)
        Ferricstore.Store.ActiveFile.init(shard_count)
        ctx = build_ctx(multi_root, shard_count: shard_count)

        assert :ok =
                 WARaftBackend.start(ctx,
                   log_module: :ferricstore_waraft_spike_segment_log,
                   commit_batch_interval_ms: 5,
                   commit_batch_max: 32
                 )

        parent = self()

        writers =
          for shard_idx <- 0..(shard_count - 1) do
            Task.async(fn ->
              for i <- 1..40 do
                key = key_for_shard(ctx, shard_idx, "multi-kill:#{shard_idx}:#{i}")
                value = "v#{shard_idx}:#{i}"
                result = WARaftBackend.write(shard_idx, {:put, key, value, 0})
                send(parent, {:waraft_multi_kill_result, shard_idx, key, value, result})
              end
            end)
          end

        acked_before_kill = wait_for_multi_kill_shard_acks(victim_shard, 3, [])
        kill_waraft_server!(victim_shard)

        Enum.each(writers, fn writer ->
          _ = Task.yield(writer, 5_000) || Task.shutdown(writer, :brutal_kill)
        end)

        acked = drain_multi_kill_results(acked_before_kill)
        assert Enum.any?(acked, fn {shard_idx, _key, _value} -> shard_idx == victim_shard end)
        assert Enum.any?(acked, fn {shard_idx, _key, _value} -> shard_idx != victim_shard end)

        assert :ok = WARaftBackend.stop()
        FerricStore.Instance.cleanup(ctx.name)

        Ferricstore.Store.ActiveFile.init(shard_count)
        restarted_ctx = build_ctx(multi_root, shard_count: shard_count)

        assert :ok =
                 WARaftBackend.start(restarted_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        for {shard_idx, key, value} <- acked do
          assert_eventually(fn -> Router.get(restarted_ctx, key) end, value)
          assert Router.shard_for(restarted_ctx, key) == shard_idx
        end

        FerricStore.Instance.cleanup(restarted_ctx.name)
      end

      test "restart replay does not double-apply non-idempotent RMW after storage-position lag",
           %{
             root: root,
             ctx: ctx
           } do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
        assert {:ok, pre_write_position} = WARaftBackend.storage_position(0)

        assert {:ok, 1} = WARaftBackend.write(0, {:incr, "rmw:lag", 1})
        assert "1" == Router.get(ctx, "rmw:lag")

        assert :ok = WARaftBackend.stop()
        rewind_waraft_storage_position!(root, 0, pre_write_position)
        FerricStore.Instance.cleanup(ctx.name)

        restarted_ctx = build_ctx(root)

        assert :ok =
                 WARaftBackend.start(restarted_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert "1" == Router.get(restarted_ctx, "rmw:lag")
      end

      test "failed Bitcask apply does not advance WARaft storage replay position", %{
        root: root,
        ctx: ctx
      } do
        previous_mode = Application.get_env(:ferricstore, :waraft_storage_apply_mode)

        try do
          Application.put_env(:ferricstore, :waraft_storage_apply_mode, :bitcask_keydir)
          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
          assert {:ok, pre_write_position} = WARaftBackend.storage_position(0)

          payload = "apply-fail:v"
          {encoded_ref, ref} = missing_blob_ref(payload)

          assert Ferricstore.ErrorReasons.write_timeout_unknown() ==
                   WARaftBackend.write(0, {:put_blob_ref, "apply-fail:k", encoded_ref, 0})

          assert nil == Router.get(ctx, "apply-fail:k")
          assert {:ok, ^pre_write_position} = WARaftBackend.storage_position(0)

          assert :ok = WARaftBackend.stop()
          write_blob_segment!(ctx, 0, ref, payload)
          FerricStore.Instance.cleanup(ctx.name)

          restarted_ctx = build_ctx(root)

          assert :ok =
                   WARaftBackend.start(restarted_ctx,
                     log_module: :ferricstore_waraft_spike_segment_log
                   )

          assert_eventually(fn -> Router.get(restarted_ctx, "apply-fail:k") end, payload)
        after
          restore_env(:waraft_storage_apply_mode, previous_mode)
        end
      end

      test "segment log append failure returns unknown outcome without applying before restart",
           %{
             root: root,
             ctx: ctx
           } do
        previous_hook = Application.get_env(:ferricstore, :waraft_segment_log_append_hook)

        try do
          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          Application.put_env(
            :ferricstore,
            :waraft_segment_log_append_hook,
            {:fail_once_after_write, self()}
          )

          assert Ferricstore.ErrorReasons.write_timeout_unknown() ==
                   WARaftBackend.write(0, {:put, "log-append-fail:k", "v1", 0})

          assert_receive {:waraft_segment_log_append_hook, :after_write}, 1_000
          assert nil == Router.get(ctx, "log-append-fail:k")

          assert :ok = WARaftBackend.stop()
          FerricStore.Instance.cleanup(ctx.name)

          restarted_ctx = build_ctx(root)

          assert :ok =
                   WARaftBackend.start(restarted_ctx,
                     log_module: :ferricstore_waraft_spike_segment_log
                   )

          assert nil == Router.get(restarted_ctx, "log-append-fail:k")
        after
          restore_env(:waraft_segment_log_append_hook, previous_hook)
        end
      end

      test "segment-projected Flow write does not append a second apply-projection record on ack path",
           %{ctx: ctx} do
        previous_hook = Application.get_env(:ferricstore, :waraft_segment_log_append_hook)

        flow_type = "apply-projection-async-#{System.unique_integer([:positive])}"
        flow_id = "apply-projection-async-id-#{System.unique_integer([:positive])}"
        partition = "apply-projection-async-partition"

        try do
          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          Application.put_env(
            :ferricstore,
            :waraft_segment_log_append_hook,
            {:fail_after_write_count, 2, self()}
          )

          assert :ok =
                   Ferricstore.Flow.create(ctx, flow_id,
                     type: flow_type,
                     partition_key: partition,
                     run_at_ms: 1_000,
                     now_ms: 900
                   )

          assert_receive {:waraft_segment_log_append_hook, :after_write, 1}, 1_000
          refute_receive {:waraft_segment_log_append_hook, :after_write, 2}, 250
          Application.delete_env(:ferricstore, :waraft_segment_log_append_hook)

          assert Ferricstore.Raft.WARaftSegmentReader.apply_projection_cache_count(
                   ctx.data_dir,
                   0
                 ) ==
                   0

          assert {:ok, [%{id: ^flow_id}]} =
                   Ferricstore.Flow.claim_due(ctx, flow_type,
                     partition_key: partition,
                     worker: "worker-apply-projection-async",
                     limit: 1,
                     now_ms: 1_000
                   )
        after
          restore_env(:waraft_segment_log_append_hook, previous_hook)
        end
      end

      test "segment log file fsync failure returns unknown outcome without applying before restart",
           %{
             root: root,
             ctx: ctx
           } do
        previous_hook = Application.get_env(:ferricstore, :waraft_segment_log_file_sync_hook)

        try do
          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          Application.put_env(
            :ferricstore,
            :waraft_segment_log_file_sync_hook,
            {:fail_once, self()}
          )

          assert Ferricstore.ErrorReasons.write_timeout_unknown() ==
                   WARaftBackend.write(0, {:put, "log-file-sync-fail:k", "v1", 0})

          assert_receive {:waraft_segment_log_file_sync, _path}, 1_000
          assert nil == Router.get(ctx, "log-file-sync-fail:k")

          assert :ok = WARaftBackend.stop()
          FerricStore.Instance.cleanup(ctx.name)

          restarted_ctx = build_ctx(root)

          assert :ok =
                   WARaftBackend.start(restarted_ctx,
                     log_module: :ferricstore_waraft_spike_segment_log
                   )

          assert nil == Router.get(restarted_ctx, "log-file-sync-fail:k")
        after
          restore_env(:waraft_segment_log_file_sync_hook, previous_hook)
        end
      end

      test "unknown post-submit blob write hardens GC protection beyond TTL", %{ctx: ctx} do
        previous_hook = Application.get_env(:ferricstore, :waraft_segment_log_file_sync_hook)
        payload = :binary.copy("unknown-blob-outcome", 300_000)
        key = "blob-unknown-after-submit:k"

        assert byte_size(payload) > ctx.blob_side_channel_threshold_bytes

        Process.put(:ferricstore_blob_store_segment_gc_grace_ms, 0)
        Process.put(:ferricstore_blob_store_protection_ttl_ms, 0)

        try do
          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          Application.put_env(
            :ferricstore,
            :waraft_segment_log_file_sync_hook,
            {:fail_once, self()}
          )

          assert ErrorReasons.write_timeout_unknown() ==
                   WARaftBackend.write(0, {:put, key, payload, 0})

          assert_receive {:waraft_segment_log_file_sync, _path}, 1_000
          assert nil == Router.get(ctx, key)

          assert [_blob_segment] = blob_regular_files(ctx.data_dir, 0)

          assert {:ok, %{deleted_files: 0, deleted_bytes: 0, kept_files: 1}} =
                   BlobStore.sweep_unreferenced(ctx.data_dir, 0, [])

          assert [_blob_segment] = blob_regular_files(ctx.data_dir, 0)
        after
          Process.delete(:ferricstore_blob_store_segment_gc_grace_ms)
          Process.delete(:ferricstore_blob_store_protection_ttl_ms)
          restore_env(:waraft_segment_log_file_sync_hook, previous_hook)
        end
      end

      test "segment log restart truncates torn oversized tail header", %{root: root, ctx: ctx} do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
        assert :ok = WARaftBackend.write(0, {:put, "log-torn-tail:k", "v1", 0})
        assert "v1" == Router.get(ctx, "log-torn-tail:k")
        assert :ok = WARaftBackend.stop()

        segment_path = Path.join(waraft_segment_log_dir(root, 0), "0.seg")
        size_before_tail = File.stat!(segment_path).size

        File.write!(segment_path, <<2_147_483_648::32, 0::32>>, [:append, :binary])
        assert File.stat!(segment_path).size == size_before_tail + 8

        FerricStore.Instance.cleanup(ctx.name)
        restarted_ctx = build_ctx(root)

        assert :ok =
                 WARaftBackend.start(restarted_ctx,
                   log_module: :ferricstore_waraft_spike_segment_log
                 )

        assert_eventually(fn -> Router.get(restarted_ctx, "log-torn-tail:k") end, "v1")
        segment = File.read!(segment_path)
        refute binary_part(segment, size_before_tail, 8) == <<2_147_483_648::32, 0::32>>
        <<valid_len::32, _valid_crc::32>> = binary_part(segment, size_before_tail, 8)
        assert valid_len < 1_073_741_824
        assert byte_size(segment) >= size_before_tail + 8 + valid_len
      end

      test "segment log recovery streams records instead of reading whole segment files" do
        source =
          Ferricstore.Test.SourceFiles.waraft_segment_log_source()

        assert [_, recovery_source] =
                 String.split(
                   source,
                   "load_segment(Ordinal, Path, Name, PreviousIndex, RecordsPerSegment) ->",
                   parts: 2
                 )

        assert [recovery_source, _] =
                 String.split(recovery_source, "insert_recovered_record(", parts: 2)

        refute recovery_source =~ "file:read_file(",
               "segment recovery must not materialize a full segment file in BEAM memory"

        assert recovery_source =~ "file:open(Path, [read, raw, binary])"
        assert recovery_source =~ "file:read(Fd, ?RECORD_HEADER_SIZE)"
      end

      test "segment log trim and truncate stream kept records into rewrite staging" do
        source =
          Ferricstore.Test.SourceFiles.waraft_segment_log_source()

        refute source =~ "kept_records_from(Name, Index)",
               "truncate rewrite must not materialize every kept record before staging"

        refute source =~ "kept_records_at_or_after(Name, Index)",
               "trim rewrite must not materialize every kept record before staging"
      end

      test "segment log config lookup uses cached latest config on append hot path" do
        source =
          Ferricstore.Test.SourceFiles.waraft_segment_log_source()

        assert [_, config_source] =
                 String.split(source, "\nconfig(Log) ->", parts: 2)

        assert [config_source, _] =
                 String.split(config_source, "\nconfig_from_index", parts: 2)

        refute config_source =~ "ets:foldl",
               "config lookup must not scan every log entry when the latest config is near the tail"

        assert config_source =~ "cached_config(Log)"
        assert String.split(config_source, "cached_config(Log)", parts: 2) |> length() == 2

        assert String.split(config_source, "config_from_index(Log, Last, First)", parts: 2)
               |> length() == 2

        assert :binary.match(config_source, "cached_config(Log)") <
                 :binary.match(config_source, "config_from_index(Log, Last, First)"),
               "append refresh_config must check the cache before walking disk-backed log records"

        assert source =~ "update_latest_config_from_records(Dir, Records)"

        assert [_, cached_source] =
                 String.split(source, "\ncached_config(Log) ->", parts: 2)

        assert [cached_source, _] =
                 String.split(cached_source, "\nupdate_latest_config_from_records", parts: 2)

        refute cached_source =~ "Index >= First",
               "trimmed configs are still the latest known config; rejecting them forces append to rescan disk"

        assert source =~ "cache_latest_config_not_found(Dir, Last)",
               "snapshot-backed bootstraps may have no config entry in the log; cache that miss instead of rescanning on every append"

        assert config_source =~ "none_cached",
               "cached config misses must return without falling through to config_from_index/3"
      end

      test "apply projection spill coalesces segment writes" do
        reader_source =
          Path.expand("../../../lib/ferricstore/raft/waraft_segment_reader.ex", __DIR__)
          |> File.read!()

        segment_source =
          Ferricstore.Test.SourceFiles.waraft_segment_log_source()

        assert reader_source =~ "write_projection_batches",
               "spilling apply-projection cache should write one segment batch, not one file append per Ra index"

        refute reader_source =~ "write_apply_projection_spill(projection_root, index, batch)",
               "per-index projection writes reopen segment files and show up as write/open/close in the hot profile"

        assert segment_source =~ "write_projection_batches/2"
      end

      test "WARaft storage opens apply-projection segment config before spill hot path", %{
        root: root,
        ctx: ctx
      } do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

        apply_projection_config =
          root
          |> waraft_segment_log_dir(0)
          |> Path.dirname()
          |> Path.join("apply_projection_log/segment_log/segment_config.term")

        assert File.exists?(apply_projection_config),
               "apply-projection spill should not create segment config from the apply hot path"
      end

      test "segment config cache key avoids absolute-path filesystem normalization on hot path" do
        source =
          Ferricstore.Test.SourceFiles.waraft_segment_log_source()

        assert [_, cache_key_source] =
                 String.split(source, "\nsegment_config_cache_key(Dir) ->", parts: 2)

        assert [cache_key_source, _] =
                 String.split(cache_key_source, "\nlatest_config_cache_key", parts: 2)

        refute cache_key_source =~ "filename:absname",
               "records_per_segment/1 runs while appending projection spills; absolute dirs must not call filename:absname/1 through the file server"
      end

      test "segment log append grouping stays linear for monotonic Raft batches" do
        source =
          Ferricstore.Test.SourceFiles.waraft_segment_log_source()

        assert [_, grouping_source] =
                 String.split(source, "\ngroup_records(Records, RecordsPerSegment) ->", parts: 2)

        assert [grouping_source, _] =
                 String.split(grouping_source, "write_record_group_list", parts: 2)

        refute grouping_source =~ "maps:update",
               "append grouping should not allocate a map for already-monotonic Raft entries"

        refute grouping_source =~ "maps:to_list",
               "append grouping should not sort a map on the append hot path"
      end

      test "segment log records per segment is configurable", %{root: root, ctx: ctx} do
        previous = Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)

        try do
          Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, 2)

          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
          assert :ok = WARaftBackend.write(0, {:put, "log-config-segment:k", "v1", 0})

          segment_files =
            root
            |> waraft_segment_log_dir(0)
            |> File.ls!()
            |> Enum.filter(&String.ends_with?(&1, ".seg"))
            |> Enum.sort()

          assert "0.seg" in segment_files
          assert "1.seg" in segment_files
        after
          WARaftBackend.stop()
          restore_env(:waraft_segment_log_records_per_segment, previous)
        end
      end

      test "segment log keeps its original segment sizing across config changes", %{ctx: ctx} do
        previous = Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)

        try do
          Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, 2)

          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
          assert :ok = WARaftBackend.write(0, {:put, "log-config-stable:first", "v1", 0})

          assert File.exists?(
                   Path.join(waraft_segment_log_dir(ctx.data_dir, 0), "segment_config.term")
                 )

          WARaftBackend.stop()

          Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, 4096)

          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
          assert :ok = WARaftBackend.write(0, {:put, "log-config-stable:second", "v2", 0})
          WARaftBackend.stop()

          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
          assert "v1" == Router.get(ctx, "log-config-stable:first")
          assert "v2" == Router.get(ctx, "log-config-stable:second")
        after
          WARaftBackend.stop()
          restore_env(:waraft_segment_log_records_per_segment, previous)
        end
      end

      test "segment log rewrite preserves its original segment sizing after config changes", %{
        root: root,
        ctx: ctx
      } do
        previous = Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)

        try do
          Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, 2)

          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
          assert :ok = WARaftBackend.write(0, {:put, "log-config-rewrite:first", "v1", 0})
          assert :ok = WARaftBackend.write(0, {:put, "log-config-rewrite:second", "v2", 0})

          segment_dir = waraft_segment_log_dir(root, 0)
          assert %{records_per_segment: 2} = read_segment_config(segment_dir)

          Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, 4096)

          log = waraft_segment_log_record(0)
          assert {:ok, _state} = :ferricstore_waraft_spike_segment_log.trim(log, 2, %{})

          assert %{records_per_segment: 2} = read_segment_config(segment_dir)
        after
          WARaftBackend.stop()
          restore_env(:waraft_segment_log_records_per_segment, previous)
        end
      end

      test "segment log close clears its segment sizing cache", %{root: root, ctx: ctx} do
        previous = Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)

        try do
          Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, 2)

          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
          assert :ok = WARaftBackend.write(0, {:put, "log-config-cache:k", "v", 0})

          segment_dir = waraft_segment_log_dir(root, 0)

          cache_key =
            {:ferricstore_waraft_spike_segment_log, :records_per_segment,
             segment_dir |> Path.absname() |> String.to_charlist()}

          assert :persistent_term.get(cache_key, :missing) == 2

          assert :ok = WARaftBackend.stop()
          assert :persistent_term.get(cache_key, :missing) == :missing
        after
          WARaftBackend.stop()
          restore_env(:waraft_segment_log_records_per_segment, previous)
        end
      end

      test "segment log open caches config miss from loaded records", %{root: root, ctx: ctx} do
        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

        for idx <- 1..10 do
          assert :ok = WARaftBackend.write(0, {:put, "log-config-miss:#{idx}", "v#{idx}", 0})
        end

        segment_dir = waraft_segment_log_dir(root, 0)

        cache_key =
          {:ferricstore_waraft_spike_segment_log, :latest_config,
           segment_dir |> Path.absname() |> String.to_charlist()}

        assert {:not_found, live_last_index} = :persistent_term.get(cache_key, :missing)
        assert is_integer(live_last_index) and live_last_index >= 10

        assert :ok = WARaftBackend.stop()
        assert :persistent_term.get(cache_key, :missing) == :missing

        assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

        log = waraft_segment_log_record(0)
        last_index = :ferricstore_waraft_spike_segment_log.last_index(log)
        assert is_integer(last_index) and last_index >= 10
        assert :persistent_term.get(cache_key, :missing) == {:not_found, last_index}
      after
        WARaftBackend.stop()
      end

      test "segment log fails closed when persisted segment sizing metadata is corrupt", %{
        root: root,
        ctx: ctx
      } do
        previous = Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)

        try do
          Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, 2)

          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
          assert :ok = WARaftBackend.write(0, {:put, "log-config-corrupt:k", "v", 0})
          WARaftBackend.stop()

          root
          |> waraft_segment_log_dir(0)
          |> Path.join("segment_config.term")
          |> File.write!("not-an-erlang-term")

          assert {:error, _reason} =
                   WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
        after
          WARaftBackend.stop()
          restore_env(:waraft_segment_log_records_per_segment, previous)
        end
      end

      test "segment log fails closed when persisted segment sizing metadata is oversized", %{
        root: root,
        ctx: ctx
      } do
        previous = Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)

        try do
          Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, 2)

          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
          assert :ok = WARaftBackend.write(0, {:put, "log-config-oversized:k", "v", 0})
          WARaftBackend.stop()

          root
          |> waraft_segment_log_dir(0)
          |> Path.join("segment_config.term")
          |> File.write!(
            :erlang.term_to_binary(%{
              version: 1,
              records_per_segment: 2,
              label: :binary.copy("x", 1_048_576)
            })
          )

          assert {:error, reason} =
                   WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          assert inspect(reason) =~ "segment_config_file_too_large"
        after
          WARaftBackend.stop()
          restore_env(:waraft_segment_log_records_per_segment, previous)
        end
      end

      test "segment log rejects persisted segment sizing metadata symlink", %{
        root: root,
        ctx: ctx
      } do
        previous = Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)

        try do
          Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, 2)

          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
          assert :ok = WARaftBackend.write(0, {:put, "log-config-symlink:k", "v", 0})
          WARaftBackend.stop()

          segment_dir = waraft_segment_log_dir(root, 0)
          segment_config = Path.join(segment_dir, "segment_config.term")
          outside_config = Path.join(root, "outside-segment-config.term")

          File.write!(
            outside_config,
            :erlang.term_to_binary(%{version: 1, records_per_segment: 2})
          )

          File.rm!(segment_config)
          assert :ok = File.ln_s(outside_config, segment_config)

          assert {:error, reason} =
                   WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          assert inspect(reason) =~ "unsafe_segment_metadata_path"
          assert {:ok, %{type: :symlink}} = File.lstat(segment_config)
        after
          WARaftBackend.stop()
          restore_env(:waraft_segment_log_records_per_segment, previous)
        end
      end

      test "segment log rejects symlinked segment files during restart", %{
        root: root,
        ctx: ctx
      } do
        previous = Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)

        try do
          Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, 16)

          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
          assert :ok = WARaftBackend.write(0, {:put, "log-segment-symlink:k", "v", 0})
          WARaftBackend.stop()

          segment_dir = waraft_segment_log_dir(root, 0)
          [segment_path | _] = Path.wildcard(Path.join(segment_dir, "*.seg"))
          outside_segment = Path.join(root, "outside-segment.seg")

          File.cp!(segment_path, outside_segment)
          File.rm!(segment_path)
          assert :ok = File.ln_s(outside_segment, segment_path)

          assert {:error, reason} =
                   WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          assert inspect(reason) =~ "unsafe_segment_path"
          assert {:ok, %{type: :symlink}} = File.lstat(segment_path)
        after
          WARaftBackend.stop()
          restore_env(:waraft_segment_log_records_per_segment, previous)
        end
      end

      test "segment log rejects symlinked segment log directory during restart", %{
        root: root,
        ctx: ctx
      } do
        previous = Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)

        try do
          Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, 16)

          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)
          assert :ok = WARaftBackend.write(0, {:put, "log-dir-symlink:k", "v", 0})
          WARaftBackend.stop()

          segment_dir = waraft_segment_log_dir(root, 0)
          outside_dir = Path.join(root, "outside-segment-log")

          File.cp_r!(segment_dir, outside_dir)
          File.rm_rf!(segment_dir)
          assert :ok = File.ln_s(outside_dir, segment_dir)

          assert {:error, reason} =
                   WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          assert inspect(reason) =~ "unsafe_segment_log_dir"
          assert {:ok, %{type: :symlink}} = File.lstat(segment_dir)
        after
          WARaftBackend.stop()
          restore_env(:waraft_segment_log_records_per_segment, previous)
        end
      end

      test "segment log rejects appending through symlinked segment files", %{
        root: root,
        ctx: ctx
      } do
        previous = Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)

        try do
          Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, 16)

          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          segment_dir = waraft_segment_log_dir(root, 0)
          [segment_path | _] = Path.wildcard(Path.join(segment_dir, "*.seg"))
          outside_segment = Path.join(root, "outside-live-append-segment.seg")

          File.cp!(segment_path, outside_segment)
          outside_size = File.stat!(outside_segment).size
          File.rm!(segment_path)
          assert :ok = File.ln_s(outside_segment, segment_path)

          assert {:error, reason} =
                   WARaftBackend.write(0, {:put, "log-segment-live-symlink:k", "v", 0})

          assert inspect(reason) =~ "unsafe_segment_path" or
                   reason in [:unknown_outcome, :timeout, {:timeout, :unknown_outcome}]

          assert File.stat!(outside_segment).size == outside_size
          assert nil == Router.get(ctx, "log-segment-live-symlink:k")
        after
          WARaftBackend.stop()
          restore_env(:waraft_segment_log_records_per_segment, previous)
        end
      end

      test "segment log rejects appending through a symlinked segment log directory", %{
        root: root,
        ctx: ctx
      } do
        previous = Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)

        try do
          Application.put_env(:ferricstore, :waraft_segment_log_records_per_segment, 16)

          assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

          segment_dir = waraft_segment_log_dir(root, 0)
          outside_dir = Path.join(root, "outside-live-append-segment-log")
          outside_segment = Path.join(outside_dir, "0.seg")

          File.rename!(segment_dir, outside_dir)
          assert :ok = File.ln_s(outside_dir, segment_dir)
          outside_size = File.stat!(outside_segment).size

          assert {:error, reason} =
                   WARaftBackend.write(0, {:put, "log-dir-live-symlink:k", "v", 0})

          assert inspect(reason) =~ "unsafe_segment_log_dir" or
                   reason in [:unknown_outcome, :timeout, {:timeout, :unknown_outcome}]

          assert File.stat!(outside_segment).size == outside_size
          assert nil == Router.get(ctx, "log-dir-live-symlink:k")
        after
          WARaftBackend.stop()
          restore_env(:waraft_segment_log_records_per_segment, previous)
        end
      end
    end
  end
end
