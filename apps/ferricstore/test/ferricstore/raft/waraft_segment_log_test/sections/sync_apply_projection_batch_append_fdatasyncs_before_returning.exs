defmodule Ferricstore.Raft.WARaftSegmentLogTest.Sections.SyncApplyProjectionBatchAppendFdatasyncsBeforeReturning do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
  test "sync apply projection batch append fdatasyncs before returning" do
    previous_hook = Application.get_env(:ferricstore, :waraft_segment_log_file_sync_hook)
    root = Path.join(System.tmp_dir!(), "ferricstore-waraft-apply-projection-sync")
    File.rm_rf!(root)

    try do
      Application.put_env(
        :ferricstore,
        :waraft_segment_log_file_sync_hook,
        {:notify_with_method, self()}
      )

      assert :ok =
               :ferricstore_waraft_spike_segment_log.write_projection_batches_sync(
                 to_charlist(root),
                 [
                   {{:raft_log_pos, 42, 7}, [{"a", "1", 0}]},
                   {{:raft_log_pos, 43, 7}, [{"b", "2", 0}]}
                 ]
               )

      assert_receive {:waraft_segment_log_file_sync, synced_path, :datasync}, 1_000
      assert synced_path |> to_string() |> String.contains?("apply-projection-sync")

      assert {:ok, {_ordinal, offset, encoded_size}} =
               :ferricstore_waraft_spike_segment_log.location_for_index(to_charlist(root), 42)

      assert {:ok, {0, {:ferricstore_segment_apply_projection_batch, _, [{"a", "1", 0}]}}} =
               :ferricstore_waraft_spike_segment_log.read_disk_at(
                 to_charlist(root),
                 42,
                 offset,
                 encoded_size
               )
    after
      restore_env(:ferricstore, :waraft_segment_log_file_sync_hook, previous_hook)
      File.rm_rf!(root)
    end
  end

  test "apply projection cache spill fdatasyncs before deleting cache rows" do
    previous_hook = Application.get_env(:ferricstore, :waraft_segment_log_file_sync_hook)

    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-waraft-apply-projection-spill-sync-#{System.unique_integer([:positive])}"
      )

    File.rm_rf!(data_dir)

    try do
      Application.put_env(
        :ferricstore,
        :waraft_segment_log_file_sync_hook,
        {:notify_with_method, self()}
      )

      Ferricstore.Raft.WARaftSegmentReader.put_apply_projection(data_dir, 0, 42, [
        {"a", "1", 0}
      ])

      assert Ferricstore.Raft.WARaftSegmentReader.apply_projection_cache_count(data_dir, 0) == 1

      assert {:ok, 1} =
               Ferricstore.Raft.WARaftSegmentReader.spill_apply_projection_cache(data_dir, 0)

      assert_receive {:waraft_segment_log_file_sync, synced_path, :datasync}, 1_000
      assert synced_path |> to_string() |> String.contains?("apply_projection_log")
      assert Ferricstore.Raft.WARaftSegmentReader.apply_projection_cache_count(data_dir, 0) == 0

      assert Ferricstore.Raft.WARaftSegmentReader.apply_projection_dependency_ready?(
               data_dir,
               0,
               42
             )
    after
      restore_env(:ferricstore, :waraft_segment_log_file_sync_hook, previous_hook)
      File.rm_rf!(data_dir)
    end
  end

  test "apply projection cache spill preserves rows when fdatasync fails" do
    previous_hook = Application.get_env(:ferricstore, :waraft_segment_log_file_sync_hook)

    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-waraft-apply-projection-spill-sync-fail-#{System.unique_integer([:positive])}"
      )

    File.rm_rf!(data_dir)

    try do
      Application.put_env(:ferricstore, :waraft_segment_log_file_sync_hook, {:fail_once, self()})

      Ferricstore.Raft.WARaftSegmentReader.put_apply_projection(data_dir, 0, 42, [
        {"a", "1", 0}
      ])

      assert Ferricstore.Raft.WARaftSegmentReader.apply_projection_cache_count(data_dir, 0) == 1

      assert {:error, {:write_apply_projection_spill_failed, _reason}} =
               Ferricstore.Raft.WARaftSegmentReader.spill_apply_projection_cache(data_dir, 0)

      assert_receive {:waraft_segment_log_file_sync, synced_path}, 1_000
      assert synced_path |> to_string() |> String.contains?("apply_projection_log")
      assert Ferricstore.Raft.WARaftSegmentReader.apply_projection_cache_count(data_dir, 0) == 1

      refute Ferricstore.Raft.WARaftSegmentReader.apply_projection_dependency_ready?(
               data_dir,
               0,
               42
             )
    after
      restore_env(:ferricstore, :waraft_segment_log_file_sync_hook, previous_hook)
      File.rm_rf!(data_dir)
    end
  end

  test "apply projection batch recovery accepts sparse raft indexes" do
    root = Path.join(System.tmp_dir!(), "ferricstore-waraft-apply-projection-sparse")
    projection_root = Path.join(root, "apply_projection_log")
    File.rm_rf!(root)

    try do
      assert :ok =
               :ferricstore_waraft_spike_segment_log.write_projection_batches(
                 to_charlist(projection_root),
                 [
                   {{:raft_log_pos, 30, 7}, [{"a", "1", 0}]},
                   {{:raft_log_pos, 32, 7}, [{"b", "2", 0}]}
                 ]
               )

      assert {:ok, records} =
               :ferricstore_waraft_spike_segment_log.fold_disk(
                 to_charlist(projection_root),
                 fn index, entry, acc -> [{index, entry} | acc] end,
                 []
               )

      assert Enum.reverse(records) == [
               {30,
                {0,
                 {:ferricstore_segment_apply_projection_batch, {:raft_log_pos, 30, 7},
                  [{"a", "1", 0}]}}},
               {32,
                {0,
                 {:ferricstore_segment_apply_projection_batch, {:raft_log_pos, 32, 7},
                  [{"b", "2", 0}]}}}
             ]
    after
      File.rm_rf!(root)
    end
  end

  test "apply projection batch write merges repeated raft index" do
    root = Path.join(System.tmp_dir!(), "ferricstore-waraft-apply-projection-merge")
    projection_root = Path.join(root, "apply_projection_log")
    File.rm_rf!(root)

    try do
      assert :ok =
               :ferricstore_waraft_spike_segment_log.write_projection_batches(
                 to_charlist(projection_root),
                 [{{:raft_log_pos, 42, 7}, [{"a", "old", 0}, {"b", "2", 0}]}]
               )

      assert :ok =
               :ferricstore_waraft_spike_segment_log.write_projection_batches(
                 to_charlist(projection_root),
                 [{{:raft_log_pos, 42, 7}, [{"a", "new", 0}, {"c", "3", 0}]}]
               )

      assert {:ok,
              {0,
               {:ferricstore_segment_apply_projection_batch, {:raft_log_pos, 42, 7},
                [{"a", "new", 0}, {"b", "2", 0}, {"c", "3", 0}]}}} =
               :ferricstore_waraft_spike_segment_log.read_disk(to_charlist(projection_root), 42)
    after
      File.rm_rf!(root)
    end
  end

  test "apply projection batch duplicate replay appends and reads merged view" do
    previous_hook = Application.get_env(:ferricstore, :waraft_segment_log_rewrite_hook)

    root =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-waraft-apply-projection-idempotent-#{System.unique_integer([:positive])}"
      )

    projection_root = Path.join(root, "apply_projection_log")
    File.rm_rf!(root)

    try do
      assert :ok =
               :ferricstore_waraft_spike_segment_log.write_projection_batches(
                 to_charlist(projection_root),
                 [{{:raft_log_pos, 42, 7}, [{"a", "1", 0}, {"b", "2", 0}]}]
               )

      Application.put_env(
        :ferricstore,
        :waraft_segment_log_rewrite_hook,
        {:fail_once_after_live_backup, self()}
      )

      assert :ok =
               :ferricstore_waraft_spike_segment_log.write_projection_batches_sync(
                 to_charlist(projection_root),
                 [{{:raft_log_pos, 42, 7}, [{"a", "1", 0}]}]
               )

      refute_receive {:waraft_segment_log_rewrite_hook, :after_live_backup}, 100

      assert {:ok, records} =
               :ferricstore_waraft_spike_segment_log.fold_disk(
                 to_charlist(projection_root),
                 fn index, entry, acc -> [{index, entry} | acc] end,
                 []
               )

      assert Enum.reverse(records) == [
               {42,
                {0,
                 {:ferricstore_segment_apply_projection_batch, {:raft_log_pos, 42, 7},
                  [{"a", "1", 0}, {"b", "2", 0}]}}},
               {42,
                {0,
                 {:ferricstore_segment_apply_projection_batch, {:raft_log_pos, 42, 7},
                  [{"a", "1", 0}]}}}
             ]

      assert {:ok,
              {0,
               {:ferricstore_segment_apply_projection_batch, {:raft_log_pos, 42, 7},
                [{"a", "1", 0}, {"b", "2", 0}]}}} =
               :ferricstore_waraft_spike_segment_log.read_disk(to_charlist(projection_root), 42)
    after
      restore_env(:ferricstore, :waraft_segment_log_rewrite_hook, previous_hook)
      File.rm_rf!(root)
    end
  end

  test "apply projection append skips overlap checks for new indexes" do
    parent = self()
    handler_id = {__MODULE__, :projection_overlap_miss, make_ref()}

    :telemetry.attach(
      handler_id,
      [:ferricstore, :waraft, :segment_log, :projection_overlap],
      &__MODULE__.handle_projection_overlap_telemetry/4,
      parent
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    root = Path.join(System.tmp_dir!(), "ferricstore-waraft-apply-projection-overlap-miss")
    projection_root = Path.join(root, "apply_projection_log")
    File.rm_rf!(root)

    try do
      assert :ok =
               :ferricstore_waraft_spike_segment_log.write_projection_batches(
                 to_charlist(projection_root),
                 [{{:raft_log_pos, 42, 7}, [{"a", "1", 0}]}]
               )

      refute_receive {:projection_overlap,
                      [:ferricstore, :waraft, :segment_log, :projection_overlap], _measurements,
                      _metadata},
                     100

      assert :ok =
               :ferricstore_waraft_spike_segment_log.write_projection_batches(
                 to_charlist(projection_root),
                 [{{:raft_log_pos, 43, 7}, [{"b", "2", 0}]}]
               )

      refute_receive {:projection_overlap,
                      [:ferricstore, :waraft, :segment_log, :projection_overlap], _measurements,
                      _metadata},
                     100
    after
      File.rm_rf!(root)
    end
  end

  test "apply projection writes track tail index for append-only overlap fast path" do
    root =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-waraft-apply-projection-tail-#{System.unique_integer([:positive])}"
      )

    projection_root = Path.join(root, "apply_projection_log")
    segment_dir = Path.join(projection_root, "segment_log")
    dir_key = :unicode.characters_to_binary(Path.expand(segment_dir))
    registry = :ferricstore_waraft_segment_offset_registry
    File.rm_rf!(root)

    try do
      if :ets.info(registry) != :undefined do
        :ets.delete_all_objects(registry)
      end

      assert :ok =
               :ferricstore_waraft_spike_segment_log.write_projection_batches(
                 to_charlist(projection_root),
                 [{{:raft_log_pos, 42, 7}, [{"a", "1", 0}]}]
               )

      assert [{{^dir_key, :last_index}, :last_index, 42, 0}] =
               :ets.lookup(registry, {dir_key, :last_index})

      assert :ok =
               :ferricstore_waraft_spike_segment_log.write_projection_batches(
                 to_charlist(projection_root),
                 [{{:raft_log_pos, 43, 7}, [{"b", "2", 0}]}]
               )

      assert [{{^dir_key, :last_index}, :last_index, 43, 0}] =
               :ets.lookup(registry, {dir_key, :last_index})
    after
      File.rm_rf!(root)
    end
  end

  test "apply projection append survives registry loss without overlap scan" do
    parent = self()
    handler_id = {__MODULE__, :projection_overlap_rebuild, make_ref()}

    :telemetry.attach(
      handler_id,
      [:ferricstore, :waraft, :segment_log, :projection_overlap],
      &__MODULE__.handle_projection_overlap_telemetry/4,
      parent
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    root = Path.join(System.tmp_dir!(), "ferricstore-waraft-apply-projection-overlap-rebuild")
    projection_root = Path.join(root, "apply_projection_log")
    File.rm_rf!(root)

    try do
      assert :ok =
               :ferricstore_waraft_spike_segment_log.write_projection_batches(
                 to_charlist(projection_root),
                 [{{:raft_log_pos, 42, 7}, [{"a", "old", 0}]}]
               )

      refute_receive {:projection_overlap, _, _measurements, _metadata}, 100

      if :ets.info(:ferricstore_waraft_segment_offset_registry) != :undefined do
        :ets.delete_all_objects(:ferricstore_waraft_segment_offset_registry)
      end

      assert :ok =
               :ferricstore_waraft_spike_segment_log.write_projection_batches(
                 to_charlist(projection_root),
                 [{{:raft_log_pos, 42, 7}, [{"a", "new", 0}, {"b", "2", 0}]}]
               )

      refute_receive {:projection_overlap,
                      [:ferricstore, :waraft, :segment_log, :projection_overlap], _measurements,
                      _metadata},
                     100

      if :ets.info(:ferricstore_waraft_segment_offset_registry) != :undefined do
        :ets.delete_all_objects(:ferricstore_waraft_segment_offset_registry)
      end

      assert {:ok,
              {0,
               {:ferricstore_segment_apply_projection_batch, {:raft_log_pos, 42, 7},
                [{"a", "new", 0}, {"b", "2", 0}]}}} =
               :ferricstore_waraft_spike_segment_log.read_disk(to_charlist(projection_root), 42)
    after
      File.rm_rf!(root)
    end
  end

  test "apply projection value read uses latest duplicate batch without folding disk" do
    assert {:ok, _apps} = Application.ensure_all_started(:telemetry)

    parent = self()
    handler_id = {__MODULE__, :apply_projection_latest_read_no_fold, make_ref()}

    :telemetry.attach(
      handler_id,
      [:ferricstore, :waraft, :segment_log, :fold_disk],
      &__MODULE__.handle_fold_telemetry/4,
      parent
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-waraft-apply-projection-latest-read-#{System.unique_integer([:positive])}"
      )

    projection_root =
      Path.join([
        data_dir,
        "waraft",
        "ferricstore_waraft_backend.1",
        "apply_projection_log"
      ])

    File.rm_rf!(data_dir)

    try do
      assert :ok =
               :ferricstore_waraft_spike_segment_log.write_projection_batches(
                 to_charlist(projection_root),
                 [{{:raft_log_pos, 42, 7}, [{"a", "old", 0}, {"b", "2", 0}]}]
               )

      assert :ok =
               :ferricstore_waraft_spike_segment_log.write_projection_batches(
                 to_charlist(projection_root),
                 [{{:raft_log_pos, 42, 7}, [{"a", "new", 0}, {"c", "3", 0}]}]
               )

      assert {:ok, "new"} =
               Ferricstore.Raft.WARaftSegmentReader.read_value_from_location(
                 %{data_dir: data_dir},
                 0,
                 {:waraft_apply_projection, 42},
                 "a"
               )

      assert {:ok, "3"} =
               Ferricstore.Raft.WARaftSegmentReader.read_value_from_location(
                 %{data_dir: data_dir},
                 0,
                 {:waraft_apply_projection, 42},
                 "c"
               )

      refute_receive {:segment_log_fold, [:ferricstore, :waraft, :segment_log, :fold_disk],
                      _measurements, _metadata},
                     100
    after
      File.rm_rf!(data_dir)
    end
  end

  test "apply projection batch read fails closed when cache hit is partial and disk read fails" do
    data_dir =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-waraft-apply-projection-partial-disk-fail-#{System.unique_integer([:positive])}"
      )

    ctx = %{data_dir: data_dir}
    projection_index = 77
    key_a = "partial-disk-fail:a"
    key_b = "partial-disk-fail:b"

    File.rm_rf!(data_dir)

    try do
      assert :ok =
               Ferricstore.Raft.WARaftSegmentReader.put_apply_projection(
                 data_dir,
                 0,
                 projection_index,
                 [{key_a, "value-a", 0}]
               )

      segment_log_path =
        Path.join([
          data_dir,
          "waraft",
          "ferricstore_waraft_backend.1",
          "apply_projection_log",
          "segment_log"
        ])

      File.mkdir_p!(Path.dirname(segment_log_path))
      File.write!(segment_log_path, "not a segment log directory")

      assert {:error, _reason} =
               Ferricstore.Raft.WARaftSegmentReader.read_values_from_location(
                 ctx,
                 0,
                 {:waraft_apply_projection, projection_index},
                 [key_a, key_b]
               )
    after
      File.rm_rf!(data_dir)
    end
  end
    end
  end
end
