Code.require_file("waraft_segment_log_test/sections/segment_log_caps_ets_tail_while_disk_backed_reads_still_see_older_entrie.exs", __DIR__)
Code.require_file("waraft_segment_log_test/sections/sync_apply_projection_batch_append_fdatasyncs_before_returning.exs", __DIR__)
Code.require_file("waraft_segment_log_test/sections/default_segment_size_does_not_roll_over_during_normal_hot_batches.exs", __DIR__)

defmodule Ferricstore.Raft.WARaftSegmentLogTest do
  use ExUnit.Case, async: false
  @moduletag :raft
  @moduletag :global_state

  def handle_corrupt_telemetry(event, measurements, metadata, parent) do
    send(parent, {:segment_log_corrupt, event, measurements, metadata})
  end

  def handle_append_telemetry(event, measurements, metadata, parent) do
    send(parent, {:segment_log_append, event, measurements, metadata})
  end

  def handle_projection_overlap_telemetry(event, measurements, metadata, parent) do
    send(parent, {:projection_overlap, event, measurements, metadata})
  end

  def handle_load_telemetry(event, measurements, metadata, parent) do
    send(parent, {:segment_log_load, event, measurements, metadata})
  end

  def handle_fold_telemetry(event, measurements, metadata, parent) do
    send(parent, {:segment_log_fold, event, measurements, metadata})
  end

  use Ferricstore.Raft.WARaftSegmentLogTest.Sections.SegmentLogCapsEtsTailWhileDiskBackedReadsStillSeeOlderEntrie

  use Ferricstore.Raft.WARaftSegmentLogTest.Sections.SyncApplyProjectionBatchAppendFdatasyncsBeforeReturning

  defp clear_segment_offset_registry do
    if :ets.info(:ferricstore_waraft_segment_offset_registry) != :undefined do
      :ets.delete_all_objects(:ferricstore_waraft_segment_offset_registry)
    end
  end

  defp with_segment_log_memory_env(opts) do
    previous_db = Application.get_env(:wa_raft, :raft_database)
    previous_records = Application.get_env(:ferricstore, :waraft_segment_log_records_per_segment)
    previous_max_bytes = Application.get_env(:ferricstore, :waraft_segment_log_max_ets_bytes)
    previous_max_entries = Application.get_env(:ferricstore, :waraft_segment_log_max_ets_entries)
    previous_min_entries = Application.get_env(:ferricstore, :waraft_segment_log_min_ets_entries)

    partition = System.unique_integer([:positive])
    table = :"ferricstore_waraft_segment_log_memory_test_#{partition}"
    log_name = :"#{table}_log_#{partition}"

    root =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-waraft-segment-log-memory-#{partition}"
      )

    try do
      Application.put_env(:wa_raft, :raft_database, to_charlist(root))

      Application.put_env(
        :ferricstore,
        :waraft_segment_log_records_per_segment,
        opts[:records_per_segment]
      )

      Application.put_env(:ferricstore, :waraft_segment_log_max_ets_bytes, opts[:max_bytes])
      Application.put_env(:ferricstore, :waraft_segment_log_max_ets_entries, opts[:max_entries])
      Application.put_env(:ferricstore, :waraft_segment_log_min_ets_entries, opts[:min_entries])

      File.rm_rf!(root)
      on_exit(fn -> File.rm_rf!(root) end)

      :wa_raft_part_sup.prepare_spec(:ferricstore_waraft_backend, %{
        table: table,
        partition: partition
      })

      log =
        {:raft_log, log_name, :ferricstore_waraft_backend, table, partition,
         :ferricstore_waraft_spike_segment_log}

      opts[:fun].(root, log, log_name)
    after
      restore_env(:wa_raft, :raft_database, previous_db)
      restore_env(:ferricstore, :waraft_segment_log_records_per_segment, previous_records)
      restore_env(:ferricstore, :waraft_segment_log_max_ets_bytes, previous_max_bytes)
      restore_env(:ferricstore, :waraft_segment_log_max_ets_entries, previous_max_entries)
      restore_env(:ferricstore, :waraft_segment_log_min_ets_entries, previous_min_entries)

      if :ets.info(log_name) != :undefined do
        :ets.delete(log_name)
      end
    end
  end

  defp restore_env(app, key, nil), do: Application.delete_env(app, key)
  defp restore_env(app, key, value), do: Application.put_env(app, key, value)

  defp unblock_pending_sync_hooks do
    receive do
      {:waraft_segment_log_file_sync_blocked, _path, _method, waiter, ref} ->
        send(waiter, {ref, :continue})
        unblock_pending_sync_hooks()
    after
      0 -> :ok
    end
  end

  defp writer_entries_for_owner(registry, owner) do
    registry
    |> :ets.tab2list()
    |> Enum.filter(fn
      {{^owner, _path}, _dir, :file_fd, _fd, _position} -> true
      {{^owner, _path}, _dir, _kind, _handle, _position} -> true
      {{^owner, _path}, _dir, _handle, _position} -> true
      _entry -> false
    end)
  end

  defp writer_entry_path({{_owner, path}, _dir, :file_fd, _fd, _position}), do: path
  defp writer_entry_path({{_owner, path}, _dir, _kind, _handle, _position}), do: path
  defp writer_entry_path({{_owner, path}, _dir, _handle, _position}), do: path

  defp map_fold_seen({:ok, entries}) do
    {:ok, Enum.reverse(entries)}
  end

  use Ferricstore.Raft.WARaftSegmentLogTest.Sections.DefaultSegmentSizeDoesNotRollOverDuringNormalHotBatches
end
