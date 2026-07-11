defmodule Ferricstore.Raft.WARaftSegmentProjectionMetricsTest do
  use ExUnit.Case, async: false
  @moduletag :raft

  alias Ferricstore.Raft.WARaftBackend

  test "hot put batches emit segment projection apply telemetry" do
    default_ctx = FerricStore.Instance.get(:default)

    root =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-waraft-segment-projection-metrics-#{System.unique_integer([:positive])}"
      )

    File.rm_rf!(root)
    Ferricstore.DataDir.ensure_layout!(root, 1)
    Ferricstore.Store.ActiveFile.init(1)

    ctx =
      FerricStore.Instance.build(
        :"waraft_segment_projection_metrics_#{System.unique_integer([:positive])}",
        data_dir: root,
        shard_count: 1,
        max_memory_bytes: 256 * 1024 * 1024,
        keydir_max_ram: 64 * 1024 * 1024,
        hot_cache_max_value_size: 65_536,
        blob_side_channel_threshold_bytes: 256 * 1024,
        max_active_file_size: 64 * 1024 * 1024,
        read_sample_rate: 100,
        lfu_decay_time: 1,
        lfu_log_factor: 10
      )

    parent = self()
    handler_id = "segment-projection-apply-#{System.unique_integer([:positive])}"
    commit_handler_id = "waraft-commit-stage-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler_id,
      [:ferricstore, :waraft, :segment_projection, :apply],
      fn event, measurements, metadata, _config ->
        send(parent, {:segment_projection_apply, event, measurements, metadata})
      end,
      nil
    )

    :telemetry.attach(
      commit_handler_id,
      [:ferricstore, :waraft, :commit, :stage],
      fn event, measurements, metadata, _config ->
        send(parent, {:waraft_commit_stage, event, measurements, metadata})
      end,
      nil
    )

    try do
      :persistent_term.erase(
        {Ferricstore.Flow.LMDBRebuilder.ActiveIndexes, :startup_active_rebuild_slots}
      )

      assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

      assert :ok =
               Ferricstore.Flow.LMDBRebuilder.__with_startup_active_rebuild_slot_for_test__(fn ->
                 :ok
               end)

      assert {:ok, [:ok, :ok, :ok]} =
               WARaftBackend.write_put_batch(0, [
                 {"segment-telemetry:a", "va", 0},
                 {"segment-telemetry:b", "vb", 0},
                 {"segment-telemetry:c", "vc", 0}
               ])

      assert_receive {:segment_projection_apply,
                      [:ferricstore, :waraft, :segment_projection, :apply],
                      %{duration_us: duration_us, applied_count: 3},
                      %{shard_index: 0, command_shape: :put_batch, result: :ok}},
                     1_000

      assert is_integer(duration_us)
      assert duration_us >= 0

      assert_receive {:waraft_commit_stage, [:ferricstore, :waraft, :commit, :stage],
                      %{duration_us: commit_duration_us, acquired_bytes: acquired_bytes},
                      %{
                        shard_index: 0,
                        command_shape: :put_batch,
                        stage: :sync,
                        path: :sync,
                        result: :ok
                      }},
                     1_000

      assert is_integer(commit_duration_us)
      assert commit_duration_us >= 0
      assert is_integer(acquired_bytes)
      assert acquired_bytes >= 0
    after
      :telemetry.detach(handler_id)
      :telemetry.detach(commit_handler_id)
      WARaftBackend.stop()
      FerricStore.Instance.cleanup(ctx.name)
      File.rm_rf!(root)
      :ok = WARaftBackend.start(default_ctx)
    end
  end
end
