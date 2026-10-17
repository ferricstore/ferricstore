Code.require_file(
  "waraft_backend_test/sections/invalid_waraft_in_flight_bytes_config_does_not_partially_publish_backend.exs",
  __DIR__
)

Code.require_file(
  "waraft_backend_test/sections/acked_writes_survive_waraft_server_kill_during_active_write_load.exs",
  __DIR__
)

Code.require_file(
  "waraft_backend_test/sections/waraft_storage_recovery_reuses_segment_locations_flow_replay.exs",
  __DIR__
)

Code.require_file(
  "waraft_backend_test/sections/restart_recovers_missing_current_storage_metadata_using_previous_durable.exs",
  __DIR__
)

Code.require_file("waraft_backend_test/sections/snapshot_copy_durability.exs", __DIR__)

Code.require_file(
  "waraft_backend_test/sections/startup_finalizes_interrupted_snapshot_swap_after_metadata_persisted.exs",
  __DIR__
)

Code.require_file(
  "waraft_backend_test/sections/snapshot_transfer_wraps_waraft_transport_exits.exs",
  __DIR__
)

Code.require_file(
  "waraft_backend_test/sections/flow_retention_cleanup_scans_all_waraft_shards.exs",
  __DIR__
)

Code.require_file(
  "waraft_backend_test/sections/segment_projection_trim_without_stall.exs",
  __DIR__
)

Code.require_file("waraft_backend_test/sections/helpers_part_01.exs", __DIR__)
Code.require_file("waraft_backend_test/sections/helpers_part_02.exs", __DIR__)

defmodule Ferricstore.Raft.WARaftBackendTest do
  use Ferricstore.Test.WARaftBackendCase

  @tag :strict_commit_apply_context
  test "commit rejects a backend context without replicated apply limits", %{ctx: ctx} do
    assert :ok = WARaftBackend.start(ctx, log_module: :ferricstore_waraft_spike_segment_log)

    context_key = {{WARaftBackend, :context}, :ferricstore_waraft_backend}
    :persistent_term.put(context_key, Map.delete(ctx, :apply_context))

    try do
      assert {:error, :missing_apply_context} =
               WARaftBackend.write(0, {:put, "commit:missing-apply-context", "value", 0})
    after
      :persistent_term.put(context_key, ctx)
    end

    assert Router.get(ctx, "commit:missing-apply-context") == nil
  end

  use Ferricstore.Raft.WARaftBackendTest.Sections.InvalidWaraftInFlightBytesConfigDoesNotPartiallyPublishBackend

  use Ferricstore.Raft.WARaftBackendTest.Sections.AckedWritesSurviveWaraftServerKillDuringActiveWriteLoad

  use Ferricstore.Raft.WARaftBackendTest.Sections.WaraftStorageRecoveryReusesSegmentLocationsFlowReplay

  use Ferricstore.Raft.WARaftBackendTest.Sections.RestartRecoversMissingCurrentStorageMetadataUsingPreviousDurable

  use Ferricstore.Raft.WARaftBackendTest.Sections.SnapshotCopyDurability

  use Ferricstore.Raft.WARaftBackendTest.Sections.StartupFinalizesInterruptedSnapshotSwapAfterMetadataPersisted

  use Ferricstore.Raft.WARaftBackendTest.Sections.SnapshotTransferWrapsWaraftTransportExits
  use Ferricstore.Raft.WARaftBackendTest.Sections.FlowRetentionCleanupScansAllWaraftShards
  use Ferricstore.Raft.WARaftBackendTest.Sections.SegmentProjectionTrimWithoutStall

  use Ferricstore.Raft.WARaftBackendTest.Sections.HelpersPart01
  use Ferricstore.Raft.WARaftBackendTest.Sections.HelpersPart02
end
