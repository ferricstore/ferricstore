Code.require_file("waraft_backend_test/sections/rejects_volatile_waraft_ets_log_module.exs", __DIR__)
Code.require_file("waraft_backend_test/sections/unified_segment_trim_prunes_flow_apply_projection_value_cache.exs", __DIR__)
Code.require_file("waraft_backend_test/sections/invalid_waraft_in_flight_bytes_config_does_not_partially_publish_backend.exs", __DIR__)
Code.require_file("waraft_backend_test/sections/acked_writes_survive_waraft_server_kill_during_active_write_load.exs", __DIR__)
Code.require_file("waraft_backend_test/sections/segment_log_rejects_records_stored_under_wrong_segment_ordinal.exs", __DIR__)
Code.require_file("waraft_backend_test/sections/waraft_generic_batches_coalesce_behind_in_flight_flush_default.exs", __DIR__)
Code.require_file("waraft_backend_test/sections/storage_metadata_hot_writes_fsync_journal_rewriting_current_metadata.exs", __DIR__)
Code.require_file("waraft_backend_test/sections/waraft_storage_recovery_reuses_segment_locations_flow_replay.exs", __DIR__)
Code.require_file("waraft_backend_test/sections/restart_recovers_missing_current_storage_metadata_using_previous_durable.exs", __DIR__)
Code.require_file("waraft_backend_test/sections/storage_rejects_snapshot_payload_dir_removed_after_verification.exs", __DIR__)
Code.require_file("waraft_backend_test/sections/startup_finalizes_interrupted_snapshot_swap_after_metadata_persisted.exs", __DIR__)
Code.require_file("waraft_backend_test/sections/snapshot_transfer_wraps_waraft_transport_exits.exs", __DIR__)
Code.require_file("waraft_backend_test/sections/waraft_storage_close_flushes_async_flow_history_before_persisting_replay.exs", __DIR__)
Code.require_file("waraft_backend_test/sections/flow_retention_cleanup_scans_all_waraft_shards.exs", __DIR__)
Code.require_file("waraft_backend_test/sections/advanced_zset_range_pop_mutations_survive_waraft_restart.exs", __DIR__)
Code.require_file("waraft_backend_test/sections/numeric_append_expiring_string_commands_survive_waraft_restart.exs", __DIR__)
Code.require_file("waraft_backend_test/sections/three_peer_backend_cluster_recovers_after_leader_os_process_kill_during.exs", __DIR__)
Code.require_file("waraft_backend_test/sections/three_peer_backend_cluster_removes_member_through_backend_api.exs", __DIR__)
Code.require_file("waraft_backend_test/sections/backend_add_member_retries_staged_participant_after_failed_transfer.exs", __DIR__)

for part <- 1..2 do
  Code.require_file("waraft_backend_test/sections/helpers_part_#{part |> Integer.to_string() |> String.pad_leading(2, "0")}.exs", __DIR__)
end

defmodule Ferricstore.Raft.WARaftBackendTest do
  use ExUnit.Case, async: false
  @moduletag :raft

  import ExUnit.CaptureLog

  alias Ferricstore.ErrorReasons
  alias Ferricstore.Raft.Cluster, as: RaftCluster
  alias Ferricstore.Raft.WARaftBackend
  alias Ferricstore.Raft.WARaftStorage
  alias Ferricstore.Store.{BlobRef, BlobStore}
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.Router

  defmodule LabelCounter do
    @moduledoc false

    def new_label(nil, _command), do: 1
    def new_label(:undefined, _command), do: 1
    def new_label(label, _command) when is_integer(label), do: label + 1
  end

  defmodule OversizedLabel do
    @moduledoc false

    def new_label(_label, _command), do: :binary.copy("x", 1_048_576)
  end

  def handle_test_telemetry(event, measurements, metadata, parent) do
    send(parent, {:waraft_storage_blocked, event, measurements, metadata})
  end

  def handle_segment_log_telemetry(event, measurements, metadata, parent) do
    send(parent, {:waraft_segment_log_telemetry, event, measurements, metadata})
  end

  def handle_namespace_batcher_telemetry(event, measurements, metadata, parent) do
    send(parent, {:waraft_namespace_batcher_flush, event, measurements, metadata})
  end

  def handle_payload_fsync_telemetry(event, measurements, metadata, parent) do
    send(parent, {:waraft_payload_fsync_telemetry, event, measurements, metadata})
  end

  def handle_blob_prepare_failed_telemetry(event, measurements, metadata, parent) do
    send(parent, {:waraft_blob_prepare_failed, event, measurements, metadata})
  end

  def handle_commit_timeout_telemetry(event, measurements, metadata, parent) do
    send(parent, {:waraft_commit_timeout, event, measurements, metadata})
  end

  def handle_storage_startup_phase_telemetry(event, measurements, metadata, parent) do
    send(parent, {:waraft_storage_startup_phase, event, measurements, metadata})
  end

  def handle_storage_apply_phase_telemetry(event, measurements, metadata, parent) do
    send(parent, {:waraft_storage_apply_phase, event, measurements, metadata})
  end

  def handle_segment_projection_checkpoint_telemetry(event, measurements, metadata, parent) do
    send(parent, {:waraft_segment_projection_checkpoint, event, measurements, metadata})
  end

  def handle_segment_projection_trim_telemetry(event, measurements, metadata, parent) do
    send(parent, {:waraft_segment_projection_trim, event, measurements, metadata})
  end

  def handle_store_unavailable_telemetry(event, measurements, metadata, parent) do
    send(parent, {:store_unavailable, event, measurements, metadata})
  end

  setup do
    root =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-waraft-backend-#{System.unique_integer([:positive])}"
      )

    File.rm_rf!(root)
    Ferricstore.DataDir.ensure_layout!(root, 1)
    Ferricstore.Store.ActiveFile.init(1)

    ctx = build_ctx(root)

    on_exit(fn ->
      WARaftBackend.stop()
      FerricStore.Instance.cleanup(ctx.name)
      File.rm_rf!(root)
    end)

    %{root: root, ctx: ctx}
  end


  use Ferricstore.Raft.WARaftBackendTest.Sections.RejectsVolatileWaraftEtsLogModule
  use Ferricstore.Raft.WARaftBackendTest.Sections.UnifiedSegmentTrimPrunesFlowApplyProjectionValueCache
  use Ferricstore.Raft.WARaftBackendTest.Sections.InvalidWaraftInFlightBytesConfigDoesNotPartiallyPublishBackend
  use Ferricstore.Raft.WARaftBackendTest.Sections.AckedWritesSurviveWaraftServerKillDuringActiveWriteLoad
  use Ferricstore.Raft.WARaftBackendTest.Sections.SegmentLogRejectsRecordsStoredUnderWrongSegmentOrdinal
  use Ferricstore.Raft.WARaftBackendTest.Sections.WaraftGenericBatchesCoalesceBehindInFlightFlushDefault
  use Ferricstore.Raft.WARaftBackendTest.Sections.StorageMetadataHotWritesFsyncJournalRewritingCurrentMetadata
  use Ferricstore.Raft.WARaftBackendTest.Sections.WaraftStorageRecoveryReusesSegmentLocationsFlowReplay
  use Ferricstore.Raft.WARaftBackendTest.Sections.RestartRecoversMissingCurrentStorageMetadataUsingPreviousDurable
  use Ferricstore.Raft.WARaftBackendTest.Sections.StorageRejectsSnapshotPayloadDirRemovedAfterVerification
  use Ferricstore.Raft.WARaftBackendTest.Sections.StartupFinalizesInterruptedSnapshotSwapAfterMetadataPersisted
  use Ferricstore.Raft.WARaftBackendTest.Sections.SnapshotTransferWrapsWaraftTransportExits
  use Ferricstore.Raft.WARaftBackendTest.Sections.WaraftStorageCloseFlushesAsyncFlowHistoryBeforePersistingReplay
  use Ferricstore.Raft.WARaftBackendTest.Sections.FlowRetentionCleanupScansAllWaraftShards
  use Ferricstore.Raft.WARaftBackendTest.Sections.AdvancedZsetRangePopMutationsSurviveWaraftRestart
  use Ferricstore.Raft.WARaftBackendTest.Sections.NumericAppendExpiringStringCommandsSurviveWaraftRestart
  use Ferricstore.Raft.WARaftBackendTest.Sections.ThreePeerBackendClusterRecoversAfterLeaderOsProcessKillDuring
  use Ferricstore.Raft.WARaftBackendTest.Sections.ThreePeerBackendClusterRemovesMemberThroughBackendApi
  use Ferricstore.Raft.WARaftBackendTest.Sections.BackendAddMemberRetriesStagedParticipantAfterFailedTransfer

  use Ferricstore.Raft.WARaftBackendTest.Sections.HelpersPart01
  use Ferricstore.Raft.WARaftBackendTest.Sections.HelpersPart02
end
