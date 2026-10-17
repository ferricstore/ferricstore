# The second half of the WARaft backend suite; see Ferricstore.Test.WARaftBackendCase.
Code.require_file(
  "waraft_backend_test/sections/rejects_volatile_waraft_ets_log_module.exs",
  __DIR__
)

Code.require_file(
  "waraft_backend_test/sections/unified_segment_trim_prunes_flow_apply_projection_value_cache.exs",
  __DIR__
)

Code.require_file(
  "waraft_backend_test/sections/segment_log_rejects_records_stored_under_wrong_segment_ordinal.exs",
  __DIR__
)

Code.require_file(
  "waraft_backend_test/sections/waraft_generic_batches_coalesce_behind_in_flight_flush_default.exs",
  __DIR__
)

Code.require_file(
  "waraft_backend_test/sections/storage_metadata_hot_writes_fsync_journal_rewriting_current_metadata.exs",
  __DIR__
)

Code.require_file(
  "waraft_backend_test/sections/storage_rejects_snapshot_payload_dir_removed_after_verification.exs",
  __DIR__
)

Code.require_file("waraft_backend_test/sections/startup_storage_deadline.exs", __DIR__)

Code.require_file(
  "waraft_backend_test/sections/waraft_storage_close_flushes_async_flow_history_before_persisting_replay.exs",
  __DIR__
)

Code.require_file(
  "waraft_backend_test/sections/advanced_zset_range_pop_mutations_survive_waraft_restart.exs",
  __DIR__
)

Code.require_file(
  "waraft_backend_test/sections/numeric_append_expiring_string_commands_survive_waraft_restart.exs",
  __DIR__
)

Code.require_file(
  "waraft_backend_test/sections/direct_waraft_shard_reads_preserve_typed_failures.exs",
  __DIR__
)

Code.require_file(
  "waraft_backend_test/sections/three_peer_backend_cluster_recovers_after_leader_os_process_kill_during.exs",
  __DIR__
)

Code.require_file(
  "waraft_backend_test/sections/three_peer_backend_cluster_removes_member_through_backend_api.exs",
  __DIR__
)

Code.require_file(
  "waraft_backend_test/sections/backend_add_member_retries_staged_participant_after_failed_transfer.exs",
  __DIR__
)

Code.require_file("waraft_backend_test/sections/helpers_part_01.exs", __DIR__)
Code.require_file("waraft_backend_test/sections/helpers_part_02.exs", __DIR__)

defmodule Ferricstore.Raft.WARaftBackendPart2Test do
  use Ferricstore.Test.WARaftBackendCase

  use Ferricstore.Raft.WARaftBackendTest.Sections.RejectsVolatileWaraftEtsLogModule

  use Ferricstore.Raft.WARaftBackendTest.Sections.UnifiedSegmentTrimPrunesFlowApplyProjectionValueCache

  use Ferricstore.Raft.WARaftBackendTest.Sections.SegmentLogRejectsRecordsStoredUnderWrongSegmentOrdinal

  use Ferricstore.Raft.WARaftBackendTest.Sections.WaraftGenericBatchesCoalesceBehindInFlightFlushDefault

  use Ferricstore.Raft.WARaftBackendTest.Sections.StorageMetadataHotWritesFsyncJournalRewritingCurrentMetadata

  use Ferricstore.Raft.WARaftBackendTest.Sections.StorageRejectsSnapshotPayloadDirRemovedAfterVerification

  use Ferricstore.Raft.WARaftBackendTest.Sections.StartupStorageDeadline

  use Ferricstore.Raft.WARaftBackendTest.Sections.WaraftStorageCloseFlushesAsyncFlowHistoryBeforePersistingReplay

  use Ferricstore.Raft.WARaftBackendTest.Sections.AdvancedZsetRangePopMutationsSurviveWaraftRestart

  use Ferricstore.Raft.WARaftBackendTest.Sections.NumericAppendExpiringStringCommandsSurviveWaraftRestart

  use Ferricstore.Raft.WARaftBackendTest.Sections.DirectWaraftShardReadsPreserveTypedFailures

  use Ferricstore.Raft.WARaftBackendTest.Sections.ThreePeerBackendClusterRecoversAfterLeaderOsProcessKillDuring

  use Ferricstore.Raft.WARaftBackendTest.Sections.ThreePeerBackendClusterRemovesMemberThroughBackendApi

  use Ferricstore.Raft.WARaftBackendTest.Sections.BackendAddMemberRetriesStagedParticipantAfterFailedTransfer

  use Ferricstore.Raft.WARaftBackendTest.Sections.HelpersPart01
  use Ferricstore.Raft.WARaftBackendTest.Sections.HelpersPart02
end
