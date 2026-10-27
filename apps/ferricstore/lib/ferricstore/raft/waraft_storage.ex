defmodule Ferricstore.Raft.WARaftStorage do
  @moduledoc false

  alias Ferricstore.CommandTime
  alias Ferricstore.ExpiryContext
  alias Ferricstore.HLC
  alias Ferricstore.Flow.HistoryProjector
  alias Ferricstore.Flow.Keys, as: FlowKeys
  alias Ferricstore.Flow.LMDB, as: FlowLMDB
  alias Ferricstore.Raft.StateMachine
  alias Ferricstore.Raft.WARaftSegmentReader
  alias Ferricstore.Store.BlobRef
  alias Ferricstore.Store.BlobStore
  alias Ferricstore.Store.BlobValue
  alias Ferricstore.Store.ColdRead
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.Promotion
  alias Ferricstore.Store.Shard.ETS, as: ShardETS
  alias Ferricstore.Store.Shard.Lifecycle, as: ShardLifecycle
  alias Ferricstore.Store.Shard.ZSetIndex

  @metadata_file "ferricstore_storage.term"
  @snapshot_metadata_file "ferricstore_snapshot.term"
  @segment_projection_dir "segment_projection_log"
  @segment_projection_checkpoint_dir "segment_projection_checkpoint_log"
  @apply_projection_dir "apply_projection_log"
  @snapshot_install_marker_file "snapshot_install.term"
  @metadata_previous_suffix ".previous"
  @metadata_journal_suffix ".journal"
  @metadata_journal_magic "FSMJ1"
  @max_storage_metadata_bytes 1_048_576
  @max_metadata_journal_record_bytes @max_storage_metadata_bytes
  @max_metadata_journal_bytes 67_108_864
  @max_snapshot_metadata_bytes @max_storage_metadata_bytes
  @max_snapshot_install_marker_bytes @max_storage_metadata_bytes
  @version 1
  @default_storage_metadata_persist_every 1_024
  @default_segment_projection_checkpoint_every 1_000_000
  @default_segment_projection_checkpoint_min_interval_ms 30_000
  # How far a trim may run ahead of the durable log floor while it waits for a
  # background checkpoint. Past this, it rebuilds the projection synchronously.
  @default_segment_projection_trim_max_deferred_entries 1_000_000
  # After a synchronous rebuild fails, trims defer for a second, doubling while
  # rebuilds keep failing, up to this maximum (0 retries at once).
  @segment_projection_rebuild_first_backoff_ms 1_000
  @default_segment_projection_trim_rebuild_backoff_max_ms 60_000
  # How long storage holds its applies for a checkpoint's keydir copy, and how
  # long the checkpoint task waits for storage.
  @segment_projection_cut_timeout_ms 30_000
  # How long a checkpoint task waits for the Flow history of the entries its
  # cut covers before it gives up and retries with a later cut.
  @default_segment_projection_checkpoint_history_timeout_ms 5_000
  # How often storage takes a new durable candidate between metadata
  # persists, and how often a waiting one asks the projector again (in case
  # a request was lost).
  @durable_candidate_interval_ms 100
  # A shard waiting for a checkpoint slot retries about once a second while it
  # still needs one; past this it no longer holds its place in line.
  @segment_projection_checkpoint_waiter_ttl_ms 5_000
  @default_metadata_compact_every 1024
  @default_snapshot_compaction_drain_timeout_ms 30_000
  # WARaft kills a storage process whose close runs past 30 s, and the next
  # start then replays as after a crash. Close waits for Flow history and cache
  # compaction only this long, leaving the rest for the payload flush and the
  # metadata write.
  @default_storage_close_budget_ms 20_000
  @cold_read_timeout_ms 10_000
  @zero_pos {:raft_log_pos, 0, 0}
  @segment_projection_registry :ferricstore_waraft_segment_projection_registry
  @storage_root "ferricstore_waraft_backend"
  @segment_log_root_cache {__MODULE__, :segment_log_root}
  @segment_value_pin_scan_limit 100_000
  @apply_projection_replay_dependencies_key :ferricstore_waraft_apply_projection_replay_dependencies

  defguardp valid_segment_backed_file_id(file_id)
            when is_tuple(file_id) and tuple_size(file_id) == 2 and
                   (elem(file_id, 0) == :waraft_segment or
                      elem(file_id, 0) == :waraft_projection or
                      elem(file_id, 0) == :waraft_apply_projection) and
                   is_integer(elem(file_id, 1)) and elem(file_id, 1) > 0

  @type handle :: map()

  require Logger

  defp storage_expiry_cutoff_ms do
    ExpiryContext.capture()
    |> ExpiryContext.safe_expiry_cutoff_ms()
  end

  use Ferricstore.Raft.WARaftStorage.Sections.Lifecycle
  use Ferricstore.Raft.WARaftStorage.Sections.SegmentProjectCommands
  use Ferricstore.Raft.WARaftStorage.Sections.SegmentProjection
  use Ferricstore.Raft.WARaftStorage.Sections.ApplyResult
  use Ferricstore.Raft.WARaftStorage.Sections.Recovery
  use Ferricstore.Raft.WARaftStorage.Sections.Metadata
  use Ferricstore.Raft.WARaftStorage.Sections.ProjectionSnapshot
  use Ferricstore.Raft.WARaftStorage.Sections.SnapshotMetadata
  use Ferricstore.Raft.WARaftStorage.Sections.SnapshotInstall
  use Ferricstore.Raft.WARaftStorage.Sections.ZSetTreeMaintenance
end
