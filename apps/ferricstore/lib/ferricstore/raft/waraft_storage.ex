defmodule Ferricstore.Raft.WARaftStorage do
  @moduledoc false

  alias Ferricstore.CommandTime
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
  @max_snapshot_metadata_bytes @max_storage_metadata_bytes
  @max_snapshot_install_marker_bytes @max_storage_metadata_bytes
  @version 1
  @default_storage_metadata_persist_every 1_024
  @default_segment_projection_checkpoint_every 1_000_000
  @default_segment_projection_checkpoint_min_interval_ms 30_000
  @default_metadata_compact_every 1024
  @default_snapshot_compaction_drain_timeout_ms 30_000
  @cold_read_timeout_ms 10_000
  @zero_pos {:raft_log_pos, 0, 0}
  @encoded_peer_tag :ferricstore_waraft_peer
  @segment_projection_registry :ferricstore_waraft_segment_projection_registry
  @storage_root "ferricstore_waraft_backend"
  @segment_value_pin_scan_limit 100_000
  @apply_projection_replay_dependencies_key :ferricstore_waraft_apply_projection_replay_dependencies

  defguardp valid_segment_backed_file_id(file_id)
            when is_tuple(file_id) and tuple_size(file_id) == 2 and
                   (elem(file_id, 0) == :waraft_segment or
                      elem(file_id, 0) == :waraft_projection or
                      elem(file_id, 0) == :waraft_apply_projection) and
                   is_integer(elem(file_id, 1)) and elem(file_id, 1) > 0

  @type handle :: map()


  use Ferricstore.Raft.WARaftStorage.Sections.Part01
  use Ferricstore.Raft.WARaftStorage.Sections.Part02
  use Ferricstore.Raft.WARaftStorage.Sections.Part03
  use Ferricstore.Raft.WARaftStorage.Sections.Part04
  use Ferricstore.Raft.WARaftStorage.Sections.Part05
  use Ferricstore.Raft.WARaftStorage.Sections.Part06
  use Ferricstore.Raft.WARaftStorage.Sections.Part07
  use Ferricstore.Raft.WARaftStorage.Sections.Part08
  use Ferricstore.Raft.WARaftStorage.Sections.Part09
end
