defmodule Ferricstore.Raft.StateMachine do
  @moduledoc """
  Replicated state machine for a single FerricStore shard.

  Each shard is an independent Raft group. The state machine receives write
  commands via `apply/3`, which deterministically applies them to both the
  Bitcask persistent store (via synchronous NIF) and the ETS hot cache.

  ## Callbacks

    * `init/1` -- receives the shard config, returns initial machine state.
    * `apply/3` -- deterministic command application (called on every node).
      Supports `:put`, `:delete`, and `:batch` commands.
    * `state_enter/2` -- lifecycle hook for leader/follower transitions.
    * `tick/2` -- periodic callback (unused currently, placeholder for metrics).
    * `init_aux/1` -- initializes non-replicated auxiliary state.
    * `handle_aux/5` -- handles non-replicated auxiliary commands (new API).
    * `overview/1` -- returns a summary map for debugging/monitoring.

  ## Design notes

  Per the spec (section 2C.4):
  - `apply/3` is deterministic and runs on every node in the Raft group.
  - Cold disk reads inside `apply/3` wait synchronously for deterministic
    results, but submit the actual file I/O through async NIFs so Normal
    schedulers do not run blocking pread work.
  - Effects (`send_msg`, `release_cursor`) are returned as the third element
    of the apply return tuple.
  - In single-node mode, the shard's Raft group has one member (self quorum),
    so every write commits immediately after local log append + fsync.

  ## HLC piggybacking (spec 2G.6)

  HLC timestamps are piggybacked on replicated commands. Submit paths stamp each
  log entry with the leader's current HLC timestamp before appending it.
  When `apply/3` processes a command carrying an `hlc_ts` metadata map, it
  calls `HLC.update/1` to merge the leader's clock into the local node's HLC
  and uses the stamped physical millisecond for TTL and lock expiry decisions.

  In single-node mode this merge is a no-op (the node merges its own
  timestamp). In multi-node clusters, followers use this to stay
  causally synchronized with the leader's clock while applying the same
  command timestamp as every other replica.

  Commands may arrive in two forms:

    * **Wrapped**: `{inner_command, %{hlc_ts: {physical_ms, logical}}}` --
      the metadata map carries the leader's HLC timestamp for merging.
    * **Unwrapped**: `inner_command` (legacy / test) -- processed as before
      without HLC merging.

  ## Log compaction (spec 2E.5)

  The replicated log grows unbounded unless compacted. Every
  `:release_cursor_interval` applied commands (default: 200_000), `apply/3`
  emits `{:checkpoint, ra_index, state}` and `{:release_cursor, ra_index}`
  effects. This tells the log that all entries up to `ra_index` are fully
  reflected in the given state checkpoint and can be safely truncated after
  the checkpoint is materialized.

  The interval is stored in the machine state at init time (from the config
  map or application env) so that `apply/3` remains deterministic -- it never
  reads runtime configuration.

  ## Performance boundary

  `apply/3` is the core durable write hot path. Do not replace section macros
  or split apply helpers without before/after DBOS Flow and memtier benchmarks.
  No behaviours/protocol dispatch, no extra maps/lists in per-command loops,
  and no extra GenServer/Task calls in apply.
  """

  import Kernel, except: [apply: 3]
  import Bitwise

  require Logger

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.CommandTime
  alias Ferricstore.Commands.Dispatcher
  alias Ferricstore.Commands.HyperLogLog
  alias Ferricstore.Raft.BlobCommand
  alias Ferricstore.Flow
  alias Ferricstore.Flow.Hibernation
  alias Ferricstore.Flow.HistoryProjector
  alias Ferricstore.Flow.Locator
  alias Ferricstore.Flow.NativeOrderedIndex, as: NativeFlowIndex
  alias Ferricstore.Flow.Keys, as: FlowKeys
  alias Ferricstore.Flow.RetryPolicy
  alias Ferricstore.HLC

  alias Ferricstore.Store.{
    BitcaskWriter,
    BlobRef,
    BlobStore,
    BlobValue,
    ColdRead,
    CompoundKey,
    ExpiryTracker,
    LFU,
    ListOps,
    Promotion,
    Router,
    ValueCodec
  }

  alias Ferricstore.Store.Shard.ZSetIndex
  alias Ferricstore.Store.Shard.Transaction, as: ShardTransaction
  alias Ferricstore.Store.Shard.Flush, as: ShardFlush
  alias Ferricstore.Transaction.Ast, as: TxAst

  @default_release_cursor_interval 200_000
  @default_max_active_file_size 8 * 1024 * 1024 * 1024
  @default_fragmentation_threshold 0.5
  @default_dead_bytes_threshold 134_217_728
  @bitcask_record_header_size 26
  @cold_read_timeout_ms 10_000
  @cold_location_retry_attempts 8
  @cold_location_retry_sleep_ms 1
  # Keep in sync with Router.max_key_size/0. This validation runs inside Flow
  # apply/claim loops, so avoid calling back through Router for a constant.
  @flow_max_key_size 65_535
  @flow_shard_marker :__flow_shard_index__
  @sm_apply_state_key :sm_apply_state
  @sm_standalone_staged_key :sm_standalone_staged_apply
  @sm_waraft_projection_writer_key :sm_waraft_projection_writer
  @sm_pending_write_keys [
    :sm_pending_writes,
    :sm_pending_originals,
    :sm_pending_values,
    :sm_pending_lmdb_values,
    :sm_pending_lmdb_mirror_ops,
    :sm_pending_lmdb_mirror_after_flush,
    :sm_pending_lmdb_projection_outbox,
    :sm_pending_lmdb_projection_dirty_shards,
    :sm_pending_lmdb_mirror_default_shard,
    :sm_pending_lmdb_mirror_tagged,
    :sm_pending_flow_hibernation_candidates,
    :sm_pending_flow_history_projections,
    :sm_pending_flow_native_ops,
    :sm_pending_flow_native_flush?,
    :sm_pending_zset_index_ops,
    :sm_pending_compound_promotions,
    :sm_pending_prob_creates,
    :sm_pending_has_delete,
    :sm_pending_fast_put_batch,
    :sm_pending_fast_delete_batch,
    :sm_pending_fast_staged_put_batch
  ]
  @sm_pending_write_initial_values [
    sm_pending_writes: [],
    sm_pending_originals: %{},
    sm_pending_values: %{},
    sm_pending_lmdb_values: %{},
    sm_pending_lmdb_mirror_ops: [],
    sm_pending_lmdb_mirror_after_flush: [],
    sm_pending_lmdb_projection_outbox: [],
    sm_pending_lmdb_projection_dirty_shards: MapSet.new(),
    sm_pending_lmdb_mirror_default_shard: nil,
    sm_pending_lmdb_mirror_tagged: false,
    sm_pending_flow_hibernation_candidates: [],
    sm_pending_flow_history_projections: [],
    sm_pending_flow_native_ops: [],
    sm_pending_flow_native_flush?: false,
    sm_pending_zset_index_ops: [],
    sm_pending_compound_promotions: MapSet.new(),
    sm_pending_prob_creates: [],
    sm_pending_has_delete: false,
    sm_pending_fast_put_batch: false,
    sm_pending_fast_delete_batch: false,
    sm_pending_fast_staged_put_batch: false
  ]

  defguardp valid_cold_location(file_id, offset, value_size)
            when is_integer(file_id) and file_id >= 0 and is_integer(offset) and offset >= 0 and
                   is_integer(value_size) and value_size >= 0

  defguardp valid_waraft_segment_location(file_id, offset, value_size)
            when is_tuple(file_id) and tuple_size(file_id) == 2 and
                   (elem(file_id, 0) == :waraft_segment or
                      elem(file_id, 0) == :waraft_projection or
                      elem(file_id, 0) == :waraft_apply_projection) and
                   is_integer(elem(file_id, 1)) and elem(file_id, 1) > 0 and
                   is_integer(offset) and offset >= 0 and
                   is_integer(value_size) and value_size >= 0

  @type shard_state :: %{
          shard_index: non_neg_integer(),
          data_dir: binary(),
          data_dir_expanded: binary(),
          shard_data_path: binary(),
          shard_data_path_expanded: binary(),
          active_file_id: non_neg_integer(),
          active_file_path: binary(),
          ets: atom(),
          applied_count: non_neg_integer(),
          release_cursor_interval: pos_integer()
        }

  # ---------------------------------------------------------------------------
  # Replicated state callbacks
  # ---------------------------------------------------------------------------

  use Ferricstore.Raft.StateMachine.Sections.Init
  use Ferricstore.Raft.StateMachine.Sections.ApplyDispatch
  use Ferricstore.Raft.StateMachine.Sections.RaftCallbacks
  use Ferricstore.Raft.StateMachine.Sections.CrossShardDispatch
  use Ferricstore.Raft.StateMachine.Sections.CrossShardReads
  use Ferricstore.Raft.StateMachine.Sections.AsyncApply
  use Ferricstore.Raft.StateMachine.Sections.CompoundApply
  use Ferricstore.Raft.StateMachine.Sections.CrossShardPending
  use Ferricstore.Raft.StateMachine.Sections.FlowCreate
  use Ferricstore.Raft.StateMachine.Sections.FlowClaimDue
  use Ferricstore.Raft.StateMachine.Sections.FlowClaimScan
  use Ferricstore.Raft.StateMachine.Sections.FlowClaimNativePlan
  use Ferricstore.Raft.StateMachine.Sections.FlowTransition
  use Ferricstore.Raft.StateMachine.Sections.FlowTerminal
  use Ferricstore.Raft.StateMachine.Sections.FlowRetentionState
  use Ferricstore.Raft.StateMachine.Sections.FlowRetentionValues
  use Ferricstore.Raft.StateMachine.Sections.FlowClaimIndexes
  use Ferricstore.Raft.StateMachine.Sections.FlowClaimStateWrites
  use Ferricstore.Raft.StateMachine.Sections.FlowHistoryWrites
  use Ferricstore.Raft.StateMachine.Sections.FlowHistoryReads
  use Ferricstore.Raft.StateMachine.Sections.FlowValues
  use Ferricstore.Raft.StateMachine.Sections.PendingWrites
  use Ferricstore.Raft.StateMachine.Sections.PendingLocations
  use Ferricstore.Raft.StateMachine.Sections.LmdbProjection
  use Ferricstore.Raft.StateMachine.Sections.DataMutations
  use Ferricstore.Raft.StateMachine.Sections.ReadWarm
  use Ferricstore.Raft.StateMachine.Sections.CompoundIndexes
end
