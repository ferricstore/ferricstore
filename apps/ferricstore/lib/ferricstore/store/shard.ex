defmodule Ferricstore.Store.Shard do
  @moduledoc """
  GenServer managing one Bitcask partition backed by an ETS hot-read cache.

  ## v2 Architecture: Pure Stateless NIFs

  All state lives in Elixir (ETS keydir + GenServer state). Rust NIFs are
  pure stateless functions: `v2_append_record`, `v2_pread_at`, `v2_fsync`,
  `v2_append_batch`, `v2_append_tombstone`, `v2_scan_file`, hint file I/O.
  No Rust-side Store resource, HashMap keydir, or Mutex.

  ## Write path: group commit

  1. The key is written to ETS immediately (reads see it at once).
  2. The entry is appended to an in-memory pending list.
  3. A recurring `:drain_pending` timer fires every `@flush_interval_ms`
     and calls `NIF.v2_append_batch_nosync/2` with all accumulated
     entries, then updates ETS entries with their disk locations
     (file_id, offset, value_size). This step moves bytes from BEAM
     memory to the kernel page cache — **no fsync**.
     Data-file durability is owned by `Ferricstore.Store.BitcaskCheckpointer`,
     which runs on its own, longer tick and issues `v2_fsync` against
     the same active file.
  4. File rotation occurs when the active file exceeds 256 MB.

  ## Read path: ETS bypass

  `Router.get/1` and `Router.get_meta/1` read ETS directly without going
  through this GenServer for hot (cached) keys. Cold keys (value=nil in ETS)
  have their disk location (file_id, offset) stored in the ETS 7-tuple,
  enabling direct `v2_pread_at` without scanning.

  ## ETS layout

  Each entry is a 7-tuple `{key, value, expire_at_ms, lfu_counter, file_id, offset, value_size}`
  where `expire_at_ms = 0` means the key never expires. The `file_id`, `offset`,
  and `value_size` fields enable cold reads without scanning, STRLEN on cold keys,
  and sendfile zero-copy. Expired entries are lazily evicted on read.

  ## Process registration

  Shards register under the name returned by
  `Ferricstore.Store.Router.shard_name/1`, e.g.
  `:"Ferricstore.Store.Shard.0"`.

  ## Performance boundary

  Shard write/read helpers are hot. Keep per-command paths allocation-light and
  avoid extra GenServer/Task hops. Any refactor in this module or its shard
  helper modules needs memtier and Flow benchmark comparison.
  """

  use GenServer

  alias Ferricstore.Bitcask.NIF
  alias Ferricstore.Flow.Hibernation
  alias Ferricstore.Flow.LMDB
  alias Ferricstore.Flow.Locator
  alias Ferricstore.Flow.NativeOrderedIndex, as: NativeFlowIndex
  alias Ferricstore.Raft.Backend, as: RaftBackend
  alias Ferricstore.Store.CompoundKey
  alias Ferricstore.Store.BlobValue
  alias Ferricstore.Store.ColdRead
  alias Ferricstore.Store.Router
  alias Ferricstore.Store.Shard.Compound, as: ShardCompound
  alias Ferricstore.Store.Shard.ETS, as: ShardETS
  alias Ferricstore.Store.Shard.Flush, as: ShardFlush
  alias Ferricstore.Store.Shard.Lifecycle, as: ShardLifecycle
  alias Ferricstore.Store.Shard.NativeOps, as: ShardNativeOps
  alias Ferricstore.Store.Shard.Reads, as: ShardReads
  alias Ferricstore.Store.Shard.Transaction, as: ShardTransaction
  alias Ferricstore.Store.Shard.Writes, as: ShardWrites
  alias Ferricstore.Store.Shard.ZSetIndex

  require Logger

  # How often (ms) to flush the pending write queue to disk.
  # 1ms gives up to 50k batched writes/s per shard (4 shards → 200k/s total).
  @flush_interval_ms 1
  @cold_read_timeout_ms 10_000
  @cold_read_compaction_retry_attempts 8
  @cold_read_compaction_retry_delay_ms 1
  @standalone_global_key "__standalone_global__"
  @standalone_flow_many_commands [
    :flow_create_many,
    :flow_complete_many,
    :flow_transition_many,
    :flow_retry_many,
    :flow_fail_many,
    :flow_cancel_many,
    :flow_run_steps_many
  ]

  # Default maximum active file size before rotation (256 MB).
  # Configurable via :max_active_file_size application env.
  @default_max_active_file_size 256 * 1024 * 1024

  # Default fragmentation thresholds for per-file dead bytes tracking.
  @default_fragmentation_threshold 0.5
  @default_dead_bytes_threshold 134_217_728

  defstruct [
    :ets,
    :keydir,
    :index,
    :data_dir,
    # Cached result of DataDir.shard_data_path(data_dir, index).
    # Computed once during init; avoids string concat on every cold read/flush.
    :shard_data_path,
    # FerricStore.Instance context — holds all per-instance refs (shard_names,
    # slot_map, keydir_refs, atomics, config) needed to route operations without
    # global state. Passed to Router.* calls instead of persistent_term lookups.
    :instance_ctx,
    :active_file_id,
    :active_file_path,
    :active_file_size,
    :active_file_preallocated_to,
    pending: [],
    pending_count: 0,
    last_flush_error: nil,
    flush_in_flight: nil,
    write_version: 0,
    sweep_at_ceiling_count: 0,
    sweep_struggling: false,
    promoted_instances: %{},
    # Per-file dead bytes tracking: %{file_id => {total_bytes, dead_bytes}}
    file_stats: %{},
    # Merge config overrides for fragmentation thresholds
    merge_config: %{},
    # Map from correlation_id => {from, key} for in-flight Tokio async reads.
    # Correlation IDs fix the LIFO ordering bug from the old list-based approach.
    pending_reads: %{},
    # Monotonically increasing counter for async read/write correlation IDs.
    next_correlation_id: 0,
    # Whether this shard has quorum-write infrastructure.
    # Application-supervised shards always have WARaft. Isolated test
    # shards with ad-hoc indices use the direct write path instead.
    raft?: true,
    # Maximum active file size before rotation. Cached from Application env
    # at init time. Updated via handle_cast(:update_max_active_file_size, n).
    max_active_file_size: 256 * 1024 * 1024,
    writes_paused: false,
    compound_member_index: nil,
    zset_score_index: nil,
    zset_score_lookup: nil,
    flow_index: nil,
    flow_lookup: nil,
    zset_index_ready: MapSet.new(),
    standalone_batch: [],
    standalone_batch_timer: nil,
    standalone_flush_ref: nil,
    standalone_flush_entries: [],
    standalone_inflight_keys: MapSet.new(),
    standalone_waiting: [],
    standalone_write_barrier: false
  ]

  # -------------------------------------------------------------------
  # Public API
  # -------------------------------------------------------------------

  @doc """
  Starts a shard GenServer.

  ## Options

    * `:index` (required) -- zero-based shard index
    * `:data_dir` (required) -- base directory for Bitcask data files
    * `:flush_interval_ms` -- batch-commit interval in ms (default: #{@flush_interval_ms})
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    index = Keyword.fetch!(opts, :index)
    ctx = Keyword.get(opts, :instance_ctx)
    name = if ctx, do: Router.shard_name(ctx, index), else: :"Ferricstore.Store.Shard.#{index}"
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  # -------------------------------------------------------------------
  # GenServer callbacks
  # -------------------------------------------------------------------

  use Ferricstore.Store.Shard.Startup
  use Ferricstore.Store.Shard.Calls
  use Ferricstore.Store.Shard.Routing
  use Ferricstore.Store.Shard.Compaction
  use Ferricstore.Store.Shard.Info
end
